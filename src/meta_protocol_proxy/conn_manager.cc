#include "src/meta_protocol_proxy/conn_manager.h"

#include <cstdint>

#include "envoy/common/exception.h"

#include "source/common/common/fmt.h"
#include "src/meta_protocol_proxy/app_exception.h"
#include "src/meta_protocol_proxy/heartbeat_response.h"
#include "src/meta_protocol_proxy/codec_impl.h"
#include "src/meta_protocol_proxy/upstream_handler_impl.h"

namespace Envoy {
namespace Extensions {
namespace NetworkFilters {
namespace MetaProtocolProxy {

constexpr uint32_t BufferLimit = UINT32_MAX;

ConnectionManager::ConnectionManager(Config& config, Random::RandomGenerator& random_generator,
                                     TimeSource& time_system,
                                     Upstream::ClusterManager& cluster_manager,
                                     std::shared_ptr<Envoy::TcloudMap::TcloudMap<std::string, std::string, Envoy::TcloudMap::LFUCachePolicy>> tcloud_map = nullptr)
    : config_(config), time_system_(time_system), stats_(config_.stats()),
      random_generator_(random_generator), codec_(config.createCodec()),
      decoder_(std::make_unique<RequestDecoder>(*codec_, *this, tcloud_map)),
      cluster_manager_(cluster_manager), tcloud_map_(tcloud_map) {
    // 给 codec 传入 tcloud_map
    codec_->setTcloudMap(tcloud_map);
}

Network::FilterStatus ConnectionManager::onData(Buffer::Instance& data, bool end_stream) {
  ENVOY_LOG(debug, "meta protocol: ConnectionManager::onData read {} bytes", data.length());
  request_buffer_.move(data);
  dispatch();

  if (end_stream) {
    ENVOY_CONN_LOG(debug, "meta protocol: ConnectionManager::onData downstream connection has been closed",
                   read_callbacks_->connection());
//=======
//    ENVOY_CONN_LOG(debug, "downstream half-closed", read_callbacks_->connection());
//
//    // Downstream has closed. Unless we're waiting for an upstream connection to complete a oneway
//    // request, close. The special case for oneway requests allows them to complete before the
//    // ConnectionManager is destroyed.
//    // 下游已关闭。除非我们正在等待上游连接完成单向请求，否则请关闭。
//    // 单向请求的特殊情况允许它们在 ConnectionManager 被销毁之前完成
//    if (stopped_) {
//      ASSERT(!active_message_list_.empty());
//      auto metadata = (*active_message_list_.begin())->metadata();
//      if (metadata && metadata->getMessageType() == MessageType::Oneway) {
//        ENVOY_CONN_LOG(trace, "waiting for one-way completion", read_callbacks_->connection());
//        half_closed_ = true;
//        return Network::FilterStatus::StopIteration;
//      }
//    }
//>>>>>>> c512818 (泳道功能fix)

    resetAllMessages(false);
    clearStream();
    read_callbacks_->connection().close(Network::ConnectionCloseType::FlushWrite);
  }

  return Network::FilterStatus::StopIteration;
}

// connManager 的初始化
Network::FilterStatus ConnectionManager::onNewConnection() {
  // init idle timer.
  if (config_.idleTimeout()) {
    idle_timer_ =
        read_callbacks_->connection().dispatcher().createTimer([this]() { this->onIdleTimeout(); });
    resetIdleTimer();
  }
  return Network::FilterStatus::Continue;
}

// 其实这里 callbacks 是 connManager 自身。
void ConnectionManager::initializeReadFilterCallbacks(Network::ReadFilterCallbacks& callbacks) {
  read_callbacks_ = &callbacks;
  read_callbacks_->connection().addConnectionCallbacks(*this);
  read_callbacks_->connection().enableHalfClose(true);
  read_callbacks_->connection().setBufferLimits(BufferLimit);
}

// 这里也只看到处理了自身的一些逻辑，并没有看到通知 ClientConn 的逻辑。
void ConnectionManager::onEvent(Network::ConnectionEvent event) {
  ENVOY_LOG(debug, "ConnectionManager onEvent {}", static_cast<int>(event));
  if (event == Network::ConnectionEvent::LocalClose) {
    disableIdleTimer();
    resetAllMessages(true);
    // clearStream();
    resetUpstreamHandlerManager();
    // read_callbacks_->connection().close(Network::ConnectionCloseType::NoFlush);
  } else if (event == Network::ConnectionEvent::RemoteClose) {
    disableIdleTimer();
    resetAllMessages(false);
    // clearStream();
    resetUpstreamHandlerManager();
    // read_callbacks_->connection().close(Network::ConnectionCloseType::NoFlush);
  }
}

void ConnectionManager::onAboveWriteBufferHighWatermark() {
  ENVOY_CONN_LOG(debug, "onAboveWriteBufferHighWatermark", read_callbacks_->connection());
  read_callbacks_->connection().readDisable(true);
}

void ConnectionManager::onBelowWriteBufferLowWatermark() {
  ENVOY_CONN_LOG(debug, "onBelowWriteBufferLowWatermark", read_callbacks_->connection());
  read_callbacks_->connection().readDisable(false);
}

MessageHandler& ConnectionManager::newMessageHandler() {
  ENVOY_LOG(debug, "meta protocol: create the new decoder event handler");

  ActiveMessagePtr new_message(std::make_unique<ActiveMessage>(*this));
  new_message->createFilterChain();
  LinkedList::moveIntoList(std::move(new_message), active_message_list_);
  return **active_message_list_.begin();
}

bool ConnectionManager::onHeartbeat(MetadataSharedPtr metadata) {
  stats_.request_event_.inc();
  if (read_callbacks_->connection().state() != Network::Connection::State::Open) {
    ENVOY_LOG(warn, "meta protocol: downstream connection is closed or closing");
    return false;
  }

  HeartbeatResponse heartbeat;
  Buffer::OwnedImpl response_buffer;

  heartbeat.encode(*metadata, *codec_, response_buffer);
  read_callbacks_->connection().write(response_buffer, false);
  return false;
}

void ConnectionManager::dispatch() {
  if (0 == request_buffer_.length()) {
    ENVOY_LOG(debug, "ConnectionManager::dispatch meta protocol: it's empty data");
    return;
  }
  // when data is not empty,it will enable timer again.
  // 重置计时器
  resetIdleTimer();
  try {
    bool underflow = false;
    // decoder return underflow in th following two cases:
    // 1. decoder needs more data to complete the decoding of the current message, in this case, the
    // buffer contains part of the incomplete message.
    // 2. all the messages in the buffer have been processed, in this case, the buffer is already
    // empty.

    // 解码器在以下两种情况下返回 underflow：
    // 1.解码器需要更多的数据来完成当前消息的解码，此时，
    // 缓冲区包含部分不完整消息。
    // 2. 缓冲区中的所有消息均已处理完毕，此时缓冲区已为空。
    while (!underflow) {
      decoder_->onData(request_buffer_, underflow);
    }
    return;
  } catch (const EnvoyException& ex) {
    ENVOY_CONN_LOG(error, "ConnectionManager::dispatch meta protocol error: {}", read_callbacks_->connection(), ex.what());
    read_callbacks_->connection().close(Network::ConnectionCloseType::NoFlush);
    stats_.request_decoding_error_.inc();
  }
  // 这个只有在 catch 之后才会执行
  resetAllMessages(true);
}

void ConnectionManager::sendLocalReply(Metadata& metadata, const DirectResponse& response,
                                       bool end_stream) {
  if (read_callbacks_->connection().state() != Network::Connection::State::Open) {
    return;
  }

  DirectResponse::ResponseType result = DirectResponse::ResponseType::ErrorReply;

  try {
    Buffer::OwnedImpl buffer;
    result = response.encode(metadata, *codec_, buffer);

    read_callbacks_->connection().write(buffer, end_stream);
  } catch (const EnvoyException& ex) {
    ENVOY_CONN_LOG(error, "meta protocol error: {}", read_callbacks_->connection(), ex.what());
  }

  if (end_stream) {
    read_callbacks_->connection().close(Network::ConnectionCloseType::FlushWrite);
  }

  switch (result) {
  case DirectResponse::ResponseType::SuccessReply:
    stats_.local_response_success_.inc();
    break;
  case DirectResponse::ResponseType::ErrorReply:
    stats_.local_response_error_.inc();
    break;
  case DirectResponse::ResponseType::Exception:
    stats_.local_response_business_exception_.inc();
    break;
  default:
    PANIC("invalid response type");
  }
}

Stream& ConnectionManager::newActiveStream(uint64_t stream_id) {
  ENVOY_CONN_LOG(debug, "meta protocol: create an active stream: {}", connection(), stream_id);
  StreamPtr new_stream(std::make_unique<Stream>(stream_id, connection(), *this, *codec_));
  active_stream_map_[stream_id] = std::move(new_stream);
  return *active_stream_map_.find(stream_id)->second;
}

Stream& ConnectionManager::getActiveStream(uint64_t stream_id) {
  auto iter = active_stream_map_.find(stream_id);
  ASSERT(iter != active_stream_map_.end());
  return *iter->second;
}

bool ConnectionManager::streamExisted(uint64_t stream_id) {
  auto iter = active_stream_map_.find(stream_id);
  return (iter != active_stream_map_.end());
}

void ConnectionManager::closeStream(uint64_t stream_id) {
  ENVOY_LOG(debug, "meta protocol: close stream {} ", stream_id);
  active_stream_map_.erase(stream_id);
}

// todo 这里没看懂
void ConnectionManager::deferredDeleteMessage(ActiveMessage& message) {
  if (!message.inserted()) {
    return;
  }
  ENVOY_LOG(debug, "meta protocol: deferred delete message, id is {}",
            message.metadata()->getRequestId());
  read_callbacks_->connection().dispatcher().deferredDelete(
      message.removeFromList(active_message_list_));
}

void ConnectionManager::resetAllMessages(bool local_reset) {
  while (!active_message_list_.empty()) {
    if (local_reset) {
      ENVOY_CONN_LOG(debug, "local close with active request", read_callbacks_->connection());
      stats_.cx_destroy_local_with_active_rq_.inc();
    } else {
      ENVOY_CONN_LOG(debug, "remote close with active request", read_callbacks_->connection());
      stats_.cx_destroy_remote_with_active_rq_.inc();
    }

    active_message_list_.front()->onReset();
  }
}

// 超时后的处理逻辑
// 这里我们看到只处理了 ServerConnection, 而并没有针对 ClientConnection 做任何处理。
// 带着这个问题我们继续往下看。
void ConnectionManager::onIdleTimeout() {
  ENVOY_CONN_LOG(debug, "meta protocol:Session timed out", read_callbacks_->connection());
  stats_.idle_timeout_.inc();
  resetAllMessages(true);
  clearStream();
  // resetUpstreamHandlerManager();
  read_callbacks_->connection().close(Network::ConnectionCloseType::NoFlush);
}

void ConnectionManager::resetIdleTimer() {
  if (idle_timer_ != nullptr) {
    ASSERT(config_.idleTimeout());
    idle_timer_->enableTimer(config_.idleTimeout().value());
  }
}

void ConnectionManager::disableIdleTimer() {
  if (idle_timer_ != nullptr) {
    idle_timer_->disableTimer();
    idle_timer_.reset();
  }
}

void ConnectionManager::resetUpstreamHandlerManager() { upstream_handler_manager_.clear(); }

GetUpstreamHandlerResult
ConnectionManager::getUpstreamHandler(const std::string& cluster_name,
                                      Upstream::LoadBalancerContext& context) {
  auto* cluster = cluster_manager_.getThreadLocalCluster(cluster_name);
  if (cluster == nullptr) {
    ENVOY_LOG(error, "unknown cluster '{}'", cluster_name);
    return {Error{ErrorType::ClusterNotFound,
                  fmt::format("meta protocol router: unknown cluster '{}'", cluster_name)},
            nullptr, "unknown_cluster"};
  }

  auto tcp_pool_data = UpstreamHandler::createTcpPoolData(*cluster, context);
  if (!tcp_pool_data) {
    ENVOY_LOG(error, "no healthy upstream for {}", cluster_name);
    return {Error{ErrorType::NoHealthyUpstream,
                  fmt::format("meta protocol router: no healthy upstream for '{}'", cluster_name)},
            nullptr, "no_healthy_upstream"};
  }
  std::string key = cluster_name + "_" + tcp_pool_data.value().host()->address()->asString();

  // get exist upstream handler
  auto upstream_handler = upstream_handler_manager_.get(key);
  if (upstream_handler) {
    ENVOY_LOG(debug, "use exist upstream handler, key:{}", key);
    return {absl::nullopt, upstream_handler, ""};
  }

  // create upstream handler
  ENVOY_LOG(debug, "create upstream handler: key={}, hostname={}, address={}", key,
            tcp_pool_data.value().host()->hostname(),
            tcp_pool_data.value().host()->address()->asString());

  auto new_upstream_handler = std::make_shared<UpstreamHandlerImpl>(
      key, read_callbacks_->connection(), config_,
      [this](const std::string& key) { this->upstream_handler_manager_.del(key); });
  ASSERT(new_upstream_handler);

  upstream_handler_manager_.add(key, new_upstream_handler);

  new_upstream_handler->start(*tcp_pool_data);
  return {absl::nullopt, new_upstream_handler, ""};
}

} // namespace  MetaProtocolProxy
} // namespace NetworkFilters
} // namespace Extensions
} // namespace Envoy
