#include "src/meta_protocol_proxy/filters/router/upstream_request.h"

#include "envoy/upstream/thread_local_cluster.h"

#include "src/meta_protocol_proxy/app_exception.h"
#include "src/meta_protocol_proxy/codec/codec.h"

namespace Envoy {
namespace Extensions {
namespace NetworkFilters {
namespace MetaProtocolProxy {
namespace Router {

//UpstreamRequest::UpstreamRequest(RequestOwner& parent, Upstream::TcpPoolData& pool,
//                                 MetadataSharedPtr& metadata, MutationSharedPtr& mutation)
//    : parent_(parent), conn_pool_(pool), metadata_(metadata), mutation_(mutation),
//      request_complete_(false), response_started_(false), response_complete_(false),
//      stream_reset_(false) {
//
UpstreamRequestBase::UpstreamRequestBase(RequestOwner& parent, MetadataSharedPtr& metadata,
                                         MutationSharedPtr& mutation)
    : parent_(parent), metadata_(metadata), mutation_(mutation), request_complete_(false),
      response_started_(false), response_complete_(false), stream_reset_(false) {
  // 这里其实是最关键的初始化
  upstream_request_buffer_.move(metadata->originMessage(), metadata->originMessage().length());
}

UpstreamRequest::UpstreamRequest(RequestOwner& parent, Upstream::TcpPoolData& pool,
                                 MetadataSharedPtr& metadata, MutationSharedPtr& mutation)
    : UpstreamRequestBase(parent, metadata, mutation), conn_pool_(pool) {}

FilterStatus UpstreamRequest::start() {
  // 这里通过调用连接池的 newConnection 方法, 获取 conn
  // 并将 UpstreamRequest 自身作为 callback 传入 newConnection 方法。
  // 如果 handle != null, 则返回的是 pendingStream
  Tcp::ConnectionPool::Cancellable* handle = conn_pool_.newConnection(*this);
  if (handle) {
    ENVOY_LOG(debug, "UpstreamRequest::start() PauseIteration tcloudTraceId={}", metadata_->getString("tcloudTraceId"));
    // Pause while we wait for a connection.
    // 暂停等待连接
    conn_pool_handle_ = handle;
    return FilterStatus::PauseIteration;
  }

  ENVOY_LOG(debug, "UpstreamRequest::start() ContinueIteration tcloudTraceId={}", metadata_->getString("tcloudTraceId"));
  return FilterStatus::ContinueIteration;
}

void UpstreamRequestBase::onUpstreamConnectionEvent(Network::ConnectionEvent event) {
  ASSERT(!response_complete_);

  switch (event) {
  case Network::ConnectionEvent::RemoteClose: {
    ENVOY_LOG(debug, "meta protocol router: upstream remote close");
    onUpstreamConnectionReset(ConnectionPool::PoolFailureReason::RemoteConnectionFailure);
    upstream_host_->outlierDetector().putResult(
        Upstream::Outlier::Result::LocalOriginConnectFailed);
    break;
  }
  case Network::ConnectionEvent::LocalClose: {
    ENVOY_LOG(debug, "meta protocol router: upstream local close");
    onUpstreamConnectionReset(ConnectionPool::PoolFailureReason::LocalConnectionFailure);
    break;
  }
  default:
    // Connected event is consumed by the connection pool.
    PANIC("not reached");
  }
}

void UpstreamRequest::releaseUpStreamConnection(bool close) {
  stream_reset_ = true;

  // we're still waiting for the connection pool to create an upstream connection
  if (conn_pool_handle_) {
    ASSERT(!conn_data_);
    conn_pool_handle_->cancel(Tcp::ConnectionPool::CancelPolicy::Default);
    conn_pool_handle_ = nullptr;
    ENVOY_LOG(debug, "meta protocol upstream request: cancel pending upstream connection");
  }

  // we already got an upstream connection from the pool
  // The event triggered by close will also release this connection so clear conn_data_ before
  // closing.
  // upstream connection is released back to the pool for re-use when it's containing
  // ConnectionData is destroyed
  // important: two threads(dispatcher thread and the upstream conn thread) are operating on the
  // class variable conn_data_?
  // Move conn_data_ to local variable because it may be released by the upstream response, which
  // will cause segment fault
  Tcp::ConnectionPool::ConnectionDataPtr conn_data = std::move(conn_data_);
  ENVOY_LOG(debug, "meta protocol upstream request: release upstream connection");
  // 增加 conn_data->connection() 判断
  if (close && conn_data != nullptr) {
      auto conn =  conn_data->connection();
      ENVOY_LOG(debug, "meta protocol upstream request: 判断是否 connection() 报错");
      // we shouldn't close the upstream connection unless explicitly asked at some exceptional cases
      conn_data->connection().close(Network::ConnectionCloseType::NoFlush);
      ENVOY_LOG(warn, "meta protocol upstream request: close upstream connection");
  }
}

void UpstreamRequest::encodeData(Buffer::Instance& data) {
  ASSERT(conn_data_);
  ASSERT(!conn_pool_handle_);

  ENVOY_LOG(debug, "proxying {} bytes, tcloudTraceId={}", data.length(), metadata_->getString("tcloudTraceId"));
  auto codec = parent_.createCodec();
  // 这里什么都没做
  codec->encode(*metadata_, *mutation_, data);
  conn_data_->connection().write(data, false);
}

void UpstreamRequest::onPoolFailure(ConnectionPool::PoolFailureReason reason, absl::string_view,
                                    Upstream::HostDescriptionConstSharedPtr host) {

  ENVOY_LOG(debug, "meta protocol upstream request onPoolFailure, tcloudTraceId={}, 上游 ip={}, reason={}", metadata_->getString("tcloudTraceId"), host->address()->asString(), int(reason));
  parent_.onUpstreamHostSelected(host);
  conn_pool_handle_ = nullptr;

  // Mimic an upstream reset.
  onUpstreamHostSelected(host);
  onUpstreamConnectionReset(reason);

  upstream_request_buffer_.drain(upstream_request_buffer_.length());

  // If it is a connection error, it means that the connection pool returned
  // the error asynchronously and the upper layer needs to be notified to continue decoding.
  // If it is a non-connection error, it is returned synchronously from the connection pool
  // and is still in the callback at the current Filter, nothing to do.
  if (reason == ConnectionPool::PoolFailureReason::Timeout ||
      reason == ConnectionPool::PoolFailureReason::LocalConnectionFailure ||
      reason == ConnectionPool::PoolFailureReason::RemoteConnectionFailure) {
    if (reason == ConnectionPool::PoolFailureReason::Timeout) {
      host->outlierDetector().putResult(Upstream::Outlier::Result::LocalOriginTimeout);
    } else if (reason == ConnectionPool::PoolFailureReason::RemoteConnectionFailure) {
      host->outlierDetector().putResult(Upstream::Outlier::Result::LocalOriginConnectFailed);
    }
    parent_.continueDecoding();
  }
}

//void UpstreamRequest::onRequestComplete() {
//  request_complete_ = true;
//  // todo
//}

void UpstreamRequest::onPoolReady(Tcp::ConnectionPool::ConnectionDataPtr&& conn_data,
                                  Upstream::HostDescriptionConstSharedPtr host) {
  ENVOY_LOG(debug, "meta protocol upstream request: tcp connection is ready");
  ENVOY_LOG(debug, "meta protocol upstream request onPoolReady, 回调 onPoolReady, tcloudTraceId={}, 上游 ip={}", metadata_->getString("tcloudTraceId"), host->address()->asString());
  parent_.onUpstreamHostSelected(host);

  // Only invoke continueDecoding if we'd previously stopped the filter chain.
  bool continue_decoding = conn_pool_handle_ != nullptr;

  onUpstreamHostSelected(host);
  host->outlierDetector().putResult(Upstream::Outlier::Result::LocalOriginConnectSuccess);

  conn_data_ = std::move(conn_data);
  // 这里 parent_.upstreamCallbacks() 是 router 的 upstreamCallbacks
  // todo 什么时候触发这个 callback 后面看
  if (metadata_->getMessageType() == MessageType::Request) {
    conn_data_->addUpstreamCallbacks(parent_.upstreamCallbacks());
  }
  conn_pool_handle_ = nullptr;

  // Store the upstream ip to the metadata, which will be used in the response
  // 因为连接池本来就是针对 host 的, 所以通过 host 获取的 connection 当然 remoteAddress 是上游的 ip 了。
  metadata_->putString(
      ReservedHeaders::RealServerAddress,
      conn_data_->connection().connectionInfoProvider().remoteAddress()->asString());

  onRequestStart(continue_decoding);
  encodeData(upstream_request_buffer_);

  if (metadata_->getMessageType() == MessageType::Stream_Init) {
    // For streaming requests, we handle the following server response message in the stream
    ENVOY_LOG(debug, "meta protocol upstream request: the request is a stream init message");
    // todo change to a more appreciate method name, maybe clearMessage()
    parent_.resetStream();
    parent_.setUpstreamConnection(std::move(conn_data_));
  }
  onRequestComplete();
}

void UpstreamRequestBase::onRequestStart(bool continue_decoding) {
  ENVOY_LOG(debug, "meta protocol upstream request: start sending data to the server {}",
            upstream_host_->address()->asString());

  if (continue_decoding) {
    parent_.continueDecoding();
  }
}

void UpstreamRequest::onResponseComplete() {
  response_complete_ = true;
  conn_data_.reset();
}

void UpstreamRequestBase::onUpstreamHostSelected(Upstream::HostDescriptionConstSharedPtr host) {
  ENVOY_LOG(debug, "meta protocol upstream request: selected upstream {}",
            host->address()->asString());
  upstream_host_ = host;
}

void UpstreamRequestBase::onUpstreamConnectionReset(ConnectionPool::PoolFailureReason reason) {
  if (metadata_->getMessageType() == MessageType::Oneway) {
    // For oneway requests, we should not attempt a response. Reset the downstream to signal
    // an error.
    ENVOY_LOG(debug,
              "meta protocol upstream request: the request is oneway, reset downstream stream");
    parent_.resetStream();
    return;
  }

  // When the filter's callback does not end, the sendLocalReply function call
  // triggers the release of the current stream at the end of the filter's callback.
  switch (reason) {
  case ConnectionPool::PoolFailureReason::Overflow:
    parent_.sendLocalReply(
        AppException(Error{ErrorType::Unspecified,
                           fmt::format("meta protocol upstream request: too many connections")}),
        false);
    break;
  case ConnectionPool::PoolFailureReason::LocalConnectionFailure:
    // Should only happen if we closed the connection, due to an error condition, in which case
    // we've already handled any possible downstream response.
    parent_.sendLocalReply(
        AppException(
            Error{ErrorType::Unspecified,
                  fmt::format("meta protocol upstream request: local connection failure '{}'",
                              upstream_host_->address()->asString())}),
        false);
    break;
  case ConnectionPool::PoolFailureReason::RemoteConnectionFailure:
    parent_.sendLocalReply(
        AppException(
            Error{ErrorType::Unspecified,
                  fmt::format("meta protocol upstream request: remote connection failure '{}'",
                              upstream_host_->address()->asString())}),
        false);
    break;
  case ConnectionPool::PoolFailureReason::Timeout:
    parent_.sendLocalReply(
        AppException(Error{
            ErrorType::Unspecified,
            fmt::format("meta protocol upstream request: connection failure '{}' due to timeout",
                        upstream_host_->address()->asString())}),
        false);
    break;
  default:
    PANIC("not reached");
  }
  if (!response_complete_) {
    parent_.resetStream();
  }
}

UpstreamRequestByHandler::UpstreamRequestByHandler(RequestOwner& parent,
                                                   MetadataSharedPtr& metadata,
                                                   MutationSharedPtr& mutation,
                                                   UpstreamHandlerSharedPtr& upstream_handler)
    : UpstreamRequestBase(parent, metadata, mutation), upstream_handler_(upstream_handler) {}

void UpstreamRequestByHandler::onPoolFailure(ConnectionPool::PoolFailureReason reason,
                                             absl::string_view,
                                             Upstream::HostDescriptionConstSharedPtr host) {
  parent_.onUpstreamHostSelected(host);

  // Mimic an upstream reset.
  onUpstreamHostSelected(host);
  onUpstreamConnectionReset(reason);

  upstream_request_buffer_.drain(upstream_request_buffer_.length());

  // If it is a connection error, it means that the connection pool returned
  // the error asynchronously and the upper layer needs to be notified to continue decoding.
  // If it is a non-connection error, it is returned synchronously from the connection pool
  // and is still in the callback at the current Filter, nothing to do.
  if (reason == ConnectionPool::PoolFailureReason::Timeout ||
      reason == ConnectionPool::PoolFailureReason::LocalConnectionFailure ||
      reason == ConnectionPool::PoolFailureReason::RemoteConnectionFailure) {
    parent_.continueDecoding();
  }
}

void UpstreamRequestByHandler::onPoolReady(Upstream::HostDescriptionConstSharedPtr host) {
  ENVOY_LOG(debug, "meta protocol upstream request: tcp connection is ready");
  parent_.onUpstreamHostSelected(host);

  onUpstreamHostSelected(host);

  onRequestStart(true);
  encodeData(upstream_request_buffer_);

  onRequestComplete();
}

//void UpstreamRequestByHandler::onRequestComplete() {
//  request_complete_ = true;
//  // todo
//}

FilterStatus UpstreamRequestByHandler::start() {
  if (upstream_handler_->isPoolReady()) {
    encodeData(upstream_request_buffer_);
    return FilterStatus::ContinueIteration;
  } else {
    upstream_handler_->addUpsteamRequestCallbacks(this);
    return FilterStatus::PauseIteration;
  }
}

void UpstreamRequestByHandler::releaseUpStreamConnection(bool) {
  upstream_handler_->removeUpsteamRequestCallbacks(this);
}

void UpstreamRequestByHandler::encodeData(Buffer::Instance& data) {
  ENVOY_LOG(trace, "proxying {} bytes", data.length());
  auto codec = parent_.createCodec();
  codec->encode(*metadata_, *mutation_, data);
  upstream_handler_->onData(upstream_request_buffer_, false);
}

} // namespace Router
} // namespace  MetaProtocolProxy
} // namespace NetworkFilters
} // namespace Extensions
} // namespace Envoy
