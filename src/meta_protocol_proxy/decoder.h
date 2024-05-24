#pragma once

#include "envoy/buffer/buffer.h"

#include "source/common/buffer/buffer_impl.h"
#include "source/common/common/logger.h"
#include "envoy/tcloud/tcloud_map.h"
#include "src/meta_protocol_proxy/codec/codec.h"
#include "src/meta_protocol_proxy/decoder_event_handler.h"

namespace Envoy {
namespace Extensions {
namespace NetworkFilters {
namespace MetaProtocolProxy {

#define ALL_PROTOCOL_STATES(FUNCTION)                                                              \
  FUNCTION(WaitForData)                                                                            \
  FUNCTION(OnDecodeStreamData)                                                                     \
  FUNCTION(Done)

/**
 * ProtocolState represents a set of states used in a state machine to decode requests and
 * responses.
 */
enum class ProtocolState { ALL_PROTOCOL_STATES(GENERATE_ENUM) };

class ProtocolStateNameValues {
public:
  static const std::string& name(ProtocolState state) {
    size_t i = static_cast<size_t>(state);
    ASSERT(i < names().size());
    return names()[i];
  }

private:
  static const std::vector<std::string>& names() {
    CONSTRUCT_ON_FIRST_USE(std::vector<std::string>, {ALL_PROTOCOL_STATES(GENERATE_STRING)});
  }
};

struct ActiveStream {
  ActiveStream(MessageHandler& handler, MetadataSharedPtr metadata, MutationSharedPtr mutation)
      : handler_(handler), metadata_(metadata), mutation_(mutation) {}
  ~ActiveStream() {
    metadata_.reset();
    mutation_.reset();
  }

  void onStreamDecoded() {
    ASSERT(metadata_ && mutation_);
    handler_.onMessageDecoded(metadata_, mutation_);
  }

  MessageHandler& handler_;
  MetadataSharedPtr metadata_;
  MutationSharedPtr mutation_;
};

using ActiveStreamPtr = std::unique_ptr<ActiveStream>;

class DecoderStateMachine : public Logger::Loggable<Logger::Id::filter> {
public:
  class Delegate {
  public:
    virtual ~Delegate() = default;
    virtual ActiveStream* newStream(MetadataSharedPtr metadata, MutationSharedPtr mutation) PURE;

    /**
     * Handle heartbeat message
     * @param metadata
     * @return whether to continue waiting for response
     */
    virtual bool onHeartbeat(MetadataSharedPtr metadata) PURE;
  };

  DecoderStateMachine(Codec& codec, MessageType messageType, Delegate& delegate,
                      std::shared_ptr<Envoy::TcloudMap::TcloudMap<std::string, std::string, Envoy::TcloudMap::LFUCachePolicy>> tcloud_map)
      : codec_(codec), messageType_(messageType), delegate_(delegate),
        state_(ProtocolState::OnDecodeStreamData), tcloud_map_(tcloud_map) {}
  ~DecoderStateMachine() {
    ENVOY_LOG(trace, "********** DecoderStateMachine destructed ***********");
  }

  /**
   * Consumes as much data from the configured Buffer as possible and executes the decoding state
   * machine. Returns ProtocolState::WaitForData if more data is required to complete processing of
   * a message. Returns ProtocolState::Done when the end of a message is successfully processed.
   * Once the Done state is reached, further invocations of run return immediately with Done.
   *
   * @param buffer a buffer containing the remaining data to be processed
   * @return ProtocolState returns with ProtocolState::WaitForData or ProtocolState::Done
   * @throw Envoy Exception if thrown by the underlying Protocol
   */
  ProtocolState run(Buffer::Instance& buffer);

  /**
   * @return the current ProtocolState
   */
  ProtocolState currentState() const { return state_; }

  // tcloud 泳道
  std::shared_ptr<Envoy::TcloudMap::TcloudMap<std::string, std::string, Envoy::TcloudMap::LFUCachePolicy>> getTcloudMap() { return tcloud_map_; }

private:
  ProtocolState onDecodeStream(Buffer::Instance& buffer);

  Codec& codec_;
  MessageType messageType_;
  Delegate& delegate_;
  ProtocolState state_;

  // tcloud 泳道相关
  std::shared_ptr<Envoy::TcloudMap::TcloudMap<std::string, std::string, Envoy::TcloudMap::LFUCachePolicy>> tcloud_map_;
};

using DecoderStateMachinePtr = std::unique_ptr<DecoderStateMachine>;

class DecoderBase : public DecoderStateMachine::Delegate,
                    public Logger::Loggable<Logger::Id::filter> {
public:
  DecoderBase(Codec& codec, MessageType messageType, std::shared_ptr<Envoy::TcloudMap::TcloudMap<std::string, std::string, Envoy::TcloudMap::LFUCachePolicy>> tcloud_map);
  ~DecoderBase() override;

  /**
   * Drains data from the given buffer
   *
   * @param data a Buffer containing protocol data
   * @param buffer_underflow bool set to true if more data is required to continue decoding
   * @throw EnvoyException on protocol errors
   */
  void onData(Buffer::Instance& data, bool& buffer_underflow);

  // It is assumed that all of the protocol parsing are stateless,
  // if there is a state of the need to provide the reset interface call here.
  void reset();

  // tcloud
  std::shared_ptr<Envoy::TcloudMap::TcloudMap<std::string, std::string, Envoy::TcloudMap::LFUCachePolicy>> getTcloudMap() { return tcloud_map_; }

protected:
  void start();
  void complete();

  Codec& codec_;
  ActiveStreamPtr stream_;
  DecoderStateMachinePtr state_machine_;
  MessageType messageType_;
  bool decode_started_{false};

  // tcloud 泳道相关
  std::shared_ptr<Envoy::TcloudMap::TcloudMap<std::string, std::string, Envoy::TcloudMap::LFUCachePolicy>> tcloud_map_;
};

/**
 * Decoder encapsulates a configured and ProtocolPtr and SerializationPtr.
 */
template <typename T> class Decoder : public DecoderBase {
public:
  Decoder(Codec& codec, T& callbacks, MessageType messageType, std::shared_ptr<Envoy::TcloudMap::TcloudMap<std::string, std::string, Envoy::TcloudMap::LFUCachePolicy>> tcloud_map)
      : DecoderBase(codec, messageType, tcloud_map), callbacks_(callbacks) {}

  ActiveStream* newStream(MetadataSharedPtr metadata, MutationSharedPtr mutation) override {
    ASSERT(!stream_);
    // request 从 connManager 过来的，这里 callbacks 就是 connManager 自身。
    // 在 connManager 中 newMessageHandler 方法返回的是 ActiveMessage
    stream_ = std::make_unique<ActiveStream>(callbacks_.newMessageHandler(), metadata, mutation);
    return stream_.get();
  }

  bool onHeartbeat(MetadataSharedPtr metadata) override { return callbacks_.onHeartbeat(metadata); }

private:
  T& callbacks_;
};

class RequestDecoder : public Decoder<RequestDecoderCallbacks> {
public:
  // 这里分别针对 Request 设置了 MessageType::Request
  RequestDecoder(Codec& codec, RequestDecoderCallbacks& callbacks, std::shared_ptr<Envoy::TcloudMap::TcloudMap<std::string, std::string, Envoy::TcloudMap::LFUCachePolicy>> tcloud_map)
      : Decoder(codec, callbacks, MessageType::Request, tcloud_map) {}
  ~RequestDecoder() { ENVOY_LOG(trace, "********** RequestDecoder destructed ***********"); };
};

using RequestDecoderPtr = std::unique_ptr<RequestDecoder>;

class ResponseDecoder : public Decoder<ResponseDecoderCallbacks> {
public:
  // 这里分别针对 Request 设置了 MessageType::Response
  ResponseDecoder(Codec& codec, ResponseDecoderCallbacks& callbacks, std::shared_ptr<Envoy::TcloudMap::TcloudMap<std::string, std::string, Envoy::TcloudMap::LFUCachePolicy>> tcloud_map = nullptr)
      : Decoder(codec, callbacks, MessageType::Response, tcloud_map) {}
  ~ResponseDecoder() { ENVOY_LOG(trace, "********** ResponseDecoder destructed ***********"); };
};

using ResponseDecoderPtr = std::unique_ptr<ResponseDecoder>;

} // namespace MetaProtocolProxy
} // namespace NetworkFilters
} // namespace Extensions
} // namespace Envoy
