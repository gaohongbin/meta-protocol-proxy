#include "src/application_protocols/dubbo/dubbo_protocol_impl.h"

#include "envoy/registry/registry.h"

#include "source/common/common/assert.h"
#include "src/application_protocols/dubbo/message_impl.h"

namespace Envoy {
namespace Extensions {
namespace NetworkFilters {
namespace MetaProtocolProxy {
namespace Dubbo {
namespace {

constexpr uint16_t MagicNumber = 0xdabb;
constexpr uint8_t MessageTypeMask = 0x80;
constexpr uint8_t TwoWayMask = 0x40;
constexpr uint8_t EventMask = 0x20;
constexpr uint8_t SerializationTypeMask = 0x1f;
constexpr uint64_t FlagOffset = 2;
constexpr uint64_t StatusOffset = 3;
constexpr uint64_t RequestIDOffset = 4;
constexpr uint64_t BodySizeOffset = 12;

// 这些字段的定义, 可以参考如下链接, 其中: FlagOffset, StatusOffset, RequestIDOffset, BodySizeOffset 分别
// 表示第几个字节, 第 0,1 字节为 dubbo 的 magic, 标识该协议为 dubbo 协议, 真正的数据从 第 2 个字节开始。
// https://dubbo.apache.org/zh/blog/2018/10/05/dubbo-%E5%8D%8F%E8%AE%AE%E8%AF%A6%E8%A7%A3/

} // namespace

// Consistent with the SerializationType
bool isValidSerializationType(SerializationType type) {
  switch (type) {
  case SerializationType::Hessian2:
    break;
  default:
    return false;
  }
  return true;
}

// Consistent with the ResponseStatus
bool isValidResponseStatus(ResponseStatus status) {
  switch (status) {
  case ResponseStatus::Ok:
  case ResponseStatus::ClientTimeout:
  case ResponseStatus::ServerTimeout:
  case ResponseStatus::BadRequest:
  case ResponseStatus::BadResponse:
  case ResponseStatus::ServiceNotFound:
  case ResponseStatus::ServiceError:
  case ResponseStatus::ClientError:
  case ResponseStatus::ServerThreadpoolExhaustedError:
    break;
  default:
    return false;
  }
  return true;
}

void parseRequestInfoFromBuffer(Buffer::Instance& data, MessageMetadataSharedPtr metadata) {
  ASSERT(data.length() >= DubboProtocolImpl::MessageSize);
  uint8_t flag = data.peekInt<uint8_t>(FlagOffset);
  bool is_two_way = (flag & TwoWayMask) == TwoWayMask ? true : false;
  SerializationType type = static_cast<SerializationType>(flag & SerializationTypeMask);
  // 这里判断是否可用的序列化 ID, 目前只支持 Hessian2, 看来后续需要以此来拓展。
  if (!isValidSerializationType(type)) {
    throw EnvoyException(
        absl::StrCat("invalid dubbo message serialization type ",
                     static_cast<std::underlying_type<SerializationType>::type>(type)));
  }

  if (!is_two_way && metadata->messageType() != MessageType::HeartbeatRequest) {
    metadata->setMessageType(MessageType::Oneway);
  }

  metadata->setSerializationType(type);
}

void parseResponseInfoFromBuffer(Buffer::Instance& buffer, MessageMetadataSharedPtr metadata) {
  ASSERT(buffer.length() >= DubboProtocolImpl::MessageSize);
  ResponseStatus status = static_cast<ResponseStatus>(buffer.peekInt<uint8_t>(StatusOffset));
  if (!isValidResponseStatus(status)) {
    throw EnvoyException(
        absl::StrCat("invalid dubbo message response status ",
                     static_cast<std::underlying_type<ResponseStatus>::type>(status)));
  }

  metadata->setResponseStatus(status);
}

std::pair<ContextSharedPtr, bool>
DubboProtocolImpl::decodeHeader(Buffer::Instance& buffer, MessageMetadataSharedPtr metadata) {
  if (!metadata) {
    throw EnvoyException("invalid metadata parameter");
  }

  if (buffer.length() < DubboProtocolImpl::MessageSize) {
    return std::pair<ContextSharedPtr, bool>(nullptr, false);
  }

  // 这里的这个 buffer 就是 原生 envoy 里面的一个类,
  // peekBEInt 方法: 从 buffer 中复制出一个大端整数, 这里复制了 uint16, 那就是复制除了 dubbo 的 魔数, 先判断是不是 dubbo 协议
  uint16_t magic_number = buffer.peekBEInt<uint16_t>();
  if (magic_number != MagicNumber) {
    throw EnvoyException(absl::StrCat("invalid dubbo message magic number ", magic_number));
  }

  // 再复制出 FlagOffset, 这里面包含了好几个指标, 可以参考最上面的网页链接。
  // 通过 FlagOffset 判断 request 还是 response
  uint8_t flag = buffer.peekInt<uint8_t>(FlagOffset);
  // 这里直接将 flag 存起来, 后面序列化的时候直接使用。
  metadata->setFlag(flag);
  MessageType type =
      (flag & MessageTypeMask) == MessageTypeMask ? MessageType::Request : MessageType::Response;
  bool is_event = (flag & EventMask) == EventMask ? true : false;
  int64_t request_id = buffer.peekBEInt<int64_t>(RequestIDOffset);
  int32_t body_size = buffer.peekBEInt<int32_t>(BodySizeOffset);

  // The body size of the heartbeat message is zero.
  if (body_size > MaxBodySize || body_size < 0) {
    throw EnvoyException(absl::StrCat("invalid dubbo message size ", body_size));
  }

  // requestId 存入 metadata, 但这里和我们的 traceId 不是一回事
  // 我们用的 traceId 是放在 attachment 里面的 key: sw8
  metadata->setRequestId(request_id);

  if (type == MessageType::Request) {
    if (is_event) {
      type = MessageType::HeartbeatRequest;
    }
    metadata->setMessageType(type);
    parseRequestInfoFromBuffer(buffer, metadata);
  } else {
    if (is_event) {
      type = MessageType::HeartbeatResponse;
    }
    metadata->setMessageType(type);
    parseResponseInfoFromBuffer(buffer, metadata);
  }

  auto context = std::make_shared<ContextImpl>();
  context->setHeaderSize(DubboProtocolImpl::MessageSize);
  context->setBodySize(body_size);
  context->setHeartbeat(is_event);

  return std::pair<ContextSharedPtr, bool>(context, true);
}

bool DubboProtocolImpl::decodeData(Buffer::Instance& buffer, ContextSharedPtr context,
                                   MessageMetadataSharedPtr metadata) {
  // 这里 serializer_ 就是一个序列化类, 可以在 protocol.h 中看到其初始化
  ASSERT(serializer_);

  // 看来 buffer 在到达这一步的时候, header 部分已经被移除了, 不然这里 buffer.length() 应该是 header + body
  if ((buffer.length()) < context->bodySize()) {
    return false;
  }

  switch (metadata->messageType()) {
  case MessageType::Oneway:
  case MessageType::Request: {
    auto ret = serializer_->deserializeRpcInvocation(buffer, context);
    if (!ret.second) {
      return false;
    }
    metadata->setInvocationInfo(ret.first);
    break;
  }
  case MessageType::Response: {
    // Non `Ok` response body has no response type info and skip deserialization.
    if (metadata->responseStatus() != ResponseStatus::Ok) {
      metadata->setMessageType(MessageType::Exception);
      break;
    }
    auto ret = serializer_->deserializeRpcResult(buffer, context);
    if (!ret.second) {
      return false;
    }
    metadata->setRpcResultInfo(ret.first);
    if (ret.first->hasException()) {
      metadata->setMessageType(MessageType::Exception);
    }
    break;
  }
  default:
    PANIC("not handled");
  }

  return true;
}

void DubboProtocolImpl::rspheaderMutation(Buffer::Instance& buffer, const MessageMetadata& metadata,
                                          const Context& ctx) {
  if (metadata.hasRpcResultInfo()) {
    auto* result =
        const_cast<RpcResultImpl*>(dynamic_cast<const RpcResultImpl*>(&metadata.rpcResultInfo()));
    if (result->attachment_ != nullptr) {
      Buffer::OwnedImpl origin_buffer;
      origin_buffer.move(buffer, buffer.length());

      constexpr size_t body_length_size = sizeof(uint32_t);

      const size_t attachment_offset = result->attachment_->attachmentOffset();
      const size_t request_header_size = ctx.headerSize();
      ASSERT(attachment_offset <= origin_buffer.length());

      // Move the other parts of the request headers except the body size to the upstream request
      // buffer.
      buffer.move(origin_buffer, request_header_size - body_length_size);
      // Discard the old body size.
      origin_buffer.drain(body_length_size);

      // Re-serialize the updated attachment.
      Buffer::OwnedImpl attachment_buffer;
      Hessian2::Encoder encoder(std::make_unique<BufferWriter>(attachment_buffer));
      encoder.encode(result->attachment_->attachment());

      size_t new_body_size = attachment_offset - request_header_size + attachment_buffer.length();

      buffer.writeBEInt<uint32_t>(new_body_size);
      buffer.move(origin_buffer, attachment_offset - request_header_size);
      buffer.move(attachment_buffer);

      origin_buffer.drain(origin_buffer.length());
    }
  }
}

bool DubboProtocolImpl::encodeRequest(MetaProtocolProxy::Metadata& metadata, Context& context, const MessageMetadata& msgMetadata) {

  ASSERT(serializer_);

  switch (msgMetadata.messageType()) {

  case MessageType::Request: {
    // 如果没有修改过, 则不做处理。
    if (metadata.getString("tcloud_changed").empty()) {
      // 按原来的来
      metadata.setBodySize(context.bodySize());
      metadata.originMessage().move(context.originMessage());
      return true;
    } else {
      Buffer::OwnedImpl body_buffer;
      size_t serialized_body_size = serializer_->serializeRpcInvocation(body_buffer, msgMetadata, context);

      Buffer::OwnedImpl originMessage;
      originMessage.writeBEInt<uint16_t>(MagicNumber);
      originMessage.writeByte(msgMetadata.flag());
      originMessage.writeByte(static_cast<uint8_t>(metadata.getResponseStatus()));
      originMessage.writeBEInt<uint64_t>(msgMetadata.requestId());
      originMessage.writeBEInt<uint32_t>(serialized_body_size);
      originMessage.move(body_buffer, serialized_body_size);

      // 将 originMessage 设置为 originMessage
      metadata.originMessage().move(originMessage);
      // 设置 bodySize
      metadata.setBodySize(serialized_body_size);
      return true;
    }
  }

  default: {
    metadata.setBodySize(context.bodySize());
    metadata.originMessage().move(context.originMessage());
    return true;
  }
  }
}

bool DubboProtocolImpl::encode(Buffer::Instance& buffer, const MessageMetadata& metadata,
                               const Context& ctx, const std::string& content,
                               RpcResponseType /*type*/) {
  ASSERT(serializer_);

  switch (metadata.messageType()) {
  case MessageType::HeartbeatResponse: {
    ASSERT(metadata.hasResponseStatus());
    ASSERT(content.empty());
    buffer.drain(buffer.length());
    buffer.writeBEInt<uint16_t>(MagicNumber);
    uint8_t flag = static_cast<uint8_t>(metadata.serializationType());
    flag = flag ^ EventMask;
    buffer.writeByte(flag);
    buffer.writeByte(static_cast<uint8_t>(metadata.responseStatus()));
    buffer.writeBEInt<uint64_t>(metadata.requestId());
    // Body of heart beat response is null.
    // TODO(wbpcode): Currently we only support the Hessian2 serialization scheme, so here we
    // directly use the 'N' for null object in Hessian2. This coupling should be unnecessary.
    buffer.writeBEInt<uint32_t>(1u);
    buffer.writeByte('N');
    return true;
  }
  case MessageType::Response: {
    ASSERT(metadata.hasResponseStatus());
    if (content == "addheader") {
      // only when server sidecar response,we need add header
      // client sidecar response not need
      rspheaderMutation(buffer, metadata, ctx);
    }
    return true;
  }
  case MessageType::Request: {
    headerMutation(buffer, metadata, ctx);
    return true;
  }
  case MessageType::Oneway:
  case MessageType::Exception:
    PANIC("not implemented");
  default:
    PANIC("not implemented");
  }
}

void DubboProtocolImpl::headerMutation(Buffer::Instance& buffer, const MessageMetadata& metadata,
                                       const Context& ctx) {
  if (metadata.hasInvocationInfo()) {
    auto* invo = const_cast<RpcInvocationImpl*>(
        dynamic_cast<const RpcInvocationImpl*>(&metadata.invocationInfo()));
    if (invo->hasAttachment() && invo->attachment().attachmentUpdated()) {
      Buffer::OwnedImpl origin_buffer;
      origin_buffer.move(buffer, buffer.length());

      constexpr size_t body_length_size = sizeof(uint32_t);

      const size_t attachment_offset = invo->attachment().attachmentOffset();
      const size_t request_header_size = ctx.headerSize();
      ASSERT(attachment_offset <= origin_buffer.length());

      // Move the other parts of the request headers except the body size to the upstream request
      // buffer.
      buffer.move(origin_buffer, request_header_size - body_length_size);
      // Discard the old body size.
      origin_buffer.drain(body_length_size);

      // Re-serialize the updated attachment.
      Buffer::OwnedImpl attachment_buffer;
      Hessian2::Encoder encoder(std::make_unique<BufferWriter>(attachment_buffer));
      encoder.encode(invo->attachment().attachment());

      size_t new_body_size = attachment_offset - request_header_size + attachment_buffer.length();

      buffer.writeBEInt<uint32_t>(new_body_size);
      buffer.move(origin_buffer, attachment_offset - request_header_size);
      buffer.move(attachment_buffer);

      origin_buffer.drain(origin_buffer.length());
    }
  }
}

class DubboProtocolConfigFactory : public ProtocolFactoryBase<DubboProtocolImpl> {
public:
  DubboProtocolConfigFactory() : ProtocolFactoryBase(ProtocolType::Dubbo) {}
};

/**
 * Static registration for the Dubbo protocol. @see RegisterFactory.
 */
REGISTER_FACTORY(DubboProtocolConfigFactory, NamedProtocolConfigFactory);
} // namespace Dubbo
} // namespace MetaProtocolProxy
} // namespace NetworkFilters
} // namespace Extensions
} // namespace Envoy
