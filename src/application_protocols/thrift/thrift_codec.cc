#include <any>

#include "envoy/buffer/buffer.h"

#include "source/common/common/logger.h"
#include "source/common/buffer/buffer_impl.h"

#include "src/meta_protocol_proxy/codec/codec.h"
#include "src/application_protocols/thrift/thrift_codec.h"
#include "src/application_protocols/thrift/metadata.h"

namespace Envoy {
namespace Extensions {
namespace NetworkFilters {
namespace MetaProtocolProxy {
namespace Thrift {

MetaProtocolProxy::DecodeStatus ThriftCodec::decode(Buffer::Instance& data,
                                                    MetaProtocolProxy::Metadata& metadata) {

  ENVOY_LOG(debug, "thrift: {} bytes available", data.length());

  if (frame_ended_) {
    // Continuation after filter stopped iteration on transportComplete callback.
    complete();
    return MetaProtocolProxy::DecodeStatus::Done;
  }

  if (!frame_started_) {
    // Look for start of next frame.
    if (!metadata_) {
      metadata_ = std::make_shared<ThriftProxy::MessageMetadata>();
    }

    // 获取 frame size
    if (!transport_->decodeFrameStart(data, *metadata_)) {
      ENVOY_LOG(debug, "thrift: need more data for {} transport start", transport_->name());
      return MetaProtocolProxy::DecodeStatus::WaitForData;
    }
    ENVOY_LOG(debug, "thrift: {} transport started", transport_->name());

    if (metadata_->hasProtocol()) {
      if (protocol_->type() == ThriftProxy::ProtocolType::Auto) {
        protocol_->setType(metadata_->protocol());
        ENVOY_LOG(debug, "thrift: {} transport forced {} protocol", transport_->name(),
                  protocol_->name());
      } else if (metadata_->protocol() != protocol_->type()) {
        throw EnvoyException(
            fmt::format("transport reports protocol {}, but configured for {}",
                        ThriftProxy::ProtocolNames::get().fromType(metadata_->protocol()),
                        ThriftProxy::ProtocolNames::get().fromType(protocol_->type())));
      }
    }
    if (metadata_->hasAppException()) {
      ThriftProxy::AppExceptionType ex_type = metadata_->appExceptionType();
      std::string ex_msg = metadata_->appExceptionMessage();
      // Force new metadata if we get called again.
      metadata_.reset();
      throw EnvoyException(fmt::format("thrift AppException: type: {}, message: {}",
			                                             static_cast<int>(ex_type), ex_msg));
    }

    frame_started_ = true;
    // 初始化状态机
    state_machine_ = std::make_unique<DecoderStateMachine>(*protocol_, *metadata_);
  }

  ASSERT(state_machine_ != nullptr);

  ENVOY_LOG(debug, "thrift: protocol {}, state {}, {} bytes available", protocol_->name(),
            ProtocolStateNameValues::name(state_machine_->currentState()), data.length());

  // 状态机来解码
  ProtocolState rv = state_machine_->run(data);
  if (rv == ProtocolState::WaitForData) {
    ENVOY_LOG(debug, "thrift: wait for data");
    return DecodeStatus::WaitForData;
  }

  ASSERT(rv == ProtocolState::Done);

  // Message complete, decode end of frame.
  if (!transport_->decodeFrameEnd(data)) {
    ENVOY_LOG(debug, "thrift: need more data for {} transport end", transport_->name());
    return DecodeStatus::WaitForData;
  }

  // 将 metadata_ 的信息存储到 metadata 中。
  toMetadata(*metadata_, metadata);
  ENVOY_LOG(debug, "request sequenceId={}, thrift: origin message length {} ", metadata.getRequestId(), metadata.originMessage().length());

  frame_ended_ = true;
  metadata_.reset();

  ENVOY_LOG(debug, "request sequenceId={}, thrift: {} transport ended", metadata.getRequestId(), transport_->name());

  // Reset for next frame.
  complete();
  return DecodeStatus::Done;
}

void ThriftCodec::complete() {
  state_machine_ = nullptr;
  frame_started_ = false;
  frame_ended_ = false;
}

void ThriftCodec::encode(const MetaProtocolProxy::Metadata& metadata,
                         const MetaProtocolProxy::Mutation& mutation, Buffer::Instance& buffer) {
  (void)buffer;
  for (const auto& keyValue : mutation) {
    ENVOY_LOG(debug, "thrift: codec mutation {} : {}", keyValue.first, keyValue.second);
  }
  ENVOY_LOG(debug, "thrift: codec server real address: {} ",
            metadata.getString(ReservedHeaders::RealServerAddress));
  // ASSERT(buffer.length() == 0);
  switch (metadata.getMessageType()) {
  case MetaProtocolProxy::MessageType::Heartbeat: {
    break;
  }
  case MetaProtocolProxy::MessageType::Request: {
    // TODO
    break;
  }
  case MetaProtocolProxy::MessageType::Response: {
    // TODO
    break;
  }
  case MetaProtocolProxy::MessageType::Error: {
    // TODO
    break;
  }
  default:
    PANIC("not reachec");
  }
}

static const std::string TApplicationException = "TApplicationException";
static const std::string MessageField = "message";
static const std::string TypeField = "type";
static const std::string StopField = "";

void ThriftCodec::onError(const MetaProtocolProxy::Metadata& metadata,
                          const MetaProtocolProxy::Error& error, Buffer::Instance& buffer) {
  ASSERT(buffer.length() == 0);

  ThriftProxy::MessageMetadata msgMetadata;
  toMsgMetadata(metadata, msgMetadata);

  // Handle cases where the exception occurs before the message name (e.g. some header transport
  // errors).
  if (!msgMetadata.hasMethodName()) {
    msgMetadata.setMethodName("");
  }
  if (!msgMetadata.hasSequenceId()) {
    msgMetadata.setSequenceId(0);
  }

  msgMetadata.setMessageType(ThriftProxy::MessageType::Exception);

  // 声明新变量
  Buffer::OwnedImpl response_buffer;

  protocol_->writeMessageBegin(response_buffer, msgMetadata);
  protocol_->writeStructBegin(response_buffer, TApplicationException);

  protocol_->writeFieldBegin(response_buffer, MessageField, ThriftProxy::FieldType::String, 1);
  protocol_->writeString(response_buffer, error.message);
  protocol_->writeFieldEnd(response_buffer);

  protocol_->writeFieldBegin(response_buffer, TypeField, ThriftProxy::FieldType::I32, 2);
  protocol_->writeInt32(response_buffer, static_cast<int32_t>(ThriftProxy::AppExceptionType::InternalError));
  protocol_->writeFieldEnd(response_buffer);

  protocol_->writeFieldBegin(response_buffer, StopField, ThriftProxy::FieldType::Stop, 0);

  protocol_->writeStructEnd(response_buffer);
  protocol_->writeMessageEnd(response_buffer);

  // 写入 frame
  transport_->encodeFrame(buffer, msgMetadata, response_buffer);
}

void ThriftCodec::toMetadata(const ThriftProxy::MessageMetadata& msgMetadata, Metadata& metadata) {
  // 存入 method
  if (msgMetadata.hasMethodName()) {
    metadata.putString("method", msgMetadata.methodName());
  }

  // 存入 RequestId
  if (msgMetadata.hasSequenceId()) {
    metadata.setRequestId(msgMetadata.sequenceId());
  }

  // 存入 tcloudTraceId
  if (msgMetadata.hasTCloudTraceId()) {
    metadata.putString("tcloudTraceId", msgMetadata.tcloudTraceId());
  }

  // 存入 MessageType
  ASSERT(msgMetadata.hasMessageType());
  switch (msgMetadata.messageType()) {
  case ThriftProxy::MessageType::Call: {
    metadata.setMessageType(MessageType::Request);
    break;
  }
  case ThriftProxy::MessageType::Reply: {
    metadata.setMessageType(MessageType::Response);
    break;
  }
  case ThriftProxy::MessageType::Oneway: {
    metadata.setMessageType(MessageType::Oneway);
    break;
  }
  case ThriftProxy::MessageType::Exception: {
    metadata.setMessageType(MessageType::Error);
    break;
  }
  default:
    PANIC("not reachec");
  }

  // 将第三个参数的内容存入到第一个参数中，并且在第一个参数中，新增 size 字段。
  transport_->encodeFrame(metadata.originMessage(), msgMetadata, state_machine_->originalMessage());
}

void ThriftCodec::toMsgMetadata(const Metadata& metadata,
                                ThriftProxy::MessageMetadata& msgMetadata) {
  auto method = metadata.getString("method");
  // TODO we should use a more appropriate method to tell if metadata contains a specific key
  if (method != "") {
    msgMetadata.setMethodName(method);
  }
  msgMetadata.setSequenceId(metadata.getRequestId());
}

// PassthroughData -> PassthroughData
// PassthroughData -> MessageEnd (all body bytes received)
ProtocolState DecoderStateMachine::passthroughData(Buffer::Instance& buffer) {
  if (body_bytes_ > buffer.length()) {
    return ProtocolState::WaitForData;
  }

  origin_message_.move(buffer, body_bytes_);
  return ProtocolState::MessageEnd;
}

// MessageBegin -> StructBegin
ProtocolState DecoderStateMachine::messageBegin(Buffer::Instance& buffer) {
  // binary 部分的长度
  const auto total = buffer.length();

  // 获取了 seqId, methodName
  if (!proto_.readMessageBegin(buffer, metadata_)) {
    return ProtocolState::WaitForData;
  }

  stack_.clear();
  // 重新将 MessageEnd 存入
  stack_.emplace_back(Frame(ProtocolState::MessageEnd));

  // 这里目前还没有实现
  if (passthrough_enabled_) {
    body_bytes_ = metadata_.frameSize() - (total - buffer.length());
    return ProtocolState::PassthroughData;
  }

  // 将相应数据写入 origin_message_
  proto_.writeMessageBegin(origin_message_, metadata_);
  return ProtocolState::StructBegin;
}

// MessageEnd -> Done
ProtocolState DecoderStateMachine::messageEnd(Buffer::Instance& buffer) {
  if (!proto_.readMessageEnd(buffer)) {
    return ProtocolState::WaitForData;
  }

  proto_.writeMessageEnd(origin_message_);
  return ProtocolState::Done;
}

// StructBegin -> FieldBegin
// 针对 binary 的 structBegin 其实是空操作
ProtocolState DecoderStateMachine::structBegin(Buffer::Instance& buffer) {
  std::string name;
  if (!proto_.readStructBegin(buffer, name)) {
    return ProtocolState::WaitForData;
  }

  proto_.writeStructBegin(origin_message_, name);
  return ProtocolState::FieldBegin;
}

// StructEnd -> stack's return state
ProtocolState DecoderStateMachine::structEnd(Buffer::Instance& buffer) {
  if (!proto_.readStructEnd(buffer)) {
    return ProtocolState::WaitForData;
  }

  proto_.writeFieldBegin(origin_message_, "", ThriftProxy::FieldType::Stop, 0);
  proto_.writeStructEnd(origin_message_);
  return popReturnState();
}

// FieldBegin -> FieldValue, or
// FieldBegin -> StructEnd (stop field)
ProtocolState DecoderStateMachine::fieldBegin(Buffer::Instance& buffer) {
  std::string name;
  ThriftProxy::FieldType field_type;
  int16_t field_id;

  // 获取 field 对应的 type 和 id
  if (!proto_.readFieldBegin(buffer, name, field_type, field_id)) {
    return ProtocolState::WaitForData;
  }

  if (field_type == ThriftProxy::FieldType::Stop) {
    return ProtocolState::StructEnd;
  }

  // 栈中压入 FieldEnd 以及 field_type, 这里我们新增一个 field_id
  stack_.emplace_back(Frame(ProtocolState::FieldEnd, field_type, field_id));

  // 将 field 的 type 和 id 写入 origin_message_
  proto_.writeFieldBegin(origin_message_, name, field_type, field_id);
  // 下一步获取对应的 field 的值
  return ProtocolState::FieldValue;
}

// FieldValue -> FieldEnd (via stack return state)
ProtocolState DecoderStateMachine::fieldValue(Buffer::Instance& buffer) {
  ASSERT(!stack_.empty());

  // 可以看到在 fieldBegin 中压入了如下信息
  // stack_.emplace_back(Frame(ProtocolState::FieldEnd, field_type));
  Frame& frame = stack_.back();
  // 这里新增 field_id 传递
  return handleValue(buffer, frame.elem_type_, frame.return_state_, frame.field_id_);
}

// FieldEnd -> FieldBegin
ProtocolState DecoderStateMachine::fieldEnd(Buffer::Instance& buffer) {
  if (!proto_.readFieldEnd(buffer)) {
    return ProtocolState::WaitForData;
  }

  popReturnState();

  proto_.writeFieldEnd(origin_message_);
  return ProtocolState::FieldBegin;
}

// ListBegin -> ListValue
ProtocolState DecoderStateMachine::listBegin(Buffer::Instance& buffer) {
  ThriftProxy::FieldType elem_type;
  uint32_t size;
  if (!proto_.readListBegin(buffer, elem_type, size)) {
    return ProtocolState::WaitForData;
  }

  stack_.emplace_back(Frame(ProtocolState::ListEnd, elem_type, size));

  proto_.writeListBegin(origin_message_, elem_type, size);
  return ProtocolState::ListValue;
}

// ListValue -> ListValue, ListBegin, MapBegin, SetBegin, StructBegin (depending on value type),
// or ListValue -> ListEnd
ProtocolState DecoderStateMachine::listValue(Buffer::Instance& buffer) {
  ASSERT(!stack_.empty());
  const uint32_t index = stack_.size() - 1;
  if (stack_[index].remaining_ == 0) {
    return popReturnState();
  }
  ProtocolState nextState = handleValue(buffer, stack_[index].elem_type_, ProtocolState::ListValue);
  if (nextState != ProtocolState::WaitForData) {
    stack_[index].remaining_--;
  }

  return nextState;
}

// ListEnd -> stack's return state
ProtocolState DecoderStateMachine::listEnd(Buffer::Instance& buffer) {
  if (!proto_.readListEnd(buffer)) {
    return ProtocolState::WaitForData;
  }

  proto_.writeListEnd(origin_message_);
  return popReturnState();
}

// MapBegin -> MapKey
ProtocolState DecoderStateMachine::mapBegin(Buffer::Instance& buffer) {
  ThriftProxy::FieldType key_type, value_type;
  uint32_t size;
  // 这里获取了 map 的 keyType 和 valueType 以及 Map 的 size
  if (!proto_.readMapBegin(buffer, key_type, value_type, size)) {
    return ProtocolState::WaitForData;
  }

  Frame& frame = stack_.back();
  // 又有了一个 MapEnd
  stack_.emplace_back(Frame(ProtocolState::MapEnd, key_type, value_type, size, frame.field_id_));

  proto_.writeMapBegin(origin_message_, key_type, value_type, size);
  return ProtocolState::MapKey;
}

// MapKey -> MapValue, ListBegin, MapBegin, SetBegin, StructBegin (depending on key type), or
// MapKey -> MapEnd
ProtocolState DecoderStateMachine::mapKey(Buffer::Instance& buffer) {
  ASSERT(!stack_.empty());
  Frame& frame = stack_.back();
  if (frame.remaining_ == 0) {
    // stack_ 出栈并返回 return_state
    // 在所有 map 元素解析完成以后, 返回 MapEnd
    return popReturnState();
  }

  // 获取 map 的 key, 并返回下一步的状态为 MapValue
  return handleValue(buffer, frame.elem_type_, ProtocolState::MapValue, frame.field_id_);
}

// MapValue -> MapKey, ListBegin, MapBegin, SetBegin, StructBegin (depending on value type), or
// MapValue -> MapKey
ProtocolState DecoderStateMachine::mapValue(Buffer::Instance& buffer) {
  ASSERT(!stack_.empty());
  // 这个 index 不就是获取栈的末尾下标吗
  const uint32_t index = stack_.size() - 1;
  ASSERT(stack_[index].remaining_ != 0);
  ProtocolState nextState = handleValue(buffer, stack_[index].value_type_, ProtocolState::MapKey, stack_[index].field_id_);
  if (nextState != ProtocolState::WaitForData) {
    stack_[index].remaining_--;
  }

  return nextState;
}

// MapEnd -> stack's return state
// 针对 binary 而言, 这两个方法均是空的
ProtocolState DecoderStateMachine::mapEnd(Buffer::Instance& buffer) {
  if (!proto_.readMapEnd(buffer)) {
    return ProtocolState::WaitForData;
  }

  proto_.writeMapEnd(origin_message_);
  return popReturnState();
}

// SetBegin -> SetValue
ProtocolState DecoderStateMachine::setBegin(Buffer::Instance& buffer) {
  ThriftProxy::FieldType elem_type;
  uint32_t size;
  if (!proto_.readSetBegin(buffer, elem_type, size)) {
    return ProtocolState::WaitForData;
  }

  stack_.emplace_back(Frame(ProtocolState::SetEnd, elem_type, size));

  proto_.writeSetBegin(origin_message_, elem_type, size);
  return ProtocolState::SetValue;
}

// SetValue -> SetValue, ListBegin, MapBegin, SetBegin, StructBegin (depending on value type), or
// SetValue -> SetEnd
ProtocolState DecoderStateMachine::setValue(Buffer::Instance& buffer) {
  ASSERT(!stack_.empty());
  const uint32_t index = stack_.size() - 1;
  if (stack_[index].remaining_ == 0) {
    return popReturnState();
  }
  ProtocolState nextState = handleValue(buffer, stack_[index].elem_type_, ProtocolState::SetValue);
  if (nextState != ProtocolState::WaitForData) {
    stack_[index].remaining_--;
  }

  return nextState;
}

// SetEnd -> stack's return state
ProtocolState DecoderStateMachine::setEnd(Buffer::Instance& buffer) {
  if (!proto_.readSetEnd(buffer)) {
    return ProtocolState::WaitForData;
  }

  proto_.writeSetEnd(origin_message_);
  return popReturnState();
}

ProtocolState DecoderStateMachine::handleValue(Buffer::Instance& buffer,
                                               ThriftProxy::FieldType elem_type,
                                               ProtocolState return_state, int16_t field_id) {
  switch (elem_type) {
  // 如果是 bool 类型,  直接读取并修改  origin_message_
  case ThriftProxy::FieldType::Bool: {
    bool value{};
    if (proto_.readBool(buffer, value)) {
      proto_.writeBool(origin_message_, value);
      return return_state;
    }
    break;
  }
  case ThriftProxy::FieldType::Byte: {
    uint8_t value{};
    if (proto_.readByte(buffer, value)) {
      proto_.writeByte(origin_message_, value);
      return return_state;
    }
    break;
  }
  case ThriftProxy::FieldType::I16: {
    int16_t value{};
    if (proto_.readInt16(buffer, value)) {
      proto_.writeInt16(origin_message_, value);
      return return_state;
    }
    break;
  }
  case ThriftProxy::FieldType::I32: {
    int32_t value{};
    if (proto_.readInt32(buffer, value)) {
      proto_.writeInt32(origin_message_, value);
      return return_state;
    }
    break;
  }
  case ThriftProxy::FieldType::I64: {
    int64_t value{};
    if (proto_.readInt64(buffer, value)) {
      proto_.writeInt64(origin_message_, value);
      return return_state;
    }
    break;
  }
  case ThriftProxy::FieldType::Double: {
    double value{};
    if (proto_.readDouble(buffer, value)) {
      proto_.writeDouble(origin_message_, value);
      return return_state;
    }
    break;
  }
  case ThriftProxy::FieldType::String: {
    std::string value;
    if (proto_.readString(buffer, value)) {
      // 这里获取到 trace_context 的 key
      ENVOY_LOG(debug, "解析出来的 string value = {}, 此时 field_id = {}", value, field_id);
      if (field_id == int16_t(11111) && return_state == ProtocolState::MapValue && Http::LowerCaseString(value).get() == "twl-span-context") {
        get_tcloud_trace_context_ = true;
      }
      // 这里判断获取到 trace_context 的 value
      if (get_tcloud_trace_context_ && return_state == ProtocolState::MapKey) {
        tcloud_trace_context_ = value;
        get_tcloud_trace_context_ = false;
      }
      // 继续原逻辑
      proto_.writeString(origin_message_, value);
      return return_state;
    }
    break;
  }
  // 如果是一个 struct
  case ThriftProxy::FieldType::Struct:
    stack_.emplace_back(Frame(return_state));
    return ProtocolState::StructBegin;
  // 如果 field 是一个 Map, 前面我们知道 stack_ 已经插入了一个 fieldEnd 了, 这里又插入了一个 fieldEnd
  case ThriftProxy::FieldType::Map:
    stack_.emplace_back(Frame(return_state, field_id));
    return ProtocolState::MapBegin;
  case ThriftProxy::FieldType::List:
    stack_.emplace_back(Frame(return_state));
    return ProtocolState::ListBegin;
  case ThriftProxy::FieldType::Set:
    stack_.emplace_back(Frame(return_state));
    return ProtocolState::SetBegin;
  default:
    throw EnvoyException(fmt::format("unknown field type {}", static_cast<int8_t>(elem_type)));
  }

  return ProtocolState::WaitForData;
}

// 从 ProtocolState::MessageBegin 开始
ProtocolState DecoderStateMachine::handleState(Buffer::Instance& buffer) {
  switch (state_) {
  case ProtocolState::PassthroughData:
    return passthroughData(buffer);
  case ProtocolState::MessageBegin:
    return messageBegin(buffer);
  case ProtocolState::StructBegin:
    return structBegin(buffer);
  case ProtocolState::StructEnd:
    return structEnd(buffer);
  case ProtocolState::FieldBegin:
    return fieldBegin(buffer);
  case ProtocolState::FieldValue:
    return fieldValue(buffer);
  case ProtocolState::FieldEnd:
    return fieldEnd(buffer);
  case ProtocolState::ListBegin:
    return listBegin(buffer);
  case ProtocolState::ListValue:
    return listValue(buffer);
  case ProtocolState::ListEnd:
    return listEnd(buffer);
  case ProtocolState::MapBegin:
    return mapBegin(buffer);
  case ProtocolState::MapKey:
    return mapKey(buffer);
  case ProtocolState::MapValue:
    return mapValue(buffer);
  case ProtocolState::MapEnd:
    return mapEnd(buffer);
  case ProtocolState::SetBegin:
    return setBegin(buffer);
  case ProtocolState::SetValue:
    return setValue(buffer);
  case ProtocolState::SetEnd:
    return setEnd(buffer);
  case ProtocolState::MessageEnd:
    return messageEnd(buffer);
  default:
    PANIC("not reachec");
  }
}

// 出栈
ProtocolState DecoderStateMachine::popReturnState() {
  ASSERT(!stack_.empty());
  ProtocolState return_state = stack_.back().return_state_;
  stack_.pop_back();
  return return_state;
}

// 这里开始解析 binary 里面的内容
ProtocolState DecoderStateMachine::run(Buffer::Instance& buffer) {
  while (state_ != ProtocolState::Done) {
    ENVOY_LOG(trace, "thrift: state {}, {} bytes available", ProtocolStateNameValues::name(state_),
              buffer.length());
    ENVOY_LOG(trace, "thrift: state {}, {} original message", ProtocolStateNameValues::name(state_),
              origin_message_.length());

    ProtocolState nextState = handleState(buffer);
    if (nextState == ProtocolState::WaitForData) {
      return ProtocolState::WaitForData;
    }

    state_ = nextState;
  }

  // 将 thrift 的 traceId 存到 metadata 中
  if (!tcloud_trace_context_.empty()) {
    std::vector<std::string> traceContextSpilts = absl::StrSplit(tcloud_trace_context_, ':');
    if (traceContextSpilts.size() >= 1) {
      metadata_.setTCloudTraceId(traceContextSpilts[0]);
    }
  }

  return state_;
}

} // namespace Thrift
} // namespace MetaProtocolProxy
} // namespace NetworkFilters
} // namespace Extensions
} // namespace Envoy
