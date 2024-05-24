#pragma once

#include <any>
#include <memory>
#include <string>

#include "envoy/buffer/buffer.h"
#include "envoy/http/header_map.h"
#include "envoy/common/optref.h"
#include "envoy/common/pure.h"
#include "source/common/buffer/buffer_impl.h"
#include "source/common/http/header_map_impl.h"

#include "src/meta_protocol_proxy/codec/codec.h"

namespace Envoy {
namespace Extensions {
namespace NetworkFilters {
namespace MetaProtocolProxy {

class MetadataImpl : public Metadata {
public:
  MetadataImpl() {
    headers_ = Http::RequestHeaderMapImpl::create();
    response_headers_ = Http::ResponseHeaderMapImpl::create();
  };
  ~MetadataImpl() = default;

  void put(std::string key, std::any value) override;
  AnyOptConstRef get(std::string key) const override;
  void putString(std::string key, std::string value) override;
  std::string getString(std::string key) const override;
  bool getBool(std::string key) const override;
  uint32_t getUint32(std::string key) const override;

  Buffer::Instance& originMessage() override { return origin_message_; };
//=======
//private:
//  std::unique_ptr<std::map<std::string, std::any>> map_;
//};
//
//class MetadataImpl : public Metadata {
//public:
//  MetadataImpl() { headers_ = Http::RequestHeaderMapImpl::create(); };
//  ~MetadataImpl() = default;
//
//  void put(std::string key, std::any value) override { properties_.put(key, value); };
//  AnyOptConstRef get(std::string key) const override { return properties_.get(key); };
//  void putString(std::string key, std::string value) override {
//    this->put(key, value);
//    auto lowcase_key = Http::LowerCaseString(key);
//    headers_->remove(lowcase_key);
//    headers_->addCopy(lowcase_key, value);
//  };
//  std::string getString(std::string key) const override { return properties_.getString(key); };
//
  // 从 headers_ 中读取数据
  std::string getFromHeaderMap(const std::string& key) const override {
    // 这里可能会有问题, 因为这一段是抄过来的
    const auto headerValue = headers_->get(Http::LowerCaseString(key));
    if (headerValue.empty()) {
      return "";
    }
    std::string value = std::string(headerValue[0]->value().getStringView());
    return value;
  }
//  bool getBool(std::string key) const override { return properties_.getBool(key); };
//  uint32_t getUint32(std::string key) const override { return properties_.getUint32(key); };
//
//  void setOriginMessage(Buffer::Instance& originMessage) override {
//    origin_message_ = originMessage;
//  };
//  Buffer::Instance& getOriginMessage() override { return origin_message_; };
//>>>>>>> 8710d91 (dubbo 泳道功能)
  void setMessageType(MessageType messageType) override { message_type_ = messageType; };
  MessageType getMessageType() const override { return message_type_; };
  void setResponseStatus(ResponseStatus responseStatus) override {
    response_status_ = responseStatus;
  };
  ResponseStatus getResponseStatus() const override { return response_status_; };
  void setRequestId(uint64_t requestId) override { request_id_ = requestId; };
  uint64_t getRequestId() const override { return request_id_; };
  void setStreamId(uint64_t streamId) override { stream_id_ = streamId; };
  uint64_t getStreamId() const override { return stream_id_; };
  size_t getMessageSize() const override { return header_size_ + body_size_; }
  void setHeaderSize(size_t headerSize) override { header_size_ = headerSize; };
  size_t getHeaderSize() const override { return header_size_; };
  void setBodySize(size_t bodySize) override { body_size_ = bodySize; };
  size_t getBodySize() const override { return body_size_; };
  void setOperationName(std::string operation_name) override { operation_name_ = operation_name; };
  std::string getOperationName() const override { return operation_name_; };
  void setStreamInfo(std::shared_ptr<StreamInfo::StreamInfo> stream_info) {
    stream_info_ = stream_info;
  };
  StreamInfo::StreamInfo& streamInfo() const override { return *stream_info_; };
  MetadataSharedPtr clone() const override;
  Http::RequestHeaderMap& getHeaders() const { return *headers_; }
  Http::ResponseHeaderMap& getResponseHeaders() const { return *response_headers_; }

  // Tracing::TraceContext
  absl::string_view protocol() const override { return "meta-protocol"; };
  absl::string_view authority() const override { return operation_name_; };
  absl::string_view path() const override { return ""; };   // not applicable for MetaProtocol
  absl::string_view method() const override { return ""; }; // not applicable for MetaProtocol
  void forEach(Envoy::Tracing::TraceContext::IterateCallback) const override;
  absl::optional<absl::string_view> getByKey(absl::string_view) const override;
  void setByKey(absl::string_view key, absl::string_view val) override;
  void setByReferenceKey(absl::string_view key, absl::string_view val) override;
  void setByReference(absl::string_view key, absl::string_view val) override;
  void removeByKey(absl::string_view key) override;

private:
  const std::string* getStringPointer(std::string key) const;
  std::map<std::string, std::any> properties_;
  Buffer::OwnedImpl origin_message_;
  MessageType message_type_{MessageType::Request};
  ResponseStatus response_status_{ResponseStatus::Ok};
  uint64_t request_id_{0};
  uint64_t stream_id_{0};
  size_t header_size_{0};
  size_t body_size_{0};
  std::string operation_name_;
  std::shared_ptr<StreamInfo::StreamInfo> stream_info_;
  // Reuse the HeaderMatcher API and related tools provided by Envoy to match the route
  std::unique_ptr<Http::RequestHeaderMap> headers_;
  std::unique_ptr<Http::ResponseHeaderMap> response_headers_;
};

} // namespace MetaProtocolProxy
} // namespace NetworkFilters
} // namespace Extensions
} // namespace Envoy
