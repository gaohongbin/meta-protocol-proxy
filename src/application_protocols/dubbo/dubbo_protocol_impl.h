#pragma once

#include "src/application_protocols/dubbo/protocol.h"
#include "source/common/common/logger.h"

namespace Envoy {
namespace Extensions {
namespace NetworkFilters {
namespace MetaProtocolProxy {
namespace Dubbo {

class DubboProtocolImpl : public Protocol {
public:
  DubboProtocolImpl() = default;
  ~DubboProtocolImpl() override = default;

  const std::string& name() const override { return ProtocolNames::get().fromType(type()); }
  ProtocolType type() const override { return ProtocolType::Dubbo; }

  std::pair<ContextSharedPtr, bool> decodeHeader(Buffer::Instance& buffer,
                                                 MessageMetadataSharedPtr metadata) override;
  bool decodeData(Buffer::Instance& buffer, ContextSharedPtr context,
                  MessageMetadataSharedPtr metadata) override;

  bool encode(Buffer::Instance& buffer, const MessageMetadata& metadata, const Context& ctx,
              const std::string& content, RpcResponseType type) override;

  bool encodeRequest(MetaProtocolProxy::Metadata& metadata, Context& context, const MessageMetadata& msgMetadata) override;

  static constexpr uint8_t MessageSize = 16;   // dubbo 头最少 16 个字节
  static constexpr int32_t MaxBodySize = 16 * 1024 * 1024;

private:
  void headerMutation(Buffer::Instance& buffer, const MessageMetadata& metadata,
                      const Context& context);
  void rspheaderMutation(Buffer::Instance& buffer, const MessageMetadata& metadata,
                         const Context& context);
};

} // namespace Dubbo
} // namespace MetaProtocolProxy
} // namespace NetworkFilters
} // namespace Extensions
} // namespace Envoy
