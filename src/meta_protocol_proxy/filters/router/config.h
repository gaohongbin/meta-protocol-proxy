#pragma once

#include "api/meta_protocol_proxy/filters/router/v1alpha/router.pb.h"
#include "api/meta_protocol_proxy/filters/router/v1alpha/router.pb.validate.h"

#include "src/meta_protocol_proxy/filters/factory_base.h"

namespace Envoy {
namespace Extensions {
namespace NetworkFilters {
namespace MetaProtocolProxy {
namespace Router {

class RouterFilterConfig
    : public FactoryBase<aeraki::meta_protocol_proxy::filters::router::v1alpha::Router> {
public:
  RouterFilterConfig() : FactoryBase("aeraki.meta_protocol.filters.router") {}

private:
  FilterFactoryCb createFilterFactoryFromProtoTyped(
      const aeraki::meta_protocol_proxy::filters::router::v1alpha::Router& proto_config,
      const std::string& stat_prefix, Server::Configuration::FactoryContext& context) override;
};

} // namespace Router
} // namespace MetaProtocolProxy
} // namespace NetworkFilters
} // namespace Extensions
} // namespace Envoy
