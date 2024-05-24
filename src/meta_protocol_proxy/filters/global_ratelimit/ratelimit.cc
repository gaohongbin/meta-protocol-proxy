#include "src/meta_protocol_proxy/filters/global_ratelimit/ratelimit.h"
#include "src/meta_protocol_proxy/app_exception.h"
#include "source/common/buffer/buffer_impl.h"
#include "src/meta_protocol_proxy/codec_impl.h"

namespace Envoy {
namespace Extensions {
namespace NetworkFilters {
namespace MetaProtocolProxy {
namespace RateLimit {

void RateLimit::onDestroy() { cleanup(); }

void RateLimit::setDecoderFilterCallbacks(DecoderFilterCallbacks& callbacks) {
  callbacks_ = &callbacks;
}

FilterStatus RateLimit::onMessageDecoded(MetadataSharedPtr metadata, MutationSharedPtr) {
  auto name = config_.rate_limit_service().grpc_service().envoy_grpc().cluster_name();
  auto cluster = cluster_manager_.getThreadLocalCluster(name);
  if (cluster == nullptr) {
    // cluster not found
    ENVOY_STREAM_LOG(debug,
                     "meta protocol global ratelimit: rate limit server cluster not found '{}'",
                     *callbacks_, name);
    callbacks_->sendLocalReply(
        AppException(Error{
            ErrorType::ClusterNotFound,
            fmt::format("meta protocol global ratelimit: rate limit server cluster not found '{}'",
                        metadata->getRequestId())}),
        false);
    return FilterStatus::AbortIteration;
  }

  auto host = cluster->loadBalancer().chooseHost(this);
  if (!host) {
    // host not found
    ENVOY_STREAM_LOG(
        debug, "meta protocol global ratelimit: rate limit server host not found cluster='{}'",
        *callbacks_, name);
    callbacks_->sendLocalReply(
        AppException(Error{
            ErrorType::ClusterNotFound,
            fmt::format(
                "meta protocol global ratelimit: rate limit server host not found for request '{}'",
                metadata->getRequestId())}),
        false);
    return FilterStatus::AbortIteration;
  }

  if (shouldRateLimit(host->address()->asString(), metadata)) {
    // 限流成功， 直接返回客户端
    ENVOY_STREAM_LOG(debug, "meta protocol global ratelimit:  '{}'", *callbacks_,
                     metadata->getRequestId());
    callbacks_->sendLocalReply(
        AppException(
            Error{ErrorType::OverLimit,
                  fmt::format("meta protocol global ratelimit: request '{}' has been rate limited",
                              metadata->getRequestId())}),
        false);
    return FilterStatus::AbortIteration;
  }

  ENVOY_STREAM_LOG(debug, "meta protocol local ratelimit: onMessageDecoded", *callbacks_);
  return FilterStatus::ContinueIteration;
}

void RateLimit::setEncoderFilterCallbacks(EncoderFilterCallbacks& callbacks) {
  encoder_callbacks_ = &callbacks;
}

FilterStatus RateLimit::onMessageEncoded(MetadataSharedPtr, MutationSharedPtr) {
  return FilterStatus::ContinueIteration;
}

void RateLimit::cleanup() {}

bool RateLimit::shouldRateLimit(const std::string& addr, MetadataSharedPtr metadata) {
  ::grpc::ClientContext ctx;
  ::envoy::service::ratelimit::v3::RateLimitRequest request;
  ::envoy::service::ratelimit::v3::RateLimitResponse response;

  // 匹配match，不匹配则直接放行
  if (config_headers_.empty()) {
    return false;
  }
  const MetadataImpl* metadataImpl = static_cast<const MetadataImpl*>(&(*metadata));
  const auto& headers = metadataImpl->getHeaders();
  if (!Http::HeaderUtility::matchHeaders(headers, config_headers_)) {
    return false;
  }

  // 匹配上match之后，构造请求参数，发请求
  request.set_domain(config_.domain());
  // request.set_hits_addend(1);

  // 发请求
  for (auto descriptor : config_.descriptors()) {
    std::string key = descriptor.property();
    std::string value = metadata->getString(key);

    auto desc = request.add_descriptors();
    auto entry = desc->add_entries();
    entry->set_key(descriptor.descriptor_key());
    entry->set_value(value);
  }

  auto stub = envoy::service::ratelimit::v3::RateLimitService::NewStub(
      grpc::CreateChannel(addr, grpc::InsecureChannelCredentials()));
  auto st = stub->ShouldRateLimit(&ctx, request, &response);

  // 请求 ratelimit 失败处理
  if (!st.ok() && config_.failure_mode_deny()) {
    ENVOY_STREAM_LOG(debug, "meta protocol global ratelimit: failed to call reatlimit service",
                     *callbacks_);
    return true;
  }

  if (envoy::service::ratelimit::v3::RateLimitResponse_Code_OVER_LIMIT == response.overall_code()) {
    return true;
  }

  return false;
}

} // namespace RateLimit
} // namespace  MetaProtocolProxy
} // namespace NetworkFilters
} // namespace Extensions
} // namespace Envoy
