#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include <grpcpp/grpcpp.h>

#include <tp_monitor.grpc.pb.h>

namespace limestone::grpc::client {

class tp_monitor_client {
public:
    struct create_result {
        bool ok{};
        std::uint64_t tpm_id{};
        std::string message{};
    };

    struct result {
        bool ok{};
        std::string message{};
    };

    explicit tp_monitor_client(std::shared_ptr<::grpc::Channel> channel);
    ~tp_monitor_client() = default;

    tp_monitor_client(const tp_monitor_client&) = delete;
    tp_monitor_client& operator=(const tp_monitor_client&) = delete;
    tp_monitor_client(tp_monitor_client&&) = delete;
    tp_monitor_client& operator=(tp_monitor_client&&) = delete;

    create_result create(std::uint32_t participant_count);
    create_result create_and_join(std::uint32_t participant_count,
                                  const std::vector<std::string>& ts_ids);
    result join(std::uint64_t tpm_id, std::string_view ts_id);
    result destroy(std::uint64_t tpm_id);
    result barrier_notify(std::uint64_t tpm_id, std::string_view ts_id);

private:
    std::unique_ptr<limestone::tpmonitor::TpMonitorService::Stub> stub_{};
};

} // namespace limestone::grpc::client
