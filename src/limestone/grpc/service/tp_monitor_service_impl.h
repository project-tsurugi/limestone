#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <string_view>

#include <grpcpp/grpcpp.h>
#include <tp_monitor.grpc.pb.h>

namespace limestone::grpc::service {

using disttx::grpc::proto::TpMonitorService;
using disttx::grpc::proto::CreateRequest;
using disttx::grpc::proto::CreateResponse;
using disttx::grpc::proto::JoinRequest;
using disttx::grpc::proto::JoinResponse;
using disttx::grpc::proto::DestroyRequest;
using disttx::grpc::proto::DestroyResponse;
using disttx::grpc::proto::BarrierRequest;
using disttx::grpc::proto::BarrierResponse;

class tp_monitor_service_impl : public TpMonitorService::Service {
public:
    tp_monitor_service_impl();
    ~tp_monitor_service_impl() override;

    tp_monitor_service_impl(const tp_monitor_service_impl&) = delete;
    tp_monitor_service_impl& operator=(const tp_monitor_service_impl&) = delete;
    tp_monitor_service_impl(tp_monitor_service_impl&&) = delete;
    tp_monitor_service_impl& operator=(tp_monitor_service_impl&&) = delete;

    ::grpc::Status Create(::grpc::ServerContext* context,
                          const CreateRequest* request,
                          CreateResponse* response) override;

    ::grpc::Status Join(::grpc::ServerContext* context,
                        const JoinRequest* request,
                        JoinResponse* response) override;

    ::grpc::Status Destroy(::grpc::ServerContext* context,
                           const DestroyRequest* request,
                           DestroyResponse* response) override;

    ::grpc::Status Barrier(::grpc::ServerContext* context,
                           const BarrierRequest* request,
                           BarrierResponse* response) override;

private:
    struct create_result {
        bool ok{};
        std::uint64_t tpm_id{};
    };

    struct result {
        bool ok{};
    };

    struct monitor_state {
        std::uint64_t tpm_id{};
        std::uint32_t participant_count{};
        std::map<std::string, std::uint64_t> participants{};
        std::set<std::string> arrived{};
        std::mutex mtx{};
        std::condition_variable cv{};
        bool destroyed{};
    };

    create_result create_monitor(std::uint32_t participant_count);

protected:
    result join_monitor(std::uint64_t tpm_id, std::string_view tx_id, std::uint64_t ts_id);
    result barrier_notify_monitor(std::uint64_t tpm_id, std::string_view tx_id);
    result destroy_monitor(std::uint64_t tpm_id);

private:
    std::shared_ptr<monitor_state> find_state(std::uint64_t tpm_id);

    std::atomic_uint64_t next_tpm_id_{1};
    std::mutex mtx_{};
    std::map<std::uint64_t, std::shared_ptr<monitor_state>> monitors_{};
};

} // namespace limestone::grpc::service
