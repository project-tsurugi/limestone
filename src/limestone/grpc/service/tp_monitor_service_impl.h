#pragma once

#include <grpcpp/grpcpp.h>

#include <tp_monitor.grpc.pb.h>
#include <grpc/backend/tp_monitor_backend.h>

namespace limestone::grpc::service {

using limestone::grpc::backend::tp_monitor_backend;
using limestone::tpmonitor::TpMonitorService;
using limestone::tpmonitor::CreateRequest;
using limestone::tpmonitor::CreateResponse;
using limestone::tpmonitor::JoinRequest;
using limestone::tpmonitor::JoinResponse;
using limestone::tpmonitor::CreateAndJoinRequest;
using limestone::tpmonitor::CreateAndJoinResponse;
using limestone::tpmonitor::DestroyRequest;
using limestone::tpmonitor::DestroyResponse;
using limestone::tpmonitor::BarrierNotifyRequest;
using limestone::tpmonitor::BarrierNotifyResponse;

class tp_monitor_service_impl final : public TpMonitorService::Service {
public:
    explicit tp_monitor_service_impl(tp_monitor_backend& backend);
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

    ::grpc::Status CreateAndJoin(::grpc::ServerContext* context,
                                 const CreateAndJoinRequest* request,
                                 CreateAndJoinResponse* response) override;

    ::grpc::Status Destroy(::grpc::ServerContext* context,
                           const DestroyRequest* request,
                           DestroyResponse* response) override;

    ::grpc::Status BarrierNotify(::grpc::ServerContext* context,
                                 const BarrierNotifyRequest* request,
                                 BarrierNotifyResponse* response) override;

private:
    tp_monitor_backend& backend_;
};

} // namespace limestone::grpc::service
