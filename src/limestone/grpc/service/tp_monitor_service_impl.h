#pragma once

#include <grpcpp/grpcpp.h>

#include <tp_monitor.grpc.pb.h>
#include <grpc/backend/tp_monitor_backend.h>

namespace limestone::grpc::service {

using limestone::grpc::backend::tp_monitor_backend;
using disttx::grpc::proto::TpMonitorService;
using disttx::grpc::proto::CreateRequest;
using disttx::grpc::proto::CreateResponse;
using disttx::grpc::proto::JoinRequest;
using disttx::grpc::proto::JoinResponse;
using disttx::grpc::proto::CreateAndJoinRequest;
using disttx::grpc::proto::CreateAndJoinResponse;
using disttx::grpc::proto::DestroyRequest;
using disttx::grpc::proto::DestroyResponse;
using disttx::grpc::proto::BarrierRequest;
using disttx::grpc::proto::BarrierResponse;

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

    ::grpc::Status Barrier(::grpc::ServerContext* context,
                           const BarrierRequest* request,
                           BarrierResponse* response) override;

private:
    tp_monitor_backend& backend_;
};

} // namespace limestone::grpc::service
