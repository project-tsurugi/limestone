#include "echo_service_impl.h"
#include <glog/logging.h>

namespace limestone::grpc::service {

::grpc::Status echo_service_impl::Echo(::grpc::ServerContext* /* context */,
                                       const limestone::grpc::proto::EchoRequest* request,
                                       limestone::grpc::proto::EchoResponse* response) {
    // Log the incoming request
    LOG(INFO) << "Echo request received: " << request->message();
    
    // Simply echo back the message
    response->set_message(request->message());
    
    // Log the response
    LOG(INFO) << "Echo response sent: " << response->message();
    
    return ::grpc::Status::OK;
}

} // namespace limestone::grpc::service
