#include <wal_sync/remote_exception.h>
#include <grpcpp/grpcpp.h>
#include <ostream>
#include <string_view>

namespace limestone::internal {

std::string_view to_string_view(remote_error_code code) {
    switch (code) {
    case remote_error_code::ok: return "ok";
    case remote_error_code::cancelled: return "cancelled";
    case remote_error_code::unknown: return "unknown";
    case remote_error_code::invalid_argument: return "invalid_argument";
    case remote_error_code::deadline_exceeded: return "deadline_exceeded";
    case remote_error_code::not_found: return "not_found";
    case remote_error_code::already_exists: return "already_exists";
    case remote_error_code::permission_denied: return "permission_denied";
    case remote_error_code::resource_exhausted: return "resource_exhausted";
    case remote_error_code::failed_precondition: return "failed_precondition";
    case remote_error_code::aborted: return "aborted";
    case remote_error_code::out_of_range: return "out_of_range";
    case remote_error_code::unimplemented: return "unimplemented";
    case remote_error_code::internal: return "internal";
    case remote_error_code::unavailable: return "unavailable";
    case remote_error_code::data_loss: return "data_loss";
    case remote_error_code::unauthenticated: return "unauthenticated";
    }
    return "(unknown)";
}

std::ostream& operator<<(std::ostream& os, remote_error_code code) {
    return os << to_string_view(code);
}


remote_exception::remote_exception(::grpc::Status const& status, std::string method)
    : std::runtime_error(status.error_message()),
      code_([&status]() {
          using grpc::StatusCode;
          switch (status.error_code()) {
          case StatusCode::OK: return remote_error_code::ok;
          case StatusCode::CANCELLED: return remote_error_code::cancelled;
          case StatusCode::UNKNOWN: return remote_error_code::unknown;
          case StatusCode::INVALID_ARGUMENT: return remote_error_code::invalid_argument;
          case StatusCode::DEADLINE_EXCEEDED: return remote_error_code::deadline_exceeded;
          case StatusCode::NOT_FOUND: return remote_error_code::not_found;
          case StatusCode::ALREADY_EXISTS: return remote_error_code::already_exists;
          case StatusCode::PERMISSION_DENIED: return remote_error_code::permission_denied;
          case StatusCode::RESOURCE_EXHAUSTED: return remote_error_code::resource_exhausted;
          case StatusCode::FAILED_PRECONDITION: return remote_error_code::failed_precondition;
          case StatusCode::ABORTED: return remote_error_code::aborted;
          case StatusCode::OUT_OF_RANGE: return remote_error_code::out_of_range;
          case StatusCode::UNIMPLEMENTED: return remote_error_code::unimplemented;
          case StatusCode::INTERNAL: return remote_error_code::internal;
          case StatusCode::UNAVAILABLE: return remote_error_code::unavailable;
          case StatusCode::DATA_LOSS: return remote_error_code::data_loss;
          case StatusCode::UNAUTHENTICATED: return remote_error_code::unauthenticated;
          default: return remote_error_code::unknown;
          }
      }()),
      method_(std::move(method))
{
}

} // namespace limestone::internal
