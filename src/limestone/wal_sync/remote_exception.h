#pragma once

#include <stdexcept>
#include <string>
#include <grpcpp/grpcpp.h>

namespace limestone::internal {

/**
 * @brief Error code for remote_exception
 */
enum class remote_error_code {
    ok,
    cancelled,
    unknown,
    invalid_argument,
    deadline_exceeded,
    not_found,
    already_exists,
    permission_denied,
    resource_exhausted,
    failed_precondition,
    aborted,
    out_of_range,
    unimplemented,
    internal,
    unavailable,
    data_loss,
    unauthenticated,
};


/**
 * @brief Returns the string representation of remote_error_code.
 */
std::string_view to_string_view(remote_error_code code);
/**
 * @brief Output operator for remote_error_code (for logging).
 */
std::ostream& operator<<(std::ostream& os, remote_error_code code);

/**
 * @brief Exception for remote (RPC) call failures.
 *
 * This exception is thrown when a remote call (e.g., gRPC) fails.
 * It is designed to be protocol-agnostic and extensible.
 */

class remote_exception : public std::runtime_error {
public:
    /**
     * @brief Construct remote_exception from gRPC Status.
     * @param status gRPC Status object
     * @param method Service and method name in the form "ServiceName/MethodName"
     *
     * Sets fields as follows:
     *   - code_   : mapped from status.error_code() (e.g., DEADLINE_EXCEEDED → timeout, UNAVAILABLE → unavailable, etc.)
     *   - message : set to status.error_message()
     *   - method  : set to the method argument ("ServiceName/MethodName")
     */
    remote_exception(::grpc::Status const& status, std::string const& method = {});

    /** @brief Get error code */
    remote_error_code code() const noexcept { return code_; }
    /** @brief Get method name */
    std::string const& method() const noexcept { return method_; }

private:
    remote_error_code code_;
    std::string method_;
};

} // namespace limestone::internal
