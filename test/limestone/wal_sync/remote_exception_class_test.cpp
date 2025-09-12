#include <array>

#include <gtest/gtest.h>
#include <limestone/wal_sync/remote_exception.h>
#include <grpcpp/grpcpp.h>
#include <string>

namespace limestone::testing {

using limestone::internal::remote_exception;
using limestone::internal::remote_error_code;


namespace {

class DummyStatus : public grpc::Status {
public:
    DummyStatus(grpc::StatusCode code, std::string const& msg, std::string const& details = {})
        : grpc::Status(code, msg, details) {}
};

} // namespace

TEST(remote_exception_test, code_and_message_are_set) {
    DummyStatus status(grpc::StatusCode::DEADLINE_EXCEEDED, "timeout error", "detail info");
    remote_exception ex(status, "TestService/TestMethod");
    EXPECT_EQ(ex.code(), remote_error_code::deadline_exceeded);
    EXPECT_STREQ(ex.what(), "timeout error");
    EXPECT_EQ(ex.method(), "TestService/TestMethod");
}

TEST(remote_exception_test, code_maps_to_unknown_on_invalid_status) {
    DummyStatus status(static_cast<grpc::StatusCode>(999), "unknown error");
    remote_exception ex(status, "Service/Method");
    EXPECT_EQ(ex.code(), remote_error_code::unknown);
}

/**
 * @brief Test mapping between gRPC StatusCode and remote_error_code using a table.
 */
struct code_map_entry {
    grpc::StatusCode grpc_code;
    remote_error_code expected;
};

static constexpr std::array<code_map_entry, 17> code_map = {{
    {grpc::StatusCode::OK, remote_error_code::ok},
    {grpc::StatusCode::CANCELLED, remote_error_code::cancelled},
    {grpc::StatusCode::UNKNOWN, remote_error_code::unknown},
    {grpc::StatusCode::INVALID_ARGUMENT, remote_error_code::invalid_argument},
    {grpc::StatusCode::DEADLINE_EXCEEDED, remote_error_code::deadline_exceeded},
    {grpc::StatusCode::NOT_FOUND, remote_error_code::not_found},
    {grpc::StatusCode::ALREADY_EXISTS, remote_error_code::already_exists},
    {grpc::StatusCode::PERMISSION_DENIED, remote_error_code::permission_denied},
    {grpc::StatusCode::RESOURCE_EXHAUSTED, remote_error_code::resource_exhausted},
    {grpc::StatusCode::FAILED_PRECONDITION, remote_error_code::failed_precondition},
    {grpc::StatusCode::ABORTED, remote_error_code::aborted},
    {grpc::StatusCode::OUT_OF_RANGE, remote_error_code::out_of_range},
    {grpc::StatusCode::UNIMPLEMENTED, remote_error_code::unimplemented},
    {grpc::StatusCode::INTERNAL, remote_error_code::internal},
    {grpc::StatusCode::UNAVAILABLE, remote_error_code::unavailable},
    {grpc::StatusCode::DATA_LOSS, remote_error_code::data_loss},
    {grpc::StatusCode::UNAUTHENTICATED, remote_error_code::unauthenticated},
}};

TEST(remote_exception_test, all_status_codes_are_mapped) {
    for (auto const& entry : code_map) {
        DummyStatus status(entry.grpc_code, "msg");
        remote_exception ex(status, "S/M");
        EXPECT_EQ(ex.code(), entry.expected) << static_cast<int>(entry.grpc_code);
    }
}

} // namespace limestone::testing