#include <gtest/gtest.h>
#include <limestone/wal_sync/remote_exception.h>
#include <sstream>

namespace limestone::testing {


using limestone::internal::remote_error_code;
using limestone::internal::to_string_view;

TEST(remote_error_code_test, to_string_view_returns_expected_string) {
    EXPECT_EQ(to_string_view(remote_error_code::ok), "ok");
    EXPECT_EQ(to_string_view(remote_error_code::cancelled), "cancelled");
    EXPECT_EQ(to_string_view(remote_error_code::unknown), "unknown");
    EXPECT_EQ(to_string_view(remote_error_code::invalid_argument), "invalid_argument");
    EXPECT_EQ(to_string_view(remote_error_code::deadline_exceeded), "deadline_exceeded");
    EXPECT_EQ(to_string_view(remote_error_code::not_found), "not_found");
    EXPECT_EQ(to_string_view(remote_error_code::already_exists), "already_exists");
    EXPECT_EQ(to_string_view(remote_error_code::permission_denied), "permission_denied");
    EXPECT_EQ(to_string_view(remote_error_code::resource_exhausted), "resource_exhausted");
    EXPECT_EQ(to_string_view(remote_error_code::failed_precondition), "failed_precondition");
    EXPECT_EQ(to_string_view(remote_error_code::aborted), "aborted");
    EXPECT_EQ(to_string_view(remote_error_code::out_of_range), "out_of_range");
    EXPECT_EQ(to_string_view(remote_error_code::unimplemented), "unimplemented");
    EXPECT_EQ(to_string_view(remote_error_code::internal), "internal");
    EXPECT_EQ(to_string_view(remote_error_code::unavailable), "unavailable");
    EXPECT_EQ(to_string_view(remote_error_code::data_loss), "data_loss");
    EXPECT_EQ(to_string_view(remote_error_code::unauthenticated), "unauthenticated");
}

TEST(remote_error_code_test, ostream_operator_outputs_string) {
    std::ostringstream oss;
    oss << remote_error_code::deadline_exceeded;
    EXPECT_EQ(oss.str(), "deadline_exceeded");
}

TEST(remote_error_code_test, to_string_view_returns_unknown_for_invalid_value) {
    // Intentionally use an invalid enum value to cover the default branch
    auto invalid = static_cast<remote_error_code>(999);
    EXPECT_EQ(to_string_view(invalid), "(unknown)");
}

} // namespace limestone::testing