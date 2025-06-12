#include "replication/validation_result.h"
#include "gtest/gtest.h"

namespace limestone::testing {
using namespace limestone::replication;

TEST(validation_result_test, success_case) {
    auto result = validation_result::success();
    EXPECT_TRUE(result.ok());
    EXPECT_EQ(result.error_code(), 0u);
    EXPECT_EQ(result.error_message(), "");
}

TEST(validation_result_test, error_case) {
    uint16_t code = 123;
    std::string msg = "validation failed";

    auto result = validation_result::error(code, msg);
    EXPECT_FALSE(result.ok());
    EXPECT_EQ(result.error_code(), code);
    EXPECT_EQ(result.error_message(), msg);
}

TEST(validation_result_test, immutability_of_result) {
    auto result = validation_result::error(7, "fatal");
    // original remains unchanged
    EXPECT_EQ(result.error_code(), 7);
    EXPECT_EQ(result.error_message(), "fatal");
    EXPECT_FALSE(result.ok());
}

}  // namespace limestone::testing
