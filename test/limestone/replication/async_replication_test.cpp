#include <gtest/gtest.h>
#include <cstdlib>
#include "replication/async_replication.h"

using limestone::replication::async_replication;
using limestone::replication::async_replication_from_env;
using limestone::replication::to_string;
using limestone::replication::async_replication_from_string;

TEST(async_replication_from_env_test, returns_disabled_for_unset_and_empty_and_disabled) {
    unsetenv("TEST_ASYNC_ENV");
    EXPECT_EQ(async_replication_from_env("TEST_ASYNC_ENV"), async_replication::disabled);

    setenv("TEST_ASYNC_ENV", "", 1);
    EXPECT_EQ(async_replication_from_env("TEST_ASYNC_ENV"), async_replication::disabled);

    setenv("TEST_ASYNC_ENV", "disabled", 1);
    EXPECT_EQ(async_replication_from_env("TEST_ASYNC_ENV"), async_replication::disabled);

    unsetenv("TEST_ASYNC_ENV");
}

TEST(async_replication_from_env_test, returns_std_async_and_single_thread_async) {
    setenv("TEST_ASYNC_ENV", "std_async", 1);
    EXPECT_EQ(async_replication_from_env("TEST_ASYNC_ENV"), async_replication::std_async);

    setenv("TEST_ASYNC_ENV", "single_thread_async", 1);
    EXPECT_EQ(async_replication_from_env("TEST_ASYNC_ENV"), async_replication::single_thread_async);

    unsetenv("TEST_ASYNC_ENV");
}

TEST(async_replication_from_env_test, returns_boost_and_tbb_thread_pool_async) {
    setenv("TEST_ASYNC_ENV", "boost_thread_pool_async", 1);
    EXPECT_EQ(async_replication_from_env("TEST_ASYNC_ENV"), async_replication::boost_thread_pool_async);

    // setenv("TEST_ASYNC_ENV", "tbb_thread_pool_async", 1);
    // EXPECT_EQ(async_replication_from_env("TEST_ASYNC_ENV"), async_replication::tbb_thread_pool_async);

    unsetenv("TEST_ASYNC_ENV");
}

TEST(async_replication_from_env_test, fatal_on_invalid_value_catches_invalid_argument_and_aborts) {
    setenv("TEST_ASYNC_ENV", "invalid_value", 1);
    EXPECT_DEATH({
        std::cerr << "Testing async_replication_from_env with invalid value" << std::endl;
        async_replication_from_env("TEST_ASYNC_ENV");
        std::cerr << "This line should not be reached" << std::endl;
    }, "Invalid value for TEST_ASYNC_ENV");
    unsetenv("TEST_ASYNC_ENV");
}

TEST(async_replication_to_string_test, returns_expected_string) {
    EXPECT_EQ(to_string(async_replication::disabled), "disabled");
    EXPECT_EQ(to_string(async_replication::std_async), "std_async");
    EXPECT_EQ(to_string(async_replication::single_thread_async), "single_thread_async");
    EXPECT_EQ(to_string(async_replication::boost_thread_pool_async), "boost_thread_pool_async");
    // EXPECT_EQ(to_string(async_replication::tbb_thread_pool_async), "tbb_thread_pool_async");
}

TEST(async_replication_to_string_test, returns_unknown_for_invalid_enum_value) {
    // Cast an invalid integer to async_replication to simulate an unknown value.
    auto invalid_value = static_cast<async_replication>(999);
    EXPECT_EQ(to_string(invalid_value), "unknown");
}

TEST(async_replication_from_string_test, throws_invalid_argument_on_invalid_string) {
    EXPECT_THROW({
        async_replication_from_string("invalid_value");
    }, std::invalid_argument);
}
