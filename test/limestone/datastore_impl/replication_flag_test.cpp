#include <gtest/gtest.h>
#include <cstdlib>
#include "datastore_impl.h"

namespace limestone::api {

using limestone::api::datastore_impl;
using limestone::replication::async_replication;

class replication_flag_test : public ::testing::Test {
protected:
    void TearDown() override {
        unsetenv("TSURUGI_REPLICATION_ENDPOINT");
        unsetenv("REPLICATION_ASYNC_SESSION_CLOSE");
        unsetenv("REPLICATION_ASYNC_GROUP_COMMIT");
    }
};

TEST_F(replication_flag_test, initial_has_no_replica_when_env_not_set) {
    unsetenv("TSURUGI_REPLICATION_ENDPOINT");
    datastore_impl datastore;
    EXPECT_FALSE(datastore.has_replica());
}

TEST_F(replication_flag_test, initial_has_replica_when_valid_endpoint) {
    setenv("TSURUGI_REPLICATION_ENDPOINT", "tcp://localhost:1234", 1);
    datastore_impl datastore;
    EXPECT_TRUE(datastore.has_replica());
}

TEST_F(replication_flag_test, initial_has_no_replica_when_invalid_endpoint) {
    setenv("TSURUGI_REPLICATION_ENDPOINT", "invalid://endpoint", 1);
    datastore_impl datastore;
    EXPECT_FALSE(datastore.has_replica());
}

TEST_F(replication_flag_test, disable_replica_sets_has_replica_false) {
    setenv("TSURUGI_REPLICATION_ENDPOINT", "tcp://localhost:1234", 1);
    datastore_impl datastore;
    EXPECT_TRUE(datastore.has_replica());

    datastore.disable_replica();
    EXPECT_FALSE(datastore.has_replica());
}

TEST_F(replication_flag_test, async_flags_default_are_false) {
    datastore_impl ds;
    EXPECT_FALSE(ds.is_async_session_close_enabled());
    EXPECT_FALSE(ds.is_async_group_commit_enabled());
}

TEST_F(replication_flag_test, async_session_close_flag_enabled) {
    setenv("REPLICATION_ASYNC_SESSION_CLOSE", "1", 1);
    datastore_impl ds;
    EXPECT_TRUE(ds.is_async_session_close_enabled());
    EXPECT_FALSE(ds.is_async_group_commit_enabled());
    unsetenv("REPLICATION_ASYNC_SESSION_CLOSE");
}

TEST_F(replication_flag_test, async_group_commit_flag_enabled) {
    setenv("REPLICATION_ASYNC_GROUP_COMMIT", "1", 1);
    datastore_impl ds;
    EXPECT_FALSE(ds.is_async_session_close_enabled());
    EXPECT_TRUE(ds.is_async_group_commit_enabled());
    unsetenv("REPLICATION_ASYNC_GROUP_COMMIT");
}

TEST_F(replication_flag_test, async_both_flags_enabled) {
    setenv("REPLICATION_ASYNC_SESSION_CLOSE", "1", 1);
    setenv("REPLICATION_ASYNC_GROUP_COMMIT", "1", 1);
    datastore_impl ds;
    EXPECT_TRUE(ds.is_async_session_close_enabled());
    EXPECT_TRUE(ds.is_async_group_commit_enabled());
    unsetenv("REPLICATION_ASYNC_SESSION_CLOSE");
    unsetenv("REPLICATION_ASYNC_GROUP_COMMIT");
}

// Test that async_replication_from_env returns disabled for unset, empty, or "disabled"
TEST(async_replication_from_env_test, returns_disabled_for_unset_and_empty_and_disabled) {
    unsetenv("TEST_ASYNC_ENV");
    EXPECT_EQ(datastore_impl::async_replication_from_env("TEST_ASYNC_ENV"), async_replication::disabled);

    setenv("TEST_ASYNC_ENV", "", 1);
    EXPECT_EQ(datastore_impl::async_replication_from_env("TEST_ASYNC_ENV"), async_replication::disabled);

    setenv("TEST_ASYNC_ENV", "disabled", 1);
    EXPECT_EQ(datastore_impl::async_replication_from_env("TEST_ASYNC_ENV"), async_replication::disabled);

    unsetenv("TEST_ASYNC_ENV");
}

// Test that async_replication_from_env returns correct enum for valid values
TEST(async_replication_from_env_test, returns_std_async_and_single_thread_async) {
    setenv("TEST_ASYNC_ENV", "std_async", 1);
    EXPECT_EQ(datastore_impl::async_replication_from_env("TEST_ASYNC_ENV"), async_replication::std_async);

    setenv("TEST_ASYNC_ENV", "single_thread_async", 1);
    EXPECT_EQ(datastore_impl::async_replication_from_env("TEST_ASYNC_ENV"), async_replication::single_thread_async);

    unsetenv("TEST_ASYNC_ENV");
}

// Test that async_replication_from_env logs fatal and terminates for invalid value
TEST(async_replication_from_env_test, fatal_on_invalid_value) {
    setenv("TEST_ASYNC_ENV", "invalid_value", 1);
    // This should terminate the process with a fatal log
    EXPECT_DEATH({
        datastore_impl::async_replication_from_env("TEST_ASYNC_ENV");
    }, "Invalid value for TEST_ASYNC_ENV");
    unsetenv("TEST_ASYNC_ENV");
}

}  // namespace limestone::api

