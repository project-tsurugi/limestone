#include <gtest/gtest.h>
#include <cstdlib>
#include "datastore_impl.h"

namespace limestone::api {

using limestone::api::datastore_impl;

class replication_flag_test : public ::testing::Test {
protected:
    void TearDown() override {
        unsetenv("TSURUGI_REPLICATION_ENDPOINT");
        unsetenv("REPLICATION_ASYNC_SESSION_CLOSE");
        unsetenv("REPLICATION_ASYNC_GROUP_COMMIT");
    }
    datastore dummy_ds;
};

TEST_F(replication_flag_test, initial_has_no_replica_when_env_not_set) {
    unsetenv("TSURUGI_REPLICATION_ENDPOINT");
    datastore_impl datastore(dummy_ds);
    EXPECT_FALSE(datastore.has_replica());
}

TEST_F(replication_flag_test, initial_has_replica_when_valid_endpoint) {
    setenv("TSURUGI_REPLICATION_ENDPOINT", "tcp://localhost:1234", 1);
    datastore_impl datastore(dummy_ds);
    EXPECT_TRUE(datastore.has_replica());
}

TEST_F(replication_flag_test, initial_has_no_replica_when_invalid_endpoint) {
    setenv("TSURUGI_REPLICATION_ENDPOINT", "invalid://endpoint", 1);
    datastore_impl datastore(dummy_ds);
    EXPECT_FALSE(datastore.has_replica());
}

TEST_F(replication_flag_test, disable_replica_sets_has_replica_false) {
    setenv("TSURUGI_REPLICATION_ENDPOINT", "tcp://localhost:1234", 1);
    datastore_impl datastore(dummy_ds);
    EXPECT_TRUE(datastore.has_replica());

    datastore.disable_replica();
    EXPECT_FALSE(datastore.has_replica());
}

TEST_F(replication_flag_test, async_flags_default_are_false) {
    datastore_impl ds(dummy_ds);
    EXPECT_FALSE(ds.is_async_session_close_enabled());
    EXPECT_FALSE(ds.is_async_group_commit_enabled());
}

TEST_F(replication_flag_test, async_session_close_flag_enabled) {
    setenv("REPLICATION_ASYNC_SESSION_CLOSE", "1", 1);
    datastore_impl ds(dummy_ds);
    EXPECT_TRUE(ds.is_async_session_close_enabled());
    EXPECT_FALSE(ds.is_async_group_commit_enabled());
    unsetenv("REPLICATION_ASYNC_SESSION_CLOSE");
}

TEST_F(replication_flag_test, async_group_commit_flag_enabled) {
    setenv("REPLICATION_ASYNC_GROUP_COMMIT", "1", 1);
    datastore_impl ds(dummy_ds);
    EXPECT_FALSE(ds.is_async_session_close_enabled());
    EXPECT_TRUE(ds.is_async_group_commit_enabled());
    unsetenv("REPLICATION_ASYNC_GROUP_COMMIT");
}

TEST_F(replication_flag_test, async_both_flags_enabled) {
    setenv("REPLICATION_ASYNC_SESSION_CLOSE", "1", 1);
    setenv("REPLICATION_ASYNC_GROUP_COMMIT", "1", 1);
    datastore_impl ds(dummy_ds);
    EXPECT_TRUE(ds.is_async_session_close_enabled());
    EXPECT_TRUE(ds.is_async_group_commit_enabled());
    unsetenv("REPLICATION_ASYNC_SESSION_CLOSE");
    unsetenv("REPLICATION_ASYNC_GROUP_COMMIT");
}

}  // namespace limestone::api
