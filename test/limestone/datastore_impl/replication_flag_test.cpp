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
        unsetenv("REPLICATION_RDMA_SLOTS");
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

TEST_F(replication_flag_test, rdma_flag_not_set_disables_rdma) {
    unsetenv("REPLICATION_RDMA_SLOTS");
    datastore_impl ds;
    EXPECT_FALSE(ds.is_rdma_enabled());
    EXPECT_FALSE(ds.rdma_slot_count().has_value());
}

TEST_F(replication_flag_test, rdma_flag_enabled_with_valid_value) {
    setenv("REPLICATION_RDMA_SLOTS", "1024", 1);
    datastore_impl ds;
    ASSERT_TRUE(ds.rdma_slot_count().has_value());
    EXPECT_TRUE(ds.is_rdma_enabled());
    EXPECT_EQ(ds.rdma_slot_count().value(), 1024);
}

TEST_F(replication_flag_test, rdma_flag_invalid_non_numeric_disables_rdma) {
    setenv("REPLICATION_RDMA_SLOTS", "invalid", 1);
    datastore_impl ds;
    EXPECT_FALSE(ds.is_rdma_enabled());
    EXPECT_FALSE(ds.rdma_slot_count().has_value());
}

TEST_F(replication_flag_test, rdma_flag_invalid_zero_disables_rdma) {
    setenv("REPLICATION_RDMA_SLOTS", "0", 1);
    datastore_impl ds;
    EXPECT_FALSE(ds.is_rdma_enabled());
    EXPECT_FALSE(ds.rdma_slot_count().has_value());
}

TEST_F(replication_flag_test, rdma_flag_invalid_negative_disables_rdma) {
    setenv("REPLICATION_RDMA_SLOTS", "-1", 1);
    datastore_impl ds;
    EXPECT_FALSE(ds.is_rdma_enabled());
    EXPECT_FALSE(ds.rdma_slot_count().has_value());
}

TEST_F(replication_flag_test, rdma_flag_invalid_overflow_disables_rdma) {
    setenv("REPLICATION_RDMA_SLOTS", "2147483648", 1); // INT32_MAX + 1
    datastore_impl ds;
    EXPECT_FALSE(ds.is_rdma_enabled());
    EXPECT_FALSE(ds.rdma_slot_count().has_value());
}

TEST_F(replication_flag_test, rdma_flag_upper_boundary_valid) {
    setenv("REPLICATION_RDMA_SLOTS", "2147483647", 1); // INT32_MAX
    datastore_impl ds;
    ASSERT_TRUE(ds.is_rdma_enabled());
    ASSERT_TRUE(ds.rdma_slot_count().has_value());
    EXPECT_EQ(ds.rdma_slot_count().value(), 2147483647);
}

TEST_F(replication_flag_test, rdma_flag_invalid_trailing_characters_disables_rdma) {
    setenv("REPLICATION_RDMA_SLOTS", "1024abc", 1);
    datastore_impl ds;
    EXPECT_FALSE(ds.is_rdma_enabled());
    EXPECT_FALSE(ds.rdma_slot_count().has_value());
}

TEST_F(replication_flag_test, rdma_flag_invalid_erange_disables_rdma) {
    // A very large number to trigger ERANGE from strtoll
    setenv("REPLICATION_RDMA_SLOTS", "9999999999999999999999999999", 1);
    datastore_impl ds;
    EXPECT_FALSE(ds.is_rdma_enabled());
    EXPECT_FALSE(ds.rdma_slot_count().has_value());
}

TEST_F(replication_flag_test, rdma_flag_invalid_empty_string_disables_rdma) {
    setenv("REPLICATION_RDMA_SLOTS", "", 1);
    datastore_impl ds;
    EXPECT_FALSE(ds.is_rdma_enabled());
    EXPECT_FALSE(ds.rdma_slot_count().has_value());
}

TEST_F(replication_flag_test, rdma_flag_invalid_whitespace_disables_rdma) {
    setenv("REPLICATION_RDMA_SLOTS", "   ", 1);
    datastore_impl ds;
    EXPECT_FALSE(ds.is_rdma_enabled());
    EXPECT_FALSE(ds.rdma_slot_count().has_value());
}

}  // namespace limestone::api
