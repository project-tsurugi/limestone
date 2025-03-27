#include <gtest/gtest.h>
#include <cstdlib>
#include "datastore_impl.h"

namespace limestone::api {

using limestone::api::datastore_impl;

class replication_flag_test : public ::testing::Test {
protected:
    void TearDown() override {
        unsetenv("TSURUGI_REPLICATION_ENDPOINT");
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

}  // namespace limestone::api
