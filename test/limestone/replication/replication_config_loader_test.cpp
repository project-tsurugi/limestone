/*
 * Copyright 2022-2026 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include <replication/replication_config_loader.h>

#include <cstdlib>

#include <gtest/gtest.h>

#include "test_root.h"
#include <limestone/api/limestone_exception.h>

namespace limestone::testing {

using limestone::replication::load_replication_config_from_environment;
using limestone::replication::replication_mode;

class replication_config_loader_test : public ::testing::Test {
protected:
    void SetUp() override {
        clear_env();
    }

    void TearDown() override {
        clear_env();
    }

    static void clear_env() {
        unsetenv("TSURUGI_REPLICATION_HANDSHAKE_SOCKET");
        unsetenv("TSURUGI_REPLICATION_SERVICE_ID");
        unsetenv("TSURUGI_REPLICATION_ENDPOINT");
        unsetenv("REPLICATION_RDMA_SLOTS");
    }

    static void set_env(char const* name, char const* value) {
        setenv(name, value, 1);
    }

    static limestone::api::configuration make_conf() {
        if (system("rm -rf /tmp/replication_config_loader_test") != 0) {
            ADD_FAILURE() << "cannot remove directory";
        }
        if (system("mkdir -p /tmp/replication_config_loader_test") != 0) {
            ADD_FAILURE() << "cannot make directory";
        }
        limestone::api::configuration conf{};
        conf.set_data_location("/tmp/replication_config_loader_test");
        return conf;
    }
};

TEST_F(replication_config_loader_test, no_env_means_replication_disabled) {
    auto result = load_replication_config_from_environment();
    EXPECT_TRUE(result.ok);
    EXPECT_EQ(result.config.mode(), replication_mode::none);
}

TEST_F(replication_config_loader_test, endpoint_env_selects_tcp_mode) {
    set_env("TSURUGI_REPLICATION_ENDPOINT", "tcp://localhost:12345");
    auto result = load_replication_config_from_environment();
    EXPECT_TRUE(result.ok);
    EXPECT_EQ(result.config.mode(), replication_mode::tcp);
}

TEST_F(replication_config_loader_test, malformed_service_id_env_is_rejected) {
    set_env("TSURUGI_REPLICATION_HANDSHAKE_SOCKET", "/tmp/hs.sock");
    set_env("TSURUGI_REPLICATION_SERVICE_ID", "abc");
    auto result = load_replication_config_from_environment();
    EXPECT_FALSE(result.ok);
    EXPECT_NE(result.error_message.find("TSURUGI_REPLICATION_SERVICE_ID"), std::string::npos);
    EXPECT_EQ(result.config.mode(), replication_mode::none);
}

TEST_F(replication_config_loader_test, datastore_construction_fails_on_invalid_config) {
    set_env("TSURUGI_REPLICATION_SERVICE_ID", "100");
    auto conf = make_conf();
    EXPECT_THROW(limestone::api::datastore_test ds{conf}, limestone::api::limestone_exception);
}

#ifdef LIMESTONE_ENABLE_RDMA

TEST_F(replication_config_loader_test, socket_and_service_id_env_select_rdma_mode) {
    set_env("TSURUGI_REPLICATION_HANDSHAKE_SOCKET", "/tmp/hs.sock");
    set_env("TSURUGI_REPLICATION_SERVICE_ID", "100");
    auto result = load_replication_config_from_environment();
    EXPECT_TRUE(result.ok);
    EXPECT_EQ(result.config.mode(), replication_mode::rdma);
    EXPECT_EQ(result.config.handshake_socket_path(), "/tmp/hs.sock");
    EXPECT_EQ(result.config.service_id(), 100U);
}

TEST_F(replication_config_loader_test, datastore_construction_fails_in_rdma_mode_without_slots) {
    set_env("TSURUGI_REPLICATION_HANDSHAKE_SOCKET", "/tmp/hs.sock");
    set_env("TSURUGI_REPLICATION_SERVICE_ID", "100");
    auto conf = make_conf();
    EXPECT_THROW(limestone::api::datastore_test ds{conf}, limestone::api::limestone_exception);
}

TEST_F(replication_config_loader_test, datastore_construction_fails_on_invalid_slots_in_rdma_mode) {
    set_env("TSURUGI_REPLICATION_HANDSHAKE_SOCKET", "/tmp/hs.sock");
    set_env("TSURUGI_REPLICATION_SERVICE_ID", "100");
    set_env("REPLICATION_RDMA_SLOTS", "abc");
    auto conf = make_conf();
    EXPECT_THROW(limestone::api::datastore_test ds{conf}, limestone::api::limestone_exception);
}

TEST_F(replication_config_loader_test, datastore_construction_succeeds_in_rdma_mode_with_slots) {
    set_env("TSURUGI_REPLICATION_HANDSHAKE_SOCKET", "/tmp/hs.sock");
    set_env("TSURUGI_REPLICATION_SERVICE_ID", "100");
    set_env("REPLICATION_RDMA_SLOTS", "4");
    auto conf = make_conf();
    EXPECT_NO_THROW(limestone::api::datastore_test ds{conf});
}

#else // LIMESTONE_ENABLE_RDMA

TEST_F(replication_config_loader_test, rdma_env_is_rejected_without_rdma_build) {
    set_env("TSURUGI_REPLICATION_HANDSHAKE_SOCKET", "/tmp/hs.sock");
    set_env("TSURUGI_REPLICATION_SERVICE_ID", "100");
    auto result = load_replication_config_from_environment();
    EXPECT_FALSE(result.ok);
    EXPECT_NE(result.error_message.find("ENABLE_RDMA=OFF"), std::string::npos);
}

TEST_F(replication_config_loader_test, datastore_construction_fails_without_rdma_build) {
    set_env("TSURUGI_REPLICATION_HANDSHAKE_SOCKET", "/tmp/hs.sock");
    set_env("TSURUGI_REPLICATION_SERVICE_ID", "100");
    set_env("REPLICATION_RDMA_SLOTS", "4");
    auto conf = make_conf();
    EXPECT_THROW(limestone::api::datastore_test ds{conf}, limestone::api::limestone_exception);
}

#endif // LIMESTONE_ENABLE_RDMA

} // namespace limestone::testing
