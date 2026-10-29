/*
 * Copyright 2025-2026 Project Tsurugi.
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
#include <gtest/gtest.h>
#include "replication/log_channel_handler_resources.h"
#include "replication/control_channel_handler_resources.h"

namespace limestone::testing {

using namespace limestone::replication;

class handler_resources_test : public ::testing::Test {
protected:
    static constexpr const char* base_location = "/tmp/handler_resources_test";

    void SetUp() override {
        boost::filesystem::remove_all(base_location);
        boost::filesystem::create_directories(base_location);
    }

    void TearDown() override { boost::filesystem::remove_all(base_location); }
};

TEST_F(handler_resources_test, returns_correct_server_and_channel) {
    replica_server server{};
    server.initialize(base_location);
    auto& ds = server.get_datastore();
    auto& channel = ds.create_channel();
    replication_message_io io("");

    log_channel_handler_resources resources{io, channel};

    EXPECT_EQ(&resources.get_log_channel(), &channel);
    EXPECT_EQ(&resources.get_replication_message_io(), &io); 
}

TEST_F(handler_resources_test, returns_correct_socket) {
    replication_message_io io("dummy");
    handler_resources resources{io};

    EXPECT_EQ(&resources.get_replication_message_io(), &io);
}

TEST_F(handler_resources_test, returns_correct_datastore) {
    replica_server server{};
    server.initialize(base_location);
    auto& ds = server.get_datastore();

    replication_message_io io("dummy");
    control_channel_handler_resources resources(io, server, ds);

    EXPECT_EQ(&resources.get_datastore(), &ds);
}


}  // namespace limestone::testing
