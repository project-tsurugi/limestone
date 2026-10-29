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
#include "replication/message_group_commit.h"
#include "replication/message_gc_boundary_switch.h"
#include "replication/replication_message.h"
#include "replication/handler_resources.h"
#include "replication/control_channel_handler_resources.h"
#include "gtest/gtest.h"
#include "test_root.h"
#include "replication_test_helper.h"

namespace limestone::testing {

using namespace limestone::replication;

static constexpr const char* base_location = "/tmp/message_group_commit_test";

class message_group_commit_test : public ::testing::Test {
protected:
    void SetUp() override {
        boost::filesystem::remove_all(base_location);
        boost::filesystem::create_directories(base_location);
        limestone::api::configuration conf{};
        conf.set_data_location(base_location);
        datastore_ = std::make_unique<limestone::api::datastore_test>(conf);
    }

    void TearDown() override {
        datastore_.reset();
        boost::filesystem::remove_all(base_location);
    }

    std::unique_ptr<api::datastore_test> datastore_;
};


TEST_F(message_group_commit_test, round_trip) {
    message_group_commit original(123456789);
    replication_message_io out("");
    replication_message::send(out, original);
    // Verify that the wire_size constant (the basis of the single-frame
    // static_assert) matches the actual serialized length.
    EXPECT_EQ(out.get_out_size(), message_group_commit::wire_size);

    replication_message_io in(out.get_out_string());
    auto received_base = replication_message::receive(in);
    auto received = dynamic_cast<message_group_commit*>(received_base.get());
    ASSERT_NE(received, nullptr);
    EXPECT_EQ(received->epoch_number(), 123456789);
}

TEST_F(message_group_commit_test, post_receive) {
    datastore_->switch_epoch(1);
    EXPECT_EQ(datastore_->epoch_id_switched(), 1);
    message_group_commit msg(999);
    replication_message_io io("");
    replica_server dummy_server{};
    control_channel_handler_resources resources(io, dummy_server, *datastore_);
    msg.post_receive(resources);
    EXPECT_EQ(get_epoch(base_location), 999);
}

}  // namespace limestone::testing
