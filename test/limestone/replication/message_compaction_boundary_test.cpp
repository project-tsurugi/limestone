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
#include <cstdint>
#include <limits>
#include <memory>

#include <boost/filesystem.hpp>

#include "replication/control_channel_handler_resources.h"
#include "replication/message_compaction_boundary.h"
#include "replication/replication_message.h"

#include "gtest/gtest.h"
#include "test_root.h"

#include "datastore_impl.h"

namespace limestone::testing {

using namespace limestone::replication;

class message_compaction_boundary_test : public ::testing::Test {
protected:
    static constexpr const char* base_location = "/tmp/message_compaction_boundary_test";

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

// Encoding and decoding preserves the boundary epoch and the BLOB ID boundary
TEST_F(message_compaction_boundary_test, round_trip) {
    const std::uint64_t epoch = std::numeric_limits<std::uint64_t>::max() - 1;
    const std::uint64_t blob_id = 0x0123456789abcdefULL;
    message_compaction_boundary original{epoch, blob_id};
    replication_message_io out("");

    replication_message::send(out, original);
    EXPECT_EQ(out.get_out_size(), message_compaction_boundary::wire_size);

    replication_message_io in(out.get_out_string());
    std::unique_ptr<replication_message> received_base = replication_message::receive(in);
    ASSERT_EQ(received_base->get_message_type_id(), message_type_id::COMPACTION_BOUNDARY);
    auto* received = dynamic_cast<message_compaction_boundary*>(received_base.get());
    ASSERT_NE(received, nullptr);
    EXPECT_EQ(received->boundary_epoch(), epoch);
    EXPECT_EQ(received->blob_id_boundary(), blob_id);
}

// The type id is 0x05
TEST_F(message_compaction_boundary_test, type_id) {
    EXPECT_EQ(static_cast<std::uint8_t>(message_type_id::COMPACTION_BOUNDARY), 0x05);
}

// The received boundary is kept in the datastore. No response is sent. A boundary received later replaces the
// earlier one
TEST_F(message_compaction_boundary_test, post_receive_keeps_the_latest_boundary) {
    ASSERT_FALSE(datastore_->get_impl()->get_received_compaction_boundary().has_value());
    replication_message_io io("");
    replica_server dummy_server{};
    control_channel_handler_resources resources(io, dummy_server, *datastore_);

    message_compaction_boundary first{10, 100};
    first.post_receive(resources);
    {
        const std::optional<api::datastore_impl::compaction_boundary> kept =
            datastore_->get_impl()->get_received_compaction_boundary();
        ASSERT_TRUE(kept.has_value());
        EXPECT_EQ(kept->boundary_epoch, 10U);
        EXPECT_EQ(kept->blob_id_boundary, 100U);
    }

    message_compaction_boundary second{20, 150};
    second.post_receive(resources);
    {
        const std::optional<api::datastore_impl::compaction_boundary> kept =
            datastore_->get_impl()->get_received_compaction_boundary();
        ASSERT_TRUE(kept.has_value());
        EXPECT_EQ(kept->boundary_epoch, 20U);
        EXPECT_EQ(kept->blob_id_boundary, 150U);
    }
    EXPECT_EQ(io.get_out_size(), 0U);
}

}  // namespace limestone::testing
