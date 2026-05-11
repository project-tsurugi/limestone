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

#include <replication/message_rdma_finalize.h>

#include <gtest/gtest.h>

#include <boost/filesystem.hpp>

#include <replication/control_channel_handler_resources.h>
#include <replication/handler_resources.h>
#include <replication/message_error.h>
#include <replication/message_rdma_finalize_ack.h>
#include <replication/replica_server.h>
#include <replication/replication_message_io.h>

#include "noop_rdma_mocks.h"

namespace limestone::testing {

using limestone::replication::handler_resources;
using limestone::replication::replication_message_io;

TEST(message_rdma_finalize_test, get_message_type_id) {
    replication::message_rdma_finalize msg{};
    EXPECT_EQ(msg.get_message_type_id(), replication::message_type_id::RDMA_FINALIZE);
}

TEST(message_rdma_finalize_test, replication_message_round_trip) {
    replication::message_rdma_finalize original{};

    replication_message_io out("");
    replication::replication_message::send(out, original);

    replication_message_io in(out.get_out_string());
    auto received_base = replication::replication_message::receive(in);
    auto received = dynamic_cast<replication::message_rdma_finalize*>(received_base.get());
    ASSERT_NE(received, nullptr);
    EXPECT_EQ(received->get_message_type_id(),
              replication::message_type_id::RDMA_FINALIZE);
}

TEST(message_rdma_finalize_test, post_receive_with_invalid_resources_returns_error) {
    replication::message_rdma_finalize msg{};
    replication_message_io io("");
    handler_resources resources{io};

    msg.post_receive(resources);

    replication_message_io reader(io.get_out_string());
    auto response = replication::replication_message::receive(reader);
    auto* err = dynamic_cast<replication::message_error*>(response.get());
    ASSERT_NE(err, nullptr);
    EXPECT_EQ(err->get_error_code(),
              replication::message_error::rdma_finalize_error_invalid_resources);
}

TEST(message_rdma_finalize_test, post_receive_returns_error_when_not_initialized) {
    boost::filesystem::path base_location = "/tmp/message_rdma_finalize_test_not_initialized";
    boost::filesystem::remove_all(base_location);
    boost::filesystem::create_directories(base_location);

    replication::replica_server server{};
    server.initialize(base_location);

    replication_message_io io("");
    replication::control_channel_handler_resources resources(io, server, server.get_datastore());
    replication::message_rdma_finalize msg{};
    msg.post_receive(resources);

    replication_message_io reader(io.get_out_string());
    auto response = replication::replication_message::receive(reader);
    auto* err = dynamic_cast<replication::message_error*>(response.get());
    ASSERT_NE(err, nullptr);
    EXPECT_EQ(err->get_error_code(),
              replication::message_error::rdma_finalize_error_not_initialized);

    boost::filesystem::remove_all(base_location);
}

TEST(message_rdma_finalize_test, post_receive_returns_ack_when_initialized_and_finalized) {
    boost::filesystem::path base_location = "/tmp/message_rdma_finalize_test_success";
    boost::filesystem::remove_all(base_location);
    boost::filesystem::create_directories(base_location);

    replication::replica_server server{};
    server.initialize(base_location);

    // Inject noop RDMA stubs so the test runs without engaging the vendor RDMA mock
    // (which is a process-wide singleton and unavailable when ENABLE_RDMA=OFF).
    server.set_rdma_receiver_factory_for_test(
        [](std::uint32_t /*slot_count*/) -> std::unique_ptr<replication::rdma_receiver_base> {
            return std::make_unique<noop_rdma_receiver>();
        });
    server.set_ack_sender_factory_for_test(
        [](std::uint32_t /*slot_count*/) -> std::unique_ptr<replication::rdma_sender_base> {
            return std::make_unique<noop_rdma_sender>();
        });
    auto init_result = server.initialize_rdma(4U, 0x1ULL);
    ASSERT_EQ(init_result, replication::replica_server::rdma_init_result::success);

    replication_message_io io("");
    replication::control_channel_handler_resources resources(io, server, server.get_datastore());
    replication::message_rdma_finalize msg{};
    msg.post_receive(resources);

    replication_message_io reader(io.get_out_string());
    auto response = replication::replication_message::receive(reader);
    auto* ack = dynamic_cast<replication::message_rdma_finalize_ack*>(response.get());
    ASSERT_NE(ack, nullptr);
    EXPECT_EQ(response->get_message_type_id(),
              replication::message_type_id::RDMA_FINALIZE_ACK);

    boost::filesystem::remove_all(base_location);
}

}  // namespace limestone::testing
