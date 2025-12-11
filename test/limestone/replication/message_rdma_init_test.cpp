/*
 * Copyright 2022-2025 Project Tsurugi.
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

#include <replication/message_rdma_init.h>

#include <gtest/gtest.h>

#include <replication/control_channel_handler_resources.h>
#include <replication/handler_resources.h>
#include <replication/message_error.h>
#include <replication/message_rdma_init_ack.h>
#include <replication/replica_server.h>
#include <replication/socket_io.h>

namespace limestone::testing {

using limestone::replication::handler_resources;
using limestone::replication::socket_io;

TEST(message_rdma_init_test, constructor_sets_slot_count) {
    replication::message_rdma_init msg(128u);
    EXPECT_EQ(msg.get_slot_count(), 128u);
}

TEST(message_rdma_init_test, get_message_type_id) {
    replication::message_rdma_init msg(0u);
    EXPECT_EQ(msg.get_message_type_id(), replication::message_type_id::RDMA_INIT);
}

TEST(message_rdma_init_test, replication_message_round_trip) {
    replication::message_rdma_init original(256u);

    socket_io out("");
    replication::replication_message::send(out, original);

    socket_io in(out.get_out_string());
    auto received_base = replication::replication_message::receive(in);
    auto received = dynamic_cast<replication::message_rdma_init*>(received_base.get());
    ASSERT_NE(received, nullptr);
    EXPECT_EQ(received->get_slot_count(), 256u);
}

TEST(message_rdma_init_test, post_receive_with_invalid_resources_returns_error) {
    replication::message_rdma_init msg(1u);
    socket_io io("");
    handler_resources resources{io};

    msg.post_receive(resources);

    socket_io reader(io.get_out_string());
    auto response = replication::replication_message::receive(reader);
    auto* err = dynamic_cast<replication::message_error*>(response.get());
    ASSERT_NE(err, nullptr);
    EXPECT_EQ(err->get_error_code(), replication::message_error::rdma_init_error_invalid_resources);
}

TEST(message_rdma_init_test, post_receive_returns_ack_then_error_on_second_init) {
    boost::filesystem::path base_location = "/tmp/message_rdma_init_test";
    boost::filesystem::remove_all(base_location);
    boost::filesystem::create_directories(base_location);

    replication::replica_server server{};
    server.initialize(base_location);

    {
        socket_io io("");
        replication::control_channel_handler_resources resources(io, server, server.get_datastore());
        replication::message_rdma_init msg(4u);
        msg.post_receive(resources);

        socket_io reader(io.get_out_string());
        auto response = replication::replication_message::receive(reader);
        auto* ack = dynamic_cast<replication::message_rdma_init_ack*>(response.get());
        ASSERT_NE(ack, nullptr);
        EXPECT_EQ(response->get_message_type_id(), replication::message_type_id::RDMA_INIT_ACK);
    }

    {
        socket_io io("");
        replication::control_channel_handler_resources resources(io, server, server.get_datastore());
        replication::message_rdma_init msg(4u);
        msg.post_receive(resources);

        socket_io reader(io.get_out_string());
        auto response = replication::replication_message::receive(reader);
        auto* err = dynamic_cast<replication::message_error*>(response.get());
        ASSERT_NE(err, nullptr);
        EXPECT_EQ(err->get_error_code(), replication::message_error::rdma_init_error_already_initialized);
    }

    boost::filesystem::remove_all(base_location);
}

}  // namespace limestone::testing
