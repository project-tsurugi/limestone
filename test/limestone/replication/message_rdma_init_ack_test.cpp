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

#include <replication/message_rdma_init_ack.h>

#include <gtest/gtest.h>

#include <replication/socket_io.h>

namespace limestone::testing {

using limestone::replication::socket_io;

TEST(message_rdma_init_ack_test, constructor_sets_remote_dma_address) {
    replication::message_rdma_init_ack msg(1234U);
    EXPECT_EQ(msg.get_remote_dma_address(), 1234U);
}

TEST(message_rdma_init_ack_test, get_message_type_id) {
    replication::message_rdma_init_ack msg(0U);
    EXPECT_EQ(msg.get_message_type_id(), replication::message_type_id::RDMA_INIT_ACK);
}

TEST(message_rdma_init_ack_test, replication_message_round_trip) {
    replication::message_rdma_init_ack original(0xdeadbeefcafebabeULL);

    socket_io out("");
    replication::replication_message::send(out, original);

    socket_io in(out.get_out_string());
    auto received_base = replication::replication_message::receive(in);
    auto received = dynamic_cast<replication::message_rdma_init_ack*>(received_base.get());
    ASSERT_NE(received, nullptr);
    EXPECT_EQ(received->get_remote_dma_address(), 0xdeadbeefcafebabeULL);
}

}  // namespace limestone::testing

