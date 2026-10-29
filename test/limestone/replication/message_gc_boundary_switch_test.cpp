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
#include "gtest/gtest.h"

namespace limestone::testing {

using namespace limestone::replication;

TEST(message_gc_boundary_switch_test, round_trip) {
    message_gc_boundary_switch original(42);
    replication_message_io out("");
    replication_message::send(out, original);

    replication_message_io in(out.get_out_string());
    auto received_base = replication_message::receive(in);
    auto received = dynamic_cast<message_gc_boundary_switch*>(received_base.get());
    ASSERT_NE(received, nullptr);
    EXPECT_EQ(received->write_version(), 42);
}

TEST(message_gc_boundary_switch_test, post_receive_throws) {
    message_gc_boundary_switch msg(123);
    replication_message_io io("");
    handler_resources resources{io};
    EXPECT_THROW(msg.post_receive(resources), std::logic_error);
}

}  // namespace limestone::testing
