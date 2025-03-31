#include "replication/message_group_commit.h"
#include "replication/message_gc_boundary_switch.h"
#include "replication/replication_message.h"
#include "replication/socket_io.h"
#include "gtest/gtest.h"

namespace limestone::testing {

using namespace limestone::replication;

TEST(message_group_commit_test, round_trip) {
    message_group_commit original(123456789);
    socket_io out("");
    replication_message::send(out, original);

    socket_io in(out.get_out_string());
    auto received_base = replication_message::receive(in);
    auto received = dynamic_cast<message_group_commit*>(received_base.get());
    ASSERT_NE(received, nullptr);
    EXPECT_EQ(received->epoch_number(), 123456789);
}

TEST(message_group_commit_test, post_receive_throws) {
    message_group_commit msg(999);
    socket_io io("");
    EXPECT_THROW(msg.post_receive(io), std::logic_error);

}

}  // namespace limestone::testing
