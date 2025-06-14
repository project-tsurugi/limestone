#include "replication/message_group_commit.h"
#include "replication/message_gc_boundary_switch.h"
#include "replication/replication_message.h"
#include "replication/handler_resources.h"
#include "gtest/gtest.h"

namespace limestone::testing {

using namespace limestone::replication;

TEST(message_gc_boundary_switch_test, round_trip) {
    message_gc_boundary_switch original(42);
    socket_io out("");
    replication_message::send(out, original);

    socket_io in(out.get_out_string());
    auto received_base = replication_message::receive(in);
    auto received = dynamic_cast<message_gc_boundary_switch*>(received_base.get());
    ASSERT_NE(received, nullptr);
    EXPECT_EQ(received->write_version(), 42);
}

TEST(message_gc_boundary_switch_test, post_receive_throws) {
    message_gc_boundary_switch msg(123);
    socket_io io("");
    handler_resources resources{io};
    EXPECT_THROW(msg.post_receive(resources), std::logic_error);
}

}  // namespace limestone::testing
