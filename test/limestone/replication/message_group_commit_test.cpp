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
    socket_io out("");
    replication_message::send(out, original);

    socket_io in(out.get_out_string());
    auto received_base = replication_message::receive(in);
    auto received = dynamic_cast<message_group_commit*>(received_base.get());
    ASSERT_NE(received, nullptr);
    EXPECT_EQ(received->epoch_number(), 123456789);
}

TEST_F(message_group_commit_test, post_receive) {
    datastore_->switch_epoch(1);
    EXPECT_EQ(datastore_->epoch_id_switched(), 1);
    message_group_commit msg(999);
    socket_io io("");
    control_channel_handler_resources resources(io, *datastore_);
    msg.post_receive(resources);
    EXPECT_EQ(get_epoch(base_location), 999);
}

}  // namespace limestone::testing
