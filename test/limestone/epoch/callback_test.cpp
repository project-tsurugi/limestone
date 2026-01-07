
#include <atomic>

#include <unistd.h>
#include <stdlib.h>
#include <xmmintrin.h>
#include "test_root.h"

namespace limestone::testing {

constexpr const char* data_location = "/tmp/callback_test/data_location";

class callback_test : public ::testing::Test {
protected:
    virtual void SetUp() {
        if (system("rm -rf /tmp/callback_test") != 0) {
            std::cerr << "cannot remove directory" << std::endl;
        }
        if (system("mkdir -p /tmp/callback_test/data_location") != 0) {
            std::cerr << "cannot make directory" << std::endl;
        }

        limestone::api::configuration conf{};
        conf.set_data_location(data_location);

        datastore_ = std::make_unique<limestone::api::datastore_test>(conf);
    }

    virtual void TearDown() {
        datastore_ = nullptr;
        if (system("rm -rf /tmp/callback_test") != 0) {
            std::cerr << "cannot remove directory" << std::endl;
        }
    }

    std::unique_ptr<limestone::api::datastore_test> datastore_{};
};

static limestone::api::epoch_id_type epoch_notified;
static std::size_t count;

void callback(limestone::api::epoch_id_type e) {
    epoch_notified = e;
    count++;
}


TEST_F(callback_test, one_log_channel) {
    limestone::api::log_channel& channel = datastore_->create_channel();
    
    datastore_->add_persistent_callback(callback);
    datastore_->ready();

    std::size_t initial_count = count;

    datastore_->switch_epoch(2);
    ASSERT_EQ(epoch_notified, datastore_->last_epoch());
    ASSERT_EQ(count, initial_count + 1);

    datastore_->switch_epoch(3);
    ASSERT_EQ(epoch_notified, datastore_->last_epoch());
    ASSERT_EQ(count, initial_count + 2);

    channel.begin_session();
    ASSERT_EQ(epoch_notified, datastore_->last_epoch());
    ASSERT_EQ(count, initial_count + 2);

    datastore_->switch_epoch(4);
    ASSERT_EQ(epoch_notified, datastore_->last_epoch());
    ASSERT_EQ(count, initial_count + 2);
    
    datastore_->switch_epoch(5);
    ASSERT_EQ(epoch_notified, datastore_->last_epoch());
    ASSERT_EQ(count, initial_count + 2);

    channel.end_session();
    ASSERT_EQ(epoch_notified, datastore_->last_epoch());
    ASSERT_EQ(count, initial_count + 3);
    
    datastore_->switch_epoch(6);
    ASSERT_EQ(epoch_notified, datastore_->last_epoch());
    ASSERT_EQ(count, initial_count + 4);

    // cleanup
    datastore_->shutdown();
}

TEST_F(callback_test, log_channels) {
    limestone::api::log_channel& channel1 = datastore_->create_channel();
    limestone::api::log_channel& channel2 = datastore_->create_channel();
    
    datastore_->add_persistent_callback(callback);
    datastore_->ready();

    std::size_t initial_count = count;

    datastore_->switch_epoch(2);
    ASSERT_EQ(epoch_notified, datastore_->last_epoch());
    ASSERT_EQ(count, initial_count + 1);

    channel1.begin_session();
    ASSERT_EQ(epoch_notified, datastore_->last_epoch());
    ASSERT_EQ(count, initial_count + 1);

    datastore_->switch_epoch(3);
    ASSERT_EQ(epoch_notified, datastore_->last_epoch());
    ASSERT_EQ(count, initial_count + 1);
    
    datastore_->switch_epoch(4);
    ASSERT_EQ(epoch_notified, datastore_->last_epoch());
    ASSERT_EQ(count, initial_count + 1);
    
    channel2.begin_session();
    ASSERT_EQ(epoch_notified, datastore_->last_epoch());
    ASSERT_EQ(count, initial_count + 1);

    datastore_->switch_epoch(5);
    ASSERT_EQ(epoch_notified, datastore_->last_epoch());
    ASSERT_EQ(count, initial_count + 1);
    
    datastore_->switch_epoch(6);
    ASSERT_EQ(epoch_notified, datastore_->last_epoch());
    ASSERT_EQ(count, initial_count + 1);

    channel1.end_session();
    ASSERT_EQ(epoch_notified, datastore_->last_epoch());
    ASSERT_EQ(count, initial_count + 2);

    datastore_->switch_epoch(7);
    ASSERT_EQ(epoch_notified, datastore_->last_epoch());
    ASSERT_EQ(count, initial_count + 2);
    
    datastore_->switch_epoch(8);
    ASSERT_EQ(epoch_notified, datastore_->last_epoch());
    ASSERT_EQ(count, initial_count + 2);
    
    channel2.end_session();
    ASSERT_EQ(epoch_notified, datastore_->last_epoch());
    ASSERT_EQ(count, initial_count + 3);

    datastore_->switch_epoch(9);
    ASSERT_EQ(epoch_notified, datastore_->last_epoch());
    ASSERT_EQ(count, initial_count + 4);
    
    datastore_->switch_epoch(10);
    ASSERT_EQ(epoch_notified, datastore_->last_epoch());
    ASSERT_EQ(count, initial_count + 5);

    // cleanup
    datastore_->shutdown();
}

}  // namespace limestone::testing
