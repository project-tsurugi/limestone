
#include <atomic>

#include <unistd.h>
#include <stdlib.h>
#include <xmmintrin.h>
#include "test_root.h"

namespace limestone::testing {

constexpr const char* data_location = "/tmp/with_log_channel_test/data_location";
constexpr const char* metadata_location = "/tmp/with_log_channel_test/metadata_location";

class with_log_channel_test : public ::testing::Test {
protected:
    virtual void SetUp() {
        if (system("rm -rf /tmp/with_log_channel_test") != 0) {
            std::cerr << "cannot remove directory" << std::endl;
        }
        if (system("mkdir -p /tmp/with_log_channel_test/data_location /tmp/with_log_channel_test/metadata_location") != 0) {
            std::cerr << "cannot make directory" << std::endl;
        }

        std::vector<std::filesystem::path> data_locations{};
        data_locations.emplace_back(data_location);
        std::filesystem::path metadata_location_path{metadata_location};
        limestone::api::configuration conf(data_locations, metadata_location_path);

        datastore_ = std::make_unique<limestone::api::datastore_test>(conf);
    }

    virtual void TearDown() {
        datastore_ = nullptr;
        if (system("rm -rf /tmp/with_log_channel_test") != 0) {
            std::cerr << "cannot remove directory" << std::endl;
        }
    }

    std::unique_ptr<limestone::api::datastore_test> datastore_{};
};

TEST_F(with_log_channel_test, one_log_channel) {
    limestone::api::log_channel& channel = datastore_->create_channel(std::filesystem::path(data_location));
    
    datastore_->ready();

    datastore_->switch_epoch(1);
    ASSERT_EQ(datastore_->last_epoch(), 0);

    datastore_->switch_epoch(2);
    ASSERT_EQ(datastore_->last_epoch(), 1);

    channel.begin_session();

    datastore_->switch_epoch(3);
    ASSERT_EQ(datastore_->last_epoch(), 1);
    
    datastore_->switch_epoch(4);
    ASSERT_EQ(datastore_->last_epoch(), 1);

    channel.end_session();
    ASSERT_EQ(datastore_->last_epoch(), 3);
    
    datastore_->switch_epoch(5);
    ASSERT_EQ(datastore_->last_epoch(), 4);

    // cleanup
    datastore_->shutdown();
}

TEST_F(with_log_channel_test, log_channels) {
    limestone::api::log_channel& channel1 = datastore_->create_channel(std::filesystem::path(data_location));
    limestone::api::log_channel& channel2 = datastore_->create_channel(std::filesystem::path(data_location));
    
    datastore_->ready();

    datastore_->switch_epoch(1);
    ASSERT_EQ(datastore_->last_epoch(), 0);

    datastore_->switch_epoch(2);
    ASSERT_EQ(datastore_->last_epoch(), 1);

    channel1.begin_session();

    datastore_->switch_epoch(3);
    ASSERT_EQ(datastore_->last_epoch(), 1);
    
    datastore_->switch_epoch(4);
    ASSERT_EQ(datastore_->last_epoch(), 1);
    
    channel2.begin_session();

    datastore_->switch_epoch(5);
    ASSERT_EQ(datastore_->last_epoch(), 1);
    
    datastore_->switch_epoch(6);
    ASSERT_EQ(datastore_->last_epoch(), 1);

    channel1.end_session();
    ASSERT_EQ(datastore_->last_epoch(), 3);

    datastore_->switch_epoch(7);
    ASSERT_EQ(datastore_->last_epoch(), 3);
    
    datastore_->switch_epoch(8);
    ASSERT_EQ(datastore_->last_epoch(), 3);
    
    channel2.end_session();
    ASSERT_EQ(datastore_->last_epoch(), 7);

    datastore_->switch_epoch(9);
    ASSERT_EQ(datastore_->last_epoch(), 8);
    
    datastore_->switch_epoch(10);
    ASSERT_EQ(datastore_->last_epoch(), 9);

    // cleanup
    datastore_->shutdown();
}

}  // namespace limestone::testing
