
#include <atomic>

#include <unistd.h>
#include <stdlib.h>
#include <xmmintrin.h>
#include "test_root.h"

namespace limestone::testing {

constexpr const char* data_location = "/tmp/finish_soon_test/data_location";
constexpr const char* metadata_location = "/tmp/finish_soon_test/metadata_location";

class finish_soon_test : public ::testing::Test {
protected:
    virtual void SetUp() {
        if (system("rm -rf /tmp/finish_soon_test") != 0) {
            std::cerr << "cannot remove directory" << std::endl;
        }
        if (system("mkdir -p /tmp/finish_soon_test/data_location /tmp/finish_soon_test/metadata_location") != 0) {
            std::cerr << "cannot make directory" << std::endl;
        }

        std::vector<boost::filesystem::path> data_locations{};
        data_locations.emplace_back(data_location);
        boost::filesystem::path metadata_location_path{metadata_location};
        limestone::api::configuration conf(data_locations, metadata_location_path);

        datastore_ = std::make_unique<limestone::api::datastore_test>(conf);
    }

    virtual void TearDown() {
        datastore_ = nullptr;
        if (system("rm -rf /tmp/finish_soon_test") != 0) {
            std::cerr << "cannot remove directory" << std::endl;
        }
    }

    std::unique_ptr<limestone::api::datastore_test> datastore_{};
};

TEST_F(finish_soon_test, same) {
    limestone::api::log_channel& channel = datastore_->create_channel(boost::filesystem::path(data_location));
    
    datastore_->ready();

    datastore_->switch_epoch(2);
    ASSERT_EQ(1, datastore_->epoch_id_informed());
    ASSERT_EQ(0, datastore_->epoch_id_to_be_recorded());

    datastore_->switch_epoch(3);
    ASSERT_EQ(2, datastore_->epoch_id_informed());
    ASSERT_EQ(0, datastore_->epoch_id_to_be_recorded());

    channel.begin_session();
    channel.end_session();
    
    ASSERT_EQ(2, datastore_->epoch_id_informed());
    ASSERT_EQ(2, datastore_->epoch_id_to_be_recorded());

    datastore_->switch_epoch(4);
    ASSERT_EQ(3, datastore_->epoch_id_informed());
    ASSERT_EQ(3, datastore_->epoch_id_to_be_recorded());
    
    datastore_->switch_epoch(5);
    ASSERT_EQ(4, datastore_->epoch_id_informed());
    ASSERT_EQ(3, datastore_->epoch_id_to_be_recorded());

    // cleanup
    datastore_->shutdown();
}

TEST_F(finish_soon_test, different) {
    limestone::api::log_channel& channel = datastore_->create_channel(boost::filesystem::path(data_location));
    
    datastore_->ready();

    datastore_->switch_epoch(2);
    ASSERT_EQ(1, datastore_->epoch_id_informed());
    ASSERT_EQ(0, datastore_->epoch_id_to_be_recorded());

    datastore_->switch_epoch(3);
    ASSERT_EQ(2, datastore_->epoch_id_informed());
    ASSERT_EQ(0, datastore_->epoch_id_to_be_recorded());

    channel.begin_session();
    ASSERT_EQ(2, datastore_->epoch_id_informed());
    ASSERT_EQ(0, datastore_->epoch_id_to_be_recorded());

    datastore_->switch_epoch(4);
    ASSERT_EQ(2, datastore_->epoch_id_informed());
    ASSERT_EQ(0, datastore_->epoch_id_to_be_recorded());
    
    channel.end_session();
    ASSERT_EQ(3, datastore_->epoch_id_informed());
    ASSERT_EQ(3, datastore_->epoch_id_to_be_recorded());

    datastore_->switch_epoch(5);
    ASSERT_EQ(4, datastore_->epoch_id_informed());
    ASSERT_EQ(3, datastore_->epoch_id_to_be_recorded());

    // cleanup
    datastore_->shutdown();
}

}  // namespace limestone::testing
