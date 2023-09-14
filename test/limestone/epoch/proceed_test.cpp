
#include <atomic>

#include <unistd.h>
#include <stdlib.h>
#include <xmmintrin.h>
#include "test_root.h"

namespace limestone::testing {

constexpr const char* data_location = "/tmp/proceed_test/data_location";
constexpr const char* metadata_location = "/tmp/proceed_test/metadata_location";

class proceed_test : public ::testing::Test {
protected:
    virtual void SetUp() {
        if (system("rm -rf /tmp/proceed_test") != 0) {
            std::cerr << "cannot remove directory" << std::endl;
        }
        if (system("mkdir -p /tmp/proceed_test/data_location /tmp/proceed_test/metadata_location") != 0) {
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
        if (system("rm -rf /tmp/proceed_test") != 0) {
            std::cerr << "cannot remove directory" << std::endl;
        }
    }

    std::unique_ptr<limestone::api::datastore_test> datastore_{};
};

TEST_F(proceed_test, one_by_one) {
    datastore_->ready();

    for (std::size_t i = 1; i < 20; i++) {
        datastore_->switch_epoch(i);
                                 
        ASSERT_EQ(datastore_->last_epoch(), i - 1);
    }

    // cleanup
    datastore_->shutdown();
}

TEST_F(proceed_test, jump) {
    datastore_->ready();

    std::size_t e = 1;
    for (std::size_t i = 1; i < 20; i++) {
        datastore_->switch_epoch(e);
        ASSERT_EQ(datastore_->last_epoch(), e - 1);
        e += i;
    }

    // cleanup
    datastore_->shutdown();
}

}  // namespace limestone::testing
