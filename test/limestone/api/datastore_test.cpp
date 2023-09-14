
#include <atomic>

#include <unistd.h>
#include <stdlib.h>
#include <xmmintrin.h>
#include "test_root.h"

namespace limestone::testing {

constexpr const char* data_location = "/tmp/datastore_test/data_location";
constexpr const char* metadata_location = "/tmp/datastore_test/metadata_location";

class datastore_test : public ::testing::Test {
public:
    static inline std::atomic<std::size_t> durable_epoch_{0};

    virtual void SetUp() {
        // initialize
        set_durable_epoch(0);
    }

    virtual void TearDown() {
        datastore_ = nullptr;
        if (system("rm -rf /tmp/datastore_test") != 0) {
            std::cerr << "cannot remove directory" << std::endl;
        }
    }

    static std::size_t get_durable_epoch() {
         return durable_epoch_.load(std::memory_order_acquire);
    }

    static void set_durable_epoch(std::size_t n) {
        durable_epoch_.store(n, std::memory_order_release);
    }

protected:
    std::unique_ptr<limestone::api::datastore_test> datastore_{};
};

TEST_F(datastore_test, add_persistent_callback_test) { // NOLINT
    FLAGS_stderrthreshold = 0;

    if (system("rm -rf /tmp/datastore_test") != 0) {
        std::cerr << "cannot remove directory" << std::endl;
    }
    if (system("mkdir -p /tmp/datastore_test/data_location /tmp/datastore_test/metadata_location") != 0) {
        std::cerr << "cannot make directory" << std::endl;
    }

    std::vector<std::filesystem::path> data_locations{};
    data_locations.emplace_back(data_location);
    std::filesystem::path metadata_location_path{metadata_location};
    limestone::api::configuration conf(data_locations, metadata_location_path);

    datastore_ = std::make_unique<limestone::api::datastore_test>(conf);

    // register persistent callback
    datastore_->add_persistent_callback(set_durable_epoch);

    // epoch 1
    datastore_->switch_epoch(1);

    // ready
    datastore_->ready();

    // epoch 2
    datastore_->switch_epoch(2);

    for (;;) {
        if (get_durable_epoch() >= 1) {
            break;
        }
        _mm_pause();
#if 1
        // todo remove this block. now, infinite loop at this.
        LOG(INFO);
        sleep(1);
#endif
    }

    // epoch 3
    datastore_->switch_epoch(3);

    for (;;) {
        if (get_durable_epoch() >= 2) {
            break;
        }
        _mm_pause();
    }

}

}  // namespace limestone::testing
