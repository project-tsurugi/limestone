
#include <atomic>

#include <unistd.h>
#include <stdlib.h>
#include <xmmintrin.h>
#include "test_root.h"

namespace limestone::testing {

constexpr const char* data_location = "/tmp/log_and_recover_test/data_location";

class log_and_recover_test : public ::testing::Test {
protected:
    virtual void SetUp() {
        if (system("rm -rf /tmp/log_and_recover_test") != 0) {
            std::cerr << "cannot remove directory" << std::endl;
        }
        if (system("mkdir -p /tmp/log_and_recover_test/data_location") != 0) {
            std::cerr << "cannot make directory" << std::endl;
        }

        limestone::api::configuration conf{};
        conf.set_data_location(data_location);

        datastore_ = std::make_unique<limestone::api::datastore_test>(conf);

        limestone::api::log_channel& channel = datastore_->create_channel();

        // prepare durable epoch
        std::atomic<std::size_t> durable_epoch{0};
        auto set_durable_epoch = [&durable_epoch](std::size_t n) {
                                     durable_epoch.store(n, std::memory_order_release);
                                 };
        auto get_durable_epoch = [&durable_epoch]() {
                                     return durable_epoch.load(std::memory_order_acquire);
                                 };

        // register persistent callback
        datastore_->add_persistent_callback(set_durable_epoch);

        // epoch 1
        datastore_->switch_epoch(1);

        // ready
        datastore_->ready();

        // log 1 entry
        channel.begin_session();
        std::string k{"k"};
        std::string v{"v"};
        limestone::api::storage_id_type st{2};
        channel.add_entry(st, k, v, {1, 0});
        channel.end_session();

        // epoch 2
        datastore_->switch_epoch(2);

        // wait epoch 1's durable
        for (;;) {
            if (get_durable_epoch() >= 1) {
                break;
            }
            _mm_pause();
        }

        // cleanup
        datastore_->shutdown();

        // second recovery
        datastore_->recover();
        datastore_->ready();
        datastore_->switch_epoch(3); // trigger of flush log record which belongs to epoch 2.
        limestone::api::log_channel& channel2 = datastore_->create_channel();
        channel2.begin_session();
        channel2.add_entry(st, "k", "v2", {2, 0}); // epoch 2 log record
        channel2.end_session(); // (*1)
        datastore_->switch_epoch(4);
        // wait durable (*1)
        for (;;) {
            if (get_durable_epoch() >= 3) {
                break;
            }
            _mm_pause();
        }

        // cleanup
        datastore_->shutdown();
    }

    virtual void TearDown() {
        datastore_ = nullptr;
        if (system("rm -rf /tmp/log_and_recover_test") != 0) {
            std::cerr << "cannot remove directory" << std::endl;
        }
    }

protected:
    std::unique_ptr<limestone::api::datastore_test> datastore_{};
};

TEST_F(log_and_recover_test, recovery) {
    // recover and ready
    datastore_->recover();
    datastore_->ready();

    // create snapshot
    auto ss = datastore_->get_snapshot();
    auto cursor = ss->get_cursor();
    ASSERT_TRUE(cursor->next()); // point first
    std::string buf{};
    cursor->key(buf);
    ASSERT_EQ(buf, "k");
    cursor->value(buf);
    ASSERT_EQ(buf, "v2");
    ASSERT_EQ(cursor->storage(), 2);
    ASSERT_FALSE(cursor->next()); // nothing

    // cleanup
    datastore_->shutdown();
}

TEST_F(log_and_recover_test, recovery_interrupt_datastore_object_reallocation) { // NOLINT
    LOG(INFO);
    limestone::api::configuration conf{};
    conf.set_data_location(data_location);

    datastore_ = std::make_unique<limestone::api::datastore_test>(conf);

    // recover and ready
    datastore_->recover();
    datastore_->ready();

    // create snapshot
    auto ss = datastore_->get_snapshot();
    auto cursor = ss->get_cursor();
    ASSERT_TRUE(cursor->next()); // point first
    std::string buf{};
    cursor->key(buf);
    ASSERT_EQ(buf, "k");
    cursor->value(buf);
    ASSERT_EQ(buf, "v2");
    ASSERT_EQ(cursor->storage(), 2);
    ASSERT_FALSE(cursor->next()); // nothing

    // cleanup
    datastore_->shutdown();
}

}  // namespace limestone::testing
