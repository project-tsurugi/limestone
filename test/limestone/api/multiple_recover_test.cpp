
#include <atomic>

#include <unistd.h>
#include <stdlib.h>
#include <xmmintrin.h>
#include "test_root.h"

namespace limestone::testing {

constexpr const char* data_location = "/tmp/multiple_recover_test/data_location";
constexpr const char* metadata_location = "/tmp/multiple_recover_test/metadata_location";

class multiple_recover_test : public ::testing::Test {
protected:
    virtual void SetUp() {}

    virtual void TearDown() {}
};

TEST_F(multiple_recover_test, two_recovery) {
    if (system("rm -rf /tmp/multiple_recover_test") != 0) {
        std::cerr << "cannot remove directory" << std::endl;
    }
    if (system("mkdir -p /tmp/multiple_recover_test/data_location /tmp/multiple_recover_test/metadata_location") != 0) {
        std::cerr << "cannot make directory" << std::endl;
    }

    std::unique_ptr<limestone::api::datastore_test> datastore{};
    std::vector<boost::filesystem::path> data_locations{};
    data_locations.emplace_back(data_location);
    boost::filesystem::path metadata_location_path{metadata_location};
    limestone::api::configuration conf(data_locations, metadata_location_path);

    datastore = std::make_unique<limestone::api::datastore_test>(conf);

    limestone::api::log_channel& channel = datastore->create_channel(boost::filesystem::path(data_location));

    // prepare durable epoch
    std::atomic<std::size_t> durable_epoch{0};
    auto set_durable_epoch = [&durable_epoch](std::size_t n) {
                                 durable_epoch.store(n, std::memory_order_release);
                             };
    auto get_durable_epoch = [&durable_epoch]() {
                                 return durable_epoch.load(std::memory_order_acquire);
                             };

    // register persistent callback
    datastore->add_persistent_callback(set_durable_epoch);

    // ready
    datastore->ready();

    // epoch 1
    datastore->switch_epoch(1);

    // log 1 entry
    channel.begin_session();
    channel.add_entry(0, "", "", {1, 0});
    channel.end_session();

    // epoch 2
    datastore->switch_epoch(2);

    // wait epoch 1's durable
    for (;;) {
        if (get_durable_epoch() >= 1) {
            break;
        }
        _mm_pause();
    }

    // cleanup
    datastore->shutdown();

    // recover and ready
    datastore->recover();
    datastore->ready();

    // epoch 1
    datastore->switch_epoch(1);

    // log 1 entry
    channel.begin_session();
    channel.add_entry(1, "", "", {1, 0});
    channel.end_session();

    // epoch 2
    datastore->switch_epoch(2);

    // wait epoch 1's durable
    for (;;) {
        if (get_durable_epoch() >= 1) {
            break;
        }
        _mm_pause();
    }

    // cleanup
    datastore->shutdown();

    // recover and ready
    datastore->recover();
    datastore->ready();

    // create snapshot
    limestone::api::snapshot* ss{datastore->get_snapshot()};
    ASSERT_TRUE(ss->get_cursor().next()); // point first
    std::string buf{};
    ss->get_cursor().key(buf);
    ASSERT_EQ(buf, "");
    ss->get_cursor().value(buf);
    ASSERT_EQ(buf, "");
    ASSERT_EQ(ss->get_cursor().storage(), 0);
    ASSERT_TRUE(ss->get_cursor().next());
    ss->get_cursor().key(buf);
    ASSERT_EQ(buf, "");
    ss->get_cursor().value(buf);
    ASSERT_EQ(buf, "");
    ASSERT_EQ(ss->get_cursor().storage(), 1);
    ASSERT_FALSE(ss->get_cursor().next()); // nothing

    // cleanup
    datastore->shutdown();
}

}  // namespace limestone::testing