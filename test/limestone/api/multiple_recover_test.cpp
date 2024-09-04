
#include <atomic>

#include <unistd.h>
#include <stdlib.h>
#include <unordered_map>
#include <xmmintrin.h>
#include "test_root.h"

namespace limestone::testing {

constexpr const char* data_location = "/tmp/multiple_recover_test/data_location";
constexpr const char* metadata_location = "/tmp/multiple_recover_test/metadata_location";

class multiple_recover_test : public ::testing::Test {
protected:
    virtual void SetUp() {}

    virtual void TearDown() {
        if (system("rm -rf /tmp/multiple_recover_test") != 0) {
            std::cerr << "cannot remove directory" << std::endl;
        }
    }
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

    // epoch 2
    datastore->switch_epoch(2);

    // log 0 entry
    channel.begin_session();
    channel.add_entry(0, "k0", "v0", {2, 0});
    channel.end_session();

    // epoch 3
    datastore->switch_epoch(3);

    // wait epoch 1's durable
    for (;;) {
        if (get_durable_epoch() >= 2) {
            break;
        }
        _mm_pause();
    }

    // cleanup
    datastore->shutdown();

    // recover and ready
    datastore->recover();
    datastore->ready();

    // epoch 4
    datastore->switch_epoch(4);

    // log 1 entry
    channel.begin_session();
    channel.add_entry(1, "k1", "v1", {4, 0});
    channel.end_session();

    // epoch 5
    datastore->switch_epoch(5);

    // wait epoch 5's durable
    for (;;) {
        if (get_durable_epoch() >= 4) {
            break;
        }
        _mm_pause();
    }

    // cleanup
    datastore->shutdown();

    // recover and ready
    datastore->recover();
    datastore->ready();

    // expectation, which is necessary because the order of data obtained from cursors
    // is different from the order in which they were put in.
    std::unordered_map<std::string, std::pair<limestone::api::storage_id_type, std::string>> expectation{ {"k0", {0, "v0"}}, {"k1", {1, "v1"}}, };

    // create snapshot
    auto ss = datastore->get_snapshot();
    auto cursor = ss->get_cursor();

    for(std::size_t i = 0; i< 2; i++) {
        ASSERT_TRUE(cursor->next());
        std::string buf{};
        cursor->key(buf);
        auto it = expectation.find(buf);
        ASSERT_NE(it,  expectation.end());
        cursor->value(buf);
        ASSERT_EQ(buf, it->second.second);
        ASSERT_EQ(cursor->storage(), it->second.first);
        expectation.erase(it);
    }
    ASSERT_TRUE(expectation.empty());
    ASSERT_FALSE(cursor->next()); // nothing

    // cleanup
    datastore->shutdown();
}

}  // namespace limestone::testing
