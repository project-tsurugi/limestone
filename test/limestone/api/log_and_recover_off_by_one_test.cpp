
#include <atomic>

#include <unistd.h>
#include <stdlib.h>
#include <unordered_map>
#include <xmmintrin.h>
#include "test_root.h"

namespace limestone::testing {

constexpr const char* data_location = "/tmp/log_and_recover_off_by_one_test/data_location";
constexpr const char* metadata_location = "/tmp/log_and_recover_off_by_one_test/metadata_location";

class log_and_recover_off_by_one_test : public ::testing::Test {
public:
    virtual void SetUp() {}

    virtual void TearDown() {
        datastore_ = nullptr;
        if (system("rm -rf /tmp/log_and_recover_off_by_one_test") != 0) {
            std::cerr << "cannot remove directory" << std::endl;
        }
    }

protected:
    std::unique_ptr<limestone::api::datastore_test> datastore_{};
};

TEST_F(log_and_recover_off_by_one_test, log_and_recovery) {
    if (system("rm -rf /tmp/log_and_recover_off_by_one_test") != 0) {
        std::cerr << "cannot remove directory" << std::endl;
    }
    if (system("mkdir -p /tmp/log_and_recover_off_by_one_test/data_location /tmp/log_and_recover_off_by_one_test/metadata_location") != 0) {
        std::cerr << "cannot make directory" << std::endl;
    }

    std::vector<boost::filesystem::path> data_locations{};
    data_locations.emplace_back(data_location);
    boost::filesystem::path metadata_location_path{metadata_location};
    limestone::api::configuration conf(data_locations, metadata_location_path);

    datastore_ = std::make_unique<limestone::api::datastore_test>(conf);

    limestone::api::log_channel& channel = datastore_->create_channel(boost::filesystem::path(data_location));

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
    channel.add_entry(st, k, v, {0, 0});
    channel.add_entry(st, "", "", {0, 0});
    channel.end_session();

    // epoch 2
    datastore_->switch_epoch(2);

    // expectation, which is necessary because the order of data obtained from cursors
    // is different from the order in which they were put in.
    std::unordered_map<std::string, std::string> expectation{ {"k", "v"}, {"", ""}, };

    // wait epoch 1's durable
    for (;;) {
        if (get_durable_epoch() >= 1) {
            break;
        }
        _mm_pause();
    }

    // cleanup
    datastore_->shutdown();

    // recover and ready
    datastore_->recover();
    datastore_->ready();

    // create snapshot
    auto ss = datastore_->get_snapshot();
    auto cursor = ss->get_cursor();
    ASSERT_TRUE(cursor->next());
    std::string buf{};

    cursor->key(buf);
    auto it1 = expectation.find(buf);
    ASSERT_NE(it1, expectation.end());
    cursor->value(buf);
    ASSERT_EQ(buf, it1->second);
    ASSERT_EQ(cursor->storage(), st);
    expectation.erase(it1);
    ASSERT_TRUE(cursor->next());

    cursor->key(buf);
    auto it2 = expectation.find(buf);
    ASSERT_NE(it2, expectation.end());
    cursor->value(buf);
    ASSERT_EQ(buf, it2->second);
    ASSERT_EQ(cursor->storage(), st);
    expectation.erase(it2);
    ASSERT_FALSE(cursor->next());

    ASSERT_TRUE(expectation.empty());
    
    // cleanup
    datastore_->shutdown();
}

}  // namespace limestone::testing
