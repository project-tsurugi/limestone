
#include <atomic>

#include <unistd.h>
#include <stdlib.h>
#include <xmmintrin.h>
#include "test_root.h"
#include "internal.h"

namespace limestone::testing {

constexpr const char* data_location = "/tmp/datastore_test/data_location";
constexpr const char* metadata_location = "/tmp/datastore_test/metadata_location";
constexpr const char* parent_directory = "/tmp/datastore_test";

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

    void verify_datastore_initialization(const boost::filesystem::path& data_location_path) {
        std::set<boost::filesystem::path> actual_files;
        actual_files.insert(data_location_path / "compaction_catalog");
        actual_files.insert(data_location_path / "epoch");
        actual_files.insert(data_location_path / "limestone-manifest.json");

        for (const auto& file : actual_files) {
            EXPECT_TRUE(boost::filesystem::exists(file)) << "Expected file not found: " << file.filename();
        }

        // Verify the content of limestone-manifest.json
        boost::filesystem::path manifest_path = data_location_path / "limestone-manifest.json";
        std::ifstream manifest_file(manifest_path.string());
        ASSERT_TRUE(manifest_file.is_open()) << "Unable to open limestone-manifest.json";

        std::stringstream buffer;
        buffer << manifest_file.rdbuf();
        std::string manifest_content = buffer.str();

        std::string expected_content = R"({
    "format_version": "1.0",
    "persistent_format_version": 2
})";

        EXPECT_EQ(manifest_content, expected_content) << "limestone-manifest.json content does not match expected content";

        // Verify that no unexpected files were created
        for (const auto& entry : boost::filesystem::directory_iterator(data_location_path)) {
            EXPECT_TRUE(actual_files.find(entry.path()) != actual_files.end()) << "Unexpected file found: " << entry.path().filename();
        }

        std::set<boost::filesystem::path> files = datastore_->files();
        EXPECT_EQ(files.size(), 3);
        for (const auto& file : files) {
            EXPECT_TRUE(boost::filesystem::exists(file)) << "Expected file not found: " << file.filename();
            std::cerr << "file: " << file << std::endl;
        }
    }
};

TEST_F(datastore_test, add_persistent_callback_test) { // NOLINT
    FLAGS_stderrthreshold = 0;

    if (system("rm -rf /tmp/datastore_test") != 0) {
        std::cerr << "cannot remove directory" << std::endl;
    }
    if (system("mkdir -p /tmp/datastore_test/data_location /tmp/datastore_test/metadata_location") != 0) {
        std::cerr << "cannot make directory" << std::endl;
    }

    std::vector<boost::filesystem::path> data_locations{};
    data_locations.emplace_back(data_location);
    boost::filesystem::path metadata_location_path{metadata_location};
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

TEST_F(datastore_test, prevent_double_start_test) { // NOLINT
    if (system("rm -rf /tmp/datastore_test") != 0) {
        std::cerr << "cannot remove directory" << std::endl;
    }
    if (system("mkdir -p /tmp/datastore_test/data_location /tmp/datastore_test/metadata_location") != 0) {
        std::cerr << "cannot make directory" << std::endl;
    }

    std::vector<boost::filesystem::path> data_locations{};
    data_locations.emplace_back(data_location);
    boost::filesystem::path metadata_location_path{metadata_location};
    limestone::api::configuration conf(data_locations, metadata_location_path);

    auto ds1 = std::make_unique<limestone::api::datastore_test>(conf);
    ds1->ready();

    // another process is using the log directory
    ASSERT_DEATH({
            auto ds2 = std::make_unique<limestone::api::datastore_test>(conf);
    }, "another process is using the log directory: /tmp/datastore_test/data_location.");

    // Ather datastore is created after the first one is destroyed
    ds1->shutdown();
    auto ds3 = std::make_unique<limestone::api::datastore_test>(conf);
    ds3->ready();
    ds3->shutdown();
}

}  // namespace limestone::testing
