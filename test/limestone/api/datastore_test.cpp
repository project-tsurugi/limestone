
#include <atomic>
#include <filesystem>
#include <fstream>
#include <sstream>

#include <unistd.h>
#include <stdlib.h>
#include <xmmintrin.h>
#include "test_root.h"
#include "internal.h"
#include "datastore_impl.h"
#include <limestone/api/limestone_exception.h>

#ifdef ENABLE_ALTIMETER
#include <altimeter/configuration.h>
#include <altimeter/event/constants.h>
#include <altimeter/logger.h>
#endif

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
        unsetenv("TSURUGI_REPLICATION_ENDPOINT");
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

#ifdef ENABLE_ALTIMETER
namespace {
class altimeter_test_logger {
public:
    explicit altimeter_test_logger(std::string const& directory) : directory_(directory) {
        altimeter::configuration event_log_cfg;
        event_log_cfg.category(altimeter::event::category);
        event_log_cfg.output(true);
        event_log_cfg.directory(directory_);
        event_log_cfg.level(altimeter::event::level::log_data_store);
        event_log_cfg.file_number(1);
        event_log_cfg.sync(true);
        event_log_cfg.buffer_size(0);
        event_log_cfg.flush_interval(0);
        event_log_cfg.flush_file_size(0);
        event_log_cfg.max_file_size(1024 * 1024);
        configs_.push_back(std::move(event_log_cfg));
        altimeter::logger::start(configs_);
    }

    ~altimeter_test_logger() { altimeter::logger::shutdown(); }

private:
    std::string directory_;
    std::vector<altimeter::configuration> configs_{};
};

[[nodiscard]] std::string read_event_log_contents(std::string const& directory) {
    namespace fs = std::filesystem;
    if (!fs::exists(directory)) {
        return {};
    }
    std::string contents;
    for (const auto& entry : fs::directory_iterator(directory)) {
        if (!entry.is_regular_file()) {
            continue;
        }
        const auto filename = entry.path().filename().string();
        if (filename.rfind("event_", 0) != 0 || entry.path().extension() != ".log") {
            continue;
        }
        std::ifstream file(entry.path());
        if (!file.is_open()) {
            continue;
        }
        std::stringstream buffer;
        buffer << file.rdbuf();
        contents += buffer.str();
    }
    return contents;
}
} // namespace
#endif

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

TEST_F(datastore_test, remove_persistent_callback_test) { // NOLINT
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

    datastore_->add_persistent_callback(set_durable_epoch);

    datastore_->switch_epoch(1);
    datastore_->ready();

    datastore_->switch_epoch(2);
    EXPECT_EQ(1U, get_durable_epoch());

    datastore_->switch_epoch(3);
    EXPECT_EQ(2U, get_durable_epoch());

    datastore_->remove_persistent_callback();
    auto previous_epoch = get_durable_epoch();

    datastore_->switch_epoch(previous_epoch + 1);
    // NOTE: add_persistent_callback_test polls because the API contract does not guarantee
    // switch_epoch() completes after invoking the callback. In this test we must ensure the
    // callback is not invoked after remove_persistent_callback(), which is difficult to prove
    // under an asynchronous implementation. Therefore, the expectations below intentionally rely
    // on the current implementation (and test setup) where switch_epoch() synchronously runs the
    // callback before returning. If that behavior changes, this test will stop being valid.
    EXPECT_EQ(previous_epoch, get_durable_epoch());
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
    ds1->wait_for_blob_file_garbace_collector();

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

TEST_F(datastore_test, datastore_impl_identity_fields_are_set) { // NOLINT
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
    conf.set_instance_id("instance-001");
    conf.set_db_name("db-alpha");

    datastore_ = std::make_unique<limestone::api::datastore_test>(conf);
    auto* impl = datastore_->get_impl();

    ASSERT_NE(impl, nullptr);
    EXPECT_EQ(impl->instance_id(), "instance-001");
    EXPECT_EQ(impl->db_name(), "db-alpha");
    EXPECT_EQ(impl->pid(), ::getpid());
}

#ifdef ENABLE_ALTIMETER
TEST_F(datastore_test, altimeter_wal_stored_log_written) { // NOLINT
    unsetenv("TSURUGI_REPLICATION_ENDPOINT");
    if (system("rm -rf /tmp/datastore_test") != 0) {
        std::cerr << "cannot remove directory" << std::endl;
    }
    if (system("mkdir -p /tmp/datastore_test/data_location /tmp/datastore_test/metadata_location /tmp/datastore_test/altimeter_log") != 0) {
        std::cerr << "cannot make directory" << std::endl;
    }

    std::vector<boost::filesystem::path> data_locations{};
    data_locations.emplace_back(data_location);
    boost::filesystem::path metadata_location_path{metadata_location};
    limestone::api::configuration conf(data_locations, metadata_location_path);
    conf.set_instance_id("instance-001");
    conf.set_db_name("db-alpha");

    std::string contents;
    {
        altimeter_test_logger logger("/tmp/datastore_test/altimeter_log");
        datastore_ = std::make_unique<limestone::api::datastore_test>(conf);
        datastore_->persist_and_propagate_epoch_id(42);
    }
    contents = read_event_log_contents("/tmp/datastore_test/altimeter_log");

    EXPECT_NE(contents.find("type:wal_stored"), std::string::npos);
    EXPECT_NE(contents.find("wal_version:42"), std::string::npos);
    EXPECT_NE(contents.find("result:1"), std::string::npos);
    EXPECT_NE(contents.find("instance_id:instance-001"), std::string::npos);
    EXPECT_NE(contents.find("dbname:db-alpha"), std::string::npos);
}

TEST_F(datastore_test, altimeter_wal_stored_log_failure_written) { // NOLINT
    unsetenv("TSURUGI_REPLICATION_ENDPOINT");
    if (system("rm -rf /tmp/datastore_test") != 0) {
        std::cerr << "cannot remove directory" << std::endl;
    }
    if (system("mkdir -p /tmp/datastore_test/data_location /tmp/datastore_test/metadata_location /tmp/datastore_test/altimeter_log") != 0) {
        std::cerr << "cannot make directory" << std::endl;
    }

    std::vector<boost::filesystem::path> data_locations{};
    data_locations.emplace_back(data_location);
    boost::filesystem::path metadata_location_path{metadata_location};
    limestone::api::configuration conf(data_locations, metadata_location_path);
    conf.set_instance_id("instance-001");
    conf.set_db_name("db-alpha");

    std::string contents;
    {
        altimeter_test_logger logger("/tmp/datastore_test/altimeter_log");
        datastore_ = std::make_unique<limestone::api::datastore_test>(conf);
        if (system("rm -rf /tmp/datastore_test/data_location") != 0) {
            std::cerr << "cannot remove data_location directory" << std::endl;
        }
        EXPECT_THROW(datastore_->persist_and_propagate_epoch_id(42), limestone::api::limestone_io_exception);
    }
    contents = read_event_log_contents("/tmp/datastore_test/altimeter_log");

    EXPECT_NE(contents.find("type:wal_stored"), std::string::npos);
    EXPECT_NE(contents.find("wal_version:42"), std::string::npos);
    EXPECT_NE(contents.find("result:2"), std::string::npos);
    EXPECT_NE(contents.find("instance_id:instance-001"), std::string::npos);
    EXPECT_NE(contents.find("dbname:db-alpha"), std::string::npos);
}

TEST_F(datastore_test, altimeter_wal_shipped_log_written) { // NOLINT
    setenv("TSURUGI_REPLICATION_ENDPOINT", "tcp://127.0.0.1:12345", 1);
    if (system("rm -rf /tmp/datastore_test") != 0) {
        std::cerr << "cannot remove directory" << std::endl;
    }
    if (system("mkdir -p /tmp/datastore_test/data_location /tmp/datastore_test/metadata_location /tmp/datastore_test/altimeter_log") != 0) {
        std::cerr << "cannot make directory" << std::endl;
    }

    std::vector<boost::filesystem::path> data_locations{};
    data_locations.emplace_back(data_location);
    boost::filesystem::path metadata_location_path{metadata_location};
    limestone::api::configuration conf(data_locations, metadata_location_path);
    conf.set_instance_id("instance-001");
    conf.set_db_name("db-alpha");

    std::string contents;
    {
        altimeter_test_logger logger("/tmp/datastore_test/altimeter_log");
        datastore_ = std::make_unique<limestone::api::datastore_test>(conf);
        auto* impl = datastore_->get_impl();
        ASSERT_NE(impl, nullptr);

        impl->set_group_commit_sender_for_tests([](uint64_t) { return false; });
        EXPECT_FALSE(impl->propagate_group_commit(100));

        impl->set_group_commit_sender_for_tests([](uint64_t) { return true; });
        EXPECT_TRUE(impl->propagate_group_commit(101));
    }
    contents = read_event_log_contents("/tmp/datastore_test/altimeter_log");

    EXPECT_NE(contents.find("type:wal_shipped"), std::string::npos);
    EXPECT_NE(contents.find("wal_version:100"), std::string::npos);
    EXPECT_NE(contents.find("wal_version:101"), std::string::npos);
    EXPECT_NE(contents.find("result:1"), std::string::npos);
    EXPECT_NE(contents.find("result:2"), std::string::npos);

    unsetenv("TSURUGI_REPLICATION_ENDPOINT");
}
#endif

}  // namespace limestone::testing
