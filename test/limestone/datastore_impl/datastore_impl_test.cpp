#include <gtest/gtest.h>
#include <limestone/datastore_impl.h>
#include <limestone/manifest.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <sstream>

#ifdef ENABLE_ALTIMETER
#include <altimeter/configuration.h>
#include <altimeter/event/constants.h>
#include <altimeter/logger.h>
#endif

namespace limestone::api {

class datastore_impl_test : public ::testing::Test {
protected:
    datastore_impl_test() = default;
    ~datastore_impl_test() override = default;
    void TearDown() override
    {
        unsetenv("TP_MONITOR_ENDPOINT");
        unsetenv("TSURUGI_REPLICATION_ENDPOINT");
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

TEST_F(datastore_impl_test, migration_info_getter_setter) {
    datastore_impl datastore;
    
    // Initially, migration_info should not have a value
    EXPECT_FALSE(datastore.get_migration_info().has_value());
    
    // Create a migration_info object and set it
    limestone::internal::manifest::migration_info info(5, 6);
    datastore.set_migration_info(info);
    
    // Verify that the migration_info is now set
    EXPECT_TRUE(datastore.get_migration_info().has_value());
    
    // Verify the values are correct
    auto& stored_info = datastore.get_migration_info().value();
    EXPECT_EQ(stored_info.get_old_version(), 5);
    EXPECT_EQ(stored_info.get_new_version(), 6);
    EXPECT_TRUE(stored_info.requires_rotation());
}

TEST_F(datastore_impl_test, migration_info_no_rotation_required) {
    datastore_impl datastore;
    
    // Create a migration_info that doesn't require rotation
    limestone::internal::manifest::migration_info info(6, 7);
    datastore.set_migration_info(info);
    
    // Verify the migration_info is set correctly
    EXPECT_TRUE(datastore.get_migration_info().has_value());
    auto& stored_info = datastore.get_migration_info().value();
    EXPECT_EQ(stored_info.get_old_version(), 6);
    EXPECT_EQ(stored_info.get_new_version(), 7);
    EXPECT_FALSE(stored_info.requires_rotation());
}

TEST_F(datastore_impl_test, migration_info_multiple_sets) {
    datastore_impl datastore;
    
    // Set first migration_info
    limestone::internal::manifest::migration_info info1(3, 4);
    datastore.set_migration_info(info1);
    
    EXPECT_TRUE(datastore.get_migration_info().has_value());
    EXPECT_EQ(datastore.get_migration_info().value().get_old_version(), 3);
    EXPECT_EQ(datastore.get_migration_info().value().get_new_version(), 4);
    
    // Overwrite with second migration_info
    limestone::internal::manifest::migration_info info2(7, 8);
    datastore.set_migration_info(info2);
    
    EXPECT_TRUE(datastore.get_migration_info().has_value());
    EXPECT_EQ(datastore.get_migration_info().value().get_old_version(), 7);
    EXPECT_EQ(datastore.get_migration_info().value().get_new_version(), 8);
}

TEST_F(datastore_impl_test, generate_reference_tag_deterministic_and_unique) {
    datastore_impl datastore;

    blob_id_type const blob_id1 = 100;
    blob_id_type const blob_id2 = 200;
    std::uint64_t const txid1 = 1000;
    std::uint64_t const txid2 = 2000;

    auto const tag1a = datastore.generate_reference_tag(blob_id1, txid1);
    auto const tag1b = datastore.generate_reference_tag(blob_id1, txid1);
    EXPECT_EQ(tag1a, tag1b);

    auto const tag2 = datastore.generate_reference_tag(blob_id2, txid1);
    EXPECT_NE(tag1a, tag2);

    auto const tag3 = datastore.generate_reference_tag(blob_id1, txid2);
    EXPECT_NE(tag1a, tag3);
}

TEST_F(datastore_impl_test, propagate_group_commit_uses_sender_and_master_flag) {
    setenv("TSURUGI_REPLICATION_ENDPOINT", "tcp://127.0.0.1:12345", 1);

    datastore_impl datastore;

    datastore.set_group_commit_sender_for_tests([](uint64_t) { return false; });
    EXPECT_FALSE(datastore.propagate_group_commit(1));

    datastore.set_group_commit_sender_for_tests([](uint64_t) { return true; });
    EXPECT_TRUE(datastore.propagate_group_commit(2));

    datastore.set_replica_role();
    EXPECT_FALSE(datastore.propagate_group_commit(3));

    unsetenv("TSURUGI_REPLICATION_ENDPOINT");
}

TEST_F(datastore_impl_test, tp_monitor_endpoint_unset_disables_tp_monitor) {
    unsetenv("TP_MONITOR_ENDPOINT");

    datastore_impl datastore;

    EXPECT_FALSE(datastore.is_tp_monitor_enabled());
    EXPECT_TRUE(datastore.tp_monitor_host().empty());
    EXPECT_EQ(datastore.tp_monitor_port(), 0);
}

TEST_F(datastore_impl_test, tp_monitor_endpoint_parsed) {
    setenv("TP_MONITOR_ENDPOINT", "tcp://127.0.0.1:50051", 1);

    datastore_impl datastore;

    EXPECT_TRUE(datastore.is_tp_monitor_enabled());
    EXPECT_EQ(datastore.tp_monitor_host(), "127.0.0.1");
    EXPECT_EQ(datastore.tp_monitor_port(), 50051);
}

TEST_F(datastore_impl_test, tp_monitor_endpoint_invalid_disables_tp_monitor) {
    setenv("TP_MONITOR_ENDPOINT", "invalid-endpoint", 1);

    datastore_impl datastore;

    EXPECT_FALSE(datastore.is_tp_monitor_enabled());
    EXPECT_TRUE(datastore.tp_monitor_host().empty());
    EXPECT_EQ(datastore.tp_monitor_port(), 0);
}

#ifdef ENABLE_ALTIMETER
TEST_F(datastore_impl_test, altimeter_wal_shipped_log_written) {
    setenv("TSURUGI_REPLICATION_ENDPOINT", "tcp://127.0.0.1:12345", 1);
    if (system("rm -rf /tmp/datastore_impl_test") != 0) {
        std::cerr << "cannot remove directory" << std::endl;
    }
    if (system("mkdir -p /tmp/datastore_impl_test/altimeter_log") != 0) {
        std::cerr << "cannot make directory" << std::endl;
    }

    std::string contents;
    {
        altimeter_test_logger logger("/tmp/datastore_impl_test/altimeter_log");
        datastore_impl datastore;
        datastore.set_instance_id("instance-001");
        datastore.set_db_name("db-alpha");
        datastore.set_pid(::getpid());

        datastore.set_group_commit_sender_for_tests([](uint64_t) { return false; });
        EXPECT_FALSE(datastore.propagate_group_commit(100));

        datastore.set_group_commit_sender_for_tests([](uint64_t) { return true; });
        EXPECT_TRUE(datastore.propagate_group_commit(101));
    }
    contents = read_event_log_contents("/tmp/datastore_impl_test/altimeter_log");

    EXPECT_NE(contents.find("type:wal_shipped"), std::string::npos);
    EXPECT_NE(contents.find("wal_version:100"), std::string::npos);
    EXPECT_NE(contents.find("wal_version:101"), std::string::npos);
    EXPECT_NE(contents.find("result:1"), std::string::npos);
    EXPECT_NE(contents.find("result:2"), std::string::npos);

    unsetenv("TSURUGI_REPLICATION_ENDPOINT");
}

TEST_F(datastore_impl_test, altimeter_wal_shipped_log_failure_written) {
    setenv("TSURUGI_REPLICATION_ENDPOINT", "tcp://127.0.0.1:12345", 1);
    if (system("rm -rf /tmp/datastore_impl_test") != 0) {
        std::cerr << "cannot remove directory" << std::endl;
    }
    if (system("mkdir -p /tmp/datastore_impl_test/altimeter_log") != 0) {
        std::cerr << "cannot make directory" << std::endl;
    }

    std::string contents;
    {
        altimeter_test_logger logger("/tmp/datastore_impl_test/altimeter_log");
        datastore_impl datastore;
        datastore.set_instance_id("instance-001");
        datastore.set_db_name("db-alpha");
        datastore.set_pid(::getpid());

        datastore.set_group_commit_sender_for_tests([](uint64_t) { return false; });
        EXPECT_FALSE(datastore.propagate_group_commit(200));
    }
    contents = read_event_log_contents("/tmp/datastore_impl_test/altimeter_log");

    EXPECT_NE(contents.find("type:wal_shipped"), std::string::npos);
    EXPECT_NE(contents.find("wal_version:200"), std::string::npos);
    EXPECT_NE(contents.find("result:2"), std::string::npos);

    unsetenv("TSURUGI_REPLICATION_ENDPOINT");
}
#endif

} // namespace limestone::api
