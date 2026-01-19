#include <filesystem>
#include <fstream>
#include <sstream>

#include <gtest/gtest.h>

#include <limestone/api/configuration.h>
#include "test_root.h"

#ifdef ENABLE_ALTIMETER
#include <altimeter/configuration.h>
#include <altimeter/event/constants.h>
#include <altimeter/logger.h>
#endif

namespace limestone::testing {

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
}  // namespace
#endif

#ifdef ENABLE_ALTIMETER
TEST(altimeter_wal_started_log_test, altimeter_wal_started_log_written) {
    if (system("rm -rf /tmp/altimeter_wal_started_log_test") != 0) {
        std::cerr << "cannot remove directory" << std::endl;
    }
    if (system("mkdir -p /tmp/altimeter_wal_started_log_test/data_location "
               "/tmp/altimeter_wal_started_log_test/metadata_location "
               "/tmp/altimeter_wal_started_log_test/altimeter_log") != 0) {
        std::cerr << "cannot make directory" << std::endl;
    }

    std::vector<boost::filesystem::path> data_locations{};
    data_locations.emplace_back("/tmp/altimeter_wal_started_log_test/data_location");
    boost::filesystem::path metadata_location_path{"/tmp/altimeter_wal_started_log_test/metadata_location"};
    limestone::api::configuration conf(data_locations, metadata_location_path);

    {
        altimeter_test_logger logger("/tmp/altimeter_wal_started_log_test/altimeter_log");
        limestone::api::datastore_test datastore(conf);
        datastore.recover();
        datastore.ready();
        datastore.shutdown();
    }

    const auto contents = read_event_log_contents("/tmp/altimeter_wal_started_log_test/altimeter_log");
    EXPECT_NE(contents.find("type:wal_started"), std::string::npos);
    EXPECT_NE(contents.find("result:1"), std::string::npos);

    if (system("rm -rf /tmp/altimeter_wal_started_log_test") != 0) {
        std::cerr << "cannot remove directory" << std::endl;
    }
}

TEST(altimeter_wal_started_log_test, altimeter_wal_started_log_failure_written) {
    if (system("rm -rf /tmp/altimeter_wal_started_log_failure_test") != 0) {
        std::cerr << "cannot remove directory" << std::endl;
    }
    if (system("mkdir -p /tmp/altimeter_wal_started_log_failure_test/data_location "
               "/tmp/altimeter_wal_started_log_failure_test/metadata_location "
               "/tmp/altimeter_wal_started_log_failure_test/altimeter_log") != 0) {
        std::cerr << "cannot make directory" << std::endl;
    }

    std::vector<boost::filesystem::path> data_locations{};
    data_locations.emplace_back("/tmp/altimeter_wal_started_log_failure_test/data_location");
    boost::filesystem::path metadata_location_path{"/tmp/altimeter_wal_started_log_failure_test/metadata_location"};
    limestone::api::configuration conf(data_locations, metadata_location_path);

    ASSERT_DEATH(
        {
            altimeter_test_logger logger("/tmp/altimeter_wal_started_log_failure_test/altimeter_log");
            limestone::api::datastore_test datastore(conf);
            if (system("rm -rf /tmp/altimeter_wal_started_log_failure_test/data_location") != 0) {
                std::cerr << "cannot remove data_location directory" << std::endl;
            }
            datastore.ready();
        },
        ".*");

    const auto contents = read_event_log_contents("/tmp/altimeter_wal_started_log_failure_test/altimeter_log");
    EXPECT_NE(contents.find("type:wal_started"), std::string::npos);
    EXPECT_NE(contents.find("result:2"), std::string::npos);

    if (system("rm -rf /tmp/altimeter_wal_started_log_failure_test") != 0) {
        std::cerr << "cannot remove directory" << std::endl;
    }
}
#endif

}  // namespace limestone::testing
