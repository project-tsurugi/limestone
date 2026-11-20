/*
 * Copyright 2022-2026 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <tuple>
#include <vector>

#include <boost/filesystem.hpp>
#include <boost/filesystem/fstream.hpp>

#include <limestone/api/backup_detail.h>
#include <limestone/api/cursor.h>
#include <limestone/api/snapshot.h>

#include "test_root.h"

#include "compaction_settings.h"
#include "compaction_catalog.h"
#include "datastore_impl.h"
#include "file_operations.h"
#include "internal.h"
#include "locator/locator_format.h"
#include "manifest.h"

namespace limestone::testing {

namespace {

// file_operations that fails only the removal of the file with the given name
class remove_failing_file_operations : public limestone::internal::real_file_operations {
public:
    void remove(const boost::filesystem::path& path, boost::system::error_code& ec) override {
        {
            std::scoped_lock<std::mutex> lock(mutex_);
            if (path.filename().string() == failing_name_) {
                ec = boost::system::errc::make_error_code(boost::system::errc::permission_denied);
                return;
            }
        }
        real_file_operations::remove(path, ec);
    }

    void fail_for(const std::string& name) {
        {
            std::scoped_lock<std::mutex> lock(mutex_);
            failing_name_ = name;
        }
    }

private:
    std::mutex mutex_{};
    std::string failing_name_{};
};

}  // namespace

using limestone::api::epoch_id_type;
using limestone::internal::compaction_settings;
using limestone::internal::compaction_catalog;

/**
 * Tests of the auto compaction. They check, by the change of the generation of the catalog, that the online
 * compaction worker runs a rotation and a compaction when the amount of the WAL exceeds the threshold.
 */
class auto_compaction_test : public ::testing::Test {
public:
    static constexpr const char* location = "/tmp/auto_compaction_test";

    // The time to wait when checking that nothing runs. Longer than the period of the loop of the worker (1 second)
    static constexpr std::chrono::milliseconds quiet_period{1500};

    void SetUp() override {
        boost::filesystem::remove_all(location);
        ASSERT_TRUE(boost::filesystem::create_directory(location));
    }

    void TearDown() override {
        datastore_ = nullptr;
        limestone::api::datastore_impl::set_file_operations_for_test(nullptr);
        boost::filesystem::remove_all(location);
    }

    // Starts the datastore. The worker thread is not started yet
    void start_datastore(const compaction_settings& settings) {
        limestone::api::configuration conf{};
        conf.set_data_location(location);
        datastore_ = std::make_unique<limestone::api::datastore_test>(conf);
        datastore_->get_impl()->set_compaction_settings(settings);
        lc0_ = &datastore_->create_channel();
        datastore_->ready();
    }

    // Creates a snapshot and destroys it, which starts the worker thread
    void start_worker() {
        std::unique_ptr<limestone::api::snapshot> snapshot = datastore_->get_snapshot();
    }

    // The settings with the threshold of one byte: writing one entry makes the amount of the WAL exceed it. The
    // measurement interval is short
    static compaction_settings eager_settings() {
        compaction_settings settings{};
        settings.threshold_bytes = 1;
        settings.measurement_interval = std::chrono::milliseconds(50);
        return settings;
    }

    // Advances the epoch by one. A rotation waits for the persistence up to the boundary epoch to complete. In
    // production the upper layer keeps advancing the epoch, so the tests also keep advancing it while they wait
    void advance_epoch() {
        ++epoch_;
        datastore_->switch_epoch(epoch_);
    }

    // Writes one entry at the current epoch, and advances the epoch
    void write_entry(const std::string& key, const std::string& value) {
        lc0_->begin_session();
        lc0_->add_entry(1, key, value, {epoch_, 0});
        lc0_->end_session();
        advance_epoch();
    }

    // Waits for the given duration, advancing the epoch
    void advance_epochs_for(std::chrono::milliseconds duration) {
        const std::chrono::steady_clock::time_point deadline = std::chrono::steady_clock::now() + duration;
        while (std::chrono::steady_clock::now() < deadline) {
            advance_epoch();
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
    }

    // The generation of the committed compaction, taken from the name of the compacted WAL file that the
    // datastore holds. The catalog file is not read (reading it while the worker is rewriting it would take it as
    // broken and restore it from its backup)
    [[nodiscard]] std::uint64_t catalog_generation() const {
        const std::optional<std::string> name = datastore_->get_impl()->get_current_compacted_file_name();
        if (! name.has_value()) {
            return 0;
        }
        for (std::uint64_t generation = 1; generation < 100; ++generation) {
            if (name.value() == compaction_catalog::get_compacted_filename_for_generation(generation)) {
                return generation;
            }
        }
        ADD_FAILURE() << "unexpected name of the compacted WAL file: " << name.value();
        return 0;
    }

    // Waits, advancing the epoch, until the generation of the catalog reaches expected or more. Returns false if
    // it does not by the timeout
    [[nodiscard]] bool wait_for_generation(std::uint64_t expected, std::chrono::milliseconds timeout) {
        const std::chrono::steady_clock::time_point deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            if (catalog_generation() >= expected) {
                return true;
            }
            advance_epoch();
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        return catalog_generation() >= expected;
    }

    // The names of the rotated WAL files in the log directory
    [[nodiscard]] static std::set<std::string> rotated_wal_files() {
        std::set<std::string> result{};
        for (const boost::filesystem::directory_entry& entry :
             boost::filesystem::directory_iterator(boost::filesystem::path(location))) {
            const std::string name = entry.path().filename().string();
            if (name.rfind("pwal_", 0) == 0 && ! limestone::internal::is_unrotated_pwal_name(name)
                && ! compaction_catalog::is_compaction_output_filename(name)
                && ! limestone::internal::locator_format::is_locator_file_name(name)) {
                result.insert(name);
            }
        }
        return result;
    }

    // Waits, advancing the epoch, until the rotated WAL files are equal to expected
    [[nodiscard]] bool wait_for_rotated_wal_files(const std::set<std::string>& expected,
                                                  std::chrono::milliseconds timeout) {
        const std::chrono::steady_clock::time_point deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            if (rotated_wal_files() == expected) {
                return true;
            }
            advance_epoch();
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        return rotated_wal_files() == expected;
    }

    // Runs the same compaction as the request by the ctrl file, without going through the worker thread
    void run_requested_compaction() {
        std::future<void> future = std::async(std::launch::async, [this]() { datastore_->compact_with_online(); });
        while (future.wait_for(std::chrono::milliseconds(20)) != std::future_status::ready) {
            advance_epoch();
        }
        future.get();
    }

    // The number of the rotated WAL files in files_ (the files a backup enumerates)
    [[nodiscard]] std::size_t rotated_wal_files_in_backup_targets() const {
        std::size_t count = 0;
        for (const boost::filesystem::path& path : datastore_->files()) {
            const std::string name = path.filename().string();
            if (name.rfind("pwal_", 0) == 0 && ! limestone::internal::is_unrotated_pwal_name(name)
                && ! compaction_catalog::is_compaction_output_filename(name)
                && ! limestone::internal::locator_format::is_locator_file_name(name)) {
                ++count;
            }
        }
        return count;
    }

    // Reads all the entries (keys and values) the snapshot returns
    [[nodiscard]] std::vector<std::tuple<std::string, std::string>> read_all_entries() {
        std::vector<std::tuple<std::string, std::string>> result{};
        std::unique_ptr<limestone::api::snapshot> snapshot = datastore_->get_snapshot();
        std::unique_ptr<limestone::api::cursor> cursor = snapshot->get_cursor();
        while (cursor->next()) {
            std::string key{};
            std::string value{};
            cursor->key(key);
            cursor->value(value);
            result.emplace_back(key, value);
        }
        return result;
    }

    std::unique_ptr<limestone::api::datastore_test> datastore_{};
    limestone::api::log_channel* lc0_{};
    epoch_id_type epoch_{0};
};

// The auto compaction runs when the amount of the WAL exceeds the threshold
TEST_F(auto_compaction_test, runs_when_threshold_is_exceeded) {
    start_datastore(eager_settings());
    advance_epoch();
    write_entry("k1", "v1");
    ASSERT_EQ(catalog_generation(), 0U);

    start_worker();

    EXPECT_TRUE(wait_for_generation(1, std::chrono::seconds(10)));
}

// The auto compaction does not run when the threshold is 0 (the default)
TEST_F(auto_compaction_test, does_not_run_with_zero_threshold) {
    compaction_settings settings{};
    settings.measurement_interval = std::chrono::milliseconds(50);
    ASSERT_EQ(settings.threshold_bytes, 0U);
    start_datastore(settings);
    advance_epoch();
    write_entry("k1", "v1");

    start_worker();
    advance_epochs_for(quiet_period);

    EXPECT_EQ(catalog_generation(), 0U);
}

// The auto compaction does not run when the amount of the WAL is at or below the threshold
TEST_F(auto_compaction_test, does_not_run_below_threshold) {
    compaction_settings settings = eager_settings();
    settings.threshold_bytes = std::uint64_t{1024} * 1024;
    start_datastore(settings);
    advance_epoch();
    write_entry("k1", "v1");

    start_worker();
    advance_epochs_for(quiet_period);

    EXPECT_EQ(catalog_generation(), 0U);
}

// With no WAL file at all, nothing runs and no error occurs
TEST_F(auto_compaction_test, does_nothing_without_wal_files) {
    start_datastore(eager_settings());
    advance_epoch();

    start_worker();
    advance_epochs_for(quiet_period);

    EXPECT_EQ(catalog_generation(), 0U);
    datastore_->shutdown();
}

// It does not run while the boundary epoch of the rotation is 0. It runs once the epoch advances
TEST_F(auto_compaction_test, waits_while_boundary_epoch_is_zero) {
    start_datastore(eager_settings());
    // Place a file that has the name of a WAL file and is larger than the threshold. Its content is not read
    {
        boost::filesystem::ofstream stream(boost::filesystem::path(location) / "pwal_0009.1700000000000.0",
                                           std::ios::binary);
        stream << "the content is not read; it only has to be larger than the threshold";
    }
    ASSERT_EQ(datastore_->epoch_id_switched(), 0U);

    start_worker();
    std::this_thread::sleep_for(quiet_period);
    EXPECT_EQ(catalog_generation(), 0U);

    boost::filesystem::remove(boost::filesystem::path(location) / "pwal_0009.1700000000000.0");
    advance_epoch();
    write_entry("k1", "v1");

    EXPECT_TRUE(wait_for_generation(1, std::chrono::seconds(10)));
}

// A measurement is not taken at an interval shorter than the measurement interval
TEST_F(auto_compaction_test, measures_at_the_measurement_interval) {
    compaction_settings settings = eager_settings();
    settings.measurement_interval = std::chrono::seconds(4);
    start_datastore(settings);
    advance_epoch();

    // The first measurement is taken right after the start. There is no WAL at this point
    start_worker();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    write_entry("k1", "v1");

    // Until the next measurement, it does not run even though the threshold is exceeded
    advance_epochs_for(quiet_period);
    EXPECT_EQ(catalog_generation(), 0U);

    EXPECT_TRUE(wait_for_generation(1, std::chrono::seconds(10)));
}

// The auto compaction does not run during a backup. It runs after the backup ends
TEST_F(auto_compaction_test, does_not_run_during_backup) {
    start_datastore(eager_settings());
    advance_epoch();
    write_entry("k1", "v1");
    datastore_->get_impl()->increment_backup_counter();

    start_worker();
    advance_epochs_for(quiet_period);
    EXPECT_EQ(catalog_generation(), 0U);

    datastore_->get_impl()->decrement_backup_counter();
    EXPECT_TRUE(wait_for_generation(1, std::chrono::seconds(10)));
}

// A request by the ctrl file and an excess over the threshold at the same time are handled one after another
TEST_F(auto_compaction_test, ctrl_file_request_and_threshold_are_handled_in_order) {
    start_datastore(eager_settings());
    advance_epoch();
    write_entry("k1", "v1");
    const boost::filesystem::path start_file = boost::filesystem::path(location) / "ctrl" / "start_compaction";
    {
        boost::filesystem::ofstream stream(start_file);
    }

    start_worker();

    EXPECT_TRUE(wait_for_generation(1, std::chrono::seconds(10)));
    EXPECT_FALSE(boost::filesystem::exists(start_file));

    // What is written next is also compacted at the next measurement
    write_entry("k2", "v2");
    EXPECT_TRUE(wait_for_generation(2, std::chrono::seconds(10)));
    datastore_->shutdown();
}

// shutdown() right after the worker starts with the auto compaction pending completes normally
TEST_F(auto_compaction_test, shutdown_while_worker_is_running) {
    start_datastore(eager_settings());
    advance_epoch();
    write_entry("k1", "v1");

    start_worker();
    datastore_->shutdown();
}

// The WAL files renamed by the rotation for the migration of the format version become, within that run, the
// targets of a backup and the input of the auto compaction. If they did not become the input, the amount of the
// WAL would never fall below the threshold and a compaction would be repeated at every measurement
TEST_F(auto_compaction_test, wal_files_rotated_for_format_version_migration_are_compacted) {
    start_datastore(compaction_settings{});
    advance_epoch();
    write_entry("k1", "v1");
    datastore_->shutdown();
    datastore_ = nullptr;

    limestone::api::configuration conf{};
    conf.set_data_location(location);
    datastore_ = std::make_unique<limestone::api::datastore_test>(conf);
    datastore_->get_impl()->set_compaction_settings(eager_settings());
    datastore_->get_impl()->set_migration_info(limestone::internal::manifest::migration_info(5, 6));
    lc0_ = &datastore_->create_channel();
    datastore_->ready();
    EXPECT_EQ(rotated_wal_files().size(), 1U);
    EXPECT_EQ(rotated_wal_files_in_backup_targets(), 1U);

    start_worker();
    ASSERT_TRUE(wait_for_generation(1, std::chrono::seconds(10)));
    // The renamed file is removed after it becomes the input. If it were left, a compaction would run again at
    // the next measurement
    advance_epochs_for(quiet_period);
    EXPECT_EQ(catalog_generation(), 1U);
    EXPECT_TRUE(rotated_wal_files().empty());
    datastore_->shutdown();
}

// shutdown() while the rotation of the auto compaction is waiting gives up the rotation and completes normally.
// No compaction is performed
TEST_F(auto_compaction_test, shutdown_while_rotation_is_waiting) {
    start_datastore(eager_settings());
    advance_epoch();
    write_entry("k1", "v1");
    // The session is not ended, so the rotation keeps waiting for the persistence to complete
    lc0_->begin_session();
    lc0_->add_entry(1, "k2", "v2", {epoch_, 0});
    std::atomic<bool> rotation_started{false};
    datastore_->on_rotate_log_files_callback = [&rotation_started]() { rotation_started.store(true); };

    start_worker();
    const std::chrono::steady_clock::time_point deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (! rotation_started.load() && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    ASSERT_TRUE(rotation_started.load());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    datastore_->shutdown();

    EXPECT_EQ(catalog_generation(), 0U);
    EXPECT_TRUE(rotated_wal_files().empty());
}

// The auto compaction removes the detached WAL files by default. The removed files are no longer targets of a
// backup
TEST_F(auto_compaction_test, removes_detached_wal_files_by_default) {
    start_datastore(eager_settings());
    advance_epoch();
    write_entry("k1", "v1");

    start_worker();

    ASSERT_TRUE(wait_for_generation(1, std::chrono::seconds(10)));
    EXPECT_TRUE(wait_for_rotated_wal_files({}, std::chrono::seconds(10)));
    datastore_->shutdown();
    EXPECT_EQ(rotated_wal_files_in_backup_targets(), 0U);
}

// When the setting of the removal is disabled, the detached WAL files remain
TEST_F(auto_compaction_test, keeps_detached_wal_files_when_disabled) {
    compaction_settings settings = eager_settings();
    settings.delete_detached = false;
    start_datastore(settings);
    advance_epoch();
    write_entry("k1", "v1");

    start_worker();

    ASSERT_TRUE(wait_for_generation(1, std::chrono::seconds(10)));
    datastore_->shutdown();
    EXPECT_EQ(rotated_wal_files().size(), 1U);
    EXPECT_EQ(rotated_wal_files_in_backup_targets(), 1U);
}

// A compaction requested by the ctrl file does not remove the detached WAL files. The next auto compaction
// removes the files left
TEST_F(auto_compaction_test, removes_wal_files_detached_by_earlier_compactions) {
    start_datastore(eager_settings());
    advance_epoch();
    write_entry("k1", "v1");

    run_requested_compaction();
    ASSERT_EQ(catalog_generation(), 1U);
    ASSERT_EQ(rotated_wal_files().size(), 1U);

    write_entry("k2", "v2");
    start_worker();

    ASSERT_TRUE(wait_for_generation(2, std::chrono::seconds(10)));
    EXPECT_TRUE(wait_for_rotated_wal_files({}, std::chrono::seconds(10)));
    datastore_->shutdown();
}

// A backup begun while a compaction is running waits for the compaction to complete. The targets of the backup
// do not include the files the compaction removed
TEST_F(auto_compaction_test, backup_waits_for_running_compaction) {
    start_datastore(eager_settings());
    advance_epoch();
    write_entry("k1", "v1");
    // Stop the compaction after the update of the catalog and before the removal
    std::promise<void> reached{};
    std::promise<void> release{};
    std::shared_future<void> released = release.get_future().share();
    std::atomic<bool> fired{false};
    datastore_->get_impl()->set_on_compaction_after_commit_for_test([&reached, released, &fired]() {
        if (! fired.exchange(true)) {
            reached.set_value();
            released.wait();
        }
    });

    start_worker();
    std::future<void> reached_future = reached.get_future();
    while (reached_future.wait_for(std::chrono::milliseconds(20)) != std::future_status::ready) {
        advance_epoch();
    }
    const std::set<std::string> detached = rotated_wal_files();
    ASSERT_EQ(detached.size(), 1U);

    std::future<std::unique_ptr<limestone::api::backup_detail>> backup = std::async(
        std::launch::async, [this]() { return datastore_->begin_backup(limestone::api::backup_type::standard); });
    advance_epochs_for(std::chrono::milliseconds(300));
    EXPECT_NE(backup.wait_for(std::chrono::milliseconds(0)), std::future_status::ready);

    release.set_value();
    while (backup.wait_for(std::chrono::milliseconds(20)) != std::future_status::ready) {
        advance_epoch();
    }
    std::unique_ptr<limestone::api::backup_detail> detail = backup.get();
    for (const limestone::api::backup_detail::entry& entry : detail->entries()) {
        EXPECT_EQ(detached.count(entry.source_path().filename().string()), 0U);
        EXPECT_TRUE(boost::filesystem::exists(entry.source_path()));
    }
    for (const std::string& name : detached) {
        EXPECT_FALSE(boost::filesystem::exists(boost::filesystem::path(location) / name));
    }
    detail->notify_end_backup();
    datastore_->shutdown();
}

// Even if the removal of a file fails, the rest are removed. The next auto compaction removes the file that
// failed
TEST_F(auto_compaction_test, continues_after_a_removal_failure) {
    remove_failing_file_operations ops{};
    limestone::api::datastore_impl::set_file_operations_for_test(&ops);
    start_datastore(eager_settings());
    advance_epoch();
    write_entry("k1", "v1");

    run_requested_compaction();
    const std::set<std::string> left = rotated_wal_files();
    ASSERT_EQ(left.size(), 1U);
    ops.fail_for(*left.begin());

    write_entry("k2", "v2");
    start_worker();

    ASSERT_TRUE(wait_for_generation(2, std::chrono::seconds(10)));
    EXPECT_TRUE(wait_for_rotated_wal_files(left, std::chrono::seconds(10)));

    ops.fail_for("");
    write_entry("k3", "v3");

    ASSERT_TRUE(wait_for_generation(3, std::chrono::seconds(10)));
    EXPECT_TRUE(wait_for_rotated_wal_files({}, std::chrono::seconds(10)));
    datastore_->shutdown();
}

// With the persistent data after the detached WAL files are removed, the next compaction, a restart, and
// inspect of tglogutil work normally
TEST_F(auto_compaction_test, data_is_usable_after_removal) {
    start_datastore(eager_settings());
    advance_epoch();
    write_entry("k1", "v1");
    start_worker();
    ASSERT_TRUE(wait_for_generation(1, std::chrono::seconds(10)));
    ASSERT_TRUE(wait_for_rotated_wal_files({}, std::chrono::seconds(10)));

    // The next compaction. The detached list of the catalog still has the names of the removed files
    write_entry("k2", "v2");
    ASSERT_TRUE(wait_for_generation(2, std::chrono::seconds(10)));
    ASSERT_TRUE(wait_for_rotated_wal_files({}, std::chrono::seconds(10)));
    datastore_->shutdown();
    datastore_ = nullptr;

    // inspect of tglogutil
    {
        const std::string command = std::string("../src/tglogutil inspect ") + location + " 2>&1";
        FILE* pipe = popen(command.c_str(), "r");
        ASSERT_NE(pipe, nullptr);
        std::string output{};
        std::array<char, 4096> buffer{};
        std::size_t read_size = 0;
        while ((read_size = fread(buffer.data(), 1, buffer.size(), pipe)) > 0) {
            output.append(buffer.data(), read_size);
        }
        EXPECT_EQ(pclose(pipe), 0) << output;
    }

    // Restart
    compaction_settings settings{};
    start_datastore(settings);
    const std::vector<std::tuple<std::string, std::string>> expected{{"k1", "v1"}, {"k2", "v2"}};
    EXPECT_EQ(read_all_entries(), expected);
}

}  // namespace limestone::testing
