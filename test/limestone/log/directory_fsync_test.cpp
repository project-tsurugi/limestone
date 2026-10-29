/*
 * Copyright 2026-2026 Project Tsurugi.
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

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <fstream>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <boost/filesystem.hpp>

#include "datastore_impl.h"
#include "dblog_scan.h"
#include "file_operations.h"
#include "internal.h"
#include "limestone/api/limestone_exception.h"
#include "limestone_exception_helper.h"
#include "test_root.h"

namespace limestone::testing {

using limestone::api::datastore_impl;
using limestone::api::limestone_io_exception;
using limestone::internal::real_file_operations;

namespace {

constexpr const char* location = "/tmp/directory_fsync_test";

// records the directories passed to fsync_directory, and makes it fail for the directories for which fail_if returns
// true. It is also called from the background threads of the datastore, so it is guarded by a mutex
class directory_sync_recorder : public real_file_operations {
public:
    int fsync_directory(const boost::filesystem::path& dir) override {
        {
            std::scoped_lock lock(mutex_);
            synced_dirs_.push_back(dir);
            if (fail_if_ && fail_if_(dir)) {
                errno = EIO;
                return -1;
            }
        }
        return real_file_operations::fsync_directory(dir);
    }
    [[nodiscard]] std::size_t count(const boost::filesystem::path& dir) const {
        std::scoped_lock lock(mutex_);
        return static_cast<std::size_t>(std::count(synced_dirs_.begin(), synced_dirs_.end(), dir));
    }
    void clear() {
        std::scoped_lock lock(mutex_);
        synced_dirs_.clear();
    }
    void fail_if(std::function<bool(const boost::filesystem::path&)> predicate) {
        std::scoped_lock lock(mutex_);
        fail_if_ = std::move(predicate);
    }
private:
    mutable std::mutex mutex_;
    std::vector<boost::filesystem::path> synced_dirs_;
    std::function<bool(const boost::filesystem::path&)> fail_if_;
};

}  // namespace

class directory_fsync_test : public ::testing::Test {
protected:
    void SetUp() override {
        limestone::testing::enable_exception_throwing = true;
        boost::filesystem::remove_all(location);
        datastore_impl::set_file_operations_for_test(&ops_);
    }

    void TearDown() override {
        datastore_ = nullptr;
        datastore_impl::set_file_operations_for_test(nullptr);
        limestone::testing::enable_exception_throwing = false;
        boost::filesystem::remove_all(location);
    }

    void gen_datastore() {
        limestone::api::configuration conf{};
        conf.set_data_location(location);
        datastore_ = std::make_unique<limestone::api::datastore_test>(conf);
    }

    const boost::filesystem::path location_{location};
    directory_sync_recorder ops_{};
    std::unique_ptr<limestone::api::datastore_test> datastore_{};
};

// a new datastore synchronizes the parent of the created directory, and the directory of the created epoch file and
// blob directory
TEST_F(directory_fsync_test, new_datastore_synchronizes_directories) {
    gen_datastore();
    EXPECT_EQ(ops_.count(location_.parent_path()), 1U);
    EXPECT_EQ(ops_.count(location_), 2U);  // the epoch file and the blob directory
}

// failing to synchronize the parent directory after creating the log directory
TEST_F(directory_fsync_test, location_directory_sync_fails) {
    ops_.fail_if([this](const boost::filesystem::path& dir) { return dir == location_.parent_path(); });
    try {
        gen_datastore();
        FAIL() << "expected limestone_io_exception";
    } catch (const limestone_io_exception& e) {
        EXPECT_EQ(e.error_code(), EIO);
        EXPECT_NE(std::string(e.what()).find("fail to fsync the parent directory of"), std::string::npos) << e.what();
    }
}

// failing to synchronize the directory after creating the epoch file
TEST_F(directory_fsync_test, epoch_file_directory_sync_fails) {
    boost::filesystem::create_directories(location_);
    limestone::internal::setup_initial_logdir(location_);
    ops_.fail_if([this](const boost::filesystem::path& dir) { return dir == location_; });
    try {
        gen_datastore();
        FAIL() << "expected limestone_io_exception";
    } catch (const limestone_io_exception& e) {
        EXPECT_EQ(e.error_code(), EIO);
        EXPECT_NE(std::string(e.what()).find("fail to fsync directory"), std::string::npos) << e.what();
    }
}

// failing to synchronize the parent directory after creating the blob directory
TEST_F(directory_fsync_test, blob_directory_sync_fails) {
    boost::filesystem::create_directories(location_);
    limestone::internal::setup_initial_logdir(location_);
    std::ofstream((location_ / std::string(limestone::internal::epoch_file_name)).string());
    ops_.fail_if([this](const boost::filesystem::path& dir) { return dir == location_; });
    try {
        gen_datastore();
        FAIL() << "expected limestone_io_exception";
    } catch (const limestone_io_exception& e) {
        EXPECT_EQ(e.error_code(), EIO);
        EXPECT_NE(std::string(e.what()).find("fail to fsync the parent directory of"), std::string::npos) << e.what();
    }
}

// the directory is synchronized for a pwal only by the first session after the channel is created or rotated
TEST_F(directory_fsync_test, pwal_creation_synchronizes_directory) {
    gen_datastore();
    auto& channel = datastore_->create_channel();
    datastore_->ready();
    ops_.clear();

    datastore_->switch_epoch(2);
    channel.begin_session();
    channel.end_session();
    std::size_t after_first = ops_.count(location_);
    EXPECT_GE(after_first, 1U);

    ops_.clear();
    channel.begin_session();
    channel.end_session();
    EXPECT_EQ(ops_.count(location_), 0U) << "the second session does not create the file";

    // the backup rotates the pwal file; the rename and the next creation synchronize the directory. The backup waits
    // for the epoch to advance, so switch the epoch in another thread
    ops_.clear();
    std::atomic<bool> backup_completed{false};
    std::atomic<limestone::api::epoch_id_type> epoch{3};
    std::thread switch_epoch_thread([&]() {
        while (!backup_completed.load()) {
            datastore_->switch_epoch(epoch++);
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    });
    (void) datastore_->begin_backup(limestone::api::backup_type::standard);
    backup_completed.store(true);
    switch_epoch_thread.join();
    EXPECT_GE(ops_.count(location_), 1U) << "the rotation renames the file";
    ops_.clear();
    channel.begin_session();
    channel.end_session();
    EXPECT_EQ(ops_.count(location_), 1U) << "the first session after the rotation creates the file";
}

// failing to synchronize the directory after creating a pwal
TEST_F(directory_fsync_test, pwal_creation_directory_sync_fails) {
    gen_datastore();
    auto& channel = datastore_->create_channel();
    datastore_->ready();
    datastore_->switch_epoch(2);
    ops_.fail_if([this](const boost::filesystem::path& dir) { return dir == location_; });
    try {
        channel.begin_session();
        FAIL() << "expected limestone_io_exception";
    } catch (const limestone_io_exception& e) {
        EXPECT_EQ(e.error_code(), EIO);
        EXPECT_NE(std::string(e.what()).find("fail to fsync directory"), std::string::npos) << e.what();
    }
    ops_.fail_if({});
}

// rotate_pwal_file synchronizes the directory after the rename, and throws if it fails
TEST_F(directory_fsync_test, rotate_pwal_file_synchronizes_directory) {
    boost::filesystem::create_directories(location_);
    const boost::filesystem::path pwal = location_ / "pwal_0000";
    std::ofstream(pwal.string()) << "data";
    directory_sync_recorder ops;

    (void) limestone::internal::rotate_pwal_file(pwal, 1, ops);
    EXPECT_EQ(ops.count(location_), 1U);

    std::ofstream(pwal.string()) << "data";
    ops.fail_if([](const boost::filesystem::path&) { return true; });
    try {
        (void) limestone::internal::rotate_pwal_file(pwal, 2, ops);
        FAIL() << "expected limestone_io_exception";
    } catch (const limestone_io_exception& e) {
        EXPECT_EQ(e.error_code(), EIO);
        EXPECT_NE(std::string(e.what()).find("Failed to fsync directory"), std::string::npos) << e.what();
    }
}

// rewriting the epoch file (the rename from the temporary file) synchronizes the directory, and throws if it fails
TEST_F(directory_fsync_test, epoch_file_rewrite_synchronizes_directory) {
    gen_datastore();
    auto& channel = datastore_->create_channel();
    datastore_->ready();
    // the durable epoch advances only with the sessions of the channels. The first session creates the pwal file
    limestone::api::epoch_id_type epoch = 2;
    datastore_->switch_epoch(epoch++);
    channel.begin_session();
    channel.end_session();
    ops_.clear();
    for (int i = 0; i <= limestone::internal::max_entries_in_epoch_file; i++) {
        datastore_->switch_epoch(epoch++);
        channel.begin_session();
        channel.end_session();
    }
    EXPECT_GE(ops_.count(location_), 1U);

    ops_.fail_if([this](const boost::filesystem::path& dir) { return dir == location_; });
    try {
        for (int i = 0; i <= limestone::internal::max_entries_in_epoch_file; i++) {
            datastore_->switch_epoch(epoch++);
            channel.begin_session();
            channel.end_session();
        }
        FAIL() << "expected limestone_io_exception";
    } catch (const limestone_io_exception& e) {
        EXPECT_EQ(e.error_code(), EIO);
        EXPECT_NE(std::string(e.what()).find("Failed to fsync directory"), std::string::npos) << e.what();
    }
    ops_.fail_if({});
}

// the rotation of the epoch file synchronizes the directory, and throws if it fails
TEST_F(directory_fsync_test, epoch_file_rotation_synchronizes_directory) {
    gen_datastore();
    datastore_->ready();
    ops_.clear();
    datastore_->rotate_epoch_file();
    EXPECT_EQ(ops_.count(location_), 1U);

    ops_.fail_if([this](const boost::filesystem::path& dir) { return dir == location_; });
    try {
        datastore_->rotate_epoch_file();
        FAIL() << "expected limestone_io_exception";
    } catch (const limestone_io_exception& e) {
        EXPECT_EQ(e.error_code(), EIO);
        EXPECT_NE(std::string(e.what()).find("Failed to fsync directory"), std::string::npos) << e.what();
    }
    ops_.fail_if({});
}

// detach of dblog_scan synchronizes the directory after the renames, and throws if it fails
TEST_F(directory_fsync_test, detach_wal_files_synchronizes_directory) {
    boost::filesystem::create_directories(location_);
    std::ofstream((location_ / "pwal_0000").string()) << "data";
    directory_sync_recorder ops;
    limestone::internal::dblog_scan ds(location_);
    ds.set_file_operations(ops);
    ds.detach_wal_files();
    EXPECT_EQ(ops.count(location_), 1U);

    std::ofstream((location_ / "pwal_0001").string()) << "data";
    ops.fail_if([](const boost::filesystem::path&) { return true; });
    try {
        ds.detach_wal_files();
        FAIL() << "expected limestone_io_exception";
    } catch (const limestone_io_exception& e) {
        EXPECT_EQ(e.error_code(), EIO);
        EXPECT_NE(std::string(e.what()).find("Failed to fsync directory"), std::string::npos) << e.what();
    }
}

// with no file to detach, the directory is not synchronized
TEST_F(directory_fsync_test, detach_wal_files_without_files_does_not_synchronize) {
    boost::filesystem::create_directories(location_);
    directory_sync_recorder ops;
    limestone::internal::dblog_scan ds(location_);
    ds.set_file_operations(ops);
    ds.detach_wal_files();
    EXPECT_EQ(ops.count(location_), 0U);
}

}  // namespace limestone::testing
