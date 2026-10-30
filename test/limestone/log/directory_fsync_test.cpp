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

#include "blob_file_resolver.h"
#include "datastore_impl.h"
#include "dblog_scan.h"
#include "file_operations.h"
#include "internal.h"
#include "limestone/api/file_set_entry.h"
#include "limestone/api/limestone_exception.h"
#include "limestone/status.h"
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
    // counts the fsyncs of files, and makes them fail if fail_file_sync is true
    int fsync(int fd) override {
        {
            std::scoped_lock lock(mutex_);
            file_syncs_++;
            if (fail_file_sync_) {
                errno = EIO;
                return -1;
            }
        }
        return real_file_operations::fsync(fd);
    }
    [[nodiscard]] std::size_t file_syncs() const {
        std::scoped_lock lock(mutex_);
        return file_syncs_;
    }
    void fail_file_sync(bool fail) {
        std::scoped_lock lock(mutex_);
        fail_file_sync_ = fail;
    }
    // makes open fail for the path given to fail_open, and close fail for an fd opened for the path given to fail_close
    int open(const char* filename, int flags) override {
        {
            std::scoped_lock lock(mutex_);
            if (fail_open_path_ == filename) {
                errno = EACCES;
                return -1;
            }
        }
        int fd = real_file_operations::open(filename, flags);
        if (fd >= 0) {
            std::scoped_lock lock(mutex_);
            if (fail_close_path_ == filename) {
                fds_to_fail_close_.push_back(fd);
            }
        }
        return fd;
    }
    int close(int fd) override {
        {
            std::scoped_lock lock(mutex_);
            auto it = std::find(fds_to_fail_close_.begin(), fds_to_fail_close_.end(), fd);
            if (it != fds_to_fail_close_.end()) {
                fds_to_fail_close_.erase(it);
                (void) real_file_operations::close(fd);  // does not leak the fd
                errno = EIO;
                return -1;
            }
        }
        return real_file_operations::close(fd);
    }
    void fail_open(const boost::filesystem::path& path) {
        std::scoped_lock lock(mutex_);
        fail_open_path_ = path;
    }
    void fail_close(const boost::filesystem::path& path) {
        std::scoped_lock lock(mutex_);
        fail_close_path_ = path;
    }
private:
    mutable std::mutex mutex_;
    std::vector<boost::filesystem::path> synced_dirs_;
    std::function<bool(const boost::filesystem::path&)> fail_if_;
    std::size_t file_syncs_{};
    bool fail_file_sync_{};
    boost::filesystem::path fail_open_path_{};
    boost::filesystem::path fail_close_path_{};
    std::vector<int> fds_to_fail_close_{};
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
        boost::filesystem::remove_all(backup_);
    }

    void gen_datastore() {
        gen_datastore_at(location);
    }

    void gen_datastore_at(const boost::filesystem::path& data_location) {
        limestone::api::configuration conf{};
        conf.set_data_location(data_location.string());
        datastore_ = std::make_unique<limestone::api::datastore_test>(conf);
    }

    // creates a manifest, a pwal and a BLOB file in the backup directory, and returns the restore destination of the
    // BLOB file
    boost::filesystem::path prepare_backup() {
        boost::filesystem::remove_all(backup_);
        boost::filesystem::create_directories(backup_);
        limestone::internal::setup_initial_logdir(backup_);
        std::ofstream((backup_ / "pwal_0000").string()) << "data";
        limestone::internal::blob_file_resolver resolver{location_};
        const boost::filesystem::path blob_destination = resolver.resolve_path(1);
        std::ofstream((backup_ / blob_destination.filename()).string()) << "blob";
        return blob_destination;
    }

    const boost::filesystem::path location_{location};
    const boost::filesystem::path backup_{std::string(location) + "_backup"};
    directory_sync_recorder ops_{};
    std::unique_ptr<limestone::api::datastore_test> datastore_{};
};

// a new datastore synchronizes the parent of the created directory, the directory of the created epoch file and
// blob directory, and the blob directory for its created subdirectories
TEST_F(directory_fsync_test, new_datastore_synchronizes_directories) {
    gen_datastore();
    EXPECT_EQ(ops_.count(location_.parent_path()), 1U);
    EXPECT_EQ(ops_.count(location_), 2U);  // the epoch file and the blob directory
    const auto& resolver = datastore_->get_impl()->blob_file_resolver();
    EXPECT_EQ(ops_.count(resolver.get_blob_root()), 1U);
    for (const boost::filesystem::path& subdirectory : resolver.get_subdirectories()) {
        EXPECT_TRUE(boost::filesystem::is_directory(subdirectory)) << subdirectory.string();
    }

    // the second startup finds the subdirectories, so it neither creates nor synchronizes
    datastore_ = nullptr;
    ops_.clear();
    gen_datastore();
    EXPECT_EQ(ops_.count(resolver.get_blob_root()), 0U);
}

// failing to synchronize the blob directory after creating its subdirectories
TEST_F(directory_fsync_test, blob_subdirectory_sync_fails) {
    boost::filesystem::create_directories(location_);
    limestone::internal::setup_initial_logdir(location_);
    std::ofstream((location_ / std::string(limestone::internal::epoch_file_name)).string());
    boost::filesystem::create_directories(location_ / "blob");
    ops_.fail_if([this](const boost::filesystem::path& dir) { return dir == location_ / "blob"; });
    try {
        gen_datastore();
        FAIL() << "expected limestone_io_exception";
    } catch (const limestone_io_exception& e) {
        EXPECT_EQ(e.error_code(), EIO);
        EXPECT_NE(std::string(e.what()).find("fail to fsync the blob directory"), std::string::npos) << e.what();
    }
}

// a relative location of one element (for which parent_path() is empty) also synchronizes the parent of the created
// directory
TEST_F(directory_fsync_test, new_datastore_with_relative_location_synchronizes_parent) {
    const boost::filesystem::path saved_cwd = boost::filesystem::current_path();
    boost::filesystem::current_path(location_.parent_path());
    gen_datastore_at(location_.filename());
    datastore_ = nullptr;  // it works with the relative path, so stop it before restoring the cwd
    boost::filesystem::current_path(saved_cwd);
    EXPECT_EQ(ops_.count(location_.parent_path()), 1U);
}

// a location with trailing separators synchronizes its parent, not itself
TEST_F(directory_fsync_test, new_datastore_with_trailing_separator_synchronizes_parent) {
    gen_datastore_at(location_.string() + "/");
    EXPECT_EQ(ops_.count(location_.parent_path()), 1U);
}

TEST_F(directory_fsync_test, new_datastore_with_two_trailing_separators_synchronizes_parent) {
    gen_datastore_at(location_.string() + "//");
    EXPECT_EQ(ops_.count(location_.parent_path()), 1U);
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

// the rotation synchronizes the directory once at its end, whatever the number of the renamed files
TEST_F(directory_fsync_test, rotation_synchronizes_directory_once) {
    gen_datastore();
    auto& channel1 = datastore_->create_channel();
    auto& channel2 = datastore_->create_channel();
    datastore_->ready();
    datastore_->switch_epoch(2);
    channel1.begin_session();
    channel1.end_session();
    channel2.begin_session();
    channel2.end_session();
    ops_.clear();

    // the backup also rotates the epoch file before the rotation (one fsync). It waits for the epoch to advance, so
    // switch the epoch in another thread
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
    EXPECT_EQ(ops_.count(location_), 2U) << "the epoch file rotation and the pwal rotation";
}

// failing to synchronize the directory at the end of the rotation
TEST_F(directory_fsync_test, rotation_directory_sync_fails) {
    gen_datastore();
    auto& channel = datastore_->create_channel();
    datastore_->ready();
    datastore_->switch_epoch(2);
    channel.begin_session();
    channel.end_session();
    // fail only the fsync after the renames (the fsync of the epoch file rotation passes)
    ops_.fail_if([this](const boost::filesystem::path& dir) {
        if (dir != location_) {
            return false;
        }
        for (const boost::filesystem::path& p : boost::filesystem::directory_iterator(location_)) {
            if (p.filename().string().rfind("pwal_0000.", 0) == 0) {
                return true;
            }
        }
        return false;
    });

    std::atomic<bool> backup_completed{false};
    std::atomic<limestone::api::epoch_id_type> epoch{3};
    std::thread switch_epoch_thread([&]() {
        while (!backup_completed.load()) {
            datastore_->switch_epoch(epoch++);
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    });
    try {
        (void) datastore_->begin_backup(limestone::api::backup_type::standard);
        FAIL() << "expected limestone_io_exception";
    } catch (const limestone_io_exception& e) {
        EXPECT_EQ(e.error_code(), EIO);
        EXPECT_NE(std::string(e.what()).find("Failed to fsync the log directory after the rotation"), std::string::npos)
            << e.what();
    }
    backup_completed.store(true);
    switch_epoch_thread.join();
    ops_.fail_if({});
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

// restore synchronizes the data of the copied files, the destination directories and the parents of the created
// directories
TEST_F(directory_fsync_test, restore_synchronizes_files_and_directories) {
    gen_datastore();
    const boost::filesystem::path blob_destination = prepare_backup();
    boost::filesystem::remove_all(blob_destination.parent_path());  // the purge in restore does not remove directories
    ops_.clear();
    const std::size_t file_syncs_before = ops_.file_syncs();

    EXPECT_EQ(datastore_->restore(backup_.string(), true), limestone::status::ok);

    EXPECT_TRUE(boost::filesystem::exists(blob_destination));
    EXPECT_GE(ops_.file_syncs() - file_syncs_before, 3U);  // the manifest, the pwal and the BLOB file at least
    EXPECT_GE(ops_.count(location_), 1U);
    EXPECT_EQ(ops_.count(blob_destination.parent_path()), 1U);
    EXPECT_EQ(ops_.count(blob_destination.parent_path().parent_path()), 1U);  // the BLOB subdirectory is created
}

// restore with file_set_entry persists them in the same way
TEST_F(directory_fsync_test, restore_with_entries_synchronizes_files_and_directories) {
    gen_datastore();
    const boost::filesystem::path blob_destination = prepare_backup();
    std::vector<limestone::api::file_set_entry> entries;
    for (const boost::filesystem::path& p : boost::filesystem::directory_iterator(backup_)) {
        entries.emplace_back(p.filename(), p.filename(), false);
    }
    ops_.clear();
    const std::size_t file_syncs_before = ops_.file_syncs();

    EXPECT_EQ(datastore_->restore(backup_.string(), entries), limestone::status::ok);

    EXPECT_TRUE(boost::filesystem::exists(blob_destination));
    EXPECT_GE(ops_.file_syncs() - file_syncs_before, 3U);
    EXPECT_GE(ops_.count(location_), 1U);
    EXPECT_EQ(ops_.count(blob_destination.parent_path()), 1U);
}

// restore returns err_permission_error if synchronizing a copied file fails
TEST_F(directory_fsync_test, restore_file_sync_fails) {
    gen_datastore();
    (void) prepare_backup();
    ops_.fail_file_sync(true);
    EXPECT_EQ(datastore_->restore(backup_.string(), true), limestone::status::err_permission_error);
    ops_.fail_file_sync(false);
}

// restore returns err_permission_error if synchronizing a directory fails
TEST_F(directory_fsync_test, restore_directory_sync_fails) {
    gen_datastore();
    (void) prepare_backup();
    ops_.fail_if([this](const boost::filesystem::path& dir) { return dir == location_; });
    EXPECT_EQ(datastore_->restore(backup_.string(), true), limestone::status::err_permission_error);
    ops_.fail_if({});
}

// restore returns err_permission_error if a copied file cannot be opened to synchronize it
TEST_F(directory_fsync_test, restore_file_open_fails) {
    gen_datastore();
    (void) prepare_backup();
    ops_.fail_open(location_ / "pwal_0000");
    EXPECT_EQ(datastore_->restore(backup_.string(), true), limestone::status::err_permission_error);
    ops_.fail_open({});
}

// restore returns err_permission_error if closing a copied file fails
TEST_F(directory_fsync_test, restore_file_close_fails) {
    gen_datastore();
    (void) prepare_backup();
    ops_.fail_close(location_ / "pwal_0000");
    EXPECT_EQ(datastore_->restore(backup_.string(), true), limestone::status::err_permission_error);
    ops_.fail_close({});
}

// restore with file_set_entry returns err_permission_error if a file cannot be copied
TEST_F(directory_fsync_test, restore_with_entries_copy_fails) {
    gen_datastore();
    (void) prepare_backup();
    std::vector<limestone::api::file_set_entry> entries;
    entries.emplace_back("limestone-manifest.json", "limestone-manifest.json", false);
    // the destination directory does not exist
    entries.emplace_back("pwal_0000", "no_such_directory/pwal_0000", false);

    EXPECT_EQ(datastore_->restore(backup_.string(), entries), limestone::status::err_permission_error);
}

// restore with file_set_entry returns err_permission_error if the directory of a BLOB file cannot be created
TEST_F(directory_fsync_test, restore_with_entries_create_blob_directory_fails) {
    gen_datastore();
    const boost::filesystem::path blob_destination = prepare_backup();
    const boost::filesystem::path blob_directory = blob_destination.parent_path();
    boost::filesystem::remove_all(blob_directory);  // the purge in restore does not remove directories
    std::vector<limestone::api::file_set_entry> entries;
    entries.emplace_back("limestone-manifest.json", "limestone-manifest.json", false);
    // places a regular file at the position of the directory of the BLOB file, so that the directory cannot be created
    entries.emplace_back("pwal_0000", blob_directory.lexically_relative(location_), false);
    entries.emplace_back(blob_destination.filename(), blob_destination.filename(), false);

    EXPECT_EQ(datastore_->restore(backup_.string(), entries), limestone::status::err_permission_error);
    EXPECT_TRUE(boost::filesystem::is_regular_file(blob_directory));
}

}  // namespace limestone::testing
