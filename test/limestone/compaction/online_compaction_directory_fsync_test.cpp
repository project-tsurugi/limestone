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

#include <cerrno>
#include <functional>
#include <mutex>
#include <string>
#include <vector>

#include <boost/filesystem.hpp>

#include "compaction_test_fixture.h"
#include "datastore_impl.h"
#include "file_operations.h"

namespace limestone::testing {

using limestone::api::datastore_impl;
using limestone::internal::real_file_operations;

namespace {

constexpr const char* test_location = "/tmp/online_compaction_directory_fsync_test";

// records the last directory passed to fsync_directory, and makes it fail for the directories for which fail_if
// returns true. It is also called from the compaction thread, so it is guarded by a mutex
class directory_sync_recorder : public real_file_operations {
public:
    int fsync_directory(const boost::filesystem::path& dir) override {
        {
            std::scoped_lock lock(mutex_);
            last_ = dir;
            if (fail_if_ && fail_if_(dir)) {
                errno = EIO;
                return -1;
            }
        }
        return real_file_operations::fsync_directory(dir);
    }
    [[nodiscard]] boost::filesystem::path last() const {
        std::scoped_lock lock(mutex_);
        return last_;
    }
    void fail_if(std::function<bool(const boost::filesystem::path&)> predicate) {
        std::scoped_lock lock(mutex_);
        fail_if_ = std::move(predicate);
    }
private:
    mutable std::mutex mutex_;
    boost::filesystem::path last_;
    std::function<bool(const boost::filesystem::path&)> fail_if_;
};

}  // namespace

class online_compaction_directory_fsync_test : public compaction_test {
public:
    online_compaction_directory_fsync_test() : compaction_test(test_location) {}

    void SetUp() override {
        compaction_test::SetUp();
        datastore_impl::set_file_operations_for_test(&ops_);
    }

    void TearDown() override {
        compaction_test::TearDown();
        datastore_impl::set_file_operations_for_test(nullptr);
    }

    void write_and_make_durable(const std::string& key, const std::string& value, epoch_id_type write_epoch) {
        lc0_->begin_session();
        lc0_->add_entry(1, key, value, {write_epoch, 0});
        lc0_->end_session();
        datastore_->switch_epoch(write_epoch + 1);
    }

protected:
    directory_sync_recorder ops_{};
};

// the log directory is synchronized after the publication (the rename from the temporary directory) and before the
// catalog records it
TEST_F(online_compaction_directory_fsync_test, publish_synchronizes_directory) {
    gen_datastore();
    datastore_->switch_epoch(1);
    write_and_make_durable("k1", "v1", 1);

    bool synced_before_commit = false;
    datastore_->get_impl()->set_on_compaction_after_publish_for_test([this, &synced_before_commit]() {
        synced_before_commit = ops_.last() == boost::filesystem::path(test_location);
    });
    run_compact_with_epoch_switch(3);
    datastore_->get_impl()->set_on_compaction_after_publish_for_test(nullptr);

    EXPECT_TRUE(synced_before_commit);
}

// failing to synchronize the directory after the publication terminates the process
TEST_F(online_compaction_directory_fsync_test, publish_directory_sync_failure_terminates) {
    const std::string saved_style = ::testing::FLAGS_gtest_death_test_style;
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";
    EXPECT_DEATH(
        {
            gen_datastore();
            datastore_->switch_epoch(1);
            write_and_make_durable("k1", "v1", 1);
            // fail only after the compacted file has been published
            const boost::filesystem::path published = boost::filesystem::path(test_location) / "pwal_0000.compacted.1";
            ops_.fail_if([published](const boost::filesystem::path& dir) {
                return dir == published.parent_path() && boost::filesystem::exists(published);
            });
            run_compact_with_epoch_switch(3);
        },
        "online compaction failed at the publish/commit/delete phase");
    ::testing::FLAGS_gtest_death_test_style = saved_style;
}

}  // namespace limestone::testing
