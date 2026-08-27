#include <atomic>
#include <boost/filesystem.hpp>
#include <boost/filesystem/fstream.hpp>
#include <chrono>
#include <fstream>
#include <future>
#include <mutex>
#include <sstream>
#include <thread>

#include "datastore_impl.h"
#include "internal.h"
#include "limestone/api/limestone_exception.h"
#include "limestone_exception_helper.h"
#include "log_channel_impl.h"
#include "test_root.h"

namespace limestone::testing {

using namespace limestone::api;

// Unit tests for the WAL rotation mechanism (issue #139, design doc 03).
// A rotation request selects its targets by a directory scan, renames the
// files of inactive channels and orphan files on the spot, and waits for the
// files of active channels to be renamed at end_session.
class rotation_mechanism_test : public ::testing::Test {
public:
    static constexpr const char* location = "/tmp/rotation_mechanism_test";

    void SetUp() override {
        boost::filesystem::remove_all(location);
        if (!boost::filesystem::create_directory(location)) {
            std::cerr << "cannot make directory" << std::endl;
        }
        limestone::internal::setup_initial_logdir(boost::filesystem::path(location));
        regen_datastore();
    }

    void regen_datastore() {
        limestone::api::configuration conf{};
        conf.set_data_location(location);
        datastore_ = nullptr;
        datastore_ = std::make_unique<limestone::api::datastore_test>(conf);
    }

    void TearDown() override {
        // Restore the state so that a test failing halfway does not affect later tests.
        limestone::testing::enable_exception_throwing = false;
        boost::system::error_code ec;
        boost::filesystem::permissions(location, boost::filesystem::owner_all, ec);
        datastore_ = nullptr;
        boost::filesystem::remove_all(location);
    }

protected:
    std::unique_ptr<limestone::api::datastore_test> datastore_{};
    std::future<void> backup_future_{};

    // Starts begin_backup() (= a rotation request) on a background thread.
    // The rotation thread is stopped right after it reads the boundary epoch
    // (before the epoch_id_informed_ wait); switch_epoch(next_epoch) then makes
    // the informed catch-up possible, in_window runs on this thread, and the
    // rotation thread is released. Use finish_rotation() to wait for the end.
    // The callback outlives this function until finish_rotation(), so its
    // lifetime is guaranteed by capturing shared_ptrs by value (no reference
    // captures of local variables are left behind).
    void start_rotation(epoch_id_type next_epoch, std::function<void()> const& in_window) {
        auto reached = std::make_shared<std::atomic<bool>>(false);
        auto released = std::make_shared<std::atomic<bool>>(false);

        datastore_->on_rotate_log_files_callback = [reached, released]() {
            reached->store(true);
            while (!released->load()) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
        };

        backup_future_ = std::async(std::launch::async, [this] {
            (void) datastore_->begin_backup(backup_type::standard);
        });
        while (!reached->load()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }

        datastore_->switch_epoch(next_epoch);
        if (in_window) {
            in_window();
        }
        released->store(true);
    }

    void finish_rotation() {
        backup_future_.get();
        datastore_->on_rotate_log_files_callback = nullptr;
    }

    // Returns the rotated file with the given prefix (e.g. "pwal_0000."), or an empty path if none.
    [[nodiscard]] boost::filesystem::path find_rotated_pwal(std::string const& prefix) const {
        boost::filesystem::directory_iterator end;
        for (boost::filesystem::directory_iterator it{boost::filesystem::path(location)}; it != end; ++it) {
            std::string name = it->path().filename().string();
            if (name.rfind(prefix, 0) == 0 && name.length() > prefix.length()) {
                return it->path();
            }
        }
        return {};
    }

    [[nodiscard]] static bool file_contains(boost::filesystem::path const& path, std::string const& needle) {
        std::ifstream in(path.string(), std::ios::in | std::ios::binary);
        std::ostringstream buf;
        buf << in.rdbuf();
        return buf.str().find(needle) != std::string::npos;
    }
};

extern void create_file(const boost::filesystem::path& path, std::string_view content);

// Test matrix "rename of an inactive channel" and "lazy creation of the new
// file": the file of a channel with no running session is renamed on the spot
// at the request, and the file with the original name is not created until
// the next begin_session.
TEST_F(rotation_mechanism_test, inactive_channel_file_is_renamed_immediately) {
    log_channel& channel = datastore_->create_channel();  // pwal_0000
    datastore_->ready();
    datastore_->switch_epoch(2);
    channel.begin_session();
    channel.add_entry(1, "k1", "v1", {2, 0});
    channel.end_session();

    start_rotation(3, {});
    finish_rotation();

    // Renamed on the spot; no file with the original name exists.
    EXPECT_FALSE(boost::filesystem::exists(boost::filesystem::path(location) / "pwal_0000"));
    boost::filesystem::path rotated = find_rotated_pwal("pwal_0000.");
    ASSERT_FALSE(rotated.empty());
    EXPECT_TRUE(file_contains(rotated, "k1"));

    // The new file is created lazily by the next begin_session.
    channel.begin_session();
    EXPECT_TRUE(boost::filesystem::exists(boost::filesystem::path(location) / "pwal_0000"));
    channel.end_session();
}

// Test matrix "status setting and rename of an active channel" and
// "completion wait of a rotation request": the file of a channel active at the
// request is not renamed on the spot; the request blocks until end_session,
// which performs the rename.
TEST_F(rotation_mechanism_test, active_channel_file_is_renamed_at_end_session) {
    log_channel& channel0 = datastore_->create_channel();  // pwal_0000
    log_channel& channel1 = datastore_->create_channel();  // pwal_0001
    datastore_->ready();
    datastore_->switch_epoch(2);
    channel0.begin_session();
    channel0.add_entry(1, "k1", "v1", {2, 0});
    channel0.end_session();

    // Hook to detect that the request has entered the completion wait.
    std::atomic<bool> waiting{false};
    datastore_->get_impl()->set_on_rotate_before_wait_for_test([&waiting] { waiting.store(true); });

    // Issue the request while a session of epoch 3, newer than the rotation
    // boundary epoch (2), is open.
    start_rotation(3, [&] {
        channel1.begin_session();
        channel1.add_entry(1, "k2", "v2", {3, 0});
    });

    // Wait until the request enters the rename wait.
    while (!waiting.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    // The active channel's file is not renamed yet and the request has not completed.
    EXPECT_TRUE(boost::filesystem::exists(boost::filesystem::path(location) / "pwal_0001"));
    EXPECT_TRUE(find_rotated_pwal("pwal_0001.").empty());
    EXPECT_EQ(backup_future_.wait_for(std::chrono::milliseconds(50)), std::future_status::timeout);

    // end_session performs the rename and the request completes.
    channel1.end_session();
    finish_rotation();
    EXPECT_FALSE(boost::filesystem::exists(boost::filesystem::path(location) / "pwal_0001"));
    boost::filesystem::path rotated = find_rotated_pwal("pwal_0001.");
    ASSERT_FALSE(rotated.empty());
    EXPECT_TRUE(file_contains(rotated, "k2"));

    datastore_->get_impl()->set_on_rotate_before_wait_for_test(nullptr);
}

// Test matrix "rotation of orphan files": a pwal_NNNN bound to no channel is
// renamed at the request.
TEST_F(rotation_mechanism_test, orphan_pwal_is_rotated) {
    create_file(boost::filesystem::path(location) / "pwal_0099", "");
    regen_datastore();

    log_channel& channel = datastore_->create_channel();  // pwal_0000
    datastore_->ready();
    datastore_->switch_epoch(2);
    channel.begin_session();
    channel.add_entry(1, "k1", "v1", {2, 0});
    channel.end_session();

    start_rotation(3, {});
    finish_rotation();

    EXPECT_FALSE(boost::filesystem::exists(boost::filesystem::path(location) / "pwal_0099"));
    EXPECT_FALSE(find_rotated_pwal("pwal_0099.").empty());
}

// Test matrix "inspection and consumption at begin_session": with the status
// artificially set to "requested", begin_session consumes the request (renames
// the file) before the fopen (defense on a path that should be unreachable).
TEST_F(rotation_mechanism_test, begin_session_consumes_pending_rotation_request) {
    log_channel& channel = datastore_->create_channel();  // pwal_0000
    datastore_->ready();
    datastore_->switch_epoch(2);
    channel.begin_session();
    channel.add_entry(1, "k1", "v1", {2, 0});
    channel.end_session();

    {
        std::lock_guard<std::mutex> lock(datastore_->get_impl()->get_rotation_state().mutex);
        channel.get_impl()->set_rotation_requested_locked();
    }

    channel.begin_session();
    // The consumption renames the file before the fopen, so both the old and
    // the new files exist.
    boost::filesystem::path rotated = find_rotated_pwal("pwal_0000.");
    ASSERT_FALSE(rotated.empty());
    EXPECT_TRUE(file_contains(rotated, "k1"));
    EXPECT_TRUE(boost::filesystem::exists(boost::filesystem::path(location) / "pwal_0000"));
    channel.end_session();

    // A consumption on the defensive path is not a target of any rotation
    // request, so no renamed file is recorded.
    EXPECT_TRUE(datastore_->get_impl()->get_rotation_state().renamed_files.empty());
}

// Test matrix "race between the rename and begin_session": the single mutex
// guarantees that begin_session never opens a file being rotated.
TEST_F(rotation_mechanism_test, begin_session_does_not_open_file_being_rotated) {
    log_channel& channel = datastore_->create_channel();  // pwal_0000
    datastore_->ready();
    datastore_->switch_epoch(2);
    channel.begin_session();
    channel.add_entry(1, "k1", "v1", {2, 0});
    channel.end_session();

    // Stop the rotation thread between the decision and the rename (inside the
    // single mutex).
    std::atomic<bool> reached{false};
    std::atomic<bool> released{false};
    datastore_->get_impl()->set_on_rotate_before_rename_for_test([&] {
        reached.store(true);
        while (!released.load()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    });

    start_rotation(3, {});
    while (!reached.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    // A begin_session trying to open the not-yet-renamed file is blocked by the
    // single mutex.
    auto begin_future = std::async(std::launch::async, [&channel] { channel.begin_session(); });
    EXPECT_EQ(begin_future.wait_for(std::chrono::milliseconds(50)), std::future_status::timeout);

    released.store(true);
    begin_future.get();
    finish_rotation();

    // begin_session created and opened the new file after the rename, so writes
    // to it never leak into the rotated file.
    channel.add_entry(1, "k2", "v2", {3, 0});
    channel.end_session();
    boost::filesystem::path rotated = find_rotated_pwal("pwal_0000.");
    ASSERT_FALSE(rotated.empty());
    EXPECT_TRUE(file_contains(rotated, "k1"));
    EXPECT_FALSE(file_contains(rotated, "k2"));

    datastore_->get_impl()->set_on_rotate_before_rename_for_test(nullptr);
}

// shutdown makes a rotation request waiting for renames give up (it does not
// keep waiting). The statuses of the given-up channels are rolled back and the
// files stay unrotated.
TEST_F(rotation_mechanism_test, shutdown_aborts_rotation_waiting_for_inflight_session) {
    limestone::testing::enable_exception_throwing = true;

    log_channel& channel0 = datastore_->create_channel();  // pwal_0000
    log_channel& channel1 = datastore_->create_channel();  // pwal_0001
    datastore_->ready();
    datastore_->switch_epoch(2);
    channel0.begin_session();
    channel0.add_entry(1, "k1", "v1", {2, 0});
    channel0.end_session();

    std::atomic<bool> waiting{false};
    datastore_->get_impl()->set_on_rotate_before_wait_for_test([&waiting] { waiting.store(true); });

    start_rotation(3, [&] {
        channel1.begin_session();
        channel1.add_entry(1, "k2", "v2", {3, 0});
    });
    while (!waiting.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    // shutdown returns without hanging and the rotation request is given up
    // with an error.
    auto shutdown_future = std::async(std::launch::async, [this] { datastore_->shutdown().wait(); });
    EXPECT_EQ(shutdown_future.wait_for(std::chrono::seconds(10)), std::future_status::ready);
    EXPECT_THROW(backup_future_.get(), limestone_exception);

    // Cleanup of the give-up: the rename-waiting set is empty and the file
    // stays unrotated.
    EXPECT_TRUE(datastore_->get_impl()->get_rotation_state().pending_channels.empty());
    EXPECT_TRUE(boost::filesystem::exists(boost::filesystem::path(location) / "pwal_0001"));
    EXPECT_TRUE(find_rotated_pwal("pwal_0001.").empty());

    datastore_->on_rotate_log_files_callback = nullptr;
    datastore_->get_impl()->set_on_rotate_before_wait_for_test(nullptr);
    limestone::testing::enable_exception_throwing = false;
}

// shutdown also makes a rotation request waiting for the epoch catch-up
// (epoch_id_informed_) give up (the epoch never progresses once shirakami
// stops).
TEST_F(rotation_mechanism_test, shutdown_aborts_rotation_waiting_for_epoch) {
    limestone::testing::enable_exception_throwing = true;

    log_channel& channel = datastore_->create_channel();  // pwal_0000
    datastore_->ready();
    datastore_->switch_epoch(2);
    channel.begin_session();
    channel.add_entry(1, "k1", "v1", {2, 0});
    channel.end_session();

    // The epoch is kept at 2, so the rotation cannot leave the catch-up wait
    // for epoch_id_informed_ >= 2.
    std::atomic<bool> reached{false};
    datastore_->on_rotate_log_files_callback = [&reached] { reached.store(true); };
    backup_future_ = std::async(std::launch::async, [this] {
        (void) datastore_->begin_backup(backup_type::standard);
    });
    while (!reached.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    auto shutdown_future = std::async(std::launch::async, [this] { datastore_->shutdown().wait(); });
    EXPECT_EQ(shutdown_future.wait_for(std::chrono::seconds(10)), std::future_status::ready);
    EXPECT_THROW(backup_future_.get(), limestone_exception);

    datastore_->on_rotate_log_files_callback = nullptr;
    limestone::testing::enable_exception_throwing = false;
}

// Test matrix "process exit on rename failure" (channel-operating thread
// side): the process exits when the consumption rename in end_session fails.
TEST_F(rotation_mechanism_test, rename_failure_in_end_session_aborts_process) {
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";

    log_channel& channel = datastore_->create_channel();  // pwal_0000
    datastore_->ready();
    datastore_->switch_epoch(2);
    channel.begin_session();
    channel.add_entry(1, "k1", "v1", {2, 0});

    {
        std::lock_guard<std::mutex> lock(datastore_->get_impl()->get_rotation_state().mutex);
        channel.get_impl()->set_rotation_requested_locked();
    }

    EXPECT_DEATH({
        // Drop the write permission of the directory to make the rename fail.
        boost::filesystem::permissions(location,
            boost::filesystem::owner_read | boost::filesystem::owner_exe);
        channel.end_session();
    }, "Failed to rename file");

    boost::filesystem::permissions(location, boost::filesystem::owner_all);
}

// Test matrix "process exit on rename failure" (rotation thread side): the
// process exits when an immediate rename at the request fails.
TEST_F(rotation_mechanism_test, rename_failure_in_rotation_thread_aborts_process) {
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";

    log_channel& channel = datastore_->create_channel();  // pwal_0000
    datastore_->ready();
    datastore_->switch_epoch(2);
    channel.begin_session();
    channel.add_entry(1, "k1", "v1", {2, 0});
    channel.end_session();

    EXPECT_DEATH({
        // Drop the write permission of the directory between the decision and
        // the rename to make the rename fail.
        datastore_->get_impl()->set_on_rotate_before_rename_for_test([this] {
            boost::filesystem::permissions(location,
                boost::filesystem::owner_read | boost::filesystem::owner_exe);
        });
        start_rotation(3, {});
        finish_rotation();
    }, "Failed to rename file");

    boost::filesystem::permissions(location, boost::filesystem::owner_all);
}

}  // namespace limestone::testing
