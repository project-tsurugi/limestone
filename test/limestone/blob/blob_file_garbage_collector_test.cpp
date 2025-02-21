#include <gtest/gtest.h>
#include <boost/filesystem.hpp>
#include <fstream>
#include <sstream>
#include <iomanip>
#include <cstdlib> 
#include "test_root.h"
#include "log_entry.h"
#include "blob_file_garbage_collector.h"
#include "limestone/logging.h"
#include "blob_file_resolver.h"

namespace limestone::testing {

using namespace limestone::internal;
using namespace limestone::api;

constexpr const char* base_directory = "/tmp/blob_file_gc_test";
const boost::filesystem::path snapshot_path("/tmp/blob_file_gc_test/pwal_0000");
const boost::filesystem::path compacted_path("/tmp/blob_file_gc_test/pwal_0001");

class testable_blob_file_garbage_collector : public blob_file_garbage_collector {
public:
    explicit testable_blob_file_garbage_collector(const blob_file_resolver& resolver)
        : blob_file_garbage_collector(resolver) {}
    using blob_file_garbage_collector::wait_for_blob_file_scan;
    using blob_file_garbage_collector::wait_for_cleanup;
    using blob_file_garbage_collector::wait_for_scan_snapshot;
    using blob_file_garbage_collector::set_file_operations;
    using blob_file_garbage_collector::get_blob_file_list;
    using blob_file_garbage_collector::get_gc_exempt_blob_list;
};

// Helper function: Generate blob file name (16-digit hexadecimal + ".blob") from the specified blob_id
std::string generate_blob_filename(blob_id_type id) {
    std::ostringstream oss;
    oss << std::hex << std::setw(16) << std::setfill('0') << id << ".blob";
    return oss.str();
}

// Helper function: From a blob_id_container reference, create a sorted list of blob IDs.
std::vector<blob_id_type> get_sorted_blob_ids(const blob_id_container &container) {
    std::vector<blob_id_type> ids;
    for (const auto &id : container) {
        ids.push_back(id);
    }
    std::sort(ids.begin(), ids.end());
    return ids;
}

class blob_file_garbage_collector_test : public ::testing::Test {
protected:
    std::unique_ptr<api::datastore_test> datastore_;
    boost::filesystem::path location_;
    log_channel* lc0_{};
    log_channel* lc1_{};

    void SetUp() override {
        // Delete and recreate the test directory
        std::string cmd = "rm -rf " + std::string(base_directory);
        if (std::system(cmd.c_str()) != 0) {
            std::cerr << "Cannot remove directory" << std::endl;
        }
        cmd = "mkdir -p " + std::string(base_directory);
        if (std::system(cmd.c_str()) != 0) {
            std::cerr << "Cannot create directory" << std::endl;
        }

        // Create blob_file_resolver
        resolver_ = std::make_unique<blob_file_resolver>(
            boost::filesystem::path(base_directory), 10 /* directory count */
        );

        // Create blob directory (the directory returned by resolver_->get_blob_root())
        boost::filesystem::create_directories(resolver_->get_blob_root());

        // Also create subdirectories (referenced in precompute_directory_cache() within resolver_)
        for (std::size_t i = 0; i < 10; ++i) {
            std::ostringstream dir_name;
            dir_name << "dir_" << std::setw(2) << std::setfill('0') << i;
            boost::filesystem::path subdir = resolver_->get_blob_root() / dir_name.str();
            boost::filesystem::create_directories(subdir);
        }

        // Create blob_file_garbage_collector (using resolver_)
        gc_ = std::make_unique<testable_blob_file_garbage_collector>(*resolver_);

        // Create an empty file
        std::ofstream ofs(snapshot_path.string());
        ofs.close();
        ofs.open(compacted_path.string());
        ofs.close();
    }

    void TearDown() override {
        if (datastore_) {
            datastore_->shutdown();
        }

        gc_->shutdown();
        gc_.reset();
        resolver_.reset();
        std::string cmd = "rm -rf " + std::string(base_directory);
        if (std::system(cmd.c_str()) != 0) {
            std::cerr << "Cannot remove directory" << std::endl;
        }
    }

    void gen_datastore() {
        std::vector<boost::filesystem::path> data_locations{};
        data_locations.emplace_back(base_directory);
        boost::filesystem::path metadata_location_path{base_directory};
        limestone::api::configuration conf(data_locations, metadata_location_path);

        datastore_ = std::make_unique<limestone::api::datastore_test>(conf);

        lc0_ = &datastore_->create_channel(base_directory);
        lc1_ = &datastore_->create_channel(base_directory);
        datastore_->ready();
    }

    std::unique_ptr<blob_file_resolver> resolver_;
    std::unique_ptr<testable_blob_file_garbage_collector> gc_;
};

// Helper function: Create a file for the specified blob_id in the appropriate subdirectory.
// The file content can be empty.
void create_blob_file(const blob_file_resolver &resolver, blob_id_type id) {
    // The file path should be obtained with resolver.resolve_path(id).
    boost::filesystem::path file_path = resolver.resolve_path(id);
    // Assume the subdirectory exists for file creation (created in SetUp)
    std::ofstream ofs(file_path.string());
    ofs << "dummy data";
    ofs.close();
}

// Test case: Files with blob_id less than or equal to the target are scanned
TEST_F(blob_file_garbage_collector_test, scan_collects_only_files_with_blob_id_leq_max) {
    // Create some blob_files for testing.
    // Specify blob_id as 100, 200, 300, 600.
    // Since max_existing_blob_id is 500, the file with 600 will be excluded.
    create_blob_file(*resolver_, 100);
    create_blob_file(*resolver_, 200);
    create_blob_file(*resolver_, 300);
    create_blob_file(*resolver_, 600); // Excluded as a new file

    // Start scan: max_existing_blob_id = 500
    gc_->scan_blob_files(500);
    gc_->wait_for_blob_file_scan();

    // Get scan results
    auto actual_ids = get_sorted_blob_ids(gc_->get_blob_file_list());

    // Expected result is only the files with blob_id 100, 200, 300
    ASSERT_EQ(actual_ids.size(), 3);
    EXPECT_EQ(actual_ids[0], 100);
    EXPECT_EQ(actual_ids[1], 200);
    EXPECT_EQ(actual_ids[2], 300);
}

// Test case: Invalid files (not in blob_file format) are ignored
TEST_F(blob_file_garbage_collector_test, scan_ignores_invalid_files) {
    // Mix valid blob_files with invalid files
    create_blob_file(*resolver_, 150); // valid
    // Invalid file: different extension
    {
        std::ostringstream filename;
        filename << std::hex << std::setw(16) << std::setfill('0') << 250 << ".dat";
        boost::filesystem::path invalid_path = resolver_->get_blob_root() / "dir_00" / filename.str();
        std::ofstream ofs(invalid_path.string());
        ofs << "invalid data";
        ofs.close();
    }
    // Invalid file: insufficient digits
    {
        boost::filesystem::path invalid_path = resolver_->get_blob_root() / "dir_00" / "1234.blob";
        std::ofstream ofs(invalid_path.string());
        ofs << "invalid data";
        ofs.close();
    }
    // Start scan: max_existing_blob_id = 500
    gc_->scan_blob_files(500);
    gc_->wait_for_blob_file_scan();

    // Expected result is only the file with blob_id 150
    auto actual_ids = get_sorted_blob_ids(gc_->get_blob_file_list());
    ASSERT_EQ(actual_ids.size(), 1);
    EXPECT_EQ(actual_ids[0], 150);
}

// Test case: get_blob_file_list() returns the correct list after scan completion
TEST_F(blob_file_garbage_collector_test, get_blob_file_list_after_scan) {
    // Create multiple valid blob_files
    create_blob_file(*resolver_, 10);
    create_blob_file(*resolver_, 20);
    create_blob_file(*resolver_, 30);

    gc_->scan_blob_files(1000); // Specify a sufficiently large value for max_existing_blob_id
    gc_->wait_for_blob_file_scan();

    // Expected files are those with blob_id 10, 20, 30
    auto actual_ids = get_sorted_blob_ids(gc_->get_blob_file_list());
    ASSERT_EQ(actual_ids.size(), 3);
    EXPECT_EQ(actual_ids[0], 10);
    EXPECT_EQ(actual_ids[1], 20);
    EXPECT_EQ(actual_ids[2], 30);
}

TEST_F(blob_file_garbage_collector_test, max_existing_blob_id_inclusive) {
    // Create a blob file with blob_id 100.
    create_blob_file(*resolver_, 100);
    // Create another blob file with blob_id 200.
    create_blob_file(*resolver_, 200);

    // Start scan with max_existing_blob_id exactly equal to 100.
    // Expected: Only the file with blob_id 100 is collected (since 200 > 100).
    gc_->scan_blob_files(100);
    gc_->wait_for_blob_file_scan();

    auto actual_ids = get_sorted_blob_ids(gc_->get_blob_file_list());
    ASSERT_EQ(actual_ids.size(), 1);
    EXPECT_EQ(actual_ids[0], 100);
}

TEST_F(blob_file_garbage_collector_test, max_existing_blob_id_exclusive) {
    // Create a blob file with blob_id 100.
    create_blob_file(*resolver_, 100);
    // Create another blob file with blob_id 200.
    create_blob_file(*resolver_, 200);

    // Start scan with max_existing_blob_id set to 99.
    // Expected: Neither file should be collected because both 100 and 200 exceed 99.
    gc_->scan_blob_files(99);
    gc_->wait_for_blob_file_scan();

    auto actual_ids = get_sorted_blob_ids(gc_->get_blob_file_list());
    EXPECT_TRUE(actual_ids.empty());
}

TEST_F(blob_file_garbage_collector_test, start_scan_called_twice_throws) {
    // Start scanning for the first time.
    gc_->scan_blob_files(1000);

    // A second call to scan_blob_files() should throw std::logic_error.
    EXPECT_THROW(gc_->scan_blob_files(1000), std::logic_error);

    gc_->wait_for_blob_file_scan();
}

TEST_F(blob_file_garbage_collector_test, scan_catches_exception_when_directory_missing) {
    // Remove the blob root directory to simulate the directory not existing.
    boost::filesystem::remove_all(resolver_->get_blob_root());

    // Start scanning. Even though the directory does not exist and an exception will occur,
    // scan_directory() should catch the exception, log the error, and mark the scan as complete.
    EXPECT_NO_THROW({
        gc_->scan_blob_files(1000);
        gc_->wait_for_blob_file_scan();
    });

    // Verify that get_blob_file_list() returns an empty list.
    auto actual_ids = get_sorted_blob_ids(gc_->get_blob_file_list());
    EXPECT_TRUE(actual_ids.empty());
}

TEST_F(blob_file_garbage_collector_test, add_gc_exempt_blob_id_adds_id_correctly) {
    // Arrange: Define a test blob_id.
    blob_id_type test_id = 123;

    // Act: Add the test blob_id to the GC exempt container.
    gc_->add_gc_exempt_blob_id(test_id);

    // Assert: Verify that the container now includes the test blob_id.
    const blob_id_container &exempt_ids = gc_->get_gc_exempt_blob_list();
    auto actual_ids = get_sorted_blob_ids(exempt_ids);
    std::vector<blob_id_type> expected_ids = { test_id };
    EXPECT_EQ(actual_ids, expected_ids);
}

TEST_F(blob_file_garbage_collector_test, finalize_scan_and_cleanup_deletes_non_exempt_files) {
    // Arrange:
    // Create dummy blob files with blob IDs 101, 102, 103.
    create_blob_file(*resolver_, 101);
    create_blob_file(*resolver_, 102);
    create_blob_file(*resolver_, 103);

    // Assume that all files have blob IDs <= 200 so that they are included in the scanned list.
    gc_->scan_blob_files(200);
    gc_->wait_for_blob_file_scan();

    // Mark blob 102 as GC exempt.
    gc_->add_gc_exempt_blob_id(102);

    // Act:
    // Call finalize_scan_and_cleanup, which spawns a thread that will delete non-exempt blob files.
    gc_->finalize_scan_and_cleanup();

    // Wait for the cleanup process to complete.
    gc_->wait_for_cleanup();

    // Assert:
    // Expect non-exempt blob files to have been deleted.
    EXPECT_FALSE(boost::filesystem::exists(resolver_->resolve_path(101)));
    EXPECT_FALSE(boost::filesystem::exists(resolver_->resolve_path(103)));

    // The GC exempt blob (102) should still exist.
    EXPECT_TRUE(boost::filesystem::exists(resolver_->resolve_path(102)));
}

TEST_F(blob_file_garbage_collector_test, finalize_scan_and_cleanup_handles_deletion_failure) {
    // Arrange:
    // Create dummy blob files with blob IDs 501 and 502.
    create_blob_file(*resolver_, 501);
    create_blob_file(*resolver_, 502);

    // Assume that all files have blob IDs <= 600 so that they are included in the scanned list.
    gc_->scan_blob_files(600);
    gc_->wait_for_blob_file_scan();

    // Set up test file operations to simulate a deletion failure for blob ID 501.
    class TestFileOperations : public real_file_operations {
    public:
        TestFileOperations(blob_id_type fail_id, blob_file_resolver* resolver)
            : real_file_operations(), fail_id_(fail_id), resolver_(resolver) {}

        void remove(const boost::filesystem::path& path, boost::system::error_code& ec) override {
            auto id = resolver_->extract_blob_id(path);
            if (id == fail_id_) {
                ec = boost::system::errc::make_error_code(boost::system::errc::permission_denied);
            } else {
                real_file_operations::remove(path, ec);
            }
        }

    private:
        blob_id_type fail_id_;
        blob_file_resolver* resolver_;
    };

    blob_id_type fail_id = 501;
    blob_file_resolver* resolver = resolver_.get(); // Capture resolver_ pointer
    gc_->set_file_operations(std::make_unique<TestFileOperations>(fail_id, resolver));

    // Call finalize_scan_and_cleanup.
    gc_->finalize_scan_and_cleanup();

    // Wait for the cleanup process to complete.
    gc_->wait_for_cleanup();

    // Assert:
    // Expect the deletion of blob ID 501 to have failed, so the file should still exist.
    EXPECT_TRUE(boost::filesystem::exists(resolver_->resolve_path(501)));

    // Expect the deletion of blob ID 502 to have succeeded, so the file should not exist.
    EXPECT_FALSE(boost::filesystem::exists(resolver_->resolve_path(502)));
}

// Test: Calling scan_blob_files() after wait_for_blob_file_scan() has been invoked should throw an exception.
TEST_F(blob_file_garbage_collector_test, start_scan_after_wait_for_scan_should_throw) {
    // Call wait_for_blob_file_scan() without starting scan.
    gc_->wait_for_blob_file_scan();
    // Since wait_for_blob_file_scan() sets a flag, starting the scan now should throw.
    EXPECT_THROW(gc_->scan_blob_files(1000), std::logic_error);
}

// Test: Calling wait_for_blob_file_scan() twice does not block.
TEST_F(blob_file_garbage_collector_test, wait_for_scan_called_twice) {
    gc_->scan_blob_files(1000);
    gc_->wait_for_blob_file_scan();
    // Second call should return immediately.
    gc_->wait_for_blob_file_scan();
    SUCCEED();
}

// Test: Calling wait_for_cleanup() twice does not block.
TEST_F(blob_file_garbage_collector_test, wait_for_cleanup_called_twice) {
    gc_->scan_blob_files(1000);
    gc_->wait_for_blob_file_scan();
    gc_->finalize_scan_and_cleanup();
    gc_->wait_for_cleanup();
    // Second call should return immediately.
    gc_->wait_for_cleanup();
    SUCCEED();
}

// Test: snapshot_scan completes successfully and wait_for_scan_snapshot returns normally.
TEST_F(blob_file_garbage_collector_test, snapshot_scan_completes_properly) {
    // Act: Start the snapshot scan and wait for it to complete.
    gc_->scan_snapshot(snapshot_path, compacted_path);
    gc_->wait_for_scan_snapshot();

    // Assert:
    // If wait_for_scan_snapshot() returns, the background scan has completed.
    SUCCEED();
}

// Test: Calling scan_snapshot() twice throws an exception.
TEST_F(blob_file_garbage_collector_test, snapshot_scan_called_twice_throws) {
    gc_->scan_snapshot(snapshot_path, compacted_path);
    // The second call should throw an exception.
    EXPECT_THROW(gc_->scan_snapshot(snapshot_path, compacted_path), std::logic_error);
    gc_->wait_for_scan_snapshot();
}

// Test: Calling wait_for_scan_snapshot() without starting snapshot scan returns immediately.
TEST_F(blob_file_garbage_collector_test, wait_for_snapshot_without_scan_returns_immediately) {
    // Even if snapshot scanning has not started, wait_for_scan_snapshot() should return immediately.
    gc_->wait_for_scan_snapshot();
    SUCCEED();
}

// Test: Calling wait_for_scan_snapshot() twice does not block.
TEST_F(blob_file_garbage_collector_test, wait_for_snapshot_called_twice) {
    gc_->scan_snapshot(snapshot_path, compacted_path);
    gc_->wait_for_scan_snapshot();
    // Second call should return immediately.
    gc_->wait_for_scan_snapshot();
    SUCCEED();
}

TEST_F(blob_file_garbage_collector_test, full_process_test) {
    // Step 1: Create multiple BLOB files with blob IDs 100, 200, 300, and 400.
    create_blob_file(*resolver_, 100);
    create_blob_file(*resolver_, 200);
    create_blob_file(*resolver_, 300);
    create_blob_file(*resolver_, 400);

    // Step 2: Call scan_blob_files with max_existing_blob_id set to 1000 so that all files are included.
    gc_->scan_blob_files(1000);

    // Step 3: Wait for the BLOB file scanning to complete.
    gc_->wait_for_blob_file_scan();

    // Step 4: Create the snapshot and compacted files.
    // In this test, two PWAL files are generated and used as the snapshot file and the compacted file.
    // The PWAL files include entries with blob IDs 200 and 400.
    boost::filesystem::remove(snapshot_path);
    boost::filesystem::remove(compacted_path);
    gen_datastore();
    lc0_->begin_session();
    lc0_->add_entry(1, "key1", "value1", {1,1}, {200});
    lc0_->end_session();  
    lc1_->begin_session();
    lc1_->add_entry(1, "key2", "value2", {1,1}, {400});
    lc1_->end_session();
    ASSERT_TRUE(boost::filesystem::exists(snapshot_path));
    ASSERT_TRUE(boost::filesystem::exists(compacted_path));

    // Step 5: Call scan_snapshot using both the snapshot file and the compacted file.
    gc_->scan_snapshot(snapshot_path, compacted_path);

    // Step 6: Wait for the snapshot scanning to complete.
    gc_->wait_for_scan_snapshot();

    // Step 7: Verify that the GC-exempt container contains the correct entries (i.e., blob IDs 200 and 400).
    auto exempt_ids = get_sorted_blob_ids(gc_->get_gc_exempt_blob_list());
    std::vector<blob_id_type> expected_exempt = {200, 400};
    EXPECT_EQ(exempt_ids, expected_exempt);

    // Step 8: Wait for the cleanup process to complete.
    gc_->wait_for_cleanup();

    // Step 9: Verify that the intended files have been deleted:
    //         - Files for blob IDs 100 and 300 (non-GC exempt) should be deleted.
    //         - Files for blob IDs 200 and 400 (GC exempt) should remain.
    boost::filesystem::path file100 = resolver_->resolve_path(100);
    boost::filesystem::path file200 = resolver_->resolve_path(200);
    boost::filesystem::path file300 = resolver_->resolve_path(300);
    boost::filesystem::path file400 = resolver_->resolve_path(400);

    EXPECT_FALSE(boost::filesystem::exists(file100));
    EXPECT_TRUE(boost::filesystem::exists(file200));
    EXPECT_FALSE(boost::filesystem::exists(file300));
    EXPECT_TRUE(boost::filesystem::exists(file400));
}

// Test: When compacted_path does not exist, scan_snapshot processes snapshot_path and reads expected data.
TEST_F(blob_file_garbage_collector_test, snapshot_scan_reads_expected_data) {
    // Arrange:
    // Ensure that compacted_path does not exist
    if (boost::filesystem::exists(compacted_path)) {
        boost::filesystem::remove(compacted_path);
    }
    EXPECT_FALSE(boost::filesystem::exists(compacted_path));
    EXPECT_TRUE(boost::filesystem::exists(snapshot_path));

    // Act:
    // Since compacted_path does not exist, scan_snapshot should only process snapshot_path.
    EXPECT_NO_THROW({
        gc_->scan_snapshot(snapshot_path, compacted_path);
        gc_->wait_for_scan_snapshot();
    });

    SUCCEED();
}

TEST_F(blob_file_garbage_collector_test, snapshot_scan_throws_when_snapshot_file_missing) {
    // Arrange:
    // Ensure that snapshot_path does not exist (simulate a missing snapshot file)
    if (boost::filesystem::exists(snapshot_path)) {
        boost::filesystem::remove(snapshot_path);
    }
    EXPECT_FALSE(boost::filesystem::exists(snapshot_path));

    // Act & Assert:
    // Since snapshot_path is missing, scan_snapshot should throw an exception.
    EXPECT_THROW({
        gc_->scan_snapshot(snapshot_path, compacted_path);
        gc_->wait_for_scan_snapshot();
    }, std::exception);
}


class blob_file_gc_state_machine_test : public ::testing::Test {
protected:
    blob_file_gc_state_machine state_machine_;

    /**
     * @brief Verifies that a valid state transition occurs.
     */
    void verify_transition(blob_file_gc_state initial_state, blob_file_gc_event event, bool should_throw) {
        state_machine_.force_set_state(initial_state);

        if (should_throw) {
            EXPECT_THROW(state_machine_.transition(event), std::logic_error)
                << "Unexpectedly allowed transition from " 
                << blob_file_gc_state_machine::to_string(initial_state) 
                << " with event " << static_cast<int>(event);
        } else {
            EXPECT_NO_THROW({
                blob_file_gc_state new_state = state_machine_.transition(event);
                EXPECT_NE(new_state, initial_state) 
                    << "Transition did not change state: " 
                    << blob_file_gc_state_machine::to_string(initial_state) 
                    << " -> " << blob_file_gc_state_machine::to_string(new_state);
            });
        }
    }
};

TEST_F(blob_file_gc_state_machine_test, test_all_state_transitions) {
    for (int s = static_cast<int>(blob_file_gc_state::not_started);
         s <= static_cast<int>(blob_file_gc_state::shutdown); ++s) {
        
        blob_file_gc_state current_state = static_cast<blob_file_gc_state>(s);
        

        for (int e = static_cast<int>(blob_file_gc_event::start_blob_scan);
             e <= static_cast<int>(blob_file_gc_event::reset); ++e) {

            blob_file_gc_event event = static_cast<blob_file_gc_event>(e);
            auto expected_next_state = state_machine_.get_next_state_if_valid(current_state, event);

            state_machine_.force_set_state(current_state);
            std::cerr << "[TEST] State: " << blob_file_gc_state_machine::to_string(current_state)
            << ", Event: " << blob_file_gc_state_machine::to_string(event)
            << " -> Expected: "
            << (expected_next_state ? blob_file_gc_state_machine::to_string(*expected_next_state) : "Exception (std::logic_error)")
            << std::endl;

            if (expected_next_state) {
                blob_file_gc_state new_state;
                state_machine_.force_set_state(current_state);
                EXPECT_NO_THROW(new_state = state_machine_.transition(event))
                    << "Valid transition failed: " 
                    << blob_file_gc_state_machine::to_string(current_state)
                    << " -> " << blob_file_gc_state_machine::to_string(*expected_next_state);

                EXPECT_EQ(new_state, *expected_next_state)
                    << "Transition result does not match expected state: " 
                    << blob_file_gc_state_machine::to_string(current_state)
                    << " -> " << blob_file_gc_state_machine::to_string(new_state);
            } else {
                EXPECT_THROW(state_machine_.transition(event), std::logic_error)
                    << "Invalid transition did not throw: " 
                    << blob_file_gc_state_machine::to_string(current_state)
                    << " -> " << blob_file_gc_state_machine::to_string(event);
            }
        }
    }
}


/**
 * @brief Test: Reset should only be allowed from shutdown.
 */
TEST_F(blob_file_gc_state_machine_test, reset_only_allowed_from_shutdown) {
    for (int s = static_cast<int>(blob_file_gc_state::not_started);
            s <= static_cast<int>(blob_file_gc_state::shutdown); ++s) {

        blob_file_gc_state current_state = static_cast<blob_file_gc_state>(s);
        state_machine_.force_set_state(current_state);

        if (current_state == blob_file_gc_state::shutdown) {
            EXPECT_NO_THROW(state_machine_.transition(blob_file_gc_event::reset))
                << "Reset should be allowed from shutdown";
        } else {
            EXPECT_THROW(state_machine_.transition(blob_file_gc_event::reset), std::logic_error)
                << "Reset should only be allowed from shutdown, but was allowed from " 
                << blob_file_gc_state_machine::to_string(current_state);
        }
    }
}

/**
 * @brief Test: Shutdown should always be allowed.
 */
TEST_F(blob_file_gc_state_machine_test, shutdown_always_allowed) {
    for (int s = static_cast<int>(blob_file_gc_state::not_started);
            s <= static_cast<int>(blob_file_gc_state::completed); ++s) {

        blob_file_gc_state current_state = static_cast<blob_file_gc_state>(s);
        state_machine_.force_set_state(current_state);

        EXPECT_NO_THROW(state_machine_.transition(blob_file_gc_event::shutdown))
            << "Shutdown should always be allowed from " 
            << blob_file_gc_state_machine::to_string(current_state);
    }
}
    
        

}  // namespace limestone::testing
