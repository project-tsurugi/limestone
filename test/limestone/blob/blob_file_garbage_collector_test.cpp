#include <gtest/gtest.h>
#include <boost/filesystem.hpp>
#include <fstream>
#include <sstream>
#include <iomanip>
#include <cstdlib> // system()
#include "blob_file_garbage_collector.h"
#include "limestone/logging.h"
#include "blob_file_resolver.h"

namespace limestone::testing {

using namespace limestone::internal;
using limestone::api::blob_id_type;

constexpr const char* base_directory = "/tmp/blob_file_gc_test";


class testable_blob_file_garbage_collector : public blob_file_garbage_collector {
public:
    explicit testable_blob_file_garbage_collector(const blob_file_resolver &resolver)
        : blob_file_garbage_collector(resolver) {}
    using blob_file_garbage_collector::wait_for_scan;
    using blob_file_garbage_collector::wait_for_cleanup;
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

// Helper function: From a blob_item_container reference, create a sorted list of blob IDs.
std::vector<blob_id_type> get_sorted_blob_ids(const blob_item_container &container) {
    std::vector<blob_id_type> ids;
    for (const auto &item : container) {
        ids.push_back(item.get_blob_id());
    }
    std::sort(ids.begin(), ids.end());
    return ids;
}

class blob_file_garbage_collector_test : public ::testing::Test {
protected:
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
    }

    void TearDown() override {
        gc_->shutdown();
        gc_.reset();
        resolver_.reset();
        std::string cmd = "rm -rf " + std::string(base_directory);
        if (std::system(cmd.c_str()) != 0) {
            std::cerr << "Cannot remove directory" << std::endl;
        }
    }

    std::unique_ptr<blob_file_resolver> resolver_;
    std::unique_ptr<testable_blob_file_garbage_collector> gc_;
};

// Helper function: Create a file for the specified blob_id in the appropriate subdirectory.
// The file content can be empty.
void create_blob_file(const blob_file_resolver &resolver, blob_id_type id) {
    // The file path should be obtained with resolver->resolve_path(id).
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
    gc_->start_scan(500);
    gc_->wait_for_scan();

    // Get scan results
    auto actual_ids = get_sorted_blob_ids(gc_->get_blob_file_list());

    // Expected result is only the paths of files with blob_id 100, 200, 300
    // Each file path should be generated with resolver_->resolve_path(blob_id)
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
    gc_->start_scan(500);
    gc_->wait_for_scan();

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

    gc_->start_scan(1000); // Specify a sufficiently large value for max_existing_blob_id
    gc_->wait_for_scan();

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
    gc_->start_scan(100);
    gc_->wait_for_scan();

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
    gc_->start_scan(99);
    gc_->wait_for_scan();

    auto actual_ids = get_sorted_blob_ids(gc_->get_blob_file_list());
    EXPECT_TRUE(actual_ids.empty());
}

TEST_F(blob_file_garbage_collector_test, start_scan_called_twice_throws) {
    // Start scanning for the first time.
    gc_->start_scan(1000);

    // A second call to start_scan() should throw std::logic_error.
    EXPECT_THROW(gc_->start_scan(1000), std::logic_error);

    gc_->wait_for_scan();
}

TEST_F(blob_file_garbage_collector_test, scan_catches_exception_when_directory_missing) {
    // Remove the blob root directory to simulate the directory not existing.
    boost::filesystem::remove_all(resolver_->get_blob_root());

    // Start scanning. Even though the directory does not exist and an exception will occur,
    // scan_directory() should catch the exception, log the error, and mark the scan as complete.
    EXPECT_NO_THROW({
        gc_->start_scan(1000);
        gc_->wait_for_scan();
    });

    // Verify that get_blob_file_list() returns an empty list.
    auto actual_ids = get_sorted_blob_ids(gc_->get_blob_file_list());
    EXPECT_TRUE(actual_ids.empty());
}

TEST_F(blob_file_garbage_collector_test, add_gc_exempt_blob_item_adds_item_correctly) {
    // Arrange: Create a blob_item with a test blob_id.
    blob_id_type test_id = 123;
    blob_item test_item(test_id);

    // Act: Add the test blob_item to the gc_exempt_blob_ container.
    gc_->add_gc_exempt_blob_item(test_item);

    // Assert: Verify that the container now includes the test blob_item.
    const blob_item_container &exempt_items = gc_->get_gc_exempt_blob_list();
    auto actual_ids = get_sorted_blob_ids(exempt_items);
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
    gc_->start_scan(200);
    gc_->wait_for_scan();

    // Mark blob 102 as GC exempt.
    gc_->add_gc_exempt_blob_item(blob_item(102));

    // Act:
    // Call finalize_scan_and_cleanup, which spawns a detached thread that will wait
    // for the scan to complete and then delete non-exempt blob files.
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
    gc_->start_scan(600);
    gc_->wait_for_scan();

    // Set up test file operations to simulate a deletion failure for blob ID 501.
    class TestFileOperations : public real_file_operations {
    public:
        TestFileOperations(blob_id_type fail_id, blob_file_resolver* resolver) : real_file_operations(), fail_id_(fail_id), resolver_(resolver) {}

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

    // Call finalize_scan_and_cleanup, which spawns a detached thread that will wait
    // for the scan to complete and then delete non-exempt blob files.
    gc_->finalize_scan_and_cleanup();

    // Wait for the cleanup process to complete.
    gc_->wait_for_cleanup();

    // Assert:
    // Expect the deletion of blob ID 501 to have failed, so the file should still exist.
    EXPECT_TRUE(boost::filesystem::exists(resolver_->resolve_path(501)));

    // Expect the deletion of blob ID 502 to have succeeded, so the file should not exist.
    EXPECT_FALSE(boost::filesystem::exists(resolver_->resolve_path(502)));
}

// Test: Calling start_scan() after wait_for_scan() has been invoked should throw an exception.
TEST_F(blob_file_garbage_collector_test, start_scan_after_wait_for_scan_should_throw) {
    // Call wait_for_scan() without starting scan.
    gc_->wait_for_scan();
    // Since wait_for_scan() sets scan_waited_, starting the scan now should throw.
    EXPECT_THROW(gc_->start_scan(1000), std::logic_error);
}

// Test: Calling finalize_scan_and_cleanup() after wait_for_cleanup() has been invoked does nothing.
// (i.e. cleanup is not started if wait_for_cleanup() has been called first.)
TEST_F(blob_file_garbage_collector_test, finalize_cleanup_after_wait_for_cleanup_does_nothing) {
    // Call wait_for_cleanup() first, which sets cleanup_waited_.
    gc_->wait_for_cleanup();
    // Now, finalize_scan_and_cleanup() should check cleanup_waited_ and refrain from starting cleanup.
    // This call should return immediately (and not hang).
    gc_->finalize_scan_and_cleanup();
    // Also, calling wait_for_cleanup() again should return immediately.
    gc_->wait_for_cleanup();
    SUCCEED();  // If we reach here, the test passes.
}

// Test: Calling wait_for_scan() twice does not block.
TEST_F(blob_file_garbage_collector_test, wait_for_scan_called_twice) {
    gc_->start_scan(1000);
    gc_->wait_for_scan();
    // Second call should return immediately.
    gc_->wait_for_scan();
    SUCCEED();
}

// Test: Calling wait_for_cleanup() twice does not block.
TEST_F(blob_file_garbage_collector_test, wait_for_cleanup_called_twice) {
    gc_->start_scan(1000);
    gc_->wait_for_scan();
    gc_->finalize_scan_and_cleanup();
    gc_->wait_for_cleanup();
    // Second call should return immediately.
    gc_->wait_for_cleanup();
    SUCCEED();
}


}  // namespace limestone::testing
