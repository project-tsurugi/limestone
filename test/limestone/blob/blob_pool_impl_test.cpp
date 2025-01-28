#include <gtest/gtest.h>
#include <boost/filesystem.hpp>
#include "limestone/api/limestone_exception.h"
#include "blob_pool_impl.h"
#include "blob_file_resolver.h"
#include "file_operations.h"

#define EXPECT_THROW_WITH_PARTIAL_MESSAGE(stmt, expected_exception, expected_partial_message) \
    try { \
        stmt; \
        FAIL() << "Expected exception of type " #expected_exception; \
    } catch (const expected_exception& e) { \
        EXPECT_TRUE(std::string(e.what()).find(expected_partial_message) != std::string::npos) \
            << "Expected partial message: \"" << expected_partial_message << "\"\n" \
            << "Actual message: \"" << e.what() << "\""; \
    } catch (...) { \
        FAIL() << "Expected exception of type " #expected_exception; \
    }

namespace limestone::testing {

using namespace limestone::internal;
using limestone::api::blob_id_type;

constexpr const char* base_directory = "/tmp/blob_pool_impl_test";
constexpr const char* blob_directory = "/tmp/blob_pool_impl_test/blob";

class testable_blob_pool_impl : public blob_pool_impl {
public:
    using blob_pool_impl::blob_pool_impl;  // Use the inherited constructor

    // Make handle_cross_filesystem_move publicly accessible
    using blob_pool_impl::handle_cross_filesystem_move;
};

class blob_pool_impl_test : public ::testing::Test {
protected:
    void SetUp() override {
        // Reset the ID generator
        current_id_ = 0;
        id_generator_ = [this]() -> blob_id_type {
            return ++current_id_;
        };

        // Remove and recreate the test directories
        if (system(("rm -rf " + std::string(base_directory)).c_str()) != 0) {
            std::cerr << "cannot remove directory" << std::endl;
        }
        if (system(("mkdir -p " + std::string(blob_directory)).c_str()) != 0) {
            std::cerr << "cannot make directory" << std::endl;
        }

        // Initialize blob_file_resolver with the blob directory
        resolver_ = std::make_unique<blob_file_resolver>(
            boost::filesystem::path(blob_directory), 10 /* directory count */);

        // Initialize blob_pool_impl with the resolver and ID generator
        pool_ = std::make_unique<testable_blob_pool_impl>(id_generator_, *resolver_);
    }

    void TearDown() override {
        pool_.reset();
        resolver_.reset();
        if (system(("rm -rf " + std::string(base_directory)).c_str()) != 0) {
            std::cerr << "cannot remove directory" << std::endl;
        }
    }

    blob_id_type current_id_{0};                        // Counter for generating unique IDs
    std::function<blob_id_type()> id_generator_;        // ID generator function
    std::unique_ptr<blob_file_resolver> resolver_;      // Resolver for managing blob paths
    std::unique_ptr<testable_blob_pool_impl> pool_;              // Pool instance
};

TEST_F(blob_pool_impl_test, register_file_with_existing_file) {
    boost::filesystem::path test_source("/tmp/blob_pool_impl_test/source_blob");
    boost::filesystem::path expected_target = resolver_->resolve_path(1);

    // Create a test source file
    std::ofstream(test_source.string()) << "test data";

    // Register the file
    blob_id_type id = pool_->register_file(test_source, false);

    EXPECT_EQ(id, 1);
    EXPECT_TRUE(boost::filesystem::exists(expected_target));
}

TEST_F(blob_pool_impl_test, register_file_with_temporary_file) {
    boost::filesystem::path test_source("/tmp/blob_pool_impl_test/source_blob_temp");
    boost::filesystem::path expected_target = resolver_->resolve_path(1);

    // Create a test source file
    std::ofstream(test_source.string()) << "test data";

    // Register the file as temporary
    blob_id_type id = pool_->register_file(test_source, true);

    EXPECT_EQ(id, 1);
    EXPECT_TRUE(boost::filesystem::exists(expected_target));
    EXPECT_FALSE(boost::filesystem::exists(test_source));
}

TEST_F(blob_pool_impl_test, register_file_fails_if_pool_released) {
    // Release the pool
    pool_->release();

    // Attempt to register a file
    EXPECT_THROW_WITH_PARTIAL_MESSAGE(
        pool_->register_file("/tmp/blob_pool_impl_test/nonexistent_file", false),
        std::runtime_error,
        "This pool is already released.");
}

TEST_F(blob_pool_impl_test, register_file_fails_if_source_does_not_exist) {
    boost::filesystem::path test_source("/tmp/blob_pool_impl_test/nonexistent_file");

    // Register a non-existent file and verify the exception message
    EXPECT_THROW_WITH_PARTIAL_MESSAGE(
        pool_->register_file(test_source, false),
        limestone_io_exception,
        "Source file does not exist: /tmp/blob_pool_impl_test/nonexistent_file"
    );
}

TEST_F(blob_pool_impl_test, register_file_rename_fails_with_cross_device_link) {
    boost::filesystem::path source_path("/tmp/blob_pool_impl_test/source_blob_rename_fail_cross");
    boost::filesystem::path target_path = resolver_->resolve_path(1);

    // Create a test source file
    std::ofstream(source_path.string()) << "test data";

    // Mock file_operations to simulate cross-device link error on rename
    class : public real_file_operations {
    public:
        void rename(const boost::filesystem::path& source, const boost::filesystem::path& target, boost::system::error_code& ec) override {
            ec = boost::system::errc::make_error_code(boost::system::errc::cross_device_link);
        }
    } custom_ops;

    pool_->set_file_operations(custom_ops);

    // Perform the registration and verify handle_cross_filesystem_move is invoked
    EXPECT_NO_THROW(pool_->register_file(source_path, true));
    EXPECT_FALSE(boost::filesystem::exists(source_path)); // Should be removed after cross-device move
    EXPECT_TRUE(boost::filesystem::exists(target_path));  // File should exist at target path
}

TEST_F(blob_pool_impl_test, DISABLED_register_file_no_mock_cross_device_test) {
    // Set up source and target paths on different filesystems
    boost::filesystem::path source_path("/dev/shm/ume/source_blob_cross_device");
    boost::filesystem::path target_path = resolver_->resolve_path(1);

    // Create a test source file in /dev/shm
    std::ofstream(source_path.string()) << "test data";

    // Ensure the target directory exists
    boost::filesystem::create_directories(target_path.parent_path());

    // Perform the registration with is_temporary_file = true
    blob_id_type id = pool_->register_file(source_path, true);

    // Verify the source file was removed
    EXPECT_FALSE(boost::filesystem::exists(source_path));

    // Verify the target file was created
    EXPECT_TRUE(boost::filesystem::exists(target_path));

    // Verify the target file content is correct
    std::ifstream target_file(target_path.string());
    std::string target_content;
    std::getline(target_file, target_content);
    EXPECT_EQ(target_content, "test data");
}


TEST_F(blob_pool_impl_test, register_file_rename_fails_with_other_error) {
    boost::filesystem::path source_path("/tmp/blob_pool_impl_test/source_blob_rename_fail_other");
    boost::filesystem::path target_path = resolver_->resolve_path(1);

    // Create a test source file
    std::ofstream(source_path.string()) << "test data";

    // Mock file_operations to simulate a different error on rename
    class : public real_file_operations {
    public:
        void rename(const boost::filesystem::path& source, const boost::filesystem::path& target, boost::system::error_code& ec) override {
            ec = boost::system::errc::make_error_code(boost::system::errc::io_error);
        }
    } custom_ops;

    pool_->set_file_operations(custom_ops);

    // Perform the registration and expect an exception
    EXPECT_THROW_WITH_PARTIAL_MESSAGE(
        pool_->register_file(source_path, true),
        limestone_io_exception,
        "Failed to move file: "
    );

    // Verify the source file still exists and the target file does not
    EXPECT_TRUE(boost::filesystem::exists(source_path));
    EXPECT_FALSE(boost::filesystem::exists(target_path));
}

TEST_F(blob_pool_impl_test, register_file_copy_file_fails) {
    boost::filesystem::path source_path("/tmp/blob_pool_impl_test/source_blob_copy_fail");
    boost::filesystem::path target_path = resolver_->resolve_path(1);

    // Create a test source file
    std::ofstream(source_path.string()) << "test data";

    // Mock file_operations to simulate copy_file failure
    class : public real_file_operations {
    public:
        void copy_file(const boost::filesystem::path& source, const boost::filesystem::path& target, boost::system::error_code& ec) override {
            ec = boost::system::errc::make_error_code(boost::system::errc::io_error);
        }
    } custom_ops;

    pool_->set_file_operations(custom_ops);

    // Perform the registration and expect an exception
    EXPECT_THROW_WITH_PARTIAL_MESSAGE(
        pool_->register_file(source_path, false),
        limestone_io_exception,
        "Failed to copy file: "
    );

    // Verify the source file still exists and the target file does not
    EXPECT_TRUE(boost::filesystem::exists(source_path));
    EXPECT_FALSE(boost::filesystem::exists(target_path));
}



TEST_F(blob_pool_impl_test, register_file_fails_if_directory_creation_fails) {
    boost::filesystem::path test_source("/tmp/blob_pool_impl_test/source_blob");

    // Create a test source file
    std::ofstream(test_source.string()) << "test data";

    // Use an anonymous class to simulate directory creation failure
    class : public real_file_operations {
    public:
        void create_directories(const boost::filesystem::path& path, boost::system::error_code& ec) override {
            ec = boost::system::errc::make_error_code(boost::system::errc::permission_denied);
            std::cerr << "Simulated failure to create directory: " << path.string() << std::endl;
        }
    } custom_ops;

    // Inject the custom file_operations instance into the pool
    pool_->set_file_operations(custom_ops);


    EXPECT_THROW_WITH_PARTIAL_MESSAGE(
        pool_->register_file(test_source, false),
        limestone_io_exception,
        "Failed to create directory: " 
    );
}

TEST_F(blob_pool_impl_test, handle_cross_filesystem_move_successful) {
    boost::filesystem::path source_path("/tmp/blob_pool_impl_test/source_blob_cross_fs");
    boost::filesystem::path target_path("/tmp/blob_pool_impl_test/blob/1");

    // Create a test source file
    std::ofstream(source_path.string()) << "test data";

    // Ensure the target directory exists
    boost::filesystem::create_directories(target_path.parent_path());

    // Perform the cross-filesystem move
    boost::system::error_code ec;
    pool_->handle_cross_filesystem_move(source_path, target_path, ec);

    // Verify the source file was removed and the target file exists
    EXPECT_FALSE(boost::filesystem::exists(source_path));
    EXPECT_TRUE(boost::filesystem::exists(target_path));
    EXPECT_FALSE(ec);
}

TEST_F(blob_pool_impl_test, handle_cross_filesystem_move_fails_to_copy) {
    boost::filesystem::path source_path("/tmp/blob_pool_impl_test/source_blob_cross_fs_fail_copy");
    boost::filesystem::path target_path("/tmp/blob_pool_impl_test/blob/1");

    // Create a test source file
    std::ofstream(source_path.string()) << "test data";

    // Use an anonymous class to simulate copy failure
    class : public real_file_operations {
    public:
        void copy_file(const boost::filesystem::path& source, const boost::filesystem::path& target, boost::system::error_code& ec) override {
            ec = boost::system::errc::make_error_code(boost::system::errc::io_error);
            std::cerr << "Simulated failure to copy file from: " << source.string() << " to " << target.string() << std::endl;
        }
    } custom_ops;

    // Inject the custom file_operations instance into the pool
    pool_->set_file_operations(custom_ops);

    boost::system::error_code ec;
    EXPECT_THROW_WITH_PARTIAL_MESSAGE(
        pool_->handle_cross_filesystem_move(source_path, target_path, ec),
        limestone_io_exception,
        "Failed to copy file across filesystems"
    );

    // Verify the source file still exists and the target file does not
    EXPECT_TRUE(boost::filesystem::exists(source_path));
    EXPECT_FALSE(boost::filesystem::exists(target_path));
}

TEST_F(blob_pool_impl_test, handle_cross_filesystem_move_fails_to_remove) {
    boost::filesystem::path source_path("/tmp/blob_pool_impl_test/source_blob_cross_fs_fail_remove");
    boost::filesystem::path target_path("/tmp/blob_pool_impl_test/blob/1");

    // Create a test source file
    std::ofstream(source_path.string()) << "test data";

    // Use an anonymous class to simulate remove failure
    class : public real_file_operations {
    public:
        void remove(const boost::filesystem::path& path, boost::system::error_code& ec) override {
            ec = boost::system::errc::make_error_code(boost::system::errc::permission_denied);
            std::cerr << "Simulated failure to remove file: " << path.string() << std::endl;
        }
    } custom_ops;

    // Inject the custom file_operations instance into the pool
    pool_->set_file_operations(custom_ops);

    // Perform the cross-filesystem move
    boost::system::error_code ec;
    EXPECT_THROW_WITH_PARTIAL_MESSAGE(
        pool_->handle_cross_filesystem_move(source_path, target_path, ec),
        limestone_io_exception,
        "Failed to remove source file after copying"
    );

    // Verify both the source and target files exist
    EXPECT_TRUE(boost::filesystem::exists(source_path));
    EXPECT_TRUE(boost::filesystem::exists(target_path));
}



}  // namespace limestone::testing
