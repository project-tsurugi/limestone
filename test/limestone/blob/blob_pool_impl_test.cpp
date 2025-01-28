#include <gtest/gtest.h>
#include <boost/filesystem.hpp>
#include <fstream>
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
constexpr const char* dev_shm_test_directory_ = "/dev/shm/blob_pool_impl_test";

class testable_blob_pool_impl : public blob_pool_impl {
public:
    // for testing purposes, expose proteced members
    using blob_pool_impl::blob_pool_impl; 
    using blob_pool_impl::handle_cross_filesystem_move;
    using blob_pool_impl::copy_file;
    using blob_pool_impl::move_file;
    using blob_pool_impl::create_directories_if_needed;
    using blob_pool_impl::copy_buffer_size;
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

        // Remove and recreate the test directory in /dev/shm
        if (system(("rm -rf " + std::string(dev_shm_test_directory_)).c_str()) != 0) {
            std::cerr << "Cannot remove directory: " << dev_shm_test_directory_ << std::endl;
        }
        if (system(("mkdir -p " + std::string(dev_shm_test_directory_)).c_str()) != 0) {
            std::cerr << "Cannot create directory: " << dev_shm_test_directory_ << std::endl;
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
        if (system(("rm -rf " + std::string(dev_shm_test_directory_)).c_str()) != 0) {
            std::cerr << "Cannot remove directory: " << dev_shm_test_directory_ << std::endl;
        }
    }

    blob_id_type current_id_{0};                     // Counter for generating unique IDs
    std::function<blob_id_type()> id_generator_;     // ID generator function
    std::unique_ptr<blob_file_resolver> resolver_;   // Resolver for managing blob paths
    std::unique_ptr<testable_blob_pool_impl> pool_;  // Pool instance
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
        std::logic_error,
        "This pool is already released.");
}

TEST_F(blob_pool_impl_test, register_file_fails_if_source_does_not_exist) {
    boost::filesystem::path test_source("/tmp/blob_pool_impl_test/nonexistent_file");

    // Register a non-existent file and verify the exception message
    EXPECT_THROW_WITH_PARTIAL_MESSAGE(
        pool_->register_file(test_source, false),
        limestone_blob_exception,
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
    boost::filesystem::path source_path("/dev/shm/blob_pool_impl_test/source_blob_cross_device");
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
        limestone_blob_exception,
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
        limestone_blob_exception,
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
        limestone_blob_exception,
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
        limestone_blob_exception,
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
        limestone_blob_exception,
        "Failed to remove source file after copying"
    );

    // Verify both the source and target files exist
    EXPECT_TRUE(boost::filesystem::exists(source_path));
    EXPECT_TRUE(boost::filesystem::exists(target_path));
}


TEST_F(blob_pool_impl_test, copy_file_file_size_boundary_tests) {
    const std::vector<size_t> test_sizes = {
        0,                              // Empty file
        1,                              // Minimum data
        testable_blob_pool_impl::copy_buffer_size - 1,  // Buffer size - 1
        testable_blob_pool_impl::copy_buffer_size,      // Buffer size
        testable_blob_pool_impl::copy_buffer_size + 1,  // Buffer size + 1
        testable_blob_pool_impl::copy_buffer_size * 5 - 1,  // 5 * Buffer size - 1
        testable_blob_pool_impl::copy_buffer_size * 5,      // 5 * Buffer size
        testable_blob_pool_impl::copy_buffer_size * 5 + 1,  // 5 * Buffer size + 1
        173205,                         // Random size 1
        223620                          // Random size 2
    };

    for (const auto& size : test_sizes) {
        SCOPED_TRACE("Testing with file size: " + std::to_string(size));

        // Generate a test file with pseudo-random data
        boost::filesystem::path source_path("/tmp/blob_pool_impl_test/source_blob");
        boost::filesystem::path destination_path("/tmp/blob_pool_impl_test/blob/1");

        {
            std::ofstream source_file(source_path.string(), std::ios::binary);
            uint32_t seed = 123456789;  // XORShift seed
            for (size_t i = 0; i < size; ++i) {
                seed ^= seed << 13;
                seed ^= seed >> 17;
                seed ^= seed << 5;
                char value = static_cast<char>(seed & 0xFF);
                source_file.put(value);
            }
        }

        // Perform the copy
        EXPECT_NO_THROW(pool_->copy_file(source_path, destination_path));

        // Validate the results
        EXPECT_TRUE(boost::filesystem::exists(destination_path));
        EXPECT_EQ(boost::filesystem::file_size(destination_path), size);

        // Verify file content
        if (size > 0) {
            std::ifstream source_file(source_path.string(), std::ios::binary);
            std::ifstream destination_file(destination_path.string(), std::ios::binary);

            uint32_t seed = 123456789;  // Reset XORShift seed
            char source_value, destination_value;
            for (size_t i = 0; i < size; ++i) {
                seed ^= seed << 13;
                seed ^= seed >> 17;
                seed ^= seed << 5;
                source_value = static_cast<char>(seed & 0xFF);

                destination_file.get(destination_value);
                ASSERT_EQ(destination_value, source_value) << "File content mismatch at byte " << i;
            }
        }

        // Clean up
        boost::filesystem::remove(source_path);
        boost::filesystem::remove(destination_path);
    }
}



TEST_F(blob_pool_impl_test, copy_file_source_not_found) {
    boost::filesystem::path source_path("/tmp/blob_pool_impl_test/nonexistent_file");
    boost::filesystem::path destination_path("/tmp/blob_pool_impl_test/blob/1");

    class : public real_file_operations {
    public:
        size_t fclose_call_count = 0;

        int fclose(FILE* file) override {
            ++fclose_call_count;
            return real_file_operations::fclose(file);
        }
    } mock_ops;

    pool_->set_file_operations(mock_ops);

    EXPECT_THROW_WITH_PARTIAL_MESSAGE(
        pool_->copy_file(source_path, destination_path),
        limestone_blob_exception,
        "Failed to open source file"
    );

    EXPECT_EQ(mock_ops.fclose_call_count, 0);  // fclose should not be called

    // Check that the destination file does not exist
    EXPECT_FALSE(boost::filesystem::exists(destination_path)) << "The destination file should not exist.";
}

TEST_F(blob_pool_impl_test, copy_file_open_dest_fails) {
    class : public real_file_operations {
    public:
        size_t fclose_call_count = 0;

        FILE* fopen(const char* path, const char* mode) override {
            if (std::string(mode) == "wb") {
                errno = EACCES;  // Simulate permission denied
                return nullptr;
            }
            return real_file_operations::fopen(path, mode);
        }

        int fclose(FILE* file) override {
            ++fclose_call_count;
            return real_file_operations::fclose(file);
        }
    } mock_ops;

    pool_->set_file_operations(mock_ops);

    boost::filesystem::path source_path("/tmp/blob_pool_impl_test/source_blob");
    boost::filesystem::path destination_path("/tmp/blob_pool_impl_test/blob/1");

    // Create a source file
    std::ofstream(source_path.string()) << "test data";

    EXPECT_THROW_WITH_PARTIAL_MESSAGE(
        pool_->copy_file(source_path, destination_path),
        limestone_blob_exception,
        "Failed to open destination file"
    );

    EXPECT_EQ(mock_ops.fclose_call_count, 1);  // Source file should be closed
    // Check that the destination file does not exist
    EXPECT_FALSE(boost::filesystem::exists(destination_path)) << "The destination file should not exist.";
}

TEST_F(blob_pool_impl_test, copy_file_source_close_fails) {
    class : public real_file_operations {
    public:
        size_t fclose_call_count = 0;
        size_t source_fclose_attempts = 0;  // Number of attempts to close the source file
        std::set<FILE*> closed_files;
        FILE* source_file = nullptr;

        FILE* fopen(const char* path, const char* mode) override {
            FILE* file = real_file_operations::fopen(path, mode);
            if (std::string(mode) == "rb") {
                source_file = file; 
            }
            return file;
        }

        int fclose(FILE* file) override {
            if (file == source_file) {
                ++source_fclose_attempts;  // Record the attempt to close the source file
                errno = EBADF;  // Simulate failure for source close
                return EOF;
            }
            closed_files.insert(file); 
            ++fclose_call_count;
            return real_file_operations::fclose(file);
        }
    } mock_ops;

    pool_->set_file_operations(mock_ops);

    boost::filesystem::path source_path("/tmp/blob_pool_impl_test/source_blob");
    boost::filesystem::path destination_path("/tmp/blob_pool_impl_test/blob/1");

    // Create a source file
    std::ofstream(source_path.string()) << "test data";

    EXPECT_NO_THROW(pool_->copy_file(source_path, destination_path));  // fclose failure is logged, not thrown

    EXPECT_EQ(mock_ops.fclose_call_count, 1);  // Only destination should be closed
    EXPECT_EQ(mock_ops.source_fclose_attempts, 1);  // Source close attempt should be 1
    EXPECT_TRUE(mock_ops.closed_files.find(mock_ops.source_file) == mock_ops.closed_files.end())
        << "Source file was not closed";
    // Check that the destination file does not exist
    EXPECT_TRUE(boost::filesystem::exists(destination_path)) << "The destination file should be exist.";
}


TEST_F(blob_pool_impl_test, copy_file_dest_close_fails) {
    class : public real_file_operations {
    public:
        size_t fclose_call_count = 0;  // Number of successful fclose calls
        size_t dest_fclose_attempts = 0;  // Number of attempts to close the destination file
        std::set<FILE*> closed_files;  // Set of successfully closed files
        FILE* dest_file = nullptr;  // Pointer to the destination file

        FILE* fopen(const char* path, const char* mode) override {
            FILE* file = real_file_operations::fopen(path, mode);
            if (std::string(mode) == "wb") {
                dest_file = file;  // Store the destination file pointer
            }
            return file;
        }

        int fclose(FILE* file) override {
            if (file == dest_file) {
                ++dest_fclose_attempts;  // Record the attempt to close the destination file
                errno = EBADF;  // Simulate failure for destination close
                return EOF;
            }
            closed_files.insert(file);  // Record the successfully closed file
            ++fclose_call_count;
            return real_file_operations::fclose(file);
        }
    } mock_ops;

    pool_->set_file_operations(mock_ops);

    boost::filesystem::path source_path("/tmp/blob_pool_impl_test/source_blob");
    boost::filesystem::path destination_path("/tmp/blob_pool_impl_test/blob/1");

    // Create a source file
    std::ofstream(source_path.string()) << "test data";

    // Perform the copy and verify that the destination close failure is logged, not thrown
    EXPECT_NO_THROW(pool_->copy_file(source_path, destination_path));

    // Verify fclose behavior
    EXPECT_EQ(mock_ops.fclose_call_count, 1);  // Only source should be successfully closed
    EXPECT_EQ(mock_ops.dest_fclose_attempts, 1);  // Destination close attempt should be exactly 1
    EXPECT_TRUE(mock_ops.closed_files.find(mock_ops.dest_file) == mock_ops.closed_files.end())
        << "Destination file was not closed successfully";
    // Check that the destination file does not exist
    EXPECT_TRUE(boost::filesystem::exists(destination_path)) << "The destination file should be exist.";
}

TEST_F(blob_pool_impl_test, copy_file_fflush_fails) {
    class : public real_file_operations {
    public:
        size_t fclose_call_count = 0;

        int fflush(FILE* file) override {
            errno = EIO;  // Simulate input/output error
            return EOF;
        }

        int fclose(FILE* file) override {
            ++fclose_call_count;
            return real_file_operations::fclose(file);
        }
    } mock_ops;

    pool_->set_file_operations(mock_ops);

    boost::filesystem::path source_path("/tmp/blob_pool_impl_test/source_blob");
    boost::filesystem::path destination_path("/tmp/blob_pool_impl_test/blob/1");

    // Create a source file
    std::ofstream(source_path.string()) << "test data";

    EXPECT_THROW_WITH_PARTIAL_MESSAGE(
        pool_->copy_file(source_path, destination_path),
        limestone_blob_exception,
        "Failed to flush data to destination file"
    );

    EXPECT_EQ(mock_ops.fclose_call_count, 2);  // Both source and destination should be closed
    // Check that the destination file does not exist
    EXPECT_FALSE(boost::filesystem::exists(destination_path)) << "The destination file should not exist.";
}

TEST_F(blob_pool_impl_test, copy_file_directory_creation_fails) {
    class : public real_file_operations {
    public:
        size_t create_directories_attempts = 0;  // Number of directory creation attempts

        void create_directories(const boost::filesystem::path& path, boost::system::error_code& ec) override {
            ++create_directories_attempts;  // Record directory creation attempts
            ec = boost::system::errc::make_error_code(boost::system::errc::permission_denied);  // Simulate failure
        }
    } mock_ops;

    pool_->set_file_operations(mock_ops);

    boost::filesystem::path source_path("/tmp/blob_pool_impl_test/source_blob");
    boost::filesystem::path destination_path("/tmp/blob_pool_impl_test/nonexistent_directory/destination_blob");

    // Create a source file
    std::ofstream(source_path.string()) << "test data";

    EXPECT_THROW_WITH_PARTIAL_MESSAGE(
        pool_->copy_file(source_path, destination_path),
        limestone_blob_exception,
        "Failed to create directory"
    );

    // Verify directory creation attempts
    EXPECT_EQ(mock_ops.create_directories_attempts, 1) << "Directory creation should have been attempted once.";
    // Check that the destination file does not exist
    EXPECT_FALSE(boost::filesystem::exists(destination_path)) << "The destination file should not exist.";
}

TEST_F(blob_pool_impl_test, copy_file_fsync_fails) {
    class : public real_file_operations {
    public:
        size_t fsync_attempts = 0;  // Number of fsync attempts

        int fsync(int fd) override {
            ++fsync_attempts;  // Record fsync attempts
            errno = EIO;  // Simulate input/output error
            return -1;
        }
    } mock_ops;

    pool_->set_file_operations(mock_ops);

    boost::filesystem::path source_path("/tmp/blob_pool_impl_test/source_blob");
    boost::filesystem::path destination_path("/tmp/blob_pool_impl_test/blob/1");

    // Create a source file
    std::ofstream(source_path.string()) << "test data";

    EXPECT_THROW_WITH_PARTIAL_MESSAGE(
        pool_->copy_file(source_path, destination_path),
        limestone_blob_exception,
        "Failed to synchronize destination file to disk"
    );

    // Verify fsync attempts
    EXPECT_EQ(mock_ops.fsync_attempts, 1) << "fsync should have been attempted once.";
    // Check that the destination file does not exist
    EXPECT_FALSE(boost::filesystem::exists(destination_path)) << "The destination file should not exist.";
}

TEST_F(blob_pool_impl_test, copy_file_read_fails) {
    class : public real_file_operations {
    public:
        size_t fread_attempts = 0;  // Number of fread attempts
        size_t fail_on_fread_attempt = 1;  // Fail on the nth fread attempt
        bool read_error_set = false;  // Flag to simulate ferror state

        size_t fread(void* ptr, size_t size, size_t count, FILE* stream) override {
            ++fread_attempts;  // Record fread attempts
            if (fread_attempts == fail_on_fread_attempt) {
                read_error_set = true;  // Set read error flag
                errno = EIO;  // Simulate input/output error
                return 0;
            }
            return real_file_operations::fread(ptr, size, count, stream);
        }

        int ferror(FILE* stream) override {
            return read_error_set ? 1 : real_file_operations::ferror(stream);  // Simulate error state
        }
    } mock_ops;

    pool_->set_file_operations(mock_ops);

    boost::filesystem::path source_path("/tmp/blob_pool_impl_test/source_blob");
    boost::filesystem::path destination_path("/tmp/blob_pool_impl_test/blob/1");

    // Create a source file with some data
    std::ofstream(source_path.string()) << "test data";

    EXPECT_THROW_WITH_PARTIAL_MESSAGE(
        pool_->copy_file(source_path, destination_path),
        limestone_blob_exception,
        "Error reading from source file"
    );

    // Verify fread and ferror behavior
    EXPECT_EQ(mock_ops.fread_attempts, 1) << "fread should have been attempted once.";
    // Check that the destination file does not exist
    EXPECT_FALSE(boost::filesystem::exists(destination_path)) << "The destination file should not exist.";
}


TEST_F(blob_pool_impl_test, copy_file_write_fails) {
    class : public real_file_operations {
    public:
        size_t fwrite_attempts = 0;  // Number of fwrite attempts
        size_t fail_on_fwrite_attempt = 1;  // Fail on the nth fwrite attempt

        size_t fwrite(const void* ptr, size_t size, size_t count, FILE* stream) override {
            ++fwrite_attempts;  // Record fwrite attempts
            if (fwrite_attempts == fail_on_fwrite_attempt) {
                errno = EIO;  // Simulate input/output error
                return 0;
            }
            return real_file_operations::fwrite(ptr, size, count, stream);
        }
    } mock_ops;

    pool_->set_file_operations(mock_ops);

    boost::filesystem::path source_path("/tmp/blob_pool_impl_test/source_blob");
    boost::filesystem::path destination_path("/tmp/blob_pool_impl_test/blob/1");

    // Create a source file with some data
    std::ofstream(source_path.string()) << "test data";

    EXPECT_THROW_WITH_PARTIAL_MESSAGE(
        pool_->copy_file(source_path, destination_path),
        limestone_blob_exception,
        "Failed to write data to destination file"
    );

    // Verify fwrite was attempted
    EXPECT_EQ(mock_ops.fwrite_attempts, 1) << "fwrite should have been attempted once.";
    // Check that the destination file does not exist
    EXPECT_FALSE(boost::filesystem::exists(destination_path)) << "The destination file should not exist.";
}

TEST_F(blob_pool_impl_test, copy_file_fails_and_cleans_up_existing_destination) {
    class : public real_file_operations {
    public:
        size_t fwrite_calls = 0;
        size_t fail_after_calls = 1;  // Fail on the second fwrite
        bool remove_called = false;  // Track if remove is called

        size_t fwrite(const void* ptr, size_t size, size_t count, FILE* stream) override {
            if (++fwrite_calls >= fail_after_calls) {
                errno = EIO;  // Simulate a write error
                return 0;  // Simulate failure
            }
            return real_file_operations::fwrite(ptr, size, count, stream);
        }
        void remove(const boost::filesystem::path& path, boost::system::error_code& ec) override {
            remove_called = true;  // Track remove call
            real_file_operations::remove(path, ec);
        }
    } mock_ops;

    pool_->set_file_operations(mock_ops);

    boost::filesystem::path source_path("/tmp/blob_pool_impl_test/source_blob");
    boost::filesystem::path destination_path("/tmp/blob_pool_impl_test/blob/1");

    // Create a source file
    std::ofstream(source_path.string()) << "test data";

    EXPECT_THROW_WITH_PARTIAL_MESSAGE(
        pool_->copy_file(source_path, destination_path),
        limestone_blob_exception,
        "Failed to write data to destination file"
    );

    // Verify that remove was called
    EXPECT_TRUE(mock_ops.remove_called) << "Destination file should be cleaned up.";
    // Check that the destination file does not exist
    EXPECT_FALSE(boost::filesystem::exists(destination_path)) << "The destination file should not exist.";
}

TEST_F(blob_pool_impl_test, copy_file_logs_when_cleanup_fails) {
    class : public real_file_operations {
    public:
        size_t fwrite_calls = 0;
        size_t fail_after_calls = 1;  // Simulate failure on the second fwrite
        bool remove_called = false;  // Track if remove is called
        boost::system::error_code remove_error;  // Simulate an error during remove

        size_t fwrite(const void* ptr, size_t size, size_t count, FILE* stream) override {
            if (++fwrite_calls >= fail_after_calls) {
                errno = EIO;  // Simulate a write error
                return 0;  // Fail the write operation
            }
            return real_file_operations::fwrite(ptr, size, count, stream);
        }

        void remove(const boost::filesystem::path& path, boost::system::error_code& ec) override {
            remove_called = true;  // Mark that remove was attempted
            ec = remove_error;     // Simulate the error
        }
    } mock_ops;

    // Set up the mock file operations
    mock_ops.remove_error = boost::system::errc::make_error_code(boost::system::errc::permission_denied);
    pool_->set_file_operations(mock_ops);

    // Create source and destination paths
    boost::filesystem::path source_path("/tmp/blob_pool_impl_test/source_blob");
    boost::filesystem::path destination_path("/tmp/blob_pool_impl_test/blob/1");

    // Create a source file
    std::ofstream(source_path.string()) << "test data";

    // Redirect the log output to a stringstream for verification
    std::ostringstream log_stream;
    std::streambuf* original_cerr = std::cerr.rdbuf(log_stream.rdbuf());

    // Execute the copy_file and expect an exception
    EXPECT_THROW_WITH_PARTIAL_MESSAGE(
        pool_->copy_file(source_path, destination_path),
        limestone_blob_exception,
        "Failed to write data to destination file"
    );

    // Restore the original cerr buffer
    std::cerr.rdbuf(original_cerr);

    // Verify that remove was attempted
    EXPECT_TRUE(mock_ops.remove_called) << "The remove operation should have been attempted.";

    // Verify that the destination file still exists
    EXPECT_TRUE(boost::filesystem::exists(destination_path))
        << "The destination file should still exist after failed removal.";
}



// Test case for creating directories when they do not exist
TEST_F(blob_pool_impl_test, create_directories_if_needed_when_directory_does_not_exist) {
    boost::filesystem::path test_dir("/tmp/blob_pool_test_dir");

    // Ensure the directory does not exist initially
    ASSERT_FALSE(boost::filesystem::exists(test_dir));

    // Call create_directories_if_needed
    EXPECT_NO_THROW(pool_->create_directories_if_needed(test_dir));

    // Verify the directory was created
    EXPECT_TRUE(boost::filesystem::exists(test_dir));
    EXPECT_TRUE(boost::filesystem::is_directory(test_dir));

    // Clean up
    boost::filesystem::remove_all(test_dir);
}

// Test case for existing directories (should not throw or fail)
TEST_F(blob_pool_impl_test, create_directories_if_needed_when_directory_already_exists) {
    boost::filesystem::path test_dir("/tmp/blob_pool_test_existing_dir");

    // Create the directory beforehand
    boost::filesystem::create_directories(test_dir);
    ASSERT_TRUE(boost::filesystem::exists(test_dir));

    // Call create_directories_if_needed
    EXPECT_NO_THROW(pool_->create_directories_if_needed(test_dir));

    // Verify the directory still exists
    EXPECT_TRUE(boost::filesystem::exists(test_dir));
    EXPECT_TRUE(boost::filesystem::is_directory(test_dir));

    // Clean up
    boost::filesystem::remove_all(test_dir);
}

// Test case for invalid directory creation
TEST_F(blob_pool_impl_test, create_directories_if_needed_invalid_directory) {
    boost::filesystem::path invalid_dir("/invalid_blob_pool_test_dir");

    // Attempt to create an invalid directory and verify it throws an exception
    EXPECT_THROW(pool_->create_directories_if_needed(invalid_dir), limestone_blob_exception);

    // Verify the directory was not created
    EXPECT_FALSE(boost::filesystem::exists(invalid_dir));
}

// Test case for successful move within the same filesystem
TEST_F(blob_pool_impl_test, move_file_within_same_filesystem) {
    boost::filesystem::path source_path("/tmp/blob_pool_impl_test/source_blob");
    boost::filesystem::path destination_path("/tmp/blob_pool_impl_test/blob/1");

    // Create a source file
    std::ofstream(source_path.string()) << "test data";

    // Perform the move
    EXPECT_NO_THROW(pool_->move_file(source_path, destination_path));

    // Verify that the source file is removed and the destination file exists
    EXPECT_FALSE(boost::filesystem::exists(source_path)) << "The source file should be removed.";
    EXPECT_TRUE(boost::filesystem::exists(destination_path)) << "The destination file should exist.";
}

// Test case for cross-filesystem move
TEST_F(blob_pool_impl_test, DISABLED_move_file_no_mock_across_filesystems) {
    // Use `/dev/shm` (RAM disk) as a different filesystem from `/tmp`
    boost::filesystem::path source_path("/dev/shm/blob_pool_impl_test/source_blob_cross_fs");
    boost::filesystem::path destination_path("/tmp/blob_pool_impl_test/blob/1");

    // Create a source file in `/dev/shm`
    std::ofstream(source_path.string()) << "test data";

    // Perform the move
    EXPECT_NO_THROW(pool_->move_file(source_path, destination_path));

    // Verify that the source file is removed and the destination file exists
    EXPECT_FALSE(boost::filesystem::exists(source_path)) << "The source file should be removed.";
    EXPECT_TRUE(boost::filesystem::exists(destination_path)) << "The destination file should exist.";

    // Verify file content
    std::ifstream dest_file(destination_path.string());
    std::string content;
    std::getline(dest_file, content);
    EXPECT_EQ(content, "test data") << "The destination file content should match the source.";
}

TEST_F(blob_pool_impl_test, move_file_across_filesystems) {
    boost::filesystem::path source_path("/tmp/blob_pool_impl_test/source_blob_cross_fs_mock");
    boost::filesystem::path destination_path("/tmp/blob_pool_impl_test/blob/1");

    // Create a source file
    std::ofstream(source_path.string()) << "test data";

    // Mock file_operations to simulate cross-device link error on rename
    class : public real_file_operations {
    public:
        void rename(const boost::filesystem::path& source, const boost::filesystem::path& target, boost::system::error_code& ec) override {
            ec = boost::system::errc::make_error_code(boost::system::errc::cross_device_link);
        }
    } custom_ops;

    // Inject the mock file operations into the pool
    pool_->set_file_operations(custom_ops);

    // Perform the move
    EXPECT_NO_THROW(pool_->move_file(source_path, destination_path));

    // Verify that the source file is removed and the destination file exists
    EXPECT_FALSE(boost::filesystem::exists(source_path)) << "The source file should be removed.";
    EXPECT_TRUE(boost::filesystem::exists(destination_path)) << "The destination file should exist.";

    // Verify the file content at the destination
    std::ifstream dest_file(destination_path.string());
    std::string content;
    std::getline(dest_file, content);
    EXPECT_EQ(content, "test data") << "The destination file content should match the source.";
}



// Test case for move failure due to other rename error
TEST_F(blob_pool_impl_test, move_file_rename_fails_with_other_error) {
    boost::filesystem::path source_path("/tmp/blob_pool_impl_test/source_blob_rename_fail");
    boost::filesystem::path destination_path("/tmp/blob_pool_impl_test/blob/1");

    // Create a source file
    std::ofstream(source_path.string()) << "test data";

    // Mock file_operations to simulate a different rename error
    class : public real_file_operations {
    public:
        void rename(const boost::filesystem::path& source, const boost::filesystem::path& target, boost::system::error_code& ec) override {
            ec = boost::system::errc::make_error_code(boost::system::errc::io_error);
        }
    } custom_ops;

    pool_->set_file_operations(custom_ops);

    // Expect an exception
    EXPECT_THROW_WITH_PARTIAL_MESSAGE(
        pool_->move_file(source_path, destination_path),
        limestone_blob_exception,
        "Failed to rename file"
    );

    // Verify the source file still exists and the destination file does not
    EXPECT_TRUE(boost::filesystem::exists(source_path)) << "The source file should still exist.";
    EXPECT_FALSE(boost::filesystem::exists(destination_path)) << "The destination file should not exist.";
}

TEST_F(blob_pool_impl_test, move_file_copy_fails) {
    // Mock file_operations to simulate rename failure and then copy_file failure
    class : public real_file_operations {
    public:
        size_t rename_attempts = 0;  // Number of rename attempts

        void rename(const boost::filesystem::path& source, const boost::filesystem::path& target, boost::system::error_code& ec) override {
            ++rename_attempts;  // Record rename attempts
            ec = boost::system::errc::make_error_code(boost::system::errc::cross_device_link);  // Simulate cross-device link error
        }

        int fsync(int fd) override {
            errno = EIO;  // Simulate input/output error
            return -1;
        }

    } mock_ops;

    pool_->set_file_operations(mock_ops);

    boost::filesystem::path source_path("/tmp/blob_pool_impl_test/source_blob");
    boost::filesystem::path destination_path("/tmp/blob_pool_impl_test/blob/1");

    // Create a source file
    std::ofstream(source_path.string()) << "test data";

    // Perform the move and expect an exception due to copy failure
    EXPECT_THROW_WITH_PARTIAL_MESSAGE(
        pool_->move_file(source_path, destination_path),
        limestone_blob_exception,
        "Failed to synchronize destination file to disk"
    );

    // Verify rename was attempted
    EXPECT_EQ(mock_ops.rename_attempts, 1) << "rename should have been attempted once.";

    // Verify the source file still exists and the destination file does not
    EXPECT_TRUE(boost::filesystem::exists(source_path)) << "The source file should still exist.";
    EXPECT_FALSE(boost::filesystem::exists(destination_path)) << "The destination file should not exist.";
}


TEST_F(blob_pool_impl_test, move_file_remove_source_fails) {
    boost::filesystem::path source_path("/tmp/blob_pool_impl_test/source_blob_remove_fail");
    boost::filesystem::path destination_path("/tmp/blob_pool_impl_test/blob/1");

    // Create a source file
    std::ofstream(source_path.string()) << "test data";

    // Mock file_operations to simulate source file removal failure
    class : public real_file_operations {
    public:
        void rename(const boost::filesystem::path& source, const boost::filesystem::path& target, boost::system::error_code& ec) override {
            ec = boost::system::errc::make_error_code(boost::system::errc::cross_device_link);  // Simulate cross-device link error
        }
        void remove(const boost::filesystem::path& path, boost::system::error_code& ec) override {
            ec = boost::system::errc::make_error_code(boost::system::errc::permission_denied);  // Simulate removal failure
        }
    } custom_ops;

    pool_->set_file_operations(custom_ops);

    // Perform the move operation and expect an exception
    EXPECT_THROW_WITH_PARTIAL_MESSAGE(
        pool_->move_file(source_path, destination_path),
        limestone_blob_exception,
        "Failed to remove source file after copy"
    );

    // Verify that both the source and destination files exist
    EXPECT_TRUE(boost::filesystem::exists(source_path)) << "The source file should still exist.";
    EXPECT_TRUE(boost::filesystem::exists(destination_path)) << "The destination file should exist.";
}


}  // namespace limestone::testing
