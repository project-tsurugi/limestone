/*
 * Copyright 2022-2025 Project Tsurugi.
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

#include "blob_pool_impl.h"
#include "limestone_exception_helper.h"
#include <filesystem>
#include <boost/filesystem.hpp>
#include <iostream>
#include <fstream>
#include <cstdio>
#include <memory>



namespace limestone::internal {

blob_pool_impl::blob_pool_impl(std::function<blob_id_type()> id_generator,
                               limestone::internal::blob_file_resolver& resolver)
    : id_generator_(std::move(id_generator)),
      resolver_(resolver),
      real_file_ops_(),
      file_ops_(&real_file_ops_) {}  // Use the address of the member variable

blob_id_type blob_pool_impl::generate_blob_id() {
    return id_generator_();
}
void blob_pool_impl::release() {
    // Release the pool
    is_released_.store(true, std::memory_order_release);
}

blob_id_type blob_pool_impl::register_file(boost::filesystem::path const& file, bool is_temporary_file) {
    // Check if the pool has already been released
    if (is_released_.load(std::memory_order_acquire)) {
        throw std::logic_error("This pool is already released.");
    }

    // Verify that the source file exists
    boost::system::error_code ec{};
    if (!file_ops_->exists(file, ec)) {
        LOG_AND_THROW_BLOB_EXCEPTION("Source file does not exist: " + file.string(), ec);
    }

    // Generate a unique BLOB ID and resolve the target path
    blob_id_type id = generate_blob_id();
    boost::filesystem::path target_path = resolver_.resolve_path(id);

    // Ensure the target directory exists
    boost::filesystem::path target_dir = target_path.parent_path();
    create_directories_if_needed(target_dir);

    // Copy or move the source file
    if (is_temporary_file) {
        move_file(file, target_path);
    } else {
        copy_file(file, target_path);
    }

    return id;
}


blob_id_type blob_pool_impl::duplicate_data(blob_id_type reference) {
    // Check if the pool has already been released
    if (is_released_.load(std::memory_order_acquire)) {
        throw std::logic_error("This pool is already released.");
    }

    // Resolve the source and destination paths
    boost::filesystem::path existing_path = resolver_.resolve_path(reference);
    boost::system::error_code ec;
    if (!file_ops_->exists(existing_path, ec) || ec) {
        LOG_AND_THROW_BLOB_EXCEPTION_NO_ERRNO("Invalid blob_id: " + std::to_string(reference) + ". Blob file does not exist: " + existing_path.string());
    }

    blob_id_type new_id = generate_blob_id();
    boost::filesystem::path link_path = resolver_.resolve_path(new_id);

    // Ensure the destination directory exists
    boost::filesystem::path destination_dir = link_path.parent_path();
    create_directories_if_needed(destination_dir);

    // Create a hard link to the source file
    file_ops_->create_hard_link(existing_path, link_path, ec);
    if (ec) {
        LOG_AND_THROW_BLOB_EXCEPTION("Failed to create hard link from " + existing_path.string() + " to " + link_path.string(), ec.value());
    }

    return new_id;
}

void blob_pool_impl::set_file_operations(file_operations& file_ops) {
    file_ops_ = &file_ops;
}

class FileCloser {
public:
    explicit FileCloser(file_operations* ops)
        : file_ops_(ops) {}

    void operator()(FILE* file) const {
        if (file && file_ops_->fclose(file) != 0) {
            VLOG_LP(log_error) << "Error closing file." << std::endl;
        }
    }

private:
    file_operations* file_ops_;
};

void blob_pool_impl::copy_file(const boost::filesystem::path& source, const boost::filesystem::path& destination) {
    // Ensure the destination directory exists
    boost::filesystem::path destination_dir = destination.parent_path();

    create_directories_if_needed(destination_dir);

    // Open source file for reading
    FILE* src_raw = file_ops_->fopen(source.string().c_str(), "rb");
    if (!src_raw) {
        int error_code = errno;
        LOG_AND_THROW_BLOB_EXCEPTION("Failed to open source file: " + source.string(), error_code);
    }

    // Use RAII for source file
    auto src_file = std::unique_ptr<FILE, FileCloser>(src_raw, FileCloser{file_ops_});

    // Open destination file for writing
    FILE* dest_raw = file_ops_->fopen(destination.string().c_str(), "wb");
    if (!dest_raw) {
        int error_code = errno;
        LOG_AND_THROW_BLOB_EXCEPTION("Failed to open destination file: " + destination.string(), error_code);
    }

    // Use RAII for destination file
    auto dest_file = std::unique_ptr<FILE, FileCloser>(dest_raw, FileCloser{file_ops_});

    try {
        // Buffer for copying
        std::array<char, copy_buffer_size> buffer = {};
        size_t bytes_read = 0;

        // Copy loop
        while ((bytes_read = file_ops_->fread(buffer.data(), 1, copy_buffer_size, src_file.get())) > 0) {
            size_t bytes_written = file_ops_->fwrite(buffer.data(), 1, bytes_read, dest_file.get());
            if (bytes_written != bytes_read) {
                int error_code = errno;
                LOG_AND_THROW_BLOB_EXCEPTION("Failed to write data to destination file: " + destination.string(), error_code);
            }
        }

        // Check for read errors
        if (file_ops_->ferror(src_file.get()) != 0) {
            int error_code = errno;
            LOG_AND_THROW_BLOB_EXCEPTION("Error reading from source file: " + source.string(), error_code);
        }

        // Flush destination file
        if (file_ops_->fflush(dest_file.get()) != 0) {
            int error_code = errno;
            LOG_AND_THROW_BLOB_EXCEPTION("Failed to flush data to destination file: " + destination.string(), error_code);
        }

        // Synchronize destination file to disk
        if (file_ops_->fsync(file_ops_->fileno(dest_file.get())) != 0) {
            int error_code = errno;
            LOG_AND_THROW_BLOB_EXCEPTION("Failed to synchronize destination file to disk: " + destination.string(), error_code);
        }

    } catch (...) {
        boost::system::error_code ec;
        file_ops_->remove(destination, ec);
        if (ec && ec != boost::system::errc::no_such_file_or_directory) {
            VLOG_LP(log_error) << "Failed to remove file: " << destination.string() << ". Error: " << ec.message();
        }
        throw;  // Re-throw the original exception
    }
}

void blob_pool_impl::move_file(const boost::filesystem::path& source, const boost::filesystem::path& destination) {
    // Ensure the destination directory exists
    boost::filesystem::path destination_dir = destination.parent_path();
    boost::system::error_code ec;

    create_directories_if_needed(destination_dir);

    file_ops_->rename(source, destination, ec);
    if (!ec) {
        return;
    }

    if (ec.value() == EXDEV) { // EXDEV: "Invalid cross-device link"
        copy_file(source, destination);
        // Remove the source file
        file_ops_->remove(source, ec);
        if (ec) {
            LOG_AND_THROW_BLOB_EXCEPTION("Failed to remove source file after copy: " + source.string(), ec.value());
        }
    } else {
        // Other errors are not recoverable
        LOG_AND_THROW_BLOB_EXCEPTION("Failed to rename file: " + source.string() + " -> " + destination.string(), ec.value());
    }
}

void blob_pool_impl::create_directories_if_needed(const boost::filesystem::path& path) {
    boost::system::error_code ec;

    if (!file_ops_->exists(path, ec)) {
        file_ops_->create_directories(path, ec);
        if (ec) {
            LOG_AND_THROW_BLOB_EXCEPTION("Failed to create directories: " + path.string(), ec.value());
        }
    }
}

blob_id_type blob_pool_impl::register_data(std::string_view data) {
    // Check if the pool has already been released
    if (is_released_.load(std::memory_order_acquire)) {
        throw std::logic_error("This pool is already released.");
    }

    // Generate a unique BLOB ID and resolve the target path
    blob_id_type id = generate_blob_id();
    boost::filesystem::path target_path = resolver_.resolve_path(id);

    // Ensure the destination directory exists
    boost::filesystem::path destination_dir = target_path.parent_path();
    boost::system::error_code ec;
    create_directories_if_needed(destination_dir);

    // Open destination file for writing
    FILE* dest_raw = file_ops_->fopen(target_path.string().c_str(), "wb");
    if (!dest_raw) {
        int error_code = errno;
        LOG_AND_THROW_BLOB_EXCEPTION("Failed to open destination file: " + target_path.string(), error_code);
    }

    // Use RAII for destination file
    auto dest_file = std::unique_ptr<FILE, FileCloser>(dest_raw, FileCloser{file_ops_});

    try {
        // Write data to the destination file
        size_t bytes_written = file_ops_->fwrite(data.data(), 1, data.size(), dest_file.get());
        if (bytes_written != data.size()) {
            int error_code = errno;
            LOG_AND_THROW_BLOB_EXCEPTION("Failed to write data to destination file: " + target_path.string(), error_code);
        }

        // Flush destination file
        if (file_ops_->fflush(dest_file.get()) != 0) {
            int error_code = errno;
            LOG_AND_THROW_BLOB_EXCEPTION("Failed to flush data to destination file: " + target_path.string(), error_code);
        }

        // Synchronize destination file to disk
        if (file_ops_->fsync(file_ops_->fileno(dest_file.get())) != 0) {
            int error_code = errno;
            LOG_AND_THROW_BLOB_EXCEPTION("Failed to synchronize destination file: " + target_path.string(), error_code);
        }
    } catch (...) {
        // Ensure file is removed in case of an exception
        file_ops_->remove(target_path, ec);
        if (ec && ec != boost::system::errc::no_such_file_or_directory) {
            VLOG_LP(log_error) << "Failed to remove file: " << target_path.string() << ". Error: " << ec.message();
        }
        throw;
    }

    return id;
}

} // namespace limestone::internal
