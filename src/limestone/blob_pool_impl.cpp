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
    if (!file_ops_->exists(target_dir, ec)) {
        file_ops_->create_directories(target_dir, ec);
        if (ec) {
            LOG_AND_THROW_BLOB_EXCEPTION("Failed to create directory: " + target_dir.string(), ec);
        }
    }

    // Copy or move the source file
    if (is_temporary_file) {
        file_ops_->rename(file, target_path, ec);

        if (ec == boost::system::errc::cross_device_link) {
            handle_cross_filesystem_move(file, target_path, ec);
        } else if (ec) {
            LOG_AND_THROW_BLOB_EXCEPTION("Failed to move file: " + file.string() + " to " + target_path.string(), ec);
        }
    } else {
        file_ops_->copy_file(file, target_path, ec);
        if (ec) {
            LOG_AND_THROW_BLOB_EXCEPTION("Failed to copy file: " + file.string() + " to " + target_path.string(), ec);
        }
    }

    return id;
}



blob_id_type blob_pool_impl::register_data(std::string_view /*data*/) {
    return generate_blob_id(); // ダミーとして新しいIDを返す
}

blob_id_type blob_pool_impl::duplicate_data(blob_id_type /*reference*/) {
    return generate_blob_id(); // ダミーとして新しいIDを返す
}

void blob_pool_impl::set_file_operations(file_operations& file_ops) {
    file_ops_ = &file_ops;
}

void blob_pool_impl::reset_file_operations() {
    file_ops_ = &real_file_ops_; 
}

void blob_pool_impl::handle_cross_filesystem_move(const boost::filesystem::path& source_path, 
                                                  const boost::filesystem::path& target_path, 
                                                  boost::system::error_code& ec) {
    file_ops_->copy_file(source_path, target_path, ec);
    if (ec) {
        LOG_AND_THROW_BLOB_EXCEPTION("Failed to copy file across filesystems: " + source_path.string() + " to " + target_path.string(), ec);
    }

    // Remove the source file after copying
    file_ops_->remove(source_path, ec);
    if (ec) {
        LOG_AND_THROW_BLOB_EXCEPTION("Failed to remove source file after copying: " + source_path.string(), ec);
    }
}

#include <cstdio>
#include <memory>
#include <iostream>

struct FileCloser {
    file_operations* file_ops;

    void operator()(FILE* file) const {
        if (file && file_ops->fclose(file) != 0) {
            std::cerr << "Error closing file." << std::endl;
        }
    }
};

void blob_pool_impl::copy_file(const boost::filesystem::path& source, const boost::filesystem::path& destination) {
    // Ensure the destination directory exists
    boost::filesystem::path destination_dir = destination.parent_path();
    boost::system::error_code ec;

    if (!file_ops_->exists(destination_dir, ec)) {
        file_ops_->create_directories(destination_dir, ec);
        if (ec) {
            LOG_AND_THROW_BLOB_EXCEPTION("Failed to create directory: " + destination_dir.string(), ec.value());
        }
    }

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

    // Buffer for copying
    char buffer[copy_buffer_size];
    size_t bytes_read = 0;

    // Copy loop
    while ((bytes_read = file_ops_->fread(buffer, 1, copy_buffer_size, src_file.get())) > 0) {
        size_t bytes_written = file_ops_->fwrite(buffer, 1, bytes_read, dest_file.get());
        if (bytes_written != bytes_read) {
            int error_code = errno;
            LOG_AND_THROW_BLOB_EXCEPTION("Failed to write data to destination file: " + destination.string(), error_code);
        }
    }

    // Check for read errors
    if (file_ops_->ferror(src_file.get())) {
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
}


} // namespace limestone::internal
