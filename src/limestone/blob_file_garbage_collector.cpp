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
 
#include "blob_file_garbage_collector.h"
#include "logging_helper.h"

#include <boost/filesystem.hpp>
#include <sstream>
#include <iomanip>
#include <iostream>
#include <stdexcept>
#include <exception>

namespace limestone::internal {

blob_file_garbage_collector::blob_file_garbage_collector(const blob_file_resolver &resolver)
    : resolver_(resolver)
{
    file_ops_ = std::make_unique<real_file_operations>(); 
}

blob_file_garbage_collector::~blob_file_garbage_collector() {
}

void blob_file_garbage_collector::start_scan(blob_id_type max_existing_blob_id) {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (scan_waited_) {
            throw std::logic_error("Cannot start scan after wait_for_scan() has been called.");
        }
        if (scan_started_) {
            throw std::logic_error("Scan has already been started.");
        }
        scan_started_ = true;
        max_existing_blob_id_ = max_existing_blob_id;
        scan_complete_ = false;
    }
    // Launch the scanning thread
    scan_thread_ = std::thread(&blob_file_garbage_collector::scan_directory, this);
}


void blob_file_garbage_collector::scan_directory() {
    try {
        // Obtain the root directory from the resolver.
        boost::filesystem::path root = resolver_.get_blob_root();

        // Iterate recursively over the root directory.
        for (boost::filesystem::recursive_directory_iterator it(root), end; it != end; ++it) {
            if (boost::filesystem::is_regular_file(it->path())) {
                const boost::filesystem::path& file_path = it->path();

                // Use blob_file_resolver's function to check if this file is a valid blob_file.
                if (!resolver_.is_blob_file(file_path)) {
                    continue;
                }
                
                // Extract blob_id from the file path.
                blob_id_type id = resolver_.extract_blob_id(file_path);

                // Only consider files with blob_id <= max_existing_blob_id_
                if (id <= max_existing_blob_id_) {
                    blob_id_type id = resolver_.extract_blob_id(file_path);
                    scaned_blobs_.add_blob_item(blob_item(id));
                }
            }
        }
    } catch (const std::exception &e) {
        LOG_LP(ERROR) << "Exception in blob_file_garbage_collector::scan_directory: " << e.what();
    }
    {
        std::lock_guard<std::mutex> lock(mutex_);
        scan_complete_ = true;
    }
    scan_cv_.notify_all();
}

void blob_file_garbage_collector::add_gc_exempt_blob_item(const blob_item &item) {
    gc_exempt_blob_.add_blob_item(item);
}

void blob_file_garbage_collector::finalize_scan_and_cleanup() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        // If wait_for_cleanup() has already been called, do not start cleanup.
        if (cleanup_waited_) {
            return;
        }
        // Mark the start of the cleanup process
        cleanup_started_ = true;
    }
    cleanup_thread_ = std::thread([this]() {
        // Wait for the scan to complete
        this->wait_for_scan();
        
        // Calculate the difference and perform deletion operations
        scaned_blobs_.diff(gc_exempt_blob_);
        for (const auto &item : scaned_blobs_) {
            boost::filesystem::path file_path = resolver_.resolve_path(item.get_blob_id());
            boost::system::error_code ec;
            file_ops_->remove(file_path, ec);
            if (ec && ec != boost::system::errc::no_such_file_or_directory) {
                LOG_LP(ERROR) << "Failed to remove file: " << file_path.string()
                              << " Error: " << ec.message();
            }
        }
        
        {
            std::lock_guard<std::mutex> lock(mutex_);
            // Mark that cleanup is complete
            cleanup_complete_ = true;
        }
        // Notify any thread waiting for cleanup completion
        cleanup_cv_.notify_all();
    });
}

void blob_file_garbage_collector::wait_for_scan() {
    std::unique_lock<std::mutex> lock(mutex_);
    // Mark that wait_for_scan() has been called.
    scan_waited_ = true;
    // If the scan has not been started, return immediately.
    if (!scan_started_) {
        return;
    }
    // Wait until the scan is complete.
    scan_cv_.wait(lock, [this]() { return scan_complete_; });
}

void blob_file_garbage_collector::wait_for_cleanup() {
    std::unique_lock<std::mutex> lock(mutex_);
    // Mark that wait_for_cleanup() has been called.
    cleanup_waited_ = true;
    // If cleanup has not started, return immediately to avoid indefinite blocking.
    if (!cleanup_started_) {
        return;
    }
    // Wait until the cleanup process is complete.
    cleanup_cv_.wait(lock, [this]() { return cleanup_complete_; });
}

void blob_file_garbage_collector::set_file_operations(std::unique_ptr<file_operations> file_ops) {
    file_ops_ = std::move(file_ops);
}

void blob_file_garbage_collector::shutdown() {
    wait_for_scan();
    wait_for_cleanup();

    if (scan_thread_.joinable()) {
        scan_thread_.join();
    }

    if (cleanup_thread_.joinable()) {
        cleanup_thread_.join();
    }
}


} // namespace limestone::internal
