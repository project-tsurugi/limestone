/*
 * Copyright 2022-2024 Project Tsurugi.
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
#pragma once

#include <atomic>

#include "limestone/api/datastore.h"
#include "limestone/logging.h"
#include "logging_helper.h"

namespace limestone::api {

// Internal implementation class for datastore (Pimpl idiom).
// This header is for internal use only.
class datastore_impl {
public:
    // Default constructor initializes the backup counter to zero.
    datastore_impl() : backup_counter_(0) {}

    // Default destructor.
    ~datastore_impl() = default;

    // Deleted copy and move constructors and assignment operators.
    datastore_impl(const datastore_impl&) = delete;
    datastore_impl& operator=(const datastore_impl&) = delete;
    datastore_impl(datastore_impl&&) = delete;
    datastore_impl& operator=(datastore_impl&&) = delete;

    // Increments the backup counter.
    void increment_backup_counter() noexcept {
        backup_counter_.fetch_add(1, std::memory_order_acq_rel);
        LOG_LP(INFO) << "Beginning backup; active backup count: " << backup_counter_.load(std::memory_order_acquire);
    }

    // Decrements the backup counter.
    void decrement_backup_counter() noexcept {
        backup_counter_.fetch_sub(1, std::memory_order_acq_rel);
        LOG_LP(INFO) << "Ending backup; active backup count: " << backup_counter_.load(std::memory_order_acquire);
    }

    // Returns true if a backup operation is in progress.
    [[nodiscard]] bool is_backup_in_progress() const noexcept {
        int count = backup_counter_.load(std::memory_order_acquire);
        VLOG_LP(log_info) << "Checking if backup is in progress; active backup count: " << count;
        return count > 0;
    }

private:
    // Atomic counter for tracking active backup operations.
    std::atomic<int> backup_counter_;
};

}  // namespace limestone::api
