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

namespace limestone::api {

// Internal implementation class for datastore (Pimpl idiom).
// This header is for internal use only.
class datastore_impl {
public:
    // Default constructor initializes the backup counter to zero.
    datastore_impl() : backup_counter_(0) {}

    // Default destructor.
    ~datastore_impl() = default;

    // Increments the backup counter.
    void increment_backup_counter() noexcept {
        backup_counter_.fetch_add(1, std::memory_order_acq_rel);
    }

    // Decrements the backup counter.
    void decrement_backup_counter() noexcept {
        backup_counter_.fetch_sub(1, std::memory_order_acq_rel);
    }

private:
    // Atomic counter for tracking active backup operations.
    std::atomic<int> backup_counter_;
};

} // namespace limestone::api
