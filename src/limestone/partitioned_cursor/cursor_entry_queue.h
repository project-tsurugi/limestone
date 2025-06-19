/*
 * Copyright 2023-2025 Project Tsurugi.
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

#include <variant>
#include <boost/lockfree/spsc_queue.hpp>
#include "log_entry.h"
#include "partitioned_cursor/end_marker.h"

namespace limestone::internal {

using limestone::api::log_entry;

/**
 * @brief Represents an item stored in the cursor queue.
 *
 * This can either be a log_entry (normal case) or an end_marker (indicating completion or error).
 */
using cursor_entry_type = std::variant<log_entry, end_marker>;

/**
 * @brief Lock-free single-producer, single-consumer queue for cursor entries.
 *
 * This class wraps a boost::lockfree::spsc_queue and is designed specifically for
 * one producer thread and one consumer thread.
 *
 * It provides non-blocking push and blocking pop, and ensures minimal overhead for
 * high-performance cursor data streaming.
 */
class cursor_entry_queue {
public:
    /**
     * @brief Constructs a new cursor_entry_queue.
     *
     * @param capacity The maximum number of entries the queue can hold.
     */
    explicit cursor_entry_queue(std::size_t capacity);

    /**
     * @brief Pushes an entry into the queue.
     *
     * @param entry The entry to push.
     * @return true if the entry was successfully pushed; false otherwise.
     *
     * @note This must be called only from the producer thread.
     */
    [[nodiscard]] bool push(const cursor_entry_type& entry) noexcept;

    /**
     * @brief Waits for and pops the next available entry.
     *
     * This is a blocking operation. It must be called only from the consumer thread.
     *
     * @return The next cursor entry in the queue.
     */
    [[nodiscard]] cursor_entry_type wait_and_pop();

private:
    boost::lockfree::spsc_queue<cursor_entry_type> queue_;
};

} // namespace limestone::internal
