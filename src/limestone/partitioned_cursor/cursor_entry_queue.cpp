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

#include "partitioned_cursor/cursor_entry_queue.h"
#include <thread>

namespace limestone::internal {

cursor_entry_queue::cursor_entry_queue(std::size_t capacity)
    : queue_(capacity) {}

bool cursor_entry_queue::push(const cursor_entry_type& entry) noexcept {
    return queue_.push(entry);
}

cursor_entry_type cursor_entry_queue::wait_and_pop() {
    cursor_entry_type entry;
    // busy-wait を避けるために軽い sleep を入れる
    while (!queue_.pop(entry)) {
        std::this_thread::sleep_for(std::chrono::microseconds(10));
    }
    return entry;
}

} // namespace limestone::internal