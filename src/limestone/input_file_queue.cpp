/*
 * Copyright 2022-2026 Project Tsurugi.
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
#include "input_file_queue.h"

#include <utility>

#include <glog/logging.h>

#include <limestone/logging.h>

#include "logging_helper.h"

namespace limestone::internal {

void input_file_queue::push(boost::filesystem::path path) {
    {
        const std::lock_guard<std::mutex> lock(mutex_);
        if (closed_) {
            LOG_LP(FATAL) << "push() after close(): " << path.string();
        }
        if (aborted_) {
            return;
        }
        paths_.push_back(std::move(path));
    }
    not_empty_.notify_one();
}

void input_file_queue::close() noexcept {
    {
        const std::lock_guard<std::mutex> lock(mutex_);
        closed_ = true;
    }
    not_empty_.notify_all();
}

void input_file_queue::abort() noexcept {
    {
        const std::lock_guard<std::mutex> lock(mutex_);
        aborted_ = true;
        paths_.clear();
    }
    not_empty_.notify_all();
}

std::optional<boost::filesystem::path> input_file_queue::pop() {
    std::unique_lock<std::mutex> lock(mutex_);
    not_empty_.wait(lock, [this] { return ! paths_.empty() || closed_ || aborted_; });
    if (aborted_ || paths_.empty()) {
        return std::nullopt;
    }
    boost::filesystem::path path = std::move(paths_.front());
    paths_.pop_front();
    return path;
}

bool input_file_queue::aborted() const noexcept {
    const std::lock_guard<std::mutex> lock(mutex_);
    return aborted_;
}

}  // namespace limestone::internal
