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
#pragma once

#include <condition_variable>
#include <deque>
#include <mutex>
#include <optional>

#include <boost/filesystem.hpp>

namespace limestone::internal {

/**
 * @brief The queue of the input files of a scan. The producer pushes the paths and closes the queue, and the
 *        threads of the scan pop them one by one.
 *
 * All the operations are thread-safe. pop() waits while the queue is empty and neither closed nor aborted.
 */
class input_file_queue {
public:
    input_file_queue() = default;
    ~input_file_queue() = default;
    input_file_queue(const input_file_queue&) = delete;
    input_file_queue& operator=(const input_file_queue&) = delete;
    input_file_queue(input_file_queue&&) = delete;
    input_file_queue& operator=(input_file_queue&&) = delete;

    /**
     * @brief Pushes a path to the end. FATAL if called after close(). Discarded after abort().
     * @param path the path of an input file
     */
    void push(boost::filesystem::path path);

    /**
     * @brief Closes the queue. push() is not allowed afterwards. pop() returns nullopt once the rest is taken out.
     *        The second and later calls do nothing.
     */
    void close() noexcept;

    /**
     * @brief Aborts the queue. The remaining paths are discarded, and every pop() afterwards returns nullopt
     *        immediately. It can be called after close(). The second and later calls do nothing.
     */
    void abort() noexcept;

    /**
     * @brief Takes the first path out. If the queue is empty and neither closed nor aborted, waits until one of
     *        them happens.
     * @return the path taken out, or nullopt if the queue is closed and empty, or aborted
     */
    std::optional<boost::filesystem::path> pop();

    /**
     * @brief Returns true if aborted.
     */
    [[nodiscard]] bool aborted() const noexcept;

private:
    mutable std::mutex mutex_;
    std::condition_variable not_empty_;
    std::deque<boost::filesystem::path> paths_{};
    bool closed_{};
    bool aborted_{};
};

}  // namespace limestone::internal
