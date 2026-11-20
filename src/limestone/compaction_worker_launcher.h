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

#include <functional>
#include <memory>
#include <mutex>

namespace limestone::internal {

/**
 * @brief Starts the online compaction worker thread only once.
 * @details launch() and close() exclude each other. launch() after close() does nothing.
 */
class compaction_worker_launcher {
public:
    /**
     * @brief Constructs a launcher.
     * @param start the function that starts the worker thread. launch() calls it at most once
     */
    explicit compaction_worker_launcher(std::function<void()> start);

    /**
     * @brief Starts the worker thread.
     * @details Does nothing if the thread has been started, or after close(). If the start fails, writes a FATAL
     *          log and aborts.
     * @attention this function is thread-safe.
     */
    void launch() noexcept;

    /**
     * @brief Disables launch() from now on.
     * @details No launch() is in progress when this function returns.
     * @attention this function is thread-safe.
     */
    void close() noexcept;

    /**
     * @brief Returns whether the worker thread has been started.
     * @attention this function is thread-safe.
     */
    [[nodiscard]] bool launched() noexcept;

private:
    std::mutex mutex_{};
    bool launched_{false};
    bool closed_{false};
    std::function<void()> start_;
};

/**
 * @brief A marker shared by a snapshot and the cursors obtained from it.
 * @details Requests the start of the worker thread when it is destroyed.
 */
class snapshot_use_marker {
public:
    /**
     * @brief Constructs a marker.
     * @param launcher the launcher to request the start from
     */
    explicit snapshot_use_marker(std::shared_ptr<compaction_worker_launcher> launcher) noexcept;

    /**
     * @brief Requests the start of the worker thread from the launcher.
     */
    ~snapshot_use_marker();

    snapshot_use_marker(const snapshot_use_marker&) = delete;
    snapshot_use_marker& operator=(const snapshot_use_marker&) = delete;
    snapshot_use_marker(snapshot_use_marker&&) = delete;
    snapshot_use_marker& operator=(snapshot_use_marker&&) = delete;

private:
    std::shared_ptr<compaction_worker_launcher> launcher_;
};

} // namespace limestone::internal
