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

#include <atomic>

#include <boost/filesystem.hpp>

namespace limestone::internal {

/**
 * @brief One input of a scan. Tells the WAL file to read and whether to stop reading.
 *
 * A scan is aborted as a whole, not per input. An input popped out of a queue answers the abort of the queue with
 * stopped(). An input made alone never stops.
 */
class scan_input {
public:
    /**
     * @brief Makes an input that never stops.
     * @param path the path of the WAL file to read
     */
    explicit scan_input(boost::filesystem::path path);

    /**
     * @brief Makes an input with a stop signal.
     * @param path the path of the WAL file to read
     * @param stop the stop signal. stopped() returns true once it becomes true. It must outlive this input
     */
    scan_input(boost::filesystem::path path, const std::atomic<bool>& stop);

    /**
     * @brief Returns the path of the WAL file to read.
     */
    [[nodiscard]] const boost::filesystem::path& path() const noexcept;

    /**
     * @brief Returns true if reading should stop.
     */
    [[nodiscard]] bool stopped() const noexcept;

private:
    boost::filesystem::path path_;
    const std::atomic<bool>* stop_{};
};

}  // namespace limestone::internal
