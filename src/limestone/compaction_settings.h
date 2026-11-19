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

#include <chrono>
#include <cstdint>

namespace limestone::internal {

/// @brief The settings of the compaction and the BLOB file GC.
struct compaction_settings {
    /// The threshold of the amount of the WAL at which the auto compaction runs, in bytes. 0 disables it
    std::uint64_t threshold_bytes{0};
    /// Whether the auto compaction removes the detached WAL files
    bool delete_detached{true};
    /// Whether the BLOB file GC runs. It applies to both the GC at startup and the GC in an online compaction,
    /// whichever the trigger of the compaction and the role (master or replica) are
    bool blob_gc{true};
    /// The interval of the measurement of the amount of the WAL. Only tests change it from the default
    std::chrono::milliseconds measurement_interval{std::chrono::seconds(10)};
};

}  // namespace limestone::internal
