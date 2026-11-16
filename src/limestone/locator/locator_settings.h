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

#include <cstddef>
#include <cstdint>

namespace limestone::internal {

/// Settings of the locator file.
struct locator_settings {
    /// The record interval (bytes). A record is written each time the bytes written to the locatee file reach a
    /// multiple of this value
    std::uint64_t record_interval_bytes{std::uint64_t{32} * 1024 * 1024};
    /// The number of records in one region (R)
    std::size_t region_records{1};
};

}  // namespace limestone::internal
