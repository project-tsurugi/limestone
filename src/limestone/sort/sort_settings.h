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

namespace limestone::internal {

/// the settings of the sort. The defaults are held here
struct sort_settings {
    /// the number of threads of the scan and the sort
    int threads{8};
    /// the size of the arena per thread in bytes
    std::size_t arena_size_bytes{std::size_t{256} * 1024 * 1024};
    /// the merge fan-in (the number of inputs opened at once)
    std::size_t merge_fanin{128};
};

}  // namespace limestone::internal
