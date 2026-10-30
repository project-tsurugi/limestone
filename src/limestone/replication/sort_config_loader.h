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

#include <cstdint>
#include <optional>
#include <string>

#include <limestone/api/configuration.h>

namespace limestone::replication {

/**
 * @brief The settings of the sort read from the environment variables of tgreplica.
 *
 * An item not set is nullopt, and the default of the configuration is kept for it.
 */
struct sort_config {
    std::optional<int> threads{};                  ///< LIMESTONE_SORT_THREADS
    std::optional<std::uint64_t> arena_size_mb{};  ///< LIMESTONE_SORT_ARENA_SIZE_MB
    std::optional<std::string> work_dir{};         ///< LIMESTONE_SORT_WORK_DIR
    std::optional<std::uint64_t> merge_fanin{};    ///< LIMESTONE_SORT_MERGE_FANIN
};

/**
 * @brief Puts the items that are set into the configuration. The ranges of the values are checked at the
 *        construction of the datastore.
 * @param config the settings read
 * @param conf the configuration to put them into
 */
void apply_sort_config(const sort_config& config, limestone::api::configuration& conf);

/**
 * @brief The result of reading the settings of the sort.
 */
struct sort_config_parse_result {
    bool ok{false};               ///< whether the reading succeeded
    std::string error_message{};  ///< the reason of the failure; empty when ok
    sort_config config{};         ///< the settings read; the defaults when not ok
};

/**
 * @brief Reads the settings of the sort from the environment variables.
 *
 * LIMESTONE_SORT_THREADS, LIMESTONE_SORT_ARENA_SIZE_MB and LIMESTONE_SORT_MERGE_FANIN are decimal non-negative
 * integers (THREADS within the range of int); any other string fails. LIMESTONE_SORT_WORK_DIR is a string, and an
 * empty one is the same as not set.
 * @return the settings read, or the reason of the failure
 */
[[nodiscard]] sort_config_parse_result load_sort_config_from_environment();

}  // namespace limestone::replication
