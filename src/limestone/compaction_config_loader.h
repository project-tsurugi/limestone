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

namespace limestone::internal {

/**
 * @brief The settings of the compaction and the BLOB file GC read from the environment variables.
 *
 * An item that is not set is nullopt, and the configuration keeps its default for it.
 */
struct compaction_config {
    std::optional<std::uint64_t> threshold_mb{};  ///< LIMESTONE_AUTO_COMPACTION_THRESHOLD_MB
    std::optional<bool> delete_detached{};        ///< LIMESTONE_AUTO_COMPACTION_DELETE_DETACHED
    std::optional<bool> blob_gc{};                ///< LIMESTONE_BLOB_GC
};

/**
 * @brief Puts the items that are set into a configuration.
 * @param config the settings read
 * @param conf the configuration to put the settings into
 */
void apply_compaction_config(const compaction_config& config, limestone::api::configuration& conf);

/**
 * @brief The result of reading the settings of the compaction and the BLOB file GC.
 */
struct compaction_config_parse_result {
    bool ok{false};               ///< whether the read succeeded
    std::string error_message{};  ///< the reason for the failure; empty if ok
    compaction_config config{};   ///< the settings read; the defaults if not ok
};

/**
 * @brief Reads the settings of the compaction and the BLOB file GC from the environment variables.
 *
 * Every variable is a non-negative decimal integer; any other string (including a signed number and an empty
 * string) is a failure.
 * LIMESTONE_AUTO_COMPACTION_THRESHOLD_MB is the threshold in MiB, and 0 disables the auto compaction.
 * For LIMESTONE_AUTO_COMPACTION_DELETE_DETACHED and LIMESTONE_BLOB_GC, 0 means disabled and any other value
 * means enabled.
 * @return the settings read, or the reason for the failure
 */
[[nodiscard]] compaction_config_parse_result load_compaction_config_from_environment();

}  // namespace limestone::internal
