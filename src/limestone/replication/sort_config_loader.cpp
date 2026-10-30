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
#include "replication/sort_config_loader.h"

#include <cerrno>
#include <cstdlib>
#include <filesystem>
#include <limits>
#include <utility>

namespace limestone::replication {

namespace {

constexpr const char* threads_env_name = "LIMESTONE_SORT_THREADS";
constexpr const char* arena_size_mb_env_name = "LIMESTONE_SORT_ARENA_SIZE_MB";
constexpr const char* work_dir_env_name = "LIMESTONE_SORT_WORK_DIR";
constexpr const char* merge_fanin_env_name = "LIMESTONE_SORT_MERGE_FANIN";

/**
 * @brief Parses a decimal non-negative integer. No sign, no whitespace, no trailing characters.
 * @param text the string to parse
 * @return the value, or nullopt if the form is wrong or the value does not fit in uint64
 */
std::optional<std::uint64_t> parse_unsigned(const std::string& text) {
    if (text.empty() || text.find_first_not_of("0123456789") != std::string::npos) {
        return std::nullopt;
    }
    errno = 0;
    const std::uint64_t parsed = std::strtoull(text.c_str(), nullptr, 10);
    if (errno == ERANGE) {
        return std::nullopt;
    }
    return parsed;
}

/**
 * @brief Makes the result of a failure.
 * @param message the reason of the failure
 * @return the result with ok = false and the default settings
 */
sort_config_parse_result fail(std::string message) {
    return {false, std::move(message), sort_config{}};
}

}  // namespace

void apply_sort_config(const sort_config& config, limestone::api::configuration& conf) {
    if (config.threads.has_value()) {
        conf.set_recover_max_parallelism(config.threads.value());
    }
    if (config.arena_size_mb.has_value()) {
        conf.set_sort_arena_size_mb(config.arena_size_mb.value());
    }
    if (config.work_dir.has_value()) {
        conf.set_sort_work_dir(std::filesystem::path(config.work_dir.value()));
    }
    if (config.merge_fanin.has_value()) {
        conf.set_sort_merge_fanin(config.merge_fanin.value());
    }
}

sort_config_parse_result load_sort_config_from_environment() {
    sort_config config{};
    if (const char* env = std::getenv(threads_env_name); env != nullptr) {
        const auto parsed = parse_unsigned(env);
        if (! parsed.has_value() || parsed.value() > static_cast<std::uint64_t>(std::numeric_limits<int>::max())) {
            return fail(std::string(threads_env_name) + " is not a valid non-negative integer: " + env);
        }
        config.threads = static_cast<int>(parsed.value());
    }
    if (const char* env = std::getenv(arena_size_mb_env_name); env != nullptr) {
        const auto parsed = parse_unsigned(env);
        if (! parsed.has_value()) {
            return fail(std::string(arena_size_mb_env_name) + " is not a valid non-negative integer: " + env);
        }
        config.arena_size_mb = parsed;
    }
    if (const char* env = std::getenv(work_dir_env_name); env != nullptr && *env != '\0') {
        config.work_dir = std::string{env};
    }
    if (const char* env = std::getenv(merge_fanin_env_name); env != nullptr) {
        const auto parsed = parse_unsigned(env);
        if (! parsed.has_value()) {
            return fail(std::string(merge_fanin_env_name) + " is not a valid non-negative integer: " + env);
        }
        config.merge_fanin = parsed;
    }
    return {true, {}, config};
}

}  // namespace limestone::replication
