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
#include "auto_compaction_config_loader.h"

#include <cerrno>
#include <cstdlib>
#include <utility>

namespace limestone::internal {

namespace {

constexpr const char* threshold_mb_env_name = "LIMESTONE_AUTO_COMPACTION_THRESHOLD_MB";
constexpr const char* delete_detached_env_name = "LIMESTONE_AUTO_COMPACTION_DELETE_DETACHED";
constexpr const char* blob_gc_env_name = "LIMESTONE_BLOB_GC";

/**
 * @brief 10 進の非負の整数を読む。符号、空白、後続の文字は許さない。
 * @param text 読む文字列
 * @return 値。形式が誤っているか、uint64 に収まらないときは nullopt
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
 * @brief 環境変数から 10 進の非負の整数を読んだ結果。
 */
struct unsigned_env_value {
    bool ok{false};                       ///< 形式が正しいか (設定されていないときも true)
    std::optional<std::uint64_t> value{};  ///< 値。設定されていないときは nullopt
};

/**
 * @brief 環境変数から 10 進の非負の整数を読む。
 * @param name 環境変数の名前
 * @return 読んだ結果
 */
unsigned_env_value read_unsigned_env(const char* name) {
    const char* env = std::getenv(name);
    if (env == nullptr) {
        return {true, std::nullopt};
    }
    const std::optional<std::uint64_t> parsed = parse_unsigned(env);
    return {parsed.has_value(), parsed};
}

/**
 * @brief 失敗の結果を作る。
 * @param name 値が誤っている環境変数の名前
 * @return ok が false で、設定が既定の結果
 */
auto_compaction_config_parse_result fail(const char* name) {
    return {false, std::string(name) + " is not a valid non-negative integer: " + std::getenv(name),
            auto_compaction_config{}};
}

}  // namespace

void apply_auto_compaction_config(const auto_compaction_config& config, limestone::api::configuration& conf) {
    if (config.threshold_mb.has_value()) {
        conf.set_auto_compaction_threshold_mb(config.threshold_mb.value());
    }
    if (config.delete_detached.has_value()) {
        conf.set_auto_compaction_delete_detached(config.delete_detached.value());
    }
    if (config.blob_gc.has_value()) {
        conf.set_blob_gc(config.blob_gc.value());
    }
}

auto_compaction_config_parse_result load_auto_compaction_config_from_environment() {
    auto_compaction_config config{};

    const unsigned_env_value threshold_mb = read_unsigned_env(threshold_mb_env_name);
    if (! threshold_mb.ok) {
        return fail(threshold_mb_env_name);
    }
    config.threshold_mb = threshold_mb.value;

    const unsigned_env_value delete_detached = read_unsigned_env(delete_detached_env_name);
    if (! delete_detached.ok) {
        return fail(delete_detached_env_name);
    }
    if (delete_detached.value.has_value()) {
        config.delete_detached = delete_detached.value.value() != 0;
    }

    const unsigned_env_value blob_gc = read_unsigned_env(blob_gc_env_name);
    if (! blob_gc.ok) {
        return fail(blob_gc_env_name);
    }
    if (blob_gc.value.has_value()) {
        config.blob_gc = blob_gc.value.value() != 0;
    }

    return {true, {}, config};
}

}  // namespace limestone::internal
