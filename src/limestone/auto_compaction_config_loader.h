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
 * @brief 環境変数から読んだ常時コンパクションの設定。
 *
 * 設定されていない項目は nullopt であり、configuration の既定のままになる。
 */
struct auto_compaction_config {
    std::optional<std::uint64_t> threshold_mb{};  ///< LIMESTONE_AUTO_COMPACTION_THRESHOLD_MB
    std::optional<bool> delete_detached{};        ///< LIMESTONE_AUTO_COMPACTION_DELETE_DETACHED
    std::optional<bool> blob_gc{};        ///< LIMESTONE_BLOB_GC
};

/**
 * @brief 設定されている項目を configuration に入れる。
 * @param config 読んだ設定
 * @param conf 設定を入れる configuration
 */
void apply_auto_compaction_config(const auto_compaction_config& config, limestone::api::configuration& conf);

/**
 * @brief 常時コンパクションの設定を読んだ結果。
 */
struct auto_compaction_config_parse_result {
    bool ok{false};                   ///< 読み込みに成功したか
    std::string error_message{};      ///< 失敗の理由。ok のときは空
    auto_compaction_config config{};  ///< 読んだ設定。ok でないときは既定
};

/**
 * @brief 常時コンパクションの設定を環境変数から読む。
 *
 * どの変数も 10 進の非負の整数であり、それ以外の文字列 (符号つき、空文字列を含む) は失敗になる。
 * LIMESTONE_AUTO_COMPACTION_THRESHOLD_MB は閾値 (MiB) で、0 は常時コンパクションを行わないことを表す。
 * LIMESTONE_AUTO_COMPACTION_DELETE_DETACHED と LIMESTONE_BLOB_GC は、0 が無効、0 以外が有効を表す。
 * @return 読んだ設定、または失敗の理由
 */
[[nodiscard]] auto_compaction_config_parse_result load_auto_compaction_config_from_environment();

}  // namespace limestone::internal
