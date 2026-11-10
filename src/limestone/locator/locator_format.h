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
#include <string>
#include <string_view>

#include <boost/filesystem.hpp>

namespace limestone::internal::locator_format {

/// ヘッダの長さ (バイト)
constexpr std::size_t header_size = 4096;

/// レコードの長さ (バイト)。ロケーティファイルの先頭からのバイト位置をリトルエンディアンで持つ
constexpr std::size_t record_size = 8;

/// 終端の長さ (バイト)。ロケーティファイルの大きさ (8 バイト) と終端のマジック (8 バイト)
constexpr std::size_t footer_size = 16;

/// ヘッダのマジック。ASCII の "LIMELOC" と 0x00 をリトルエンディアンの整数として読んだ値
constexpr std::uint64_t header_magic = 0x00434F4C454D494CULL;

/// 終端のマジック。ヘッダのマジックの最上位ビットを 1 にした値
constexpr std::uint64_t footer_magic = 0x80434F4C454D494CULL;

/// ロケータファイルの形式バージョン。読む側はこの値以外を不正とする
constexpr std::uint64_t format_version = 1;

/// ロケータファイルの名前の末尾
constexpr std::string_view suffix = ".loc";

/**
 * @brief ロケーティファイルの名前からロケータファイルの名前を作る。
 * @param locatee_name ロケーティファイルの名前
 * @return 名前に .loc を付けたもの
 */
[[nodiscard]] std::string locator_file_name(std::string_view locatee_name);

/**
 * @brief ロケータファイルの名前からロケーティファイルの名前を作る。
 * @param locator_name ロケータファイルの名前 (.loc で終わること。そうでなければ FATAL ログを書いて abort する)
 * @return 名前から .loc を除いたもの
 */
[[nodiscard]] std::string locatee_file_name(std::string_view locator_name);

/**
 * @brief ロケーティファイルのパスからロケータファイルのパスを作る。同じディレクトリで、名前に .loc を付けたもの。
 * @param locatee ロケーティファイルのパス
 * @return ロケータファイルのパス
 */
[[nodiscard]] boost::filesystem::path locator_file_path(const boost::filesystem::path& locatee);

/**
 * @brief 名前がロケータファイルの名前 (.loc で終わる) かを返す。
 * @param name ファイルの名前 (ディレクトリを含まない)
 * @return .loc で終われば true
 */
[[nodiscard]] bool is_locator_file_name(std::string_view name);

}  // namespace limestone::internal::locator_format
