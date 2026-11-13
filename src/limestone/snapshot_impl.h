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

#include <limestone/api/cursor.h>
#include <limestone/api/write_version_type.h>

#include <atomic>
#include <boost/filesystem.hpp>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "locator/locator.h"

namespace limestone::internal {

using limestone::api::cursor;
using limestone::api::storage_id_type;
using limestone::api::write_version_type;

class snapshot_impl {
public:
    /**
     * @param location データの置き場所
     * @param clear_storage ストレージごとの、これより古いエントリを読み飛ばす write_version
     * @param compacted_file_name カタログに記録された現在のコンパクション済み WAL ファイルの名前。なければ nullopt
     * @param region_records get_partitioned_cursors() の領域 1 つのロケータのレコード数 (R)
     */
    explicit snapshot_impl(boost::filesystem::path location, std::map<storage_id_type, write_version_type> clear_storage,
                           std::optional<std::string> compacted_file_name, std::size_t region_records) noexcept;

    /**
     * @brief スナップショットを n 本の cursor で分担して読むための cursor を返す。
     *
     * スナップショットファイルと (カタログにあれば) コンパクション済み WAL ファイルのロケータを読み、領域の割り当てを
     * 1 つ作って、それを共有する n 本の cursor を返す。各 cursor はファイルをここで開く (開いた後にファイルが削除されても
     * 読める)。領域は cursor の next() の中で受け取る。
     * @param n cursor の本数
     * @return n 本の cursor
     * @throws std::invalid_argument n が 0 のとき
     * @throws limestone_exception 2 回目の呼び出し、ロケータファイルが不正なとき
     * @throws limestone_io_exception ファイルを開けないか読めないとき
     */
    std::vector<std::unique_ptr<limestone::api::cursor>> get_partitioned_cursors(std::size_t n);
    [[nodiscard]] std::unique_ptr<cursor> get_cursor() const;

private:
    /**
     * @brief ファイルのロケータファイルを読む。
     * @throws limestone_exception ロケータファイルが不正なとき
     * @throws limestone_io_exception ファイルの大きさを取得できないか、ロケータファイルを読めないとき
     */
    [[nodiscard]] static locator read_locator(const boost::filesystem::path& locatee);

    boost::filesystem::path location_;
    std::map<storage_id_type, write_version_type> clear_storage;
    std::optional<std::string> compacted_file_name_;  ///< name of the current compacted file recorded in the catalog; std::nullopt if there is none
    std::size_t region_records_;
    std::atomic<bool> partitioned_called_{false};
};

} // namespace limestone::internal