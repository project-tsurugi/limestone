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
#include <limestone/api/blob_id_type.h>
#include <limestone/api/cursor.h>
#include <limestone/api/storage_id_type.h>

#include <boost/filesystem.hpp>
#include <map>
#include <memory>
#include <optional>
#include <string>

#include "cursor_impl_base.h"
#include "log_entry/log_entry.h"
#include "log_entry/log_entry_reader.h"
#include "log_entry/log_entry_view.h"
#include "region_allocator.h"

namespace limestone::internal {

using limestone::api::cursor;
using limestone::api::storage_id_type;
using limestone::api::write_version_type;    
class cursor_impl : public cursor_impl_base {
public:
    cursor_impl(const boost::filesystem::path& snapshot_file, std::map<storage_id_type, write_version_type> clear_storage);

    cursor_impl(const boost::filesystem::path& snapshot_file, const boost::filesystem::path& compacted_file,
                std::map<storage_id_type, write_version_type> clear_storage);

    /**
     * @brief 領域の割り当てから領域を受け取って読む cursor を作る。
     *
     * ファイルはここで開く。領域は next() の中で、読むものがなくなるたびに割り当てに要求し、残りがなくなるまで読む。
     * 他の cursor と割り当てを共有し、全体でそれぞれの領域を 1 回ずつ読む。
     * @param snapshot_file スナップショットファイル
     * @param compacted_file コンパクション済み WAL ファイル。なければ nullopt (割り当てのロケータと一致していること)
     * @param clear_storage ストレージごとの、これより古いエントリを読み飛ばす write_version
     * @param allocator 領域の割り当て。空なら FATAL ログを書いて abort する
     * @throws limestone_io_exception ファイルを開けないとき
     */
    cursor_impl(const boost::filesystem::path& snapshot_file, const std::optional<boost::filesystem::path>& compacted_file,
                std::map<storage_id_type, write_version_type> clear_storage, std::shared_ptr<region_allocator> allocator);

    static std::unique_ptr<cursor> create_cursor(const boost::filesystem::path& snapshot_file,
                                                  const std::map<limestone::api::storage_id_type, limestone::api::write_version_type>& clear_storage);
    static std::unique_ptr<cursor> create_cursor(const boost::filesystem::path& snapshot_file, const boost::filesystem::path& compacted_file,
                                                  const std::map<limestone::api::storage_id_type, limestone::api::write_version_type>& clear_storage);
    /**
     * @brief 領域の割り当てから領域を受け取って読む cursor を作り、api::cursor に包んで返す。引数は同じ引数の
     *        コンストラクタと同じ。
     */
    static std::unique_ptr<cursor> create_cursor(const boost::filesystem::path& snapshot_file,
                                                 const std::optional<boost::filesystem::path>& compacted_file,
                                                 const std::map<limestone::api::storage_id_type, limestone::api::write_version_type>& clear_storage,
                                                 std::shared_ptr<region_allocator> allocator);
protected:
    /// 1 つのファイルを読み進める状態
    struct input_stream {
        /// メッセージに使う、ファイルの種類の名前
        std::string name{};
        /// ファイルを読む reader。読むファイルがなければ空
        std::optional<log_entry_reader> reader{};
        /// 今の領域を読み終えたかどうか。true なら、次の領域を受け取るまで reader から読まない
        bool finished{true};
        /// 読んで、まだ返していないエントリ
        std::optional<limestone::api::log_entry> entry{};
        /// 昇順の検査に使う、直前に読んだエントリの key_sid
        std::string previous_key_sid{};
    };

private:
    limestone::api::log_entry log_entry_;
    input_stream snapshot_{"Snapshot"};
    input_stream compacted_{"Compacted"};
    std::map<limestone::api::storage_id_type, limestone::api::write_version_type> clear_storage_; 
    /// 領域の割り当て。割り当てを渡さずに作った cursor (ファイル全体を読む) では空
    std::shared_ptr<region_allocator> allocator_{};
    /// 今の領域の開始キー。これより小さいエントリは読み飛ばす。なければ読み飛ばさない
    std::optional<std::string> start_key_{};
    /// 今の領域の終了キー。これ以上のエントリに達したら、そのファイルの今の領域を読み終える。なければ領域の終了位置まで読む
    std::optional<std::string> end_key_{};

    /**
     * @brief 割り当てに次の領域を要求し、あれば reader の読む範囲とキーの範囲をその領域に設定する。
     * @return 次の領域があれば true。割り当てがない (領域を受け取らずに読む cursor)、残りがない、閉じた後なら false
     * @throws limestone_exception 割り当てのプローブが失敗したとき (ロケータが不正)
     * @throws limestone_io_exception 読み込みに失敗したとき
     */
    bool acquire_next_range();

protected:
    void close() override;

    bool next() override;

    /**
     * @brief ファイルから、次に返す候補のエントリを 1 件読んで stream.entry に入れる。
     *
     * データ系でないエントリ、直前と同じキーのエントリ、clear_storage の境界より古いエントリ、開始キーより小さい
     * エントリは読み飛ばす。領域の終了位置に達したか、終了キー以上のエントリに達したら (今の領域を読み終えたら)
     * stream.finished を true にし、stream.entry は空のままにする。stream.finished が true なら何もしない。
     * @param stream 読み進める状態
     * @throws limestone_exception ファイルの内容が形式に合わないとき、キーが昇順でないとき
     * @throws limestone_io_exception 読み込みに失敗したとき
     */
    void validate_and_read_stream(input_stream& stream);

    [[nodiscard]] limestone::api::storage_id_type storage() const noexcept override;
    void key(std::string& buf) const noexcept override;
    void value(std::string& buf) const noexcept override;
    [[nodiscard]] std::vector<limestone::api::blob_id_type> blob_ids() const override;
    [[nodiscard]] limestone::api::log_entry::entry_type type() const override;
    bool is_relevant_entry(const log_entry_view& entry);
    [[nodiscard]] log_entry& current() override;
    // Making the cursor class a friend so that it can access protected members
    friend class limestone::api::cursor;
};

} // namespace limestone::internal
