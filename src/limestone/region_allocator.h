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

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>

#include "locator/locator.h"
#include "log_entry/log_entry_reader.h"

namespace limestone::internal {

/**
 * @brief 領域 1 つの範囲。region_allocator::allocate() が返す。
 *
 * 位置はいずれもファイルの先頭からのバイト位置で、開始位置は読み始めるエントリの先頭、終了位置はそこまで読んで止まる
 * エントリの境界 (次のエントリの先頭かファイルの末尾)。キーは serialized key_sid で、開始キーより小さいエントリは
 * 読み飛ばし、終了キー以上のエントリに達したら読み終える。
 */
struct region_range {
    /// スナップショットファイルの読み始める位置
    std::uint64_t snapshot_start{};
    /// スナップショットファイルの読み終える位置
    std::uint64_t snapshot_end{};
    /// コンパクション済み WAL ファイルの読み始める位置。そのファイルがなければ 0
    std::uint64_t compacted_start{};
    /// コンパクション済み WAL ファイルの読み終える位置。そのファイルがなければ 0
    std::uint64_t compacted_end{};
    /// 開始キー。先頭の領域ではなし
    std::optional<std::string> start_key{};
    /// 終了キー。最後の領域ではなし
    std::optional<std::string> end_key{};
};

/**
 * @brief 領域の割り当て。インデックス反映の読み取りを分担する n 本の cursor が 1 つを共有し、未処理の領域を 1 つずつ
 *        配る。
 *
 * スナップショットファイルとコンパクション済み WAL ファイルのロケータと R (領域 1 つのレコード数) を受け取り、レコード数の
 * 多いほうを主ファイル、他方を副ファイルとする (同数ならコンパクション済み WAL ファイルが主。コンパクション済み WAL
 * ファイルがなければスナップショットファイルが主で、副はない)。領域数は ceil(主のレコード数 ÷ R) で、主のレコードが
 * 0 個なら領域はない。
 *
 * 領域 j の範囲は次のとおり。
 * - 主ファイルの開始位置はレコード j × R の位置、終了位置はレコード (j + 1) × R の位置 (なければファイルの末尾)。
 * - 開始キーは開始位置のエントリのキー (先頭の領域ではなし)、終了キーは終了位置のエントリのキー (最後の領域ではなし)。
 * - 副ファイルの開始位置は、キーが開始キーより小さい最後のレコードの位置 (先頭の領域と、そのレコードがなければ 0)。
 *   終了位置は、キーが終了キー以上の最初のレコードの位置 (最後の領域と、そのレコードがなければファイルの末尾)。
 *
 * キーは、呼び出し側の reader で、レコードの位置のエントリを 1 件読んで求める (プローブ)。副ファイルのレコードは
 * 二分探索する。ファイルの大きさはロケータの終端の値を使う。
 * fd は持たない。書き換わる状態は次に配る領域の番号だけなので、allocate() は複数のスレッドから同時に呼べる。
 */
class region_allocator {
public:
    /// プローブの 1 回目の読み込みの大きさ (バイト)。エントリがこれに収まらなければ、必要な分を読み直す
    static constexpr std::size_t probe_size = 4096;

    /**
     * @brief 主ファイルと副ファイル、領域数を決める。
     * @param snapshot_locator スナップショットファイルのロケータ
     * @param compacted_locator コンパクション済み WAL ファイルのロケータ。そのファイルがなければ nullopt
     * @param region_records 領域 1 つのレコード数 (R)。0 なら FATAL ログを書いて abort する
     */
    region_allocator(locator snapshot_locator, std::optional<locator> compacted_locator, std::size_t region_records);

    /**
     * @brief 次の領域を 1 つ選び、その範囲を返す。
     *
     * 渡された reader でプローブする。reader の読む範囲は書き換わるので、呼び出し側は返った範囲を set_range()
     * し直す。
     * @param snapshot_reader スナップショットファイルの reader
     * @param compacted_reader コンパクション済み WAL ファイルの reader。そのファイルがなければ nullptr。ロケータが
     *                         あるのに nullptr なら FATAL ログを書いて abort する
     * @return 領域の範囲。残りがなければ nullopt
     * @throws limestone_exception プローブしたエントリが読めない (ロケータが不正) か、データ系でないとき
     * @throws limestone_io_exception 読み込みに失敗したとき
     */
    [[nodiscard]] std::optional<region_range> allocate(log_entry_reader& snapshot_reader,
                                                       log_entry_reader* compacted_reader);

    /**
     * @brief 領域数を返す。
     */
    [[nodiscard]] std::size_t region_count() const noexcept;

    /**
     * @brief プローブ: 位置を指定してエントリを 1 件読み、その serialized key_sid を返す。
     *
     * 読む範囲を offset から probe_size に設定して読み、エントリがそれに収まらなければ、必要なバイト数の分に
     * 設定し直して読む。システムコールは pread だけ。
     * @param reader 読む reader。読む範囲は書き換わる
     * @param offset エントリの先頭の位置
     * @param file_size ファイルの大きさ (バイト)。ロケータの終端の値
     * @return key_sid のコピー
     * @throws limestone_exception offset がファイルの大きさ以上のとき、エントリがファイルの大きさの中で読み切れない
     *         とき、エントリの種類が不正なとき、データ系でないとき (ERROR ログを書く)
     * @throws limestone_io_exception 読み込みに失敗したとき
     */
    [[nodiscard]] static std::string probe(log_entry_reader& reader, std::uint64_t offset, std::uint64_t file_size);

private:
    /**
     * @brief 領域 j の範囲を計算する。
     */
    [[nodiscard]] region_range compute_range(std::size_t j, log_entry_reader& primary_reader,
                                             log_entry_reader* secondary_reader) const;

    /**
     * @brief 副ファイルのレコードのうち、キーが key 以上の最初のレコードの番号を二分探索で返す。なければレコード数。
     */
    [[nodiscard]] std::size_t lower_bound(log_entry_reader& secondary_reader, const std::string& key) const;

    locator snapshot_locator_;
    std::optional<locator> compacted_locator_;
    std::size_t region_records_;
    /// 主ファイルがスナップショットファイルかどうか
    bool primary_is_snapshot_{true};
    /// 主ファイルのロケータ (snapshot_locator_ か compacted_locator_)
    const locator* primary_{};
    /// 副ファイルのロケータ (primary_ でないほう)。副ファイルがなければ nullptr
    const locator* secondary_{};
    std::size_t region_count_{0};
    /// 次に配る領域の番号
    std::atomic<std::size_t> next_region_{0};
};

}  // namespace limestone::internal
