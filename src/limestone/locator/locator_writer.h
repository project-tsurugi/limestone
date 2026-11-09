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

#include <boost/filesystem.hpp>

#include "sort/buffered_output_file.h"

namespace limestone::internal {

/**
 * @brief ロケータファイルを書き出す。
 *
 * 構築時にファイルを作ってヘッダを書き、ロケーティファイルの書き出しに合わせて note_entry() でデータ系のエントリの
 * 開始位置を受け取り、記録間隔の規則に従ってレコードを書く。close() で終端を書き、fsync して閉じる。
 * close() せずに破棄すると、書きかけのファイルが残る (呼び出し側が削除する)。
 *
 * スレッドセーフではない (1 つのスレッドから使う)。
 */
class locator_writer {
public:
    /**
     * @brief ロケータファイルを作り、ヘッダを書く。
     * @param file ロケータファイル。存在すれば切り詰める
     * @param record_interval_bytes 記録間隔 (バイト)。0 なら FATAL ログを書いて abort する
     * @throws limestone_io_exception ファイルを作れないか、ヘッダを書けないとき (errno 付き)
     */
    locator_writer(boost::filesystem::path file, std::uint64_t record_interval_bytes);

    /**
     * @brief close() されていなければファイルを閉じる (失敗は無視する)。
     */
    ~locator_writer() = default;

    locator_writer(const locator_writer&) = delete;
    locator_writer& operator=(const locator_writer&) = delete;
    locator_writer(locator_writer&&) = delete;
    locator_writer& operator=(locator_writer&&) = delete;

    /**
     * @brief データ系のエントリの開始位置を受け取り、記録の要否を判定してレコードを書く。
     *
     * 次に記録する閾値を持ち、offset が閾値以上なら記録して、閾値を (offset ÷ 記録間隔 + 1) × 記録間隔に進める。
     * 初期の閾値は 0 であり、最初に受け取った位置がレコード 0 になる。1 つのエントリで複数の倍数を越えても記録は 1 つになる。
     * @param offset エントリのロケーティファイルの先頭からのバイト位置。前回の位置より小さければ FATAL ログを書いて
     *               abort する。close() の後に呼んでも同様
     * @throws limestone_io_exception レコードを書けないとき (errno 付き)
     */
    void note_entry(std::uint64_t offset);

    /**
     * @brief 終端を書き、fsync してファイルを閉じる。close() の後に呼ぶと FATAL ログを書いて abort する。
     * @param locatee_size ロケーティファイルの大きさ (バイト)。最後に note_entry() に渡した位置より小さければ FATAL
     *                     ログを書いて abort する
     * @throws limestone_io_exception 終端を書けないか、fflush、fsync、fclose に失敗したとき (errno 付き)
     */
    void close(std::uint64_t locatee_size);

    /**
     * @brief これまでに書いたレコードの数を返す。
     */
    [[nodiscard]] std::size_t record_count() const noexcept;

    /**
     * @brief 既存のロケーティファイルを先頭から読み、そのロケータファイルを生成する。
     *
     * データ系のエントリの開始位置を note_entry() に渡し、末尾に達したら close() する。書き出し時に生成するのと同じ内容になる。
     * @param locatee ロケーティファイル。先頭から末尾までエントリが途切れなく並んでいること
     * @param file 生成するロケータファイル。存在すれば切り詰める
     * @param record_interval_bytes 記録間隔 (バイト)
     * @return 書いたレコードの数
     * @throws limestone_io_exception ロケーティファイルを読めないとき、ロケーティファイルの内容が形式に合わないとき、
     *         ロケータファイルを書けないとき。このとき書きかけのロケータファイルが残る (呼び出し側が削除する)
     */
    static std::size_t build_from_file(const boost::filesystem::path& locatee, const boost::filesystem::path& file,
                                       std::uint64_t record_interval_bytes);

private:
    buffered_output_file file_;
    std::uint64_t record_interval_bytes_;
    // 次にレコードを記録する位置の閾値
    std::uint64_t next_threshold_{};
    // 前回 note_entry() に渡された位置。位置が戻っていないことの検査に使う
    std::uint64_t last_offset_{};
    std::size_t record_count_{};
};

}  // namespace limestone::internal
