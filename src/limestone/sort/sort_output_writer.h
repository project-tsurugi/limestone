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

#include <boost/filesystem.hpp>

#include "locator/locator_writer.h"
#include "log_entry/log_entry.h"
#include "log_entry/log_entry_format.h"
#include "log_entry/log_entry_view.h"
#include "sort/buffered_output_file.h"

namespace limestone::internal {

/**
 * @brief Writes the output of the sort (the snapshot file or the compacted WAL file).
 *
 * The output begins with a marker_begin of epoch 0, followed by data entries. The result of the final merge is passed
 * to write() entry by entry (the order is not checked and duplicates are not removed). How the write_version is
 * written, whether remove_entries are written, and whether fsync is performed at close are specified by format.
 *
 * ロケータファイルの出力を指定すると、書き出しに合わせてロケータファイルも生成する。書いたデータ系のエントリの開始位置
 * (marker_begin を含む、それまでに書いたバイト数) を locator_writer に渡し、close() でロケーティファイルを閉じた後に
 * ロケータファイルの終端を書いて fsync して閉じる。
 *
 * Not thread-safe (used from one thread).
 */
class sort_output_writer {
public:
    /// the format of the output
    struct format {
        /// how the write_version is written
        limestone::api::log_entry::write_version_handling write_version;
        /// whether remove_entries are written. If false, the remove_entries passed to write() are not written
        bool keep_remove_entry;
        /// whether close() performs fsync
        bool sync_on_close;
    };

    /// ロケータファイルの出力
    struct locator_output {
        /// ロケータファイル。存在すれば切り詰める
        boost::filesystem::path file;
        /// 記録間隔 (バイト)
        std::uint64_t record_interval_bytes;
    };

    /**
     * @brief Creates and opens the file, and writes a marker_begin of epoch 0.
     *
     * ロケータファイルの出力を指定すると、ロケータファイルも作ってヘッダを書く。
     * @param file the file to write. If it exists, it is truncated
     * @param label the name of the kind of the file used in error messages (for example "snapshot file")
     * @param output_format the format of the output
     * @param locator ロケータファイルの出力。std::nullopt ならロケータファイルを生成しない。記録間隔が 0 なら FATAL
     *                ログを書いて abort する
     * @throws limestone_io_exception if the file cannot be opened or the marker_begin cannot be written (with errno).
     *         ロケータファイルを作れないか、そのヘッダを書けないときも同様
     */
    sort_output_writer(boost::filesystem::path file, std::string label, const format& output_format,
                       const std::optional<locator_output>& locator);

    /**
     * @brief Closes the file if close() has not been called (a failure is ignored).
     */
    ~sort_output_writer() = default;

    sort_output_writer(const sort_output_writer&) = delete;
    sort_output_writer& operator=(const sort_output_writer&) = delete;
    sort_output_writer(sort_output_writer&&) = delete;
    sort_output_writer& operator=(sort_output_writer&&) = delete;

    /**
     * @brief Writes the entry according to format.
     *
     * ロケータファイルを出力するときは、エントリを書く前に、その開始位置 (それまでに書いたバイト数) をロケータファイルに
     * 記録する。format により書かない remove_entry は記録しない。
     * @param entry a data entry. Writes a FATAL log and aborts if it is not a data entry. Also writes a FATAL log and
     *              aborts if called after close()
     * @throws limestone_io_exception if writing fails (with errno)
     */
    void write(const log_entry_view& entry);

    /**
     * @brief Finishes writing and closes the file (fflush, fsync if sync_on_close of format, fclose).
     *
     * The file is closed even on failure. If called after the file is closed, writes a WARNING log and does nothing.
     * However, with sync_on_close, if the previous close() failed and the contents are not fsynced, writes a FATAL
     * log and aborts.
     *
     * ロケータファイルを出力するときは、ファイルを閉じた後に、書いたバイト数を終端に書いてロケータファイルを fsync して
     * 閉じる。ファイルを閉じるのに失敗したときはロケータファイルを閉じず、2 回目以降の呼び出しではロケータファイルに
     * 何もしない。このとき、ロケータファイルは不完全なまま残る (呼び出し側が削除する)。
     * @throws limestone_io_exception if fflush, fsync or fclose fails (with errno). ロケータファイルの終端を書けないか、
     *         閉じられないときも同様
     */
    void close();

private:
    buffered_output_file file_;
    format format_;
    // ロケータファイルの書き出し。出力を指定されたときだけ持つ
    std::optional<locator_writer> locator_{};
    // これまでに書いたバイト数 (marker_begin を含む)。次に書くエントリの開始位置
    std::uint64_t written_bytes_{log_entry_format::marker_size};
};

}  // namespace limestone::internal
