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
#include <vector>

#include <boost/filesystem.hpp>

#include "file_operations.h"

namespace limestone::internal {

/**
 * @brief 読み込んだロケータファイル。レコードの列とロケーティファイルの大きさを持つ。
 *
 * 構築時にファイル全体を読み、不正でないことを確かめる。不正の条件は次の 5 つである。
 * - ヘッダのマジックが一致しない。
 * - ヘッダの形式バージョンが 1 でない。
 * - ロケータファイルの大きさから 4096 と 16 を引いた値が、0 未満であるか、8 で割り切れない。
 * - 終端のマジックが一致しない。
 * - 終端のロケーティファイルの大きさが、与えられたロケーティファイルの大きさと一致しない。
 */
class locator {
public:
    /**
     * @brief ロケータファイルを読み、不正でないことを確かめる。
     * @param file ロケータファイル
     * @param locatee_size 終端の値と照合するロケーティファイルの大きさ (バイト)
     * @throws limestone_io_exception ファイルを開けないか読めないとき (errno 付き)
     * @throws limestone_exception ロケータファイルが不正なとき
     */
    locator(boost::filesystem::path file, std::uint64_t locatee_size);

    /**
     * @brief ファイル操作を差し替えて、ロケータファイルを読み、不正でないことを確かめる。テスト専用であり、製品コードから
     *        使わない。
     * @param file ロケータファイル
     * @param locatee_size ロケーティファイルの大きさ (バイト)
     * @param file_ops ファイル操作。構築の間だけ使う
     * @throws limestone_io_exception ファイルを開けないか読めないとき (errno 付き)
     * @throws limestone_exception ロケータファイルが不正なとき
     */
    locator(boost::filesystem::path file, std::uint64_t locatee_size, file_operations& file_ops);

    /**
     * @brief レコードの数を返す。
     */
    [[nodiscard]] std::size_t record_count() const noexcept;

    /**
     * @brief k 番目 (0 始まり) のレコードの値 (ロケーティファイルの先頭からのバイト位置) を返す。
     * @param k レコードの番号 (0 始まり)。record_count() 以上なら FATAL ログを書いて abort する
     * @return バイト位置
     */
    [[nodiscard]] std::uint64_t position(std::size_t k) const;

    /**
     * @brief すべてのレコードの値をレコードの番号の順に返す。
     */
    [[nodiscard]] const std::vector<std::uint64_t>& positions() const noexcept;

    /**
     * @brief 終端に記録されたロケーティファイルの大きさを返す。
     */
    [[nodiscard]] std::uint64_t locatee_size() const noexcept;

    /**
     * @brief 読んだロケータファイルを返す。
     */
    [[nodiscard]] const boost::filesystem::path& file() const noexcept;

private:
    /**
     * @brief ロケータファイルを読んで検査し、positions_ と locatee_size_ を設定する。構築子から呼ぶ
     */
    void read(file_operations& file_ops, std::uint64_t locatee_size);

    boost::filesystem::path file_;
    std::vector<std::uint64_t> positions_{};
    std::uint64_t locatee_size_{};
};

}  // namespace limestone::internal
