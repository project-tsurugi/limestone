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

#include <chrono>
#include <cstdint>

namespace limestone::internal {

/// コンパクションと BLOB ファイルの GC の設定。
struct compaction_settings {
    /// 常時コンパクションを実行する WAL の量の閾値 (バイト)。0 のときは常時コンパクションを行わない
    std::uint64_t threshold_bytes{0};
    /// 常時コンパクションで、detached の WAL ファイルを削除するか
    bool delete_detached{true};
    /// BLOB ファイルの GC を行うか。起動時の GC と、オンラインコンパクションでの GC の両方に効く。コンパクションの契機と
    /// 役割 (master、レプリカ) によらない
    bool blob_gc{true};
    /// WAL の量を測定する間隔。プロダクトコードでは既定値のまま使い、テストだけが変える
    std::chrono::milliseconds measurement_interval{std::chrono::seconds(10)};
};

}  // namespace limestone::internal
