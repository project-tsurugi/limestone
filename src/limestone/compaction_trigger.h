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
#include <ostream>
#include <string_view>

namespace limestone::internal {

/**
 * @brief オンラインコンパクションを実行する契機。
 */
enum class compaction_trigger : std::uint8_t {
    /// ctrl ファイルによる指示
    ctrl_file,
    /// WAL の量が閾値を超えたこと (常時コンパクション)
    threshold,
};

/**
 * @brief 契機の名前を返す。
 * @param trigger 契機
 * @return ログに書く名前
 */
[[nodiscard]] std::string_view to_string_view(compaction_trigger trigger) noexcept;

/**
 * @brief 契機の名前をストリームに書く。
 */
std::ostream& operator<<(std::ostream& out, compaction_trigger trigger);

} // namespace limestone::internal
