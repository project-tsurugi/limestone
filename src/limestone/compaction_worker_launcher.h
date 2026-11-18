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

#include <functional>
#include <memory>
#include <mutex>

namespace limestone::internal {

/**
 * @brief オンラインコンパクションのワーカースレッドを 1 回だけ起動する。
 * @details launch() と close() は排他される。close() の後の launch() は何もしない。
 */
class compaction_worker_launcher {
public:
    /**
     * @param start ワーカースレッドを起動する関数。launch() から高々 1 回呼ばれる
     */
    explicit compaction_worker_launcher(std::function<void()> start);

    /**
     * @brief ワーカースレッドを起動する。
     * @details 起動済みのとき、close() の後は何もしない。起動に失敗したときは FATAL のログを書いて abort する。
     * @attention この関数はスレッドセーフである。
     */
    void launch() noexcept;

    /**
     * @brief 以後の launch() を無効にする。
     * @details 戻った時点で、実行中の launch() はない。
     * @attention この関数はスレッドセーフである。
     */
    void close() noexcept;

    /**
     * @brief ワーカースレッドを起動したかを返す。
     * @attention この関数はスレッドセーフである。
     */
    [[nodiscard]] bool launched() noexcept;

private:
    std::mutex mutex_{};
    bool launched_{false};
    bool closed_{false};
    std::function<void()> start_;
};

/**
 * @brief snapshot と、そこから取得した cursor が共有する印。
 * @details 破棄されるときに、ワーカースレッドの起動を求める。
 */
class snapshot_use_marker {
public:
    explicit snapshot_use_marker(std::shared_ptr<compaction_worker_launcher> launcher) noexcept;
    ~snapshot_use_marker();

    snapshot_use_marker(const snapshot_use_marker&) = delete;
    snapshot_use_marker& operator=(const snapshot_use_marker&) = delete;
    snapshot_use_marker(snapshot_use_marker&&) = delete;
    snapshot_use_marker& operator=(snapshot_use_marker&&) = delete;

private:
    std::shared_ptr<compaction_worker_launcher> launcher_;
};

} // namespace limestone::internal
