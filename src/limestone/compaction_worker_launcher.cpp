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
#include "compaction_worker_launcher.h"

#include <exception>
#include <utility>

#include <glog/logging.h>
#include <limestone/logging.h>

#include "logging_helper.h"

namespace limestone::internal {

compaction_worker_launcher::compaction_worker_launcher(std::function<void()> start) : start_(std::move(start)) {}

void compaction_worker_launcher::launch() noexcept {
    {
        std::scoped_lock<std::mutex> lock(mutex_);
        if (launched_ || closed_) {
            return;
        }
        try {
            start_();
        } catch (const std::exception& e) {
            // 呼び出し元は snapshot や cursor のデストラクタであり、失敗に対処する手段がない
            LOG_LP(FATAL) << "failed to start the online compaction worker: " << e.what();
        }
        launched_ = true;
    }
}

void compaction_worker_launcher::close() noexcept {
    {
        std::scoped_lock<std::mutex> lock(mutex_);
        closed_ = true;
    }
}

bool compaction_worker_launcher::launched() noexcept {
    {
        std::scoped_lock<std::mutex> lock(mutex_);
        return launched_;
    }
}

snapshot_use_marker::snapshot_use_marker(std::shared_ptr<compaction_worker_launcher> launcher) noexcept
    : launcher_(std::move(launcher)) {}

snapshot_use_marker::~snapshot_use_marker() {
    launcher_->launch();
}

} // namespace limestone::internal
