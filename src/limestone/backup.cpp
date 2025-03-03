/*
 * Copyright 2022-2023 Project Tsurugi.
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
#include <limestone/api/backup.h>
#include "datastore_impl.h"

namespace limestone::api {

backup::backup(const std::set<boost::filesystem::path>& files, datastore_impl& ds_impl) noexcept
    : ds_impl_(&ds_impl), backup_finished_(false)
{
    for(auto& e : files) {
        files_.emplace_back(e);
    }
    ds_impl_->increment_backup_counter();
}

backup::~backup() noexcept = default;

bool backup::is_ready() const noexcept {
    return true;  // FIXME
}

bool backup::wait_for_ready([[maybe_unused]] std::size_t duration) const noexcept {
    return true;  // FIXME
}

std::vector<boost::filesystem::path>& backup::files() noexcept {
    return files_;
}

void backup::notify_end_backup() noexcept {
    bool expected = false;
    if (backup_finished_.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
        ds_impl_->decrement_backup_counter();
    }
}

} // namespace limestone::api
