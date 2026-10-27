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
#include "sort/wal_sorter.h"

#include <algorithm>
#include <utility>

#include <glog/logging.h>

#include <limestone/logging.h>

#include "log_entry/log_entry_format.h"
#include "log_entry/log_entry_view.h"
#include "logging_helper.h"

namespace limestone::internal {

using limestone::api::blob_id_type;
using limestone::api::epoch_id_type;
using limestone::api::log_entry;

namespace format = log_entry_format;

wal_sorter::wal_sorter(dblog_scan& scan, const sort_settings& settings, boost::filesystem::path work_dir_parent)
    : scan_(&scan), settings_(settings), work_dir_parent_(std::move(work_dir_parent)) {
    if (settings.threads < 1) {
        LOG_LP(FATAL) << "the number of threads " << settings.threads << " must be at least 1";
    }
}

wal_scan_result wal_sorter::scan(std::optional<epoch_id_type> boundary_epoch, epoch_id_type ld_epoch,
                                 const owned_entry_callback& on_normal_with_blob) {
    if (directory_) {
        LOG_LP(FATAL) << "scan() is called twice";
    }
    directory_ = std::make_unique<sorting_directory>(work_dir_parent_);
    for (int i = 0; i < settings_.threads; ++i) {
        workers_.push_back(std::make_unique<sort_worker>(*directory_, settings_.arena_size_bytes));
    }
    max_blob_ids_.assign(static_cast<std::size_t>(settings_.threads), 0);

    // the hook and add_entry refer to this only during the scan; the hook is unset once the scan returns (also on
    // failure)
    scan_->set_thread_num(settings_.threads);
    scan_->set_thread_end_hook([this](int worker) { workers_[static_cast<std::size_t>(worker)]->finish(); });
    const dblog_scan::add_entry_func_t add = [this, &on_normal_with_blob](log_entry& entry, int worker) {
        add_entry(entry, worker, on_normal_with_blob);
    };
    epoch_id_type max_appeared_epoch{};
    try {
        max_appeared_epoch = boundary_epoch ? scan_->scan_pwal_files_for_compaction(*boundary_epoch, add)
                                            : scan_->scan_pwal_files_throws(ld_epoch, add);
    } catch (...) {
        scan_->set_thread_end_hook({});
        throw;
    }
    scan_->set_thread_end_hook({});

    for (const auto& worker : workers_) {
        const auto& files = worker->sorted_files();
        sorted_files_.insert(sorted_files_.end(), files.begin(), files.end());
    }
    wal_scan_result result;
    result.max_appeared_epoch = max_appeared_epoch;
    result.max_blob_id = *std::max_element(max_blob_ids_.begin(), max_blob_ids_.end());
    result.deletions = deletions_;
    return result;
}

void wal_sorter::merge(const std::optional<sorted_file_merger::compacted_wal_input>& compacted,
                       const entry_callback& output) {
    if (! directory_) {
        LOG_LP(FATAL) << "merge() is called before scan()";
    }
    if (merged_) {
        LOG_LP(FATAL) << "merge() is called twice";
    }
    merged_ = true;
    sorted_file_merger merger(*directory_, settings_.merge_fanin);
    merger.merge(sorted_files_, compacted, deletions_, output);
    directory_->remove();
}

void wal_sorter::add_entry(log_entry& entry, int worker, const owned_entry_callback& on_normal_with_blob) {
    const log_entry_view view = entry.view();
    switch (view.type()) {
    case format::entry_type::normal_with_blob: {
        blob_id_type& max_blob_id = max_blob_ids_[static_cast<std::size_t>(worker)];
        for (const blob_id_type id : view.blob_ids()) {
            max_blob_id = std::max(max_blob_id, id);
        }
        if (on_normal_with_blob) {
            on_normal_with_blob(entry);
        }
        workers_[static_cast<std::size_t>(worker)]->add(view);
        return;
    }
    case format::entry_type::normal_entry:
    case format::entry_type::remove_entry:
        workers_[static_cast<std::size_t>(worker)]->add(view);
        return;
    case format::entry_type::clear_storage:
    case format::entry_type::remove_storage: {  // remove_storage is treated the same as clear_storage
        const auto write_version = view.write_version();
        const std::lock_guard<std::mutex> lock(deletions_mutex_);
        const auto [it, inserted] = deletions_.try_emplace(view.storage(), write_version);
        if (! inserted && it->second < write_version) {
            it->second = write_version;
        }
        return;
    }
    case format::entry_type::add_storage:
        return;  // ignored
    case format::entry_type::this_id_is_not_used:
    case format::entry_type::marker_begin:
    case format::entry_type::marker_end:
    case format::entry_type::marker_durable:
    case format::entry_type::marker_invalidated_begin:
        break;
    }
    LOG_LP(FATAL) << "unexpected entry type " << view.type();
}

}  // namespace limestone::internal
