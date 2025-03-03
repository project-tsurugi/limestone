/*
 * Copyright 2023-2023 Project Tsurugi.
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

 #include <limestone/api/datastore.h>
 #include <limestone/api/backup_detail.h>
 #include "datastore_impl.h"
 
 namespace limestone::api {
 
 // for LOG-0
 epoch_id_type backup_detail::log_finish() const noexcept { 
     return log_finish_; 
 }
 
 // restriction of current implementation:
 // blocks and wait for ready in construct phase; so this object returns true for is_ready
 bool backup_detail::is_ready() const {
     return true;
 }
 
 backup_detail::backup_detail(std::vector<backup_detail::entry>& entries, epoch_id_type log_finish, datastore_impl& ds_impl)
      : log_finish_(log_finish)
      , entries_(std::move(entries))
      , ds_impl_(&ds_impl)
 {
     configuration_id_ = "0";
     ds_impl_->increment_backup_counter();
 }
 
 void backup_detail::notify_end_backup() noexcept {
     bool expected = false;
     if (backup_finished_.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
         ds_impl_->decrement_backup_counter();
     }
 }
 
 } // namespace limestone::api
 