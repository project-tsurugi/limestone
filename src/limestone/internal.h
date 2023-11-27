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

#include <optional>

#include <limestone/api/datastore.h>

namespace limestone::internal {
using namespace limestone::api;

// return max epoch in file.
std::optional<epoch_id_type> last_durable_epoch(const boost::filesystem::path& file);

epoch_id_type scan_one_pwal_file(const boost::filesystem::path& pwal, epoch_id_type ld_epoch, const std::function<void(log_entry&)>& add_entry);

inline constexpr const std::string_view manifest_file_name = "limestone-manifest.json";

void setup_initial_logdir(const boost::filesystem::path& logdir);

void check_logdir_format(const boost::filesystem::path& logdir);

}
