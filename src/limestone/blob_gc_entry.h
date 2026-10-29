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

#include <string>
#include <string_view>
#include <vector>

#include <limestone/api/blob_id_type.h>
#include <limestone/api/write_version_type.h>

#include "log_entry/log_entry_view.h"

namespace limestone::internal {

/**
 * @brief An entry for the BLOB GC: an owning type that holds only the key_sid, the write_version and the blob_ids of
 *        a normal_with_blob.
 *
 * Used by the BLOB GC to keep only the values needed for its decisions, taken from a normal_with_blob read by a
 * scan.
 */
class blob_gc_entry {
public:
    using blob_id_type = limestone::api::blob_id_type;
    using write_version_type = limestone::api::write_version_type;

    /**
     * @brief Creates an entry from a view of a normal_with_blob.
     * @param entry a view of a normal_with_blob. Writes a FATAL log and aborts if it is of another type
     */
    explicit blob_gc_entry(const log_entry_view& entry);

    /**
     * @brief Returns the serialized key_sid.
     * @return the serialized key_sid, valid as long as this object is valid
     */
    [[nodiscard]] std::string_view serialized_key_sid() const noexcept;

    /**
     * @brief Returns the write_version.
     * @return the write_version
     */
    [[nodiscard]] write_version_type write_version() const noexcept;

    /**
     * @brief Returns the blob_ids.
     * @return the blob_ids
     */
    [[nodiscard]] const std::vector<blob_id_type>& blob_ids() const noexcept;

private:
    std::string key_sid_{};
    write_version_type write_version_{};
    std::vector<blob_id_type> blob_ids_{};
};

}  // namespace limestone::internal
