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

#include <optional>
#include <string>

#include "log_entry/log_entry_view.h"

namespace limestone::internal {

/**
 * @brief Removes duplicates: from a stream of entries ordered by key_sid, drops the entries whose key_sid is the same
 *        as that of the entry accepted last.
 *
 * The stream must be in ascending order of key_sid (log_entry_format::compare_key_sid()). The first of the entries
 * with the same key_sid is accepted, so in a stream ordered by write_version in descending order, the entry with the
 * greatest write_version remains.
 */
class key_sid_deduplicator {
public:
    /**
     * @brief Returns whether to accept the entry.
     *
     * Accepts (true) if the key_sid differs from that of the entry accepted last, and drops (false) if it is the
     * same. The first entry is always accepted.
     * @param entry a data entry. Writes a FATAL log and aborts if it is not a data entry, or if its key_sid is
     *              smaller than that of the entry accepted last (a violation of the ordering contract)
     * @return true to accept
     */
    [[nodiscard]] bool accept(const log_entry_view& entry);

private:
    /// a copy of the key_sid of the entry accepted last, or nullopt if none has been accepted yet
    std::optional<std::string> last_key_sid_{};
};

}  // namespace limestone::internal
