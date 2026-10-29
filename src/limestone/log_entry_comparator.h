/*
 * Copyright 2022-2024 Project Tsurugi.
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

#include "blob_gc_entry.h"
#include "log_entry/log_entry_format.h"

namespace limestone::internal {

using namespace limestone::api;

/**
 * @brief A comparator that orders blob_gc_entry in ascending order of the pair (key_sid, write_version).
 *
 * Compares the serialized key_sids in the order of log_entry_format::compare_key_sid(), and if they are equal,
 * compares the write_versions in ascending order.
 */
class log_entry_comparator {
public:
    /**
     * @brief Returns whether a comes before b.
     * @param a an entry to compare
     * @param b an entry to compare
     * @return true if a comes before b
     */
    bool operator()(const blob_gc_entry& a, const blob_gc_entry& b) const {
        const int c = log_entry_format::compare_key_sid(a.serialized_key_sid(), b.serialized_key_sid());
        if (c != 0) {
            return c < 0;
        }
        return a.write_version() < b.write_version();
    }
};

}  // namespace limestone::internal
