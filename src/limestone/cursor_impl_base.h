/*
 * Copyright 2022-2025 Project Tsurugi.
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
#include <vector>
#include <limestone/api/storage_id_type.h>
#include <limestone/api/blob_id_type.h>
#include "log_entry.h"

namespace limestone::internal {

class cursor_impl_base {
public:
    cursor_impl_base() = default;
    virtual ~cursor_impl_base() = default;

    cursor_impl_base(const cursor_impl_base&) = delete;
    cursor_impl_base& operator=(const cursor_impl_base&) = delete;
    cursor_impl_base(cursor_impl_base&&) = delete;
    cursor_impl_base& operator=(cursor_impl_base&&) = delete;

    virtual bool next() = 0;
    [[nodiscard]] virtual api::storage_id_type storage() const noexcept = 0;
    virtual void key(std::string& buf) const noexcept = 0;
    virtual void value(std::string& buf) const noexcept = 0;
    [[nodiscard]] virtual api::log_entry::entry_type type() const = 0;
    [[nodiscard]] virtual std::vector<api::blob_id_type> blob_ids() const = 0;
    virtual void close() = 0;
};

} // namespace limestone::internal
