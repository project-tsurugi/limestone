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

#include <limestone/api/blob_id_type.h>
#include <limestone/api/storage_id_type.h>
#include <log_entry.h>

#include <memory>
#include <optional>
#include <string>
#include <variant>
#include <vector>

#include "cursor_entry_queue.h"
#include "cursor_impl_base.h"
#include "end_marker.h"

namespace limestone::internal {

class partitioned_cursor : public cursor_impl_base {
public:
    explicit partitioned_cursor(std::shared_ptr<cursor_entry_queue> queue);
    ~partitioned_cursor() override = default;

    partitioned_cursor(const partitioned_cursor&) = delete;
    partitioned_cursor& operator=(const partitioned_cursor&) = delete;
    partitioned_cursor(partitioned_cursor&&) = delete;
    partitioned_cursor& operator=(partitioned_cursor&&) = delete;

    bool next() override;
    [[nodiscard]] api::storage_id_type storage() const noexcept override;
    void key(std::string& buf) const noexcept override;
    void value(std::string& buf) const noexcept override;
    [[nodiscard]] api::log_entry::entry_type type() const override;
    [[nodiscard]] std::vector<api::blob_id_type> blob_ids() const override;
    [[nodiscard]] const log_entry& current() const override;
    void close() override;

private:
    std::shared_ptr<cursor_entry_queue> queue_;
    cursor_entry_type current_{};
};

} // namespace limestone::internal
