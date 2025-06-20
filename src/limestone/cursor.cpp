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
#include <limestone/api/cursor.h>

#include <glog/logging.h>
#include <limestone/logging.h>
#include "logging_helper.h"
#include "limestone_exception_helper.h"
#include "log_entry.h"
#include "cursor_impl.h"


namespace limestone::api {


cursor::cursor(const boost::filesystem::path& snapshot_file)
    : pimpl(std::make_unique<limestone::internal::cursor_impl>(snapshot_file)) {}

cursor::cursor(const boost::filesystem::path& snapshot_file, const boost::filesystem::path& compacted_file)
    : pimpl(std::make_unique<limestone::internal::cursor_impl>(snapshot_file, compacted_file)) {}

cursor::cursor(const boost::filesystem::path& snapshot_file, const boost::filesystem::path& compacted_file, chunk_offset_t offset)
    : pimpl(std::make_unique<limestone::internal::cursor_impl>(snapshot_file, compacted_file, offset)) {}

cursor::~cursor() noexcept {
    // TODO: handle close failure
    pimpl->close();
}

bool cursor::next() {
    try {
        return pimpl->next();
    } catch (...) {
        HANDLE_EXCEPTION_AND_ABORT();
        throw; // Unreachable, but required to satisfy the compiler
    }
}

storage_id_type cursor::storage() const noexcept {
    return pimpl->storage();
}

void cursor::key(std::string& buf) const noexcept {
    pimpl->key(buf);
}

void cursor::value(std::string& buf) const noexcept {
    pimpl->value(buf);
}

} // namespace limestone::api
