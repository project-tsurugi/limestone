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

using limestone::api::log_entry;
class cursor_impl_base {
public:
    cursor_impl_base() = default;
    virtual ~cursor_impl_base() = default;

    cursor_impl_base(const cursor_impl_base&) = delete;
    cursor_impl_base& operator=(const cursor_impl_base&) = delete;
    cursor_impl_base(cursor_impl_base&&) = delete;
    cursor_impl_base& operator=(cursor_impl_base&&) = delete;

    /**
     * @brief Advances the cursor to the next entry.
     * @return true if a valid entry was found, false if the end has been reached.
     */
    virtual bool next() = 0;

    /**
     * @brief Returns the storage ID of the current entry.
     * @return the storage ID.
     * @note Only valid after a successful call to `next()`.
     */
    [[nodiscard]] virtual api::storage_id_type storage() const noexcept = 0;

    /**
     * @brief Retrieves the key of the current entry.
     * @param[out] buf the buffer to store the key.
     * @note Only valid after a successful call to `next()`.
     */
    virtual void key(std::string& buf) const noexcept = 0;

    /**
     * @brief Retrieves the value of the current entry.
     * @param[out] buf the buffer to store the value.
     * @note Only valid after a successful call to `next()`.
     */
    virtual void value(std::string& buf) const noexcept = 0;

    /**
     * @brief Returns the type of the current entry.
     * @return the entry type.
     * @note Only valid after a successful call to `next()`.
     */
    [[nodiscard]] virtual api::log_entry::entry_type type() const = 0;

    /**
     * @brief Returns the list of blob IDs associated with the current entry.
     * @return a vector of blob IDs.
     * @note Only valid after a successful call to `next()`.
     */
    [[nodiscard]] virtual std::vector<api::blob_id_type> blob_ids() const = 0;

    /**
     * @brief Returns the current entry.
     * @return the current log_entry, typically moved.
     * @details Only valid after a successful call to `next()`.
     *          The returned object is intended to be moved.
     *          After calling this method, the cursor may enter an unspecified internal state,
     *          and calling any method other than `next()` may result in undefined behavior.
     */
    [[nodiscard]] virtual log_entry& current() = 0;


    /**
     * @brief Closes the cursor and releases any held resources.
     * @details After calling this method, no further operations on the cursor are valid.
     */
    virtual void close() = 0;
};


} // namespace limestone::internal
