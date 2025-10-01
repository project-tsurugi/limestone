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

#include "blob_id_type.h"
#include <boost/filesystem.hpp>

namespace limestone::api {

/// @brief BLOB reference type.
using blob_id_type = std::uint64_t;

/// @brief BLOB reference tag type.
using blob_reference_tag_type = std::uint64_t;

/**
 * @brief represents a pool for provisional registration of BLOB data.
 */
class blob_pool {
public:

    /**
     * @brief creates a new object.
     */
    blob_pool() = default;

    /**
     * @brief destroys this object.
     */
    virtual ~blob_pool() = default;

    blob_pool(blob_pool const&) = delete;
    blob_pool(blob_pool&&) = delete;
    blob_pool& operator=(blob_pool const&) = delete;
    blob_pool& operator=(blob_pool&&) = delete;

    /**
     * @brief Discards all BLOB data provisionally registered in this pool, except for those that have already been persistent.
     * @note After this operation, this pool will be unusable.
     * @note This operation is idempotent.
     * @attention Undefined behavior if attempting to access the data of non-persistent BLOBs in this pool after this operation.
     *     It depends on the implementation when the BLOB data is actually removed.
     */
    virtual void release() = 0;

    /**
     * @brief registers a BLOB file provisionally into this BLOB pool.
     * @param is_temporary_file true to allow remove the source file, or false to copy the source file
     * @return the corresponding BLOB reference
     * @attention This only act as provisional registration for the BLOB, and it may be lost after release() was called.
     *     To avoid it, you need to pass the BLOB references to log_channel::add_entry() to persistent them.
     * @throws std::logic_error if this pool is already released
     * @throws limestone_blob_exception if an I/O error occurs during the operation
     */
    [[nodiscard]] virtual blob_id_type register_file(
            boost::filesystem::path const& file,
            bool is_temporary_file) = 0;

    /**
     * @brief registers a BLOB data provisionally into this BLOB pool.
     * @param data the target BLOB data
     * @return the corresponding BLOB reference
     * @attention This only act as provisional registration for the BLOB, and it may be lost after release() was called.
     *     To avoid it, you need to pass the BLOB references to log_channel::add_entry() to persistent them.
     * @throws std::logic_error if this pool is already released
     * @throws limestone_blob_exception if an I/O error occurs during the operation
     */
    [[nodiscard]] virtual blob_id_type register_data(std::string_view data) = 0;

    /**
     * @brief duplicates the registered BLOB data, and registers the copy provisionally into this BLOB pool.
     * @param reference the source BLOB reference
     * @return the corresponding BLOB reference of the duplicated one
     * @attention This only act as provisional registration for the BLOB, and it may be lost after release() was called.
     *     To avoid it, you need to pass the BLOB references to log_channel::add_entry() to persistent them.
     * @throws std::logic_error if this pool is already released
     * @throws limestone_blob_exception if an I/O error occurs during the operation
     */
    [[nodiscard]] virtual blob_id_type duplicate_data(blob_id_type reference) = 0;

    /**
     * @brief generates a BLOB reference tag for access control.
     * @param blob_id the BLOB reference
     * @param transaction_id the transaction ID
     * @return the generated BLOB reference tag
     * @throws limestone_blob_exception if an internal error occurs during tag generation.
     *         Possible reasons include:
     *         - Environment issues (e.g., cryptographic library not initialized or misconfigured)
     *         - Resource exhaustion (e.g., out of memory)
     *         - Other unexpected internal errors
     * @note No validation is performed for blob_id or transaction_id values; any value is accepted.
     */
    [[nodiscard]] virtual blob_reference_tag_type generate_reference_tag(
            blob_id_type blob_id,
            std::uint64_t transaction_id) = 0;
};

} // namespace limestone::api
