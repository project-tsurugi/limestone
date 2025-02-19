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

#include <cstdint>
#include <vector>
#include <memory>
#include <algorithm>
#include <stdexcept>
#include "limestone/api/blob_id_type.h"

namespace limestone::internal {

using limestone::api::blob_id_type;    

/**
 * @brief The blob_id_container class manages a collection of blob_id_type values.
 *
 * It provides functions for adding blob IDs, removing blob IDs (diff) and merging
 * contents (merge), as well as iterating over all elements.
 *
 * Once an iterator is obtained, the container becomes read-only.
 */
class blob_id_container {
public:
    using container_type = std::vector<blob_id_type>;
    using iterator = container_type::const_iterator;
    using const_iterator = container_type::const_iterator;

    blob_id_container() = default;
    ~blob_id_container() = default;

    blob_id_container(const blob_id_container&) = delete;
    blob_id_container& operator=(const blob_id_container&) = delete;
    blob_id_container(blob_id_container&&) = delete;
    blob_id_container& operator=(blob_id_container&&) = delete;

    /**
     * @brief Adds a blob_id to the container.
     *
     * @note Once an iterator is obtained, the container becomes permanently read-only.
     *
     * @param id The blob_id to add.
     * @throws std::logic_error if the container is locked for modifications.
     */
    void add_blob_id(blob_id_type id);

    /**
     * @brief Removes from this container all blob_ids that are present in the other container.
     *
     * Duplicate blob_ids are eliminated.
     *
     * @param other The container containing blob_ids to be removed.
     * @throws std::logic_error if the container is locked for modifications.
     */
    void diff(const blob_id_container &other);

    /**
     * @brief Merges the contents of the specified container into this container.
     *
     * @param other The container whose blob_ids are to be added.
     * @throws std::logic_error if the container is locked for modifications.
     */
    void merge(const blob_id_container &other);

    [[nodiscard]] const_iterator begin() const;
    [[nodiscard]] const_iterator end() const;

    // Returns a string representation of the blob IDs for debugging.
    [[nodiscard]] std::string debug_string() const;

private:
    bool iterator_used_ = false;
    container_type ids_;

    /// @brief Helper function to sort the blob IDs.
    void sort();
};

} // namespace limestone::internal
