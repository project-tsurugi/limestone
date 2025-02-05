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

namespace limestone::api {

/**
 * @brief The blob_item class represents a BLOB item that holds a blob ID
 *        and, in the future, may include additional metadata (e.g., version).
 *
 * Currently, it only holds the blob_id, but the design allows for future expansion.
 * Copy and move operations are permitted (using the implicitly-defined defaults),
 * while the default constructor is explicitly deleted to enforce providing a blob ID.
 */
class blob_item {
public:
    /// @brief Default constructor is explicitly deleted.
    blob_item() = delete;

    /// @brief Constructs a blob_item with the given blob ID.
    /// @param blob_id The BLOB ID to store.
    explicit blob_item(blob_id_type blob_id);

    /// @brief Returns the blob ID stored in this blob_item.
    /// @return The blob ID.
    blob_id_type get_blob_id() const;

    // Future expansion: Additional accessors for version and other metadata can be added here.

private:
    blob_id_type blob_id_;
};

/**
 * @brief The blob_item_container class manages a collection of blob_item objects.
 *
 * It provides functions for adding blob_items, removing items (diff) and merging
 * contents (merge), as well as iterating over all elements.
 *
 * This design is intentionally optimized for performance and safety:
 *
 *   - The container is NOT thread-safe. However, this is a deliberate decision.
 *     In a multithreaded context, each thread is expected to maintain its own
 *     blob_item_container and merge them later.
 *
 *   - The provided iterator is read-only (const_iterator only). Once an iterator is
 *     obtained via begin(), the container becomes permanently read-only.
 *     This restriction simplifies the iterator implementation, prevents accidental
 *     modifications during iteration, and improves performance.
 *
 *   - The diff method removes from this container all items that are present in
 *     the specified other container.
 *
 *   - The merge method adds the items from the specified containers into this container.
 *
 * These design decisions were made after careful consideration of the requirements,
 * potential future extensions, and optimal performance. This class is intended for
 * internal use within limestone, where the usage patterns and constraints are well understood.
 */
class blob_item_container {
public:
    using container_type = std::vector<blob_item>;
    using iterator = container_type::const_iterator;
    using const_iterator = container_type::const_iterator;

    /// @brief Default constructor.
    blob_item_container() = default;

    /// @brief Default destructor.
    ~blob_item_container() = default;

    // Delete copy constructor and copy assignment operator.
    blob_item_container(const blob_item_container&) = delete;
    blob_item_container& operator=(const blob_item_container&) = delete;

    // Delete move constructor and move assignment operator.
    blob_item_container(blob_item_container&&) = delete;
    blob_item_container& operator=(blob_item_container&&) = delete;

    /**
     * @brief Adds a blob_item to the container.
     *
     * @note Once an iterator has been obtained, the container becomes permanently read-only.
     *       Calling this function after obtaining an iterator will throw an exception.
     *
     * @param item The blob_item to add.
     * @throws std::logic_error if the container is locked for modifications.
     */
    void add_blob_item(const blob_item &item);

    /**
     * @brief Removes from this container all items that are present in the other container.
     *
     * In other words, for each blob_item in this container, if an item with the same blob_id exists in the
     * specified other container, that item is removed. In case of duplicate blob IDs in this container,
     * all occurrences that match an ID in the other container are removed.
     *
     * @param other The container containing items to be removed from this container.
     */
    void diff(const blob_item_container &other);

    /**
     * @brief Merges the contents of the specified blob_item_container objects into this container.
     *
     * This function adds the items from the specified containers to this container and then sorts the result.
     * Note that merge does not remove duplicate blob IDs.
     *
     * @param containers A vector of blob_item_container objects whose items are to be added to this container.
     */
    void merge(const std::vector<blob_item_container> &containers);

    /// @brief Returns an iterator to the beginning of the container.
    /// @return A const iterator to the first element.
    const_iterator begin() const;

    /// @brief Returns an iterator to the end of the container.
    /// @return A const iterator to one past the last element.
    const_iterator end() const;

private:
    bool iterator_used_ = false;
    container_type items_;

    /// @brief Helper function to sort the items.
    void sort();
};

} // namespace limestone::api
