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
#include "limestone/api/blob_id_type.h"

namespace limestone::api {

/**
 * @brief The blob_item class represents a BLOB item that holds a blob ID
 *        and, in the future, may include additional metadata (e.g., version).
 *
 * Currently, it only holds the blob_id, but the design allows for future expansion.
 * Copy and move operations are disabled to prevent unintended copying.
 */
class blob_item {
public:
    /// @brief Constructs a blob_item with the given blob ID.
    /// @param blob_id The BLOB ID to store.
    explicit blob_item(blob_id_type blob_id);

    // Delete copy constructor and copy assignment operator.
    blob_item(const blob_item&) = delete;
    blob_item& operator=(const blob_item&) = delete;

    // Delete move constructor and move assignment operator.
    blob_item(blob_item&&) = delete;
    blob_item& operator=(blob_item&&) = delete;

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
 * It provides functions for adding blob_items, computing the difference between
 * two containers, merging multiple containers, and iterating over all elements.
 *
 * This design is intentionally optimized for performance and safety:
 *
 *   - The container is NOT thread-safe. However, this is a deliberate decision.
 *     In a multithreaded context, each thread is expected to maintain its own
 *     blob_item_container and merge them later.
 *
 *   - The provided iterator is read-only (const_iterator only). Once an iterator is
 *     obtained via begin(), the container becomes permanently read-only. This
 *     restriction simplifies the iterator implementation, prevents accidental
 *     modifications during iteration, and improves performance.
 *
 * These design decisions were made after careful consideration of the requirements,
 * potential future extensions, and optimal performance. This class is intended for
 * internal use within limestone, where the usage patterns and constraints are well understood.
 */
class blob_item_container {
public:
    using container_type = std::vector<blob_item>;
    using iterator = container_type::iterator;
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

    /// @brief Adds a blob_item to the container.
    /// @param item The blob_item to add.
    void add_blob_item(const blob_item &item);

    /**
     * @brief Creates a new blob_item_container that contains the difference
     *        between this container and the other container.
     *
     * The returned container will include items that are present in this container
     * but not in the other container. Internal sorting and duplicate removal are
     * performed as necessary.
     *
     * @param other The container to compare against.
     * @return A new blob_item_container containing the difference.
     */
    blob_item_container diff(const blob_item_container &other) const;

    /**
     * @brief Merges multiple blob_item_container objects into a single container.
     *
     * This function is useful in a multithreaded context where each thread produces
     * its own blob_item_container and these need to be merged into one.
     * Duplicate removal and internal consistency (e.g., sorting) are handled during the merge.
     *
     * @param containers A vector of blob_item_container objects to merge.
     * @return A new blob_item_container resulting from the merge.
     */
    static blob_item_container merge(const std::vector<blob_item_container> &containers);

    /// @brief Returns an iterator to the beginning of the container.
    /// @return An iterator to the first element.
    iterator begin();

    /// @brief Returns a const iterator to the beginning of the container.
    /// @return A const iterator to the first element.
    const_iterator begin() const;

    /// @brief Returns an iterator to the end of the container.
    /// @return An iterator to one past the last element.
    iterator end();

    /// @brief Returns a const iterator to the end of the container.
    /// @return A const iterator to one past the last element.
    const_iterator end() const;

private:
    container_type items_;

    /// @brief Helper function to sort the items and remove duplicates.
    void sort_and_deduplicate();
};

} // namespace limestone::api
