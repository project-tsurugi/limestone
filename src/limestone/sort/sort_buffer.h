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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <ostream>
#include <string_view>
#include <vector>

#include "log_entry/log_entry_view.h"

namespace limestone::internal {

/**
 * @brief The buffer in which a sort thread accumulates entries. It consists of a fixed-size arena and an index that
 *        holds the position of each entry.
 *
 * The arena is allocated once at the first append(), and is reused, not released, after clear().
 * append() copies the serialized value of an entry into the arena as it is, and adds its position to the index.
 * entry_at() returns the entries as views in the order of the index.
 * Not thread-safe.
 */
class sort_buffer {
public:
    /**
     * @brief The result of append().
     */
    enum class append_result : std::uint8_t {
        /// The entry is put into the arena.
        appended,
        /// The remaining space of the arena is not enough for the entry. The entry is not put.
        full,
        /// The entry is larger than the whole arena. The entry is not put.
        oversized,
    };

    /// the maximum size of the arena in bytes
    static constexpr std::size_t max_arena_size_bytes = std::size_t{1} << 32U;

    /**
     * @brief Creates a buffer with the given arena size. The arena is not allocated yet.
     * @param arena_size_bytes the size of the arena in bytes. Writes a FATAL log and aborts if it is 0 or exceeds
     *                         max_arena_size_bytes
     */
    explicit sort_buffer(std::size_t arena_size_bytes);

    ~sort_buffer() = default;
    sort_buffer(const sort_buffer&) = delete;
    sort_buffer& operator=(const sort_buffer&) = delete;
    sort_buffer(sort_buffer&&) = delete;
    sort_buffer& operator=(sort_buffer&&) = delete;

    /**
     * @brief Copies the serialized value of the entry into the arena, and adds its position to the index.
     *
     * Allocates the arena at the first call.
     * @param entry the entry to put. Writes a FATAL log and aborts if it is not a data entry
     * @return appended: put. full: the remaining space of the arena is not enough. oversized: the entry is larger
     *         than the whole arena. For full and oversized, the buffer is not changed
     */
    [[nodiscard]] append_result append(const log_entry_view& entry);

    /**
     * @brief Returns the size of the arena in bytes.
     */
    [[nodiscard]] std::size_t arena_size_bytes() const noexcept;

    /**
     * @brief Returns the number of entries in the buffer.
     */
    [[nodiscard]] std::size_t entry_count() const noexcept;

    /**
     * @brief Returns true if the buffer holds no entry.
     */
    [[nodiscard]] bool empty() const noexcept;

    /**
     * @brief Returns the i-th entry of the index as a view.
     *
     * At each call, creates the view by parsing the bytes in the arena at the position held by the index (without
     * allocation). The view is not kept.
     * The returned view refers to the arena, and is valid until clear() is called next.
     * @param i the position in the index. Writes a FATAL log and aborts if it is not less than entry_count()
     * @return the view of the entry
     */
    [[nodiscard]] log_entry_view entry_at(std::size_t i) const;

    /**
     * @brief Sorts the index.
     *
     * The order is by the bytes of key_sid (log_entry_format::compare_key_sid()) as the first key, and by
     * write_version in descending order as the second key. The bytes in the arena are not moved. Duplicates are not
     * removed (entries with the same key_sid are visible through entry_at() next to each other, the one with the
     * greatest write_version first).
     */
    void sort();

    /**
     * @brief Empties the index. The arena is not released, and is reused from the next append().
     */
    void clear() noexcept;

private:
    std::size_t arena_size_bytes_;
    /// the arena. arena_size_bytes_ bytes are allocated (not initialized) at the first append()
    std::unique_ptr<char[]> arena_{};  // NOLINT(*-avoid-c-arrays)
    /// the number of bytes used in the arena
    std::size_t used_{};
    /// the position of each entry in the arena (the offset of the first byte of the entry)
    std::vector<std::uint32_t> index_{};
};

/**
 * @brief Returns the name of an append_result.
 */
[[nodiscard]] std::string_view to_string_view(sort_buffer::append_result result) noexcept;

/**
 * @brief Writes the name of an append_result.
 */
std::ostream& operator<<(std::ostream& out, sort_buffer::append_result result);

}  // namespace limestone::internal
