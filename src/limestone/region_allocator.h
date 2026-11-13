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

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>

#include "locator/locator.h"
#include "log_entry/log_entry_reader.h"

namespace limestone::internal {

/**
 * @brief The range of one region. Returned by region_allocator::allocate().
 *
 * The positions are byte positions from the beginning of the file: a start position is the beginning of the entry to
 * start reading at, and an end position is the entry boundary (the beginning of the next entry or the end of the
 * file) to stop reading at. The keys are serialized key_sids: entries below the start key are skipped, and reading
 * stops at an entry at or above the end key.
 */
struct region_range {
    /// the position to start reading the snapshot file at
    std::uint64_t snapshot_start{};
    /// the position to stop reading the snapshot file at
    std::uint64_t snapshot_end{};
    /// the position to start reading the compacted WAL file at. 0 if there is no such file
    std::uint64_t compacted_start{};
    /// the position to stop reading the compacted WAL file at. 0 if there is no such file
    std::uint64_t compacted_end{};
    /// the start key. None for the first region
    std::optional<std::string> start_key{};
    /// the end key. None for the last region
    std::optional<std::string> end_key{};
};

/**
 * @brief The region allocator. The n cursors that share the reading for the index reflection share one allocator,
 *        which hands out the unprocessed regions one by one.
 *
 * Takes the locators of the snapshot file and the compacted WAL file and R (the number of records in one region),
 * and makes the file with more records the primary file and the other the secondary file (the compacted WAL file on
 * a tie; if there is no compacted WAL file, the snapshot file is the primary and there is no secondary). The number
 * of regions is ceil(the number of records of the primary / R); there is no region if the primary has no record.
 *
 * The range of region j is as follows.
 * - The start position in the primary file is the position of record j * R, and the end position is the position of
 *   record (j + 1) * R (or the end of the file if there is no such record).
 * - The start key is the key of the entry at the start position (none for the first region), and the end key is the
 *   key of the entry at the end position (none for the last region).
 * - The start position in the secondary file is the position of the last record whose key is below the start key
 *   (0 for the first region, or if there is no such record). The end position is the position of the first record
 *   whose key is at or above the end key (the end of the file for the last region, or if there is no such record).
 *
 * A key is obtained by reading one entry at the position of a record with a reader of the caller (a probe). The
 * records of the secondary file are searched by bisection. The size of a file is the value recorded at the end of
 * the locator.
 * Holds no file descriptor. The only state that changes is the number of the next region to hand out, so allocate()
 * can be called from several threads at the same time.
 */
class region_allocator {
public:
    /// The size of the first read of a probe (bytes). If the entry does not fit in it, what the entry needs is read
    /// again
    static constexpr std::size_t probe_size = 4096;

    /**
     * @brief Decides the primary file, the secondary file and the number of regions.
     * @param snapshot_locator the locator of the snapshot file
     * @param compacted_locator the locator of the compacted WAL file, or nullopt if there is no such file
     * @param region_records the number of records in one region (R). Writes a FATAL log and aborts if it is 0
     */
    region_allocator(locator snapshot_locator, std::optional<locator> compacted_locator, std::size_t region_records);

    /**
     * @brief Chooses the next region and returns its range.
     *
     * Probes with the given readers. The read ranges of the readers are changed, so the caller sets the returned
     * range to them again by set_range().
     * @param snapshot_reader the reader of the snapshot file
     * @param compacted_reader the reader of the compacted WAL file, or nullptr if there is no such file. Writes a
     *                         FATAL log and aborts if its presence does not agree with the locator of the file
     * @return the range of the region, or nullopt if none is left
     * @throws limestone_exception if a probed entry cannot be read (the locator is invalid) or is not a data entry
     * @throws limestone_io_exception if reading fails
     */
    [[nodiscard]] std::optional<region_range> allocate(log_entry_reader& snapshot_reader,
                                                       log_entry_reader* compacted_reader);

    /**
     * @brief Returns the number of regions.
     */
    [[nodiscard]] std::size_t region_count() const noexcept;

    /**
     * @brief A probe: reads one entry at the given position and returns its serialized key_sid.
     *
     * Sets the read range to probe_size from offset and reads. If the entry does not fit in it, sets the range to the
     * number of bytes the entry needs and reads again. The only system call is pread.
     * @param reader the reader to read with. Its read range is changed
     * @param offset the position of the beginning of the entry
     * @param file_size the size of the file (bytes): the value recorded at the end of the locator
     * @return a copy of the key_sid
     * @throws limestone_exception if offset is at or beyond the size of the file, the entry cannot be read within the
     *         size of the file, the entry type is invalid, or the entry is not a data entry (writes an ERROR log)
     * @throws limestone_io_exception if reading fails
     */
    [[nodiscard]] static std::string probe(log_entry_reader& reader, std::uint64_t offset, std::uint64_t file_size);

private:
    /**
     * @brief Computes the range of region j.
     */
    [[nodiscard]] region_range compute_range(std::size_t j, log_entry_reader& primary_reader,
                                             log_entry_reader* secondary_reader) const;

    /**
     * @brief Returns the number of the first record of the secondary file whose key is at or above key, found by
     *        bisection. The number of records if there is no such record.
     */
    [[nodiscard]] std::size_t lower_bound(log_entry_reader& secondary_reader, const std::string& key) const;

    locator snapshot_locator_;
    std::optional<locator> compacted_locator_;
    std::size_t region_records_;
    /// whether the primary file is the snapshot file
    bool primary_is_snapshot_{true};
    /// the locator of the primary file (snapshot_locator_ or compacted_locator_)
    const locator* primary_{};
    /// the locator of the secondary file (the one that is not primary_). nullptr if there is no secondary file
    const locator* secondary_{};
    std::size_t region_count_{0};
    /// the number of the next region to hand out
    std::atomic<std::size_t> next_region_{0};
};

}  // namespace limestone::internal
