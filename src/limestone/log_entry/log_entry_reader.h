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
#include <ostream>
#include <string>
#include <string_view>

#include <boost/filesystem.hpp>

#include "file_operations.h"
#include "log_entry.h"
#include "log_entry/log_entry_view.h"

namespace limestone::internal {

/**
 * @brief The result of log_entry_reader::next().
 *
 * Created only by ok(), end() and error(). Only the values valid for the result kind can be read; an accessor writes
 * a FATAL log and aborts if it is asked for a value that is not valid.
 */
class log_entry_read_result {
public:
    /**
     * @brief The kind of the result.
     */
    enum class status : std::uint8_t {
        /// One entry is read. view and offset are valid.
        ok,
        /// The file ends at an entry boundary. offset is valid.
        end,
        /// The contents of the file do not match the format. error and offset are valid.
        error,
    };

    /**
     * @brief Creates a result of kind ok (one entry is read).
     * @param view the entry read. Writes a FATAL log and aborts if it is in the default-constructed state
     * @param offset the position of the first byte of the entry
     * @return the result
     */
    [[nodiscard]] static log_entry_read_result ok(const log_entry_view& view, std::uint64_t offset);

    /**
     * @brief Creates a result of kind end (the file ends at an entry boundary).
     * @param offset the position next to the last byte of the last entry
     * @return the result
     */
    [[nodiscard]] static log_entry_read_result end(std::uint64_t offset) noexcept;

    /**
     * @brief Creates a result of kind error (the contents of the file do not match the format).
     * @param error the kind of the failure (short_entry or unknown_type) and the value of the first byte of the entry
     *              that could not be parsed
     * @param offset the position of the first byte of the entry that could not be parsed
     * @return the result
     */
    [[nodiscard]] static log_entry_read_result error(const limestone::api::log_entry::read_error& error,
                                                     std::uint64_t offset) noexcept;

    /**
     * @brief Returns the kind of the result.
     */
    [[nodiscard]] status result() const noexcept;

    /**
     * @brief Returns the entry read. Valid when the result kind is ok.
     *
     * The returned view refers to the buffer of the log_entry_reader, and is valid until log_entry_reader::next() is
     * called next or the log_entry_reader is destroyed.
     */
    [[nodiscard]] log_entry_view view() const;

    /**
     * @brief Returns a position in bytes, counted from 0 at the beginning of the file. Valid for all result kinds.
     *
     * - ok: the position of the first byte (the entry type) of the returned entry.
     * - error: the position of the first byte (the entry type) of the entry that could not be parsed. It is not the
     *   position where reading or parsing stopped.
     * - end: the position next to the last byte of the last entry. It equals the size of the file.
     *
     * For every result, the bytes from the start position up to just before this position are the entries returned
     * as ok, with no gap between them.
     */
    [[nodiscard]] std::uint64_t offset() const noexcept;

    /**
     * @brief Returns the kind of the failure and the value of the first byte of the entry that could not be parsed.
     *        Valid when the result kind is error.
     *
     * The kind of the failure (read_error::value()) is short_entry or unknown_type. The value of the first byte
     * (read_error::entry_type()) is as follows.
     * - short_entry: a defined entry type (other than this_id_is_not_used).
     * - unknown_type: an undefined value, or this_id_is_not_used. It has no meaning as an entry type.
     */
    [[nodiscard]] limestone::api::log_entry::read_error error() const;

private:
    /**
     * @brief Creates a result from the values of its members. Called only by ok(), end() and error().
     */
    log_entry_read_result(status result, const log_entry_view& view, std::uint64_t offset,
                          const limestone::api::log_entry::read_error& error) noexcept;

    status result_{};
    log_entry_view view_{};
    std::uint64_t offset_{};
    limestone::api::log_entry::read_error error_{};
};

/**
 * @brief Returns the string representation of a log_entry_read_result::status value.
 * @param result the result kind
 * @return the name of the result kind, or "unknown" for an undefined value
 */
[[nodiscard]] std::string_view to_string_view(log_entry_read_result::status result) noexcept;

/**
 * @brief Writes the string representation of a log_entry_read_result::status value to a stream.
 * @param out the output stream
 * @param result the result kind
 * @return out
 */
std::ostream& operator<<(std::ostream& out, log_entry_read_result::status result);

/**
 * @brief Reads entries from a file in order from the beginning.
 *
 * Reads the file into a buffer in large units, and repeats log_entry_view::parse() on it. The file is treated as not
 * changing while it is read.
 */
class log_entry_reader {
public:
    /// The normal size of the buffer (the size read by one read).
    static constexpr std::size_t buffer_size = 16U * 1024U * 1024U;

    /**
     * @brief Opens the file and creates a reader that starts reading at the given position.
     * @param file the file to read
     * @param start_offset the position to start reading at. It must be the beginning of an entry
     * @throws limestone_io_exception if the file cannot be opened or its status cannot be obtained (with errno), or if
     *         start_offset exceeds the size of the file (without errno)
     */
    explicit log_entry_reader(const boost::filesystem::path& file, std::uint64_t start_offset = 0);

    /**
     * @brief Creates a reader with the file operations replaced. For testing only; must not be used by production
     *        code.
     * @param file the file to read
     * @param start_offset the position to start reading at. It must be the beginning of an entry
     * @param file_ops the file operations. It must be valid until this object is destroyed
     * @throws limestone_io_exception the same as the first constructor
     */
    log_entry_reader(const boost::filesystem::path& file, std::uint64_t start_offset, file_operations& file_ops);

    /**
     * @brief Closes the file.
     */
    ~log_entry_reader();

    log_entry_reader(const log_entry_reader&) = delete;
    log_entry_reader& operator=(const log_entry_reader&) = delete;
    log_entry_reader(log_entry_reader&&) = delete;
    log_entry_reader& operator=(log_entry_reader&&) = delete;

    /**
     * @brief Reads the next entry.
     *
     * The result is one of the following.
     * - ok: one entry is read. The view is valid until next() is called next.
     * - end: the file ends at an entry boundary.
     * - error: the contents of the file do not match the format. The kind is either an invalid entry type
     *   (unknown_type) or the file ending in the middle of an entry (short_entry).
     *
     * A call after end or error is returned returns the same result.
     * @return the result
     * @throws limestone_io_exception if reading fails (with errno)
     */
    [[nodiscard]] log_entry_read_result next();

private:
    /**
     * @brief Opens the file, obtains its size, and sets the start position. Called only by the constructors.
     * @param start_offset the position to start reading at
     * @throws limestone_io_exception the same as the constructors. The file is closed before throwing
     */
    void open_file(std::uint64_t start_offset);

    /**
     * @brief Creates the result returned at the end of the file: end if there is no unparsed part, and error with
     *        short_entry otherwise.
     */
    [[nodiscard]] log_entry_read_result end_or_short_entry() const noexcept;

    /**
     * @brief Prepares to refill the buffer. Places the unparsed part at the beginning of the buffer, and makes free
     *        space of at least the lower bound.
     * @param required the lower bound of the number of bytes needed to parse, counted from the beginning of the
     *                 unparsed part
     */
    void prepare_buffer(std::size_t required);

    /**
     * @brief Reads into the whole free space of the buffer.
     * @return the number of bytes read. 0 means the end of the file
     * @throws limestone_io_exception if reading fails (with errno)
     */
    std::size_t read_into_buffer();

    /**
     * @brief Returns the remaining size of the file (the size of the file minus the position of the beginning of the
     *        unparsed part).
     */
    [[nodiscard]] std::uint64_t remaining_file_size() const noexcept;

    real_file_operations real_file_ops_{};
    file_operations* file_ops_;
    boost::filesystem::path file_;
    int fd_{-1};
    std::uint64_t file_size_{};

    std::string buffer_{};
    /// the file position of the beginning of buffer_
    std::uint64_t buffer_offset_{};
    /// the beginning of the unparsed part (a position in buffer_)
    std::size_t begin_{};
    /// the end of the data read (a position in buffer_)
    std::size_t end_{};
};

}  // namespace limestone::internal
