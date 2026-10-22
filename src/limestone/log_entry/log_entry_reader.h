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

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <ostream>
#include <string>
#include <string_view>

#include <boost/filesystem.hpp>

#include "file_operations.h"
#include "log_entry.h"
#include "log_entry/file_write_state.h"
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
        /// The file ends at an entry boundary. offset is valid. Also returned after short_entry.
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
     * @param offset the size of the file
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
     * - end: the size of the file.
     *
     * For ok and error, the bytes from the start position up to just before this position are the entries returned
     * as ok, with no gap between them. The same holds for end, except that for end after short_entry, the bytes of
     * the cut entry lie in between.
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
 * Reads the file into a buffer in large units, and repeats log_entry_view::parse() on it.
 *
 * When created without a state function, the file is treated as not changing while it is read. When created with a
 * state function, the file is treated as growing toward its end while the state is writing, and the reader waits for
 * more data to arrive. After the state becomes written, the file is treated in the same way as without a state
 * function.
 *
 * @note Not thread-safe. An object must be used by only one thread at a time. When handing it over to another thread,
 *       or reading a view returned by next() in another thread, the caller must establish the ordering by
 *       synchronization (a mutex, starting and joining a thread, and so on). Different objects may be used
 *       concurrently in different threads, even if they read the same file. The state function is called in the
 *       thread that calls next().
 */
class log_entry_reader {
public:
    /// The normal size of the buffer (the size read by one read).
    static constexpr std::size_t buffer_size = std::size_t{16} * 1024 * 1024;

    /// The interval of waiting for more data in a file being written. The state function is called at each wait.
    static constexpr std::chrono::milliseconds wait_interval{10};

    /**
     * @brief Opens the file and creates a reader that starts reading at the given position.
     * @param file the file to read
     * @param start_offset the position to start reading at. It must be the beginning of an entry
     * @throws limestone_io_exception if the file cannot be opened or its status cannot be obtained (with errno), or if
     *         start_offset exceeds the size of the file (without errno)
     */
    explicit log_entry_reader(boost::filesystem::path file, std::uint64_t start_offset = 0);

    /**
     * @brief Creates a reader of a file being written that starts reading at the given position.
     *
     * While the state is writing, the file must be written in order from the beginning, and the bytes up to the
     * current size of the file must hold their final contents (the size must not be extended first with the contents
     * written later).
     * If the file does not exist, or start_offset exceeds the size of the file, it does not throw but waits in
     * next().
     * @param file the file to read
     * @param start_offset the position to start reading at. It must be the beginning of an entry
     * @param state the state function. It must not be empty. As long as it returns writing, next() keeps waiting
     * @throws limestone_io_exception if the file cannot be opened for a reason other than not existing (with errno)
     */
    log_entry_reader(boost::filesystem::path file, std::uint64_t start_offset, file_write_state_function state);

    /**
     * @brief Creates a reader with the file operations replaced. For testing only; must not be used by production
     *        code.
     * @param file the file to read
     * @param start_offset the position to start reading at. It must be the beginning of an entry
     * @param state the state function. If empty, behaves as the first constructor; otherwise as the second
     * @param file_ops the file operations. It must be valid until this object is destroyed
     * @throws limestone_io_exception the same as the first or the second constructor, depending on state
     */
    log_entry_reader(boost::filesystem::path file, std::uint64_t start_offset, file_write_state_function state,
                     file_operations& file_ops);

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
     * A call after end returns end. A call after short_entry returns end (at the size of the file). A call after
     * unknown_type returns the same unknown_type.
     *
     * When created with a state function, while the state is writing, it does not return end or short_entry, but
     * waits for more data, calling the state function every wait_interval.
     * @return the result
     * @throws limestone_io_exception if reading fails (with errno)
     * @throws limestone_io_exception if the state function returns failed (without errno; later calls throw the same
     *         exception without calling the state function), if the file cannot be opened for a reason other than
     *         not existing or its status cannot be obtained while waiting (with errno), if the file cannot be opened
     *         after the state becomes written (with errno), or if the start position or the end of the range already
     *         read exceeds the size of the file after the state becomes written (without errno)
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
     * @brief Opens the file if it exists. Used for a file being written.
     * @return true if opened, false if the file does not exist
     * @throws limestone_io_exception if the file cannot be opened for a reason other than not existing (with errno)
     */
    bool open_if_exists();

    /**
     * @brief Gets the size of the file again. If the file is not open, opens it if it exists. If it does not exist,
     *        the size is 0.
     * @throws limestone_io_exception if the file cannot be opened for a reason other than not existing, or its status
     *         cannot be obtained (with errno)
     */
    void refresh_file_size();

    /**
     * @brief Calls the state function, and updates the handling by its result. Call it only while the file is
     *        treated as being written.
     *
     * If written, gets the size of the file again, and treats the file as not being written from then on. If failed,
     * records it and throws.
     * @throws limestone_io_exception if the state is failed (without errno). If written, and the file cannot be opened
     *         or its status cannot be obtained (with errno), or the start position or the end of the range already
     *         read exceeds the size of the file (without errno)
     */
    void update_write_state();

    /**
     * @brief For a file being written, waits for and reads the data needed to parse the next entry.
     * @param required the lower bound of the number of bytes needed to parse, counted from the beginning of the
     *                 unparsed part
     * @throws limestone_io_exception the same as refresh_file_size(), update_write_state() and read_into_buffer()
     */
    void wait_and_read(std::size_t required);

    /**
     * @brief Creates the result returned at the end of the file: end if there is no unparsed part, and error with
     *        short_entry otherwise.
     *
     * When returning short_entry, moves the read position to the end of the file so that the next call returns end.
     * Called only while reading a file not being written (one that does not grow while it is read).
     */
    [[nodiscard]] log_entry_read_result end_or_short_entry() noexcept;

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
    file_operations* file_ops_{};
    boost::filesystem::path file_{};
    file_write_state_function state_{};
    /// whether the file is treated as being written: true while created with a state function and not yet written
    bool writing_{};
    /// whether the state function returned failed. If true, next() throws without calling the state function
    bool failed_{};
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
