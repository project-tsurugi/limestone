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
#include <cstdio>
#include <string>

#include <boost/filesystem.hpp>

namespace limestone::internal {

/**
 * @brief A file written through its own stdio buffer.
 *
 * Takes care of opening, finishing and closing (with or without fsync), and the cleanup when destroyed without being
 * closed. The user writes the contents to stream().
 *
 * Not thread-safe (used from one thread).
 */
class buffered_output_file {
public:
    /// the size of the stdio buffer in bytes. The fwrite calls are accumulated up to this size before write
    static constexpr std::size_t stream_buffer_size = std::size_t{128} * 1024;

    /**
     * @brief Creates and opens the file.
     * @param file the file to write. If it exists, it is truncated
     * @param label the name of the kind of the file used in error messages (for example "sorted file")
     * @throws limestone_io_exception if the file cannot be opened (with errno)
     */
    buffered_output_file(boost::filesystem::path file, std::string label);

    /**
     * @brief Closes the file if neither close() nor sync_and_close() has been called (a failure is ignored).
     */
    ~buffered_output_file();

    buffered_output_file(const buffered_output_file&) = delete;
    buffered_output_file& operator=(const buffered_output_file&) = delete;
    buffered_output_file(buffered_output_file&&) = delete;
    buffered_output_file& operator=(buffered_output_file&&) = delete;

    /**
     * @brief Returns the stream to write to. Writes a FATAL log and aborts if called after the file is closed.
     */
    [[nodiscard]] FILE* stream() const;

    /**
     * @brief Returns whether the file is open (neither close() nor sync_and_close() has been called).
     */
    [[nodiscard]] bool is_open() const noexcept;

    /**
     * @brief Finishes writing and closes the file (fflush, fclose). fsync is not performed.
     *
     * The file is closed even on failure. If called after the file is closed, writes a WARNING log and does nothing.
     * @throws limestone_io_exception if fflush or fclose fails (with errno)
     */
    void close();

    /**
     * @brief Finishes writing, writes the contents out to the disk, and then closes the file (fflush, fsync,
     *        fclose).
     *
     * The file is closed even on failure. If called after a successful sync_and_close(), writes a WARNING log and
     * does nothing. If called after the file is closed otherwise (after close(), or after a failed sync_and_close()),
     * writes a FATAL log and aborts, because the contents are not fsynced.
     * @throws limestone_io_exception if fflush, fsync or fclose fails (with errno)
     */
    void sync_and_close();

    /**
     * @brief Returns the file to write.
     */
    [[nodiscard]] const boost::filesystem::path& file() const noexcept;

private:
    /**
     * @brief Releases and closes the stream: fflush, fsync (if sync), then fclose.
     * @param sync whether to fsync
     * @throws limestone_io_exception if any of them fails (with errno)
     */
    void finish(bool sync);

    boost::filesystem::path file_;
    std::string label_;
    // the stdio buffer. Supplied because setvbuf is not guaranteed to honor the size without a buffer
    std::string stream_buffer_;
    FILE* stream_{};
    // whether sync_and_close() has succeeded
    bool synced_{};
};

}  // namespace limestone::internal
