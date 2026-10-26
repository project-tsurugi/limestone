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
#include <cstdint>

#include <boost/filesystem.hpp>

namespace limestone::internal {

/**
 * @brief The place of the sorted files. Creates and removes the subdirectory with the fixed name "sorting" under the
 *        parent directory.
 *
 * If sorting already exists at creation, it is removed with its contents before being created. remove() removes it
 * with its contents. It is also removed when destroyed without remove() (a failure is ignored).
 * The paths of the sorted files are numbered by new_file_path(), which is thread-safe.
 */
class sorting_directory {
public:
    /// the name of the subdirectory
    static constexpr const char* name = "sorting";

    /**
     * @brief Creates sorting under parent. If it already exists, removes it with its contents first.
     * @param parent the parent directory. It must exist
     * @throws limestone_io_exception if the removal or the creation fails (with errno)
     */
    explicit sorting_directory(const boost::filesystem::path& parent);

    /**
     * @brief Removes sorting with its contents if remove() has not been called (a failure is ignored).
     */
    ~sorting_directory();

    sorting_directory(const sorting_directory&) = delete;
    sorting_directory& operator=(const sorting_directory&) = delete;
    sorting_directory(sorting_directory&&) = delete;
    sorting_directory& operator=(sorting_directory&&) = delete;

    /**
     * @brief Returns the path of sorting.
     */
    [[nodiscard]] const boost::filesystem::path& path() const noexcept;

    /**
     * @brief Numbers and returns the path of a new sorted file. Does not create the file.
     *
     * Returns a different path (directly under sorting, with a sequential number) at each call. May be called from
     * multiple threads.
     * @return the path of a new sorted file
     */
    [[nodiscard]] boost::filesystem::path new_file_path();

    /**
     * @brief Removes sorting with its contents.
     *
     * Writes a FATAL log and aborts if called twice.
     * @throws limestone_io_exception if the removal fails (with errno)
     */
    void remove();

private:
    boost::filesystem::path path_;
    std::atomic<std::uint64_t> next_file_number_{};
    bool removed_{};
};

}  // namespace limestone::internal
