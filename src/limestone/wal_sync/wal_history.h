/*
 * Copyright 2023-2025 Project Tsurugi.
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

#include <string>
#include <vector>
#include <memory>
#include "file_operations.h"
#include <ctime>
#include <boost/filesystem.hpp>
#include <boost/uuid/uuid.hpp>

namespace limestone::internal {

using epoch_id_type = uint64_t;


class wal_history {
public:
    /**
     * @brief Size of the unique_id field in bytes.
     */
    static constexpr std::size_t identity_size = sizeof(uint64_t);

    /**
     * @brief Structure representing a single WAL history record.
     */
    struct record {
        epoch_id_type epoch;  ///< Epoch value.
        uint64_t identity;    ///< identity of the epoch.
        std::time_t timestamp; ///< UNIX timestamp (seconds since epoch).
    };


    /**
     * @brief Constructs a wal_history object for the specified directory.
     * @param dir_path Directory path where the wal_history file is stored.
     * @note This constructor does not throw exceptions.
     */
    explicit wal_history(boost::filesystem::path dir_path) noexcept;

    /**
     * @brief Appends a new WAL history record for the given epoch.
     * @param epoch The epoch value to append.
     * @throws limestone::api::limestone_io_exception I/O error
     */
    void append(epoch_id_type epoch);

    /**
     * @brief Returns a list of all WAL history records.
     * @return Vector of WAL history records.
     * @throws limestone::api::limestone_io_exception I/O error
     */
    [[nodiscard]] std::vector<record> list() const;

    /**
     * @brief Overwrites wal_history file with provided records.
     * @param records records to write in order
     * @throws limestone::api::limestone_io_exception I/O error
     */
    void write_records(const std::vector<record>& records);

    /**
     * @brief Checks the WAL history file and recovers if necessary.
     * @throws limestone::api::limestone_io_exception I/O error
     */
    void check_and_recover();

    /**
     * @brief Checks if the WAL history file exists.
     * @return true if the file exists, false otherwise.
     * @throws limestone::api::limestone_io_exception I/O error
     */
    [[nodiscard]] bool exists() const;

    /**
     * @brief Returns the file path of the WAL history file.
     * @return Path to the WAL history file.
     */
    [[nodiscard]] boost::filesystem::path get_file_path() const noexcept;

    /**
     * @brief Returns the file name of the WAL history file.
     * @return File name as a string literal.
     */
    [[nodiscard]] static const char* file_name() noexcept;

protected:
    // Note: These members are protected (not private) to allow access from test subclasses.
    static constexpr std::size_t record_size = sizeof(epoch_id_type) + identity_size + sizeof(std::int64_t);
    static constexpr const char* file_name_ = "wal_history";
    static constexpr const char* tmp_file_name_ = "wal_history.tmp";

    void set_file_operations(std::unique_ptr<file_operations> file_ops) { file_ops_ = std::move(file_ops); }
    void reset_file_operations() { file_ops_ = std::make_unique<real_file_operations>(); }
    void write_record(FILE* fp, epoch_id_type epoch, uint64_t identity, std::int64_t timestamp);
    [[nodiscard]] static record parse_record(const std::array<std::byte, record_size>& buf);
    [[nodiscard]] std::vector<record> read_all_records(const boost::filesystem::path& file_path) const;

private:
    boost::filesystem::path dir_path_;
    std::unique_ptr<file_operations> file_ops_ = std::make_unique<real_file_operations>();
};

} // namespace limestone
