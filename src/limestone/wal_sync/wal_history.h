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
    struct record {
        epoch_id_type epoch;
        boost::uuids::uuid uuid;
        std::time_t timestamp;
    };

    explicit wal_history(boost::filesystem::path dir_path);

    void append(epoch_id_type epoch);
    [[nodiscard]] std::vector<record> list() const;
    void check_and_recover();
    [[nodiscard]] bool exists() const;
    [[nodiscard]] boost::filesystem::path get_file_path() const;

protected:
    // Note: These members are protected (not private) to allow access from test subclasses.
    static constexpr std::size_t record_size = sizeof(epoch_id_type) + 16 + sizeof(std::int64_t);
    static constexpr const char* file_name_ = "wal_history";
    static constexpr const char* tmp_file_name_ = "wal_history.tmp";

    void set_file_operations(std::unique_ptr<file_operations> file_ops) { file_ops_ = std::move(file_ops); }
    void reset_file_operations() { file_ops_ = std::make_unique<real_file_operations>(); }
    void write_record(FILE* fp, epoch_id_type epoch, const boost::uuids::uuid& uuid, std::int64_t timestamp);
    [[nodiscard]] static record parse_record(const std::array<std::byte, record_size>& buf);
    [[nodiscard]] std::vector<record> read_all_records(const boost::filesystem::path& file_path) const;

private:
    boost::filesystem::path dir_path_;
    std::unique_ptr<file_operations> file_ops_ = std::make_unique<real_file_operations>();
};

} // namespace limestone
