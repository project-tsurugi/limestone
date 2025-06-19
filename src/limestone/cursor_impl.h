/*
 * Copyright 2022-2023 Project Tsurugi.
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
#include <limestone/api/blob_id_type.h>
#include <limestone/api/cursor.h>
#include <limestone/api/storage_id_type.h>

#include <boost/filesystem.hpp>
#include <boost/filesystem/fstream.hpp>
#include <map>
#include <optional>

#include "cursor_impl_base.h"
#include "log_entry.h"

namespace limestone::internal {

using limestone::api::cursor;    
class cursor_impl : public cursor_impl_base {
public:
    explicit cursor_impl(const boost::filesystem::path& snapshot_file);
    explicit cursor_impl(const boost::filesystem::path& snapshot_file, const boost::filesystem::path& compacted_file);

    static std::unique_ptr<cursor> create_cursor(const boost::filesystem::path& snapshot_file,
                                                  const std::map<limestone::api::storage_id_type, limestone::api::write_version_type>& clear_storage);
    static std::unique_ptr<cursor> create_cursor(const boost::filesystem::path& snapshot_file, const boost::filesystem::path& compacted_file,
                                                  const std::map<limestone::api::storage_id_type, limestone::api::write_version_type>& clear_storage);

    void set_clear_storage(const std::map<limestone::api::storage_id_type, limestone::api::write_version_type>& clear_storage) override;

private:
    limestone::api::log_entry log_entry_;
    std::optional<limestone::api::log_entry> snapshot_log_entry_;
    std::optional<limestone::api::log_entry> compacted_log_entry_;
    std::optional<boost::filesystem::ifstream> snapshot_istrm_;
    std::optional<boost::filesystem::ifstream> compacted_istrm_;
    std::string previous_snapshot_key_sid;
    std::string previous_compacted_key_sid;
    std::map<limestone::api::storage_id_type, limestone::api::write_version_type> clear_storage_; 

protected:
    void open(const boost::filesystem::path& file, std::optional<boost::filesystem::ifstream>& stream);
    void close() override;

    bool next() override;
    void validate_and_read_stream(std::optional<boost::filesystem::ifstream>& stream, const std::string& stream_name, 
                                  std::optional<limestone::api::log_entry>& log_entry, std::string& previous_key_sid);

    [[nodiscard]] limestone::api::storage_id_type storage() const noexcept override;
    void key(std::string& buf) const noexcept override;
    void value(std::string& buf) const noexcept override;
    std::vector<limestone::api::blob_id_type> blob_ids() const override;
    [[nodiscard]] limestone::api::log_entry::entry_type type() const override;
    bool is_relevant_entry(const limestone::api::log_entry& entry);
    // Making the cursor class a friend so that it can access protected members
    friend class limestone::api::cursor;
};

} // namespace limestone::internal
