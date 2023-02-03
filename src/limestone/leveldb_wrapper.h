/*
 * Copyright 2022-2022 tsurugi project.
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
// #include <boost/filesystem/operations.hpp>
// #include <boost/foreach.hpp>
#include <leveldb/db.h>

// #include <glog/logging.h>
// #include <limestone/logging.h>

// #include <limestone/api/datastore.h>
// #include "log_entry.h"

namespace limestone::api::impl {

static constexpr const std::string_view leveldb_dir = "leveldb";

class leveldb_wrapper {
public:
    /**
     * @brief create new object
     * @param dir the directory where LevelDB files will be placed
     */
    explicit leveldb_wrapper(const boost::filesystem::path& dir) : lvldb_path_(dir / boost::filesystem::path(std::string(leveldb_dir))) {
        clear_directory();
        
        leveldb::Options options;
        options.create_if_missing = true;
        if (leveldb::Status status = leveldb::DB::Open(options, lvldb_path_.string(), &lvldb_); !status.ok()) {
            LOG_LP(ERROR) << "Unable to open/create LevelDB database, status = " << status.ToString();
            std::abort();
        }
    }

    /**
     * @brief destruct object
     */
    ~leveldb_wrapper() {
        delete lvldb_;
        clear_directory();
    }

    leveldb_wrapper() noexcept = delete;
    leveldb_wrapper(leveldb_wrapper const& other) noexcept = delete;
    leveldb_wrapper& operator=(leveldb_wrapper const& other) noexcept = delete;
    leveldb_wrapper(leveldb_wrapper&& other) noexcept = delete;
    leveldb_wrapper& operator=(leveldb_wrapper&& other) noexcept = delete;

    [[nodiscard]] leveldb::DB* db() const noexcept {
        return lvldb_;
    }
    
private:
    leveldb::DB* lvldb_{};
    boost::filesystem::path lvldb_path_;

    void clear_directory() const noexcept {
        if (boost::filesystem::exists(lvldb_path_)) {
            if (boost::filesystem::is_directory(lvldb_path_)) {
                boost::filesystem::remove_all(lvldb_path_);
            } else {
                LOG_LP(ERROR) << lvldb_path_.string() << " is not a directory";
                std::abort();
            }
        }
    }
};

} // namespace limestone::api::impl
