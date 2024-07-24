/*
 * Copyright 2022-2024 Project Tsurugi.
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

#include <boost/filesystem.hpp>

#ifdef SORT_METHOD_USE_ROCKSDB
#include <rocksdb/db.h>
#include <rocksdb/comparator.h>
#else
#include <leveldb/db.h>
#include <leveldb/comparator.h>
#endif

#include <glog/logging.h>

#include <limestone/logging.h>

namespace limestone::api {
#ifdef SORT_METHOD_USE_ROCKSDB
    using namespace rocksdb;
#else
    using namespace leveldb;
#endif

static constexpr const std::string_view sortdb_dir = "sorting";

class sortdb_wrapper {
public:
    // type of user-defined key-comparator function
    using keycomp = int(*)(const std::string_view& a, const std::string_view& b);

    /**
     * @brief create new object
     * @param dir the directory where DB library files will be placed
     * @param keycomp (optional) user-defined comparator
     */
    explicit sortdb_wrapper(const boost::filesystem::path& dir, keycomp keycomp = nullptr)
        : workdir_path_(dir / boost::filesystem::path(std::string(sortdb_dir))) {
        clear_directory();
        
        Options options;
        options.create_if_missing = true;
        if (keycomp != nullptr) {
            comp_ = std::make_unique<comparator>(keycomp);
            options.comparator = comp_.get();
        }
        if (Status status = DB::Open(options, workdir_path_.string(), &sortdb_); !status.ok()) {
            LOG_LP(ERROR) << "Unable to open/create database working files, status = " << status.ToString();
            std::abort();
        }
    }

    /**
     * @brief destruct object
     */
    ~sortdb_wrapper() {
        delete sortdb_;
        clear_directory();
    }

    sortdb_wrapper() noexcept = delete;
    sortdb_wrapper(sortdb_wrapper const& other) noexcept = delete;
    sortdb_wrapper& operator=(sortdb_wrapper const& other) noexcept = delete;
    sortdb_wrapper(sortdb_wrapper&& other) noexcept = delete;
    sortdb_wrapper& operator=(sortdb_wrapper&& other) noexcept = delete;

    void put(const std::string& key, const std::string& value) {
        WriteOptions write_options{};
        auto status = sortdb_->Put(write_options, key, value);
        if (status.ok()) { return; }
        LOG_LP(ERROR) << "sortdb put error, status: " << status.ToString();
        throw std::runtime_error("error in sortdb put");
    }

    bool get(const std::string& key, std::string* value) {
        ReadOptions read_options{};
        auto status = sortdb_->Get(read_options, key, value);
        if (status.ok()) { return true; }
        if (status.IsNotFound()) { return false; }
        LOG_LP(ERROR) << "sortdb get error, status: " << status.ToString();
        throw std::runtime_error("error in sortdb get");
    }

    void each(const std::function<void(std::string_view, std::string_view)>& fun) {
        std::unique_ptr<Iterator> it{sortdb_->NewIterator(ReadOptions())};
        for (it->SeekToFirst(); it->Valid(); it->Next()) {
            Slice key = it->key();
            Slice value = it->value();
            fun(std::string_view(key.data(), key.size()), std::string_view(value.data(), value.size()));
        }
        if (!it->status().ok()) {
            LOG_LP(ERROR) << "sortdb iterator invalidated, status: " << it->status().ToString();
            throw std::runtime_error("error in sortdb read iteration");
        }
    }
    
private:
    DB* sortdb_{};

    // user-defined comparator wrapper
    class comparator : public Comparator {
        keycomp keycomp_;
    public:
        explicit comparator(keycomp keycomp) : keycomp_(keycomp) {}
        [[nodiscard]] const char *Name() const override { return "custom comparator"; }
        void FindShortestSeparator(std::string *, const Slice&) const override {}
        void FindShortSuccessor(std::string *) const override {}
        [[nodiscard]] int Compare(const Slice& a, const Slice& b) const override {
            return keycomp_(std::string_view{a.data(), a.size()}, std::string_view{b.data(), b.size()});
        }
    };

    std::unique_ptr<comparator> comp_{};

    boost::filesystem::path workdir_path_;

    void clear_directory() const noexcept {
        if (boost::filesystem::exists(workdir_path_)) {
            if (boost::filesystem::is_directory(workdir_path_)) {
                boost::filesystem::remove_all(workdir_path_);
            } else {
                LOG_LP(ERROR) << workdir_path_.string() << " is not a directory";
                std::abort();
            }
        }
    }
};

} // namespace limestone::api
