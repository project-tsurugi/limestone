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
#include <boost/filesystem/operations.hpp>
#include <boost/foreach.hpp>
#include <cstdlib>

#include <glog/logging.h>
#include <limestone/logging.h>
#include "logging_helper.h"

#include "datastore.h"
#include "leveldb_wrapper.h"
#include "log_entry.h"
#include "log_channel.h"
#include "snapshot.h"

namespace limestone::api::impl {

// return max epoch in file.
static std::optional<epoch_id_type> last_durable_epoch(const boost::filesystem::path& file) noexcept {
    std::optional<epoch_id_type> rv;

    boost::filesystem::ifstream istrm;
    log_entry e;
    istrm.open(file, std::ios_base::in | std::ios_base::binary);
    while (e.read(istrm)) {
        if (!rv.has_value() || e.epoch_id() > rv) {
            rv = e.epoch_id();
        }
    }
    istrm.close();
    return rv;
}

epoch_id_type datastore::last_durable_epoch_in_dir() noexcept {
    auto& from_dir = location_;
    // read main epoch file first
    std::optional<epoch_id_type> ld_epoch = last_durable_epoch(from_dir / std::string(log_channel::prefix));
    if (ld_epoch.has_value()) {
        return *ld_epoch;
    }

    // main epoch file is empty,
    // read all rotated-epoch files
    for (const boost::filesystem::path& p : boost::filesystem::directory_iterator(from_dir)) {
        if (p.filename().string().rfind(log_channel::prefix, 0) == 0) {  // starts_with(log_channel::prefix)
            // this is epoch file (main one or rotated)
            std::optional<epoch_id_type> epoch = last_durable_epoch(p);
            if (!epoch.has_value()) {
                continue;  // file is empty
            }
            // ld_epoch = max(ld_epoch, epoch)
            if (!ld_epoch.has_value() || *ld_epoch < *epoch) {
                ld_epoch = epoch;
            }
        }
    }
    return ld_epoch.value_or(0);  // 0 = minimum epoch
}

void datastore::create_snapshot() noexcept {
    auto& from_dir = location_;
    auto lvldb = std::make_unique<leveldb_wrapper>(from_dir);

    epoch_id_type ld_epoch = last_durable_epoch_in_dir();
    epoch_id_switched_.store(ld_epoch + 1);

    BOOST_FOREACH(const boost::filesystem::path& p, std::make_pair(boost::filesystem::directory_iterator(from_dir), boost::filesystem::directory_iterator())) {
        if (p.filename().string().substr(0, log_channel::prefix.length()) == log_channel::prefix) {
            VLOG_LP(log_info) << "processing pwal file: " << p.filename().string();
            log_entry e;
            epoch_id_type current_epoch{UINT64_MAX};

            boost::filesystem::ifstream istrm;
            istrm.open(p, std::ios_base::in | std::ios_base::binary);
            while(e.read(istrm)) {
                switch(e.type()) {
                case log_entry::entry_type::marker_begin:
                {
                    current_epoch = e.epoch_id();
                    break;
                }
                case log_entry::entry_type::normal_entry:
                {
                    if (current_epoch <= ld_epoch) {
                        bool need_write = true;
                        std::string value;
                        write_version_type write_version;
                        e.write_version(write_version);

                        // skip older entry than already inserted
                        leveldb::ReadOptions read_options;
                        if (auto status = lvldb->db()->Get(read_options, e.key_sid(), &value); status.ok()) {
                            if (write_version < write_version_type(value.substr(1))) {
                                need_write = false;
                            }
                        }
                        if (need_write) {
                            std::string db_value;
                            db_value.append(1, static_cast<char>(e.type()));
                            db_value.append(e.value_etc());
                            leveldb::WriteOptions write_options;
                            lvldb->db()->Put(write_options, e.key_sid(), db_value);
                        }
                    }
                    break;
                }
                case log_entry::entry_type::remove_entry:
                {
                    if (current_epoch <= ld_epoch) {
                        bool need_write = true;
                        std::string value;
                        write_version_type write_version;
                        e.write_version(write_version);

                        // skip older entry than already inserted
                        leveldb::ReadOptions read_options;
                        if (auto status = lvldb->db()->Get(read_options, e.key_sid(), &value); status.ok()) {
                            if (write_version < write_version_type(value.substr(1))) {
                                need_write = false;
                            }
                        }
                        if (need_write) {
                            std::string db_value;
                            db_value.append(1, static_cast<char>(e.type()));
                            db_value.append(e.value_etc());
                            leveldb::WriteOptions write_options;
                            lvldb->db()->Put(write_options, e.key_sid(), db_value);
                        }
                    }
                    break;
                }
                default:
                    break;
                }
            }
            istrm.close();
        }
    }

    boost::filesystem::path sub_dir = location_ / boost::filesystem::path(std::string(snapshot::subdirectory_name_));
    boost::system::error_code error;
    const bool result_check = boost::filesystem::exists(sub_dir, error);
    if (!result_check || error) {
        const bool result_mkdir = boost::filesystem::create_directory(sub_dir, error);
        if (!result_mkdir || error) {
            LOG_LP(ERROR) << "fail to create directory";
            std::abort();
        }
    }

    boost::filesystem::ofstream ostrm{};
    boost::filesystem::path snapshot_file = sub_dir / boost::filesystem::path(std::string(snapshot::file_name_));
    ostrm.open(snapshot_file, std::ios_base::out | std::ios_base::trunc | std::ios_base::binary);
    if( ostrm.fail() ){
        LOG_LP(ERROR) << "cannot create snapshot file (" << snapshot_file << ")";
        std::abort();
    }
    leveldb::Iterator* it = lvldb->db()->NewIterator(leveldb::ReadOptions());  // NOLINT (typical usage of leveldb)
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        leveldb::Slice db_value = it->value();
        static_assert(sizeof(log_entry::entry_type) == 1);
        auto entry_type = static_cast<log_entry::entry_type>(db_value[0]);
        db_value.remove_prefix(1);
        switch (entry_type) {
        case log_entry::entry_type::normal_entry:
            log_entry::write(ostrm, it->key().ToString(), db_value.ToString());
            break;
        case log_entry::entry_type::remove_entry:
            break;  // skip
        default:
            LOG_LP(ERROR) << "never reach " << static_cast<int>(entry_type);
            std::abort();
        }
    }
    ostrm.close();
    delete it;  // NOLINT (typical usage of leveldb)
}

} // namespace limestone::api::impl
