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

#include <byteswap.h>
#include <boost/filesystem/fstream.hpp>
#include <cstdlib>
#include <cstring>
#include <mutex>

#include <glog/logging.h>
#include <limestone/logging.h>
#include "logging_helper.h"

#include <limestone/api/datastore.h>
#include "dblog_scan.h"
#include "internal.h"
#include "log_entry.h"
#include "sortdb_wrapper.h"

namespace limestone::internal {

constexpr std::size_t write_version_size = sizeof(epoch_id_type) + sizeof(std::uint64_t);
static_assert(write_version_size == 16);

[[maybe_unused]]
static void store_bswap64_value(void *dest, const void *src) {
    auto* p64_dest = reinterpret_cast<std::uint64_t*>(dest);  // NOLINT(*-reinterpret-cast)
    auto* p64_src = reinterpret_cast<const std::uint64_t*>(src);  // NOLINT(*-reinterpret-cast)
    *p64_dest = __bswap_64(*p64_src);
}

[[maybe_unused]]
static int comp_twisted_key(const std::string_view& a, const std::string_view& b) {
    std::size_t a_strlen = a.size() - write_version_size;
    std::size_t b_strlen = b.size() - write_version_size;
    std::string_view a_str(a.data() + write_version_size, a_strlen);
    std::string_view b_str(b.data() + write_version_size, b_strlen);
    if (int c = a_str.compare(b_str); c != 0) return c;
    return std::memcmp(b.data(), a.data(), write_version_size);
}

[[maybe_unused]]
static void insert_entry_or_update_to_max(sortdb_wrapper* sortdb, log_entry& e) {
    bool need_write = true;
    // skip older entry than already inserted
    std::string value;
    if (sortdb->get(e.key_sid(), &value)) {
        write_version_type write_version;
        e.write_version(write_version);
        if (write_version < write_version_type(value.substr(1))) {
            need_write = false;
        }
    }
    if (need_write) {
        std::string db_value;
        db_value.append(1, static_cast<char>(e.type()));
        db_value.append(e.value_etc());
        sortdb->put(e.key_sid(), db_value);
    }
}

[[maybe_unused]]
static void insert_twisted_entry(sortdb_wrapper* sortdb, log_entry& e) {
    // key_sid: storage_id[8] key[*], value_etc: epoch[8]LE minor_version[8]LE value[*], type: type[1]
    // db_key: epoch[8]BE minor_version[8]BE storage_id[8] key[*], db_value: type[1] value[*]
    std::string db_key(write_version_size + e.key_sid().size(), '\0');
    store_bswap64_value(&db_key[0], &e.value_etc()[0]);  // NOLINT(readability-container-data-pointer)
    store_bswap64_value(&db_key[8], &e.value_etc()[8]);
    std::memcpy(&db_key[write_version_size], e.key_sid().data(), e.key_sid().size());
    std::string db_value(1, static_cast<char>(e.type()));
    db_value.append(e.value_etc().substr(write_version_size));
    sortdb->put(db_key, db_value);
}

static std::pair<epoch_id_type, std::unique_ptr<sortdb_wrapper>> create_sortdb_from_wals(
    const boost::filesystem::path& from_dir,
    int num_worker,
    const std::set<std::string>& file_names = std::set<std::string>()) {
#if defined SORT_METHOD_PUT_ONLY
    auto sortdb = std::make_unique<sortdb_wrapper>(from_dir, comp_twisted_key);
#else
    auto sortdb = std::make_unique<sortdb_wrapper>(from_dir);
#endif
    dblog_scan logscan = file_names.empty() ? dblog_scan{from_dir} : dblog_scan{from_dir, file_names};

    epoch_id_type ld_epoch = logscan.last_durable_epoch_in_dir();

#if defined SORT_METHOD_PUT_ONLY
    const auto add_entry_to_point = insert_twisted_entry;
    bool works_with_multi_thread = true;
#else
    const auto add_entry_to_point = insert_entry_or_update_to_max;
    bool works_with_multi_thread = false;
#endif
    auto add_entry = [&sortdb, &add_entry_to_point](log_entry& e){
        switch (e.type()) {
        case log_entry::entry_type::normal_entry:
        case log_entry::entry_type::remove_entry:
            add_entry_to_point(sortdb.get(), e);
            break;
        case log_entry::entry_type::clear_storage:
        case log_entry::entry_type::remove_storage:
            break;  // TODO: implement
        case log_entry::entry_type::add_storage:
            break;  // ignore
        default:
            assert(false);
        }
    };

    if (!works_with_multi_thread && num_worker > 1) {
        LOG(INFO) << "/:limestone:config:datastore this sort method does not work correctly with multi-thread, so force the number of recover process thread = 1";
        num_worker = 1;
    }
    logscan.set_thread_num(num_worker);
    try {
        epoch_id_type max_appeared_epoch = logscan.scan_pwal_files_throws(ld_epoch, add_entry);
        return {max_appeared_epoch, std::move(sortdb)};
    } catch (std::runtime_error& e) {
        VLOG_LP(log_info) << "failed to scan pwal files: " << e.what();
        LOG(ERROR) << "/:limestone recover process failed. (cause: corruption detected in transaction log data directory), "
                   << "see https://github.com/project-tsurugi/tsurugidb/blob/master/docs/troubleshooting-guide.md";
        LOG(ERROR) << "/:limestone dblogdir (transaction log directory): " << from_dir;
        throw std::runtime_error("dblogdir is corrupted");
    }
}

static void sortdb_foreach(sortdb_wrapper *sortdb, std::function<void(const std::string_view key, const std::string_view value)> write_snapshot_entry) {
    static_assert(sizeof(log_entry::entry_type) == 1);
#if defined SORT_METHOD_PUT_ONLY
    sortdb->each([write_snapshot_entry, last_key = std::string{}](const std::string_view db_key, const std::string_view db_value) mutable {
        // using the first entry in GROUP BY (original-)key
        // NB: max versions comes first (by the custom-comparator)
        std::string_view key(db_key.data() + write_version_size, db_key.size() - write_version_size);
        if (key == last_key) {  // same (original-)key with prev
            return; // skip
        }
        last_key.assign(key);

        auto entry_type = static_cast<log_entry::entry_type>(db_value[0]);
        switch (entry_type) {
        case log_entry::entry_type::normal_entry: {
            std::string value(write_version_size + db_value.size() - 1, '\0');
            store_bswap64_value(&value[0], &db_key[0]);
            store_bswap64_value(&value[8], &db_key[8]);
            std::memcpy(&value[write_version_size], &db_value[1], db_value.size() - 1);
            write_snapshot_entry(key, value);
            break;
        }
        case log_entry::entry_type::remove_entry:
            break;  // skip
        default:
            LOG(ERROR) << "never reach " << static_cast<int>(entry_type);
            std::abort();
        }
    });
#else
    sortdb->each([&write_snapshot_entry](const std::string_view db_key, const std::string_view db_value) {
        auto entry_type = static_cast<log_entry::entry_type>(db_value[0]);
        switch (entry_type) {
        case log_entry::entry_type::normal_entry:
            write_snapshot_entry(db_key, db_value.substr(1));
            break;
        case log_entry::entry_type::remove_entry:
            break;  // skip
        default:
            LOG(ERROR) << "never reach " << static_cast<int>(entry_type);
            std::abort();
        }
    });
#endif
}

void create_compact_pwal(
    const boost::filesystem::path& from_dir, 
    const boost::filesystem::path& to_dir, 
    int num_worker,
    const std::set<std::string>& file_names) {
    auto [max_appeared_epoch, sortdb] = create_sortdb_from_wals(from_dir, num_worker, file_names);

    boost::system::error_code error;
    const bool result_check = boost::filesystem::exists(to_dir, error);
    if (!result_check || error) {
        const bool result_mkdir = boost::filesystem::create_directory(to_dir, error);
        if (!result_mkdir || error) {
            LOG_LP(ERROR) << "fail to create directory " << to_dir;
            throw std::runtime_error("I/O error");
        }
    }

    boost::filesystem::path snapshot_file = to_dir / boost::filesystem::path("pwal_0000.compacted");
    VLOG_LP(log_info) << "generating compacted pwal file: " << snapshot_file;
    FILE* ostrm = fopen(snapshot_file.c_str(), "w");  // NOLINT(*-owning-memory)
    if (!ostrm) {
        LOG_LP(ERROR) << "cannot create snapshot file (" << snapshot_file << ")";
        throw std::runtime_error("I/O error");
    }
    setvbuf(ostrm, nullptr, _IOFBF, 128L * 1024L);  // NOLINT, NB. glibc may ignore size when _IOFBF and buffer=NULL
    bool rewind = true;  // TODO: change by flag
    epoch_id_type epoch = rewind ? 0 : max_appeared_epoch;
    log_entry::begin_session(ostrm, epoch);
    auto write_snapshot_entry = [&ostrm, &rewind](std::string_view key_stid, std::string_view value_etc) {
        if (rewind) {
            static std::string value{};
            value = value_etc;
            std::memset(value.data(), 0, 16);
            log_entry::write(ostrm, key_stid, value);
        } else {
            log_entry::write(ostrm, key_stid, value_etc);
        }
    };
    sortdb_foreach(sortdb.get(), write_snapshot_entry);
    //log_entry::end_session(ostrm, epoch);
    if (fclose(ostrm) != 0) {  // NOLINT(*-owning-memory)
        LOG_LP(ERROR) << "cannot close snapshot file (" << snapshot_file << "), errno = " << errno;
        throw std::runtime_error("I/O error");
    }
}

}

namespace limestone::api {
using namespace limestone::internal;

void datastore::create_snapshot(const std::set<std::string>& file_names) {
    const auto& from_dir = location_;
    auto [max_appeared_epoch, sortdb] = create_sortdb_from_wals(from_dir, recover_max_parallelism_, file_names);
    epoch_id_switched_.store(max_appeared_epoch);
    epoch_id_informed_.store(max_appeared_epoch);

    boost::filesystem::path sub_dir = location_ / boost::filesystem::path(std::string(snapshot::subdirectory_name_));
    boost::system::error_code error;
    const bool result_check = boost::filesystem::exists(sub_dir, error);
    if (!result_check || error) {
        const bool result_mkdir = boost::filesystem::create_directory(sub_dir, error);
        if (!result_mkdir || error) {
            LOG_LP(ERROR) << "fail to create directory";
            throw std::runtime_error("I/O error");
        }
    }

    boost::filesystem::path snapshot_file = sub_dir / boost::filesystem::path(std::string(snapshot::file_name_));
    VLOG_LP(log_info) << "generating snapshot file: " << snapshot_file;
    FILE* ostrm = fopen(snapshot_file.c_str(), "w");  // NOLINT(*-owning-memory)
    if (!ostrm) {
        LOG_LP(ERROR) << "cannot create snapshot file (" << snapshot_file << ")";
        throw std::runtime_error("I/O error");
    }
    setvbuf(ostrm, nullptr, _IOFBF, 128L * 1024L);  // NOLINT, NB. glibc may ignore size when _IOFBF and buffer=NULL
    auto write_snapshot_entry = [&ostrm](std::string_view key, std::string_view value){log_entry::write(ostrm, key, value);};
    sortdb_foreach(sortdb.get(), write_snapshot_entry);
    if (fclose(ostrm) != 0) {  // NOLINT(*-owning-memory)
        LOG_LP(ERROR) << "cannot close snapshot file (" << snapshot_file << "), errno = " << errno;
        throw std::runtime_error("I/O error");
    }
}

} // namespace limestone::api
