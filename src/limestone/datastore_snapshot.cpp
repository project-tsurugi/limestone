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
#include <map>
#include <mutex>

#include <glog/logging.h>
#include <limestone/logging.h>
#include "logging_helper.h"

#include <limestone/api/datastore.h>
#include "limestone_exception_helper.h"
#include "compaction_catalog.h"
#include "dblog_scan.h"
#include "internal.h"
#include "log_entry.h"
#include "sortdb_wrapper.h"

namespace limestone::internal {

constexpr std::size_t write_version_size = sizeof(epoch_id_type) + sizeof(std::uint64_t);
static_assert(write_version_size == 16);

class sorting_context {
public:
    sorting_context(sorting_context&& obj) noexcept : sortdb(std::move(obj.sortdb)) {
        std::unique_lock lk{obj.mtx_clear_storage};
        clear_storage = std::move(obj.clear_storage);  // NOLINT(*-prefer-member-initializer): need lock
    }
    sorting_context(const sorting_context&) = delete;
    sorting_context& operator=(const sorting_context&) = delete;
    sorting_context& operator=(sorting_context&&) = delete;
    sorting_context() = default;
    ~sorting_context() = default;
    explicit sorting_context(std::unique_ptr<sortdb_wrapper>&& s) noexcept : sortdb(std::move(s)) {
    }

    // point entries
private:
    std::unique_ptr<sortdb_wrapper> sortdb;
public:
    sortdb_wrapper* get_sortdb() { return sortdb.get(); }

    // range delete entries
private:
    std::mutex mtx_clear_storage;
    std::map<storage_id_type, write_version_type> clear_storage;
public:
    void clear_storage_update(const storage_id_type sid, const write_version_type wv) {
        std::unique_lock lk{mtx_clear_storage};
        if (auto [it, inserted] = clear_storage.emplace(sid, wv);
            !inserted) {
            it->second = std::max(it->second, wv);
        }
    }
    std::optional<write_version_type> clear_storage_find(const storage_id_type sid) {
        // no need to lock, for now
        auto itr = clear_storage.find(sid);
        if (itr == clear_storage.end()) return {};
        return {itr->second};
    }
};

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
static void insert_entry_or_update_to_max(sortdb_wrapper* sortdb, const log_entry& e) {
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
static void insert_twisted_entry(sortdb_wrapper* sortdb, const log_entry& e) {
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

static std::pair<epoch_id_type, sorting_context> create_sorted_from_wals(
    const boost::filesystem::path& from_dir,
    int num_worker,
    const std::set<std::string>& file_names = std::set<std::string>()) {
#if defined SORT_METHOD_PUT_ONLY
    sorting_context sctx{std::make_unique<sortdb_wrapper>(from_dir, comp_twisted_key)};
#else
    sorting_context sctx{std::make_unique<sortdb_wrapper>(from_dir)};
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
    auto add_entry = [&sctx, &add_entry_to_point](const log_entry& e){
        switch (e.type()) {
        case log_entry::entry_type::normal_entry:
        case log_entry::entry_type::remove_entry:
            add_entry_to_point(sctx.get_sortdb(), e);
            break;
        case log_entry::entry_type::clear_storage:
        case log_entry::entry_type::remove_storage: {  // remove_storage is treated as clear_storage
            // clear_storage[st] = max(clear_storage[st], wv)
            write_version_type wv;
            e.write_version(wv);
            sctx.clear_storage_update(e.storage(), wv);
            return;
        }
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
        return {max_appeared_epoch, std::move(sctx)};
    } catch (limestone_exception& e) {
        VLOG_LP(log_info) << "failed to scan pwal files: " << e.what();
        LOG(ERROR) << "/:limestone recover process failed. (cause: corruption detected in transaction log data directory), "
                   << "see https://github.com/project-tsurugi/tsurugidb/blob/master/docs/troubleshooting-guide.md";
        LOG(ERROR) << "/:limestone dblogdir (transaction log directory): " << from_dir;
        throw limestone_exception("dblogdir is corrupted");
    }
}


[[maybe_unused]]
static write_version_type extract_write_version(const std::string_view& db_key) {
    std::string wv(write_version_size, '\0');
    store_bswap64_value(&wv[0], &db_key[0]);  // NOLINT(readability-container-data-pointer)
    store_bswap64_value(&wv[8], &db_key[8]); 
    return write_version_type{wv};
}

[[maybe_unused]]
static std::string create_value_from_db_key_and_value(const std::string_view& db_key, const std::string_view& db_value) {
    std::string value(write_version_size + db_value.size() - 1, '\0');
    store_bswap64_value(&value[0], &db_key[0]);  // NOLINT(readability-container-data-pointer)
    store_bswap64_value(&value[8], &db_key[8]);
    std::memcpy(&value[write_version_size], &db_value[1], db_value.size() - 1);
    return value;
}

static void sortdb_foreach(sorting_context& sctx,
                           const std::function<void(const std::string_view key_sid, const std::string_view value_etc)>& write_snapshot_entry,
                           const std::function<void(const std::string_view key_sid, const std::string_view value_etc)>& write_snapshot_remove_entry) {
    static_assert(sizeof(log_entry::entry_type) == 1);
#if defined SORT_METHOD_PUT_ONLY
    sctx.get_sortdb()->each([&sctx, write_snapshot_entry, write_snapshot_remove_entry, last_key = std::string{}](const std::string_view db_key, const std::string_view db_value) mutable {
        // using the first entry in GROUP BY (original-)key
        // NB: max versions comes first (by the custom-comparator)
        std::string_view key(db_key.data() + write_version_size, db_key.size() - write_version_size);
        if (key == last_key) {  // same (original-)key with prev
            return; // skip
        }
        last_key.assign(key);
        storage_id_type st_bytes{};
        memcpy(static_cast<void*>(&st_bytes), key.data(), sizeof(storage_id_type));
        storage_id_type st = le64toh(st_bytes);

        if (auto ret = sctx.clear_storage_find(st); ret) {
            // check range delete
            write_version_type range_ver = ret.value();
            if (extract_write_version(db_key) < range_ver) {
                return;  // skip
            }
        }

        auto entry_type = static_cast<log_entry::entry_type>(db_value[0]);
        switch (entry_type) {
        case log_entry::entry_type::normal_entry:
            write_snapshot_entry(key, create_value_from_db_key_and_value(db_key, db_value));
            break;
        case log_entry::entry_type::remove_entry: {
            write_snapshot_remove_entry(key, create_value_from_db_key_and_value(db_key, db_value));
            break;
        }
        default:
            LOG(ERROR) << "never reach " << static_cast<int>(entry_type);
            std::abort();
        }
    });
#else
    sctx.get_sortdb()->each([&sctx, &write_snapshot_entry, write_snapshot_remove_entry](const std::string_view db_key, const std::string_view db_value) {
        storage_id_type st_bytes{};
        memcpy(static_cast<void*>(&st_bytes), db_key.data(), sizeof(storage_id_type));
        storage_id_type st = le64toh(st_bytes);
        if (auto ret = sctx.clear_storage_find(st); ret) {
            // check range delete
            write_version_type range_ver = ret.value();
            write_version_type point_ver{db_value.substr(1)};
            if (point_ver < range_ver) {
                return;  // skip
            }
        }
        auto entry_type = static_cast<log_entry::entry_type>(db_value[0]);
        switch (entry_type) {
        case log_entry::entry_type::normal_entry:
            write_snapshot_entry(db_key, db_value.substr(1));
            break;
        case log_entry::entry_type::remove_entry: 
            write_snapshot_remove_entry(db_key, db_value.substr(1));
            break;
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
    auto [max_appeared_epoch, sctx] = create_sorted_from_wals(from_dir, num_worker, file_names);

    boost::system::error_code error;
    const bool result_check = boost::filesystem::exists(to_dir, error);
    if (!result_check || error) {
        const bool result_mkdir = boost::filesystem::create_directory(to_dir, error);
        if (!result_mkdir || error) {
            LOG_AND_THROW_IO_EXCEPTION("fail to create directory " + to_dir.string(), error);
        }
    }

    boost::filesystem::path snapshot_file = to_dir / boost::filesystem::path("pwal_0000.compacted");
    VLOG_LP(log_info) << "generating compacted pwal file: " << snapshot_file;
    FILE* ostrm = fopen(snapshot_file.c_str(), "w");  // NOLINT(*-owning-memory)
    if (!ostrm) {
        LOG_AND_THROW_IO_EXCEPTION("cannot create snapshot file (" + snapshot_file.string() + ")", errno);
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
    sortdb_foreach(sctx, write_snapshot_entry, [](std::string_view, std::string_view) {});
    //log_entry::end_session(ostrm, epoch);
    if (fclose(ostrm) != 0) {  // NOLINT(*-owning-memory)
        LOG_AND_THROW_IO_EXCEPTION("cannot close snapshot file (" + snapshot_file.string() + ")", errno);
    }
}

std::set<std::string> assemble_snapshot_input_filenames(
    const std::unique_ptr<compaction_catalog>& compaction_catalog,
    const boost::filesystem::path& location,
    file_operations& file_ops) {
    std::set<std::string> detached_pwals = compaction_catalog->get_detached_pwals();
    std::set<std::string> filename_set;
    boost::system::error_code error;
    boost::filesystem::directory_iterator it(location, error);
    boost::filesystem::directory_iterator end;

    if (error) {
        LOG_AND_THROW_IO_EXCEPTION("Failed to initialize directory iterator, path: " + location.string(), error);
    }

    for (; it != end; file_ops.directory_iterator_next(it, error)) {
        if (error) {
            LOG_AND_THROW_IO_EXCEPTION("Failed to access directory entry, path: " + location.string(), error);
        }
        if (boost::filesystem::is_regular_file(it->path())) {
            std::string filename = it->path().filename().string();
            if (detached_pwals.find(filename) == detached_pwals.end() 
                && filename != compaction_catalog::get_catalog_filename()
                && filename != compaction_catalog::get_compacted_filename()) {
                filename_set.insert(filename);
            }
        }
    }
    return filename_set;
}

std::set<std::string> assemble_snapshot_input_filenames(
    const std::unique_ptr<compaction_catalog>& compaction_catalog,
    const boost::filesystem::path& location) {
    real_file_operations file_ops;
    return assemble_snapshot_input_filenames(compaction_catalog, location, file_ops);
}



} // namespace limestone::internal

namespace limestone::api {
using namespace limestone::internal;

void datastore::create_snapshot() {
    const auto& from_dir = location_;
    std::set<std::string> file_names = assemble_snapshot_input_filenames(compaction_catalog_, from_dir);
    auto [max_appeared_epoch, sctx] = create_sorted_from_wals(from_dir, recover_max_parallelism_, file_names);
    epoch_id_switched_.store(max_appeared_epoch);
    epoch_id_informed_.store(max_appeared_epoch);

    boost::filesystem::path sub_dir = location_ / boost::filesystem::path(std::string(snapshot::subdirectory_name_));
    boost::system::error_code error;
    const bool result_check = boost::filesystem::exists(sub_dir, error);
    if (!result_check || error) {
        const bool result_mkdir = boost::filesystem::create_directory(sub_dir, error);
        if (!result_mkdir || error) {
            LOG_AND_THROW_IO_EXCEPTION("fail to create directory", error);
        }
    }

    boost::filesystem::path snapshot_file = sub_dir / boost::filesystem::path(std::string(snapshot::file_name_));
    VLOG_LP(log_info) << "generating snapshot file: " << snapshot_file;
    FILE* ostrm = fopen(snapshot_file.c_str(), "w");  // NOLINT(*-owning-memory)
    if (!ostrm) {
        LOG_AND_THROW_IO_EXCEPTION("cannot create snapshot file", errno);
    }
    log_entry::begin_session(ostrm, 0);
    setvbuf(ostrm, nullptr, _IOFBF, 128L * 1024L);  // NOLINT, NB. glibc may ignore size when _IOFBF and buffer=NULL
    auto write_snapshot_entry = [&ostrm](std::string_view key_sid, std::string_view value_etc) { log_entry::write(ostrm, key_sid, value_etc); };

    std::function<void(std::string_view key, std::string_view value_etc)> write_snapshot_remove_entry;
    if (compaction_catalog_->get_compacted_files().empty()) {
        write_snapshot_remove_entry = [](std::string_view, std::string_view) {};
    } else {
        write_snapshot_remove_entry = [&ostrm](std::string_view key, std::string_view value_etc) {
            log_entry::write_remove(ostrm, key, value_etc);
        };
    }
    sortdb_foreach(sctx, write_snapshot_entry, write_snapshot_remove_entry);
    if (fclose(ostrm) != 0) {  // NOLINT(*-owning-memory)
        LOG_AND_THROW_IO_EXCEPTION("cannot close snapshot file (" + snapshot_file.string() + ")", errno);
    }
}

} // namespace limestone::api
