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
#include <glog/logging.h>
#include <limestone/api/datastore.h>
#include <limestone/logging.h>

#include <boost/filesystem/fstream.hpp>
#include <cstdlib>
#include <cstring>
#include <map>
#include <mutex>

#include "compaction_catalog.h"
#include "datastore_impl.h"
#include "dblog_scan.h"
#include "internal.h"
#include "limestone_exception_helper.h"
#include "log_entry.h"
#include "logging_helper.h"
#include "snapshot_impl.h"
#include "sortdb_wrapper.h"
#include "sorting_context.h"
#include "wal_sync/wal_history.h"
namespace  {
using namespace limestone;
using namespace limestone::internal;

constexpr std::size_t write_version_size = sizeof(epoch_id_type) + sizeof(std::uint64_t);
static_assert(write_version_size == 16);

[[maybe_unused]]
void store_bswap64_value(void *dest, const void *src) {
    auto* p64_dest = reinterpret_cast<std::uint64_t*>(dest);  // NOLINT(*-reinterpret-cast)
    auto* p64_src = reinterpret_cast<const std::uint64_t*>(src);  // NOLINT(*-reinterpret-cast)
    *p64_dest = __bswap_64(*p64_src);
}

[[maybe_unused]]
int comp_twisted_key(const std::string_view& a, const std::string_view& b) {
    std::size_t a_strlen = a.size() - write_version_size;
    std::size_t b_strlen = b.size() - write_version_size;
    std::string_view a_str(a.data() + write_version_size, a_strlen);
    std::string_view b_str(b.data() + write_version_size, b_strlen);
    if (int c = a_str.compare(b_str); c != 0) return c;
    return std::memcmp(b.data(), a.data(), write_version_size);
}

[[maybe_unused]]
void insert_entry_or_update_to_max(sortdb_wrapper* sortdb, const log_entry& e) {
    bool need_write = true;
    // Skip writing if an older entry is already stored.
    std::string value;
    if (sortdb->get(e.key_sid(), &value)) {
        write_version_type stored_write_version;
        if (e.type() == log_entry::entry_type::normal_with_blob) {
            // For normal_with_blob, the stored format is:
            // [0]: entry_type, [1,8]: value_size, [9, ...]: value_etc (which starts with write_version)
            stored_write_version = write_version_type(value.substr(1 + sizeof(std::size_t)));
        } else {
            // For non-normal_with_blob, the stored format is:
            // [0]: entry_type, [1, ...]: value_etc (which starts with write_version)
            stored_write_version = write_version_type(value.substr(1));
        }
        
        write_version_type new_write_version;
        e.write_version(new_write_version);
        if (new_write_version < stored_write_version) {
            need_write = false;
        }
    }
    if (need_write) {
        std::string db_value;
        db_value.push_back(static_cast<char>(e.type()));
        if (e.type() == log_entry::entry_type::normal_with_blob) {
            // For normal_with_blob entries, insert an 8-byte value_size field
            // (no endian conversion required because only the writing process will read it),
            // followed by the value data and then the BLOB ID data.
            // This allows later splitting the stored data into the value part and the blob IDs.
            std::size_t value_size = e.value_etc().size();
            db_value.append(reinterpret_cast<const char*>(&value_size), sizeof(value_size));  // NOLINT(*-reinterpret-cast)
            db_value.append(e.value_etc());
            db_value.append(e.raw_blob_ids());
        } else {
            db_value.append(e.value_etc());
        }
        sortdb->put(e.key_sid(), db_value);
    }
}



[[maybe_unused]]
void insert_twisted_entry(sortdb_wrapper* sortdb, const log_entry& e) {
    // key_sid: storage_id[8] key[*], value_etc: epoch[8]LE minor_version[8]LE value[*], type: type[1]
    // db_key: epoch[8]BE minor_version[8]BE storage_id[8] key[*], db_value: type[1] value[*]
    std::string db_key(write_version_size + e.key_sid().size(), '\0');
    store_bswap64_value(&db_key[0], &e.value_etc()[0]);  // NOLINT(readability-container-data-pointer)
    store_bswap64_value(&db_key[8], &e.value_etc()[8]);
    std::memcpy(&db_key[write_version_size], e.key_sid().data(), e.key_sid().size());
    std::string value = e.value_etc().substr(write_version_size);
    std::string db_value(1, static_cast<char>(e.type()));
    if (e.type() == log_entry::entry_type::normal_with_blob) {
        std::size_t value_size = value.size();
        std::size_t value_size_le = htole64(value_size);
        db_value.append(reinterpret_cast<const char*>(&value_size_le), sizeof(value_size_le));  // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)

        db_value.append(value);
        db_value.append(e.raw_blob_ids());
    } else {
        db_value.append(value);
    }
    sortdb->put(db_key, db_value);
}

std::pair<epoch_id_type, sorting_context> create_sorted_from_wals(compaction_options &options) {
    auto from_dir = options.get_from_dir();
    auto file_names = options.get_file_names();
    auto num_worker = options.get_num_worker();
#if defined SORT_METHOD_PUT_ONLY
    sorting_context sctx{std::make_unique<sortdb_wrapper>(from_dir, comp_twisted_key)};
#else
    sorting_context sctx{std::make_unique<sortdb_wrapper>(from_dir)};
#endif
    dblog_scan logscan = file_names.empty() ? dblog_scan{from_dir} : dblog_scan{from_dir, options};

    epoch_id_type ld_epoch = logscan.last_durable_epoch_in_dir();

#if defined SORT_METHOD_PUT_ONLY
    const auto add_entry_to_point = insert_twisted_entry;
    bool works_with_multi_thread = true;
#else
    const auto add_entry_to_point = insert_entry_or_update_to_max;
    bool works_with_multi_thread = false;
#endif
    auto add_entry = [&sctx, &add_entry_to_point, &options](const log_entry& e){
        switch (e.type()) {
        case log_entry::entry_type::normal_with_blob:
            if (options.is_gc_enabled()) {
                options.get_gc_snapshot().sanitize_and_add_entry(e);
            }
            add_entry_to_point(sctx.get_sortdb(), e);
            break;
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
        THROW_LIMESTONE_EXCEPTION("dblogdir is corrupted");
    }
}


[[maybe_unused]]
write_version_type extract_write_version(const std::string_view& db_key) {
    std::string wv(write_version_size, '\0');
    store_bswap64_value(&wv[0], &db_key[0]);  // NOLINT(readability-container-data-pointer)
    store_bswap64_value(&wv[8], &db_key[8]); 
    return write_version_type{wv};
}

[[maybe_unused]]
std::string create_value_from_db_key_and_value(const std::string_view& db_key, const std::string_view& db_value) {
    std::string value(write_version_size + db_value.size() - 1, '\0');
    store_bswap64_value(&value[0], &db_key[0]);  // NOLINT(readability-container-data-pointer)
    store_bswap64_value(&value[8], &db_key[8]);
    std::memcpy(&value[write_version_size], &db_value[1], db_value.size() - 1);
    return value;
}

std::pair<std::string, std::string_view> split_db_value_and_blob_ids(const std::string_view raw_db_value) {

#if defined SORT_METHOD_PUT_ONLY
    // The first byte is entry_type
    const char entry_type = raw_db_value[0];
    const std::string_view remaining_data = raw_db_value.substr(1);

    // Retrieve value_size
    std::size_t value_size = le64toh(*reinterpret_cast<const std::size_t*>(remaining_data.data()));  // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)

    // Split value_data and blob_ids_part
    std::string_view value_data = remaining_data.substr(sizeof(std::size_t), value_size);
    std::string_view blob_ids_part = remaining_data.substr(sizeof(std::size_t) + value_size);

    // Calculate the required size and create a buffer
    std::string combined_value(value_size + 1, '\0'); // entry_type (1 byte) + value_data (variable length)

    // Copy data
    combined_value[0] = entry_type; // Copy entry_type to the beginning
    std::memcpy(&combined_value[1], value_data.data(), value_data.size()); // Copy the remaining data

    return {combined_value, blob_ids_part};
#else
    // Layout of raw_db_value for normal_with_blob entries:
    // [0]                     : entry_type (1 byte)
    // [1, 8]                  : value_etc size (8 bytes, stored directly)
    // [9, 9 + value_etc_size) : value_etc (value_etc_size bytes)
    // [9 + value_etc_size, ...) : blob_ids

    // 1. Extract value_etc size from the 8 bytes following the entry_type.
    // No endian conversion is required.
    const std::size_t value_etc_size = *reinterpret_cast<const std::size_t*>(raw_db_value.data() + 1);  // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)

    // 2. The value_etc data starts after the entry_type and size field.
    // The size field occupies 8 bytes, so the offset is 1 + 8 = 9.
    const std::size_t value_etc_offset = 1 + sizeof(std::size_t);

    // 3. Create a string to hold the value_etc data.
    std::string value_etc(value_etc_size, '\0');

    // Copy value_etc data from raw_db_value starting at value_etc_offset for value_etc_size bytes.
    std::memcpy(value_etc.data(), raw_db_value.data() + value_etc_offset, value_etc_size);

    // 4. The blob_ids part is the remainder of raw_db_value after value_etc.
    const std::size_t blob_ids_offset = value_etc_offset + value_etc_size;
    std::string_view blob_ids_part(raw_db_value.data() + blob_ids_offset,
                                   raw_db_value.size() - blob_ids_offset);

    return {value_etc, blob_ids_part};

#endif

}









void sortdb_foreach(
    [[maybe_unused]]  compaction_options &options,
    sorting_context& sctx,
    const std::function<void(
        const log_entry::entry_type entry_type,
        const std::string_view key_sid,
        const std::string_view value_etc,
        const std::string_view blob_ids
    )>& write_snapshot_entry) {
    static_assert(sizeof(log_entry::entry_type) == 1);
#if defined SORT_METHOD_PUT_ONLY
    sctx.get_sortdb()->each([&sctx, &write_snapshot_entry, last_key = std::string{}](const std::string_view db_key, const std::string_view db_value) mutable {
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
            case log_entry::entry_type::remove_entry:
                write_snapshot_entry(entry_type, key, create_value_from_db_key_and_value(db_key, db_value), {});
                break;
            case log_entry::entry_type::normal_with_blob: {
                auto [db_value_without_blob_ids, blob_ids] = split_db_value_and_blob_ids(db_value);
                write_snapshot_entry(entry_type, key, create_value_from_db_key_and_value(db_key, db_value_without_blob_ids), blob_ids);
                sctx.update_max_blob_id(log_entry::parse_blob_ids(blob_ids));
                break;
            }
           default:
                LOG(ERROR) << "never reach " << static_cast<int>(entry_type);
                std::abort();
        }
    });
#else
    sctx.get_sortdb()->each([&sctx, &write_snapshot_entry](const std::string_view db_key, const std::string_view db_value) {
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
            case log_entry::entry_type::remove_entry:
                write_snapshot_entry(entry_type, db_key, db_value.substr(1), {});
                break;
            case log_entry::entry_type::normal_with_blob: {
                auto [value_etc, blob_ids] = split_db_value_and_blob_ids(db_value);
                write_snapshot_entry(entry_type, db_key, value_etc, blob_ids);
                sctx.update_max_blob_id(log_entry::parse_blob_ids(blob_ids));
                break;
            } break;
            default:
                LOG(ERROR) << "never reach " << static_cast<int>(entry_type);
                std::abort();
        }
    });
#endif
}

}  // namespace

namespace limestone::internal {

blob_id_type create_compact_pwal_and_get_max_blob_id(compaction_options &options) {
    auto [max_appeared_epoch, sctx] = create_sorted_from_wals(options);

    boost::system::error_code error;
    const auto &to_dir = options.get_to_dir();
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

    auto write_snapshot_entry = [&ostrm, rewind](
        log_entry::entry_type entry_type, 
        std::string_view key_sid, 
        std::string_view value_etc, 
                                                        std::string_view blob_ids) {
        switch (entry_type) {
            case log_entry::entry_type::normal_entry:
                if (rewind) {
                    static std::string value{};
                    value = value_etc;
                    std::memset(value.data(), 0, 16);
                    log_entry::write(ostrm, key_sid, value);
                } else {
                log_entry::write(ostrm, key_sid, value_etc);
                }
                break;
            case log_entry::entry_type::normal_with_blob:
                if (rewind) {
                    static std::string value{};
                    value = value_etc;
                    std::memset(value.data(), 0, 16);
                    log_entry::write_with_blob(ostrm, key_sid, value, blob_ids);
                } else {
                log_entry::write_with_blob(ostrm, key_sid, value_etc, blob_ids);
                }
                break;
            case log_entry::entry_type::remove_entry:
                // No action needed
                break;
            default:
                LOG(ERROR) << "Unexpected entry type: " << static_cast<int>(entry_type);
                std::abort();
        }
    };
    

    sortdb_foreach(options, sctx, write_snapshot_entry);
    //log_entry::end_session(ostrm, epoch);
    if (fclose(ostrm) != 0) {  // NOLINT(*-owning-memory)
        LOG_AND_THROW_IO_EXCEPTION("cannot close snapshot file (" + snapshot_file.string() + ")", errno);
    }

    return sctx.get_max_blob_id();
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

std::set<boost::filesystem::path> filter_epoch_files(const boost::filesystem::path& directory) {
    std::set<boost::filesystem::path> epoch_files;
    for (const auto& entry : boost::filesystem::directory_iterator(directory)) {
        if (entry.path().filename().string().rfind(epoch_file_name, 0) == 0) { 
            epoch_files.insert(entry.path());
        }
    }
    return epoch_files;
}

void cleanup_rotated_epoch_files(const boost::filesystem::path& directory) {
    // Retrieve all epoch files in the directory
    std::set<boost::filesystem::path> epoch_files = filter_epoch_files(directory);

    // Define the main epoch file path
    boost::filesystem::path main_epoch_file = directory / std::string(epoch_file_name);

    // Check if the main epoch file exists among the filtered files
    auto main_file_it = epoch_files.find(main_epoch_file);
    if (main_file_it == epoch_files.end()) {
        LOG_AND_THROW_EXCEPTION("Epoch file does not exist: " + main_epoch_file.string());
    }

    // Remove the main epoch file from the set of epoch files
    epoch_files.erase(main_file_it);

    // Iterate through the remaining epoch files and remove them
    for (const auto& file : epoch_files) {
        boost::system::error_code ec;
        boost::filesystem::remove(file, ec);
        if (ec) {
            LOG_AND_THROW_IO_EXCEPTION("Failed to remove file: " + file.string() + ". Error: ", ec);
        }
    }
}

} // namespace limestone::internal

namespace limestone::api {
using namespace limestone::internal;
 
snapshot::~snapshot() = default;

blob_id_type datastore::create_snapshot_and_get_max_blob_id() {
    const auto& from_dir = location_;
    std::set<std::string> file_names = assemble_snapshot_input_filenames(compaction_catalog_, from_dir);
    compaction_options options(from_dir, recover_max_parallelism_, file_names);
    auto [max_appeared_epoch, sctx] = create_sorted_from_wals(options);
    epoch_id_switched_.store(max_appeared_epoch);
    epoch_id_informed_.store(max_appeared_epoch);
    impl_->set_boot_durable_epoch_id(max_appeared_epoch);
    wal_history wal_history_(location_);
    wal_history_.append(max_appeared_epoch);
    add_file(wal_history_.get_file_path());

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

    const bool should_write_remove_entry = !compaction_catalog_->get_compacted_files().empty();
    auto write_snapshot_entry = [&ostrm, should_write_remove_entry](
        log_entry::entry_type entry_type, 
        std::string_view key_sid, 
        std::string_view value_etc, 
        std::string_view blob_ids) {
        switch (entry_type) {
        case log_entry::entry_type::normal_entry:
            log_entry::write(ostrm, key_sid, value_etc);
            break;
        case log_entry::entry_type::normal_with_blob:
            log_entry::write_with_blob(ostrm, key_sid, value_etc, blob_ids);
            break;
        case log_entry::entry_type::remove_entry:
            if (should_write_remove_entry) {
                log_entry::write_remove(ostrm, key_sid, value_etc);
            }
            break;
        default:
            LOG(ERROR) << "Unexpected entry type: " << static_cast<int>(entry_type);
            std::abort();
        }
    };

    sortdb_foreach(options, sctx, write_snapshot_entry);
    if (fclose(ostrm) != 0) {  // NOLINT(*-owning-memory)
        LOG_AND_THROW_IO_EXCEPTION("cannot close snapshot file (" + snapshot_file.string() + ")", errno);
    }

    clear_storage = sctx.get_clear_storage();

    return sctx.get_max_blob_id();
}

} // namespace limestone::api
