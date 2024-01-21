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

#include <byteswap.h>
#include <boost/filesystem/fstream.hpp>
#include <cstdlib>
#include <cstring>
#include <mutex>

#include <glog/logging.h>
#include <limestone/logging.h>
#include "logging_helper.h"

#include <limestone/api/datastore.h>
#include "log_entry.h"
#include "sortdb_wrapper.h"

namespace limestone::internal {
using namespace limestone::api;

// return max epoch in file.
std::optional<epoch_id_type> last_durable_epoch(const boost::filesystem::path& file) {
    std::optional<epoch_id_type> rv;

    boost::filesystem::ifstream istrm;
    log_entry e;
    istrm.open(file, std::ios_base::in | std::ios_base::binary);
    while (e.read(istrm)) {
        if (e.type() != log_entry::entry_type::marker_durable) {
            LOG_LP(ERROR) << "this epoch file is broken: unexpected log_entry type: " << static_cast<int>(e.type());
            throw std::runtime_error("unexpected log_entry type for epoch file");
        }
        if (!rv.has_value() || e.epoch_id() > rv) {
            rv = e.epoch_id();
        }
    }
    istrm.close();
    return rv;
}

}

namespace limestone::api {
using namespace limestone::internal;

constexpr std::size_t write_version_size = sizeof(epoch_id_type) + sizeof(std::uint64_t);
static_assert(write_version_size == 16);

epoch_id_type datastore::last_durable_epoch_in_dir() {
    auto& from_dir = location_;
    // read main epoch file first
    std::optional<epoch_id_type> ld_epoch = last_durable_epoch(from_dir / std::string(epoch_file_name));
    if (ld_epoch.has_value()) {
        return *ld_epoch;
    }

    // main epoch file is empty,
    // read all rotated-epoch files
    for (const boost::filesystem::path& p : boost::filesystem::directory_iterator(from_dir)) {
        if (p.filename().string().rfind(epoch_file_name, 0) == 0) {  // starts_with(epoch_file_name)
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

}

namespace limestone::internal {
using namespace limestone::api;

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

epoch_id_type scan_one_pwal_file(const boost::filesystem::path& p, epoch_id_type ld_epoch, const std::function<void(log_entry&)>& add_entry) {
    VLOG_LP(log_info) << "processing pwal file: " << p.filename().string();
    log_entry e;
    epoch_id_type current_epoch{UINT64_MAX};
    epoch_id_type max_epoch_of_file{0};

    boost::filesystem::fstream strm;
    strm.open(p, std::ios_base::in | std::ios_base::out | std::ios_base::binary);
    bool skipping = false;  // scanning in the invalidated epoch snippet
    while (e.read(strm)) {
        switch (e.type()) {
        case log_entry::entry_type::marker_begin: {
            current_epoch = e.epoch_id();
            max_epoch_of_file = std::max(max_epoch_of_file, current_epoch);
            if (current_epoch <= ld_epoch) {
                skipping = false;
            } else {
                auto pos = strm.tellg();
                strm.seekp(-9, std::ios::cur);  // size of marker_begin entry
                char buf = static_cast<char>(log_entry::entry_type::marker_invalidated_begin);
                strm.write(&buf, sizeof(char));
                strm.flush();
                strm.seekg(pos, std::ios::beg);  // restore position
                skipping = true;
            }
            break;
        }
        case log_entry::entry_type::marker_invalidated_begin: {
            max_epoch_of_file = std::max(max_epoch_of_file, e.epoch_id());
            skipping = true;
            break;
        }
        case log_entry::entry_type::normal_entry:
        case log_entry::entry_type::remove_entry: {
            if (!skipping) {
                add_entry(e);
            }
            break;
        }
        default:
            break;
        }
    }
    strm.close();
    return max_epoch_of_file;
}

epoch_id_type scan_pwal_files_in_dir(const boost::filesystem::path& from_dir, int num_worker,
                                     const std::function<bool(const boost::filesystem::path&)>& is_wal,
                                     epoch_id_type ld_epoch, const std::function<void(log_entry&)>& add_entry) {
    std::atomic<epoch_id_type> max_appeared_epoch{ld_epoch};
    auto process_file = [&](const boost::filesystem::path& p) {
        if (is_wal(p)) {
            epoch_id_type max_epoch_of_file = scan_one_pwal_file(p, ld_epoch, add_entry);
            epoch_id_type t = max_appeared_epoch.load();
            while (t < max_epoch_of_file
                   && !max_appeared_epoch.compare_exchange_weak(t, max_epoch_of_file)) {
                /* nop */
            }
        }
    };
    std::mutex dir_mtx;
    auto dir_begin = boost::filesystem::directory_iterator(from_dir);
    auto dir_end = boost::filesystem::directory_iterator();
    std::vector<std::thread> workers;
    std::mutex ex_mtx;
    std::exception_ptr ex_ptr{};
    workers.reserve(num_worker);
    for (int i = 0; i < num_worker; i++) {
        workers.emplace_back(std::thread([&](){
            for (;;) {
                boost::filesystem::path p;
                {
                    std::lock_guard<std::mutex> g{dir_mtx};
                    if (dir_begin == dir_end) break;
                    p = *dir_begin++;
                }
                try {
                    process_file(p);
                } catch (std::runtime_error& ex) {
                    VLOG(log_info) << "/:limestone catch runtime_error(" << ex.what() << ")";
                    std::lock_guard<std::mutex> g2{ex_mtx};
                    if (!ex_ptr) {  // only save one
                        ex_ptr = std::current_exception();
                    }
                    std::lock_guard<std::mutex> g{dir_mtx};
                    dir_begin = dir_end;  // skip all unprocessed files
                    break;
                }
            }
        }));
    }
    for (int i = 0; i < num_worker; i++) {
        workers[i].join();
    }
    if (ex_ptr) {
        std::rethrow_exception(ex_ptr);
    }
    return max_appeared_epoch;
}

}

namespace limestone::api {
using namespace limestone::internal;

void datastore::create_snapshot() {
    auto& from_dir = location_;
#if defined SORT_METHOD_PUT_ONLY
    auto sortdb = std::make_unique<sortdb_wrapper>(from_dir, comp_twisted_key);
#else
    auto sortdb = std::make_unique<sortdb_wrapper>(from_dir);
#endif

    epoch_id_type ld_epoch = last_durable_epoch_in_dir();

    [[maybe_unused]]
    auto insert_entry_or_update_to_max = [&sortdb](log_entry& e){
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
    };
    [[maybe_unused]]
    auto insert_twisted_entry = [&sortdb](log_entry& e){
        // key_sid: storage_id[8] key[*], value_etc: epoch[8]LE minor_version[8]LE value[*], type: type[1]
        // db_key: epoch[8]BE minor_version[8]BE storage_id[8] key[*], db_value: type[1] value[*]
        std::string db_key(write_version_size + e.key_sid().size(), '\0');
        store_bswap64_value(&db_key[0], &e.value_etc()[0]);  // NOLINT(readability-container-data-pointer)
        store_bswap64_value(&db_key[8], &e.value_etc()[8]);
        std::memcpy(&db_key[write_version_size], e.key_sid().data(), e.key_sid().size());
        std::string db_value(1, static_cast<char>(e.type()));
        db_value.append(e.value_etc().substr(write_version_size));
        sortdb->put(db_key, db_value);
    };
#if defined SORT_METHOD_PUT_ONLY
    auto add_entry = insert_twisted_entry;
    bool works_with_multi_thread = true;
#else
    auto add_entry = insert_entry_or_update_to_max;
    bool works_with_multi_thread = false;
#endif

    int num_worker = recover_max_parallelism_;
    if (!works_with_multi_thread && num_worker > 1) {
        LOG(INFO) << "/:limestone:config:datastore this sort method does not work correctly with multi-thread, so force the number of recover process thread = 1";
        num_worker = 1;
    }
    auto is_wal = [](const boost::filesystem::path& p){ return p.filename().string().substr(0, log_channel::prefix.length()) == log_channel::prefix; };
    epoch_id_type max_appeared_epoch = scan_pwal_files_in_dir(from_dir, num_worker, is_wal, ld_epoch, add_entry);
    // epoch +1 for margin (probably unnecessary)
    epoch_id_switched_.store(max_appeared_epoch + 1);
    epoch_id_informed_.store(max_appeared_epoch + 1);

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
    static_assert(sizeof(log_entry::entry_type) == 1);
#if defined SORT_METHOD_PUT_ONLY
    sortdb->each([&ostrm, last_key = std::string{}](std::string_view db_key, std::string_view db_value) mutable {
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
            log_entry::write(ostrm, key, value);
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
    sortdb->each([&ostrm](std::string_view db_key, std::string_view db_value) {
        auto entry_type = static_cast<log_entry::entry_type>(db_value[0]);
        db_value.remove_prefix(1);
        switch (entry_type) {
        case log_entry::entry_type::normal_entry:
            log_entry::write(ostrm, db_key, db_value);
            break;
        case log_entry::entry_type::remove_entry:
            break;  // skip
        default:
            LOG(ERROR) << "never reach " << static_cast<int>(entry_type);
            std::abort();
        }
    });
#endif
    if (fclose(ostrm) != 0) {  // NOLINT(*-owning-memory)
        LOG_LP(ERROR) << "cannot close snapshot file (" << snapshot_file << "), errno = " << errno;
        throw std::runtime_error("I/O error");
    }
}

} // namespace limestone::api
