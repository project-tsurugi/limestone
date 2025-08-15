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

#include "wal_history.h"

#include <endian.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <boost/filesystem.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <fstream>
#include <stdexcept>
#include <vector>
#include <random>

#include "limestone_exception_helper.h"

namespace limestone::internal {

namespace {
    constexpr std::size_t epoch_offset      = 0;
    constexpr std::size_t epoch_size        = sizeof(std::uint64_t);
    constexpr std::size_t identity_offset   = epoch_offset + epoch_size;
    constexpr std::size_t identity_size     = sizeof(uint64_t);
    constexpr std::size_t timestamp_offset  = identity_offset + identity_size;
    constexpr std::size_t timestamp_size    = sizeof(std::uint64_t);
}

void wal_history::write_record(FILE* fp,
                                epoch_id_type epoch,
                                uint64_t identity,
                                std::int64_t timestamp) {
    std::array<std::byte, record_size> buf{};
    const auto be_epoch = htobe64(static_cast<std::uint64_t>(epoch));
    std::memcpy(&buf[epoch_offset], &be_epoch, epoch_size);
    const auto be_identity = htobe64(identity);
    std::memcpy(&buf[identity_offset], &be_identity, identity_size);
    const auto be_timestamp = htobe64(static_cast<std::uint64_t>(timestamp));
    std::memcpy(&buf[timestamp_offset], &be_timestamp, timestamp_size);
    auto cur = buf.cbegin();
    const auto end = buf.cend();
    while (cur != end) {
        const auto remaining = static_cast<size_t>(std::distance(cur, end));
        size_t n = file_ops_->fwrite(static_cast<const void*>(std::addressof(*cur)), 1, remaining, fp);
        if (n == 0) {
            int err = errno;
            LOG_AND_THROW_IO_EXCEPTION("Failed to write wal_history record", err);
        }
        std::advance(cur, static_cast<std::ptrdiff_t>(n));
    }
}


wal_history::record wal_history::parse_record(const std::array<std::byte, record_size>& buf) {
    record rec{};

    std::uint64_t be_epoch = 0;
    std::memcpy(&be_epoch, &buf[epoch_offset], epoch_size);
    rec.epoch = be64toh(be_epoch);

    std::uint64_t be_identity = 0;
    std::memcpy(&be_identity, &buf[identity_offset], identity_size);
    rec.identity = be64toh(be_identity);

    std::int64_t be_timestamp = 0;
    std::memcpy(&be_timestamp, &buf[timestamp_offset], timestamp_size);
    rec.timestamp = be64toh(be_timestamp);

    return rec;
}

std::vector<wal_history::record> wal_history::read_all_records(const boost::filesystem::path& file_path) const {
    std::vector<record> records;
    boost::system::error_code ec;
    bool exists = file_ops_->exists(file_path, ec);
    if (ec) {
        if (ec == boost::system::errc::no_such_file_or_directory || ec == boost::system::errc::not_a_directory) {
            return records;
        }
        LOG_AND_THROW_IO_EXCEPTION("Failed to check existence of wal_history: " + file_path.string(), ec.value());
    }
    if (!exists) {
        return records;
    }
    auto ifs = file_ops_->open_ifstream(file_path.string());
    if (!ifs || !file_ops_->is_open(*ifs)) {
        int err = errno;
        LOG_AND_THROW_IO_EXCEPTION("Failed to open wal_history for read: " + file_path.string(), err);
    }
    std::array<std::byte, record_size> buf{};
    while (true) {
        file_ops_->ifs_read(*ifs, buf.data(), buf.size());
        std::streamsize bytes_read = ifs->gcount();
        if (bytes_read == 0) {
            break; // EOF
        }
        if (bytes_read < static_cast<std::streamsize>(record_size)) {
            LOG_AND_THROW_IO_EXCEPTION("Failed to read wal_history file: partial record read: " + file_path.string(), 0);
        }
        records.push_back(parse_record(buf));
    }
    if (!file_ops_->is_eof(*ifs) && file_ops_->has_error(*ifs)) {
        int err = errno;
        LOG_AND_THROW_IO_EXCEPTION("Failed to read wal_history file: stream error: " + file_path.string(), err);
    }
    return records;
}



wal_history::wal_history(boost::filesystem::path dir_path) noexcept
    : dir_path_(std::move(dir_path)) {}


boost::filesystem::path wal_history::get_file_path() const noexcept {
    return dir_path_ / file_name_;
}
    
void wal_history::append(epoch_id_type epoch) {
    boost::filesystem::path file_path = dir_path_ / file_name_;
    boost::filesystem::path tmp_path = dir_path_ / tmp_file_name_;
    std::vector<record> records = read_all_records(file_path);
    // Add a new record
    std::random_device rd;
    boost::uuids::random_generator uuid_gen;
    boost::uuids::uuid uuid = uuid_gen();
    // Use the first 8 bytes of the UUID as the identity
    uint64_t identity = 0;
    for (int i = 0; i < 8; ++i) {
        identity = (identity << 8) | uuid.data[i];
    }
    auto timestamp = static_cast<std::int64_t>(std::time(nullptr));
    records.push_back(record{epoch, identity, timestamp});
    // Write to temporary file
    {
        FILE* fp = file_ops_->fopen(tmp_path.string().c_str(), "wb");
        int err = errno;
        if (!fp) {
            LOG_AND_THROW_IO_EXCEPTION("Failed to open wal_history.tmp for write: " + tmp_path.string(), err);
        }
        for (const auto& rec : records) {
            write_record(fp, rec.epoch, rec.identity, rec.timestamp);
        }
        if (file_ops_->fflush(fp) != 0) {
            int err = errno;
            file_ops_->fclose(fp);
            LOG_AND_THROW_IO_EXCEPTION("Failed to flush wal_history.tmp: " + tmp_path.string(), err);
        }
        int fd = file_ops_->fileno(fp);
        if (fd < 0) {
            int err = errno;
            file_ops_->fclose(fp);
            LOG_AND_THROW_IO_EXCEPTION("Failed to get file descriptor for wal_history.tmp: " + tmp_path.string(), err);
        }
        if (file_ops_->fsync(fd) != 0) {
            int err = errno;
            file_ops_->fclose(fp);
            LOG_AND_THROW_IO_EXCEPTION("Failed to fsync wal_history.tmp: " + tmp_path.string(), err);
        }
        if (file_ops_->fclose(fp) != 0) {
            int err = errno;
            LOG_AND_THROW_IO_EXCEPTION("Failed to close wal_history.tmp: " + tmp_path.string(), err);
        }
    }
    boost::system::error_code ec;
    file_ops_->rename(tmp_path, file_path, ec);
    if (ec) {
        LOG_AND_THROW_IO_EXCEPTION("Failed to rename wal_history.tmp to wal_history: " + tmp_path.string() + " -> " + file_path.string(), ec.value());
    }
}

std::vector<wal_history::record> wal_history::list() const {
    boost::filesystem::path file_path = dir_path_ / file_name_;
    return read_all_records(file_path);
}

void wal_history::check_and_recover() {
    boost::filesystem::path file_path = dir_path_ / file_name_;
    boost::filesystem::path tmp_path = dir_path_ / tmp_file_name_;
    boost::system::error_code ec;
    bool has_main = file_ops_->exists(file_path, ec);
    bool has_tmp = file_ops_->exists(tmp_path, ec);

    if (has_main && has_tmp) {
        // Pattern 4: Both exist. Rollback: remove .tmp
        if (file_ops_->unlink(tmp_path.c_str()) != 0) {
            int err = errno;
            LOG_AND_THROW_IO_EXCEPTION("Failed to remove wal_history.tmp during recovery: " + tmp_path.string(), err);
        }
    } else if (!has_main && has_tmp) {
        // Pattern 1: Only .tmp exists. Try to recover by renaming .tmp to main
        if (file_ops_->rename(tmp_path.c_str(), file_path.c_str()) != 0) {
            int err = errno;
            LOG_AND_THROW_IO_EXCEPTION("Failed to recover wal_history from wal_history.tmp: " + tmp_path.string() + " -> " + file_path.string(), err);
        }
    }
    // Pattern 2 (only main) and 3 (neither) are normal, do nothing
}

bool wal_history::exists() const {
    boost::filesystem::path file_path = dir_path_ / file_name_;
    boost::system::error_code ec;
    return file_ops_->exists(file_path, ec) && !ec;
}

const char* wal_history::file_name() noexcept {
    return file_name_;
}


} // namespace limestone::internal
