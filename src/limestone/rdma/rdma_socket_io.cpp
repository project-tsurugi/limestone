/*
 * Copyright 2022-2025 Project Tsurugi.
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

#include <rdma/rdma_socket_io.h>

#include <algorithm>
#include <cstdio>
#include <limits>
#include <vector>
#include <boost/filesystem.hpp>

#include "limestone_exception_helper.h"

namespace limestone::replication {

rdma_socket_io::rdma_socket_io(rdma_send_stream_base& rdma_stream, datastore& ds)
    : socket_io(std::string{})
    , rdma_stream_(rdma_stream)
    , datastore_(ds)
{}

// TODO: The blob file open/size-check/chunk-read logic in open_blob_file() and
//       send_blob_data() is duplicated in blob_socket_io::send_blob. Consider
//       extracting a shared helper.
void rdma_socket_io::send_blob(blob_id_type blob_id) {
    auto blob_file = datastore_.get_blob_file(blob_id);
    auto path = blob_file.path();
    auto status = boost::filesystem::symlink_status(path);
    if (boost::filesystem::is_symlink(status)) {
        path = boost::filesystem::canonical(path);
        status = boost::filesystem::status(path);
    }
    if (! boost::filesystem::is_regular_file(status)) {
        LOG_AND_THROW_IO_EXCEPTION("Unsupported blob path type: " + path.string(), errno);
    }

    uint32_t remaining = 0;
    FILE* fp = open_blob_file(path, remaining);

    // First flush any non-blob data buffered in the inherited output stream,
    // then send the blob header (blob_id + size) as part of the same flush.
    send_uint64(blob_id);
    send_uint32(remaining);
    // Flush the header (and any preceding serialized data) via RDMA.
    auto header_str = get_out_string();
    if (! header_str.empty()) {
        std::vector<std::uint8_t> header_bytes(header_str.begin(), header_str.end());
        auto result = rdma_stream_.send_all_bytes(header_bytes, 0, header_bytes.size());
        if (! result.success) {
            safe_close(fp);
            LOG_AND_THROW_IO_EXCEPTION("RDMA send_all_bytes failed for blob header: " + result.error_message, EIO);
        }
        reset_output_buffer();
    }

    send_blob_data(fp, path, remaining);
    safe_close(fp);
}

FILE* rdma_socket_io::open_blob_file(boost::filesystem::path const& path, uint32_t& out_size) {
    FILE* fp = std::fopen(path.string().c_str(), "rb");  // NOLINT(cppcoreguidelines-owning-memory)
    if (! fp) {
        LOG_AND_THROW_IO_EXCEPTION("Failed to open blob for reading: " + path.string(), errno);
    }
    if (std::fseek(fp, 0, SEEK_END) != 0) {
        int ec = errno;
        safe_close(fp);
        LOG_AND_THROW_IO_EXCEPTION("Failed to seek blob file: " + path.string(), ec);
    }
    // std::ftell returns long; use auto to match the return type exactly.
    // Note: this implementation intentionally limits blob file size to uint32_t::max
    // (4 GiB - 1) so the transferred size always fits in the 32-bit protocol field.
    auto pos = std::ftell(fp);
    if (pos == -1) {
        int ec = errno;
        safe_close(fp);
        LOG_AND_THROW_IO_EXCEPTION("Failed to tell blob file: " + path.string(), ec);
    }
    if (static_cast<uint64_t>(pos) > std::numeric_limits<uint32_t>::max()) {
        safe_close(fp);
        LOG_AND_THROW_IO_EXCEPTION("Blob file too large: " + path.string(), EIO);
    }
    out_size = static_cast<uint32_t>(pos);
    if (std::fseek(fp, 0, SEEK_SET) != 0) {
        int ec = errno;
        safe_close(fp);
        LOG_AND_THROW_IO_EXCEPTION("Failed to rewind blob file: " + path.string(), ec);
    }
    return fp;
}

void rdma_socket_io::send_blob_data(
        FILE* fp, boost::filesystem::path const& path, uint32_t remaining) {
    // Send blob data in chunks directly via RDMA (no full in-memory copy).
    std::vector<std::uint8_t> buffer(blob_buffer_size);
    while (remaining > 0) {
        std::size_t chunk = std::min(blob_buffer_size, static_cast<std::size_t>(remaining));
        std::size_t total_read = 0;
        while (total_read < chunk) {
            std::size_t r = std::fread(
                &buffer[total_read], 1, chunk - total_read, fp);
            if (r == 0) {
                // Capture errno immediately after fread before any other call may clobber it.
                int ec = errno;
                if (ec == EINTR) {
                    std::clearerr(fp);
                    continue;
                }
                if (std::feof(fp) != 0) {
                    safe_close(fp);
                    LOG_AND_THROW_IO_EXCEPTION("Unexpected EOF reading blob: " + path.string(), ec);
                }
                safe_close(fp);
                LOG_AND_THROW_IO_EXCEPTION("Failed to read blob chunk: " + path.string(), ec);
            }
            total_read += r;
        }
        auto result = rdma_stream_.send_all_bytes(buffer, 0, total_read);
        if (! result.success) {
            safe_close(fp);
            LOG_AND_THROW_IO_EXCEPTION("RDMA send_all_bytes failed for blob data: " + result.error_message, EIO);
        }
        remaining -= static_cast<uint32_t>(total_read);
    }
}

void rdma_socket_io::safe_close(FILE* fp) {
    if (fp) {
        int ret = std::fclose(fp);  // NOLINT(cppcoreguidelines-owning-memory)
        if (ret != 0) {
            LOG_LP(WARNING) << "fclose failed for blob file: " << strerror(errno);
        }
    }
}

} // namespace limestone::replication
