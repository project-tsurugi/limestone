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
#include <array>
#include <cstring>
#include <vector>

#include "replication/blob_send_utils.h"
#include "limestone_exception_helper.h"

namespace limestone::replication {

rdma_socket_io::rdma_socket_io(rdma_send_stream_base& rdma_stream, datastore& ds)
    : socket_io(std::string{})
    , rdma_stream_(rdma_stream)
    , datastore_(ds)
{}

void rdma_socket_io::send_blob(blob_id_type blob_id) {
    auto opened = open_blob_file_for_send(datastore_, blob_id);
    auto remaining = opened.size;

    flush_buffered_bytes();
    send_blob_header_and_first_chunk(blob_id, opened.fp, opened.path, remaining);
    send_blob_data(opened.fp, opened.path, remaining);
    safe_close_blob_file(opened.fp, "fclose failed for blob file");
}

void rdma_socket_io::flush_buffered_bytes() {
    auto buffered = get_out_string();
    if (buffered.empty()) {
        return;
    }
    std::vector<std::uint8_t> payload(buffered.begin(), buffered.end());
    auto result = rdma_stream_.send_all_bytes(payload, 0, payload.size());
    if (! result.success || result.bytes_written != payload.size()) {
        LOG_AND_THROW_IO_EXCEPTION(
            "RDMA send_bytes failed for buffered payload: " + result.error_message, EIO);
    }
    reset_output_buffer();
}

void rdma_socket_io::send_blob_header_and_first_chunk(
        blob_id_type blob_id,
        FILE* fp,
        boost::filesystem::path const& path,
        std::uint32_t& remaining) {
    constexpr std::size_t blob_header_size = sizeof(std::uint64_t) + sizeof(std::uint32_t);
    auto const blob_size = remaining;
    auto result = rdma_stream_.send_with_writer(
        blob_header_size + static_cast<std::size_t>(blob_size),
        [blob_id, blob_size, fp, &path](std::uint8_t* buffer, std::size_t capacity)
            -> rdma_send_stream_base::buffer_fill_result {
            if (capacity < blob_header_size) {
                return {false, "RDMA send buffer is smaller than blob header"};
            }
            socket_io header_io(std::string{});
            header_io.send_uint64(blob_id);
            header_io.send_uint32(blob_size);
            auto encoded = header_io.get_out_string();
            if (encoded.size() != blob_header_size) {
                return {false, "encoded blob header size mismatch"};
            }
            std::memcpy(buffer, encoded.data(), encoded.size());
            auto const payload_capacity = capacity - blob_header_size;
            if (payload_capacity > 0U) {
                [[maybe_unused]] auto const bytes_read = read_blob_chunk(
                    fp,
                    path,
                    reinterpret_cast<char*>(buffer + blob_header_size),
                    payload_capacity);
            }
            return {true, ""};
        });
    if (! result.success || result.bytes_written < blob_header_size) {
        LOG_AND_THROW_IO_EXCEPTION(
            "RDMA send_with_writer failed for blob header and first chunk: " + result.error_message, EIO);
    }
    remaining -= static_cast<std::uint32_t>(result.bytes_written - blob_header_size);
}

void rdma_socket_io::send_blob_data(
        FILE* fp, boost::filesystem::path const& path, std::uint32_t remaining) {
    while (remaining > 0) {
        auto result = rdma_stream_.send_with_writer(
            remaining,
            [fp, &path](std::uint8_t* buffer, std::size_t capacity)
                -> rdma_send_stream_base::buffer_fill_result {
                [[maybe_unused]] auto const bytes_read =
                    read_blob_chunk(fp, path, reinterpret_cast<char*>(buffer), capacity);
                return {true, ""};
            });
        if (! result.success || result.bytes_written == 0) {
            LOG_AND_THROW_IO_EXCEPTION(
                "RDMA send_with_writer failed for blob data: " + result.error_message, EIO);
        }
        remaining -= static_cast<std::uint32_t>(result.bytes_written);
    }
}

} // namespace limestone::replication
