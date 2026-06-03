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

#include <rdma/rdma_replication_message_io.h>

#include <algorithm>
#include <iterator>
#include <vector>

#include "replication/opened_blob_file.h"
#include "replication/primitive_wire_codec.h"
#include "limestone_exception_helper.h"

namespace limestone::replication {

rdma_replication_message_io::rdma_replication_message_io(rdma_send_stream_base& rdma_stream, datastore& ds)
    : replication_message_io(std::string{})
    , rdma_stream_(rdma_stream)
    , datastore_(ds)
{}

void rdma_replication_message_io::send_blob(blob_id_type blob_id) {
    auto opened = opened_blob_file::open_for_send(datastore_, blob_id);
    auto remaining = opened.size();

    push_staged_bytes();
    send_blob_header_and_first_chunk(blob_id, opened, remaining);
    send_blob_data(opened, remaining);
}

void rdma_replication_message_io::push_staged_bytes() {
    auto buffered = get_out_string();
    if (buffered.empty()) {
        return;
    }
    std::vector<std::uint8_t> payload(buffered.begin(), buffered.end());
    auto result = rdma_stream_.send_all_bytes(payload, 0, payload.size());
    if (! result.success || result.bytes_written != payload.size()) {
        LOG_AND_THROW_IO_EXCEPTION(
            "RDMA send_all_bytes failed for buffered payload: " + result.error_message, EIO);
    }
    reset_output_buffer();
}

void rdma_replication_message_io::send_blob_header_and_first_chunk(
        blob_id_type blob_id,
        opened_blob_file& blob,
        std::uint32_t& remaining) {
    constexpr std::size_t blob_header_size = sizeof(std::uint64_t) + sizeof(std::uint32_t);
    auto const blob_size = remaining;
    // Require the first frame to hold the blob header together with at least one byte
    // of payload (when any payload exists), so the small-message common case never pays
    // for a header-only frame. An empty blob needs only the header itself.
    auto const min_capacity = blob_size > 0U
        ? blob_header_size + 1U
        : blob_header_size;
    // The writer callback receives the RDMA send buffer allocated by rdma-comm-lib.
    auto result = rdma_stream_.send_with_writer(
        blob_header_size + static_cast<std::size_t>(blob_size),
        [this, blob_id, blob_size, &blob](std::uint8_t* buffer, std::size_t capacity) {
            return fill_blob_header_and_first_chunk(blob_id, blob_size, blob, buffer, capacity);
        },
        min_capacity);
    if (! result.success || result.bytes_written < blob_header_size) {
        LOG_AND_THROW_IO_EXCEPTION(
            "RDMA send_with_writer failed for blob header and first chunk: " + result.error_message, EIO);
    }
    remaining -= static_cast<std::uint32_t>(result.bytes_written - blob_header_size);
}

rdma_send_stream_base::buffer_fill_result rdma_replication_message_io::fill_blob_header_and_first_chunk(
        blob_id_type blob_id,
        std::uint32_t blob_size,
        opened_blob_file& blob,
        std::uint8_t* buffer,
        std::size_t capacity) {
    constexpr std::size_t blob_header_size = sizeof(std::uint64_t) + sizeof(std::uint32_t);
    if (capacity < blob_header_size) {
        return {false, "RDMA send buffer is smaller than blob header"};
    }
    auto const encoded_blob_id = primitive_wire_codec::encode_uint64(blob_id);
    auto const encoded_blob_size = primitive_wire_codec::encode_uint32(blob_size);
    auto out = std::copy(encoded_blob_id.begin(), encoded_blob_id.end(), buffer);
    std::copy(encoded_blob_size.begin(), encoded_blob_size.end(), out);
    auto const payload_capacity = capacity - blob_header_size;
    if (payload_capacity > 0U) {
        // opened_blob_file::read_chunk() returns only after reading the requested length;
        // otherwise it throws, so bytes_read is intentionally not used here.
        [[maybe_unused]] auto const bytes_read =
            blob.read_chunk(std::next(buffer, blob_header_size), payload_capacity);
    }
    return {true, ""};
}

void rdma_replication_message_io::send_blob_data(
        opened_blob_file& blob, std::uint32_t remaining) {
    while (remaining > 0) {
        // The writer callback receives the RDMA send buffer allocated by rdma-comm-lib.
        auto result = rdma_stream_.send_with_writer(
            remaining,
            [this, &blob](std::uint8_t* buffer, std::size_t capacity) {
                return fill_blob_data_chunk(blob, buffer, capacity);
            },
            0);  // no minimum: blob data may be split across frames freely
        if (! result.success || result.bytes_written == 0) {
            LOG_AND_THROW_IO_EXCEPTION(
                "RDMA send_with_writer failed for blob data: " + result.error_message, EIO);
        }
        remaining -= static_cast<std::uint32_t>(result.bytes_written);
    }
}

rdma_send_stream_base::buffer_fill_result rdma_replication_message_io::fill_blob_data_chunk(
        opened_blob_file& blob,
        std::uint8_t* buffer,
        std::size_t capacity) {
    // opened_blob_file::read_chunk() returns only after reading the requested length;
    // otherwise it throws, so bytes_read is intentionally not used here.
    [[maybe_unused]] auto const bytes_read =
        blob.read_chunk(buffer, capacity);
    return {true, ""};
}

} // namespace limestone::replication
