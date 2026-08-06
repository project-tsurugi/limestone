/*
 * Copyright 2022-2026 Project Tsurugi.
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
#include <cstddef>
#include <cstdint>
#include <iterator>

#include "replication/opened_blob_file.h"
#include "replication/primitive_wire_codec.h"
#include "limestone_exception_helper.h"

namespace limestone::replication {

namespace {

/// @brief Wire size of the BLOB header: [blob_id: 8B][size: 4B].
constexpr std::size_t blob_header_size = sizeof(std::uint64_t) + sizeof(std::uint32_t);

} // namespace

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
    auto result = rdma_stream_.send_all_bytes(buffered);
    if (! result.success) {
        LOG_AND_THROW_IO_EXCEPTION(
            "failed to send the buffered payload over RDMA: " + result.error_message, EIO);
    }
    reset_output_buffer();
}

void rdma_replication_message_io::send_blob_header_and_first_chunk(
        blob_id_type blob_id,
        opened_blob_file& blob,
        std::uint32_t& remaining) {
    auto const blob_size = remaining;
    auto const max_payload = blob_header_size + static_cast<std::size_t>(blob_size);
    // Require the whole header in this frame so it is never split across frames, plus one
    // data byte when there is data to send. An empty BLOB needs the header alone.
    auto const min_capacity = (blob_size == 0)
        ? blob_header_size
        : blob_header_size + 1;
    auto frame = rdma_stream_.acquire_frame_buffer(max_payload, min_capacity);
    if (! frame) {
        LOG_AND_THROW_IO_EXCEPTION(
            "failed to acquire an RDMA frame buffer for the blob header", EIO);
    }

    auto const encoded_blob_id = primitive_wire_codec::encode_uint64(blob_id);
    auto const encoded_blob_size = primitive_wire_codec::encode_uint32(blob_size);
    auto* out = std::copy(encoded_blob_id.begin(), encoded_blob_id.end(), frame->payload());
    std::copy(encoded_blob_size.begin(), encoded_blob_size.end(), out);

    // The granted capacity can exceed the requested max_payload: the send ring hands out
    // whole slots, so a small request rounds up to a full slot. Clamp to the bytes that
    // actually exist, or read_chunk() would run past the end of the BLOB.
    auto const chunk = std::min(frame->capacity() - blob_header_size,
                                static_cast<std::size_t>(blob_size));
    if (chunk > 0) {
        // opened_blob_file::read_chunk() returns only after reading the requested length;
        // otherwise it throws, so bytes_read is intentionally not used here. The blob data
        // lands directly in the RDMA send buffer, with no intermediate copy.
        [[maybe_unused]] auto const bytes_read = blob.read_chunk(
            std::next(frame->payload(), blob_header_size), chunk);
    }

    auto result = rdma_stream_.submit_frame_buffer(*frame, blob_header_size + chunk);
    if (! result.success) {
        LOG_AND_THROW_IO_EXCEPTION(
            "failed to submit the RDMA frame for the blob header: " + result.error_message, EIO);
    }
    remaining -= static_cast<std::uint32_t>(chunk);
}

void rdma_replication_message_io::send_blob_data(
        opened_blob_file& blob, std::uint32_t remaining) {
    while (remaining > 0) {
        auto frame = rdma_stream_.acquire_frame_buffer(remaining, 1);
        if (! frame) {
            LOG_AND_THROW_IO_EXCEPTION(
                "failed to acquire an RDMA frame buffer for the blob data", EIO);
        }
        auto const chunk = std::min(frame->capacity(), static_cast<std::size_t>(remaining));
        // As above: read_chunk() throws on a short read, and writes straight into the
        // RDMA send buffer.
        [[maybe_unused]] auto const bytes_read = blob.read_chunk(frame->payload(), chunk);
        auto result = rdma_stream_.submit_frame_buffer(*frame, chunk);
        if (! result.success) {
            LOG_AND_THROW_IO_EXCEPTION(
                "failed to submit an RDMA frame for the blob data: " + result.error_message, EIO);
        }
        remaining -= static_cast<std::uint32_t>(chunk);
    }
}

} // namespace limestone::replication
