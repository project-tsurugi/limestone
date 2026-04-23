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

#pragma once

#include <cstdio>
#include <vector>

#include <boost/filesystem.hpp>

#include <rdma/rdma_send_stream_base.h>
#include <replication/socket_io.h>
#include <limestone/api/blob_id_type.h>
#include <limestone/api/datastore.h>

namespace limestone::replication {

using limestone::api::datastore;
using limestone::api::blob_id_type;

/**
 * @brief A socket_io subclass for the RDMA send path.
 *
 * Inherits all serialization methods from socket_io (used for non-blob data).
 * Overrides send_blob() to read the blob file in chunks and transmit each chunk
 * directly via rdma_send_stream_base::send_with_writer(), avoiding full in-memory buffering.
 *
 * receive_blob() is not supported on this class (FATAL if called). RDMA receive
 * is handled by rdma_log_entries_receiver / rdma_log_entries_parser, not by
 * socket_io-style deserialization.
 *
 * TODO: For very large BLOBs the send path still buffers the non-blob portion of
 * the message in the inherited socket_io output stream before flushing.  The blob
 * data itself is streamed in blob_buffer_size (64 KB) chunks and therefore does
 * not require full in-memory allocation.
 */
class rdma_socket_io : public socket_io {
public:
    static constexpr std::size_t blob_buffer_size = 64UL * 1024UL;

    rdma_socket_io(const rdma_socket_io&) = delete;
    rdma_socket_io& operator=(const rdma_socket_io&) = delete;
    rdma_socket_io(rdma_socket_io&&) = delete;
    rdma_socket_io& operator=(rdma_socket_io&&) = delete;

    ~rdma_socket_io() override = default;

    /**
     * @brief Construct in string-mode backed by the given RDMA send stream and datastore.
     * @param rdma_stream RDMA send stream used to transmit data.
     * @param ds Datastore used to resolve blob file paths.
     */
    rdma_socket_io(rdma_send_stream_base& rdma_stream, datastore& ds);

    /**
     * @brief Send a blob file via RDMA.
     *
     * First sends any accumulated non-blob data from the inherited output buffer,
     * then fills RDMA send buffers directly from the blob file via
     * rdma_send_stream_base::send_with_writer(). The wire format is identical to
     * blob_socket_io::send_blob(): [blob_id: 8B][size: 4B][data: size bytes].
     *
     * @param blob_id ID of the blob to send.
     * @throws limestone::api::limestone_io_exception if the BLOB file cannot be opened,
     *         read, or sent via the RDMA stream.
     */
    void send_blob(blob_id_type blob_id) override;

private:
    /**
     * @brief Send any accumulated non-BLOB bytes buffered in the inherited output stream.
     * @throws limestone::api::limestone_io_exception if sending staged bytes via the RDMA stream fails.
     */
    void push_staged_bytes();

    /**
     * @brief Send the BLOB header together with the first BLOB data chunk via RDMA.
     * @param blob_id ID of the blob being transmitted.
     * @param fp Open FILE pointer positioned at the beginning of the blob data.
     * @param path Path used only for error messages.
     * @param[in,out] remaining Total bytes left to send; decremented by the first chunk size.
     * @throws limestone::api::limestone_io_exception if sending the first RDMA buffer fails.
     */
    void send_blob_header_and_first_chunk(
        blob_id_type blob_id,
        FILE* fp,
        boost::filesystem::path const& path,
        std::uint32_t& remaining);

    /**
     * @brief Fill an RDMA send buffer with the BLOB header and first BLOB data chunk.
     * @param blob_id ID of the blob being transmitted.
     * @param blob_size Total BLOB size in bytes.
     * @param fp Open FILE pointer positioned at the beginning of the blob data.
     * @param path Path used only for error messages.
     * @param buffer RDMA send buffer to fill.
     * @param capacity Writable size of buffer in bytes.
     * @return Result describing whether the buffer was filled successfully.
     * @throws limestone::api::limestone_io_exception if the BLOB file cannot provide
     *         enough bytes for the first data chunk.
     */
    rdma_send_stream_base::buffer_fill_result fill_blob_header_and_first_chunk(
        blob_id_type blob_id,
        std::uint32_t blob_size,
        FILE* fp,
        boost::filesystem::path const& path,
        std::uint8_t* buffer,
        std::size_t capacity);

    /**
     * @brief Read the blob file and send its remaining content in chunks via RDMA.
     * @param fp Open FILE pointer positioned after the first chunk of blob data.
     * @param path Path used only for error messages.
     * @param remaining Total bytes still to send.
     * @throws limestone::api::limestone_io_exception if sending a BLOB data chunk fails.
     */
    void send_blob_data(FILE* fp, boost::filesystem::path const& path, std::uint32_t remaining);

    /**
     * @brief Fill an RDMA send buffer with BLOB data only.
     * @param fp Open FILE pointer positioned at the next BLOB byte to send.
     * @param path Path used only for error messages.
     * @param buffer RDMA send buffer to fill.
     * @param capacity Writable size of buffer in bytes.
     * @return Result describing whether the buffer was filled successfully.
     * @throws limestone::api::limestone_io_exception if the BLOB file cannot provide
     *         enough bytes for the data chunk.
     */
    rdma_send_stream_base::buffer_fill_result fill_blob_data_chunk(
        FILE* fp,
        boost::filesystem::path const& path,
        std::uint8_t* buffer,
        std::size_t capacity);

    rdma_send_stream_base& rdma_stream_;
    datastore& datastore_;
};

} // namespace limestone::replication
