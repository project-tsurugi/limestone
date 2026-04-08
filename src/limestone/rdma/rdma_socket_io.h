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
 * directly via rdma_send_stream_base::send_all_bytes(), avoiding full in-memory buffering.
 *
 * receive_blob() is not supported on this class (FATAL if called); RDMA receive
 * uses blob_socket_io in string mode instead.
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
     * First flushes any accumulated non-blob data from the inherited output buffer,
     * then reads the blob file in blob_buffer_size chunks and sends each chunk via
     * rdma_send_stream_base::send_all_bytes().  The wire format is identical to
     * blob_socket_io::send_blob(): [blob_id: 8B][size: 4B][data: size bytes].
     *
     * @param blob_id ID of the blob to send.
     */
    void send_blob(blob_id_type blob_id) override;

private:
    void safe_close(FILE* fp);

    /**
     * @brief Open a blob file and return its size as uint32_t.
     *        Throws on any I/O error or if the file is too large (> uint32_t max).
     * @param path Resolved filesystem path to the blob file.
     * @param[out] out_size File size in bytes.
     * @return Open FILE pointer; caller must call safe_close() when done.
     */
    FILE* open_blob_file(boost::filesystem::path const& path, uint32_t& out_size);

    /**
     * @brief Read the blob file and send its content in chunks via RDMA.
     * @param fp Open FILE pointer positioned at the beginning of the blob data.
     * @param path Path used only for error messages.
     * @param remaining Total bytes to send.
     */
    void send_blob_data(FILE* fp, boost::filesystem::path const& path, uint32_t remaining);

    rdma_send_stream_base& rdma_stream_;
    datastore& datastore_;
};

} // namespace limestone::replication
