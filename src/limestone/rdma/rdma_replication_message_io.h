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

#include <vector>

#include <rdma/rdma_send_stream_base.h>
#include <replication/opened_blob_file.h>
#include <replication/replication_message_io.h>
#include <limestone/api/blob_id_type.h>
#include <limestone/api/datastore.h>

namespace limestone::replication {

using limestone::api::datastore;
using limestone::api::blob_id_type;

/**
 * @brief A replication_message_io subclass for the RDMA send path.
 *
 * Inherits all serialization methods from replication_message_io (used for non-blob data).
 * Overrides send_blob() to read the blob file straight into RDMA send frames acquired
 * from rdma_send_stream_base, avoiding full in-memory buffering.
 *
 * receive_blob() is not supported on this class (FATAL if called). RDMA receive
 * is handled by rdma_log_entries_receiver / rdma_log_entries_parser, not by
 * replication_message_io-style deserialization.
 *
 * TODO: For very large BLOBs the send path still buffers the non-blob portion of
 * the message in the inherited replication_message_io output stream before flushing.  The blob
 * data itself is read straight into RDMA send frames, one acquire_frame_buffer() grant at
 * a time, and therefore does not require full in-memory allocation.
 */
class rdma_replication_message_io : public replication_message_io {
public:
    rdma_replication_message_io(const rdma_replication_message_io&) = delete;
    rdma_replication_message_io& operator=(const rdma_replication_message_io&) = delete;
    rdma_replication_message_io(rdma_replication_message_io&&) = delete;
    rdma_replication_message_io& operator=(rdma_replication_message_io&&) = delete;

    ~rdma_replication_message_io() override = default;

    /**
     * @brief Construct in string-mode backed by the given RDMA send stream and datastore.
     * @param rdma_stream RDMA send stream used to transmit data.
     * @param ds Datastore used to resolve blob file paths.
     */
    rdma_replication_message_io(rdma_send_stream_base& rdma_stream, datastore& ds);

    /**
     * @brief Send a blob file via RDMA.
     *
     * First sends any accumulated non-blob data from the inherited output buffer,
     * then reads the blob file straight into RDMA send frames acquired from the
     * stream. The wire format is identical to tcp_replication_message_io::send_blob():
     * [blob_id: 8B][size: 4B][data: size bytes].
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
     *
     * Acquires a frame large enough to hold the header plus at least one data byte,
     * so the header is never split across frames.
     *
     * @param blob_id ID of the blob being transmitted.
     * @param blob Opened BLOB file positioned at the beginning of the blob data.
     * @param[in,out] remaining Total bytes left to send; decremented by the first chunk size.
     * @throws limestone::api::limestone_io_exception if sending the first RDMA frame fails.
     */
    void send_blob_header_and_first_chunk(
        blob_id_type blob_id,
        opened_blob_file& blob,
        std::uint32_t& remaining);

    /**
     * @brief Read the blob file and send its remaining content in chunks via RDMA.
     * @param blob Opened BLOB file positioned after the first chunk of blob data.
     * @param remaining Total bytes still to send.
     * @throws limestone::api::limestone_io_exception if sending a BLOB data chunk fails.
     */
    void send_blob_data(opened_blob_file& blob, std::uint32_t remaining);

    rdma_send_stream_base& rdma_stream_;
    datastore& datastore_;
};

} // namespace limestone::replication
