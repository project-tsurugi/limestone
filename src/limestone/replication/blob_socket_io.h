#pragma once

#include "socket_io.h"
#include <limestone/api/blob_id_type.h>
#include <limestone/api/datastore.h>
#include <cstdio>

namespace limestone::replication {

using limestone::api::datastore;
using limestone::api::blob_id_type;

/**
 * @brief socket_io extension that can send and receive BLOB payloads on the TCP path.
 *
 * TCP replication still deserializes LOG_ENTRY messages via
 * replication_message::receive() / message_log_entries::receive_body(), so BLOB
 * payload handling remains here.
 */
class blob_socket_io : public socket_io {
public:
    static constexpr std::size_t blob_buffer_size = 64UL * 1024UL;
    
    // Disallow copy and move operations
    blob_socket_io(const blob_socket_io&) = delete;
    blob_socket_io& operator=(const blob_socket_io&) = delete;
    blob_socket_io(blob_socket_io&&) = delete;
    blob_socket_io& operator=(blob_socket_io&&) = delete;

    // Default constructor
    ~blob_socket_io() override = default;

    // Real‑socket constructor
    blob_socket_io(int fd, datastore& ds);

    // String‑mode constructor (for tests)
    blob_socket_io(const std::string& initial, datastore& ds);

    // Send/receive blob methods
    void send_blob(blob_id_type blob_id) override;
    blob_id_type receive_blob() override;
private:
    void safe_close(FILE *fp);
    datastore& datastore_;
};

} // namespace limestone::replication
