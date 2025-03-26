#pragma once

#include "socket_io.h"
#include "blob_file_resolver.h"
#include <limestone/api/blob_id_type.h>
#include <cstdio>

namespace limestone::replication {

using limestone::internal::blob_file_resolver;    
using limestone::api::blob_id_type;

class blob_socket_io : public socket_io {
public:
    // Real‑socket constructor
    blob_socket_io(int fd, blob_file_resolver& resolver);

    // String‑mode constructor (for tests)
    blob_socket_io(const std::string& initial, blob_file_resolver& resolver);

    // Send/receive blob methods
    void send_blob(blob_id_type blob_id);
    blob_id_type receive_blob();
private:
    void safe_close(FILE *fp);
    static constexpr std::size_t blob_buffer_size = 64UL * 1024UL;
    blob_file_resolver &blob_resolver_;
};

} // namespace limestone::replication
