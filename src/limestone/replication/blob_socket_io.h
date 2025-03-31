#pragma once

#include "socket_io.h"
#include <limestone/api/blob_id_type.h>
#include <limestone/api/datastore.h>
#include <cstdio>

namespace limestone::replication {

using limestone::internal::blob_file_resolver;    
using limestone::api::datastore;
using limestone::api::blob_id_type;

// TODO socket_ioから、blob_sokcet_ioを派生するのはソースコードの修正を少なくするためで、
// 本当は、socket_ioクラスにblob_socket_ioの機能を組み込むべき。
class blob_socket_io : public socket_io {
public:
    static constexpr std::size_t blob_buffer_size = 64UL * 1024UL;
    
    // Real‑socket constructor
    blob_socket_io(int fd, datastore& ds);

    // String‑mode constructor (for tests)
    blob_socket_io(const std::string& initial, datastore& ds);

    // Send/receive blob methods
    void send_blob(blob_id_type blob_id);
    blob_id_type receive_blob();
private:
    void safe_close(FILE *fp);
    datastore& datastore_;
};

} // namespace limestone::replication
