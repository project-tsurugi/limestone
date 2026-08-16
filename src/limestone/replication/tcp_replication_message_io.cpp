#include "tcp_replication_message_io.h"

#include <unistd.h>

#include <algorithm>
#include <boost/filesystem.hpp>
#include <limits>
#include <stdexcept>
#include <vector>
#include <cstdio>
#include "opened_blob_file.h"
#include "datastore_impl.h"
#include "limestone_exception_helper.h"

namespace limestone::replication {

tcp_replication_message_io::tcp_replication_message_io(int fd, datastore &ds)
    : replication_message_io(fd), datastore_(ds) {}

tcp_replication_message_io::tcp_replication_message_io(const std::string &initial, datastore &ds)
    : replication_message_io(initial), datastore_(ds) {}

void tcp_replication_message_io::send_blob(const blob_id_type blob_id) {
    auto opened = opened_blob_file::open_for_send(datastore_, blob_id);
    auto remaining = opened.size();

    send_uint64(blob_id);
    send_uint32(remaining);

    std::vector<std::uint8_t> buffer(blob_buffer_size);
    while (remaining > 0) {
        std::size_t chunk = std::min(blob_buffer_size, static_cast<std::size_t>(remaining));
        std::size_t total_read = opened.read_chunk(buffer.data(), chunk);
        write_out_bytes(
            reinterpret_cast<char const*>(buffer.data()),  // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
            total_read);
        remaining -= static_cast<uint32_t>(total_read);
    }

    flush();
}

blob_id_type tcp_replication_message_io::receive_blob() {
    blob_id_type blob_id = receive_uint64();
    uint32_t remaining = receive_uint32();
    auto path = datastore_.get_impl()->resolve_blob_path(blob_id);
    auto parent = path.parent_path();

    if (!boost::filesystem::exists(parent)) {
        try {
            boost::filesystem::create_directory(parent);
        } catch (const boost::filesystem::filesystem_error &e) {
            LOG_AND_THROW_IO_EXCEPTION(
                "Failed to create directory for blob file: " + parent.string(),
                e.code().value()
            );
        }
    } else if (!boost::filesystem::is_directory(parent)) {
        LOG_AND_THROW_IO_EXCEPTION(
            "Expected directory at path for blob file: " + parent.string(),
            EIO
        );
    }

    FILE* fp = std::fopen(path.string().c_str(), "wb"); // NOLINT(cppcoreguidelines-owning-memory)
    if (!fp) {
        LOG_AND_THROW_IO_EXCEPTION("Failed to open blob for writing: " + path.string(), errno);
    }

    std::vector<char> buffer(blob_buffer_size);
    while (remaining > 0) {
        std::size_t chunk = std::min(blob_buffer_size, static_cast<std::size_t>(remaining));
        get_in_stream().read(buffer.data(), static_cast<std::streamsize>(chunk));
        std::streamsize got = get_in_stream().gcount();
        if (got <= 0) {
            safe_close(fp);
            LOG_AND_THROW_IO_EXCEPTION("Failed to read blob from stream", EIO);
        }
        if (std::fwrite(buffer.data(), 1, static_cast<std::size_t>(got), fp) != static_cast<std::size_t>(got)) {
            int ec = errno;
            safe_close(fp);
            LOG_AND_THROW_IO_EXCEPTION("Failed to write blob chunk: " + path.string(), ec);
        }
        remaining -= static_cast<uint32_t>(got);
    }

    if (std::fflush(fp) != 0) {
        int ec = errno;
        safe_close(fp);
        LOG_AND_THROW_IO_EXCEPTION("Failed to flush blob file: " + path.string(), ec);
    }
    if (fsync(fileno(fp)) == -1) {
        int ec = errno;
        safe_close(fp);
        LOG_LP(WARNING) << "fsync failed: " << strerror(ec);
    }
    safe_close(fp);

    return blob_id;
}

void tcp_replication_message_io::safe_close(FILE *fp) {
    if (fp) {
        int ret = std::fclose(fp);  // NOLINT(cppcoreguidelines-owning-memory)
        if (ret != 0) {
            LOG_LP(ERROR) << "Failed to close file: " << strerror(errno);
        }
        fp=nullptr;
    }
}

} // namespace limestone::replication
