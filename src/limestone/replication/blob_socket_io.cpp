#include "blob_socket_io.h"

#include <unistd.h>

#include <algorithm>
#include <boost/filesystem.hpp>
#include <limits>
#include <stdexcept>
#include <vector>
#include <cstdio>
#include "limestone_exception_helper.h"

namespace limestone::replication {

blob_socket_io::blob_socket_io(int fd, blob_file_resolver &resolver)
    : socket_io(fd), blob_resolver_(resolver) {}

blob_socket_io::blob_socket_io(const std::string &initial, blob_file_resolver &resolver)
    : socket_io(initial), blob_resolver_(resolver) {}

void blob_socket_io::send_blob(const blob_id_type blob_id) {
    auto path = blob_resolver_.resolve_path(blob_id);
    auto status = boost::filesystem::symlink_status(path);
    if (boost::filesystem::is_symlink(status)) {
        path = boost::filesystem::canonical(path);
        status = boost::filesystem::status(path);
    }
    if (!boost::filesystem::is_regular_file(status)) {
        LOG_AND_THROW_IO_EXCEPTION("Unsupported blob path type: " + path.string(), errno);
    }

    FILE* fp = std::fopen(path.string().c_str(), "rb");  // NOLINT(cppcoreguidelines-owning-memory)
    if (!fp) {
        LOG_AND_THROW_IO_EXCEPTION("Failed to open blob for reading: " + path.string(), errno);
    }

    if (std::fseek(fp, 0, SEEK_END) != 0) {
        int ec = errno;
        safe_close(fp);
        LOG_AND_THROW_IO_EXCEPTION("Failed to seek blob file: " + path.string(), ec);
    }
    int64_t pos = std::ftell(fp);
    if (pos == -1) {
        int ec = errno;
        safe_close(fp);
        LOG_AND_THROW_IO_EXCEPTION("Failed to tell blob file: " + path.string(), ec);
    }
    if (static_cast<uint64_t>(pos) > std::numeric_limits<uint32_t>::max()) {
        safe_close(fp);
        LOG_AND_THROW_IO_EXCEPTION("Blob file too large: " + path.string(), EIO);
    }
    auto remaining = static_cast<uint32_t>(pos);
    std::rewind(fp);

    send_uint64(blob_id);
    send_uint32(remaining);

    std::vector<char> buffer(blob_buffer_size);
    while (remaining > 0) {
        std::size_t chunk = std::min(blob_buffer_size, static_cast<std::size_t>(remaining));
        std::size_t total_read = 0;
        while (total_read < chunk) {
            std::size_t r = std::fread(&*std::next(buffer.begin(), static_cast<std::vector<char>::difference_type>(total_read)), 1, chunk - total_read, fp);
            if (r == 0) {
                if (std::feof(fp) != 0) {
                    safe_close(fp);
                    LOG_AND_THROW_IO_EXCEPTION("Unexpected EOF reading blob: " + path.string(), errno);
                }
                if (std::ferror(fp) != 0 && errno == EINTR) {
                    std::clearerr(fp);
                    continue;
                }
                int ec = errno;
                safe_close(fp);
                LOG_AND_THROW_IO_EXCEPTION("Failed to read blob chunk: " + path.string(), ec);
            }
            total_read += r;
        }
        get_out_stream().write(buffer.data(), static_cast<std::streamsize>(total_read));
        remaining -= static_cast<uint32_t>(total_read);
    }

    safe_close(fp);
    flush();
}

blob_id_type blob_socket_io::receive_blob() {
    blob_id_type blob_id = receive_uint64();
    uint32_t remaining = receive_uint32();

    auto path = blob_resolver_.resolve_path(blob_id);
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

void blob_socket_io::safe_close(FILE *fp) {
    if (fp) {
        int ret = std::fclose(fp);  // NOLINT(cppcoreguidelines-owning-memory)
        if (ret != 0) {
            LOG_LP(ERROR) << "Failed to close file: " << strerror(errno);
        }
        fp=nullptr;
    }
}

} // namespace limestone::replication
