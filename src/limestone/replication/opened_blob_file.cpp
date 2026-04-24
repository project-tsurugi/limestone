#include "opened_blob_file.h"

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <iterator>
#include <limits>
#include <utility>

#include <boost/filesystem.hpp>

#include "limestone_exception_helper.h"
#include <limestone/api/blob_file.h>
#include <limestone/api/datastore.h>

namespace limestone::replication {

namespace {

void safe_close_blob_file(FILE* fp, char const* failure_message_prefix) {
    if (! fp) {
        return;
    }
    int ret = std::fclose(fp);  // NOLINT(cppcoreguidelines-owning-memory)
    if (ret != 0) {
        LOG_LP(WARNING) << failure_message_prefix << ": " << std::strerror(errno);
    }
}

std::size_t read_blob_chunk(
        FILE* fp,
        boost::filesystem::path const& path,
        std::uint8_t* buffer,
        std::size_t length) {
    std::size_t total_read = 0;
    while (total_read < length) {
        using buffer_offset_type = std::iterator_traits<std::uint8_t*>::difference_type;
        auto const offset = static_cast<buffer_offset_type>(total_read);
        std::size_t r = std::fread(std::next(buffer, offset), 1, length - total_read, fp);
        int const saved_errno = errno;
        if (r > 0) {
            total_read += r;
            continue;
        }
        if (std::ferror(fp) != 0) {
            int ec = saved_errno;
            if (ec == EINTR) {
                std::clearerr(fp);
                continue;
            }
            if (ec == 0) {
                ec = EIO;
            }
            LOG_AND_THROW_IO_EXCEPTION("Failed to read blob chunk: " + path.string(), ec);
        }
        if (std::feof(fp) != 0) {
            LOG_AND_THROW_IO_EXCEPTION("Unexpected EOF reading blob: " + path.string(), EIO);
        }
        // Defensive fallback: zero-byte fread() without ferror() or feof()
        // should not normally occur. Treat it as an I/O failure to avoid
        // spinning forever without making progress.
        LOG_AND_THROW_IO_EXCEPTION(
                "Failed to read blob chunk without progress: " + path.string(), EIO);
    }
    return total_read;
}

}  // namespace

opened_blob_file::opened_blob_file(boost::filesystem::path path, FILE* fp, std::uint32_t size) noexcept
    : path_(std::move(path))
    , fp_(fp)
    , size_(size) {}

opened_blob_file::~opened_blob_file() noexcept {
    close_noexcept("fclose failed for blob file");
}

opened_blob_file::opened_blob_file(opened_blob_file&& other) noexcept
    : path_(std::move(other.path_))
    , fp_(other.fp_)
    , size_(other.size_) {
    other.fp_ = nullptr;
    other.size_ = 0;
}

opened_blob_file& opened_blob_file::operator=(opened_blob_file&& other) noexcept {
    if (this != &other) {
        close_noexcept("fclose failed for blob file");
        path_ = std::move(other.path_);
        fp_ = other.fp_;
        size_ = other.size_;
        other.fp_ = nullptr;
        other.size_ = 0;
    }
    return *this;
}

boost::filesystem::path const& opened_blob_file::path() const noexcept {
    return path_;
}

std::uint32_t opened_blob_file::size() const noexcept {
    return size_;
}

std::size_t opened_blob_file::read_chunk(std::uint8_t* buffer, std::size_t length) {
    return read_blob_chunk(fp_, path_, buffer, length);
}

void opened_blob_file::close_noexcept(char const* failure_message_prefix) noexcept {
    safe_close_blob_file(fp_, failure_message_prefix);
    fp_ = nullptr;
}

opened_blob_file opened_blob_file::open_for_send(
        api::datastore& datastore,
        api::blob_id_type blob_id) {
    api::blob_file blob_file = datastore.get_blob_file(blob_id);
    boost::filesystem::path path = blob_file.path();
    // Resolve a symlink target and require the final path to be a regular file.
    auto status = boost::filesystem::symlink_status(path);
    if (boost::filesystem::is_symlink(status)) {
        path = boost::filesystem::canonical(path);
        status = boost::filesystem::status(path);
    }
    if (! boost::filesystem::is_regular_file(status)) {
        LOG_AND_THROW_EXCEPTION("Unsupported blob path type: " + path.string());
    }

    FILE* fp = std::fopen(path.string().c_str(), "rb");  // NOLINT(cppcoreguidelines-owning-memory)
    if (! fp) {
        LOG_AND_THROW_IO_EXCEPTION("Failed to open blob for reading: " + path.string(), errno);
    }
    if (std::fseek(fp, 0, SEEK_END) != 0) {
        int ec = errno;
        safe_close_blob_file(fp, "fclose failed for blob file after seek error");
        LOG_AND_THROW_IO_EXCEPTION("Failed to seek blob file: " + path.string(), ec);
    }
    auto pos = std::ftell(fp);
    if (pos == -1) {
        int ec = errno;
        safe_close_blob_file(fp, "fclose failed for blob file after tell error");
        LOG_AND_THROW_IO_EXCEPTION("Failed to tell blob file: " + path.string(), ec);
    }
    if (static_cast<std::uint64_t>(pos) > std::numeric_limits<std::uint32_t>::max()) {
        safe_close_blob_file(fp, "fclose failed for oversized blob file");
        LOG_AND_THROW_IO_EXCEPTION("Blob file too large: " + path.string(), EIO);
    }
    if (std::fseek(fp, 0, SEEK_SET) != 0) {
        int ec = errno;
        safe_close_blob_file(fp, "fclose failed for blob file after rewind error");
        LOG_AND_THROW_IO_EXCEPTION("Failed to rewind blob file: " + path.string(), ec);
    }

    return {std::move(path), fp, static_cast<std::uint32_t>(pos)};
}

} // namespace limestone::replication
