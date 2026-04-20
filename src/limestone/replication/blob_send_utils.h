#pragma once

#include <cstdio>
#include <cstdint>

#include <boost/filesystem/path.hpp>

#include <limestone/api/blob_id_type.h>

namespace limestone::api {
class datastore;
}

namespace limestone::replication {

struct opened_blob_file {
    boost::filesystem::path path{};
    FILE* fp{};  // NOLINT(cppcoreguidelines-owning-memory)
    std::uint32_t size{};
};

/**
 * @brief Open a BLOB file for sending and return its resolved path, handle, and size.
 * @param datastore Datastore used to resolve the BLOB file path.
 * @param blob_id BLOB identifier to open.
 * @return Opened file information including the canonicalized path and byte size.
 * @throws limestone::api::limestone_io_exception if the file cannot be opened,
 *         is not a regular file, or exceeds the supported size limit.
 */
[[nodiscard]] opened_blob_file open_blob_file_for_send(
    api::datastore& datastore,
    api::blob_id_type blob_id);

/**
 * @brief Close an opened BLOB file and log a warning if fclose fails.
 * @param fp File handle to close. May be null.
 * @param failure_message_prefix Prefix used for the warning log on close failure.
 */
void safe_close_blob_file(FILE* fp, char const* failure_message_prefix);

/**
 * @brief Read exactly the requested number of bytes from an opened BLOB file.
 * @param fp File handle positioned at the next unread byte.
 * @param path Path used only for diagnostics.
 * @param buffer Destination buffer.
 * @param length Number of bytes to read.
 * @return Number of bytes read, always equal to @p length on success.
 * @throws limestone::api::limestone_io_exception if the file cannot provide
 *         exactly @p length bytes.
 */
[[nodiscard]] std::size_t read_blob_chunk(
    FILE* fp,
    boost::filesystem::path const& path,
    char* buffer,
    std::size_t length);

} // namespace limestone::replication
