#pragma once

#include <cstdio>
#include <cstdint>
#include <cstddef>

#include <boost/filesystem/path.hpp>

#include <limestone/api/blob_id_type.h>
#include <limestone/api/datastore.h>

namespace limestone::replication {

/**
 * @brief RAII wrapper for an opened BLOB file used by replication senders.
 *
 * This class owns the opened file handle, closes it on destruction, and exposes
 * the resolved path, byte size, and exact-length chunk reads needed by BLOB
 * transfer code. Instances are move-only so the file ownership cannot be copied.
 */
class opened_blob_file {
public:
    ~opened_blob_file() noexcept;

    opened_blob_file(opened_blob_file const&) = delete;
    opened_blob_file& operator=(opened_blob_file const&) = delete;
    opened_blob_file(opened_blob_file&& other) noexcept;
    opened_blob_file& operator=(opened_blob_file&& other) noexcept;

    /**
     * @brief Open a BLOB file for sending and return its resolved path, handle, and size.
     * @param datastore Datastore used to resolve the BLOB file path.
     * @param blob_id BLOB identifier to open.
     * @return Opened file information including the canonicalized path and byte size.
     * @throws limestone::api::limestone_io_exception if the file cannot be opened,
     *         is not a regular file, or exceeds the supported size limit.
     */
    [[nodiscard]] static opened_blob_file open_for_send(
            api::datastore& datastore,
            api::blob_id_type blob_id);

    /**
     * @brief Return the resolved path of the opened BLOB file.
     * @return Canonicalized file path used to open the BLOB.
     */
    [[nodiscard]] boost::filesystem::path const& path() const noexcept;

    /**
     * @brief Return the size of the opened BLOB file in bytes.
     * @return BLOB file size encoded as an unsigned 32-bit value for transfer.
     */
    [[nodiscard]] std::uint32_t size() const noexcept;

    /**
     * @brief Read an exact-length chunk from the opened BLOB file.
     * @param buffer Destination buffer for the bytes read from the file.
     * @param length Number of bytes to read.
     * @return Number of bytes read, equal to @p length on success.
     * @throws limestone::api::limestone_io_exception if the requested byte count
     *         cannot be read because of an I/O error or unexpected EOF.
     */
    [[nodiscard]] std::size_t read_chunk(std::uint8_t* buffer, std::size_t length);

private:
    opened_blob_file(boost::filesystem::path path, FILE* fp, std::uint32_t size) noexcept;

    void close_noexcept(char const* failure_message_prefix) noexcept;

    boost::filesystem::path path_{};
    FILE* fp_{};  // NOLINT(cppcoreguidelines-owning-memory)
    std::uint32_t size_{};
};

} // namespace limestone::replication
