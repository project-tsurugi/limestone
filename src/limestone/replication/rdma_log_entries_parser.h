#pragma once

#include <cstddef>
#include <cstdio>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

#include "limestone/api/datastore.h"
#include "message_log_entries.h"

namespace limestone::replication {

/**
 * @brief Incremental parser for the fixed portion of an RDMA LOG_ENTRY message body.
 *
 * This class parses the LOG_ENTRY message body directly from RDMA frame payload
 * bytes without using @c message_log_entries::receive_body().  The existing
 * receive_body() function remains the TCP/socket-stream deserializer and is
 * intentionally not used by the RDMA streaming path.
 *
 * The input to this parser must start at the LOG_ENTRY body, not at the
 * replication message type byte.  In other words, callers are expected to consume
 * or validate @c message_type_id::LOG_ENTRY before passing bytes here.
 *
 * The parser follows the current replication wire format:
 *
 * @code
 * epoch_id
 * entry_count
 *
 * repeated entry_count times:
 *   entry_type
 *   storage_id
 *   key
 *   value
 *   write_version.major
 *   write_version.minor
 *   blob_count
 *   blob_id + blob_size + blob_bytes   repeated blob_count times
 *
 * operation_flags
 * @endcode
 *
 * If an entry has no BLOBs, the entry is immediately added to the in-progress
 * @c message_log_entries.  If an entry has BLOBs, the parser either stops in
 * @c status::awaiting_blob after consuming @c blob_count, or streams the BLOB
 * bytes into replica BLOB files when constructed with a datastore.
 *
 * A single call to @c consume() may provide any number of bytes, including a
 * partial scalar or a partial string.  The parser retains unfinished field bytes
 * internally and resumes when more bytes are supplied.
 */
class rdma_log_entries_parser {
public:
    /**
     * @brief Construct a parser that stops at BLOB payload boundaries.
     */
    rdma_log_entries_parser() = default;

    /**
     * @brief Construct a parser that streams BLOB payloads into @p datastore.
     */
    explicit rdma_log_entries_parser(limestone::api::datastore& datastore) noexcept;

    rdma_log_entries_parser(const rdma_log_entries_parser&) = delete;
    rdma_log_entries_parser& operator=(const rdma_log_entries_parser&) = delete;
    rdma_log_entries_parser(rdma_log_entries_parser&&) = delete;
    rdma_log_entries_parser& operator=(rdma_log_entries_parser&&) = delete;

    ~rdma_log_entries_parser();

    /**
     * @brief High-level parser state visible to the caller.
     */
    enum class status {
        /// More LOG_ENTRY body bytes are needed, or the current input can still be consumed.
        reading,

        /// The parser stopped just after a non-zero blob_count and is waiting for BLOB handling.
        awaiting_blob,

        /// The full LOG_ENTRY body has been parsed and a message can be taken.
        complete,
    };

    /**
     * @brief Consume as many bytes as possible from @p bytes.
     *
     * @return Number of bytes consumed from @p bytes.  This can be smaller than
     *         @c bytes.size() when the parser reaches @c awaiting_blob or
     *         @c complete and leaves subsequent bytes for the caller.
     */
    [[nodiscard]] std::size_t consume(std::string_view bytes);

    /**
     * @brief Return the current high-level parser status.
     */
    [[nodiscard]] status get_status() const noexcept;

    /**
     * @brief Return true if a complete @c message_log_entries can be taken.
     */
    [[nodiscard]] bool complete() const noexcept;

    /**
     * @brief Return true if parsing is paused before BLOB header/body handling.
     */
    [[nodiscard]] bool awaiting_blob() const noexcept;

    /**
     * @brief Number of BLOBs expected for the current entry.
     *
     * This is meaningful when @c awaiting_blob() is true.
     */
    [[nodiscard]] std::uint32_t pending_blob_count() const noexcept;

    /**
     * @brief Number of entries not yet fully added to the in-progress message.
     */
    [[nodiscard]] std::uint32_t entries_remaining() const noexcept;

    /**
     * @brief Return the completed message and reset the parser for reuse.
     *
     * @throws std::logic_error if the parser has not reached @c status::complete.
     */
    [[nodiscard]] std::unique_ptr<message_log_entries> take_message();

private:
    /**
     * @brief Fine-grained state for parsing fields that may be split across RDMA frames.
     */
    enum class parse_state {
        epoch_id,
        entry_count,
        entry_type,
        storage_id,
        key_length,
        key_bytes,
        value_length,
        value_bytes,
        write_version_major,
        write_version_minor,
        blob_count,
        blob_id,
        blob_size,
        blob_bytes,
        operation_flags,
        complete,
        awaiting_blob,
    };

    bool read_bytes(std::string_view bytes, std::size_t& offset, std::size_t size);
    bool read_uint8(std::string_view bytes, std::size_t& offset, std::uint8_t& value);
    bool read_uint32(std::string_view bytes, std::size_t& offset, std::uint32_t& value);
    bool read_uint64(std::string_view bytes, std::size_t& offset, std::uint64_t& value);
    bool read_string_length(std::string_view bytes, std::size_t& offset, parse_state next);
    bool read_string_bytes(std::string_view bytes, std::size_t& offset, std::string& target, parse_state next);
    bool read_blob_bytes(std::string_view bytes, std::size_t& offset);

    /**
     * @brief Add the currently parsed entry to the in-progress message.
     */
    void add_current_entry();

    /**
     * @brief Apply the final operation_flags byte to the in-progress message.
     */
    void apply_operation_flags(std::uint8_t flags);
    void open_current_blob_file();
    void finish_current_blob_file();
    void close_current_blob_file_noexcept() noexcept;

    parse_state state_{parse_state::epoch_id};
    std::unique_ptr<message_log_entries> message_{};
    message_log_entries::entry current_entry_{};
    std::uint32_t entries_remaining_{0};
    std::uint32_t pending_blob_count_{0};
    std::uint32_t string_length_{0};
    std::string string_buffer_{};
    std::uint64_t write_version_major_{0};
    std::string scalar_buffer_{};
    limestone::api::datastore* datastore_{nullptr};
    limestone::api::blob_id_type current_blob_id_{0};
    std::uint32_t current_blob_remaining_{0};
    std::uint32_t blobs_remaining_in_entry_{0};
    std::FILE* current_blob_file_{nullptr};
    std::string current_blob_path_{};
};

}  // namespace limestone::replication
