#pragma once

#include <cstddef>
#include <cstdint>

#include <limestone/api/epoch_id_type.h>
#include <limestone/api/write_version_type.h>

#include "message_log_entries.h"

namespace limestone::replication {

class socket_io;

namespace message_log_entries_wire_codec {

struct message_header {
    limestone::api::epoch_id_type epoch_id{};
    std::uint32_t entry_count{};
};

/**
 * @brief Send the LOG_ENTRY body header fields.
 * @param io Destination socket-style wire writer.
 * @param epoch_id Epoch ID carried by the LOG_ENTRY message.
 * @param entry_count Number of entries in the message.
 */
void send_message_header(
        socket_io& io,
        limestone::api::epoch_id_type epoch_id,
        std::size_t entry_count);

/**
 * @brief Receive the LOG_ENTRY body header fields.
 * @param io Source socket-style wire reader.
 * @return Decoded epoch ID and entry count.
 */
[[nodiscard]] message_header receive_message_header(socket_io& io);

/**
 * @brief Send the fixed fields of one LOG_ENTRY entry, excluding BLOB payloads.
 * @param io Destination socket-style wire writer.
 * @param entry Entry whose type, storage ID, key, value, and write version are sent.
 */
void send_entry_fixed_fields(socket_io& io, message_log_entries::entry const& entry);

/**
 * @brief Receive the fixed fields of one LOG_ENTRY entry, excluding BLOB payloads.
 * @param io Source socket-style wire reader.
 * @return Entry populated with type, storage ID, key, value, and write version.
 */
[[nodiscard]] message_log_entries::entry receive_entry_fixed_fields(socket_io& io);

/**
 * @brief Send the number of BLOBs attached to the current entry.
 * @param io Destination socket-style wire writer.
 * @param blob_count Number of BLOB IDs and payloads that follow.
 */
void send_blob_count(socket_io& io, std::size_t blob_count);

/**
 * @brief Receive the number of BLOBs attached to the current entry.
 * @param io Source socket-style wire reader.
 * @return Number of BLOB IDs and payloads that follow.
 */
[[nodiscard]] std::uint32_t receive_blob_count(socket_io& io);

/**
 * @brief Send the final LOG_ENTRY operation flags byte.
 * @param io Destination socket-style wire writer.
 * @param flags Bitset of session begin, session end, and flush flags.
 */
void send_operation_flags(socket_io& io, std::uint8_t flags);

/**
 * @brief Receive the final LOG_ENTRY operation flags byte.
 * @param io Source socket-style wire reader.
 * @return Bitset of session begin, session end, and flush flags.
 */
[[nodiscard]] std::uint8_t receive_operation_flags(socket_io& io);

/**
 * @brief Convert an encoded entry type byte into the LOG entry enum.
 * @param value Wire value of the entry type field.
 * @return Decoded LOG entry type.
 */
[[nodiscard]] limestone::api::log_entry::entry_type decode_entry_type(std::uint8_t value) noexcept;

/**
 * @brief Build a write version from decoded wire fields.
 * @param major Major write version field.
 * @param minor Minor write version field.
 * @return Combined write version value.
 */
[[nodiscard]] limestone::api::write_version_type make_write_version(
        std::uint64_t major,
        std::uint64_t minor) noexcept;

/**
 * @brief Apply decoded operation flags to a LOG_ENTRY message object.
 * @param message Message whose session and flush flags are updated.
 * @param flags Bitset of session begin, session end, and flush flags.
 */
void apply_operation_flags(message_log_entries& message, std::uint8_t flags);

}  // namespace message_log_entries_wire_codec

}  // namespace limestone::replication
