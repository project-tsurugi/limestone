#include "message_log_entries_wire_codec.h"

#include <limits>

#include <glog/logging.h>

#include <limestone/logging.h>

#include "limestone_exception_helper.h"
#include "log_entry/log_entry_format.h"
#include "logging_helper.h"
#include "replication_message_io.h"

namespace limestone::replication::message_log_entries_wire_codec {

namespace {

[[nodiscard]] std::uint32_t checked_count(std::size_t count, char const* message) {
    if (count > std::numeric_limits<std::uint32_t>::max()) {
        LOG_AND_THROW_EXCEPTION(message);
    }
    return static_cast<std::uint32_t>(count);
}

}  // namespace

void send_message_header(
        replication_message_io& io,
        limestone::api::epoch_id_type epoch_id,
        std::size_t entry_count) {
    io.send_uint64(static_cast<std::uint64_t>(epoch_id));
    io.send_uint32(checked_count(entry_count, "Too many log entries in replication message"));
}

message_header receive_message_header(replication_message_io& io) {
    message_header header{};
    header.epoch_id = limestone::api::epoch_id_type{io.receive_uint64()};
    header.entry_count = io.receive_uint32();
    return header;
}

void send_entry_fixed_fields(replication_message_io& io, message_log_entries::entry const& entry) {
    io.send_uint8(static_cast<std::uint8_t>(entry.type));
    io.send_uint64(entry.storage_id);
    io.send_string(entry.key);
    io.send_string(entry.value);
    io.send_uint64(entry.write_version.get_major());
    io.send_uint64(entry.write_version.get_minor());
}

message_log_entries::entry receive_entry_fixed_fields(replication_message_io& io) {
    message_log_entries::entry entry{};
    entry.type = decode_entry_type(io.receive_uint8());
    entry.storage_id = io.receive_uint64();
    entry.key = io.receive_string();
    entry.value = io.receive_string();
    auto const major = io.receive_uint64();
    auto const minor = io.receive_uint64();
    entry.write_version = make_write_version(major, minor);
    return entry;
}

void send_blob_count(replication_message_io& io, std::size_t blob_count) {
    io.send_uint32(checked_count(blob_count, "Too many blob IDs in replication message entry"));
}

std::uint32_t receive_blob_count(replication_message_io& io) {
    return io.receive_uint32();
}

void send_operation_flags(replication_message_io& io, std::uint8_t flags) {
    io.send_uint8(flags);
}

std::uint8_t receive_operation_flags(replication_message_io& io) {
    return io.receive_uint8();
}

limestone::api::log_entry::entry_type decode_entry_type(std::uint8_t value) noexcept {
    using limestone::internal::log_entry_format::entry_type;
    // this_id_is_not_used is a defined value, so it passes here; the receivers abort on it as a type not allowed in a
    // message
    if (value != static_cast<std::uint8_t>(entry_type::this_id_is_not_used)
            && ! limestone::internal::log_entry_format::is_valid_entry_type(value)) {
        LOG_LP(FATAL) << "undefined entry type value " << static_cast<int>(value);
    }
    return static_cast<limestone::api::log_entry::entry_type>(value);
}

limestone::api::write_version_type make_write_version(
        std::uint64_t major,
        std::uint64_t minor) noexcept {
    return limestone::api::write_version_type{major, minor};
}

void apply_operation_flags(message_log_entries& message, std::uint8_t flags) {
    message.set_session_begin_flag((flags & message_log_entries::SESSION_BEGIN_FLAG) != 0);
    message.set_session_end_flag((flags & message_log_entries::SESSION_END_FLAG) != 0);
    message.set_flush_flag((flags & message_log_entries::FLUSH_FLAG) != 0);
}

}  // namespace limestone::replication::message_log_entries_wire_codec
