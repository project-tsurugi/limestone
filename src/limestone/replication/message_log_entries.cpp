#include "message_log_entries.h"

#include <cassert>

#include "blob_socket_io.h"
#include "limestone_exception_helper.h"
#include "socket_io.h"
#include "log_channel_handler_resources.h"
#include "limestone/api/log_channel.h"
#include "message_ack.h"
namespace limestone::replication {

using limestone::api::epoch_id_type;

void message_log_entries::send_body(socket_io& io) const {
    TRACE_START << "epcoh id =" << epoch_id_ << ", entries size = " << entries_.size();
    auto size = entries_.size();
    io.send_uint64(static_cast<uint64_t>(epoch_id_)); 
    auto entry_count = static_cast<uint32_t>(size);
    io.send_uint32(entry_count); // TODO: オーバーフローのチェックが必要

    // Send each entry
    for (const auto& entry : entries_) {
        io.send_uint8(static_cast<uint8_t>(entry.type));
        io.send_uint64(entry.storage_id);
        io.send_string(entry.key);
        io.send_string(entry.value);
        io.send_uint64(entry.write_version.get_major());
        io.send_uint64(entry.write_version.get_minor());
        
        // Send the blob list
        io.send_uint32(static_cast<uint32_t>(entry.blob_ids.size())); // TODO: オーバーフローのチェックが必要
        if (!entry.blob_ids.empty()) {
            auto* blob_io = dynamic_cast<blob_socket_io*>(&io);
            if (!blob_io) {
                LOG_LP(FATAL) << "Cannot process blob entries without blob_socket_io";
            }
            for (const auto& blob_id : entry.blob_ids) {
                blob_io->send_blob(blob_id);
            }
        }
    }

    // Send the operation flags (session begin, end, flush)
    io.send_uint8(operation_flags_);
    TRACE_END;
}

void message_log_entries::receive_body(socket_io& io) {
    // Receive the number of entries
    epoch_id_ = io.receive_uint64();
    uint32_t entry_count = io.receive_uint32();

    // Clear existing entries and reserve space
    entries_.clear();
    entries_.reserve(entry_count);

    // Receive each entry
    for (uint32_t i = 0; i < entry_count; ++i) {
        entry new_entry;
        new_entry.type = static_cast<log_entry::entry_type>(io.receive_uint8());
        new_entry.storage_id = io.receive_uint64();
        new_entry.key = io.receive_string();
        new_entry.value = io.receive_string();

        epoch_id_type epoch_number = io.receive_uint64();
        std::uint64_t minor_write_version = io.receive_uint64();
        new_entry.write_version  = write_version_type(epoch_number, minor_write_version);

        // Receive blob list
        uint32_t blob_count = io.receive_uint32();
        if (blob_count > 0) {
            auto* blob_io = dynamic_cast<blob_socket_io*>(&io);
            if (!blob_io) {
                LOG_LP(FATAL) << "Cannot process blob entries without blob_socket_io";
            }
            new_entry.blob_ids.resize(blob_count);
            for (uint32_t j = 0; j < blob_count; ++j) {
                new_entry.blob_ids[j] = blob_io->receive_blob();
            }
        }

        // Add the entry to the vector
        entries_.push_back(std::move(new_entry));
    }

    // Receive the operation flags (session begin, end, flush)
    operation_flags_ = io.receive_uint8();
}

epoch_id_type message_log_entries::get_epoch_id() const {
    return epoch_id_;
}

bool message_log_entries::has_session_begin_flag() const {
    return (operation_flags_ & SESSION_BEGIN_FLAG) != 0;
}

bool message_log_entries::has_session_end_flag() const {
    return (operation_flags_ & SESSION_END_FLAG) != 0;
}

bool message_log_entries::has_flush_flag() const {
    return (operation_flags_ & FLUSH_FLAG) != 0;
}


void message_log_entries::set_session_begin_flag(bool flag) {
    if (flag) {
        operation_flags_ |= static_cast<std::uint8_t>(SESSION_BEGIN_FLAG);
    } else {
        operation_flags_ &= static_cast<std::uint8_t>(~static_cast<std::uint8_t>(SESSION_BEGIN_FLAG));
    }
}

void message_log_entries::set_session_end_flag(bool flag) {
    if (flag) {
        operation_flags_ |= static_cast<std::uint8_t>(SESSION_END_FLAG);
    } else {
        operation_flags_ &= static_cast<std::uint8_t>(~static_cast<std::uint8_t>(SESSION_END_FLAG));
    }
}

void message_log_entries::set_flush_flag(bool flag) {
    if (flag) {
        operation_flags_ |= static_cast<std::uint8_t>(FLUSH_FLAG);
    } else {
        operation_flags_ &= static_cast<std::uint8_t>(~static_cast<std::uint8_t>(FLUSH_FLAG));
    }
}

void message_log_entries::add_normal_entry(storage_id_type storage_id, std::string_view key, std::string_view value, write_version_type write_version) {
    entry new_entry;
    new_entry.type = log_entry::entry_type::normal_entry;
    new_entry.storage_id = storage_id;
    new_entry.key = std::string(key);
    new_entry.value = std::string(value);
    new_entry.write_version = write_version;
    entries_.push_back(std::move(new_entry));
}

void message_log_entries::add_normal_with_blob(storage_id_type storage_id, std::string_view key, std::string_view value, write_version_type write_version, const std::vector<blob_id_type>& large_objects) {
    entry new_entry;
    new_entry.type = log_entry::entry_type::normal_with_blob;
    new_entry.storage_id = storage_id;
    new_entry.key = std::string(key);
    new_entry.value = std::string(value);
    new_entry.write_version = write_version;
    new_entry.blob_ids = large_objects;
    entries_.push_back(std::move(new_entry));
}

void message_log_entries::add_remove_entry(storage_id_type storage_id, std::string_view key, write_version_type write_version) {
    entry new_entry;
    new_entry.type = log_entry::entry_type::remove_entry;
    new_entry.storage_id = storage_id;
    new_entry.key = std::string(key);
    new_entry.write_version = write_version;
    entries_.push_back(std::move(new_entry));
}

void message_log_entries::add_clear_storage(storage_id_type storage_id, write_version_type write_version) {
    entry new_entry;
    new_entry.type = log_entry::entry_type::clear_storage;
    new_entry.storage_id = storage_id;
    new_entry.write_version = write_version;
    entries_.push_back(std::move(new_entry));
}

void message_log_entries::add_add_storage(storage_id_type storage_id, write_version_type write_version) {
    entry new_entry;
    new_entry.type = log_entry::entry_type::add_storage;
    new_entry.storage_id = storage_id;
    new_entry.write_version = write_version;
    entries_.push_back(std::move(new_entry));
}

void message_log_entries::add_remove_storage(storage_id_type storage_id, write_version_type write_version) {
    entry new_entry;
    new_entry.type = log_entry::entry_type::remove_storage;
    new_entry.storage_id = storage_id;
    new_entry.write_version = write_version;
    entries_.push_back(std::move(new_entry));
}

const std::vector<message_log_entries::entry>& message_log_entries::get_entries() const {
    return entries_;
}


std::unique_ptr<replication_message> message_log_entries::create() {
    return std::make_unique<message_log_entries>(epoch_id_type{0});
}

void message_log_entries::post_receive(handler_resources& resources) {
    auto& lch_resources = dynamic_cast<log_channel_handler_resources&>(resources);
    auto& log_channel = lch_resources.get_log_channel();
    if (has_session_begin_flag()) {
        log_channel.begin_session();
    }
    for (const auto& entry : entries_) {
        switch (entry.type) {
            case log_entry::entry_type::normal_entry:
                log_channel.add_entry(entry.storage_id, entry.key, entry.value, entry.write_version);
                break;
            case log_entry::entry_type::normal_with_blob:
                log_channel.add_entry(entry.storage_id, entry.key, entry.value, entry.write_version, entry.blob_ids);
                break;
            case log_entry::entry_type::remove_entry:
                log_channel.remove_entry(entry.storage_id, entry.key, entry.write_version);
                break;
            case log_entry::entry_type::clear_storage:
                log_channel.truncate_storage(entry.storage_id, entry.write_version);
                break;
            case log_entry::entry_type::add_storage:
                log_channel.add_storage(entry.storage_id, entry.write_version);
                break;
            case log_entry::entry_type::remove_storage:
                log_channel.remove_storage(entry.storage_id, entry.write_version);
                break;
            case log_entry::entry_type::this_id_is_not_used:
            case log_entry::entry_type::marker_begin:
            case log_entry::entry_type::marker_end:
            case log_entry::entry_type::marker_durable:
            case log_entry::entry_type::marker_invalidated_begin:
                std::string msg = "Invalid entry type: " + std::to_string(static_cast<int>(entry.type));
                LOG_AND_THROW_EXCEPTION(msg);
                break;
        }
    }
    if (has_session_end_flag() || has_flush_flag()) {
        log_channel.end_session();
        message_ack ack;
        socket_io& io = lch_resources.get_socket_io();
        replication_message::send(io, ack);
        io.flush();
    }
}

}  // namespace limestone::replication
