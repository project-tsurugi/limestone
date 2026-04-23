#include "message_log_entries.h"

#include <algorithm>
#include <cassert>

#include "limestone_exception_helper.h"
#include "replication_message_io.h"
#include "log_channel_handler_resources.h"
#include "limestone/api/log_channel.h"
#include "message_ack.h"
#include "message_log_entries_wire_codec.h"
namespace limestone::replication {

using limestone::api::epoch_id_type;

void message_log_entries::send_body(replication_message_io& io) const {
    TRACE_START << "epoch id =" << epoch_id_ << ", entries size = " << entries_.size();
    message_log_entries_wire_codec::send_message_header(io, epoch_id_, entries_.size());

    // Send each entry
    for (const auto& entry : entries_) {
        message_log_entries_wire_codec::send_entry_fixed_fields(io, entry);
        
        // Send the blob list
        message_log_entries_wire_codec::send_blob_count(io, entry.blob_ids.size());
        for (const auto& blob_id : entry.blob_ids) {
            io.send_blob(blob_id);
        }
    }

    // Send the operation flags (session begin, end, flush)
    message_log_entries_wire_codec::send_operation_flags(io, operation_flags_);
    TRACE_END;
}

void message_log_entries::receive_body(replication_message_io& io) {
    // Receive the number of entries
    auto const header = message_log_entries_wire_codec::receive_message_header(io);
    epoch_id_ = header.epoch_id;

    // Clear existing entries and reserve space
    entries_.clear();
    entries_.reserve(header.entry_count);

    // Receive each entry
    for (uint32_t i = 0; i < header.entry_count; ++i) {
        entry new_entry = message_log_entries_wire_codec::receive_entry_fixed_fields(io);

        // Receive blob list
        uint32_t blob_count = message_log_entries_wire_codec::receive_blob_count(io);
        new_entry.blob_ids.resize(blob_count);
        for (uint32_t j = 0; j < blob_count; ++j) {
            new_entry.blob_ids[j] = io.receive_blob();
        }

        // Add the entry to the vector
        entries_.push_back(std::move(new_entry));
    }

    // Receive the operation flags (session begin, end, flush)
    operation_flags_ = message_log_entries_wire_codec::receive_operation_flags(io);
}

bool message_log_entries::has_any_blobs() const noexcept {
    return std::any_of(entries_.begin(), entries_.end(),
        [](entry const& e) { return ! e.blob_ids.empty(); });
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
        if (lch_resources.ack_enabled()) {
            message_ack ack;
            replication_message_io& io = lch_resources.get_replication_message_io();
            replication_message::send(io, ack);
            io.flush();
        }
    }
}

}  // namespace limestone::replication
