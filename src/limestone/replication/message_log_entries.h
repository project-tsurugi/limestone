#pragma once

#include <limestone/api/blob_id_type.h>
#include <limestone/api/epoch_id_type.h>
#include <limestone/api/storage_id_type.h>
#include <limestone/api/write_version_type.h>

#include <string_view>
#include <vector>

#include "log_entry.h"
#include "replication_message.h"

namespace limestone::replication {

    using limestone::api::storage_id_type;
    using limestone::api::write_version_type;
    using limestone::api::blob_id_type;
    using limestone::api::log_entry;
    using limestone::api::epoch_id_type;
    
    
class message_log_entries : public replication_message {

public:
    // Define flag values as constants
    static constexpr uint8_t SESSION_BEGIN_FLAG = 0x01;  // 00000001
    static constexpr uint8_t SESSION_END_FLAG = 0x02;    // 00000010
    static constexpr uint8_t FLUSH_FLAG = 0x04;          // 00000100

    // Internal class to represent each entry
    struct entry {
        // Entry data structure that holds necessary information for each log entry
        log_entry::entry_type type{};
        storage_id_type storage_id{};
        std::string key{};
        std::string value{};
        write_version_type write_version{};
        std::vector<blob_id_type> blob_ids{};
        // Add other fields as needed based on entry type
    };
    // Constructor
    explicit message_log_entries(epoch_id_type epoch_id) : epoch_id_(epoch_id) {}
    ~message_log_entries() override = default;

    // Delete copy and move constructors and assignment operators
    message_log_entries(const message_log_entries& other) = delete;
    message_log_entries(message_log_entries&& other) noexcept = delete;
    message_log_entries& operator=(const message_log_entries& other) = delete;
    message_log_entries& operator=(message_log_entries&& other) noexcept = delete;

    // Override method to return message type id
    [[nodiscard]] message_type_id get_message_type_id() const override {
        return message_type_id::LOG_ENTRY;
    }

    // Override method to send message body to socket
    void send_body(socket_io& io) const override;

    // Getter
    [[nodiscard]] epoch_id_type get_epoch_id() const;
    [[nodiscard]] bool has_session_begin_flag() const;
    [[nodiscard]] bool has_session_end_flag() const;
    [[nodiscard]] bool has_flush_flag() const;

    // Override method to receive message body from socket
    void receive_body(socket_io& io) override;

    // Set session begin flag (0x01)
    void set_session_begin_flag(bool flag);

    // Set session end flag (0x02)
    void set_session_end_flag(bool flag);

    // Set flush flag (0x04)
    void set_flush_flag(bool flag);

    // Add normal entry without blob
    void add_normal_entry(storage_id_type storage_id, std::string_view key, std::string_view value, write_version_type write_version);

    // Add normal entry with blob
    void add_normal_with_blob(storage_id_type storage_id, std::string_view key, std::string_view value, write_version_type write_version, const std::vector<blob_id_type>& large_objects);

    // Add remove entry
    void add_remove_entry(storage_id_type storage_id, std::string_view key, write_version_type write_version);

    // Add clear storage entry
    void add_clear_storage(storage_id_type storage_id, write_version_type write_version);

    // Add add storage entry
    void add_add_storage(storage_id_type storage_id, write_version_type write_version);

    // Add remove storage entry
    void add_remove_storage(storage_id_type storage_id, write_version_type write_version);

    [[nodiscard]] const std::vector<entry>& get_entries() const;

    /**
     * @brief Factory method to create an instance of message_log_entries.
     * @return a unique_ptr to the newly created message_log_entries object.
     */
    [[nodiscard]] static std::unique_ptr<replication_message> create();
    
private:
    // Register LOG_ENTRY in replication_message factory map.
    // The static initialization here is intentional. If an exception occurs,
    // the program should terminate immediately. We ignore the clang-tidy warning
    // (cert-err58-cpp) as this behavior is desired.
    // NOLINTNEXTLINE(cert-err58-cpp)
    inline static const bool registered_ = []() {
        replication_message::register_message_type(message_type_id::LOG_ENTRY, &message_log_entries::create);
        return true;
    }();

    epoch_id_type epoch_id_{};  // Epoch ID for the log entries

    // Internal structure to hold log entries (private)
    std::vector<entry> entries_;

    // Flags to store session and flush operations (private)
    uint8_t operation_flags_ = 0;  // Bitfield for session and flush flags

};

}  // namespace limestone::replication
