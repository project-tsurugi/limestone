#pragma once

namespace limestone::grpc::service {

constexpr uint32_t list_wal_history_message_version = 1;
constexpr uint32_t begin_backup_message_version = 1;
constexpr uint32_t keep_alive_message_version = 1;
constexpr uint32_t end_backup_message_version = 1;
constexpr uint32_t get_object_message_version = 1;

} // namespace limestone::grpc::service
