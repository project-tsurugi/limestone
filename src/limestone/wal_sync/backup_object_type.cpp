/*
 * Copyright 2022-2025 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <wal_sync/backup_object_type.h>

#include <cstddef>
#include <string>

#include <backup.pb.h>

namespace limestone::internal {

using limestone::grpc::proto::BackupObjectType;

namespace backup_object_type_helper {

/**
 * @brief Returns the string representation of the backup_object_type enum value.
 * @param value The backup_object_type enum value.
 * @return The string_view representing the enum value.
 */
std::string_view to_string_view(backup_object_type value) noexcept {
    switch (value) {
        case backup_object_type::unspecified:
            return "unspecified";
        case backup_object_type::log:
            return "log";
        case backup_object_type::snapshot:
            return "snapshot";
        case backup_object_type::blob:
            return "blob";
        case backup_object_type::metadata:
            return "metadata";
        default:
            return "unspecified";
    }
}

std::ostream& operator<<(std::ostream& os, backup_object_type value) {
    os << to_string_view(value);
    return os;
}

backup_object_type from_proto(limestone::grpc::proto::BackupObjectType value) {
    using limestone::grpc::proto::BackupObjectType;
    switch (value) {
        case BackupObjectType::UNSPECIFIED:
            return backup_object_type::unspecified;
        case BackupObjectType::LOG:
            return backup_object_type::log;
        case BackupObjectType::SNAPSHOT:
            return backup_object_type::snapshot;
        case BackupObjectType::BLOB:
            return backup_object_type::blob;
        case BackupObjectType::METADATA:
            return backup_object_type::metadata;
        case BackupObjectType::BackupObjectType_INT_MIN_SENTINEL_DO_NOT_USE_:
        case BackupObjectType::BackupObjectType_INT_MAX_SENTINEL_DO_NOT_USE_:
            // Sentinel values should not occur, but return unspecified for safety.
            return backup_object_type::unspecified;
    }
    return backup_object_type::unspecified;
}

limestone::grpc::proto::BackupObjectType to_proto(backup_object_type value) noexcept {
    using limestone::grpc::proto::BackupObjectType;
    switch (value) {
        case backup_object_type::log:
            return BackupObjectType::LOG;
        case backup_object_type::snapshot:
            return BackupObjectType::SNAPSHOT;
        case backup_object_type::blob:
            return BackupObjectType::BLOB;
        case backup_object_type::metadata:
            return BackupObjectType::METADATA;
        case backup_object_type::unspecified:
        default:
            return BackupObjectType::UNSPECIFIED;
    }
}

} // namespace backup_object_type_helper

} // namespace limestone::internal
