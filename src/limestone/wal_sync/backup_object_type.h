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
#pragma once

#include <ostream>
#include <string_view>
#include "backup.pb.h"

namespace limestone::internal {

/**
 * @brief Backup object type shared across components.
 */
enum class backup_object_type {
    unspecified = 0,
    log = 1,
    snapshot = 2,
    blob = 3,
    metadata = 4,
};

namespace backup_object_type_helper {

/**
 * @brief Convert backup_object_type to string view.
 * @param value backup object type value
 * @return string representation
 */
[[nodiscard]] std::string_view to_string_view(backup_object_type value) noexcept;

/**
 * @brief Output operator for backup_object_type.
 * @param os output stream
 * @param value backup object type value
 * @return reference to the output stream
 */
std::ostream& operator<<(std::ostream& os, backup_object_type value);

/**
 * @brief Convert protobuf BackupObjectType to backup_object_type.
 * @param value protobuf enum value
 * @return converted backup_object_type
 */
[[nodiscard]] backup_object_type from_proto(limestone::grpc::proto::BackupObjectType value);

/**
 * @brief Convert backup_object_type to protobuf BackupObjectType.
 * @param value backup object type
 * @return protobuf enum value
 */
[[nodiscard]] limestone::grpc::proto::BackupObjectType to_proto(backup_object_type value) noexcept;

} // namespace backup_object_type_helper

using backup_object_type_helper::operator<<;

} // namespace limestone::internal
