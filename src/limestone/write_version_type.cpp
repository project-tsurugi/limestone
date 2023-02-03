/*
 * Copyright 2022-2022 tsurugi project.
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
#include <limestone/api/write_version_type.h>

#include "log_entry.h"

namespace limestone::api {

write_version_type::write_version_type() = default;
write_version_type::write_version_type(epoch_id_type epoch_number, std::uint64_t minor_write_version)
    : epoch_number_(epoch_number), minor_write_version_(minor_write_version) {
}
write_version_type::write_version_type(const std::string& version_string)
    : epoch_number_(impl::log_entry::write_version_epoch_number(version_string)), minor_write_version_(impl::log_entry::write_version_minor_write_version(version_string)) {
}

} // namespace limestone::api
