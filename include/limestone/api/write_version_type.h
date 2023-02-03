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
#pragma once

#include <string>
#include <cstdint>

#include <limestone/api/epoch_id_type.h>


namespace limestone::api {

using epoch_t = std::int64_t;  // from shirakami/src/concurrency_control/silo/include/epoch.h

class write_version_type {
    friend class datastore;
    friend class log_channel;
    friend class log_entry;

public:
    write_version_type();
    write_version_type(epoch_id_type epoch_number, std::uint64_t minor_write_version);
    explicit write_version_type(const std::string& version_string);
    bool operator==(write_version_type value) const {
        return (this->epoch_number_ == value.epoch_number_) && (this->minor_write_version_ == value.minor_write_version_);
    }
    bool operator<(write_version_type value) const {
        if (this->epoch_number_ == value.epoch_number_) {
            return this->minor_write_version_ < value.minor_write_version_;
        }
        return this->epoch_number_ < value.epoch_number_;
    }

    epoch_id_type epoch_number() const noexcept { return epoch_number_; }
    std::uint64_t minor_write_version() const noexcept { return minor_write_version_; }

private:
    /**
     * @brief For PITR and major write version
     */
    epoch_id_type epoch_number_{};
        
    /**
     * @brief The order in the same epoch.
     * @apis bit layout:
     * 1 bits: 0 - short tx, 1 - long tx.
     * 63 bits: the order between short tx or long tx id.
     */
    std::uint64_t minor_write_version_{};
};

} // namespace limestone::api
