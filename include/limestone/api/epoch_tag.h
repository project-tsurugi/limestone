/*
 * Copyright 2022-2023 Project Tsurugi.
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

#include <string_view>
#include <string>
#include <chrono>

#include <limestone/api/epoch_id_type.h>

namespace limestone::api {

class tag_repository;

/**
 * @brief a tag associated with a specific epoch
 */
class epoch_tag {
public:
    /**
     * @brief returns the name of the tag
     * @attention this function is thread-safe.
     * @return the tag name
     */
    [[nodiscard]] std::string_view name() const noexcept { return name_; }

    /**
     * @brief returns the comment added to the tag
     * @attention this function is thread-safe.
     * @return the comment
     */
    [[nodiscard]] std::string_view comments() const noexcept { return comments_; }

    /**
     * @brief returns the epoch ID corresponding to the tag
     * @attention this function is thread-safe.
     * @return the epoch ID
     */
    [[nodiscard]] epoch_id_type epoch_id() const noexcept { return epoch_id_; }

    /**
     * @brief returns the time the tag was created
     * @attention this function is thread-safe.
     * @return the time the tag was created
     */
    [[nodiscard]] std::chrono::system_clock::time_point timestamp() const noexcept { return timestamp_; }

private:
    std::string name_;

    std::string comments_;

    epoch_id_type epoch_id_;

    std::chrono::system_clock::time_point timestamp_;

    epoch_tag(std::string_view name, std::string_view comments, epoch_id_type epoch_id, std::chrono::system_clock::time_point timestamp) noexcept
        : name_(std::string(name)), comments_(std::string(comments)), epoch_id_(epoch_id), timestamp_(timestamp) {
    }

    epoch_tag(std::string_view name, std::string_view comments) noexcept : epoch_tag(name, comments, 0, std::chrono::system_clock::now()) {
    }

    friend class tag_repository;
};
    
} // namespace limestone::api
