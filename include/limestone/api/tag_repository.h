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

#include <unordered_map>
#include <vector>
#include <string>
#include <optional>

#include <limestone/api/epoch_tag.h>

namespace limestone::api::impl {
class datastore;
} // namespace limestone::api::impl

namespace limestone::api {

/**
 * @brief a repository of epoch tags
 */
class tag_repository {
public:
    /**
     * @brief returns a list of registered epoch tags
     * @attention this function is thread-safe.
     * @return a list of registered epoch tags
     */
    std::vector<epoch_tag>& list() noexcept;

    /**
     * @brief register the current epoch as epoch tag
     * @param name the name the string to identify the tag
     * @param comment a comment describing the tag
     * @attention this function is not thread-safe.
     * @note multiple epoch tags with the same name cannot be registered
     */
    void register_tag(std::string& name, std::string& comments) noexcept;

    /**
     * @brief return an epoch tag with the specified name
     * @param name the name of the epoch tag to be searched
     * @attention this function is not thread-safe.
     * @return the epoch_tag specified by name. If no such tag exists, std::nullopt is returned.
     * @attention this function is not thread-safe.
     */
    std::optional<epoch_tag> find(std::string_view name) const noexcept;


    /**
     * @brief remove epoch tag with specified name
     * @param name the name the string to identify the tag to be removed
     * @attention this function is not thread-safe.
     * @note if no such tag exists, do nothing
     */
    void unregister_tag(std::string_view name) noexcept;

private:
    std::unordered_map<std::string, epoch_tag> map_;

    std::vector<epoch_tag> list_;

    tag_repository() noexcept;

    friend class impl::datastore;
};

} // namespace limestone::api
