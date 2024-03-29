/*
 * Copyright 2023-2023 Project Tsurugi.
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

#include <optional>
#include <set>
#include <string_view>
#include <vector>

#include <boost/filesystem.hpp>

#include <limestone/api/epoch_id_type.h>

namespace limestone::api {

class datastore;

/**
 * @brief information for prusik-era backup
 */
class backup_detail {
public:
    /**
     * @brief information for each backup target file
     */
    class entry {
    public:
        entry() noexcept = default;
        ~entry() = default;
        entry(entry const &) = default;
        entry& operator=(entry const &) = default;
        entry(entry &&) noexcept = default;
        entry& operator=(entry &&) noexcept = default;

        entry(boost::filesystem::path source_path, boost::filesystem::path destination_path, bool is_mutable, bool is_detached)
        : source_path_(std::move(source_path)), destination_path_(std::move(destination_path)), is_mutable_(is_mutable), is_detached_(is_detached) {}

        [[nodiscard]] boost::filesystem::path source_path() const { return source_path_; }
        [[nodiscard]] boost::filesystem::path destination_path() const { return destination_path_; }
        [[nodiscard]] bool is_mutable() const { return is_mutable_; }
        [[nodiscard]] bool is_detached() const { return is_detached_; }
    private:
        boost::filesystem::path source_path_ {};
        boost::filesystem::path destination_path_ {};
        bool is_mutable_ {};
        bool is_detached_ {};
    };

    std::string_view configuration_id() {
        return configuration_id_;
    }

    /**
     * @brief returns minimum epoch of log files
     * @note for LOG-0, always returns 0
     */
    [[nodiscard]] epoch_id_type log_start() const {
        return 0;
    }

    /**
     * @brief returns maximum epoch of log files
     */
    [[nodiscard]] epoch_id_type log_finish() const;

    /**
     * @brief returns maximum epoch that is included in dataabase image
     * @note for LOG-0, always returns nullopt of std::optional
     */
    [[nodiscard]] std::optional<epoch_id_type> image_finish() const {
        return std::nullopt;
    }

    const std::vector<backup_detail::entry>& entries() {
        return entries_;
    }

    [[nodiscard]] bool is_ready () const;

private:
    std::string_view configuration_id_;

    epoch_id_type log_finish_;

    backup_detail(std::vector<backup_detail::entry>&, epoch_id_type log_finish);

    std::vector<backup_detail::entry> entries_;

    friend class datastore;
};

enum class backup_type { standard, transaction };

} // namespace limestone::api

