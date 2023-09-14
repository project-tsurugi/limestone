/*
 * Copyright 2023-2023 tsurugi project.
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

#include <filesystem>
#include <string_view>

namespace limestone::api {

/**
 * @brief information for each backup target file
 */
class file_set_entry {
public:
    file_set_entry() noexcept = default;
    ~file_set_entry() = default;
    file_set_entry(file_set_entry const &) = default;
    file_set_entry& operator=(file_set_entry const &) = default;
    file_set_entry(file_set_entry &&) noexcept = default;
    file_set_entry& operator=(file_set_entry &&) noexcept = default;

    file_set_entry(std::filesystem::path source_path, std::filesystem::path destination_path, bool is_detached)
    : source_path_(std::move(source_path)), destination_path_(std::move(destination_path)), is_detached_(is_detached) {}

    [[nodiscard]] std::filesystem::path source_path() const { return source_path_; }
    [[nodiscard]] std::filesystem::path destination_path() const { return destination_path_; }
    [[nodiscard]] bool is_detached() const { return is_detached_; }

private:
    std::filesystem::path source_path_ {};
    std::filesystem::path destination_path_ {};
    bool is_detached_ {};
};

} // namespace limestone::api
