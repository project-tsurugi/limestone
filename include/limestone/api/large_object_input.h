/*
 * Copyright 2022-2023 tsurugi project.
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
#include <string>

namespace limestone::api {

class large_object_input {
public:
    
    explicit large_object_input(std::string buffer);
    explicit large_object_input(std::filesystem::path path);
    ~large_object_input();

    large_object_input(large_object_input const& other) = delete;
    large_object_input& operator=(large_object_input const& other) = delete;
    large_object_input(large_object_input&& other) noexcept = delete;
    large_object_input& operator=(large_object_input&& other) noexcept = delete;

    void locate(std::filesystem::path path);

    void detach();

};

} // namespace limestone::api
