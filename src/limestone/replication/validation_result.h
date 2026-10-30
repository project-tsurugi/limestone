/*
 * Copyright 2025-2026 Project Tsurugi.
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

namespace limestone::replication {

class validation_result {
public:
    static validation_result success() noexcept;
    static validation_result error(uint16_t code, std::string message);

    [[nodiscard]] bool ok() const noexcept;
    [[nodiscard]] uint16_t error_code() const noexcept;
    [[nodiscard]] const std::string& error_message() const noexcept;

private:
    validation_result(bool ok, uint16_t code, std::string message);

    bool ok_;
    uint16_t error_code_;
    std::string error_message_;
};

// TODO: Replace dummy error codes with appropriate values.

} // namespace limestone::replication
