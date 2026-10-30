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
#include "replication/validation_result.h"

namespace limestone::replication {

validation_result validation_result::success() noexcept {
    return {true, 0, {}};
}

validation_result validation_result::error(uint16_t code, std::string message) {
    return {false, code, std::move(message)};
}

validation_result::validation_result(bool ok, uint16_t code, std::string message)
    : ok_(ok), error_code_(code), error_message_(std::move(message)) {}

bool validation_result::ok() const noexcept {
    return ok_;
}

uint16_t validation_result::error_code() const noexcept {
    return error_code_;
}

const std::string& validation_result::error_message() const noexcept {
    return error_message_;
}

} // namespace limestone::replication