/*
 * Copyright 2023-2025 Project Tsurugi.
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

namespace limestone::internal {

/**
 * @brief A marker object indicating the end of a cursor data stream.
 *
 * This class is used to signal that no more entries will follow.
 * It encapsulates the completion status and an optional message.
 * Typically used as a value object passed through a queue.
 */
class end_marker {
public:
    end_marker() = default;
    explicit end_marker(bool s, std::string m = {})
        : success_(s), message_(std::move(m)) {}

    [[nodiscard]] bool success() const { return success_; }
    [[nodiscard]] const std::string& message() const { return message_; }

private:
    bool success_{true};
    std::string message_;
};


} // namespace limestone::internal
