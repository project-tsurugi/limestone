/*
 * Copyright 2022-2024 Project Tsurugi.
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

#include <cerrno>      
#include <cstring>     
#include <stdexcept>
#include <string>
#include <iostream>

namespace limestone::api {

class limestone_exception : public std::runtime_error {
public:
    explicit limestone_exception(const std::string& message)
        : std::runtime_error(message) {}

    explicit limestone_exception(const std::string& message, int error_code)
        : std::runtime_error(message), error_code_(error_code) {}

    int error_code() const noexcept { return error_code_; }

private:
    int error_code_{0};
};

class limestone_io_exception : public limestone_exception {
public:
    // Constructor that takes an error message and errno as arguments
    explicit limestone_io_exception(const std::string& message, int error_num)
        : limestone_exception(format_message(message, error_num), error_num) {}

private:
    // Helper function to format the error message
    static std::string format_message(const std::string& message, int error_num) {
        // Retrieve the system error message corresponding to errno
        std::string errno_str = std::strerror(error_num);

        // Format the complete error message
        return "I/O Error (" + errno_str + "): " + message + " (errno = " + std::to_string(error_num) + ")";

    }
};


} // namespace limestone::api