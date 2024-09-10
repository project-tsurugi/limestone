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
#include <filesystem>

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
    explicit limestone_io_exception(const std::string& message, int error_code)
        : limestone_exception(format_message(message, error_code), error_code) {}

    // Helper function to format the error message (made public)
    static std::string format_message(const std::string& message, int error_code) {
        // Retrieve the system error message corresponding to errno
        std::string errno_str = std::strerror(error_code);

        // Format the complete error message
        return "I/O Error (" + errno_str + "): " + message + " (errno = " + std::to_string(error_code) + ")";
    }
};

// Macro to throw exceptions with file and line information

#define THROW_LIMESTONE_EXCEPTION(message) \
    throw limestone_exception(std::string(message) + " (at " + std::filesystem::path(__FILE__).filename().string() + ":" + std::to_string(__LINE__) + ")")

#define THROW_LIMESTONE_IO_EXCEPTION(message, error_code) \
    throw limestone_io_exception(std::string(message) + " (at " + std::filesystem::path(__FILE__).filename().string() + ":" + std::to_string(__LINE__) + ")", error_code)

#define LOG_AND_THROW_EXCEPTION(message) \
    { \
        LOG_LP(ERROR) << message; \
        THROW_LIMESTONE_EXCEPTION(message); \
    }

#define LOG_AND_THROW_IO_EXCEPTION(message, error_code) \
    { \
        std::string full_message = limestone_io_exception::format_message(message, error_code); \
        LOG_LP(ERROR) << full_message; \
        THROW_LIMESTONE_IO_EXCEPTION(message, error_code); \
    }


} // namespace limestone::api
