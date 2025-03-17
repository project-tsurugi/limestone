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
#include <boost/system/error_code.hpp>


namespace limestone::api {

enum class exception_type {
    blob_error,
    fatal_error,
    initialization_failure 
};

class limestone_exception : public std::runtime_error {
public:
    explicit limestone_exception(const exception_type type, const std::string& message) noexcept
        : std::runtime_error(message), type_(type)  {}

    explicit limestone_exception(const exception_type type, const std::string& message, int error_code) noexcept
        : std::runtime_error(message), type_(type), error_code_(error_code) {}

    [[nodiscard]] int error_code() const noexcept { return error_code_; }
    [[nodiscard]] exception_type type() const noexcept {return type_;}

private:
    exception_type type_;
    int error_code_{0};
};

class limestone_io_exception : public limestone_exception {
public:
    // Constructor that takes an error message and errno as arguments (int)
    explicit limestone_io_exception(const exception_type type, const std::string& message, int error_code) noexcept
        : limestone_exception(type, message, error_code) {}

    // Constructor that takes an error message and boost::system::error_code as arguments
    explicit limestone_io_exception(const exception_type type, const std::string& message, const boost::system::error_code& error_code) noexcept
        : limestone_exception(type, message, error_code.value()) {}

    // Helper function to format the error message for int error_code
    static std::string format_message(const std::string& message, int error_code) noexcept{
        // Retrieve the system error message corresponding to errno
        std::string errno_str = std::strerror(error_code);
        // Format the complete error message
        return "I/O Error (" + errno_str + "): " + message + " (errno = " + std::to_string(error_code) + ")";
    }

    // Helper function to format the error message for boost::system::error_code
    static std::string format_message(const std::string& message, const boost::system::error_code& error_code) noexcept {
        return format_message(message, error_code.value());
    }
};

class limestone_blob_exception : public limestone_exception {
public:
    explicit limestone_blob_exception(const exception_type type, const std::string& message, int error_code) noexcept
        : limestone_exception(type, message, error_code) {}

    explicit limestone_blob_exception(const exception_type type, const std::string& message, const boost::system::error_code& error_code) noexcept
        : limestone_exception(type, message, error_code.value()) {}

    static std::string format_message(const std::string& message, int error_code) noexcept {
        std::string errno_str = std::strerror(error_code);
        return "Blob Error (" + errno_str + "): " + message + " (errno = " + std::to_string(error_code) + ")";
    }

    static std::string format_message(const std::string& message, const boost::system::error_code& error_code) noexcept {
        return format_message(message, error_code.value());
    }
};

} // namespace limestone::api