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

#include <glog/logging.h>
#include <limestone/logging.h>
#include <boost/filesystem.hpp>

#include "logging_helper.h"
#include <limestone/api/limestone_exception.h>

#pragma once

namespace limestone {

using limestone::api::limestone_exception;
using limestone::api::limestone_io_exception;    

inline void throw_limestone_exception(const std::string& message, const char* file, int line) {
    throw limestone_exception(message + " (at " + std::filesystem::path(file).filename().string() + ":" + std::to_string(line) + ")");
}

inline void throw_limestone_io_exception(const std::string& message, int error_code, const char* file, int line) {
    throw limestone_io_exception(message + " (at " + std::filesystem::path(file).filename().string() + ":" + std::to_string(line) + ")", error_code);
}

inline void throw_limestone_io_exception(const std::string& message, const boost::system::error_code& error_code, const char* file, int line) {
    throw limestone_io_exception(message + " (at " + std::filesystem::path(file).filename().string() + ":" + std::to_string(line) + ")", error_code.value());
}

inline void log_and_throw_exception(const std::string& message, const char* file, int line) {
    LOG_LP(ERROR) << message;
    throw_limestone_exception(message, file, line);
}

inline void log_and_throw_io_exception(const std::string& message, int error_code, const char* file, int line) {
    std::string full_message = limestone_io_exception::format_message(message, error_code);
    LOG_LP(ERROR) << full_message;
    throw_limestone_io_exception(message, error_code, file, line);
}

inline void log_and_throw_io_exception(const std::string& message, const boost::system::error_code& error_code, const char* file, int line) {
    std::string full_message = limestone_io_exception::format_message(message, error_code.value());
    LOG_LP(ERROR) << full_message;
    throw_limestone_io_exception(message, error_code, file, line);
}

// NOLINTNEXTLINE
#define THROW_LIMESTONE_EXCEPTION(message) throw_limestone_exception(message, __FILE__, __LINE__)

// NOLINTNEXTLINE
#define THROW_LIMESTONE_IO_EXCEPTION(message, error_code) throw_limestone_io_exception(message, error_code, __FILE__, __LINE__)

// NOLINTNEXTLINE
#define LOG_AND_THROW_EXCEPTION(message) log_and_throw_exception(message, __FILE__, __LINE__)

// NOLINTNEXTLINE
#define LOG_AND_THROW_IO_EXCEPTION(message, error_code) log_and_throw_io_exception(message, error_code, __FILE__, __LINE__)

} // namespace limestone