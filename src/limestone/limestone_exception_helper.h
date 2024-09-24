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


inline std::string extract_filename(const std::string& path) {
    // Use boost::filesystem::path to handle file path and extract filename
    boost::filesystem::path p(path);
    return p.filename().string();
}

#define THROW_LIMESTONE_EXCEPTION(message) \
    { \
        LOG_LP(ERROR) << message; \
        throw limestone_exception(std::string(message) + " (at " + extract_filename(__FILE__) + ":" + std::to_string(__LINE__) + ")"); \
    }

#define THROW_LIMESTONE_IO_EXCEPTION(message, error_code) \
    { \
        std::string full_message = limestone_io_exception::format_message(message, error_code); \
        LOG_LP(ERROR) << full_message; \
        throw limestone_io_exception(full_message + " (at " + extract_filename(__FILE__) + ":" + std::to_string(__LINE__) + ")", error_code); \
    }

#define LOG_AND_THROW_EXCEPTION(message) \
    { \
        LOG_LP(ERROR) << message; \
        throw limestone_exception(std::string(message) + " (at " + extract_filename(__FILE__) + ":" + std::to_string(__LINE__) + ")"); \
    }

#define LOG_AND_THROW_IO_EXCEPTION(message, error_code) \
    { \
        std::string full_message = limestone_io_exception::format_message(message, error_code); \
        LOG_LP(ERROR) << full_message; \
        throw limestone_io_exception(full_message + " (at " + extract_filename(__FILE__) + ":" + std::to_string(__LINE__) + ")", error_code); \
    }




} // namespace limestone