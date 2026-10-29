/*
 * Copyright 2024-2026 Project Tsurugi.
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
#include <algorithm>
#include <sstream>
#include <iomanip>
#include <cctype>
#include <iostream>
#include <limestone/logging.h>

#include <boost/filesystem.hpp>

#include "dblog_scan.h"
#include "internal.h"
#include "log_entry.h"

#include "test_root.h"
#include "testdata.h"

namespace limestone::testing {


std::string data_manifest(int persistent_format_version) {
    std::ostringstream ss;
    ss << "{ \"format_version\": \"1.0\", \"persistent_format_version\": " << persistent_format_version << " }";
    return ss.str();
}

void create_file(const boost::filesystem::path& path, std::string_view content) {
    boost::filesystem::ofstream strm{};
    strm.open(path, std::ios_base::out | std::ios_base::binary);
    strm.write(content.data(), content.size());
    strm.flush();
    LOG_IF(FATAL, !strm || !strm.is_open() || strm.bad() || strm.fail());
    strm.close();
}

std::string read_entire_file(const boost::filesystem::path& path) {
    boost::filesystem::ofstream strm{};
    strm.open(path, std::ios_base::in | std::ios_base::binary);
    assert(strm.good());
    std::ostringstream ss;
    ss << strm.rdbuf();
    strm.close();
    return ss.str();
}

void hexdump(std::string_view data, const std::string& name) {
    const size_t bytes_per_line = 16;

    if (!name.empty()) {
        std::cerr << name << ":\n";
    }

    for (size_t i = 0; i < data.size(); i += bytes_per_line) {
        std::cerr << std::setw(4) << std::setfill('0') << std::hex << i << ": ";

        // Output bytes in hexadecimal
        for (size_t j = 0; j < bytes_per_line; ++j) {
            if (i + j < data.size()) {
                std::cerr << std::setw(2) << static_cast<unsigned>(static_cast<unsigned char>(data[i + j])) << " ";
            } else {
                std::cerr << "   ";
            }
        }

        std::cerr << " ";

        // Output bytes as ASCII
        for (size_t j = 0; j < bytes_per_line; ++j) {
            if (i + j < data.size()) {
                unsigned char c = static_cast<unsigned char>(data[i + j]);
                if (std::isprint(c)) {
                    std::cerr << c;
                } else {
                    std::cerr << ".";
                }
            }
        }

        std::cerr << "\n";
    }
    std::cerr << std::dec;
}

} // namespace limestone::testing
