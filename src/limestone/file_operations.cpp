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

#include "file_operations.h"
#include <unistd.h>  // for fsync
#include <cerrno>    // for errno
#include <cstring>   // for strerror
#include <sstream>   // for std::stringstream
#include <array>     // for std::array
#include <limestone/api/limestone_exception.h>

namespace limestone::internal {

using namespace limestone::api;

// -----------------------------------------
// C-style file operations
// -----------------------------------------

FILE* real_file_operations::fopen(const char* filename, const char* mode) {
    return ::fopen(filename, mode); // NOLINT(cppcoreguidelines-owning-memory)
}

size_t real_file_operations::fwrite(const void* ptr, size_t size, size_t count, FILE* stream) {
    return ::fwrite(ptr, size, count, stream);
}

int real_file_operations::fflush(FILE* stream) {
    return ::fflush(stream);
}

int real_file_operations::fclose(FILE* stream) {
    return ::fclose(stream); // NOLINT(cppcoreguidelines-owning-memory)
}

int real_file_operations::ferror(FILE* stream) {
    return ::ferror(stream);
}

int real_file_operations::fileno(FILE* stream) {
    return ::fileno(stream);
}

int real_file_operations::fsync(int fd) {
    return ::fsync(fd);
}

int real_file_operations::rename(const char* oldname, const char* newname) {
    return ::rename(oldname, newname);
}

int real_file_operations::unlink(const char* filename) {
    return ::unlink(filename);
}

// -----------------------------------------
// C++-style file operations
// -----------------------------------------

std::unique_ptr<std::ifstream> real_file_operations::open_ifstream(const std::string& path) {
    return std::make_unique<std::ifstream>(path);
}

bool real_file_operations::getline(std::ifstream& file, std::string& line) {
    return static_cast<bool>(std::getline(file, line));
}

bool real_file_operations::has_error(std::ifstream& file) {
    return file.fail();
}

bool real_file_operations::is_eof(std::ifstream& file) {
    return file.eof();
}


// -----------------------------------------
// Boost filesystem operations
// -----------------------------------------


bool real_file_operations::exists(const boost::filesystem::path& p, boost::system::error_code& ec) {
    return boost::filesystem::exists(p, ec);
}

}  // namespace limestone::internal


