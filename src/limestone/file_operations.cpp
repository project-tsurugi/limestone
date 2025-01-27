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
    int ret = ::rename(oldname, newname);
    return ret;
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

bool real_file_operations::is_open(std::ifstream& file) {
    return file.is_open();
}


// -----------------------------------------
// Boost filesystem operations
// -----------------------------------------


bool real_file_operations::exists(const boost::filesystem::path& p, boost::system::error_code& ec) {
    return boost::filesystem::exists(p, ec);
}

void real_file_operations::directory_iterator_next(boost::filesystem::directory_iterator& it, boost::system::error_code& ec) {
    it.increment(ec);
}

void real_file_operations::rename(const boost::filesystem::path& old_path, const boost::filesystem::path& new_path, boost::system::error_code& ec) {
    boost::filesystem::rename(old_path, new_path, ec);
}

/**
 * @brief Copies a file from the source path to the destination path.
 * 
 * This implementation uses std::filesystem::copy_file instead of boost::filesystem::copy_file
 * to work around a known issue in Boost's copy_file implementation.
 * The issue is documented in the following GitHub issue:
 * https://github.com/boostorg/filesystem/issues/254
 * 
 * The issue arises when performing a copy operation across different filesystems
 * (e.g., from /dev/shm to /tmp), where Boost's copy_file may fail with errno = 18
 * (Invalid cross-device link). std::filesystem::copy_file handles this case correctly.
 * 
 * @param source The source file path (Boost filesystem path).
 * @param destination The destination file path (Boost filesystem path).
 * @param ec A Boost error code object to capture any error that occurs during the operation.
 */
void real_file_operations::copy_file(const boost::filesystem::path& source, const boost::filesystem::path& destination, boost::system::error_code& ec) {
    // Standard error code to handle std::filesystem operations
    std::error_code std_ec;

    // Convert Boost paths to std::filesystem paths
    std::filesystem::path std_source(source.string());
    std::filesystem::path std_destination(destination.string());

    // Use std::filesystem::copy_file with overwrite_existing option
    std::filesystem::copy_file(std_source, std_destination, std::filesystem::copy_options::overwrite_existing, std_ec);

    // Convert std::error_code to Boost error_code
    ec = boost::system::error_code(std_ec.value(), boost::system::generic_category());
}

void real_file_operations::remove(const boost::filesystem::path& path, boost::system::error_code& ec) {
    boost::filesystem::remove(path, ec);
}

void real_file_operations::create_directory(const boost::filesystem::path& path, boost::system::error_code& ec) {
    boost::filesystem::create_directory(path, ec);
}

void real_file_operations::create_directories(const boost::filesystem::path& path, boost::system::error_code& ec) {
    boost::filesystem::create_directories(path, ec);
}

}  // namespace limestone::internal


