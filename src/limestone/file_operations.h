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

#include <cstdio>
#include <string> 
#include <boost/filesystem.hpp>
#include <boost/system/error_code.hpp>
namespace limestone::internal {

/**
 * @brief Interface and implementation for file operations abstraction.
 *
 * This header defines the abstract interface `file_operations` for handling file
 * operations such as opening, writing, flushing, closing, and syncing files.
 * It also provides a concrete class `real_file_operations`, which uses the C standard
 * library functions (e.g., fopen, fwrite, fflush, fclose, and fsync) to perform
 * actual file operations.
 *
 * The purpose of this abstraction is to enable dependency injection for file operations,
 * allowing easier testing and mocking in various parts of the system.
 * For instance, in tests, a mock implementation of this interface can be used to simulate
 * file operations without actually interacting with the file system.
 *
 * Note: This file is intended to be used in multiple parts of the project, and it
 * should be placed alongside other source files for consistency.
 */
class file_operations {
public:
    virtual ~file_operations() = default;

    // -----------------------------------------
    // C-style file operations
    // -----------------------------------------

    // Opens a file and returns a FILE pointer
    virtual FILE* fopen(const char* filename, const char* mode) = 0;

    // Writes data to a file
    virtual size_t fwrite(const void* ptr, size_t size, size_t count, FILE* stream) = 0;

    // Flushes the output buffer of a file
    virtual int fflush(FILE* stream) = 0;

    // Closes a file
    virtual int fclose(FILE* stream) = 0;

    // Synchronizes a file's state with storage
    virtual int fsync(int fd) = 0;

    // Renames a file
    virtual int rename(const char* oldname, const char* newname) = 0;

    // Unlinks (deletes) a file
    virtual int unlink(const char* filename) = 0;

    // -----------------------------------------
    // C++-style file operations
    // -----------------------------------------

    // Opens an input file stream
    virtual std::unique_ptr<std::ifstream> open_ifstream(const std::string& path) = 0;

    // Reads a line from an input file stream
    virtual bool read_line(std::ifstream& file, std::string& line) = 0;

    // Checks if the file stream reached EOF
    virtual bool is_eof(std::ifstream& file) = 0;

    // Checks if there's an error in the file stream
    virtual bool has_error(std::ifstream& file) = 0;

    // -----------------------------------------
    // Boost filesystem operations
    // -----------------------------------------

    // Checks if a file or directory exists (Boost)
    virtual bool exists(const boost::filesystem::path& p, boost::system::error_code& ec) = 0;
};

class real_file_operations : public file_operations {
public:
    FILE* fopen(const char* filename, const char* mode) override;
    size_t fwrite(const void* ptr, size_t size, size_t count, FILE* stream) override;
    int fflush(FILE* stream) override;
    int fclose(FILE* stream) override;
    int fsync(int fd) override;
    int rename(const char* oldname, const char* newname) override;
    int unlink(const char* filename) override;

    std::unique_ptr<std::ifstream> open_ifstream(const std::string& path) override;
    bool read_line(std::ifstream& file, std::string& line) override;
    bool is_eof(std::ifstream& file) override;
    bool has_error(std::ifstream& file) override;

    bool exists(const boost::filesystem::path& p, boost::system::error_code& ec) override;
};

}  // namespace limestone::internal

