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
#include <memory>
#include <fstream>

#include <boost/filesystem.hpp>
#include <boost/system/error_code.hpp>
namespace limestone::internal {

/**
 * @brief Abstract interface for mocking file operations in test environments.
 *
 * This interface abstracts file operations to simulate file handling through mocking 
 * in test environments. It provides unified methods for handling both C-style file 
 * operations (e.g., fopen, fwrite, fflush) and C++-style file operations 
 * (e.g., std::ifstream).
 *
 * Implementing classes like `real_file_operations` use the C standard library and 
 * C++ iostreams to perform actual file operations.
 *
 * It also supports Boost filesystem operations, allowing for the mocking of 
 * cross-platform file system paths and error code handling.
 */
class file_operations {
public:
    virtual ~file_operations() = default;
    file_operations() = default;
    file_operations(const file_operations&) = delete;
    file_operations& operator=(const file_operations&) = delete;
    file_operations(file_operations&&) = delete;
    file_operations& operator=(file_operations&&) = delete;

    // -----------------------------------------
    // C-style file operations
    // -----------------------------------------

    // Opens a file and returns a FILE pointer
    virtual FILE* fopen(const char* filename, const char* mode) = 0;

    // Writes data to a file
    virtual size_t fwrite(const void* ptr, size_t size, size_t count, FILE* stream) = 0;

    // Reads data from a file
    virtual size_t fread(void* ptr, size_t size, size_t count, FILE* stream) = 0;

    // Flushes the output buffer of a file
    virtual int fflush(FILE* stream) = 0;

    // Closes a file
    virtual int fclose(FILE* stream) = 0;

    // Gets the error number for a file stream
    virtual int ferror(FILE* stream) = 0;

    // Gets the file descriptor for a file stream
    virtual int fileno(FILE* stream) = 0;

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
    virtual bool getline(std::ifstream& file, std::string& line) = 0;

    // Checks if the file stream reached EOF
    virtual bool is_eof(std::ifstream& file) = 0;

    // Checks if the file stream is open
    virtual bool is_open(std::ifstream& file) = 0;

    // Checks if there's an error in the file stream
    virtual bool has_error(std::ifstream& file) = 0;


    // -----------------------------------------
    // Boost filesystem operations
    // -----------------------------------------

    // Checks if a file or directory exists (Boost)
    virtual bool exists(const boost::filesystem::path& p, boost::system::error_code& ec) = 0;

    // Advances the directory iterator to the next element (Boost)
    virtual void directory_iterator_next(boost::filesystem::directory_iterator& it, boost::system::error_code& ec) = 0;

    // Renames a file or directory (Boost)
    virtual void rename(const boost::filesystem::path& old_path, const boost::filesystem::path& new_path, boost::system::error_code& ec) = 0;

    // Copies a file from source to destination (Boost)
    virtual void copy_file(const boost::filesystem::path& source, const boost::filesystem::path& destination, boost::system::error_code& ec) = 0;

    // Removes a file or directory (Boost)
    virtual void remove(const boost::filesystem::path& path, boost::system::error_code& ec) = 0;

    // Creates a directory (Boost)
    virtual void create_directory(const boost::filesystem::path& path, boost::system::error_code& ec) = 0;

    // Creates directories recursively (Boost)
    virtual void create_directories(const boost::filesystem::path& path, boost::system::error_code& ec) = 0;

};

class real_file_operations : public file_operations {
public:
    FILE* fopen(const char* filename, const char* mode) override;
    size_t fwrite(const void* ptr, size_t size, size_t count, FILE* stream) override;
    size_t fread(void* ptr, size_t size, size_t count, FILE* stream) override;
    int fflush(FILE* stream) override;
    int fclose(FILE* stream) override;
    int ferror(FILE* stream) override;
    int fileno(FILE* stream) override;
    int fsync(int fd) override;
    int rename(const char* oldname, const char* newname) override;
    int unlink(const char* filename) override;

    std::unique_ptr<std::ifstream> open_ifstream(const std::string& path) override;
    bool getline(std::ifstream& file, std::string& line) override;
    bool is_eof(std::ifstream& file) override;
    bool is_open(std::ifstream& file) override;
    bool has_error(std::ifstream& file) override;

    bool exists(const boost::filesystem::path& p, boost::system::error_code& ec) override;
    void directory_iterator_next(boost::filesystem::directory_iterator& it, boost::system::error_code& ec) override;
    void rename(const boost::filesystem::path& old_path, const boost::filesystem::path& new_path, boost::system::error_code& ec) override;

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
    void copy_file(const boost::filesystem::path& source, const boost::filesystem::path& destination, boost::system::error_code& ec) override;
    void remove(const boost::filesystem::path& path, boost::system::error_code& ec) override;
    void create_directory(const boost::filesystem::path& path, boost::system::error_code& ec) override;
    void create_directories(const boost::filesystem::path& path, boost::system::error_code& ec) override;
};

}  // namespace limestone::internal

