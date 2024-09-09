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

    // Opens a file and returns a FILE pointer
    virtual FILE* open(const char* filename, const char* mode) = 0;

    // Writes data to a file
    virtual size_t write(const void* ptr, size_t size, size_t count, FILE* stream) = 0;

    // Flushes the output buffer of a file
    virtual int flush(FILE* stream) = 0;

    // Closes a file
    virtual int close(FILE* stream) = 0;

    // Synchronizes a file's state with storage
    virtual int sync(int fd) = 0;

    // Reads a line from a file with a specific buffer size
    virtual char* read_line(char* buffer, int size, FILE* stream) = 0;

    // Checks if there is an error in the file stream
    virtual bool has_error(FILE* stream) = 0;

    // Checks if the file stream has reached end-of-file
    virtual bool is_eof(FILE* stream) = 0;

    // Reads a line from a file with dynamic buffer handling
    virtual std::string read_line(FILE* stream, int& error_code) = 0;

    // Reads a line from a file (fgets wrapper)
    virtual char* fgets(char* buffer, int size, FILE* stream) = 0;
};

class real_file_operations : public file_operations {
public:
    FILE* open(const char* filename, const char* mode) override;
    size_t write(const void* ptr, size_t size, size_t count, FILE* stream) override;
    int flush(FILE* stream) override;
    int close(FILE* stream) override;
    int sync(int fd) override;
    
    char* read_line(char* buffer, int size, FILE* stream) override;
    bool has_error(FILE* stream) override;
    bool is_eof(FILE* stream) override;

    std::string read_line(FILE* stream, int& error_code) override;
    char* fgets(char* buffer, int size, FILE* stream) override;
};

}  // namespace limestone::internal

