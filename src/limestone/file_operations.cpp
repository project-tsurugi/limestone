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
#include <limestone/api/limestone_exception.h>

namespace limestone::internal {

using namespace limestone::api;

FILE* real_file_operations::open_file(const char* filename, const char* mode) {
    return ::fopen(filename, mode);
}

size_t real_file_operations::write_file(const void* ptr, size_t size, size_t count, FILE* stream) {
    return ::fwrite(ptr, size, count, stream);
}

int real_file_operations::flush_file(FILE* stream) {
    return ::fflush(stream);
}

int real_file_operations::close_file(FILE* stream) {
    return ::fclose(stream);
}

int real_file_operations::sync_file(int fd) {
    return ::fsync(fd);
}

char* real_file_operations::read_line(char* buffer, int size, FILE* stream) {
    return ::fgets(buffer, size, stream);
}

bool real_file_operations::has_error(FILE* stream) {
    return ::ferror(stream) != 0;
}

bool real_file_operations::is_eof(FILE* stream) {
    return ::feof(stream) != 0;
}

std::string real_file_operations::read_line(FILE* stream, int& error_code) {
    std::string line;
    char buffer[1024];

    while (true) {
        char* result = fgets(buffer, sizeof(buffer), stream);
        int ret = errno;
        if (result) {
            std::string chunk(buffer);

            // Append the chunk to the line
            line += chunk;

            // Check if the chunk contains a newline character
            size_t pos;
            if ((pos = line.find('\n')) != std::string::npos) {
                // Handle CRLF case
                if (pos > 0 && line[pos - 1] == '\r') {
                    line.erase(pos - 1); // Remove the trailing '\r'
                } else {
                    line.erase(pos); // Remove the trailing '\n'
                }
                break; // Exit the loop after finding a newline
            }
        } else {
            // Handle end of file or error
            if (is_eof(stream)) {
                // If no newline was found and the line is empty, return the line
                if (line.empty()) {
                    return line;
                }
                break;
            }
            // Set the error code and throw an exception
            error_code = ret;
            return ""; // Return an empty string on error
        }
    }
    return line;
}


char* real_file_operations::fgets(char* buffer, int size, FILE* stream) {
    return ::fgets(buffer, size, stream);
}

}  // namespace limestone::internal


