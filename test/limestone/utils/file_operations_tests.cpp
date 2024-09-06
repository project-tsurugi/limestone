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

#include "gtest/gtest.h"
#include "file_operations.h"
#include <cstdio>  // For FILE*
#include <cerrno>  // For errno
#include <cstring> // For std::strlen

namespace limestone::testing {

using namespace limestone::internal;

// Helper function to write data to a file
void write_to_file(const char* filename, const char* data) {
    real_file_operations file_ops;
    FILE* file = file_ops.open_file(filename, "w+");
    ASSERT_NE(file, nullptr);
    file_ops.write_file(data, std::strlen(data), 1, file);
    file_ops.flush_file(file);
    file_ops.close_file(file);
}

TEST(real_file_operations_test, read_line_no_newline) {
    const char* filename = "test_file_no_newline.txt";
    const char* test_data = "This is a line without a newline at EOF";
    write_to_file(filename, test_data);

    real_file_operations file_ops;
    int error_code = 0;
    FILE* file = file_ops.open_file(filename, "r");
    ASSERT_NE(file, nullptr);

    std::string line = file_ops.read_line(file, error_code);
    ASSERT_EQ(error_code, 0);
    ASSERT_EQ(line, "This is a line without a newline at EOF");
}

TEST(real_file_operations_test, read_line_crlf) {
    const char* filename = "test_file_crlf.txt";
    const char* test_data = "Line with CRLF\r\nAnother line\r\n";
    write_to_file(filename, test_data);

    real_file_operations file_ops;
    int error_code = 0;
    FILE* file = file_ops.open_file(filename, "r");
    ASSERT_NE(file, nullptr);

    std::string line = file_ops.read_line(file, error_code);
    ASSERT_EQ(error_code, 0);
    ASSERT_EQ(line, "Line with CRLF");

    line = file_ops.read_line(file, error_code);
    ASSERT_EQ(error_code, 0);
    ASSERT_EQ(line, "Another line");
}

TEST(real_file_operations_test, read_line_lf) {
    const char* filename = "test_file_lf.txt";
    const char* test_data = "Line with LF\nAnother line\n";
    write_to_file(filename, test_data);

    real_file_operations file_ops;
    int error_code = 0;
    FILE* file = file_ops.open_file(filename, "r");
    ASSERT_NE(file, nullptr);

    std::string line = file_ops.read_line(file, error_code);
    ASSERT_EQ(error_code, 0);
    ASSERT_EQ(line, "Line with LF");

    line = file_ops.read_line(file, error_code);
    ASSERT_EQ(error_code, 0);
    ASSERT_EQ(line, "Another line");
}

// Test case for an empty file
TEST(real_file_operations_test, read_line_empty_file) {
    const char* filename = "test_file_empty.txt";
    const char* test_data = "";
    write_to_file(filename, test_data);

    real_file_operations file_ops;
    int error_code = 0;
    FILE* file = file_ops.open_file(filename, "r");
    ASSERT_NE(file, nullptr);

    std::string line = file_ops.read_line(file, error_code);
    ASSERT_EQ(error_code, 0);
    ASSERT_EQ(line, "");
}

// Test case for mixed CRLF and LF
TEST(real_file_operations_test, read_line_mixed_crlf_lf) {
    const char* filename = "test_file_mixed_crlf_lf.txt";
    const char* test_data = "Line with CRLF\r\nLine with LF\nAnother line\r\n";
    write_to_file(filename, test_data);

    real_file_operations file_ops;
    int error_code = 0;
    FILE* file = file_ops.open_file(filename, "r");
    ASSERT_NE(file, nullptr);

    std::string line = file_ops.read_line(file, error_code);
    ASSERT_EQ(error_code, 0);
    ASSERT_EQ(line, "Line with CRLF");

    line = file_ops.read_line(file, error_code);
    ASSERT_EQ(error_code, 0);
    ASSERT_EQ(line, "Line with LF");

    line = file_ops.read_line(file, error_code);
    ASSERT_EQ(error_code, 0);
    ASSERT_EQ(line, "Another line");
}

// Test case for an empty line with CRLF
TEST(real_file_operations_test, read_line_empty_line_crlf) {
    const char* filename = "test_file_empty_line_crlf.txt";
    const char* test_data = "Line with data\r\n\r\nAnother line\r\n";
    write_to_file(filename, test_data);

    real_file_operations file_ops;
    int error_code = 0;
    FILE* file = file_ops.open_file(filename, "r");
    ASSERT_NE(file, nullptr);

    std::string line = file_ops.read_line(file, error_code);
    ASSERT_EQ(error_code, 0);
    ASSERT_EQ(line, "Line with data");

    line = file_ops.read_line(file, error_code);
    ASSERT_EQ(error_code, 0);
    ASSERT_EQ(line, ""); // Expecting empty line

    line = file_ops.read_line(file, error_code);
    ASSERT_EQ(error_code, 0);
    ASSERT_EQ(line, "Another line");
}

// Test case for an empty line with LF
TEST(real_file_operations_test, read_line_empty_line_lf) {
    const char* filename = "test_file_empty_line_lf.txt";
    const char* test_data = "Line with data\n\nAnother line\n";
    write_to_file(filename, test_data);

    real_file_operations file_ops;
    int error_code = 0;
    FILE* file = file_ops.open_file(filename, "r");
    ASSERT_NE(file, nullptr);

    std::string line = file_ops.read_line(file, error_code);
    ASSERT_EQ(error_code, 0);
    ASSERT_EQ(line, "Line with data");

    line = file_ops.read_line(file, error_code);
    ASSERT_EQ(error_code, 0);
    ASSERT_EQ(line, "");  // Expecting empty line

    line = file_ops.read_line(file, error_code);
    ASSERT_EQ(error_code, 0);
    ASSERT_EQ(line, "Another line");
}
// Test for a single long line of 5000 characters
TEST(real_file_operations_test, read_long_line_5000_chars) {
    const char* filename = "test_file_5000_chars.txt";
    std::string long_line(5000, 'a'); // Create a line with 5000 'a' characters
    write_to_file(filename, long_line.c_str());

    real_file_operations file_ops;
    int error_code = 0;
    FILE* file = file_ops.open_file(filename, "r");
    ASSERT_NE(file, nullptr);

    std::string line = file_ops.read_line(file, error_code);
    ASSERT_EQ(line.size(), 5000);
    ASSERT_EQ(line, long_line);
    ASSERT_EQ(error_code, 0);

    file_ops.close_file(file);
}

// Test for lines of length buffer_size-3 to buffer_size+3
constexpr size_t buffer_size = 1024;

TEST(real_file_operations_test, read_line_various_lengths) {
    const char* filename = "test_file_various_lengths.txt";
    real_file_operations file_ops;
    FILE* file = file_ops.open_file(filename, "w+");
    ASSERT_NE(file, nullptr);

    // Test cases for different line lengths
    for (int i = -3; i <= 3; ++i) {
        size_t line_length = buffer_size + i;
        std::string line(line_length, 'b'); // Create a line with 'b' characters
        file_ops.write_file(line.c_str(), line.size(), 1, file);
        file_ops.write_file("\n", 1, 1, file); // Append newline
    }
    file_ops.flush_file(file);
    file_ops.close_file(file);

    // Reopen file and verify each line length
    file = file_ops.open_file(filename, "r");
    ASSERT_NE(file, nullptr);

    for (int i = -3; i <= 3; ++i) {
        size_t expected_length = buffer_size + i;
        int error_code = 0;
        std::string line = file_ops.read_line(file, error_code);
        ASSERT_EQ(line.size(), expected_length);
        ASSERT_EQ(line, std::string(expected_length, 'b'));
        ASSERT_EQ(error_code, 0);
    }

    file_ops.close_file(file);
}

class mock_file_operations : public real_file_operations {
public:
    // Constructor to set the failure call count
    mock_file_operations(int fail_on_call)
        : fail_on_call_(fail_on_call), call_count_(0) {}

    char* fgets(char* buffer, int size, FILE* stream) override {
        if (++call_count_ == fail_on_call_) {
            errno = EIO; // Simulate I/O error
            return nullptr;
        } else {
            return ::fgets(buffer, size, stream); // Normal behavior
        }
    }

private:
    int fail_on_call_;  // Specifies on which call to simulate failure
    int call_count_;    // Counts the number of calls to fgets
};
TEST(real_file_operations_test, first_fgets_error) {
    const char* filename = "test_file_first_fgets_error.txt";
    const char* test_data = "This line will not be read.";
    write_to_file(filename, test_data);

    mock_file_operations file_ops(1);
    int error_code = 0;
    FILE* file = file_ops.open_file(filename, "r");
    ASSERT_NE(file, nullptr);

    std::string line = file_ops.read_line(file, error_code);
    ASSERT_TRUE(line.empty()); // Expecting empty line due to fgets failure
    ASSERT_EQ(error_code, EIO); // Expecting EIO error code

    file_ops.close_file(file);
}

TEST(real_file_operations_test, second_fgets_error) {
    const char* filename = "test_file_second_fgets_error.txt";
    const char* test_data = "First line\nSecond line\n";
    write_to_file(filename, test_data);

    mock_file_operations file_ops(2);
    int error_code = 0;
    FILE* file = file_ops.open_file(filename, "r");
    ASSERT_NE(file, nullptr);

    // First call should succeed
    std::string line = file_ops.read_line(file, error_code);
    ASSERT_EQ(line, "First line"); // Expecting the first line to be read
    ASSERT_EQ(error_code, 0); // No error expected after the first fgets

    // Second call should fail
    line = file_ops.read_line(file, error_code);
    ASSERT_TRUE(line.empty()); // Expecting empty line due to fgets failure
    ASSERT_EQ(error_code, EIO); // Expecting EIO error code

    file_ops.close_file(file);
}

// Test case where the first fgets reads part of a very long line and the second fgets fails
TEST(real_file_operations_test, long_line_fgets_error_on_second_call) {
    const char* filename = "test_file_long_line_fgets_error.txt";

    std::string long_line(5000, 'a'); // Create a line with 5000 'a' characters
    write_to_file(filename, long_line.c_str());

    mock_file_operations file_ops(2); // Simulate error on the second fgets call
    int error_code = 0;
    FILE* file = file_ops.open_file(filename, "r");
    ASSERT_NE(file, nullptr);

    // First call should succeed and read the long line (part of it)
    std::string line = file_ops.read_line(file, error_code);
    ASSERT_TRUE(line.empty()); // Expecting empty line due to fgets failure
    ASSERT_EQ(error_code, EIO); // Expecting EIO error code

    file_ops.close_file(file);
}


}  // namespace limestone::internal
