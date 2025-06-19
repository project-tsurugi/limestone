#include <gtest/gtest.h>
#include <memory>
#include <vector>
#include "partitioned_cursor/partitioned_cursor.h"
#include "partitioned_cursor/cursor_entry_queue.h"
#include "partitioned_cursor/end_marker.h"
#include "log_entry.h"
#include <limestone/api/storage_id_type.h>
#include <limestone/api/write_version_type.h>

namespace limestone::testing {

using limestone::internal::partitioned_cursor;
using limestone::internal::cursor_entry_queue;
using limestone::internal::end_marker;
using limestone::api::log_entry;
using limestone::api::storage_id_type;
using limestone::api::write_version_type;

static const std::string tmp_file_path = "/tmp/test_log_entry.tmp";

class partitioned_cursor_test : public ::testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {
        // Clean up the temporary file after each test
        boost::filesystem::remove(tmp_file_path);
    }

    // Create a log_entry instance by writing a normal entry into a temporary file
    // and reading it back. Uses the log_entry::write() and log_entry::read() methods.
    log_entry create_normal_log_entry(storage_id_type storage,
                                      const std::string &key,
                                      const std::string &value,
                                      const write_version_type &wversion) {
        FILE* out = std::fopen(tmp_file_path.c_str(), "wb");
        if (!out) {
            throw std::runtime_error("Failed to open temporary file for writing.");
        }
        // Write a normal log entry.
        log_entry::write(out, storage, key, value, wversion);
        std::fclose(out);

        std::ifstream in(tmp_file_path, std::ios::binary);
        if (!in.is_open()) {
            throw std::runtime_error("Failed to open temporary file for reading.");
        }
        log_entry entry;
        bool rc = entry.read(in);
        in.close();
        boost::filesystem::remove(tmp_file_path);
        if (!rc) {
            throw std::runtime_error("Failed to read log entry from temporary file.");
        }
        return entry;
    }
};

TEST_F(partitioned_cursor_test, single_entry_and_end_marker) {
    auto queue = std::make_shared<cursor_entry_queue>(8);
    partitioned_cursor cursor(queue);

    // Push one log_entry
    EXPECT_TRUE(queue->push(create_normal_log_entry(123, "key", "value", {12, 34})));

    // Push end_marker
    EXPECT_TRUE(queue->push(end_marker{true, ""}));

    // First next() should return true
    EXPECT_TRUE(cursor.next());
    EXPECT_EQ(cursor.storage(), 123);
    EXPECT_EQ(cursor.type(), log_entry::entry_type::normal_entry);
    EXPECT_EQ(cursor.blob_ids().size(), 0);
    std::string key;
    cursor.key(key);
    EXPECT_EQ(key, "key");
    std::string value;
    cursor.value(value);
    EXPECT_EQ(value, "value");

    // Second next() should return false (end_marker received)
    EXPECT_FALSE(cursor.next());
}

TEST_F(partitioned_cursor_test, next_returns_false_after_cursor_closed) {
    auto queue = std::make_shared<cursor_entry_queue>(8);
    partitioned_cursor cursor(queue);

    // Push multiple log_entries
    EXPECT_TRUE(queue->push(create_normal_log_entry(123, "key1", "value1", {12, 34})));
    EXPECT_TRUE(queue->push(create_normal_log_entry(456, "key2", "value2", {56, 78})));

    cursor.close();
    EXPECT_FALSE(cursor.next());
}

}  // namespace limestone::testing
