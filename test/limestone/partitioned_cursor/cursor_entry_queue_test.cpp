#include <gtest/gtest.h>
#include <thread>
#include <vector>
#include <variant>
#include <fstream>
#include <boost/filesystem.hpp>
#include "partitioned_cursor/cursor_entry_queue.h"
#include "log_entry.h"
#include "partitioned_cursor/end_marker.h"

namespace limestone::testing {

using limestone::api::log_entry;
using limestone::api::write_version_type;
using limestone::internal::end_marker;
using limestone::internal::cursor_entry_queue;
using limestone::internal::cursor_entry_type;


static const std::string tmp_file_path = "/tmp/test_log_entry.tmp";

// Generate a test log_entry (using only storage_id as the identifier)
log_entry create_log_entry(std::uint64_t storage_id) {
    {
        FILE* fp = std::fopen(tmp_file_path.c_str(), "wb");
        log_entry::write(fp, storage_id, "key", "value", write_version_type{1234, 1});
        std::fclose(fp);
    }

    std::ifstream ifs(tmp_file_path, std::ios::binary);
    log_entry entry;
    entry.read(ifs);
    boost::filesystem::remove(tmp_file_path);
    return entry;
}

class cursor_entry_queue_test : public ::testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {
        // Clean up any temporary files created during tests
        if (boost::filesystem::exists(tmp_file_path)) {
            boost::filesystem::remove(tmp_file_path);
        }
    }
};

TEST_F(cursor_entry_queue_test, push_and_wait_and_pop_single_entry) {
    cursor_entry_queue queue(8);
    log_entry le = create_log_entry(1);

    std::thread producer([&]() {
        EXPECT_TRUE(queue.push(le));
    });

    auto result = queue.wait_and_pop();
    ASSERT_TRUE(std::holds_alternative<log_entry>(result));
    const auto& actual = std::get<log_entry>(result);
    EXPECT_EQ(actual.storage(), 1);
    producer.join();
}

TEST_F(cursor_entry_queue_test, push_before_calling_wait_and_pop) {
    cursor_entry_queue queue(8);

    // Push before calling wait_and_pop (no thread involved)
    log_entry le = create_log_entry(10);
    EXPECT_TRUE(queue.push(le));

    auto result = queue.wait_and_pop();
    ASSERT_TRUE(std::holds_alternative<log_entry>(result));
    const auto& actual = std::get<log_entry>(result);
    EXPECT_EQ(actual.storage(), 10);
}

TEST_F(cursor_entry_queue_test, wait_and_pop_before_push) {
    cursor_entry_queue queue(8);

    std::thread producer([&]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));  // Ensure wait_and_pop starts first
        log_entry le = create_log_entry(20);
        EXPECT_EQ(le.storage(), 20);
        EXPECT_TRUE(queue.push(le));
    });

    auto result = queue.wait_and_pop();
    ASSERT_TRUE(std::holds_alternative<log_entry>(result));
    const auto& actual = std::get<log_entry>(result);
    EXPECT_EQ(actual.storage(), 20);

    producer.join();
}

TEST_F(cursor_entry_queue_test, push_and_wait_and_pop_end_marker) {
    cursor_entry_queue queue(8);

    end_marker marker{false, "error occurred"};
    std::thread producer([&]() {
        EXPECT_TRUE(queue.push(marker));
    });

    auto result = queue.wait_and_pop();
    ASSERT_TRUE(std::holds_alternative<end_marker>(result));
    const auto& actual = std::get<end_marker>(result);
    EXPECT_FALSE(actual.success());
    EXPECT_EQ(actual.message(), "error occurred");
    producer.join();
}

TEST_F(cursor_entry_queue_test, multiple_entries) {
    cursor_entry_queue queue(16);
    std::vector<log_entry> sent;
    for (std::size_t i = 0; i < 5; ++i) {
        sent.emplace_back(create_log_entry(i + 10));
    }

    std::thread producer([&]() {
        for (const auto& e : sent) {
            EXPECT_TRUE(queue.push(e));
        }
    });

    for (std::size_t i = 0; i < sent.size(); ++i) {
        auto result = queue.wait_and_pop();
        ASSERT_TRUE(std::holds_alternative<log_entry>(result));
        const auto& actual = std::get<log_entry>(result);
        EXPECT_EQ(actual.storage(), sent[i].storage());
    }

    producer.join();
}

TEST_F(cursor_entry_queue_test, push_all_pushes_up_to_available_space) {
    cursor_entry_queue queue(4);  // Small queue
    std::vector<cursor_entry_type> entries;
    for (std::size_t i = 0; i < 5; ++i) {
        entries.emplace_back(create_log_entry(100 + i));
    }

    std::size_t pushed = queue.push_all(entries);

    // Since the queue capacity is 4, at most 4 entries should be pushed
    EXPECT_LE(pushed, 4);
    EXPECT_GT(pushed, 0);

    for (std::size_t i = 0; i < pushed; ++i) {
        auto result = queue.wait_and_pop();
        ASSERT_TRUE(std::holds_alternative<log_entry>(result));
        const auto& actual = std::get<log_entry>(result);
        EXPECT_EQ(actual.storage(), 100 + i);
    }
}

TEST_F(cursor_entry_queue_test, push_all_returns_zero_when_queue_full) {
    cursor_entry_queue queue(2);

    // First, fill up the queue
    auto le1 = create_log_entry(1);
    auto le2 = create_log_entry(2);

    EXPECT_TRUE(queue.push(le1));
    EXPECT_TRUE(queue.push(le2));

    // Prepare entries to push
    std::vector<cursor_entry_type> entries;
    entries.emplace_back(create_log_entry(3));
    entries.emplace_back(create_log_entry(4));

    // push_all should not be able to push anything (queue is full)
    std::size_t pushed = queue.push_all(entries);
    EXPECT_EQ(pushed, 0);
}

}  // namespace limestone::testing
