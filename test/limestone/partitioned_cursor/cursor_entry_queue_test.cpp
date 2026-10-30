/*
 * Copyright 2025-2026 Project Tsurugi.
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
#include <gtest/gtest.h>
#include <thread>
#include <vector>
#include <variant>
#include <fstream>
#include <boost/filesystem.hpp>
#include "partitioned_cursor/cursor_entry_queue.h"
#include "log_entry/log_entry.h"
#include "partitioned_cursor/end_marker.h"

namespace limestone::testing {

using limestone::api::log_entry;
using limestone::api::write_version_type;
using limestone::internal::end_marker;
using limestone::internal::cursor_entry_queue;
using limestone::internal::cursor_entry_type;


// Generate a test log_entry (using only storage_id as the identifier)
log_entry create_log_entry(std::uint64_t storage_id) {
    return log_entry::make_normal_entry(storage_id, "key", "value", write_version_type{1234, 1});
}

class cursor_entry_queue_test : public ::testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(cursor_entry_queue_test, push_and_wait_and_pop_single_entry) {
    cursor_entry_queue queue(8);
    log_entry le = create_log_entry(1);

    std::thread producer([&]() {
        EXPECT_TRUE(queue.push(std::vector<log_entry>{le}));
    });

    auto result = queue.wait_and_pop();
    ASSERT_TRUE(std::holds_alternative<std::vector<log_entry>>(result));
    const auto& actual_batch = std::get<std::vector<log_entry>>(result);
    ASSERT_EQ(actual_batch.size(), 1);
    const auto& actual = actual_batch[0];
    EXPECT_EQ(actual.storage(), 1);

    producer.join();
}


TEST_F(cursor_entry_queue_test, push_before_calling_wait_and_pop) {
    cursor_entry_queue queue(8);

    // Push before calling wait_and_pop (no thread involved)
    log_entry le = create_log_entry(10);
    EXPECT_TRUE(queue.push(std::vector<log_entry>{le}));

    auto result = queue.wait_and_pop();
    ASSERT_TRUE(std::holds_alternative<std::vector<log_entry>>(result));
    const auto& actual_batch = std::get<std::vector<log_entry>>(result);
    ASSERT_EQ(actual_batch.size(), 1);
    const auto& actual = actual_batch[0];
    EXPECT_EQ(actual.storage(), 10);
}


TEST_F(cursor_entry_queue_test, wait_and_pop_before_push) {
    cursor_entry_queue queue(8);

    std::thread producer([&]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));  // Ensure wait_and_pop starts first
        log_entry le = create_log_entry(20);
        EXPECT_TRUE(queue.push(std::vector<log_entry>{le}));
    });

    auto result = queue.wait_and_pop();
    ASSERT_TRUE(std::holds_alternative<std::vector<log_entry>>(result));
    const auto& actual_batch = std::get<std::vector<log_entry>>(result);
    ASSERT_EQ(actual_batch.size(), 1);
    const auto& actual = actual_batch[0];
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
            EXPECT_TRUE(queue.push(std::vector<log_entry>{e}));
        }
    });

    for (std::size_t i = 0; i < sent.size(); ++i) {
        auto result = queue.wait_and_pop();
        ASSERT_TRUE(std::holds_alternative<std::vector<log_entry>>(result));
        const auto& batch = std::get<std::vector<log_entry>>(result);
        ASSERT_EQ(batch.size(), 1);
        EXPECT_EQ(batch[0].storage(), sent[i].storage());
    }

    producer.join();
}

}  // namespace limestone::testing
