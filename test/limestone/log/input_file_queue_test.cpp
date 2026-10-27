/*
 * Copyright 2022-2026 Project Tsurugi.
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
#include <atomic>
#include <chrono>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include <boost/filesystem.hpp>

#include "test_root.h"

#include "input_file_queue.h"

namespace limestone::testing {

using limestone::internal::input_file_queue;
using limestone::internal::scan_input;

/// the path of the result of pop() (nullopt as it is)
std::optional<boost::filesystem::path> path_of(const std::optional<scan_input>& input) {
    if (! input) {
        return std::nullopt;
    }
    return input->path();
}

/// calls pop() once and returns its path
std::optional<boost::filesystem::path> pop_path(input_file_queue& queue) {
    return path_of(queue.pop());
}

// a helper that calls pop() once in another thread, to observe whether it has returned
class popper {
public:
    explicit popper(input_file_queue& queue) : thread_([this, &queue] {
        result_ = path_of(queue.pop());
        returned_ = true;
    }) {}
    ~popper() { thread_.join(); }
    popper(const popper&) = delete;
    popper& operator=(const popper&) = delete;
    popper(popper&&) = delete;
    popper& operator=(popper&&) = delete;

    /// true if pop() has not returned after a short wait
    bool still_waiting() {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        return ! returned_;
    }
    /// waits for pop() to return and returns its result
    std::optional<boost::filesystem::path> result() {
        while (! returned_) {
            std::this_thread::yield();
        }
        return result_;
    }

private:
    std::optional<boost::filesystem::path> result_{};
    std::atomic<bool> returned_{false};
    std::thread thread_;
};

// the paths are popped in the order of push, and nullopt comes after the rest once closed
TEST(input_file_queue_test, pop_in_push_order_then_nullopt_after_close) {
    input_file_queue queue;
    queue.push("a");
    queue.push("b");
    queue.close();
    EXPECT_EQ(pop_path(queue), boost::filesystem::path("a"));
    EXPECT_EQ(pop_path(queue), boost::filesystem::path("b"));
    EXPECT_EQ(pop_path(queue), std::nullopt);
    EXPECT_EQ(pop_path(queue), std::nullopt);  // nullopt however many times it is called
    EXPECT_FALSE(queue.aborted());
}

// pop() waits while empty and not closed, and wakes up on push
TEST(input_file_queue_test, pop_waits_until_push) {
    input_file_queue queue;
    popper p(queue);
    EXPECT_TRUE(p.still_waiting());
    queue.push("a");
    EXPECT_EQ(p.result(), boost::filesystem::path("a"));
}

// a pop() waiting on an empty queue wakes up with nullopt on close
TEST(input_file_queue_test, pop_wakes_on_close) {
    input_file_queue queue;
    popper p(queue);
    EXPECT_TRUE(p.still_waiting());
    queue.close();
    EXPECT_EQ(p.result(), std::nullopt);
}

// abort discards the rest, and every pop() afterwards, including a waiting one, returns nullopt immediately
TEST(input_file_queue_test, abort_discards_the_rest_and_wakes) {
    input_file_queue queue;
    queue.push("a");
    EXPECT_EQ(pop_path(queue), boost::filesystem::path("a"));
    popper p(queue);
    EXPECT_TRUE(p.still_waiting());
    queue.push("b");  // the abort may come before this is popped, so the result is b or nullopt
    queue.abort();
    const auto popped = p.result();
    EXPECT_TRUE(! popped || *popped == boost::filesystem::path("b"));
    EXPECT_TRUE(queue.aborted());
    EXPECT_EQ(pop_path(queue), std::nullopt);
    queue.push("c");  // a push after abort is discarded
    EXPECT_EQ(pop_path(queue), std::nullopt);
}

// abort after close takes effect too (no thread waits on a closed empty queue, but aborted() becomes true)
TEST(input_file_queue_test, abort_after_close) {
    input_file_queue queue;
    queue.push("a");
    queue.close();
    queue.abort();
    EXPECT_TRUE(queue.aborted());
    EXPECT_EQ(pop_path(queue), std::nullopt);
}

// push after close is FATAL
TEST(input_file_queue_test, push_after_close_is_fatal) {
    input_file_queue queue;
    queue.close();
    EXPECT_DEATH(queue.push("a"), R"(push\(\) after close\(\))");
}

// an input popped out of the queue is stopped by abort, and one made alone is never stopped
TEST(input_file_queue_test, popped_input_is_stopped_by_abort) {
    input_file_queue queue;
    queue.push("a");
    const std::optional<scan_input> input = queue.pop();
    ASSERT_TRUE(input);
    EXPECT_EQ(input->path(), boost::filesystem::path("a"));
    EXPECT_FALSE(input->stopped());
    queue.close();
    EXPECT_FALSE(input->stopped());
    queue.abort();
    EXPECT_TRUE(input->stopped());
    const scan_input alone("b");
    EXPECT_FALSE(alone.stopped());
}

// each path is popped exactly once even with multiple popping threads
TEST(input_file_queue_test, each_path_is_popped_once_across_threads) {
    input_file_queue queue;
    constexpr int count = 1000;
    std::atomic<int> popped{0};
    std::vector<std::thread> threads;
    threads.reserve(4);
    for (int t = 0; t < 4; ++t) {
        threads.emplace_back([&] {
            while (queue.pop()) {
                ++popped;
            }
        });
    }
    for (int i = 0; i < count; ++i) {
        queue.push(std::to_string(i));
    }
    queue.close();
    for (auto& thread : threads) {
        thread.join();
    }
    EXPECT_EQ(popped, count);
}

}  // namespace limestone::testing
