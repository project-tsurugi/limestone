#include "partitioned_cursor/cursor_distributor.h"

#include <gtest/gtest.h>

#include <chrono>
#include <deque>
#include <future>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "cursor_impl_base.h"
#include "partitioned_cursor/cursor_entry_queue.h"
#include "partitioned_cursor/end_marker.h"

namespace limestone::testing {

namespace {

using namespace limestone::internal;

class testable_cursor_distributor : public internal::cursor_distributor {
public:
    using internal::cursor_distributor::cursor_distributor;
    using internal::cursor_distributor::set_on_complete;
    using internal::cursor_distributor::try_push_all_to_queues;
};


class mock_cursor : public cursor_impl_base {
public:
    explicit mock_cursor(std::vector<api::log_entry> entries)
        : entries_(std::move(entries)), index_(0) {}

    bool next() override {
        if (index_ < entries_.size()) {
            current_ = entries_[index_++];
            return true;
        }
        return false;
    }

    api::storage_id_type storage() const noexcept override {
        return current_.storage();
    }

    void key(std::string& buf) const noexcept override {
        current_.key(buf);
    }

    void value(std::string& buf) const noexcept override {
        current_.value(buf);
    }

    api::log_entry::entry_type type() const override {
        return current_.type();
    }

    std::vector<api::blob_id_type> blob_ids() const override {
        return current_.get_blob_ids();
    }

    void close() override {}

    const api::log_entry& current() const override {
        return current_;
    }

private:
    std::vector<api::log_entry> entries_;
    std::size_t index_;
    api::log_entry current_;
};

class mock_cursor_entry_queue : public cursor_entry_queue {
public:
    mock_cursor_entry_queue(int fail_push_all = 0, int fail_push = 0)
        : cursor_entry_queue(8)  // dummy size
        , fail_push_all_(fail_push_all)
        , fail_push_(fail_push)
    {}

    std::size_t push_all(const std::vector<cursor_entry_type>& entries) override {
        if (fail_push_all_ > 0) {
            --fail_push_all_;
            return 0;
        }
        for (const auto& e : entries) {
            pushed_entries_.emplace_back(e);
            local_queue_.emplace_back(e);
        }
        return entries.size();
    }

    bool push(const cursor_entry_type& entry) noexcept override {
        if (fail_push_ > 0) {
            --fail_push_;
            return false;
        }
        pushed_entries_.emplace_back(entry);
        local_queue_.emplace_back(entry);  
        return true;
    }

    // テスト用: pushされたエントリの確認
    const std::vector<cursor_entry_type>& pushed_entries() const {
        return pushed_entries_;
    }

    // オプション: wait_and_pop を簡易実装してテスト可能にする
    cursor_entry_type wait_and_pop() override {
        while (local_queue_.empty()) {
            std::this_thread::sleep_for(std::chrono::microseconds(100));
        }
        cursor_entry_type front = local_queue_.front();
        local_queue_.pop_front();
        return front;
    }

    void enqueue_for_pop(cursor_entry_type entry) {
        local_queue_.emplace_back(std::move(entry));
    }

private:
    int fail_push_all_;
    int fail_push_;
    std::vector<cursor_entry_type> pushed_entries_;

    // wait_and_pop用の疑似queue
    std::deque<cursor_entry_type> local_queue_;
};


}  // namespace

class cursor_distributor_test : public ::testing::Test {};

TEST_F(cursor_distributor_test, distributes_entries_to_queues_and_adds_end_marker) {
    std::vector<api::log_entry> entries = {
        api::log_entry{}, api::log_entry{}, api::log_entry{}
    };

    auto cursor = std::make_unique<mock_cursor>(entries);

    std::vector<std::shared_ptr<cursor_entry_queue>> queues = {
        std::make_shared<cursor_entry_queue>(8),
        std::make_shared<cursor_entry_queue>(8)
    };

    auto distributor = std::make_shared<cursor_distributor>(std::move(cursor), queues);
    distributor->start();

    int entry_count = 0;
    int end_marker_count = 0;
    for (auto& q : queues) {
        while (true) {
            auto e = q->wait_and_pop();
            if (std::holds_alternative<api::log_entry>(e)) {
                ++entry_count;
            } else if (std::holds_alternative<end_marker>(e)) {
                ++end_marker_count;
                break; 
            }
        }
    }

    EXPECT_EQ(entry_count, 3);
    EXPECT_EQ(end_marker_count, 2);
}

TEST_F(cursor_distributor_test, sends_only_end_marker_when_cursor_is_empty) {
    std::vector<api::log_entry> entries = {};
    auto cursor = std::make_unique<mock_cursor>(entries);

    std::vector<std::shared_ptr<cursor_entry_queue>> queues = {
        std::make_shared<cursor_entry_queue>(8),
        std::make_shared<cursor_entry_queue>(8)
    };

    auto distributor = std::make_shared<cursor_distributor>(std::move(cursor), queues);
    distributor->start();

    int entry_count = 0;
    int end_marker_count = 0;

    for (auto& q : queues) {
        while (true) {
            auto e = q->wait_and_pop();
            if (std::holds_alternative<api::log_entry>(e)) {
                ++entry_count;
            } else if (std::holds_alternative<end_marker>(e)) {
                ++end_marker_count;
                break;
            }
        }
    }

    EXPECT_EQ(entry_count, 0);
    EXPECT_EQ(end_marker_count, 2);
}

TEST_F(cursor_distributor_test, aborts_when_shared_from_this_fails) {
    std::vector<std::shared_ptr<cursor_entry_queue>> queues = {
        std::make_shared<cursor_entry_queue>(8)
    };

    auto cursor = std::make_unique<mock_cursor>(std::vector<api::log_entry>{});
    // Intentionally instantiate without using shared_ptr
    cursor_distributor distributor(std::move(cursor), queues);

    // In EXPECT_DEATH, you can check part of the string output to stdout/stderr
    EXPECT_DEATH(
        distributor.start(),
        "shared_from_this\\(\\) failed"
    );
}


TEST_F(cursor_distributor_test, aborts_when_push_entry_fails) {
    std::vector<api::log_entry> entries = { api::log_entry{} };
    auto cursor = std::make_unique<mock_cursor>(entries);

    auto queue = std::make_shared<mock_cursor_entry_queue>(999,999);  // Always fail
    std::vector<std::shared_ptr<cursor_entry_queue>> queues = { queue };

    EXPECT_DEATH({
        auto distributor = std::make_shared<cursor_distributor>(std::move(cursor), queues);
        distributor->start();
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }, "failed to push all entries after retry");
}


TEST_F(cursor_distributor_test, aborts_when_push_end_marker_fails) {
    auto cursor = std::make_unique<mock_cursor>(std::vector<api::log_entry>{});  // Empty cursor

    auto queue = std::make_shared<mock_cursor_entry_queue>(999,9999);  // Always fail
    std::vector<std::shared_ptr<cursor_entry_queue>> queues = { queue };

    EXPECT_DEATH({
        auto distributor = std::make_shared<cursor_distributor>(std::move(cursor), queues);
        distributor->start();
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }, "failed to push end_marker to queue");
}

TEST_F(cursor_distributor_test, aborts_when_flushing_remaining_entries_fails) {
    std::vector<api::log_entry> entries = { api::log_entry{} };
    auto cursor = std::make_unique<mock_cursor>(entries);

    auto queue = std::make_shared<mock_cursor_entry_queue>(9999,9999);
    std::vector<std::shared_ptr<cursor_entry_queue>> queues = { queue };

    EXPECT_DEATH({
        auto distributor = std::make_shared<cursor_distributor>(std::move(cursor), queues);
        distributor->start();
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }, "failed to push all entries after retry");
}

TEST_F(cursor_distributor_test, retries_on_partial_push_and_advances_queue_index) {
    std::vector<api::log_entry> entries = { api::log_entry{}, api::log_entry{} };
    auto cursor = std::make_unique<mock_cursor>(entries);

    auto queue1 = std::make_shared<mock_cursor_entry_queue>(1, 0);
    auto queue2 = std::make_shared<mock_cursor_entry_queue>(1, 0);

    std::vector<std::shared_ptr<cursor_entry_queue>> queues = { queue1, queue2 };

    auto distributor = std::make_shared<testable_cursor_distributor>(
        std::move(cursor), queues,
        2, 10
    );

    std::promise<void> done;
    std::future<void> done_future = done.get_future();
    distributor->set_on_complete([&done]() {
        done.set_value();
    });

    distributor->start();
    ASSERT_EQ(done_future.wait_for(std::chrono::seconds(1)), std::future_status::ready)
        << "cursor_distributor did not complete in time";

    // Validate queue1
    {
        auto pushed = queue1->pushed_entries();
        ASSERT_EQ(pushed.size(), 3);  // 2 entries + 1 end_marker

        EXPECT_TRUE(std::holds_alternative<api::log_entry>(pushed[0]));
        EXPECT_TRUE(std::holds_alternative<api::log_entry>(pushed[1]));
        EXPECT_TRUE(std::holds_alternative<end_marker>(pushed[2]));
    }

    // Validate queue2
    {
        auto pushed = queue2->pushed_entries();
        ASSERT_EQ(pushed.size(), 1);
        EXPECT_TRUE(std::holds_alternative<end_marker>(pushed[0]));
    }

    // Validation using wait_and_pop()
    int entry_count = 0;
    int end_marker_count = 0;
    for (auto& q : queues) {
        while (true) {
            auto e = q->wait_and_pop();  
            if (std::holds_alternative<api::log_entry>(e)) {
                ++entry_count;
            } else if (std::holds_alternative<end_marker>(e)) {
                ++end_marker_count;
                break;
            }
        }
    }

    EXPECT_EQ(entry_count, 2);
    EXPECT_EQ(end_marker_count, 2);
}

TEST_F(cursor_distributor_test, try_push_all_to_queues_returns_expected_results) {
    using test_distributor = testable_cursor_distributor;

    // ✅ Case 1: 成功して buffer.empty() == true
    {
        std::vector<api::log_entry> entries = { api::log_entry{} };
        auto cursor = std::make_unique<mock_cursor>(entries);

        // 成功する queue
        auto queue = std::make_shared<mock_cursor_entry_queue>(0);  // 失敗なし
        std::vector<std::shared_ptr<cursor_entry_queue>> queues = { queue };

        auto distributor = std::make_shared<test_distributor>(
            std::move(cursor), queues,
            2, 0  // retries, no delay
        );

        std::vector<cursor_entry_type> buffer = { api::log_entry{} };
        std::size_t index = 0;

        bool result = distributor->try_push_all_to_queues(buffer, index);
        EXPECT_TRUE(result);
        EXPECT_TRUE(buffer.empty());
    }

    // ✅ Case 2: 失敗して buffer.empty() == false
    {
        std::vector<api::log_entry> entries = { api::log_entry{} };
        auto cursor = std::make_unique<mock_cursor>(entries);

        // 常に失敗する queue
        auto queue = std::make_shared<mock_cursor_entry_queue>(999);  // push_all 常に失敗
        std::vector<std::shared_ptr<cursor_entry_queue>> queues = { queue };

        auto distributor = std::make_shared<test_distributor>(
            std::move(cursor), queues,
            2, 0
        );

        std::vector<cursor_entry_type> buffer = { api::log_entry{} };
        std::size_t index = 0;

        bool result = distributor->try_push_all_to_queues(buffer, index);
        EXPECT_FALSE(result);
        EXPECT_FALSE(buffer.empty());
    }
}

}  // namespace limestone::testing
