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

using api::log_entry;
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

    log_entry& current() override {
        return current_;
    }

private:
    std::vector<log_entry> entries_;
    std::size_t index_;
    log_entry current_;
};

class mock_cursor_entry_queue : public cursor_entry_queue {
public:
    // fail_push_until_n: number of initial push attempts to fail
    explicit mock_cursor_entry_queue(int fail_push_until_n = 0)
        : cursor_entry_queue(8)
        , fail_push_until_n_(fail_push_until_n)
        , total_push_attempts_(0)
    {}

    bool push(const cursor_entry_type& entry) noexcept override {
        ++total_push_attempts_;

        if (fail_push_until_n_ > 0) {
            --fail_push_until_n_;
            return false;
        }

        pushed_entries_.emplace_back(entry);
        return true;
    }

    const std::vector<cursor_entry_type>& pushed_entries() const {
        return pushed_entries_;
    }

    std::size_t total_push_attempts() const {
        return total_push_attempts_;
    }

private:
    int fail_push_until_n_;  // fail first N pushes
    std::size_t total_push_attempts_;
    std::vector<cursor_entry_type> pushed_entries_;
};


}  // namespace

class cursor_distributor_test : public ::testing::Test {};

class testable_cursor_distributor : public cursor_distributor {
public:
    using cursor_distributor::cursor_distributor;
    using cursor_distributor::push_batch;  
    using cursor_distributor::push_end_markers;
    using cursor_distributor::read_batch_from_cursor;
};


TEST_F(cursor_distributor_test, does_nothing_when_pushing_empty_batch) {
    auto queue = std::make_shared<mock_cursor_entry_queue>();
    std::vector<std::shared_ptr<cursor_entry_queue>> queues = { queue };

    auto cursor = std::make_unique<mock_cursor>(std::vector<api::log_entry>{});
    auto distributor = std::make_shared<testable_cursor_distributor>(
        std::move(cursor), queues
    );

    std::vector<api::log_entry> empty_batch;
    distributor->push_batch(std::move(empty_batch), *queue);

    EXPECT_TRUE(queue->pushed_entries().empty());
}



TEST_F(cursor_distributor_test, distributes_entries_to_queues_and_adds_end_marker) {
    constexpr std::size_t total_entries = 6; 

    std::vector<api::log_entry> entries(total_entries, api::log_entry{});
    auto cursor = std::make_unique<mock_cursor>(entries);

    std::vector<std::shared_ptr<cursor_entry_queue>> queues = {
        std::make_shared<cursor_entry_queue>(16),
        std::make_shared<cursor_entry_queue>(16)
    };

    auto distributor = std::make_shared<cursor_distributor>(std::move(cursor), queues);
    distributor->start();

    int entry_count = 0;
    int end_marker_count = 0;
    for (auto& q : queues) {
        while (true) {
            auto e = q->wait_and_pop();
            if (std::holds_alternative<std::vector<api::log_entry>>(e)) {
                const auto& batch = std::get<std::vector<api::log_entry>>(e);
                entry_count += static_cast<int>(batch.size());
            } else if (std::holds_alternative<end_marker>(e)) {
                ++end_marker_count;
                break;
            }
        }
    }

    EXPECT_EQ(entry_count, total_entries);
    EXPECT_EQ(end_marker_count, queues.size());
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
            if (std::holds_alternative<std::vector<api::log_entry>>(e)) {
                const auto& batch = std::get<std::vector<api::log_entry>>(e);
                entry_count += static_cast<int>(batch.size());
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

    auto queue = std::make_shared<mock_cursor_entry_queue>(999); // always fail
    std::vector<std::shared_ptr<cursor_entry_queue>> queues = { queue };

    EXPECT_DEATH({
        auto distributor = std::make_shared<cursor_distributor>(std::move(cursor), queues);
        distributor->start();
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }, "failed to push log_entry batch");
}

TEST_F(cursor_distributor_test, aborts_when_push_end_marker_fails) {
    auto cursor = std::make_unique<mock_cursor>(std::vector<api::log_entry>{});

    auto queue = std::make_shared<mock_cursor_entry_queue>(999); // always fail
    std::vector<std::shared_ptr<cursor_entry_queue>> queues = { queue };

    EXPECT_DEATH({
        auto distributor = std::make_shared<cursor_distributor>(std::move(cursor), queues);
        distributor->start();
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }, "failed to push end_marker to queue");
}

TEST_F(cursor_distributor_test, aborts_when_flushing_remaining_entries_fails) {
    std::vector<api::log_entry> entries;
    for (int i = 0; i < 10; ++i) entries.emplace_back(api::log_entry{});
    auto cursor = std::make_unique<mock_cursor>(entries);

    auto queue = std::make_shared<mock_cursor_entry_queue>(999); // always fail
    std::vector<std::shared_ptr<cursor_entry_queue>> queues = { queue };

    EXPECT_DEATH({
        auto distributor = std::make_shared<cursor_distributor>(std::move(cursor), queues);
        distributor->start();
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }, "failed to push log_entry batch");
}

TEST_F(cursor_distributor_test, distributes_all_entries_and_adds_end_marker_regardless_of_batch_size) {
    constexpr std::size_t total_entries = 4;

    std::vector<api::log_entry> entries(total_entries, api::log_entry{});
    auto cursor = std::make_unique<mock_cursor>(entries);

    auto queue1 = std::make_shared<cursor_entry_queue>(16);
    auto queue2 = std::make_shared<cursor_entry_queue>(16);
    std::vector<std::shared_ptr<cursor_entry_queue>> queues = { queue1, queue2 };

    auto distributor = std::make_shared<cursor_distributor>(std::move(cursor), queues);
    distributor->start();

    int log_entry_count = 0;
    int end_marker_count = 0;

    for (auto& queue : queues) {
        while (true) {
            auto e = queue->wait_and_pop();
            if (std::holds_alternative<std::vector<api::log_entry>>(e)) {
                log_entry_count += std::get<std::vector<api::log_entry>>(e).size();
            } else if (std::holds_alternative<end_marker>(e)) {
                ++end_marker_count;
                break;
            }
        }
    }

    EXPECT_EQ(log_entry_count, total_entries);
    EXPECT_EQ(end_marker_count, queues.size());
}

TEST_F(cursor_distributor_test, push_batch_retries_and_succeeds) {
    auto queue = std::make_shared<mock_cursor_entry_queue>(2); // 最初の2回失敗
    std::vector<api::log_entry> batch = { api::log_entry{}, api::log_entry{} };

    auto cursor = std::make_unique<mock_cursor>(std::vector<api::log_entry>{});
    auto distributor = std::make_shared<testable_cursor_distributor>(
        std::move(cursor),
        std::vector<std::shared_ptr<cursor_entry_queue>>{
            std::static_pointer_cast<cursor_entry_queue>(queue)
        },
        5,   // max_retries
        10,  // retry_delay_us
        2    // batch_size
    );

    distributor->push_batch(std::move(batch), *queue);

    ASSERT_EQ(queue->pushed_entries().size(), 1);
    EXPECT_TRUE(std::holds_alternative<std::vector<api::log_entry>>(queue->pushed_entries()[0]));
    EXPECT_EQ(queue->total_push_attempts(), 3);
}


TEST_F(cursor_distributor_test, push_end_marker_retries_and_succeeds) {
    auto queue = std::make_shared<mock_cursor_entry_queue>(3);
    std::vector<std::shared_ptr<cursor_entry_queue>> queues = {
        std::static_pointer_cast<cursor_entry_queue>(queue)
    };

    auto cursor = std::make_unique<mock_cursor>(std::vector<api::log_entry>{});
    auto distributor = std::make_shared<testable_cursor_distributor>(
        std::move(cursor),
        queues,
        5,   // max_retries
        10,  // retry_delay_us
        1    // batch_size
    );

    distributor->push_end_markers();

    ASSERT_EQ(queue->pushed_entries().size(), 1);
    EXPECT_TRUE(std::holds_alternative<end_marker>(queue->pushed_entries()[0]));
    EXPECT_EQ(queue->total_push_attempts(), 4);  // 3 failures + 1 success
}

TEST_F(cursor_distributor_test, read_batch_stops_at_batch_size_limit) {
    std::vector<api::log_entry> entries = {
        api::log_entry{}, api::log_entry{}, api::log_entry{}
    };

    auto raw_cursor = new mock_cursor(entries);
    std::unique_ptr<mock_cursor> cursor_ptr(raw_cursor);

    auto distributor = std::make_shared<testable_cursor_distributor>(
        std::move(cursor_ptr),
        std::vector<std::shared_ptr<cursor_entry_queue>>{},
        5,   // max_retries
        10,  // retry_delay_us
        2    // batch_size
    );


    auto batch = distributor->read_batch_from_cursor(*raw_cursor);

    EXPECT_EQ(batch.size(), 2);  
}


}  // namespace limestone::testing
