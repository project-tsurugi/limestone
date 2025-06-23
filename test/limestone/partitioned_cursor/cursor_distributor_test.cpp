#include <gtest/gtest.h>
#include <memory>
#include <vector>
#include <chrono>
#include <thread>
#include <string>
#include <optional>
#include <iostream>

#include "partitioned_cursor/cursor_distributor.h"
#include "partitioned_cursor/cursor_entry_queue.h"
#include "cursor_impl_base.h"
#include "partitioned_cursor/end_marker.h"

namespace limestone::testing {

namespace {

using namespace limestone::internal;

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
    explicit mock_cursor_entry_queue(int fail_count_before_success = 0)
        : cursor_entry_queue(8), fail_count_before_success_(fail_count_before_success) {}

    bool push(const cursor_entry_type& entry) noexcept override {
        if (fail_count_before_success_ > 0) {
            --fail_count_before_success_;
            return false;
        }
        pushed_entries_.push_back(entry);
        return true;
    }

    const std::vector<cursor_entry_type>& pushed_entries() const {
        return pushed_entries_;
    }

private:
    int fail_count_before_success_;
    std::vector<cursor_entry_type> pushed_entries_;
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

    auto queue = std::make_shared<mock_cursor_entry_queue>(999);  // 永遠に失敗
    std::vector<std::shared_ptr<cursor_entry_queue>> queues = { queue };

    EXPECT_DEATH({
        auto distributor = std::make_shared<cursor_distributor>(std::move(cursor), queues);
        distributor->start();
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }, "failed to push entry to queue");
}


TEST_F(cursor_distributor_test, aborts_when_push_end_marker_fails) {
    auto cursor = std::make_unique<mock_cursor>(std::vector<api::log_entry>{});  // 空カーソル

    auto queue = std::make_shared<mock_cursor_entry_queue>(999);  // 永遠に失敗
    std::vector<std::shared_ptr<cursor_entry_queue>> queues = { queue };

    EXPECT_DEATH({
        auto distributor = std::make_shared<cursor_distributor>(std::move(cursor), queues);
        distributor->start();
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }, "failed to push end_marker to queue");
}

}  // namespace limestone::testing
