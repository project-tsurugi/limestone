#include "cursor_distributor.h"

#include <iostream>
#include <thread>

#include "limestone_exception_helper.h"

namespace limestone::internal {

cursor_distributor::cursor_distributor(
    std::unique_ptr<cursor_impl_base> cursor,
    std::vector<std::shared_ptr<cursor_entry_queue>> queues,
    std::size_t max_retries,
    std::size_t retry_delay_us
)
    : cursor_(std::move(cursor))
    , queues_(std::move(queues))
    , max_retries_(max_retries)
    , retry_delay_us_(retry_delay_us)
{}

void cursor_distributor::start() {
    try {
        auto self = shared_from_this();
        std::thread([self]() {
            pthread_setname_np(pthread_self(), "cursor_dist");
            self->run();
        }).detach();
    } catch (const std::bad_weak_ptr&) {
        std::cerr << "[cursor_distributor] shared_from_this() failed. Instance must be managed by shared_ptr.\n";
        std::abort();
    }
}

bool cursor_distributor::push_with_retry(
    const std::shared_ptr<cursor_entry_queue>& queue,
    const cursor_entry_type& entry,
    std::size_t queue_index
) const {
    for (std::size_t i = 0; i <= max_retries_; ++i) {
        if (queue->push(entry)) {
            return true;
        }
        if (i < max_retries_) {
            std::this_thread::sleep_for(std::chrono::microseconds(retry_delay_us_));
        }
    }
    LOG_LP(ERROR) << "[cursor_distributor] Failed to push entry to queue " << queue_index
              << " after " << (max_retries_ + 1) << " attempts\n";
    return false;
}

void cursor_distributor::run() {
    std::size_t index = 0;
    std::size_t count = queues_.size();

    while (cursor_->next()) {
        auto& queue = queues_[index % count];
        if (!push_with_retry(queue, cursor_->current(), index % count)) {
            cursor_->close();
            LOG_LP(FATAL) << "[cursor_distributor] Fatal: failed to push entry to queue " << (index % count) << ". Aborting.\n";
            std::abort();
        }
        ++index;
    }

    // Push end_marker to all queues
    for (std::size_t i = 0; i < count; ++i) {
        if (!push_with_retry(queues_[i], end_marker{true, ""}, i)) {
            cursor_->close();
            LOG_LP(FATAL) << "[cursor_distributor] Fatal: failed to push end_marker to queue " << i << ". Aborting.\n";
            std::abort();
        }
    }

    cursor_->close();
    LOG_LP(INFO) << "[cursor_distributor] Distribution completed. Total entries: ";
}

}  // namespace limestone::internal
