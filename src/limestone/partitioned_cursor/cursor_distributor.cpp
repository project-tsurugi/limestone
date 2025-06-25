#include "cursor_distributor.h"

#include <iostream>
#include <thread>

#include "limestone_exception_helper.h"
#include "partitioned_cursor_consts.h"

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


bool cursor_distributor::try_push_all_to_queues(std::vector<cursor_entry_type>& buffer,
                                                std::size_t& queue_index) {
    const std::size_t queue_count = queues_.size();
    for (std::size_t retry = 0; retry <= max_retries_; ++retry) {
        std::size_t attempts = 0;
        while (attempts < queue_count) {
            auto& queue = queues_[queue_index % queue_count];
            std::size_t pushed_count = queue->push_all(buffer);
            if (pushed_count > 0) {
                buffer.erase(buffer.begin(), buffer.begin() + pushed_count);
                if (buffer.empty()) {
                    return true;
                }
            }
            ++queue_index;
            ++attempts;
        }
        if (retry < max_retries_) {
            std::this_thread::sleep_for(std::chrono::microseconds(retry_delay_us_));
        }
    }
    // All retries exhausted. Normally buffer should not be empty here.
    // However, return buffer.empty() defensively in case all entries were pushed just before exiting.
    return buffer.empty();
}

void cursor_distributor::push_end_markers() {
    for (std::size_t i = 0; i < queues_.size(); ++i) {
        for (std::size_t retry = 0; retry <= max_retries_; ++retry) {
            if (queues_[i]->push(end_marker{true, ""})) {
                break;
            }
            if (retry == max_retries_) {
                LOG_LP(FATAL) << "[cursor_distributor] Fatal: failed to push end_marker to queue " << i << ". Aborting.";
                std::abort();
            }
            std::this_thread::sleep_for(std::chrono::microseconds(retry_delay_us_));
        }
    }
}

void cursor_distributor::run() {
    std::size_t queue_index = 0;

    std::vector<cursor_entry_type> buffer;
    buffer.reserve(CURSOR_DISTRIBUTOR_BATCH_SIZE);

    while (cursor_->next()) {
        buffer.emplace_back(cursor_->current());

        if (buffer.size() >= CURSOR_DISTRIBUTOR_BATCH_SIZE) {
            if (!try_push_all_to_queues(buffer, queue_index)) {
                cursor_->close();
                LOG_LP(FATAL) << "[cursor_distributor] Fatal: failed to push all entries after retry. Aborting.";
                std::abort();
            }
        }
    }

    if (!buffer.empty()) {
        if (!try_push_all_to_queues(buffer, queue_index)) {
            cursor_->close();
            LOG_LP(FATAL) << "[cursor_distributor] Fatal: failed to flush remaining entries. Aborting.";
            std::abort();
        }
    }

    push_end_markers();
    cursor_->close();
    LOG_LP(INFO) << "[cursor_distributor] Distribution completed.";

    if (on_complete_) {
        on_complete_();
    }
}


}  // namespace limestone::internal
