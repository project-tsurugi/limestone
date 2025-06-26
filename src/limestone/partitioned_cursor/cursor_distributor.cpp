#include "cursor_distributor.h"

#include <boost/asio/post.hpp>
#include <boost/asio/thread_pool.hpp>
#include <future>
#include <iostream>
#include <thread>

#include "limestone_exception_helper.h"
#include "partitioned_cursor_consts.h"

namespace limestone::internal {

cursor_distributor::cursor_distributor(
    std::unique_ptr<cursor_impl_base> cursor,
    std::vector<std::shared_ptr<cursor_entry_queue>> queues,
    std::size_t max_retries,
    std::size_t retry_delay_us,
    std::size_t batch_size
)
    : cursor_(std::move(cursor))
    , queues_(std::move(queues))
    , max_retries_(max_retries)
    , retry_delay_us_(retry_delay_us)
    , batch_size_(batch_size) 
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

void cursor_distributor::push_batch(std::vector<log_entry>&& buffer, cursor_entry_queue& queue) {
    if (buffer.empty()) {
        return;
    }

    cursor_entry_type batch = std::move(buffer); 

    for (std::size_t retry = 0; retry <= max_retries_; ++retry) {
        if (queue.push(batch)) {
            return;
        }

        if (retry < max_retries_) {
            std::this_thread::sleep_for(std::chrono::microseconds(retry_delay_us_));
        }
    }

    LOG_LP(FATAL) << "[cursor_distributor] Fatal: failed to push log_entry batch after retries. Aborting.";
    std::abort();
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

std::vector<log_entry> cursor_distributor::read_batch_from_cursor(cursor_impl_base& cursor) {
    std::vector<log_entry> buffer;
    buffer.reserve(batch_size_);

    while (cursor.next()) {
        buffer.emplace_back(std::move(cursor.current()));
        if (buffer.size() >= batch_size_) {
            break;
        }
    }
    return buffer;
}

void cursor_distributor::set_on_complete(std::function<void()> callback) {
    on_complete_ = std::move(callback);
}

void cursor_distributor::run() {
    std::size_t queue_index = 0;
    const std::size_t queue_count = queues_.size();

    while (true) {
        auto batch = read_batch_from_cursor(*cursor_);
        if (batch.empty()) {
            break;
        }

        auto& queue = *queues_[queue_index % queue_count];

        // 同期的にpush_batch
        push_batch(std::move(batch), queue);

        ++queue_index;
    }

    push_end_markers();
    cursor_->close();
    LOG_LP(INFO) << "[cursor_distributor] Distribution completed.";

    if (on_complete_) {
        on_complete_();
    }

}



}  // namespace limestone::internal
