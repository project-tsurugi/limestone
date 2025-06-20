#pragma once

#include <memory>
#include <thread>
#include <vector>
#include <optional>
#include <chrono>

#include "cursor_entry_queue.h"
#include "cursor_impl_base.h"

namespace limestone::internal {

/**
 * @brief Distributes entries from a cursor to multiple queues for partitioned consumption.
 */
class cursor_distributor {
public:
    /**
     * @brief Constructs a new cursor_distributor.
     * @param cursor the cursor to read entries from
     * @param queues the target queues to distribute entries to
     * @param max_retries number of times to retry if a push fails
     * @param retry_delay_us delay between retries in microseconds
     */
    cursor_distributor(std::unique_ptr<cursor_impl_base> cursor,
                       std::vector<std::shared_ptr<cursor_entry_queue>> queues,
                       std::size_t max_retries = 3,
                       std::size_t retry_delay_us = 1000);

    /**
     * @brief Starts the distribution thread.
     */
    void start();

    cursor_distributor(const cursor_distributor&) = delete;
    cursor_distributor& operator=(const cursor_distributor&) = delete;
    cursor_distributor(cursor_distributor&&) = delete;
    cursor_distributor& operator=(cursor_distributor&&) = delete;

protected:
    /**
     * @brief Pushes an entry into a queue with retry logic.
     * @return true if successful, false if all retries failed.
     */
    bool push_with_retry(const std::shared_ptr<cursor_entry_queue>& queue,
                         const cursor_entry_type& entry,
                         std::size_t queue_index) const;

private:
    void run();

    std::unique_ptr<cursor_impl_base> cursor_;
    std::vector<std::shared_ptr<cursor_entry_queue>> queues_;
    std::optional<std::thread> worker_;
    std::size_t max_retries_;
    std::size_t retry_delay_us_;
};

}  // namespace limestone::internal
