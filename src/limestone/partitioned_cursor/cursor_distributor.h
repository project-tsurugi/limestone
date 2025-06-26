#pragma once

#include <memory>
#include <thread>
#include <vector>
#include <optional>
#include <chrono>

#include "cursor_entry_queue.h"
#include "cursor_impl_base.h"
#include "partitioned_cursor_consts.h"

namespace limestone::internal {

/**
 * @brief Distributes entries from a cursor to multiple queues for partitioned consumption.
 * @details
 * This class must be managed using std::shared_ptr, as it internally calls shared_from_this().
 * The `start()` method launches a background thread which continues running even after the
 * original object is destroyed. Therefore, it is essential that the object remains valid
 * for the duration of the thread, which is ensured by the shared ownership.
 *
 * WARNING:
 * - Never instantiate this class on the stack or using raw pointers.
 * - Always use std::make_shared<cursor_distributor>(...) to create instances.
 * - Be aware that `start()` detaches the thread. The caller must ensure that
 *   any needed synchronization is handled externally if required.
 */
class cursor_distributor : public std::enable_shared_from_this<cursor_distributor> {
public:
    /**
     * @brief Constructs a new cursor_distributor.
     * @param cursor the cursor to read entries from
     * @param queues the target queues to distribute entries to
     * @param max_retries number of times to retry if a push fails
     * @param retry_delay_us delay between retries in microseconds
     * @param batch_size number of entries per batch to send to a queue
     */
    cursor_distributor(std::unique_ptr<cursor_impl_base> cursor,
                       std::vector<std::shared_ptr<cursor_entry_queue>> queues,
                       std::size_t max_retries = CURSOR_PUSH_RETRY_COUNT,
                       std::size_t retry_delay_us = CURSOR_PUSH_RETRY_INTERVAL_US,
                       std::size_t batch_size = CURSOR_DISTRIBUTOR_BATCH_SIZE); 

    /**
     * @brief Starts the distribution thread.
     */
    void start();


    ~cursor_distributor() = default;

    cursor_distributor(const cursor_distributor&) = delete;
    cursor_distributor& operator=(const cursor_distributor&) = delete;
    cursor_distributor(cursor_distributor&&) = delete;
    cursor_distributor& operator=(cursor_distributor&&) = delete;

protected:
    /**
     * @brief Pushes a batch of log_entry objects to a specific queue.
     *        Retries up to `max_retries_` times if the push fails.
     *        If all retries fail, logs a fatal error and aborts the process.
     *
     * @param buffer the batch of log entries to push
     * @param queue the target queue to receive the entries
     */
    void push_batch(std::vector<log_entry>& buffer, cursor_entry_queue& queue);

    /**
     * @brief Pushes end_marker entries to all queues with retry logic.
     *
     * If any push fails after all retries, the process is aborted.
     */
    void push_end_markers();

    /**
     * @brief Reads entries from the cursor until the buffer reaches a configured batch size,
     *        or the cursor reaches end-of-stream.
     *
     * @param buffer the buffer to store read entries; cleared at start
     * @return true if at least one entry was read, false if end-of-stream with no data
     */
    std::vector<log_entry> read_batch_from_cursor(cursor_impl_base& cursor);
private:
    void run();

    std::unique_ptr<cursor_impl_base> cursor_;
    std::vector<std::shared_ptr<cursor_entry_queue>> queues_;
    std::size_t max_retries_;
    std::size_t retry_delay_us_;
    std::size_t batch_size_; 
};

}  // namespace limestone::internal

