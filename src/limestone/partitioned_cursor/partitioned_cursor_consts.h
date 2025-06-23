#pragma once

#include <cstddef>
#include <chrono>

/**
 * @file partitioned_cursor_consts.h
 * @brief Temporary hardcoded constants for partitioned cursor behavior.
 *
 * These constants are placeholders and will be replaced by external
 * configuration values in the future.
 *
 * Note: This file and its contents are not intended for long-term use.
 * Avoid relying on these values as fixed design choices.
 */

namespace limestone::internal {

/**
 * @brief Maximum number of entries the cursor queue can hold.
 * This value is temporarily hardcoded and will be loaded from configuration in the future.
 *
 * 65536 entries × ~112B = ~7.2MB per queue
 * (Assuming sizeof(log_entry) ≈ 112 bytes — this may change if log_entry's layout changes.)
 *
 * For 32 queues = ~230MB total, suitable for development environments.
 */
constexpr std::size_t CURSOR_QUEUE_CAPACITY = 65536;

/**
 * @brief Maximum number of retry attempts when pushing to a queue fails.
 */
constexpr std::size_t CURSOR_PUSH_RETRY_COUNT = 3;

/**
 * @brief Delay between push retry attempts, in microseconds.
 */
constexpr std::size_t CURSOR_PUSH_RETRY_INTERVAL_US = 1000;

/**
 * @brief Polling interval when waiting to pop from the queue, in microseconds.
 */
constexpr std::size_t CURSOR_POP_POLL_INTERVAL_US = 10;

}  // namespace limestone::internal
