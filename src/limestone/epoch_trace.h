/*
 * Copyright 2026 Project Tsurugi.
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
#pragma once

#include <cstdint>
#include <mutex>
#include <string_view>

#include <glog/logging.h>

namespace limestone::internal {

// Temporary instrumentation for measuring how far log-channel sessions run
// ahead of the durable (informed) epoch in a real workload.
//
// Four events are emitted: switch_begin / switch_end (datastore::switch_epoch)
// and session_begin / session_end (log_channel). All events are written under
// a single mutex so that the emitted line order forms one linearization that
// an offline script can replay to reconstruct the set of active sessions at
// every switch_epoch completion.
//
// Note: the state change itself (atomic store) happens just before the log
// call and outside the mutex, so orderings may flip within a microsecond-scale
// window at event boundaries. Sessions that span an epoch switch live for
// milliseconds or longer, so this skew is statistical noise for the purpose
// of this measurement.
inline std::mutex epoch_trace_mutex;

inline void epoch_trace(std::string_view event, std::string_view channel,
                        std::uint64_t epoch, std::uint64_t switched, std::uint64_t informed) {
    {
        std::lock_guard<std::mutex> lock(epoch_trace_mutex);
        LOG(INFO) << "EPOCHTRACE " << event
                  << " ch=" << (channel.empty() ? std::string_view{"-"} : channel)
                  << " epoch=" << epoch
                  << " switched=" << switched
                  << " informed=" << informed;
    }
}

} // namespace limestone::internal
