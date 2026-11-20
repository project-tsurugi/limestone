/*
 * Copyright 2022-2026 Project Tsurugi.
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
#include <ostream>
#include <string_view>

namespace limestone::internal {

/**
 * @brief What triggers an online compaction.
 */
enum class compaction_trigger : std::uint8_t {
    /// A request by the ctrl file
    ctrl_file,
    /// The amount of the WAL exceeding the threshold (the auto compaction)
    threshold,
};

/**
 * @brief Returns the name of a trigger.
 * @param trigger the trigger
 * @return the name to write to the log
 */
[[nodiscard]] std::string_view to_string_view(compaction_trigger trigger) noexcept;

/**
 * @brief Writes the name of a trigger to a stream.
 */
std::ostream& operator<<(std::ostream& out, compaction_trigger trigger);

} // namespace limestone::internal
