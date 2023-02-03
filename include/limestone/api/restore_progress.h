/*
 * Copyright 2022-2022 tsurugi project.
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

#include <limestone/status.h>

namespace limestone::api {

class datastore;

/**
 * @brief a tag associated with a specific epoch
 */
class restore_progress {
public:
    /**
     * @brief type of a result of query processing by restore_status()
     */
    enum class status_kind : std::int64_t {
        prepareing = 0,
        running = 1,
        completed = 2,
        failed = -1,
        canceled = -2,
    };

    // FIXME
    // restore_progress() = delete;
    restore_progress() {}

    static const restore_progress FIXME;
};
    
} // namespace limestone::api
