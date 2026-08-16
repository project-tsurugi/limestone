/*
 * Copyright 2022-2025 Project Tsurugi.
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

#include "replication_message_io.h"

namespace limestone::replication {

class handler_resources {
public:
    explicit handler_resources(replication_message_io& io)
        : replication_message_io_(io) {}
    virtual ~handler_resources() = default;

    handler_resources(const handler_resources&) = delete;
    handler_resources& operator=(const handler_resources&) = delete;
    handler_resources(handler_resources&&) = delete;
    handler_resources& operator=(handler_resources&&) = delete;

    [[nodiscard]] replication_message_io& get_replication_message_io() const { return replication_message_io_; }

private:
    replication_message_io& replication_message_io_;
};

}  // namespace limestone::replication
