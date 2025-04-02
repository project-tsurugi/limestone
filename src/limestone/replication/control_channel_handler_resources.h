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


#include "handler_resources.h"
#include "limestone/api/datastore.h"

namespace limestone::replication {

using namespace limestone::api;
    
class control_channel_handler_resources : public handler_resources {
public:
control_channel_handler_resources(
        socket_io& io,
        datastore& ds)
        : handler_resources(io)
        , datastore_(ds) {}

    [[nodiscard]] datastore& get_datastore() const { return datastore_; }

private:
    datastore& datastore_;
};

}  // namespace limestone::replication