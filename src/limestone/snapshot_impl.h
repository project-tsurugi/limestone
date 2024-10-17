/*
 * Copyright 2022-2023 Project Tsurugi.
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


#include <memory>
#include <string_view>
#include <boost/filesystem.hpp>
#include <limestone/api/cursor.h>

namespace limestone::internal {

using limestone::api::cursor;
using limestone::api::storage_id_type;    

class snapshot_impl {
public:
    explicit snapshot_impl(boost::filesystem::path location) noexcept;
    [[nodiscard]] std::unique_ptr<cursor> get_cursor() const;

private:
    boost::filesystem::path location_;
};

} // namespace limestone::internal