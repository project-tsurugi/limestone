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

#include <unordered_map>
#include <vector>
#include <string>
#include <optional>

#include <limestone/api/epoch_tag.h>

namespace limestone::api {

class tag_repository {
public:

    std::vector<epoch_tag>& list();

    void register_tag(std::string name, std::string comments);

    std::optional<epoch_tag> find(std::string_view name);

    void unregister_tag(std::string_view name);

private:
    std::unordered_map<std::string, epoch_tag> map_;
    std::vector<epoch_tag> list_;

};

} // namespace limestone::api