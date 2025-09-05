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
#include "blob_test_helpers.h"
#include <fstream>
#include <boost/filesystem.hpp>

namespace limestone::testing {

void create_blob_file(const blob_file_resolver& resolver, std::uint64_t id) {
    boost::filesystem::path file_path = resolver.resolve_path(id);
    boost::filesystem::path dir = file_path.parent_path();
    if (!dir.empty() && !boost::filesystem::exists(dir)) {
        boost::filesystem::create_directories(dir);
    }
    std::ofstream ofs(file_path.string());
    ofs << "dummy data";
    ofs.close();
}

} // namespace limestone::testing