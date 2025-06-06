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

#include <glog/logging.h>
#include <limestone/api/datastore.h>
#include <limestone/logging.h>

#include <boost/filesystem.hpp>
#include <iostream>
#include <string>
#include <chrono> // 追加
#include <locale>
#include <sstream>

#include "logging_helper.h"

using namespace limestone;

void show_usage(const std::string& program_name) {
    std::cerr << "Usage: " << program_name << " <logdir>" << std::endl;
    std::cerr << "Note: The environment variable TSURUGI_REPLICATION_ENDPOINT must be set with the endpoint URL." << std::endl;
    std::cerr << "      For example: tcp://localhost:1234" << std::endl;
}

int main(int argc, char* argv[]) {
    // Convert argv to vector<string> to avoid direct pointer arithmetic.
    std::vector<std::string> args(argv, argv + argc);
    
    // Retrieve program name from args[0].
    const std::string program_name = boost::filesystem::path(args[0]).filename().string();
    
    if (args.size() != 2) {
        show_usage(program_name);
        return 1;
    }

    // Check logdir using args[1]
    boost::filesystem::path log_dir_path(args[1]);
    if (!boost::filesystem::exists(log_dir_path)) {
        std::cerr << "Error: Directory does not exist: " << log_dir_path.string() << std::endl;
        show_usage(program_name);
        return 1;
    }
    if (!boost::filesystem::is_directory(log_dir_path)) {
        std::cerr << "Error: Specified path is not a directory: " << log_dir_path.string() << std::endl;
        show_usage(program_name);
        return 1;
    }

    std::vector<boost::filesystem::path> data_locations{};
    data_locations.emplace_back(log_dir_path);
    boost::filesystem::path metadata_location{log_dir_path};
    limestone::api::configuration conf(data_locations, metadata_location);

    auto datastore_ = std::make_unique<limestone::api::datastore>(conf);

    datastore_->ready();
    
    auto snapshot = datastore_->get_snapshot();
    auto cursor = snapshot->get_cursor();

    LOG_LP(INFO) << "start snapshot reading";
    int i = 0;
    std::size_t total_bytes = 0;
    auto start = std::chrono::steady_clock::now(); // 開始時刻取得
    while (cursor->next()) {
        i++;
        auto storage_id = cursor->storage();
        std::string key;
        std::string value;
        cursor->key(key);
        cursor->value(value);

        total_bytes += sizeof(storage_id) + key.size() + value.size();

        if (i % 1000000 == 0) {
            std::ostringstream oss;
            oss.imbue(std::locale(""));
            oss << "processed entries: " << i
                << " | total bytes: " << total_bytes;
            LOG_LP(INFO) << oss.str();
        }
    }
    auto end = std::chrono::steady_clock::now(); // 終了時刻取得
    auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

    std::ostringstream oss;
    oss.imbue(std::locale(""));
    oss << "end snapshot reading"
        << " | entries: " << i
        << " | elapsed(ms): " << duration_ms
        << " | total bytes: " << total_bytes;
    LOG_LP(INFO) << oss.str();


    return 0;
}



