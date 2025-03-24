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

#include <iostream>
#include <boost/filesystem.hpp>
#include <string>
#include "replication/replication_endpoint.h"
#include "replication/replica_server.h"

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

    limestone::replication::replication_endpoint endpoint{};
    if (endpoint.env_defined()) {
        std::cout << "Endpoint: " << endpoint.host() << ":" << endpoint.port() << std::endl;
    } else {
        std::cerr << "Error: TSURUGI_REPLICATION_ENDPOINT environment variable is not set." << std::endl;
        show_usage(program_name);
        return 1;
    }
    if (!endpoint.is_valid()) {
        std::cerr << "Error: Invalid endpoint specified in TSURUGI_REPLICATION_ENDPOINT." << std::endl;
        show_usage(program_name);
        return 1;
    }

    limestone::replication::replica_server server{};

    server.initialize(log_dir_path);        

    bool success = server.start_listener(endpoint.get_sockaddr());
    if (!success) {
        return 1;
    }

    server.accept_loop();

    return 0;
}

