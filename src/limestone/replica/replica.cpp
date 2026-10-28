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

#include <chrono>
#include <iostream>
#include <boost/filesystem.hpp>
#include <string>
#include <thread>
#include <limestone/api/configuration.h>
#include <limestone/api/limestone_exception.h>
#include <replication/replication_config_loader.h>
#include "replication/sort_config_loader.h"
#include "replication/replication_endpoint.h"
#include "replication/replica_server.h"

// NOLINTBEGIN(performance-avoid-endl)

namespace {

void show_usage(const std::string& program_name) {
    std::cerr << "Usage: " << program_name << " <logdir>" << std::endl;
    std::cerr << "Note: The environment variable TSURUGI_REPLICATION_ENDPOINT must be set with the endpoint URL." << std::endl;
    std::cerr << "      For example: tcp://localhost:1234" << std::endl;
    std::cerr << "      The sort at startup can be configured with LIMESTONE_SORT_THREADS," << std::endl;
    std::cerr << "      LIMESTONE_SORT_ARENA_SIZE_MB, LIMESTONE_SORT_WORK_DIR and LIMESTONE_SORT_MERGE_FANIN."
              << std::endl;
    std::cerr << "      The sort creates its working directory sorting-XXXXXX under LIMESTONE_SORT_WORK_DIR"
              << " and removes it when it ends (a crash may leave it)." << std::endl;
}

/**
 * @brief Initializes the replica_server. If the checks of the construction of the datastore fail, writes the reason
 *        to stderr and returns false.
 * @param server the server to initialize
 * @param log_dir_path the log directory
 * @param conf the configuration of the datastore
 * @return whether it succeeded
 */
bool initialize_server(limestone::replication::replica_server& server, const boost::filesystem::path& log_dir_path,
                       const limestone::api::configuration& conf) {
    try {
        server.initialize(log_dir_path, conf);
    } catch (const limestone::api::limestone_exception& e) {
        std::cerr << "Error: failed to initialize the replica: " << e.what() << std::endl;
        return false;
    }
    return true;
}

} // namespace

int main(int argc, char* argv[]) {
    // Convert argv to vector<string> to avoid direct pointer arithmetic.
    std::vector<std::string> args(argv, argv + argc);  // NOLINT(*-pointer-arithmetic)
    
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

    auto const config_result = limestone::replication::load_replication_config_from_environment();
    if (!config_result.ok) {
        std::cerr << "Error: invalid replication configuration: " << config_result.error_message
                  << std::endl;
        show_usage(program_name);
        return 1;
    }
    limestone::replication::replication_config const& config = config_result.config;

    // The settings of the sort (environment variables). The ranges of the values are checked at the construction of
    // the datastore, and initialize() throws if they are invalid.
    auto const sort_result = limestone::replication::load_sort_config_from_environment();
    if (!sort_result.ok) {
        std::cerr << "Error: invalid sort configuration: " << sort_result.error_message << std::endl;
        show_usage(program_name);
        return 1;
    }
    limestone::api::configuration conf{};
    limestone::replication::apply_sort_config(sort_result.config, conf);

    if (config.mode() == limestone::replication::replication_mode::rdma) {
        limestone::replication::replica_server server{};
        if (!initialize_server(server, log_dir_path, conf)) {
            return 1;
        }

        std::cout << "[replica] waiting for the master on the handshake daemon: "
                  << config.handshake_socket_path() << std::endl;
        if (!server.establish_rdma_session(config.handshake_socket_path(), config.service_id())) {
            std::cerr << "Error: failed to establish the RDMA replication session." << std::endl;
            return 1;
        }
        std::cout << "[replica] initialized and listening" << std::endl;

        // Replication data arrives on the RDMA receive threads; the main thread only
        // keeps the process alive until it is stopped by a signal.
        // A clean shutdown will be defined as part of the planned replica detach
        // protocol (the replica requests detachment, the master detaches it and
        // responds, and the master keeps running), which is a separate task.
        for (;;) {
            std::this_thread::sleep_for(std::chrono::seconds(60));
        }
    }
    if (config.mode() == limestone::replication::replication_mode::none) {
        std::cerr << "Error: TSURUGI_REPLICATION_ENDPOINT environment variable is not set." << std::endl;
        show_usage(program_name);
        return 1;
    }

    limestone::replication::replication_endpoint endpoint{};
    if (!endpoint.is_valid()) {
        std::cerr << "Error: Invalid endpoint specified in TSURUGI_REPLICATION_ENDPOINT." << std::endl;
        show_usage(program_name);
        return 1;
    }
    std::cout << "Endpoint: " << endpoint.host() << ":" << endpoint.port() << std::endl;

    limestone::replication::replica_server server{};

    if (!initialize_server(server, log_dir_path, conf)) {
        return 1;
    }

    bool success = server.start_listener(endpoint.get_sockaddr());
    if (!success) {
        return 1;
    }

    std::cout << "[replica] initialized and listening" << std::endl;

    server.accept_loop();

    return 0;
}

// NOLINTEND(performance-avoid-endl)