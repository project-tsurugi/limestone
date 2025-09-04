/*
 * Copyright 2024-2025 Project Tsurugi.
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
#include <string>
#include <gflags/gflags.h>
#include <boost/filesystem.hpp>
#include <atomic>
#include <csignal>
#include <thread>
#include <chrono>
#include <grpcpp/grpcpp.h>
#include "grpc/backend/grpc_service_backend.h"
#include "grpc/service/backup_service_impl.h"
#include "grpc/service/wal_history_service_impl.h"
#include "grpc/service/ping_service.h"

// NOLINTBEGIN(performance-avoid-endl)

DEFINE_string(host, "0.0.0.0", "Listen address (default: 0.0.0.0)");
DEFINE_int32(port, 39514, "Listen port (default: 39514)");
DEFINE_bool(help, false, "Show this help message and exit");

namespace {

void show_usage(const std::string& program_name) {
    std::cout << "Usage: " << program_name << " [options] <logdir>\n";
    std::cout << "\nOptions:\n";
    std::cout << "  --host <address>   Listen address (default: 0.0.0.0)\n";
    std::cout << "  --port <port>      Listen port (default: 39514)\n";
    std::cout << "  --help             Show this help message and exit\n";
    std::cout << "\nArguments:\n";
    std::cout << "  <logdir>           Target log directory\n";
}

// Use a function-local static to avoid a non-const global variable.
std::atomic<bool>& shutdown_requested() {
    static std::atomic<bool> instance{false};
    return instance;
}

void handle_signal(int) {
    shutdown_requested().store(true);
}

void initialize_and_run_grpc_server(const std::string& logdir, const std::string& host, int port) {
    // Setup signal handler for graceful shutdown
    if (std::signal(SIGINT, handle_signal) == SIG_ERR) {
        std::cerr << "Failed to set SIGINT handler" << std::endl;
        std::exit(1);
    }
    if (std::signal(SIGTERM, handle_signal) == SIG_ERR) {
        std::cerr << "Failed to set SIGTERM handler" << std::endl;
        std::exit(1);
    }

    // Create backend
    auto backend = limestone::grpc::backend::grpc_service_backend::create_standalone(logdir);

    // Create service implementations
    limestone::grpc::service::BackupServiceImpl backup_service(*backend);
    limestone::grpc::service::wal_history_service_impl wal_history_service(*backend);
    limestone::grpc::service::ping_service ping_service;

    // Build server address
    std::string server_address = host + ":" + std::to_string(port);

    // Build and start gRPC server
    grpc::ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&backup_service);
    builder.RegisterService(&wal_history_service);
    builder.RegisterService(&ping_service);
    std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
    if (!server) {
        std::cerr << "Failed to start gRPC server on " << server_address << std::endl;
        std::exit(1);
    }
    std::cout << "gRPC server started on " << server_address << std::endl;

    // Wait for shutdown signal
    while (!shutdown_requested().load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
    std::cout << "Shutdown signal received. Stopping server..." << std::endl;
    server->Shutdown();
    server->Wait();
    std::cout << "Server stopped." << std::endl;
}

} // namespace

int main(int argc, char* argv[]) {
    std::vector<std::string> args(argv, argv + argc);

    gflags::SetUsageMessage("Usage: tg-grpc-backupd [options] <logdir>");
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    const std::string program_name = boost::filesystem::path(std::string(args.at(0))).filename().string();

    if (FLAGS_help) {
        show_usage(program_name);
        return 0;
    }

    if (argc != 2) {
        std::cerr << "Error: <logdir> argument is required.\n";
        show_usage(program_name);
        return 1;
    }

    std::string logdir = args.at(1);
    boost::filesystem::path log_dir_path(logdir);
    if (!boost::filesystem::exists(log_dir_path)) {
        std::cerr << "Error: Directory does not exist: " << logdir << std::endl;
        return 1;
    }
    if (!boost::filesystem::is_directory(log_dir_path)) {
        std::cerr << "Error: Specified path is not a directory: " << logdir << std::endl;
        return 1;
    }

    std::cout << "tg-grpc-backupd (gRPC remote backup service) starting..." << std::endl;
    std::cout << "Listen address: " << FLAGS_host << std::endl;
    std::cout << "Listen port: " << FLAGS_port << std::endl;
    std::cout << "Log directory: " << logdir << std::endl;
    std::cout << "To stop the server, press CTRL+C." << std::endl;

    initialize_and_run_grpc_server(logdir, FLAGS_host, FLAGS_port);
    return 0;
}

// NOLINTEND(performance-avoid-endl)




