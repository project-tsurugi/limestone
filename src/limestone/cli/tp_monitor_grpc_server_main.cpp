/*
 * Copyright 2026 Project Tsurugi.
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

#include <algorithm>
#include <atomic>
#include <charconv>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <iostream>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include <gflags/gflags.h>
#include <grpcpp/grpcpp.h>

#include <grpc/service/tp_monitor_service_impl.h>

// NOLINTBEGIN(performance-avoid-endl)

DEFINE_string(host, "0.0.0.0", "Listen address (default: 0.0.0.0)");
DEFINE_int32(port, 39515, "Listen port (default: 39515)");

namespace {

std::atomic<bool>& shutdown_requested() {
    static std::atomic<bool> instance{false};
    return instance;
}

void handle_signal(int) {
    shutdown_requested().store(true);
}

bool parse_string_flag(int& index,
                       const std::vector<std::string_view>& args,
                       std::string_view flag,
                       std::string& value) {
    std::string_view arg{args.at(index)};
    if (arg.rfind(flag, 0) == 0 && arg.size() > flag.size()
        && arg.compare(flag.size(), 1, "=") == 0) {
        value = std::string(arg.substr(flag.size() + 1));
        return true;
    }
    if (arg == flag) {
        if (index + 1 >= static_cast<int>(args.size())) {
            return false;
        }
        value = std::string(args.at(++index));
        return true;
    }
    return false;
}

bool parse_port_flag(int& index, const std::vector<std::string_view>& args, int& value) {
    std::string_view arg{args.at(index)};
    std::string_view flag = "--port";
    std::string_view prefix = "--port=";
    std::string_view text{};
    if (arg.rfind(prefix, 0) == 0) {
        text = arg.substr(prefix.size());
    } else if (arg == flag) {
        if (index + 1 >= static_cast<int>(args.size())) {
            return false;
        }
        text = args.at(++index);
    } else {
        return false;
    }

    int parsed = 0;
    auto begin = text.begin();
    auto end = text.end();
    auto [ptr, ec] = std::from_chars(begin, end, parsed);
    if (ec != std::errc() || ptr != end) {
        return false;
    }
    if (parsed < 1 || parsed > 65535) {
        return false;
    }
    value = parsed;
    return true;
}

void show_usage(const std::string& program_name) {
    std::cout << "Usage: " << program_name << " [options]\n";
    std::cout << "\nOptions:\n";
    std::cout << "  --host <address>   Listen address (default: 0.0.0.0)\n";
    std::cout << "  --port <port>      Listen port (default: 39515)\n";
    std::cout << "  --usage            Show this usage and exit\n";
}

bool parse_args(const std::vector<std::string_view>& args, const std::string& program_name) {
    for (int i = 1; i < static_cast<int>(args.size()); ++i) {
        std::string_view arg{args.at(i)};
        if (arg == "--usage" || arg == "--help" || arg == "-h") {
            show_usage(program_name);
            return false;
        }
        if (parse_string_flag(i, args, "--host", FLAGS_host)) {
            continue;
        }
        if (parse_port_flag(i, args, FLAGS_port)) {
            continue;
        }
        show_usage(program_name);
        return false;
    }
    return true;
}

void initialize_and_run_grpc_server(const std::string& host, int port) {
    if (std::signal(SIGINT, handle_signal) == SIG_ERR) {
        std::cerr << "Failed to set SIGINT handler" << std::endl;
        std::exit(1);
    }
    if (std::signal(SIGTERM, handle_signal) == SIG_ERR) {
        std::cerr << "Failed to set SIGTERM handler" << std::endl;
        std::exit(1);
    }

    limestone::grpc::service::tp_monitor_service_impl tp_monitor_service{};

    std::string server_address = host + ":" + std::to_string(port);

    grpc::ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&tp_monitor_service);
    std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
    if (!server) {
        std::cerr << "Failed to start gRPC server on " << server_address << std::endl;
        std::exit(1);
    }
    std::cout << "gRPC tp_monitor server started on " << server_address << std::endl;

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
    std::vector<std::string_view> args{};
    args.reserve(static_cast<std::size_t>(argc));
    std::vector<char*> argv_values{};
    argv_values.reserve(static_cast<std::size_t>(argc));
    std::copy_n(argv, argc, std::back_inserter(argv_values));
    for (auto const* arg : argv_values) {
        args.emplace_back(arg);
    }

    const std::string program_name = args.empty() ? "tg-grpc-tpmonitor" : std::string(args.at(0));

    if (!parse_args(args, program_name)) {
        return 2;
    }

    std::cout << "tg-grpc-tpmonitor (gRPC TP monitor service) starting..." << std::endl;
    std::cout << "Listen address: " << FLAGS_host << std::endl;
    std::cout << "Listen port: " << FLAGS_port << std::endl;
    std::cout << "To stop the server, press CTRL+C." << std::endl;

    initialize_and_run_grpc_server(FLAGS_host, FLAGS_port);
    return 0;
}

// NOLINTEND(performance-avoid-endl)
