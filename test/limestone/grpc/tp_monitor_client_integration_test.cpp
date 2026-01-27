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
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <future>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>

#include <limits.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <grpcpp/grpcpp.h>

#include <grpc/client/tp_monitor_client.h>
#include <test_root.h>

namespace limestone::testing {

namespace {

constexpr int tp_monitor_server_port = 39515;
constexpr std::chrono::milliseconds server_ready_timeout{5000};

std::string build_server_path() {
    char exe_path[PATH_MAX] = {};
    auto len = ::readlink("/proc/self/exe", exe_path, sizeof(exe_path) - 1);
    if (len <= 0) {
        return {};
    }
    exe_path[len] = '\0';
    auto test_path = std::filesystem::path(exe_path);
    auto build_dir = test_path.parent_path().parent_path();
    auto server_path = build_dir / "src" / "tg-grpc-tpmonitor";
    return server_path.string();
}

std::string build_server_address() {
    return "127.0.0.1:" + std::to_string(tp_monitor_server_port);
}

bool wait_for_server_ready(const std::string& address) {
    auto channel = ::grpc::CreateChannel(address, ::grpc::InsecureChannelCredentials());
    auto deadline = std::chrono::system_clock::now() + server_ready_timeout;
    return channel->WaitForConnected(deadline);
}

class tp_monitor_server_process {
public:
    tp_monitor_server_process() = default;
    ~tp_monitor_server_process() { stop(); }

    tp_monitor_server_process(const tp_monitor_server_process&) = delete;
    tp_monitor_server_process& operator=(const tp_monitor_server_process&) = delete;
    tp_monitor_server_process(tp_monitor_server_process&&) = delete;
    tp_monitor_server_process& operator=(tp_monitor_server_process&&) = delete;

    bool start() {
        auto server_path = build_server_path();
        if (server_path.empty()) {
            return false;
        }
        std::string port_arg = std::to_string(tp_monitor_server_port);
        log_path_ = std::filesystem::temp_directory_path() / "tp_monitor_server_test.log";
        pid_ = ::fork();
        if (pid_ == 0) {
            int fd = ::open(log_path_.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
            if (fd >= 0) {
                ::dup2(fd, STDOUT_FILENO);
                ::dup2(fd, STDERR_FILENO);
                ::close(fd);
            }
            char const* argv[] = {
                server_path.c_str(),
                "--host",
                "0.0.0.0",
                "--port",
                port_arg.c_str(),
                nullptr
            };
            ::execv(server_path.c_str(), const_cast<char* const*>(argv));
            std::cerr << "execv failed: " << std::strerror(errno) << std::endl;
            std::abort();
        }
        return pid_ > 0;
    }

    void stop() {
        if (pid_ <= 0) {
            return;
        }
        ::kill(pid_, SIGTERM);
        int status = 0;
        (void)::waitpid(pid_, &status, 0);
        pid_ = -1;
    }

    void dump_log() const {
        std::ifstream in(log_path_);
        if (!in) {
            std::cerr << "Failed to open server log: " << log_path_ << std::endl;
            return;
        }
        std::cerr << "=== tp_monitor server log (" << log_path_ << ") ===" << std::endl;
        std::string line;
        while (std::getline(in, line)) {
            std::cerr << line << std::endl;
        }
    }

private:
    pid_t pid_{-1};
    std::filesystem::path log_path_{};
};

} // namespace

class tp_monitor_client_integration_test : public ::testing::Test {};

TEST_F(tp_monitor_client_integration_test, client_talks_to_external_server) { // NOLINT
    tp_monitor_server_process server{};
    ASSERT_TRUE(server.start());

    auto server_address = build_server_address();
    if (!wait_for_server_ready(server_address)) {
        server.dump_log();
        FAIL() << "Server not ready";
    }

    auto channel = ::grpc::CreateChannel(server_address, ::grpc::InsecureChannelCredentials());
    limestone::grpc::client::tp_monitor_client client(channel);

    auto create_result = client.create("tx-1", 1U);
    ASSERT_TRUE(create_result.ok);
    ASSERT_TRUE(create_result.tpm_id != 0U);

    auto join_result = client.join(create_result.tpm_id, "tx-2", 2U);
    ASSERT_TRUE(join_result.ok);

    auto first_notify = std::async(std::launch::async, [&client, &create_result]() {
        return client.barrier_notify(create_result.tpm_id, 1U);
    });
    auto notify_result = client.barrier_notify(create_result.tpm_id, 2U);
    EXPECT_TRUE(notify_result.ok);
    EXPECT_TRUE(first_notify.get().ok);

    auto destroy_result = client.destroy(create_result.tpm_id);
    EXPECT_TRUE(destroy_result.ok);
}

} // namespace limestone::testing
