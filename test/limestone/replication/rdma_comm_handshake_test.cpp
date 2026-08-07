/*
 * Copyright 2022-2026 Project Tsurugi.
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
#ifdef LIMESTONE_ENABLE_RDMA

#include <gtest/gtest.h>

#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <fcntl.h>
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#include <rdma/handshake_client_base.h>
#include <rdma/rdma_factory.h>

#ifndef RDMA_HANDSHAKED_BIN
#error "RDMA_HANDSHAKED_BIN must be defined by the build to locate the daemon executable"
#endif

namespace limestone::testing {

namespace {

using limestone::replication::handshake_acceptor_base;
using limestone::replication::handshake_client_base;
using limestone::replication::handshake_connector_base;
using limestone::replication::make_handshake_acceptor;
using limestone::replication::make_handshake_connector;

constexpr std::chrono::milliseconds default_wait_timeout{5000};

/**
 * @brief Forks and execs a real rdma_handshaked process, capturing its stderr so tests can
 *        wait for specific log lines (e.g. bring-up completion).
 *
 * Mirrors the daemon_process helper in rdma-comm-lib's own
 * tests/src/handshake/rdma_handshaked_scenario_test.cpp: rdma_comm_handshake_connector and
 * _acceptor have no vendor mock of their own, so exercising them end-to-end requires two real
 * daemon processes paired over the vendor mock's shared memory.
 */
class daemon_process {
public:
    explicit daemon_process(std::vector<std::string> const& args) {
        ::setenv("GLOG_logtostderr", "1", 1);
        ::setenv("GLOG_v", "30", 1);

        std::vector<std::string> argv_storage;
        argv_storage.reserve(args.size() + 1);
        argv_storage.emplace_back(RDMA_HANDSHAKED_BIN);
        for (auto const& arg : args) {
            argv_storage.push_back(arg);
        }
        std::vector<char*> argv;
        argv.reserve(argv_storage.size() + 1);
        for (auto& arg : argv_storage) {
            argv.push_back(arg.data());
        }
        argv.push_back(nullptr);

        int pipe_fds[2]{};
        if (::pipe2(pipe_fds, O_CLOEXEC) != 0) {
            ADD_FAILURE() << "pipe2() failed for the daemon stderr capture";
            return;
        }

        pid_ = ::fork();
        if (pid_ < 0) {
            ::close(pipe_fds[0]);
            ::close(pipe_fds[1]);
            ADD_FAILURE() << "fork() failed to start the daemon binary";
            return;
        }

        if (pid_ == 0) {
            ::dup2(pipe_fds[1], STDERR_FILENO);
            ::dup2(pipe_fds[1], STDOUT_FILENO);
            ::close(pipe_fds[0]);
            ::close(pipe_fds[1]);

            ::execv(RDMA_HANDSHAKED_BIN, argv.data());
            ::perror("execv rdma_handshaked");
            ::_exit(127);
        }

        ::close(pipe_fds[1]);
        log_fd_ = pipe_fds[0];
        reader_ = std::thread([this] { read_loop(); });
    }

    daemon_process(daemon_process const&) = delete;
    daemon_process& operator=(daemon_process const&) = delete;
    daemon_process(daemon_process&&) = delete;
    daemon_process& operator=(daemon_process&&) = delete;

    ~daemon_process() {
        terminate();
        if (!wait_exit(default_wait_timeout) && pid_ > 0 && !reaped_) {
            ::kill(pid_, SIGKILL);
            int status{};
            (void) ::waitpid(pid_, &status, 0);
            reaped_ = true;
        }
        if (log_fd_ >= 0) {
            ::close(log_fd_);
        }
        if (reader_.joinable()) {
            reader_.join();
        }
        log_fd_ = -1;
    }

    [[nodiscard]] bool started() const noexcept { return pid_ > 0; }

    [[nodiscard]] bool wait_for_log(
            std::string const&        needle,
            std::chrono::milliseconds timeout = default_wait_timeout) {
        std::unique_lock<std::mutex> lock{mutex_};
        cv_.wait_for(lock, timeout, [&] { return contains_locked(needle) || eof_; });
        return contains_locked(needle);
    }

    void terminate() noexcept {
        if (pid_ > 0 && !reaped_) {
            ::kill(pid_, SIGTERM);
        }
    }

    [[nodiscard]] bool wait_exit(std::chrono::milliseconds timeout = default_wait_timeout) {
        if (pid_ <= 0) {
            return false;
        }
        if (reaped_) {
            return true;
        }
        auto const deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            int         status{};
            pid_t const result = ::waitpid(pid_, &status, WNOHANG);
            if (result == pid_) {
                reaped_ = true;
                return true;
            }
            if (result < 0) {
                return false;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds{20});
        }
        return false;
    }

private:
    [[nodiscard]] bool contains_locked(std::string const& needle) const {
        for (auto const& line : lines_) {
            if (line.find(needle) != std::string::npos) {
                return true;
            }
        }
        return false;
    }

    void read_loop() {
        std::string partial;
        char        buffer[4096];
        for (;;) {
            ssize_t const count = ::read(log_fd_, buffer, sizeof(buffer));
            if (count > 0) {
                partial.append(buffer, static_cast<std::size_t>(count));
                std::size_t newline{};
                while ((newline = partial.find('\n')) != std::string::npos) {
                    push_line(partial.substr(0, newline));
                    partial.erase(0, newline + 1);
                }
                continue;
            }
            if (count == 0) {
                if (!partial.empty()) {
                    push_line(partial);
                }
                break;
            }
            if (errno == EINTR) {
                continue;
            }
            break;
        }
        {
            std::lock_guard<std::mutex> lock{mutex_};
            eof_ = true;
        }
        cv_.notify_all();
    }

    void push_line(std::string line) {
        {
            std::lock_guard<std::mutex> lock{mutex_};
            lines_.push_back(std::move(line));
        }
        cv_.notify_all();
    }

    pid_t       pid_{-1};
    int         log_fd_{-1};
    std::thread reader_;

    std::mutex               mutex_;
    std::condition_variable  cv_;
    std::vector<std::string> lines_;
    bool                     eof_{false};
    bool                     reaped_{false};
};

} // namespace

/**
 * @brief End-to-end coverage of rdma_comm_handshake_connector / _acceptor against two real
 *        rdma_handshaked daemon processes.
 *
 * Brings up a server/client daemon pair over a conn_info file (the same bring-up sequence
 * operators use: --export-conn-info first, then --import-conn-info once the file exists), then
 * drives a full application handshake through make_handshake_connector() / _acceptor() and
 * asserts the exact payload round trip at every step.
 */
class rdma_comm_handshake_test : public ::testing::Test {
protected:
    void SetUp() override {
        std::error_code ec;
        auto const      tmp_root = std::filesystem::temp_directory_path(ec);
        ASSERT_FALSE(ec) << "temp_directory_path failed: " << ec.message();
        auto        base = tmp_root / "rdma_comm_handshake_test_XXXXXX";
        std::string templ = base.string();
        char* const dir = ::mkdtemp(templ.data());
        ASSERT_NE(dir, nullptr) << "mkdtemp failed for the test temp directory";
        temp_dir_ = dir;
        conn_info_path_ = (std::filesystem::path{temp_dir_} / "conn.info").string();
        server_socket_path_ = (std::filesystem::path{temp_dir_} / "server.sock").string();
        client_socket_path_ = (std::filesystem::path{temp_dir_} / "client.sock").string();
    }

    void TearDown() override {
        if (!temp_dir_.empty()) {
            std::error_code ec;
            std::filesystem::remove_all(temp_dir_, ec);
        }
    }

    std::string temp_dir_;
    std::string conn_info_path_;
    std::string server_socket_path_;
    std::string client_socket_path_;

    static constexpr char const* handshake_timeout_arg = "--handshake-timeout=2000";
    static constexpr std::uint64_t service_id = 42U;
};

TEST_F(rdma_comm_handshake_test, full_handshake_round_trips_payloads) {
    daemon_process server{{
        "--export-conn-info=" + conn_info_path_,
        "--rdma-port=0",
        "--listen=" + server_socket_path_,
        handshake_timeout_arg,
    }};
    ASSERT_TRUE(server.started());
    ASSERT_TRUE(server.wait_for_log("exported connection-info"))
        << "server daemon did not export the connection-info file in time";

    daemon_process client{{
        "--import-conn-info=" + conn_info_path_,
        "--rdma-port=0",
        "--listen=" + client_socket_path_,
        handshake_timeout_arg,
    }};
    ASSERT_TRUE(client.started());

    ASSERT_TRUE(server.wait_for_log("listening for local applications on " + server_socket_path_))
        << "server daemon did not start listening in time";
    ASSERT_TRUE(client.wait_for_log("listening for local applications on " + client_socket_path_))
        << "client daemon did not start listening in time";

    std::vector<std::uint8_t> const start_payload{1, 2, 3, 4, 5};
    std::vector<std::uint8_t> const response_payload{9, 8, 7};
    std::vector<std::uint8_t> const finalize_payload{0xAAU, 0xBBU};

    // The daemon rejects a start that arrives before the accept session registers, and the
    // rejection closes the connector's socket, so recovery is a new create_connector(), not a
    // re-call on the same instance (see handshake_client_base.h's ordering note). Provoke that
    // rejection deterministically before the accept side comes up.
    {
        auto doomed_result = make_handshake_connector(client_socket_path_, default_wait_timeout);
        ASSERT_TRUE(doomed_result.status.success) << doomed_result.status.error_message;
        auto doomed_start = doomed_result.instance->start(service_id, start_payload);
        EXPECT_FALSE(doomed_start.success)
            << "start() before the accept side registered should have been rejected";
    }

    // Bring up the accept side on a worker thread (create_acceptor() + wait_for_start()) while
    // the connect side retries start() from a fresh connector each time. Mirrors rdma-comm-lib's
    // own rdma_handshaked_scenario_test.cpp application_handshake_relays_payloads_end_to_end.
    // No ASSERT is allowed between here and the join below: a fatal assertion would return with
    // acceptor_thread still joinable and std::terminate the whole binary.
    std::unique_ptr<handshake_acceptor_base> acceptor;
    handshake_client_base::receive_result    accept_start;
    std::thread acceptor_thread([&] {
        auto acceptor_result = make_handshake_acceptor(server_socket_path_, default_wait_timeout);
        if (!acceptor_result.status.success) {
            accept_start = {false, acceptor_result.status.error_message, {}};
            return;
        }
        acceptor = std::move(acceptor_result.instance);
        accept_start = acceptor->wait_for_start(service_id);
    });

    std::unique_ptr<handshake_connector_base> connector;
    handshake_client_base::operation_result   connect_start;
    auto const retry_deadline = std::chrono::steady_clock::now() + default_wait_timeout;
    for (;;) {
        auto connector_result = make_handshake_connector(client_socket_path_, default_wait_timeout);
        if (!connector_result.status.success) {
            connect_start = {false, connector_result.status.error_message};
            server.terminate();
            break;
        }
        auto candidate = std::move(connector_result.instance);
        connect_start = candidate->start(service_id, start_payload);
        if (connect_start.success) {
            connector = std::move(candidate);
            break;
        }
        if (std::chrono::steady_clock::now() >= retry_deadline) {
            // Unblock the accept worker's wait_for_start() so the join below cannot hang.
            server.terminate();
            ADD_FAILURE() << "connect side did not recover by retrying start within the deadline";
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{10});
    }
    acceptor_thread.join();
    EXPECT_TRUE(connect_start.success) << connect_start.error_message;
    EXPECT_TRUE(accept_start.success) << accept_start.error_message;
    if (!connect_start.success || !accept_start.success) {
        return;
    }
    EXPECT_EQ(accept_start.payload, start_payload);

    auto accept_response = acceptor->send_response(response_payload);
    ASSERT_TRUE(accept_response.success) << accept_response.error_message;

    auto connect_response = connector->receive_response();
    ASSERT_TRUE(connect_response.success) << connect_response.error_message;
    EXPECT_EQ(connect_response.payload, response_payload);

    auto connect_finalize = connector->send_finalize(finalize_payload);
    ASSERT_TRUE(connect_finalize.success) << connect_finalize.error_message;

    auto accept_finalize = acceptor->receive_finalize();
    ASSERT_TRUE(accept_finalize.success) << accept_finalize.error_message;
    EXPECT_EQ(accept_finalize.payload, finalize_payload);

    handshake_client_base::operation_result accept_complete;
    std::thread complete_thread([&] {
        accept_complete = acceptor->complete();
    });

    auto connect_ready = connector->send_ready();
    complete_thread.join();
    ASSERT_TRUE(connect_ready.success) << connect_ready.error_message;
    EXPECT_TRUE(accept_complete.success) << accept_complete.error_message;

    auto connect_completion = connector->receive_completion();
    EXPECT_TRUE(connect_completion.success) << connect_completion.error_message;
}

} // namespace limestone::testing

#endif // LIMESTONE_ENABLE_RDMA
