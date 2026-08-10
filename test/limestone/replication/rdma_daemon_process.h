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
#pragma once

#ifdef LIMESTONE_ENABLE_RDMA

#include <gtest/gtest.h>

#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <fcntl.h>
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#ifndef RDMA_HANDSHAKED_BIN
#error "RDMA_HANDSHAKED_BIN must be defined by the build to locate the daemon executable"
#endif

namespace limestone::testing {

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
    static constexpr std::chrono::milliseconds default_wait_timeout{5000};

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

} // namespace limestone::testing

#endif // LIMESTONE_ENABLE_RDMA
