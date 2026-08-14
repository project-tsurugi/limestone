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

#include <chrono>
#include <cstdint>
#include <string>
#include <vector>

#include <rdma/handshake_client_base.h>

namespace limestone::testing {

/**
 * @brief Script and call record shared between a test and its mock connector.
 *
 * establish_rdma_session() owns the connector it obtains from the factory and
 * destroys it before returning, so the scripted results and the recorded
 * arguments live here, owned by the test, and survive the connector.
 */
struct mock_handshake_connector_script {
    using operation_result = limestone::replication::handshake_client_base::operation_result;
    using receive_result = limestone::replication::handshake_client_base::receive_result;

    // Scripted results; the defaults describe an all-success handshake with an
    // empty response payload. A test overrides the step it wants to fail or the
    // payload it wants the master to decode.
    operation_result start_result{true, {}};
    receive_result response_result{true, {}, {}};
    operation_result finalize_result{true, {}};
    operation_result ready_result{true, {}};
    operation_result completion_result{true, {}};

    // Factory arguments recorded when the installed factory runs.
    int factory_calls{};
    std::string daemon_socket_path{};
    std::chrono::milliseconds operation_timeout{};

    // Call arguments recorded by the connector.
    std::uint64_t target_service{};
    std::vector<std::uint8_t> start_payload{};
    std::vector<std::uint8_t> finalize_payload{};

    // Call counters, one per handshake step.
    int start_calls{};
    int receive_response_calls{};
    int send_finalize_calls{};
    int send_ready_calls{};
    int receive_completion_calls{};
};

/**
 * @brief Scripted connect-side handshake client for tests.
 *
 * Each step records its arguments into the shared script and returns the
 * script's preconfigured result, so a test can fail any single handshake step
 * deterministically and inspect what the caller sent up to that point.
 */
class mock_handshake_connector : public limestone::replication::handshake_connector_base {
public:
    explicit mock_handshake_connector(mock_handshake_connector_script& script) noexcept
        : script_(script) {}

    [[nodiscard]] operation_result start(
            std::uint64_t target_service,
            std::vector<std::uint8_t> const& start_payload) noexcept override {
        ++script_.start_calls;
        script_.target_service = target_service;
        script_.start_payload = start_payload;
        return script_.start_result;
    }

    [[nodiscard]] receive_result receive_response() noexcept override {
        ++script_.receive_response_calls;
        return script_.response_result;
    }

    [[nodiscard]] operation_result send_finalize(
            std::vector<std::uint8_t> const& finalize_payload) noexcept override {
        ++script_.send_finalize_calls;
        script_.finalize_payload = finalize_payload;
        return script_.finalize_result;
    }

    [[nodiscard]] operation_result send_ready() noexcept override {
        ++script_.send_ready_calls;
        return script_.ready_result;
    }

    [[nodiscard]] operation_result receive_completion() noexcept override {
        ++script_.receive_completion_calls;
        return script_.completion_result;
    }

private:
    mock_handshake_connector_script& script_;
};

}  // namespace limestone::testing
