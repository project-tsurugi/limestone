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
#include <grpc/backend/tp_monitor_backend.h>

#include <cstdlib>

#include <glog/logging.h>

#include <limestone/logging.h>
#include <logging_helper.h>

namespace limestone::grpc::backend {

namespace {

// Minimum implementation assumes AP1/AP2 only; changing this affects create/join flow and tests.
constexpr std::uint32_t default_participant_count = 2U;

tp_monitor_backend::result make_result(bool ok, std::string_view message) {
    return tp_monitor_backend::result{ok, std::string(message)};
}

tp_monitor_backend::create_result make_create_result(
        bool ok,
        std::uint64_t tpm_id,
        std::string_view message) {
    return tp_monitor_backend::create_result{ok, tpm_id, std::string(message)};
}

} // namespace

std::shared_ptr<tp_monitor_backend::monitor_state> tp_monitor_backend::find_state(
        std::uint64_t tpm_id) {
    std::lock_guard<std::mutex> lock(mtx_);
    auto iter = monitors_.find(tpm_id);
    if (iter == monitors_.end()) {
        return {};
    }
    return iter->second;
}

tp_monitor_backend::create_result tp_monitor_backend::create(std::string_view tx_id,
                                                             std::uint64_t ts_id) {
    static_cast<void>(tx_id);
    std::uint64_t tpm_id = next_tpm_id_.fetch_add(1U);
    if (tpm_id == 0U) {
        LOG_LP(FATAL) << "tpm_id overflow";
        std::abort();
    }
    auto state = std::make_shared<monitor_state>();
    state->tpm_id = tpm_id;
    state->participant_count = default_participant_count;
    state->participants.insert(ts_id);
    {
        std::lock_guard<std::mutex> lock(mtx_);
        monitors_.emplace(tpm_id, state);
    }
    return make_create_result(true, tpm_id, "");
}

tp_monitor_backend::create_result tp_monitor_backend::create_and_join(
        std::string_view tx_id1,
        std::uint64_t ts_id1,
        std::string_view tx_id2,
        std::uint64_t ts_id2) {
    static_cast<void>(tx_id1);
    static_cast<void>(tx_id2);
    std::uint64_t tpm_id = next_tpm_id_.fetch_add(1U);
    if (tpm_id == 0U) {
        LOG_LP(FATAL) << "tpm_id overflow";
        std::abort();
    }

    auto state = std::make_shared<monitor_state>();
    state->tpm_id = tpm_id;
    state->participant_count = default_participant_count;
    {
        std::lock_guard<std::mutex> lock(state->mtx);
        state->participants.insert(ts_id1);
        state->participants.insert(ts_id2);
    }

    {
        std::lock_guard<std::mutex> lock(mtx_);
        monitors_.emplace(tpm_id, state);
    }
    return make_create_result(true, tpm_id, "");
}

tp_monitor_backend::result tp_monitor_backend::join(std::uint64_t tpm_id,
                                                    std::string_view tx_id,
                                                    std::uint64_t ts_id) {
    static_cast<void>(tx_id);
    auto state = find_state(tpm_id);
    if (!state) {
        LOG_LP(WARNING) << "tpm_id not found. ignored. tpm_id=" << tpm_id;
        return make_result(false, "tpm_id not found");
    }

    std::lock_guard<std::mutex> lock(state->mtx);
    if (state->destroyed) {
        LOG_LP(WARNING) << "tpm_id is destroyed. ignored. tpm_id=" << tpm_id;
        return make_result(false, "tpm_id is destroyed");
    }
    if (state->participants.size() >= state->participant_count) {
        LOG_LP(WARNING) << "participants already full. ignored. tpm_id=" << tpm_id;
        return make_result(false, "participants already full");
    }
    auto [_, inserted] = state->participants.insert(ts_id);
    if (!inserted) {
        LOG_LP(WARNING) << "duplicate ts_id detected. ignored. ts_id=" << ts_id;
        return make_result(false, "duplicate ts_id");
    }
    return make_result(true, "");
}

tp_monitor_backend::result tp_monitor_backend::barrier_notify(
        std::uint64_t tpm_id,
        std::uint64_t ts_id) {
    auto state = find_state(tpm_id);
    if (!state) {
        LOG_LP(WARNING) << "tpm_id not found. ignored. tpm_id=" << tpm_id;
        return make_result(false, "tpm_id not found");
    }

    std::unique_lock<std::mutex> lock(state->mtx);
    if (state->destroyed) {
        LOG_LP(WARNING) << "tpm_id is destroyed. ignored. tpm_id=" << tpm_id;
        return make_result(false, "tpm_id is destroyed");
    }
    if (state->participants.find(ts_id) == state->participants.end()) {
        LOG_LP(WARNING) << "ts_id not registered. ignored. ts_id=" << ts_id;
        return make_result(false, "ts_id not registered");
    }

    state->arrived.insert(ts_id);
    if (state->arrived.size() >= state->participant_count) {
        state->cv.notify_all();
        return make_result(true, "");
    }

    state->cv.wait(lock, [&state]() {
        return state->destroyed || state->arrived.size() >= state->participant_count;
    });

    if (state->destroyed) {
        LOG_LP(WARNING) << "tpm_id is destroyed while waiting. ignored. tpm_id=" << tpm_id;
        return make_result(false, "tpm_id is destroyed");
    }

    return make_result(true, "");
}

tp_monitor_backend::result tp_monitor_backend::destroy(std::uint64_t tpm_id) {
    auto state = find_state(tpm_id);
    if (!state) {
        LOG_LP(WARNING) << "tpm_id not found. ignored. tpm_id=" << tpm_id;
        return make_result(false, "tpm_id not found");
    }

    {
        std::lock_guard<std::mutex> lock(state->mtx);
        state->destroyed = true;
        state->cv.notify_all();
    }

    {
        std::lock_guard<std::mutex> lock(mtx_);
        monitors_.erase(tpm_id);
    }

    return make_result(true, "");
}

} // namespace limestone::grpc::backend
