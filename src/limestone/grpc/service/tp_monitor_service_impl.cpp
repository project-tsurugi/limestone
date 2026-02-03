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
#include <grpc/service/tp_monitor_service_impl.h>

#include <cstdlib>

#include <glog/logging.h>

#include <limestone/logging.h>
#include <logging_helper.h>

namespace limestone::grpc::service {

tp_monitor_service_impl::tp_monitor_service_impl() = default;

tp_monitor_service_impl::~tp_monitor_service_impl() = default;

::grpc::Status tp_monitor_service_impl::Create(::grpc::ServerContext*,
                                               const CreateRequest* request,
                                               CreateResponse* response) {
    std::uint32_t participant_count = request->participantcount();
    if (participant_count == 0U) {
        return {::grpc::StatusCode::INVALID_ARGUMENT, "participant_count must be >= 1"};
    }
    auto result = create_monitor(participant_count);
    if (! result.ok) {
        return {::grpc::StatusCode::INVALID_ARGUMENT, "create failed"};
    }
    response->set_tpmid(result.tpm_id);
    return ::grpc::Status::OK;
}

::grpc::Status tp_monitor_service_impl::Join(::grpc::ServerContext*,
                                             const JoinRequest* request,
                                             JoinResponse* response) {
    auto result = join_monitor(request->tpmid(), request->txid(), request->tsid());
    response->set_success(result.ok);
    return ::grpc::Status::OK;
}

::grpc::Status tp_monitor_service_impl::Destroy(::grpc::ServerContext*,
                                                const DestroyRequest* request,
                                                DestroyResponse* response) {
    auto result = destroy_monitor(request->tpmid());
    response->set_success(result.ok);
    return ::grpc::Status::OK;
}

::grpc::Status tp_monitor_service_impl::Barrier(::grpc::ServerContext*,
                                                const BarrierRequest* request,
                                                BarrierResponse* response) {
    auto result = barrier_notify_monitor(request->tpmid(), request->txid());
    response->set_success(result.ok);
    return ::grpc::Status::OK;
}

std::shared_ptr<tp_monitor_service_impl::monitor_state> tp_monitor_service_impl::find_state(
        std::uint64_t tpm_id) {
    std::lock_guard<std::mutex> lock(mtx_);
    auto iter = monitors_.find(tpm_id);
    if (iter == monitors_.end()) {
        return {};
    }
    return iter->second;
}

tp_monitor_service_impl::create_result tp_monitor_service_impl::create_monitor(
        std::uint32_t participant_count) {
    if (participant_count == 0U) {
        LOG_LP(WARNING) << "participant_count must be >= 1";
        return {false, 0U};
    }
    std::uint64_t tpm_id = next_tpm_id_.fetch_add(1U);
    if (tpm_id == 0U) {
        LOG_LP(FATAL) << "tpm_id overflow";
        std::abort();
    }
    auto state = std::make_shared<monitor_state>();
    state->tpm_id = tpm_id;
    state->participant_count = participant_count;
    {
        std::lock_guard<std::mutex> lock(mtx_);
        monitors_.emplace(tpm_id, state);
    }
    return {true, tpm_id};
}

tp_monitor_service_impl::result tp_monitor_service_impl::join_monitor(
        std::uint64_t tpm_id,
        std::string_view tx_id,
        std::uint64_t ts_id) {
    if (tx_id.empty()) {
        LOG_LP(WARNING) << "tx_id is empty. continuing with empty tx_id.";
    }
    auto state = find_state(tpm_id);
    if (! state) {
        LOG_LP(WARNING) << "tpm_id not found. ignored. tpm_id=" << tpm_id;
        return {false};
    }

    std::lock_guard<std::mutex> lock(state->mtx);
    if (state->destroyed) {
        LOG_LP(WARNING) << "tpm_id is destroyed. ignored. tpm_id=" << tpm_id;
        return {false};
    }
    if (state->participants.size() >= state->participant_count) {
        LOG_LP(WARNING) << "participants already full. ignored. tpm_id=" << tpm_id;
        return {false};
    }
    auto [_, inserted] = state->participants.emplace(std::string(tx_id), ts_id);
    if (!inserted) {
        LOG_LP(WARNING) << "duplicate tx_id detected. ignored. tx_id=" << tx_id;
        return {false};
    }
    return {true};
}

tp_monitor_service_impl::result tp_monitor_service_impl::barrier_notify_monitor(
        std::uint64_t tpm_id,
        std::string_view tx_id) {
    auto state = find_state(tpm_id);
    if (! state) {
        LOG_LP(WARNING) << "tpm_id not found. ignored. tpm_id=" << tpm_id;
        return {false};
    }

    std::unique_lock<std::mutex> lock(state->mtx);
    if (state->destroyed) {
        LOG_LP(WARNING) << "tpm_id is destroyed. ignored. tpm_id=" << tpm_id;
        return {false};
    }
    auto tx_id_key = std::string(tx_id);
    if (state->participants.find(tx_id_key) == state->participants.end()) {
        LOG_LP(WARNING) << "tx_id not registered. ignored. tx_id=" << tx_id;
        return {false};
    }

    state->arrived.insert(std::move(tx_id_key));
    if (state->arrived.size() >= state->participant_count) {
        state->cv.notify_all();
        return {true};
    }

    state->cv.wait(lock, [&state]() {
        return state->destroyed || state->arrived.size() >= state->participant_count;
    });

    if (state->destroyed) {
        LOG_LP(WARNING) << "tpm_id is destroyed while waiting. ignored. tpm_id=" << tpm_id;
        return {false};
    }

    return {true};
}

tp_monitor_service_impl::result tp_monitor_service_impl::destroy_monitor(std::uint64_t tpm_id) {
    auto state = find_state(tpm_id);
    if (! state) {
        LOG_LP(WARNING) << "tpm_id not found. ignored. tpm_id=" << tpm_id;
        return {false};
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

    return {true};
}

} // namespace limestone::grpc::service
