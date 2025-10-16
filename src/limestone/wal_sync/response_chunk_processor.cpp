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

#include <wal_sync/response_chunk_processor.h>

#include <algorithm>
#include <utility>

#include <boost/system/error_code.hpp>

namespace limestone::internal {

response_chunk_processor::response_chunk_processor(
    file_operations& file_ops,
    boost::filesystem::path output_dir,
    std::vector<backup_object> const& objects
)
    : file_ops_(file_ops)
    , base_dir_(std::move(output_dir)) {
    states_.reserve(objects.size());
    for (auto const& object : objects) {
        transfer_state state{};
        state.final_path = base_dir_ / boost::filesystem::path(object.path);
        states_.emplace(object.id, std::move(state));
    }
}

void response_chunk_processor::handle_response(limestone::grpc::proto::GetObjectResponse const& response) {
    if (failed_) {
        return;
    }

    boost::filesystem::path object_rel_path;
    if (!ensure_metadata_and_path(response, object_rel_path)) {
        return;
    }

    auto const& object_proto = response.object();
    auto* state = find_or_create_state(object_proto, object_rel_path, response.is_first());
    if (state == nullptr) {
        return;
    }

    if (!prepare_first_chunk_if_needed(*state, response, object_rel_path)) {
        return;
    }

    if (!validate_stream_and_offset(*state, response)) {
        return;
    }

    if (!write_chunk(*state, response)) {
        return;
    }

    if (!finalize_if_last(*state, response)) {
        return;
    }
}

bool response_chunk_processor::ensure_metadata_and_path(
    limestone::grpc::proto::GetObjectResponse const& response,
    boost::filesystem::path& rel_path
) {
    if (!response.has_object()) {
        set_failure("received response without object metadata", nullptr);
        return false;
    }
    auto const& object_proto = response.object();
    rel_path = boost::filesystem::path(object_proto.path());
    return validate_relative_path(object_proto.object_id(), rel_path, nullptr);
}

response_chunk_processor::transfer_state* response_chunk_processor::find_or_create_state(
    limestone::grpc::proto::BackupObject const& object_proto,
    boost::filesystem::path const& rel_path,
    bool is_first
) {
    auto it = states_.find(object_proto.object_id());
    if (it == states_.end()) {
        if (!is_first) {
            set_failure("received chunk before first for unknown object_id: " + object_proto.object_id(), nullptr);
            return nullptr;
        }
        transfer_state state{};
        state.final_path = base_dir_ / rel_path;
        auto insert_result = states_.emplace(object_proto.object_id(), std::move(state));
        it = insert_result.first;
    }
    return &it->second;
}

bool response_chunk_processor::prepare_first_chunk_if_needed(
    transfer_state& state,
    limestone::grpc::proto::GetObjectResponse const& response,
    boost::filesystem::path const& rel_path
) {
    auto const& object_proto = response.object();

    if (state.completed) {
        set_failure("received chunk after completion for object_id: " + object_proto.object_id(), &state);
        return false;
    }

    if (!state.saw_first_chunk) {
        if (!response.is_first()) {
            set_failure("first chunk must be marked as is_first for object_id: " + object_proto.object_id(), &state);
            return false;
        }
        boost::filesystem::path expected_path = base_dir_ / rel_path;
        if (state.final_path != expected_path) {
            set_failure("object path mismatch for object_id: " + object_proto.object_id(), &state);
            return false;
        }
        state.expected_total_size = response.total_size();
        auto parent = state.final_path.parent_path();
        if (!parent.empty()) {
            boost::system::error_code ec;
            file_ops_.create_directories(parent, ec);
            if (ec) {
                set_failure("failed to create parent directory: " + parent.string() + ", ec=" + ec.message(), &state);
                return false;
            }
        }
        state.stream = file_ops_.open_ofstream(state.final_path.string());
        if (!state.stream || !state.stream->is_open()) {
            set_failure("failed to open output file: " + state.final_path.string(), &state);
            return false;
        }
        state.saw_first_chunk = true;
    } else if (response.is_first()) {
        set_failure("received duplicate first chunk for object_id: " + object_proto.object_id(), &state);
        return false;
    }
    return true;
}

bool response_chunk_processor::validate_stream_and_offset(
    transfer_state& state,
    limestone::grpc::proto::GetObjectResponse const& response
) {
    auto const& object_proto = response.object();

    if (!state.stream || !state.stream->is_open()) {
        set_failure("output stream missing for object_id: " + object_proto.object_id(), &state);
        return false;
    }

    auto expected_offset = state.received_bytes;
    if (response.offset() != expected_offset) {
        set_failure(
            "unexpected offset for object_id: " + object_proto.object_id() +
            ", expected=" + std::to_string(expected_offset) +
            ", actual=" + std::to_string(response.offset()),
            &state
        );
        return false;
    }
    return true;
}

bool response_chunk_processor::write_chunk(
    transfer_state& state,
    limestone::grpc::proto::GetObjectResponse const& response
) {
    auto const& object_proto = response.object();
    auto const& chunk = response.chunk();
    if (chunk.empty()) {
        return true;
    }

    file_ops_.ofs_write(*state.stream, chunk.data(), static_cast<std::streamsize>(chunk.size()));
    if (!state.stream->good()) {
        set_failure("failed to write chunk for object_id: " + object_proto.object_id(), &state);
        return false;
    }
    state.received_bytes += static_cast<std::uint64_t>(chunk.size());
    return true;
}

bool response_chunk_processor::finalize_if_last(
    transfer_state& state,
    limestone::grpc::proto::GetObjectResponse const& response
) {
    if (!response.is_last()) {
        return true;
    }
    auto const& object_proto = response.object();

    if (state.expected_total_size != 0 && state.received_bytes != state.expected_total_size) {
        set_failure(
            "size mismatch for object_id: " + object_proto.object_id() +
            ", expected=" + std::to_string(state.expected_total_size) +
            ", actual=" + std::to_string(state.received_bytes),
            &state
        );
        return false;
    }
    file_ops_.ofs_flush(*state.stream);
    if (!state.stream->good()) {
        set_failure("failed to flush stream for object_id: " + object_proto.object_id(), &state);
        return false;
    }
    file_ops_.ofs_close(*state.stream);
    if (state.stream->fail()) {
        set_failure("failed to close stream for object_id: " + object_proto.object_id(), &state);
        return false;
    }
    state.stream.reset();
    state.completed = true;
    return true;
}

bool response_chunk_processor::validate_relative_path(
    std::string const& object_id,
    boost::filesystem::path const& rel_path,
    transfer_state* state
) {
    if (rel_path.empty()) {
        set_failure("object path is empty for object_id: " + object_id, state);
        return false;
    }
    if (rel_path.is_absolute()) {
        set_failure("object path must be relative for object_id: " + object_id, state);
        return false;
    }
    auto contains_dotdot = std::any_of(
        rel_path.begin(),
        rel_path.end(),
        [](boost::filesystem::path const& component) {
            return component == "..";
        }
    );
    if (contains_dotdot) {
        set_failure("object path must not contain '..' for object_id: " + object_id, state);
        return false;
    }
    return true;
}

bool response_chunk_processor::failed() const noexcept {
    return failed_;
}

std::string const& response_chunk_processor::error_message() const noexcept {
    return error_message_;
}

void response_chunk_processor::cleanup_partials() {
    for (auto& entry : states_) {
        cleanup_state(entry.second);
    }
}

bool response_chunk_processor::all_completed() const noexcept {
    return std::all_of(
        states_.cbegin(),
        states_.cend(),
        [](auto const& entry) {
            return entry.second.completed;
        }
    );
}

std::vector<std::string> response_chunk_processor::incomplete_object_ids() const {
    std::vector<std::string> result;
    for (auto const& entry : states_) {
        if (!entry.second.completed) {
            result.emplace_back(entry.first);
        }
    }
    return result;
}

void response_chunk_processor::set_failure(std::string message, transfer_state* state) {
    if (!failed_) {
        failed_ = true;
        error_message_ = std::move(message);
    }
    if (state != nullptr) {
        cleanup_state(*state);
    }
}

void response_chunk_processor::cleanup_state(transfer_state& state) {
    if (state.stream && state.stream->is_open()) {
        state.stream->close();
    }
    state.stream.reset();
    if (!state.completed) {
        boost::system::error_code remove_ec;
        file_ops_.remove(state.final_path, remove_ec);
    }
}

std::vector<response_chunk_processor::transfer_state_snapshot> response_chunk_processor::snapshot_states() const {
    std::vector<transfer_state_snapshot> snapshots;
    snapshots.reserve(states_.size());
    for (auto const& entry : states_) {
        auto const& state = entry.second;
        snapshots.emplace_back(transfer_state_snapshot{
            entry.first,
            state.expected_total_size,
            state.received_bytes,
            state.saw_first_chunk,
            state.completed,
            state.final_path
        });
    }
    return snapshots;
}

} // namespace limestone::internal::wal_sync_detail
