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
#pragma once

#include <cstdint>
#include <fstream>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <boost/filesystem/path.hpp>

#include <file_operations.h>
#include <wal_sync/wal_sync_client.h>
#include <backup.pb.h>

namespace limestone::internal {


/**
 * @brief Processes streamed backup object chunks and writes them to disk.
 */
class response_chunk_processor {
public:
    /**
     * @brief Construct a response_chunk_processor.
     * @param file_ops file operation interface
     * @param output_dir base directory where files are written
     * @param objects list of backup objects obtained from begin_backup()
     */
    response_chunk_processor(
        file_operations& file_ops,
        boost::filesystem::path output_dir,
        std::vector<backup_object> const& objects
    );
    ~response_chunk_processor() = default;
    response_chunk_processor(response_chunk_processor const&) = delete;
    response_chunk_processor& operator=(response_chunk_processor const&) = delete;
    response_chunk_processor(response_chunk_processor&&) = delete;
    response_chunk_processor& operator=(response_chunk_processor&&) = delete;

    /**
     * @brief Process a GetObjectResponse chunk.
     * @param response response message from the server
     */
    void handle_response(limestone::grpc::proto::GetObjectResponse const& response);

    /**
     * @brief Determine whether an error has occurred.
     * @return true if an error was detected
     */
    [[nodiscard]] bool failed() const noexcept;

    /**
     * @brief Get the error message when failed().
     * @return error message string
     */
    [[nodiscard]] std::string const& error_message() const noexcept;

    /**
     * @brief Remove any partially written output files.
     */
    void cleanup_partials();

    /**
     * @brief Check whether all objects have been copied successfully.
     * @return true if every object completed
     */
    [[nodiscard]] bool all_completed() const noexcept;

    /**
     * @brief Retrieve object IDs that did not complete successfully.
     * @return vector of incomplete object IDs
     */
    [[nodiscard]] std::vector<std::string> incomplete_object_ids() const;

private:
    struct transfer_state {
        std::unique_ptr<std::ofstream> stream{};
        std::uint64_t expected_total_size = 0;
        std::uint64_t received_bytes = 0;
        bool saw_first_chunk = false;
        bool completed = false;
        boost::filesystem::path final_path;
    };

    void set_failure(std::string message, transfer_state* state);
    void cleanup_state(transfer_state& state);

public:
    struct transfer_state_snapshot {
        std::string object_id;
        std::uint64_t expected_total_size = 0;
        std::uint64_t received_bytes = 0;
        bool saw_first_chunk = false;
        bool completed = false;
        boost::filesystem::path final_path;
    };

    [[nodiscard]] std::vector<transfer_state_snapshot> snapshot_states() const;

private:
    bool ensure_metadata_and_path(
        limestone::grpc::proto::GetObjectResponse const& response,
        boost::filesystem::path& rel_path
    );
    transfer_state* find_or_create_state(
        limestone::grpc::proto::BackupObject const& object_proto,
        boost::filesystem::path const& rel_path,
        bool is_first
    );
    bool prepare_first_chunk_if_needed(
        transfer_state& state,
        limestone::grpc::proto::GetObjectResponse const& response,
        boost::filesystem::path const& rel_path
    );
    bool validate_stream_and_offset(
        transfer_state& state,
        limestone::grpc::proto::GetObjectResponse const& response
    );
    bool write_chunk(
        transfer_state& state,
        limestone::grpc::proto::GetObjectResponse const& response
    );
    bool finalize_if_last(
        transfer_state& state,
        limestone::grpc::proto::GetObjectResponse const& response
    );
    bool validate_relative_path(
        std::string const& object_id,
        boost::filesystem::path const& rel_path,
        transfer_state* state
    );

    file_operations& file_ops_;
    boost::filesystem::path base_dir_;
    std::unordered_map<std::string, transfer_state> states_;
    bool failed_ = false;
    std::string error_message_;
};


} // namespace limestone::internal
