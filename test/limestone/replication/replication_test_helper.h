#pragma once

#include <cstdint>
#include <vector>
#include <netinet/in.h>
#include "log_entry.h"
#include <gtest/gtest.h>
#include <limestone/api/storage_id_type.h>
#include <limestone/api/write_version_type.h>
#include <limestone/api/epoch_id_type.h>
#include <limestone/api/blob_id_type.h>


namespace limestone::testing {

using limestone::api::log_entry;
using limestone::api::storage_id_type;
using limestone::api::epoch_id_type;
using limestone::api::write_version_type;
using limestone::api::blob_id_type;    

uint16_t get_free_port();
int start_test_server(uint16_t port, bool echo_message, bool close_immediately = false);
sockaddr_in make_listen_addr(uint16_t port);

// TODO: 以下の関数と同内容の関数が、 compaction_test_fixture.(h|cpp) にもあるので、
// 共通化する。
std::vector<log_entry> read_log_file(boost::filesystem::path log_path);
std::vector<log_entry> read_log_file(boost::filesystem::path dir_path, const std::string& filename);
epoch_id_type get_epoch(boost::filesystem::path location);
void print_log_entry(const log_entry& entry);
::testing::AssertionResult AssertLogEntry(const log_entry& entry, const std::optional<storage_id_type>& expected_storage_id, const std::optional<std::string>& expected_key,
    const std::optional<std::string>& expected_value, const std::optional<epoch_id_type>& expected_epoch_number,
    const std::optional<std::uint64_t>& expected_minor_version, const std::vector<blob_id_type>& expected_blob_ids,
    log_entry::entry_type expected_type);
   
}