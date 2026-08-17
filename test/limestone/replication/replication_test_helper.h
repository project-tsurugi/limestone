#pragma once

#include <cstdint>
#include <optional>
#include <ostream>
#include <string>
#include <vector>
#include <netinet/in.h>
#include <boost/filesystem/path.hpp>
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

// A session marker (marker_begin/marker_end) read from a pwal file.
// read_log_file() cannot observe markers: its dblog_scan consumes them
// internally as scan context and never hands them to the entry callback.
struct session_marker {
    log_entry::entry_type type{};
    epoch_id_type epoch{};

    friend bool operator==(session_marker const& a, session_marker const& b) {
        return a.type == b.type && a.epoch == b.epoch;
    }
    friend std::ostream& operator<<(std::ostream& os, session_marker const& m) {
        return os << (m.type == log_entry::entry_type::marker_begin ? "begin"
                      : m.type == log_entry::entry_type::marker_end ? "end"
                      : "type#" + std::to_string(static_cast<int>(m.type)))
                  << "(" << m.epoch << ")";
    }
};

// Every log entry in the file in file order, session markers included.
std::vector<log_entry> read_raw_log_file(boost::filesystem::path const& log_path);

// The (type, epoch) sequence of the session markers in the file, in file order.
std::vector<session_marker> read_session_markers(boost::filesystem::path const& log_path);

// EXPECTs that the two files hold exactly the same bytes; on mismatch, reports
// the sizes and the offset of the first differing byte.
void expect_files_byte_identical(boost::filesystem::path const& expected_path, boost::filesystem::path const& actual_path);

}
