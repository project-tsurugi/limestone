
#include <algorithm>
#include <sstream>
#include <limestone/logging.h>

#include <boost/filesystem.hpp>

#include "dblog_scan.h"
#include "internal.h"
#include "log_entry.h"

#include "test_root.h"

namespace limestone::testing {

using namespace std::literals;

extern constexpr const std::string_view epoch_0_str = "\x04\x00\x00\x00\x00\x00\x00\x00\x00"sv;
static_assert(epoch_0_str.length() == 9);
extern constexpr const std::string_view epoch_0x100_str = "\x04\x00\x01\x00\x00\x00\x00\x00\x00"sv;
static_assert(epoch_0x100_str.length() == 9);

extern constexpr const std::string_view data_normal =
    "\x02\xff\x00\x00\x00\x00\x00\x00\x00"  // marker_begin 0xff
    // XXX: epoch footer...
    "\x02\x00\x01\x00\x00\x00\x00\x00\x00"  // marker_begin 0x100
    // XXX: epoch footer...
    ""sv;

extern constexpr const std::string_view data_normal2 =
    "\x02\xf0\x00\x00\x00\x00\x00\x00\x00"  // marker_begin 0xf0
    "\x01\x04\x00\x00\x00\x04\x00\x00\x00" "storage1" "1234" "vermajor" "verminor" "1234"  // normal_entry
    // XXX: epoch footer...
    "\x02\xf1\x00\x00\x00\x00\x00\x00\x00"  // marker_begin 0xf1
    "\x01\x04\x00\x00\x00\x04\x00\x00\x00" "storage1" "1235" "vermajor" "verminor" "1235"  // normal_entry
    // XXX: epoch footer...
    "\x02\x00\x01\x00\x00\x00\x00\x00\x00"  // marker_begin 0x100
    "\x01\x04\x00\x00\x00\x04\x00\x00\x00" "storage1" "1236" "vermajor" "verminor" "1236"  // normal_entry
    // XXX: epoch footer...
    ""sv;

extern constexpr const std::string_view data_nondurable =
    "\x02\xff\x00\x00\x00\x00\x00\x00\x00"  // marker_begin 0xff
    // XXX: epoch footer...
    "\x02\x01\x01\x00\x00\x00\x00\x00\x00"  // marker_begin 0x101 (nondurable)
    // XXX: epoch footer...
    ""sv;

extern constexpr const std::string_view data_repaired_nondurable =
    "\x02\xf0\x00\x00\x00\x00\x00\x00\x00"  // marker_begin 0xf0
    "\x01\x04\x00\x00\x00\x04\x00\x00\x00" "storage1" "1234" "vermajor" "verminor" "1234"  // normal_entry
    // XXX: epoch footer...
    "\x06\xf1\x00\x00\x00\x00\x00\x00\x00"  // marker_invalidated_begin 0xf1
    "\x01\x04\x00\x00\x00\x04\x00\x00\x00" "storage1" "1235" "vermajor" "verminor" "1235"  // normal_entry
    // XXX: epoch footer...
    "\x02\x00\x01\x00\x00\x00\x00\x00\x00"  // marker_begin 0x100
    "\x01\x04\x00\x00\x00\x04\x00\x00\x00" "storage1" "1236" "vermajor" "verminor" "1236"  // normal_entry
    // XXX: epoch footer...
    ""sv;

extern constexpr const std::string_view data_zerofill =
    "\x02\xff\x00\x00\x00\x00\x00\x00\x00"  // marker_begin 0xff
    // XXX: epoch footer...
    "\x02\x01\x01\x00\x00\x00\x00\x00\x00"  // marker_begin 0x101 (nondurable)
    // XXX: epoch footer...
    "\x00\x00\x00\x00\x00\x00\x00\x00\x00"  // UNKNOWN_TYPE_entry
    ""sv;

extern constexpr const std::string_view data_truncated_normal_entry =
    "\x02\xff\x00\x00\x00\x00\x00\x00\x00"  // marker_begin 0xff
    // XXX: epoch footer...
    "\x02\x01\x01\x00\x00\x00\x00\x00\x00"  // marker_begin 0x101 (nondurable)
    "\x01\x04\x00\x00\x00\x04\x00\x00\x00"  // SHORT_normal_entry
    ""sv;

extern constexpr const std::string_view data_truncated_epoch_header =
    "\x02\xff\x00\x00\x00\x00\x00\x00\x00"  // marker_begin 0xff
    "\x01\x04\x00\x00\x00\x04\x00\x00\x00" "storage1" "1234" "vermajor" "verminor" "1234"  // normal_entry
    // XXX: epoch footer...
    // offset 50
    "\x02\x01\x01\x00\x00\x00\x00\x00"  // SHORT_marker_begin
    ""sv;
static_assert(data_truncated_epoch_header.at(50) == '\x02');

extern constexpr const std::string_view data_truncated_invalidated_normal_entry =
    "\x02\xff\x00\x00\x00\x00\x00\x00\x00"  // marker_begin 0xff
    // XXX: epoch footer...
    "\x06\x01\x01\x00\x00\x00\x00\x00\x00"  // marker_invalidated_begin 0x101
    "\x01\x04\x00\x00\x00\x04\x00\x00\x00"  // SHORT_normal_entry
    ""sv;

extern constexpr const std::string_view data_truncated_invalidated_epoch_header =
    "\x02\xff\x00\x00\x00\x00\x00\x00\x00"  // marker_begin 0xff
    "\x01\x04\x00\x00\x00\x04\x00\x00\x00" "storage1" "1234" "vermajor" "verminor" "1234"  // normal_entry
    // XXX: epoch footer...
    // offset 50
    "\x06\x01\x01\x00\x00\x00\x00\x00"  // SHORT_marker_inv_begin
    ""sv;
static_assert(data_truncated_invalidated_epoch_header.at(50) == '\x06');

extern constexpr const std::string_view data_allzero =
    "\x00\x00\x00\x00\x00\x00\x00\x00\x00"  // UNKNOWN_TYPE_entry
    ""sv;

std::string data_manifest(int persistent_format_version = 1) {
    std::ostringstream ss;
    ss << "{ \"format_version\": \"1.0\", \"persistent_format_version\": " << persistent_format_version << " }";
    return ss.str();
}

void create_file(const boost::filesystem::path& path, std::string_view content) {
    boost::filesystem::ofstream strm{};
    strm.open(path, std::ios_base::out | std::ios_base::binary);
    strm.write(content.data(), content.size());
    strm.flush();
    LOG_IF(FATAL, !strm || !strm.is_open() || strm.bad() || strm.fail());
    strm.close();
}

std::string read_entire_file(const boost::filesystem::path& path) {
    boost::filesystem::ofstream strm{};
    strm.open(path, std::ios_base::in | std::ios_base::binary);
    assert(strm.good());
    std::ostringstream ss;
    ss << strm.rdbuf();
    strm.close();
    return ss.str();
}

}
