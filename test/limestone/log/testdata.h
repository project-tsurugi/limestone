#pragma once

#include <string_view>

namespace limestone::testing {

using namespace std::literals;

// Binary test data definitions (C++17: inline constexpr)
inline constexpr const std::string_view epoch_0_str = "\x04\x00\x00\x00\x00\x00\x00\x00\x00"sv;
inline constexpr const std::string_view epoch_0x100_str = "\x04\x00\x01\x00\x00\x00\x00\x00\x00"sv;
inline constexpr const std::string_view epoch_0xff_str = "\x04\xff\x00\x00\x00\x00\x00\x00\x00"sv;
inline constexpr const std::string_view data_normal =
    "\x02\xff\x00\x00\x00\x00\x00\x00\x00"  // marker_begin 0xff
    // XXX: epoch footer...
    "\x02\x00\x01\x00\x00\x00\x00\x00\x00"  // marker_begin 0x100
    // XXX: epoch footer...
    ""sv;
inline constexpr const std::string_view data_normal2 =
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
inline constexpr const std::string_view data_nondurable =
    "\x02\xff\x00\x00\x00\x00\x00\x00\x00"  // marker_begin 0xff
    // XXX: epoch footer...
    "\x02\x01\x01\x00\x00\x00\x00\x00\x00"  // marker_begin 0x101 (nondurable)
    // XXX: epoch footer...
    ""sv;
inline constexpr const std::string_view data_repaired_nondurable =
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
inline constexpr const std::string_view data_zerofill =
    "\x02\xff\x00\x00\x00\x00\x00\x00\x00"  // marker_begin 0xff
    // XXX: epoch footer...
    "\x02\x01\x01\x00\x00\x00\x00\x00\x00"  // marker_begin 0x101 (nondurable)
    // XXX: epoch footer...
    "\x00\x00\x00\x00\x00\x00\x00\x00\x00"  // UNKNOWN_TYPE_entry
    ""sv;
inline constexpr const std::string_view data_truncated_normal_entry =
    "\x02\xff\x00\x00\x00\x00\x00\x00\x00"  // marker_begin 0xff
    // XXX: epoch footer...
    "\x02\x01\x01\x00\x00\x00\x00\x00\x00"  // marker_begin 0x101 (nondurable)
    "\x01\x04\x00\x00\x00\x04\x00\x00\x00"  // SHORT_normal_entry
    ""sv;
inline constexpr const std::string_view data_truncated_epoch_header =
    "\x02\xff\x00\x00\x00\x00\x00\x00\x00"  // marker_begin 0xff
    "\x01\x04\x00\x00\x00\x04\x00\x00\x00" "storage1" "1234" "vermajor" "verminor" "1234"  // normal_entry
    // XXX: epoch footer...
    // offset 50
    "\x02\x01\x01\x00\x00\x00\x00\x00"  // SHORT_marker_begin
    ""sv;
inline constexpr const std::string_view data_truncated_invalidated_normal_entry =
    "\x02\xff\x00\x00\x00\x00\x00\x00\x00"  // marker_begin 0xff
    // XXX: epoch footer...
    "\x06\x01\x01\x00\x00\x00\x00\x00\x00"  // marker_invalidated_begin 0x101
    "\x01\x04\x00\x00\x00\x04\x00\x00\x00"  // SHORT_normal_entry
    ""sv;
inline constexpr const std::string_view data_truncated_invalidated_epoch_header =
    "\x02\xff\x00\x00\x00\x00\x00\x00\x00"  // marker_begin 0xff
    "\x01\x04\x00\x00\x00\x04\x00\x00\x00" "storage1" "1234" "vermajor" "verminor" "1234"  // normal_entry
    // XXX: epoch footer...
    // offset 50
    "\x06\x01\x01\x00\x00\x00\x00\x00"  // SHORT_marker_inv_begin
    ""sv;
inline constexpr const std::string_view data_allzero =
    "\x00\x00\x00\x00\x00\x00\x00\x00\x00"  // UNKNOWN_TYPE_entry
    ""sv;
inline constexpr const std::string_view data_marker_end_only =
    "\x02\x00\x01\x00\x00\x00\x00\x00\x00"  // marker_begin epoch 0x100
    "\x01\x04\x00\x00\x00\x04\x00\x00\x00" "storage1" "1234" "vermajor" "verminor" "1234"  // normal_entry
    "\x03\x00\x01\x00\x00\x00\x00\x00\x00" // marker_end epoch 0x100
    ""sv;
inline constexpr const std::string_view data_marker_end_followed_by_normal_entry =
    "\x02\x00\x01\x00\x00\x00\x00\x00\x00"  // marker_begin epoch 0x100
    "\x01\x04\x00\x00\x00\x04\x00\x00\x00" "storage1" "1234" "vermajor" "verminor" "1234"  // normal_entry
    "\x03\x00\x01\x00\x00\x00\x00\x00\x00"  // marker_end epoch 0x100
    "\x01\x20\x00\x00\x00" "storage1" "1234" "vermajor" "verminor" "1234"  // normal_entry again
    ""sv;
inline constexpr const std::string_view data_marker_end_followed_by_marker_begin =
    "\x02\x00\x01\x00\x00\x00\x00\x00\x00"  // marker_begin epoch 0x100
    "\x01\x04\x00\x00\x00\x04\x00\x00\x00" "storage1" "1234" "vermajor" "verminor" "1234"  // normal_entry
    "\x03\x00\x01\x00\x00\x00\x00\x00\x00"  // marker_end epoch 0x100
    "\x02\x01\x01\x00\x00\x00\x00\x00\x00"  // marker_begin epoch 0x101
    "\x01\x04\x00\x00\x00\x04\x00\x00\x00" "storage1" "1234" "vermajor" "verminor" "1234"  // normal_entry
    "\x03\x01\x01\x00\x00\x00\x00\x00\x00" // marker_end epoch 0x101
    ""sv;
inline constexpr const std::string_view data_marker_end_followed_by_marker_inv_begin =
    "\x02\x00\x01\x00\x00\x00\x00\x00\x00"  // marker_begin epoch 0x100
    "\x01\x04\x00\x00\x00\x04\x00\x00\x00" "storage1" "1234" "vermajor" "verminor" "1234"  // normal_entry
    "\x03\x00\x01\x00\x00\x00\x00\x00\x00"  // marker_end epoch 0x100
    "\x06\x01\x01\x00\x00\x00\x00\x00\x00"  // marker_inv_begin epoch 0x101
    "\x01\x04\x00\x00\x00\x04\x00\x00\x00" "storage1" "1234" "vermajor" "verminor" "1234"  // normal_entry in invalidated snippet
    "\x03\x01\x01\x00\x00\x00\x00\x00\x00" // marker_end epoch 0x101
    ""sv;
inline constexpr const std::string_view data_marker_end_followed_by_short_entry =
    "\x02\x00\x01\x00\x00\x00\x00\x00\x00"  // marker_begin epoch 0x100
    "\x01\x04\x00\x00\x00\x04\x00\x00\x00" "storage1" "1234" "vermajor" "verminor" "1234"  // normal_entry
    "\x03\x00\x01\x00\x00\x00\x00\x00\x00"  // marker_end epoch 0x100
    "\x01\x20\x00\x00"                     // SHORT normal_entry (incomplete)
    ""sv;
inline constexpr const std::string_view data_short_marker_end_only =
    "\x02\x00\x01\x00\x00\x00\x00\x00\x00"  // marker_begin epoch 0x100
    "\x01\x04\x00\x00\x00\x04\x00\x00\x00" "storage1" "1234" "vermajor" "verminor" "1234"  // normal_entry
    "\x03\x00\x01\x00\x00\x00\x00"         // SHORT_marker_end (incomplete)
    ""sv;
inline constexpr std::string_view data_all_zerofill =
    "\x00\x00\x00\x00\x00\x00\x00\x00\x00"
    ""sv;
inline constexpr std::string_view data_marker_begin_partial_zerofill =
    "\x02\x00\x01\x00\x00\x00\x00"  // SHORT marker_begin (7 bytes)
    "\x00"                      // 0fill
    ""sv;
inline constexpr std::string_view data_marker_begin_followed_by_zerofill =
    "\x02\x00\x01\x00\x00\x00\x00\x00\x00"  // normal marker_begin
    "\x00\x00\x00\x00\x00\x00\x00\x00\x00"  // 0fill
    ""sv;
inline constexpr std::string_view data_marker_begin_normal_entry_partial_zerofill =
    "\x02\x00\x01\x00\x00\x00\x00\x00\x00"  // normal marker_begin
    "\x01\x04\x00\x00\x00\x04\x00"          // SHORT normal_entry (incomplete)
    "\x00\x00"                              // 0fill
    ""sv;
inline constexpr std::string_view data_marker_begin_normal_entry_followed_by_zerofill =
    "\x02\x00\x01\x00\x00\x00\x00\x00\x00"  // normal marker_begin
    "\x01\x04\x00\x00\x00\x04\x00\x00\x00" "storage1" "1234" "vermajor" "verminor" "1234" // normal_entry
    "\x00\x00\x00\x00\x00\x00\x00\x00\x00"  // 0fill
    ""sv;
inline constexpr std::string_view data_marker_end_partial_zerofill =
    "\x02\x00\x01\x00\x00\x00\x00\x00\x00"  // normal marker_begin
    "\x01\x04\x00\x00\x00\x04\x00\x00\x00" "storage1" "1234" "vermajor" "verminor" "1234" // normal_entry
    "\x03\x01\x01\x00\x00"                  // SHORT marker_end (5 bytes)
    "\x00"                      // 0fill
    ""sv;
inline constexpr std::string_view valid_snippet =
    "\x02\xff\x00\x00\x00\x00\x00\x00\x00"   // marker_begin (epoch = 0x100)
    "\x01\x04\x00\x00\x00\x04\x00\x00\x00" "storage1" "1234" "vermajor" "verminor" "1234"
    "\x03\x00\x01\x00\x00\x00\x00\x00\x00"  // marker_end
    ""sv;

// Debug utilities
void hexdump(std::string_view data, const std::string& name = "");

// File helpers
void create_file(const boost::filesystem::path& path, std::string_view content);
std::string read_entire_file(const boost::filesystem::path& path);

std::string data_manifest(int persistent_format_version = 1);

} // namespace limestone::testing
