#include "rdma_log_entries_parser.h"

#include <arpa/inet.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstring>
#include <iterator>
#include <stdexcept>

#include <boost/filesystem.hpp>

#include "limestone_exception_helper.h"

namespace limestone::replication {

rdma_log_entries_parser::rdma_log_entries_parser(limestone::api::datastore& datastore) noexcept
    : datastore_(&datastore) {}

rdma_log_entries_parser::~rdma_log_entries_parser() {
    close_current_blob_file_noexcept();
}

// Keep this state machine in one switch so the wire-field order and transitions
// can be read top-to-bottom. Splitting each state into small functions lowers
// the metric but makes the parser flow harder to audit.
// NOLINTNEXTLINE(readability-function-cognitive-complexity)
std::size_t rdma_log_entries_parser::consume(std::string_view bytes) {
    std::size_t offset = 0;
    while (offset < bytes.size()) {
        switch (state_) {
            case parse_state::epoch_id: {
                std::uint64_t epoch_id = 0;
                if (!read_uint64(bytes, offset, epoch_id)) {
                    return offset;
                }
                message_ = std::make_unique<message_log_entries>(epoch_id_type{epoch_id});
                state_ = parse_state::entry_count;
                break;
            }
            case parse_state::entry_count:
                if (!read_uint32(bytes, offset, entries_remaining_)) {
                    return offset;
                }
                state_ = entries_remaining_ == 0 ? parse_state::operation_flags : parse_state::entry_type;
                break;
            case parse_state::entry_type: {
                std::uint8_t value = 0;
                if (!read_uint8(bytes, offset, value)) {
                    return offset;
                }
                current_entry_ = message_log_entries::entry{};
                current_entry_.type = static_cast<log_entry::entry_type>(value);
                state_ = parse_state::storage_id;
                break;
            }
            case parse_state::storage_id:
                if (!read_uint64(bytes, offset, current_entry_.storage_id)) {
                    return offset;
                }
                state_ = parse_state::key_length;
                break;
            case parse_state::key_length:
                if (!read_string_length(bytes, offset, parse_state::key_bytes)) {
                    return offset;
                }
                break;
            case parse_state::key_bytes:
                if (!read_string_bytes(bytes, offset, current_entry_.key, parse_state::value_length)) {
                    return offset;
                }
                break;
            case parse_state::value_length:
                if (!read_string_length(bytes, offset, parse_state::value_bytes)) {
                    return offset;
                }
                break;
            case parse_state::value_bytes:
                if (!read_string_bytes(bytes, offset, current_entry_.value, parse_state::write_version_major)) {
                    return offset;
                }
                break;
            case parse_state::write_version_major:
                if (!read_uint64(bytes, offset, write_version_major_)) {
                    return offset;
                }
                state_ = parse_state::write_version_minor;
                break;
            case parse_state::write_version_minor: {
                std::uint64_t minor = 0;
                if (!read_uint64(bytes, offset, minor)) {
                    return offset;
                }
                current_entry_.write_version = write_version_type{write_version_major_, minor};
                state_ = parse_state::blob_count;
                break;
            }
            case parse_state::blob_count:
                if (!read_uint32(bytes, offset, pending_blob_count_)) {
                    return offset;
                }
                if (pending_blob_count_ == 0) {
                    add_current_entry();
                    state_ = entries_remaining_ == 0 ? parse_state::operation_flags : parse_state::entry_type;
                } else if (datastore_ != nullptr) {
                    blobs_remaining_in_entry_ = pending_blob_count_;
                    state_ = parse_state::blob_id;
                } else {
                    state_ = parse_state::awaiting_blob;
                    return offset;
                }
                break;
            case parse_state::blob_id:
                if (!read_uint64(bytes, offset, current_blob_id_)) {
                    return offset;
                }
                current_entry_.blob_ids.push_back(current_blob_id_);
                state_ = parse_state::blob_size;
                break;
            case parse_state::blob_size:
                if (!read_uint32(bytes, offset, current_blob_remaining_)) {
                    return offset;
                }
                if (current_blob_remaining_ == 0) {
                    open_current_blob_file();
                    finish_current_blob_file();
                    if (--blobs_remaining_in_entry_ == 0) {
                        add_current_entry();
                        state_ = entries_remaining_ == 0 ? parse_state::operation_flags : parse_state::entry_type;
                    } else {
                        state_ = parse_state::blob_id;
                    }
                } else {
                    state_ = parse_state::blob_bytes;
                }
                break;
            case parse_state::blob_bytes:
                if (!read_blob_bytes(bytes, offset)) {
                    return offset;
                }
                finish_current_blob_file();
                if (--blobs_remaining_in_entry_ == 0) {
                    add_current_entry();
                    state_ = entries_remaining_ == 0 ? parse_state::operation_flags : parse_state::entry_type;
                } else {
                    state_ = parse_state::blob_id;
                }
                break;
            case parse_state::operation_flags: {
                std::uint8_t flags = 0;
                if (!read_uint8(bytes, offset, flags)) {
                    return offset;
                }
                apply_operation_flags(flags);
                state_ = parse_state::complete;
                return offset;
            }
            case parse_state::complete:
            case parse_state::awaiting_blob:
                return offset;
        }
    }
    return offset;
}

rdma_log_entries_parser::status rdma_log_entries_parser::get_status() const noexcept {
    if (state_ == parse_state::complete) {
        return status::complete;
    }
    if (state_ == parse_state::awaiting_blob) {
        return status::awaiting_blob;
    }
    return status::reading;
}

bool rdma_log_entries_parser::complete() const noexcept {
    return state_ == parse_state::complete;
}

bool rdma_log_entries_parser::awaiting_blob() const noexcept {
    return state_ == parse_state::awaiting_blob;
}

std::uint32_t rdma_log_entries_parser::pending_blob_count() const noexcept {
    return pending_blob_count_;
}

std::uint32_t rdma_log_entries_parser::entries_remaining() const noexcept {
    return entries_remaining_;
}

std::unique_ptr<message_log_entries> rdma_log_entries_parser::take_message() {
    if (!complete()) {
        throw std::logic_error("RDMA LOG_ENTRY parser message is not complete");
    }
    state_ = parse_state::epoch_id;
    entries_remaining_ = 0;
    pending_blob_count_ = 0;
    current_blob_id_ = 0;
    current_blob_remaining_ = 0;
    blobs_remaining_in_entry_ = 0;
    string_length_ = 0;
    string_buffer_.clear();
    scalar_buffer_.clear();
    current_entry_ = message_log_entries::entry{};
    return std::move(message_);
}

bool rdma_log_entries_parser::read_bytes(std::string_view bytes, std::size_t& offset, std::size_t size) {
    std::size_t needed = size - scalar_buffer_.size();
    std::size_t available = bytes.size() - offset;
    std::size_t take = std::min(needed, available);
    scalar_buffer_.append(bytes.substr(offset, take));
    offset += take;
    return scalar_buffer_.size() == size;
}

bool rdma_log_entries_parser::read_uint8(std::string_view bytes, std::size_t& offset, std::uint8_t& value) {
    if (!read_bytes(bytes, offset, sizeof(value))) {
        return false;
    }
    std::memcpy(&value, scalar_buffer_.data(), sizeof(value));
    scalar_buffer_.clear();
    return true;
}

bool rdma_log_entries_parser::read_uint32(std::string_view bytes, std::size_t& offset, std::uint32_t& value) {
    std::uint32_t net_value = 0;
    if (!read_bytes(bytes, offset, sizeof(net_value))) {
        return false;
    }
    std::memcpy(&net_value, scalar_buffer_.data(), sizeof(net_value));
    scalar_buffer_.clear();
    value = ntohl(net_value);
    return true;
}

bool rdma_log_entries_parser::read_uint64(std::string_view bytes, std::size_t& offset, std::uint64_t& value) {
    if (!read_bytes(bytes, offset, sizeof(std::uint64_t))) {
        return false;
    }
    std::array<char, sizeof(std::uint64_t)> raw{};
    std::copy_n(scalar_buffer_.begin(), raw.size(), raw.begin());

    std::uint32_t high = 0;
    std::uint32_t low = 0;
    std::array<char, sizeof(high)> high_bytes{};
    std::array<char, sizeof(low)> low_bytes{};
    std::copy_n(raw.begin(), high_bytes.size(), high_bytes.begin());
    std::copy_n(std::next(raw.begin(), static_cast<std::ptrdiff_t>(high_bytes.size())),
            low_bytes.size(), low_bytes.begin());
    std::memcpy(&high, high_bytes.data(), sizeof(high));
    std::memcpy(&low, low_bytes.data(), sizeof(low));
    scalar_buffer_.clear();
    value = (static_cast<std::uint64_t>(ntohl(high)) << 32U) | static_cast<std::uint64_t>(ntohl(low));
    return true;
}

bool rdma_log_entries_parser::read_string_length(
        std::string_view bytes,
        std::size_t& offset,
        parse_state next) {
    if (!read_uint32(bytes, offset, string_length_)) {
        return false;
    }
    string_buffer_.clear();
    string_buffer_.reserve(string_length_);
    state_ = next;
    return true;
}

bool rdma_log_entries_parser::read_string_bytes(
        std::string_view bytes,
        std::size_t& offset,
        std::string& target,
        parse_state next) {
    std::size_t needed = string_length_ - string_buffer_.size();
    std::size_t available = bytes.size() - offset;
    std::size_t take = std::min(needed, available);
    string_buffer_.append(bytes.substr(offset, take));
    offset += take;
    if (string_buffer_.size() != string_length_) {
        return false;
    }
    target = std::move(string_buffer_);
    string_buffer_.clear();
    string_length_ = 0;
    state_ = next;
    return true;
}

bool rdma_log_entries_parser::read_blob_bytes(std::string_view bytes, std::size_t& offset) {
    if (current_blob_file_ == nullptr) {
        open_current_blob_file();
    }

    std::size_t available = bytes.size() - offset;
    std::size_t take = std::min<std::size_t>(current_blob_remaining_, available);
    std::string_view chunk = bytes.substr(offset, take);
    std::size_t written = std::fwrite(chunk.data(), 1, chunk.size(), current_blob_file_);
    if (written != chunk.size()) {
        int ec = errno;
        close_current_blob_file_noexcept();
        LOG_AND_THROW_IO_EXCEPTION(
                "Failed to write RDMA blob chunk: " + current_blob_path_
                        + ", expected=" + std::to_string(chunk.size())
                        + ", actual=" + std::to_string(written),
                ec);
    }

    offset += take;
    current_blob_remaining_ -= static_cast<std::uint32_t>(take);
    return current_blob_remaining_ == 0;
}

void rdma_log_entries_parser::add_current_entry() {
    switch (current_entry_.type) {
        case log_entry::entry_type::normal_entry:
            message_->add_normal_entry(
                    current_entry_.storage_id,
                    current_entry_.key,
                    current_entry_.value,
                    current_entry_.write_version);
            break;
        case log_entry::entry_type::remove_entry:
            message_->add_remove_entry(
                    current_entry_.storage_id,
                    current_entry_.key,
                    current_entry_.write_version);
            break;
        case log_entry::entry_type::clear_storage:
            message_->add_clear_storage(current_entry_.storage_id, current_entry_.write_version);
            break;
        case log_entry::entry_type::add_storage:
            message_->add_add_storage(current_entry_.storage_id, current_entry_.write_version);
            break;
        case log_entry::entry_type::remove_storage:
            message_->add_remove_storage(current_entry_.storage_id, current_entry_.write_version);
            break;
        case log_entry::entry_type::normal_with_blob:
            message_->add_normal_with_blob(
                    current_entry_.storage_id,
                    current_entry_.key,
                    current_entry_.value,
                    current_entry_.write_version,
                    current_entry_.blob_ids);
            break;
        case log_entry::entry_type::this_id_is_not_used:
        case log_entry::entry_type::marker_begin:
        case log_entry::entry_type::marker_end:
        case log_entry::entry_type::marker_durable:
        case log_entry::entry_type::marker_invalidated_begin:
            throw std::runtime_error("Invalid entry type in RDMA LOG_ENTRY parser");
    }
    --entries_remaining_;
}

void rdma_log_entries_parser::apply_operation_flags(std::uint8_t flags) {
    message_->set_session_begin_flag((flags & message_log_entries::SESSION_BEGIN_FLAG) != 0);
    message_->set_session_end_flag((flags & message_log_entries::SESSION_END_FLAG) != 0);
    message_->set_flush_flag((flags & message_log_entries::FLUSH_FLAG) != 0);
}

void rdma_log_entries_parser::open_current_blob_file() {
    auto blob_file = datastore_->get_blob_file(current_blob_id_);
    auto& path = blob_file.path();
    auto parent = path.parent_path();

    if (!boost::filesystem::exists(parent)) {
        try {
            boost::filesystem::create_directory(parent);
        } catch (const boost::filesystem::filesystem_error& e) {
            LOG_AND_THROW_IO_EXCEPTION(
                    "Failed to create directory for RDMA blob file: " + parent.string(),
                    e.code().value());
        }
    } else if (!boost::filesystem::is_directory(parent)) {
        LOG_AND_THROW_IO_EXCEPTION(
                "Expected directory at path for RDMA blob file: " + parent.string(),
                EIO);
    }

    current_blob_path_ = path.string();
    current_blob_file_ = std::fopen(current_blob_path_.c_str(), "wb");  // NOLINT(cppcoreguidelines-owning-memory)
    if (current_blob_file_ == nullptr) {
        LOG_AND_THROW_IO_EXCEPTION("Failed to open RDMA blob for writing: " + current_blob_path_, errno);
    }
}

void rdma_log_entries_parser::finish_current_blob_file() {
    if (current_blob_file_ == nullptr) {
        return;
    }
    if (std::fflush(current_blob_file_) != 0) {
        int ec = errno;
        close_current_blob_file_noexcept();
        LOG_AND_THROW_IO_EXCEPTION("Failed to flush RDMA blob file: " + current_blob_path_, ec);
    }
    if (fsync(fileno(current_blob_file_)) == -1) {
        int ec = errno;
        LOG_LP(WARNING) << "fsync failed for RDMA blob file: "
                        << current_blob_path_ << ": " << strerror(ec);
    }
    close_current_blob_file_noexcept();
}

void rdma_log_entries_parser::close_current_blob_file_noexcept() noexcept {
    if (current_blob_file_ == nullptr) {
        return;
    }
    if (std::fclose(current_blob_file_) != 0) {  // NOLINT(cppcoreguidelines-owning-memory)
        LOG_LP(ERROR) << "Failed to close RDMA blob file: " << strerror(errno);
    }
    current_blob_file_ = nullptr;
}

}  // namespace limestone::replication
