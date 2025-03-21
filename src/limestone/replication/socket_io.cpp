#include "socket_io.h"
#include "../limestone_exception_helper.h"
#include <cstring>
#include <memory>
#include <array>

namespace limestone::replication {

// Constructor for real socket mode.
socket_io::socket_io(int fd)
    : is_string_mode_(false),
      socket_fd_(fd),
      socket_buf_(std::make_unique<socket_streambuf>(fd)),
      out_stream_(std::make_unique<std::ostringstream>(std::ios_base::out))
{
    // Create std::istream using the raw pointer from socket_buf_.
    in_stream_ = std::make_unique<std::istream>(socket_buf_.get());
}

// Constructor for string mode.
socket_io::socket_io(const std::string &initial)
    : is_string_mode_(true),
      socket_fd_(-1),
      socket_buf_(nullptr),
      out_stream_(std::make_unique<std::ostringstream>(std::ios_base::out))
{
    // Create std::istringstream from the initial string.
    in_stream_ = std::unique_ptr<std::istream>( new std::istringstream(initial) );
}

socket_io::~socket_io() {
    close();
}

bool socket_io::write_raw(const std::string &data) const{
    if (is_string_mode_) {
        // In string mode, writing is buffered.
        return false;
    }
    // In real socket mode: send data using send() with MSG_NOSIGNAL to avoid SIGPIPE.
    // TODO: 全部送るまでループする。
    // TODO: TCP_NODELAYをつけるかつけないかを指定可能にする。
    ssize_t bytes_sent = ::send(socket_fd_, data.data(), data.size(), MSG_NOSIGNAL);
    if (bytes_sent < 0 || static_cast<size_t>(bytes_sent) != data.size()) {
        LOG_LP(ERROR) << "Failed to send complete data: " << strerror(errno);
        return false;
    }
    return true;
}

void socket_io::send_uint16(uint16_t value) {
    uint16_t net_value = htons(value);
    std::array<char, sizeof(net_value)> buffer{};
    std::memcpy(buffer.data(), &net_value, sizeof(net_value));
    out_stream_->write(buffer.data(), buffer.size());
}

void socket_io::send_uint32(uint32_t value) {
    uint32_t net_value = htonl(value);
    std::array<char, sizeof(net_value)> buffer{};
    std::memcpy(buffer.data(), &net_value, sizeof(net_value));
    out_stream_->write(buffer.data(), buffer.size());
}

void socket_io::send_uint64(uint64_t value) {
    constexpr uint64_t mask32 = 0xFFFFFFFFULL;
    uint32_t high = htonl(static_cast<uint32_t>(value >> 32U));
    uint32_t low  = htonl(static_cast<uint32_t>(value & mask32));
    std::array<char, sizeof(high) + sizeof(low)> buffer{};
    std::memcpy(buffer.data(), &high, sizeof(high));
    std::memcpy(buffer.data() + sizeof(high), &low, sizeof(low));
    out_stream_->write(buffer.data(), buffer.size());
}

void socket_io::send_uint8(uint8_t value) {
    std::array<char, sizeof(value)> buffer{};
    std::memcpy(buffer.data(), &value, sizeof(value));
    out_stream_->write(buffer.data(), buffer.size());
}

void socket_io::send_string(const std::string &value) {
    send_uint32(static_cast<uint32_t>(value.size()));
    out_stream_->write(value.data(), static_cast<std::streamsize>(value.size()));
}

uint16_t socket_io::receive_uint16() {
    std::array<char, sizeof(uint16_t)> buffer{};
    in_stream_->read(buffer.data(), buffer.size());
    if (!(*in_stream_)) {
        LOG_AND_THROW_IO_EXCEPTION("Failed to read uint16_t from input stream", errno);
    }
    uint16_t net_value = 0;
    std::memcpy(&net_value, buffer.data(), sizeof(net_value));
    return ntohs(net_value);
}

uint32_t socket_io::receive_uint32() {
    std::array<char, sizeof(uint32_t)> buffer{};
    in_stream_->read(buffer.data(), buffer.size());
    if (!(*in_stream_)) {
        LOG_AND_THROW_IO_EXCEPTION("Failed to read uint32_t from input stream", errno);
    }
    uint32_t net_value = 0;
    std::memcpy(&net_value, buffer.data(), sizeof(net_value));
    return ntohl(net_value);
}

uint64_t socket_io::receive_uint64() {
    uint32_t high = 0;
    uint32_t low = 0;
    {
        std::array<char, sizeof(uint32_t)> buffer{};
        in_stream_->read(buffer.data(), buffer.size());
        if (!(*in_stream_)) {
            LOG_AND_THROW_IO_EXCEPTION("Failed to read high 32 bits of uint64_t from input stream", errno);
        }
        std::memcpy(&high, buffer.data(), sizeof(uint32_t));
    }
    {
        std::array<char, sizeof(uint32_t)> buffer{};
        in_stream_->read(buffer.data(), buffer.size());
        if (!(*in_stream_)) {
            LOG_AND_THROW_IO_EXCEPTION("Failed to read low 32 bits of uint64_t from input stream", errno);
        }
        std::memcpy(&low, buffer.data(), sizeof(uint32_t));
    }
    uint64_t value = (static_cast<uint64_t>(ntohl(high)) << 32U)
                     | static_cast<uint64_t>(ntohl(low));
    return value;
}

uint8_t socket_io::receive_uint8() {
    std::array<char, sizeof(uint8_t)> buffer{};
    in_stream_->read(buffer.data(), buffer.size());
    if (!(*in_stream_)) {
        LOG_AND_THROW_IO_EXCEPTION("Failed to read uint8_t from input stream", errno);
    }
    uint8_t value = 0;
    std::memcpy(&value, buffer.data(), sizeof(value));
    return value;
}

std::string socket_io::receive_string() {
    uint32_t len = receive_uint32();
    std::string result;
    result.resize(len);
    in_stream_->read(result.data(), static_cast<std::streamsize>(result.size()));
    if (!(*in_stream_)) {
        LOG_AND_THROW_IO_EXCEPTION("Failed to read string body from input stream", errno);
    }
    return result;
}

bool socket_io::flush() {
    if (is_string_mode_) {
        return true;
    }
    std::string data = out_stream_->str();
    if (data.empty()) {
        return true;
    }
    bool ret = write_raw(data);
    out_stream_->str("");
    out_stream_->clear();
    return ret;
}

std::string socket_io::get_out_string() const {
    return out_stream_->str();
}

void socket_io::close() {
    flush();
    if (!is_string_mode_) {
        if (socket_fd_ != -1) {
            int ret = 0;
            do {
                ret = ::close(socket_fd_);
            } while (ret == -1 && errno == EINTR);
            if (ret == -1 && errno != EBADF) {
                LOG_LP(WARNING) << "close() failed: " << strerror(errno);
            }
            socket_fd_ = -1;
        }
    }
}

}  // namespace limestone::replication
