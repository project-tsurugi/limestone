#include "socket_streambuf.h"
#include <sys/socket.h>
#include <iterator>  // for std::next
#include <cstddef>   // for std::ptrdiff_t
#include "../limestone_exception_helper.h"

namespace limestone::replication {

socket_streambuf::socket_streambuf(int socket_fd) : socket_fd_(socket_fd) {
    // Retrieve the socket's receive buffer size.
    int rcv_buf_size = 65536;  // Fallback default: 64KB.
    socklen_t optlen = sizeof(rcv_buf_size);
    if (getsockopt(socket_fd_, SOL_SOCKET, SO_RCVBUF, &rcv_buf_size, &optlen) != 0) {
        LOG_LP(WARNING) << "getsockopt(SO_RCVBUF) failed: " << strerror(errno)
                        << ", using default 1024 bytes." << std::endl;
    }
    // Resize the buffer to the received size.
    buffer_.resize(static_cast<std::size_t>(rcv_buf_size));
    // Use std::next with an explicit cast to avoid narrowing conversion.
    auto* buf_begin = buffer_.data();
    auto* buf_end = std::next(buf_begin, static_cast<std::ptrdiff_t>(buffer_.size()));
    // Initialize get area as empty, but with allocated capacity.
    setg(buf_begin, buf_end, buf_end);
}

std::streambuf::int_type socket_streambuf::underflow() {
    if (gptr() < egptr()) {  // Buffer not exhausted.
        return traits_type::to_int_type(*gptr());
    }
    ssize_t bytes_read = 0;
    // Loop to retry recv() if interrupted by EINTR.
    while (true) {
        bytes_read = ::recv(socket_fd_, buffer_.data(), buffer_.size(), 0);
        if (bytes_read < 0) {
            if (errno == EINTR) {
                // Retry on EINTR.
                continue;
            }
            // Log warning for other errors and treat as EOF.
            LOG_LP(WARNING) << "recv() failed with error: " << strerror(errno) << std::endl;
            return traits_type::eof();
        }
        break;
    }
    if (bytes_read == 0) {
        // Connection closed gracefully.
        return traits_type::eof();
    }
    // Use std::next with explicit cast.
    auto* buf_begin = buffer_.data();
    auto* new_end = std::next(buf_begin, static_cast<std::ptrdiff_t>(bytes_read));
    // Set the get area with the newly read data.
    setg(buf_begin, buf_begin, new_end);
    return traits_type::to_int_type(*gptr());
}

} // namespace limestone::replication
