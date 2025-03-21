#pragma once

#include <streambuf>
#include <vector>
#include <sys/types.h>
#include <sys/socket.h>
#include <unistd.h>
#include <cerrno>
#include <cstring>
#include <iostream>

namespace limestone::replication {

// Custom streambuf to wrap a socket file descriptor.
class socket_streambuf : public std::streambuf {
public:
    // Constructor: initialize with a valid socket file descriptor.
    explicit socket_streambuf(int socket_fd);

protected:
    // Override underflow() to fill the buffer from socket.
    int_type underflow() override;

private:
    int socket_fd_;
    std::vector<char> buffer_;
};

} // namespace limestone::replication
