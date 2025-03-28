#include "replication_test_helper.h"
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

uint16_t get_free_port() {
    int sock = ::socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;
    ::bind(sock, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    socklen_t len = sizeof(addr);
    ::getsockname(sock, reinterpret_cast<sockaddr*>(&addr), &len);
    uint16_t port = ntohs(addr.sin_port);
    ::close(sock);
    return port;
}

int start_test_server(uint16_t port, bool echo_message, bool close_immediately) {
    int listen_fd = ::socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(port);

    int opt = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    bind(listen_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    listen(listen_fd, 1);
    return listen_fd;
}

sockaddr_in make_listen_addr(uint16_t port) {
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(port);
    return addr;
}