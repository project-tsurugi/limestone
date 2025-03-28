#pragma once

#include <cstdint>
#include <netinet/in.h>

uint16_t get_free_port();
int start_test_server(uint16_t port, bool echo_message, bool close_immediately = false);
sockaddr_in make_listen_addr(uint16_t port);