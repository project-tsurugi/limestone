/*
 * Copyright 2022-2025 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

 #include "replication_endpoint.h"
#include <cstdlib>
#include <netdb.h>
#include <arpa/inet.h>
#include <cstring>
#include <array>

namespace limestone::replication {

replication_endpoint::replication_endpoint() {
    load_from_env("TSURUGI_REPLICATION_ENDPOINT");
}

replication_endpoint::replication_endpoint(const char* env_name) {
    load_from_env(env_name);
}

void replication_endpoint::load_from_env(const char* env_name) {
    // Prepare default dummy sockaddr_in.
    std::memset(&sockaddr_, 0, sizeof(sockaddr_));
    sockaddr_.sin_family = AF_INET;
    sockaddr_.sin_port = 0;
    sockaddr_.sin_addr.s_addr = inet_addr("0.0.0.0");

    const char* env_val = std::getenv(env_name);
    if (env_val) {
        env_defined_ = true;
        endpoint_is_valid_ = parse_endpoint(std::string(env_val));
    }
}

bool replication_endpoint::env_defined() const {
    return env_defined_;
}

bool replication_endpoint::is_valid() const {
    return endpoint_is_valid_;
}

replication_protocol replication_endpoint::protocol() const {
    return protocol_;
}

std::string replication_endpoint::host() const {
    return host_;
}

int replication_endpoint::port() const {
    return port_;
}

bool replication_endpoint::parse_endpoint(const std::string &endpoint_str) {
    // Parse endpoint of the form "tcp://<host>:<port>" using a regular expression.
    std::regex regex_pattern("^tcp://([^:]+):(\\d+)$");
    std::smatch match;
    if (!std::regex_match(endpoint_str, match, regex_pattern)) {
        return false;
    }
    host_ = match[1].str();
    port_ = std::stoi(match[2].str());
    protocol_ = replication_protocol::TCP;
    
    // Attempt to resolve host_ using getaddrinfo, regardless of whether it's a hostname or numeric IP.
    struct addrinfo hints{};
    struct addrinfo *res = nullptr;
    hints.ai_family = AF_INET; // IPv4
    hints.ai_socktype = SOCK_STREAM;
    int error = getaddrinfo(host_.c_str(), nullptr, &hints, &res);
    if (error != 0 || res == nullptr) {
        return false;
    }
    std::array<char, INET_ADDRSTRLEN> ipstr{};
    // NOLINTNEXTLINE(bugprone-casting-through-void, cppcoreguidelines-pro-type-cstyle-cast)
    auto* ipv4 = static_cast<struct sockaddr_in*>(static_cast<void*>(res->ai_addr));
    inet_ntop(AF_INET, &(ipv4->sin_addr), ipstr.data(), ipstr.size());
    resolved_ip_ = std::string(ipstr.data());
    freeaddrinfo(res);

    // Prepare sockaddr_in structure.
    std::memset(&sockaddr_, 0, sizeof(sockaddr_));
    sockaddr_.sin_family = AF_INET;
    sockaddr_.sin_port = htons(port_);
    // Convert resolved_ip_ (string) to binary form and store in sockaddr_.sin_addr.
    // Normally, resolved_ip_ is guaranteed to be valid (from getaddrinfo),
    // so this branch should never be reached. However, we include it as a defensive
    // check to ensure that any unexpected invalid IP string is caught.
    return inet_pton(AF_INET, resolved_ip_.c_str(), &sockaddr_.sin_addr) == 1;
}

std::string replication_endpoint::get_ip_address() const {
    // Simply return the pre-generated resolved IP.
    return resolved_ip_;
}

struct sockaddr_in replication_endpoint::get_sockaddr() const {
    // Return the pre-generated sockaddr_in.
    return sockaddr_;
}

} // namespace limestone::replication
