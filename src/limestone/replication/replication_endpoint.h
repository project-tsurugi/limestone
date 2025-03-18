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

 #pragma once

 #include <string>
 #include <regex>
 #include <netinet/in.h>
 
 namespace limestone::replication {

 // Enumeration for replication protocols. Currently, only TCP is supported.
 enum class replication_protocol {
     TCP,
 };
 
 /// Class to parse and manage the TSURUGI_REPLICATION_ENDPOINT environment variable.
 /// 
 /// This class supports endpoints specified either as a hostname or an IP address (IPv4 only),
 /// and pre-generates the resolved IP address and sockaddr_in structure in the constructor.
 /// If the endpoint is invalid or name resolution fails, the default dummy values ("0.0.0.0" and port 0)
 /// remain and is_valid() returns false.
 ///
 /// Note: This implementation is IPv4-only. IPv6 addresses are not supported.
 class replication_endpoint {
 public:
     /// Constructor: Retrieves, parses, and resolves the TSURUGI_REPLICATION_ENDPOINT environment variable.
     /// If the variable is defined and both parsing and name resolution succeed,
     /// the resolved IP and sockaddr_in are generated.
     /// Otherwise, default dummy values ("0.0.0.0" and port 0) remain.
     replication_endpoint();

     /// Returns true if the environment variable is defined.
     [[nodiscard]] bool env_defined() const;

     /// Returns true if the endpoint was successfully parsed and resolved.
     [[nodiscard]] bool is_valid() const;

     /// Returns the protocol (currently always TCP).
     [[nodiscard]] replication_protocol protocol() const;

     /// Returns the host part (as provided, either a hostname or an IP address).
     [[nodiscard]] std::string host() const;

     /// Returns the port number.
     [[nodiscard]] int port() const;

     /// Returns the resolved IP address as a numeric string.
     [[nodiscard]] std::string get_ip_address() const;

     /// Returns a sockaddr_in structure that can be used directly with bind() or connect().
     [[nodiscard]] struct sockaddr_in get_sockaddr() const;

     private:
     bool env_defined_{false};            // Whether the environment variable is defined.
     bool endpoint_is_valid_{false};      // Whether the endpoint was successfully parsed and resolved.
     replication_protocol protocol_{replication_protocol::TCP}; // Protocol type (currently only TCP).
     std::string host_{"0.0.0.0"};        // Host part extracted from the environment variable.
     int port_{0};                        // Port number extracted from the environment variable.
  
     std::string resolved_ip_{"0.0.0.0"}; // Resolved IP address (numeric string) or default.
     struct sockaddr_in sockaddr_{};    // Pre-generated sockaddr_in structure.
 
     /// Parses the endpoint string using a regular expression and performs name resolution.
     /// Expected format: "tcp://<host>:<port>".
     /// If parsing and resolution succeed, returns true and overwrites the default dummy values.
     /// Otherwise, returns false, leaving the default dummy values.
     /// 
     /// Note: Name resolution is performed using getaddrinfo(), which accepts both hostnames
     /// and numeric IPv4 addresses. This class does not support IPv6.
     bool parse_endpoint(const std::string& endpoint_str);
 };

 }  // namespace limestone::replication