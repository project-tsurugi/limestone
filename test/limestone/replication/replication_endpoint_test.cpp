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

#include "replication/replication_endpoint.h"
#include <gtest/gtest.h>
#include <cstdlib>
#include <cstring>
#include <netinet/in.h>
#include <arpa/inet.h>

namespace limestone::testing {

using namespace limestone::replication;    

// Helper function to set environment variable (POSIX)
void set_env(const char* name, const char* value) {
    setenv(name, value, 1);
}

// Helper function to unset environment variable
void unset_env(const char* name) {
    unsetenv(name);
}

// Test fixture for replication_endpoint tests.
class replication_endpoint_test : public ::testing::Test {
protected:
    void TearDown() override {
        // Cleanup environment variable after each test.
        unset_env("TSURUGI_REPLICATION_ENDPOINT");
    }
};

// Test case: environment variable is not set.
TEST_F(replication_endpoint_test, env_not_set) {
    // Ensure the variable is not set.
    unset_env("TSURUGI_REPLICATION_ENDPOINT");
    replication_endpoint ep;
    EXPECT_FALSE(ep.env_defined());
    EXPECT_FALSE(ep.is_valid());
    // Dummy values should be returned.
    EXPECT_EQ(ep.host(), "0.0.0.0");
    EXPECT_EQ(ep.port(), 0);
}

// Test case: valid endpoint string (using localhost and a test port).
TEST_F(replication_endpoint_test, valid_endpoint) {
    // Set environment variable with a valid endpoint.
    set_env("TSURUGI_REPLICATION_ENDPOINT", "tcp://localhost:1234");
    replication_endpoint ep;
    EXPECT_TRUE(ep.env_defined());
    EXPECT_TRUE(ep.is_valid());
    EXPECT_EQ(ep.protocol(), replication_protocol::TCP);
    // Parsed host should be "localhost" and port 1234.
    EXPECT_EQ(ep.host(), "localhost");
    EXPECT_EQ(ep.port(), 1234);

    // Check get_ip_address() returns "127.0.0.1" (for localhost).
    std::string ip_addr = ep.get_ip_address();
    EXPECT_EQ(ip_addr, "127.0.0.1");

    // Check get_sockaddr() returns correct port and IP.
    struct sockaddr_in addr = ep.get_sockaddr();
    EXPECT_EQ(addr.sin_family, AF_INET);
    EXPECT_EQ(ntohs(addr.sin_port), 1234); // Compare port in host order.
    char addr_str[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &(addr.sin_addr), addr_str, sizeof(addr_str));
    EXPECT_STREQ(addr_str, "127.0.0.1");
}

// Test case: invalid endpoint string.
TEST_F(replication_endpoint_test, invalid_endpoint) {
    // Set environment variable with an invalid endpoint string.
    set_env("TSURUGI_REPLICATION_ENDPOINT", "invalid_endpoint");
    replication_endpoint ep;
    // Env is defined but parsing fails.
    EXPECT_TRUE(ep.env_defined());
    EXPECT_FALSE(ep.is_valid());
    // Dummy values should be set.
    EXPECT_EQ(ep.host(), "0.0.0.0");
    EXPECT_EQ(ep.port(), 0);
}


// Test case: getaddrinfo (or inet_pton) failure.
// Here we use a hostname that is extremely unlikely to resolve.
TEST_F(replication_endpoint_test, resolution_failure) {
    // "nonexistent.invalid" is reserved for invalid domains.
    set_env("TSURUGI_REPLICATION_ENDPOINT", "tcp://nonexistent.invalid:1234");
    replication_endpoint ep;
    EXPECT_TRUE(ep.env_defined());
    EXPECT_FALSE(ep.is_valid());
    // Host remains as parsed.
    EXPECT_EQ(ep.host(), "nonexistent.invalid");
    EXPECT_EQ(ep.port(), 1234);
    // Expect dummy values due to resolution failure.
    EXPECT_EQ(ep.get_ip_address(), "0.0.0.0");
    struct sockaddr_in addr = ep.get_sockaddr();
    EXPECT_EQ(ntohs(addr.sin_port), 0);
    char addr_str[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &(addr.sin_addr), addr_str, sizeof(addr_str));
    EXPECT_STREQ(addr_str, "0.0.0.0");
}

} // namespace limestone::testing