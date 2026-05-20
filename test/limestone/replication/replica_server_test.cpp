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

#include "replication/replica_server.h"

#include <arpa/inet.h>
#include <gtest/gtest.h>

#include <boost/filesystem.hpp>
#include <chrono>
#include <future>
#include <thread>

#include "rdma/rdma_receiver_base.h"
#include "rdma/rdma_sender_base.h"
#include "replication/channel_handler_base.h"
#include "replication/message_error.h"
#include "replication/message_session_begin.h"
#include "replication/replica_connector.h"
#include "replication/replication_message_io.h"
#include "replication/handler_resources.h"
#include "replication/log_channel_handler.h"
#include "noop_rdma_mocks.h"
#include "replication_test_helper.h"
namespace limestone::testing {

using namespace limestone::replication;

 class replica_server_test : public ::testing::Test {
 public:
     static constexpr const char* base_location = "/tmp/replica_server_test";
     boost::filesystem::path location1 = boost::filesystem::path(base_location) / "replica1";
     boost::filesystem::path location2 = boost::filesystem::path(base_location) / "replica2";

     void SetUp() override {
         if (boost::filesystem::exists(base_location)) {
             boost::filesystem::permissions(base_location, boost::filesystem::owner_all);
         }
         boost::filesystem::remove_all(base_location);
         boost::filesystem::create_directories(location1);
         boost::filesystem::create_directories(location2);
     }

     void TearDown() override {
         if (boost::filesystem::exists(base_location)) {
             boost::filesystem::permissions(base_location, boost::filesystem::owner_all);
         }
         boost::filesystem::remove_all(base_location);
     }
 };

class test_session_handler : public limestone::replication::channel_handler_base {
public:
    test_session_handler(limestone::replication::replica_server& server, replication_message_io& io, std::promise<bool>& invoked) noexcept : channel_handler_base(server, io), invoked_(invoked) {}

 protected:
     limestone::replication::validation_result authorize() override { return limestone::replication::validation_result::success(); }
     limestone::replication::validation_result validate_initial(std::unique_ptr<limestone::replication::replication_message> /*req*/) override {
         invoked_.set_value(true);
         return limestone::replication::validation_result::success();
     }
     void send_initial_ack() const override {}
     void dispatch(limestone::replication::replication_message& /*msg*/, limestone::replication::handler_resources& /*resources*/) override {}

 private:
     std::promise<bool>& invoked_;
 };

class fake_log_channel_handler : public log_channel_handler {
public:
    fake_log_channel_handler(replica_server& server, replication_message_io& io, bool& invoked) noexcept
        : log_channel_handler(server, io),
          invoked_(invoked) {}

    void handle_rdma_data_event(rdma_data_event const& /*event*/) override {
        invoked_ = true;
    }

private:
    bool& invoked_;
};

 TEST_F(replica_server_test, initialize_does_not_throw) {
     replication::replica_server server;
     EXPECT_NO_THROW(server.initialize(location1));
 }
 
 TEST_F(replica_server_test, start_listener_succeeds) {
     replication::replica_server server;
     server.initialize(location1);
     auto addr = make_listen_addr(get_free_port());
     EXPECT_TRUE(server.start_listener(addr));
     server.shutdown();
 }
 
 TEST_F(replica_server_test, start_listener_fails_if_port_in_use) {
    replication::replica_server s1;
    s1.initialize(location1);
    uint16_t port = get_free_port();
    auto addr = make_listen_addr(port);
    ASSERT_TRUE(s1.start_listener(addr));

    replication::replica_server s2;
    s2.initialize(location2);
    EXPECT_FALSE(s2.start_listener(addr));  // should fail but not abort

    s1.shutdown();
}

 
 TEST_F(replica_server_test, accept_loop_can_be_shutdown) {
     replication::replica_server server;
     server.initialize(location1);
     auto addr = make_listen_addr(get_free_port());
     ASSERT_TRUE(server.start_listener(addr));
 
     std::thread accept_thread([&server]() {
         server.accept_loop();
     });
 
     std::this_thread::sleep_for(std::chrono::milliseconds(50));
     EXPECT_NO_THROW(server.shutdown());
     accept_thread.join();
 }
 
 TEST_F(replica_server_test, start_listener_and_client_connect_disconnect) {
    replication::replica_server server;
    server.initialize(location1);
    uint16_t port = get_free_port();
    auto addr = make_listen_addr(port);
    ASSERT_TRUE(server.start_listener(addr));

    std::thread accept_thread([&server]() {
        server.accept_loop();
    });

    // Allow time for the listener to be ready (can be removed if eventfd is used)
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    replication::replica_connector client;
    EXPECT_TRUE(client.connect_to_server("127.0.0.1", port));
    client.close_session();

    server.shutdown();
    accept_thread.join();
}

TEST_F(replica_server_test, no_handler_returns_error) {
    replication::replica_server server;
    server.initialize(location1);
    server.clear_handlers();
    uint16_t port = get_free_port();
    auto addr = make_listen_addr(port);
    ASSERT_TRUE(server.start_listener(addr));

    std::thread server_thread([&server]() { server.accept_loop(); });

    replication::replica_connector client;
    ASSERT_TRUE(client.connect_to_server("127.0.0.1", port));

    auto request = message_session_begin::create();
    static_cast<message_session_begin*>(request.get())->set_param("config", 42);
    EXPECT_TRUE(client.send_message(*request));

    auto response = client.receive_message();
    ASSERT_NE(response, nullptr);
    EXPECT_EQ(response->get_message_type_id(), message_type_id::COMMON_ERROR);

    client.close_session();
    server.shutdown();
    server_thread.join();
}

TEST_F(replica_server_test, registered_handler_is_called) {
    replication::replica_server server;
    server.initialize(location1);
    server.clear_handlers();
    uint16_t port = get_free_port();
    auto addr = make_listen_addr(port);
    ASSERT_TRUE(server.start_listener(addr));

    std::promise<bool> invoked;
    
    server.register_handler(replication::message_type_id::SESSION_BEGIN,
        [&server, &invoked](replication_message_io& io) {
            return std::make_shared<test_session_handler>(server, io, invoked);
        });
    

    std::thread server_thread([&server]() { server.accept_loop(); });

    replication::replica_connector client;
    ASSERT_TRUE(client.connect_to_server("127.0.0.1", port));
    auto request = message_session_begin::create();
    static_cast<message_session_begin*>(request.get())->set_param("config", 100);
    EXPECT_TRUE(client.send_message(*request));

    client.close_session();
    EXPECT_TRUE(invoked.get_future().get());

    server.shutdown();
    server_thread.join();
}

TEST_F(replica_server_test, shutdown_wakes_client_handler_blocked_in_receive) {
    replication::replica_server server;
    server.initialize(location1);
    server.clear_handlers();
    uint16_t port = get_free_port();
    auto addr = make_listen_addr(port);
    ASSERT_TRUE(server.start_listener(addr));

    std::promise<bool> invoked;
    server.register_handler(replication::message_type_id::SESSION_BEGIN,
        [&server, &invoked](replication_message_io& io) {
            return std::make_shared<test_session_handler>(server, io, invoked);
        });

    std::thread server_thread([&server]() { server.accept_loop(); });

    replication::replica_connector client;
    ASSERT_TRUE(client.connect_to_server("127.0.0.1", port));
    auto request = message_session_begin::create();
    static_cast<message_session_begin*>(request.get())->set_param("config", 100);
    ASSERT_TRUE(client.send_message(*request));
    ASSERT_TRUE(invoked.get_future().get());

    auto shutdown_future = std::async(std::launch::async, [&server]() {
        server.shutdown();
    });

    if (shutdown_future.wait_for(std::chrono::seconds{2}) != std::future_status::ready) {
        ADD_FAILURE() << "server.shutdown() did not wake a client handler blocked in receive";
        client.close_session();
        ASSERT_EQ(shutdown_future.wait_for(std::chrono::seconds{2}), std::future_status::ready);
    }
    shutdown_future.get();
    server_thread.join();
}


TEST_F(replica_server_test, shutdown_before_accept_loop_starts) {
    replication::replica_server server;
    server.initialize(location1);
    uint16_t port = get_free_port();
    auto addr = make_listen_addr(port);
    ASSERT_TRUE(server.start_listener(addr));

    // Call shutdown before starting the accept loop
    EXPECT_NO_THROW(server.shutdown());

    // Start the accept loop after shutdown
    std::thread accept_thread([&server]() {
        server.accept_loop();
    });

    // Try connecting to the server again, should be a failure after shutdown
    replication::replica_connector client;
    EXPECT_FALSE(client.connect_to_server("127.0.0.1", port));

    // Join the accept thread
    accept_thread.join();
}

TEST_F(replica_server_test, listener_restart_multiple_times) {
    replication::replica_server server;
    server.initialize(location1);
    
    uint16_t port = get_free_port();
    auto addr = make_listen_addr(port);

    // Start listener for the first time
    ASSERT_TRUE(server.start_listener(addr));

    // Try to connect to the server
    std::thread accept_thread([&server]() {
        server.accept_loop();
    });

    replication::replica_connector client;
    EXPECT_TRUE(client.connect_to_server("127.0.0.1", port));
    client.close_session();

    // Shutdown the server
    server.shutdown();
    accept_thread.join();

    // shutdown() releases the datastore, so re-initialize before restarting.
    server.initialize(location1);

    // Restart listener for the second time
    ASSERT_TRUE(server.start_listener(addr));

    std::thread accept_thread_2([&server]() {
        server.accept_loop();
    });

    EXPECT_TRUE(client.connect_to_server("127.0.0.1", port));
    client.close_session();

    // Shutdown the server again
    server.shutdown();
    accept_thread_2.join();
}

TEST_F(replica_server_test, get_datastore_returns_valid_instance) {
    replication::replica_server server;
    server.initialize(location1);
    
    auto& ds = server.get_datastore();
    EXPECT_NE(&ds, nullptr);
}

TEST_F(replica_server_test, get_location_returns_correct_path) {
    replication::replica_server server;
    server.initialize(location1);
    
    auto location = server.get_location();
    EXPECT_EQ(location.string(), location1.string());
}

TEST_F(replica_server_test, mark_control_channel_created_sets_flag) {
    replication::replica_server server;
    server.initialize(location1);

    EXPECT_TRUE(server.mark_control_channel_created());
}

TEST_F(replica_server_test, initialize_rdma_with_only_receiver_set_returns_failed) {
    replication::replica_server server;
    server.initialize(location1);

    // Force a partial state: only the data receiver is set, ack_sender is null.
    // initialize_rdma() must surface this as failed rather than already_initialized.
    server.set_rdma_receiver_for_test(std::make_unique<noop_rdma_receiver>());

    auto result = server.initialize_rdma(4, 0x1ULL);
    EXPECT_EQ(result, replication::replica_server::rdma_init_result::failed);
}

TEST_F(replica_server_test, initialize_rdma_with_only_sender_set_returns_failed) {
    replication::replica_server server;
    server.initialize(location1);

    // Symmetric to the previous test: only the ack_sender is set.
    server.set_ack_sender_for_test(std::make_unique<noop_rdma_sender>());

    auto result = server.initialize_rdma(4, 0x1ULL);
    EXPECT_EQ(result, replication::replica_server::rdma_init_result::failed);
}

#ifdef LIMESTONE_ENABLE_RDMA
TEST_F(replica_server_test, initialize_rdma_success_then_already_initialized) {
    replication::replica_server server;
    server.initialize(location1);

    // Dummy DMA address; ack_sender->initialize() with a real RDMA stack would
    // ultimately require a peer reachable at this address. The existing baseline
    // tolerates this not working end-to-end at the §2 stage.
    constexpr std::uint64_t dummy_leader_ack_dma_address = 0x1ULL;
    auto first = server.initialize_rdma(4, dummy_leader_ack_dma_address);
    EXPECT_EQ(first, replication::replica_server::rdma_init_result::success);

    auto second = server.initialize_rdma(4, dummy_leader_ack_dma_address);
    EXPECT_EQ(second, replication::replica_server::rdma_init_result::already_initialized);
}
#endif // LIMESTONE_ENABLE_RDMA

TEST_F(replica_server_test, finalize_rdma_returns_not_initialized_before_initialize) {
    replication::replica_server server;
    server.initialize(location1);

    auto result = server.finalize_rdma();
    EXPECT_EQ(result, replication::replica_server::rdma_finalize_result::not_initialized);
}

TEST_F(replica_server_test, finalize_rdma_returns_success_after_initialize) {
    replication::replica_server server;
    server.initialize(location1);

    server.set_rdma_receiver_factory_for_test(
        [](std::uint32_t /*slot_count*/) -> std::unique_ptr<replication::rdma_receiver_base> {
            return std::make_unique<noop_rdma_receiver>();
        });
    server.set_ack_sender_factory_for_test(
        [](std::uint32_t /*slot_count*/) -> std::unique_ptr<replication::rdma_sender_base> {
            return std::make_unique<noop_rdma_sender>();
        });
    ASSERT_EQ(server.initialize_rdma(4U, 0x1ULL),
              replication::replica_server::rdma_init_result::success);

    auto result = server.finalize_rdma();
    EXPECT_EQ(result, replication::replica_server::rdma_finalize_result::success);
}

namespace {

// Receiver stub whose finalize_channel_setup_with_sender deterministically fails,
// exercising the rdma_finalize_result::failed branch of replica_server::finalize_rdma.
class failing_finalize_rdma_receiver : public noop_rdma_receiver {
public:
    operation_result finalize_channel_setup_with_sender(
            replication::rdma_sender_base* /*sender*/) noexcept override {
        return {false, "stub finalize failure"};
    }
};

}  // namespace

TEST_F(replica_server_test, finalize_rdma_returns_failed_when_receiver_finalize_fails) {
    replication::replica_server server;
    server.initialize(location1);

    server.set_rdma_receiver_factory_for_test(
        [](std::uint32_t /*slot_count*/) -> std::unique_ptr<replication::rdma_receiver_base> {
            return std::make_unique<failing_finalize_rdma_receiver>();
        });
    server.set_ack_sender_factory_for_test(
        [](std::uint32_t /*slot_count*/) -> std::unique_ptr<replication::rdma_sender_base> {
            return std::make_unique<noop_rdma_sender>();
        });
    ASSERT_EQ(server.initialize_rdma(4U, 0x1ULL),
              replication::replica_server::rdma_init_result::success);

    auto result = server.finalize_rdma();
    EXPECT_EQ(result, replication::replica_server::rdma_finalize_result::failed);
}

TEST_F(replica_server_test, on_rdma_receive_invokes_handler_for_data_event) {
    replica_server server;
    server.initialize(location1);

    int pipefd[2];
    ASSERT_EQ(::pipe(pipefd), 0);
    replication_message_io io(pipefd[1]);
    bool invoked = false;
    auto handler = std::make_shared<fake_log_channel_handler>(server, io, invoked);
    server.set_log_channel_handler_for_test(1U, handler);

    rdma_data_event ev{};
    ev.header.version = rdma_frame_current_version;
    ev.header.channel_id = 1U;
    ev.header.sequence_number = 0U;
    ev.header.payload_size = 0U;
    ev.payload = {};

    server.on_rdma_receive(rdma_receive_event{ev});
    EXPECT_TRUE(invoked);

    ::close(pipefd[0]);
    ::close(pipefd[1]);
}

TEST_F(replica_server_test, on_rdma_receive_error_event_does_not_invoke_handler) {
    replica_server server;
    server.initialize(location1);

    int pipefd[2];
    ASSERT_EQ(::pipe(pipefd), 0);
    replication_message_io io(pipefd[1]);
    bool invoked = false;
    auto handler = std::make_shared<fake_log_channel_handler>(server, io, invoked);
    server.set_log_channel_handler_for_test(1U, handler);

    rdma_error_event err{};
    err.error_message = "test-error";

    server.on_rdma_receive(rdma_receive_event{err});
    EXPECT_FALSE(invoked);

    ::close(pipefd[0]);
    ::close(pipefd[1]);
}

}  // namespace limestone::testing
