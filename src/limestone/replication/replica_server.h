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

#include <array>
#include <functional>
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <mutex>
#include <vector>
#include <boost/filesystem.hpp>
#include "replication_message.h"
#include <limestone/api/datastore.h>
#include "limestone_exception_helper.h"
#include <rdma/rdma_receiver_base.h>
#include <rdma/rdma_receive_event.h>
#include "log_channel_limits.h"


namespace limestone::replication {

class channel_handler_base;
class log_channel_handler;

class replica_server {
public:
    using handler_fn = std::function<void(int, std::unique_ptr<replication_message>)>;
    static constexpr std::size_t max_log_channel_slots = log_channel_slots_limit;

    ~replica_server() = default;
    replica_server() = default;
    replica_server(replica_server const&) = delete;
    replica_server& operator=(replica_server const&) = delete;
    replica_server(replica_server&&) = delete;
    replica_server& operator=(replica_server&&) = delete;

    /**
     * Initialize internal datastore and metadata from the given filesystem path.
     * @param location filesystem path used for storing replication data and metadata.
     */
    void initialize(const boost::filesystem::path& location);

    /**
     * Bind and listen on the specified IPv4 address for incoming replication clients.
     * @return true if listener was successfully started; false on error.
     */
    [[nodiscard]] bool start_listener(const struct sockaddr_in& listen_addr);

    /**
     * Run the accept loop in the current thread, dispatching each new client connection
     * to handle_client(). Exits immediately when shutdown() is called.
     */
    void accept_loop();

    /**
     * Process a single client connection: configure socket options, receive exactly one
     * replication_message, invoke the registered handler (or return an error), then close.
     */
    void handle_client(int client_fd);

    /**
     * Register a handler function for a specific message_type_id.
     */
    void register_handler(message_type_id type, std::function<std::shared_ptr<channel_handler_base>(socket_io&)> factory) noexcept;

    /**
     * Clear all registered handlers. Intended for testing only.
     */
    void clear_handlers() noexcept;

    /**
     * Signal the accept_loop() to exit and close the listening socket.
     */
    void shutdown();

    /**
     * Get the underlying datastore instance.
     * @return the underlying datastore instance.
     */
    [[nodiscard]] limestone::api::datastore& get_datastore();

    /**
     * Get the location of the datastore.
     * @return the filesystem path for the datastore.
     */
    [[nodiscard]] boost::filesystem::path get_location() const noexcept;

    /**
     * @brief Attempts to mark the control channel as created.
     * @return true if the flag was not set and is now set successfully, false if it was already set.
     */
    [[nodiscard]] bool mark_control_channel_created() noexcept;

    /**
     * @brief Initialize RDMA receiver for replica side.
     * @param slot_count requested RDMA slot count.
     * @return initialization result.
     */
    enum class rdma_init_result {
        success,
        already_initialized,
        failed,
    };
    [[nodiscard]] rdma_init_result initialize_rdma_receiver(uint32_t slot_count);

    /**
     * @brief Get remote DMA address exposed by receiver.
     * @return optional DMA address if available.
     */
    [[nodiscard]] std::optional<std::uint64_t> get_rdma_dma_address() const noexcept;

    /**
     * @brief Accessor for log channel handler lookup.
     */
    [[nodiscard]] std::shared_ptr<class log_channel_handler> get_log_channel_handler(
        std::uint64_t id) const noexcept;

    /**
     * @brief Test hook to set a log channel handler into a slot.
     * @note Intended for testing only.
     */
    void set_log_channel_handler_for_test(
        std::uint64_t id, std::shared_ptr<class log_channel_handler> handler);

    /**
     * @brief RDMA receive handler entry point.
     * @param event RDMA receive event.
     */
    void on_rdma_receive(rdma_receive_event const& event);

    /**
     * @brief RDMA data event handler (exposed for testing).
     * @param event RDMA data event.
     */
    void handle_rdma_data_event(rdma_data_event const& event);
private:
    boost::filesystem::path location_;                      ///< filesystem path for datastore
    std::unordered_map<message_type_id, std::function<std::shared_ptr<channel_handler_base>(socket_io&)>> handler_factories_;
                                                            ///< factories for creating handlers
    std::unique_ptr<limestone::api::datastore> datastore_;  ///< underlying datastore instance
    std::mutex state_mutex_;                                ///< mutex for thread safety
    int event_fd_{-1};                                      ///< eventfd used to unblock poll()
    int sockfd_{-1};                                        ///< listening socket file descriptor
    std::atomic<bool> control_channel_created_{false};      ///< flag to indicate if control channel is created
    std::unique_ptr<rdma_receiver_base> rdma_receiver_; ///< RDMA receiver owned for process lifetime
    std::mutex rdma_init_mutex_{};                                      ///< Protect RDMA receiver initialization
    
    std::vector<std::future<void>> client_futures_;         ///< futures for client handling threads
    std::mutex futures_mutex_;                              ///< mutex for thread-safe access to client_futures_
    std::unordered_set<int> active_client_fds_;             ///< accepted client sockets currently handled
    std::mutex active_client_fds_mutex_;                    ///< protects active_client_fds_

    // Pending RDMA registrations until rdma_receiver_ is initialized.
    std::mutex pending_rdma_channels_mutex_{};
    std::vector<std::pair<std::uint64_t, int>> pending_rdma_channels_;  ///< store raw fds; converted to unique_fd on registration

    enum class poll_result {
        shutdown_event,
        client_event,
        poll_error,
    };

    poll_result poll_shutdown_event_or_client();
    void handle_shutdown_event();
    void accept_new_client();
    void cleanup_completed_futures();
    void register_active_client(int fd);
    void unregister_active_client(int fd) noexcept;

    class active_client_guard {
    public:
        active_client_guard(replica_server& server, int fd) noexcept;

        active_client_guard(active_client_guard const&) = delete;
        active_client_guard& operator=(active_client_guard const&) = delete;
        active_client_guard(active_client_guard&&) = delete;
        active_client_guard& operator=(active_client_guard&&) = delete;

        ~active_client_guard();

    private:
        replica_server* server_;
        int fd_;
    };

    /**
     * @brief Perform LOG_CHANNEL_CREATE specific setup for the newly created handler.
     *
     * Validates the channel id, registers the RDMA ACK channel (or defers it),
     * and stores the handler in the log_channel_handlers_ slot.
     *
     * @param msg  The received LOG_CHANNEL_CREATE message.
     * @param handler The handler created by the factory for this connection.
     * @param client_fd The accepted client file descriptor.
     */
    void setup_log_channel_handler(
        replication_message& msg,
        std::shared_ptr<channel_handler_base> const& handler,
        int client_fd);

    // Use fixed-size arrays to avoid reallocations and allow lock-per-slot access.
    std::array<std::shared_ptr<class log_channel_handler>, max_log_channel_slots>
        log_channel_handlers_{};
    mutable std::array<std::mutex, max_log_channel_slots> log_channel_slot_mutexes_{};
};

} // namespace limestone::replication
