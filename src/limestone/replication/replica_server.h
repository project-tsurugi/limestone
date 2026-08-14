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
#include <ostream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <mutex>
#include <vector>
#include <boost/filesystem.hpp>
#include "replication_message.h"
#include <limestone/api/datastore.h>
#include "limestone_exception_helper.h"
#include <rdma/handshake_client_base.h>
#include <rdma/rdma_receiver_base.h>
#include <rdma/rdma_receive_event.h>
#include <rdma/rdma_sender_base.h>
#include "log_channel_limits.h"


namespace limestone::replication {

class channel_handler_base;
class log_channel_handler;
class rdma_log_channel_receiver;

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
    void register_handler(message_type_id type, std::function<std::shared_ptr<channel_handler_base>(replication_message_io&)> factory) noexcept;

    /**
     * Clear all registered handlers. Intended for testing only.
     */
    void clear_handlers() noexcept;

    /**
     * @brief Shut down the server and release the datastore.
     *
     * Signal accept_loop() to exit, close the listening socket, and release the
     * datastore (including its manifest lock). To run the server again
     * afterwards, call initialize() before start_listener().
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
     * @brief Initialize the replica-side RDMA stack.
     *
     * Creates the data receiver and the ACK sender, calling each instance's
     * initialize() so the data receiver exposes its DMA address and the ACK
     * sender targets the leader's ACK receive buffer. The pair is left in the
     * SETUP phase; finalize_channel_setup_with_sender / finalize_channel_setup
     * are not invoked here (handled in a later step).
     *
     * @param slot_count requested RDMA slot count.
     * @param leader_ack_dma_address DMA address of the leader's ACK receive buffer.
     * @return initialization result.
     */
    enum class rdma_init_result {
        success,
        already_initialized,
        failed,
    };
    [[nodiscard]] rdma_init_result initialize_rdma(
        uint32_t slot_count, std::uint64_t leader_ack_dma_address);

    /**
     * @brief Finalize the replica-side RDMA stack.
     *
     * Binds the data receiver to the ACK sender via
     * rdma_receiver_base::finalize_channel_setup_with_sender, transitioning the
     * receiver into the TRANSFER phase so that incoming data frames can be
     * acknowledged via RDMA.
     *
     * @return finalize result.
     */
    enum class rdma_finalize_result {
        success,
        not_initialized,
        failed,
    };
    [[nodiscard]] rdma_finalize_result finalize_rdma();

    /**
     * @brief Get DMA address exposed by the data receiver.
     * @return optional DMA address if available.
     */
    [[nodiscard]] std::optional<std::uint64_t> get_rdma_dma_address() const noexcept;

    /**
     * @brief Establishes the RDMA replication session as the accept side.
     *
     * Registers on the handshake daemon and waits for the master's start,
     * validates the start payload, initializes the RDMA stack, responds with
     * the replica DMA address, registers a log channel receiver for each data
     * channel (ids 0 .. channel_count - 1), and completes the handshake.
     * The control channel id is not registered as a log channel.
     *
     * @param handshake_socket_path Filesystem path of the handshake daemon socket.
     * @param service_id Handshake service id shared with the master.
     * @return true when the session is established; false on failure.
     * @note One-shot: on failure, log channel receivers already registered during
     *       this call are not rolled back, so this instance must not be reused
     *       for a retry.
     */
    [[nodiscard]] bool establish_rdma_session(
        std::string const& handshake_socket_path, std::uint64_t service_id);

    /**
     * @brief Accessor for log channel handler lookup.
     */
    [[nodiscard]] std::shared_ptr<class log_channel_handler> get_log_channel_handler(
        std::uint64_t id) const noexcept;

    /**
     * @brief Registration result of an RDMA data frame receiving end
     *        (rdma_log_channel_receiver).
     */
    enum class register_rdma_receiver_result {
        success,
        invalid_channel_id,
        already_registered,
    };

    /**
     * @brief Returns a short string representation of a register_rdma_receiver_result value.
     */
    [[nodiscard]] static std::string_view to_string_view(register_rdma_receiver_result result) noexcept;

    /**
     * @brief Registers an rdma_log_channel_receiver for the given channel id.
     *
     * Creates a replica-side log channel via datastore::create_channel() and
     * stores an rdma_log_channel_receiver bound to it in the channel_id slot,
     * so that subsequent RDMA frames for that id are dispatched to the
     * receiver. Used by the RDMA modes that skip the TCP LOG_CHANNEL_CREATE
     * handshake (the TCP-less establishment and the RDMA_FINALIZE handling).
     *
     * @param channel_id Channel id assigned by the master.
     * @return Registration result.
     */
    register_rdma_receiver_result register_rdma_log_channel_receiver(std::uint64_t channel_id);

    /**
     * @brief Returns the rdma_log_channel_receiver for the given channel id.
     * @param id Channel id.
     * @return The registered receiver, or nullptr when unregistered or out of range.
     */
    [[nodiscard]] std::shared_ptr<rdma_log_channel_receiver> get_rdma_log_channel_receiver(
        std::uint64_t id) const noexcept;

    /**
     * @brief Test hook to set an rdma_log_channel_receiver into a slot directly.
     * @note Intended for testing only.
     */
    void set_rdma_log_channel_receiver_for_test(
        std::uint64_t id, std::shared_ptr<rdma_log_channel_receiver> receiver);

    /**
     * @brief Test hook to inject the RDMA data receiver instance.
     * @note Test-only; do not use in production code.
     */
    void set_rdma_receiver_for_test(std::unique_ptr<rdma_receiver_base> receiver) noexcept;

    /**
     * @brief Test hook to inject the RDMA ACK sender instance.
     * @note Test-only; do not use in production code.
     */
    void set_ack_sender_for_test(std::unique_ptr<rdma_sender_base> sender) noexcept;

    /**
     * @brief Factory function used by initialize_rdma() to create the data receiver.
     *
     * Tests can install a custom factory via set_rdma_receiver_factory_for_test() to
     * substitute a stub instance for the real make_rdma_data_receiver() result.
     *
     * @param slot_count requested RDMA slot count, forwarded from initialize_rdma().
     * @return Newly created rdma_receiver_base instance, transferred to the caller.
     */
    using rdma_receiver_factory =
        std::function<std::unique_ptr<rdma_receiver_base>(std::uint32_t slot_count)>;

    /**
     * @brief Factory function used by initialize_rdma() to create the ACK sender.
     *
     * Tests can install a custom factory via set_ack_sender_factory_for_test() to
     * substitute a stub instance for the real make_rdma_ack_sender() result.
     *
     * @param slot_count requested RDMA slot count, forwarded from initialize_rdma().
     * @return Newly created rdma_sender_base instance, transferred to the caller.
     */
    using rdma_sender_factory =
        std::function<std::unique_ptr<rdma_sender_base>(std::uint32_t slot_count)>;

    /**
     * @brief Test hook to override the factory used by initialize_rdma() for the data receiver.
     *
     * When unset, initialize_rdma() falls back to make_rdma_data_receiver(). Tests can install a
     * factory that returns a stub instance to bypass the vendor RDMA mock.
     *
     * @note Test-only; do not use in production code.
     */
    void set_rdma_receiver_factory_for_test(rdma_receiver_factory factory) noexcept;

    /**
     * @brief Test hook to override the factory used by initialize_rdma() for the ACK sender.
     * @note Test-only; do not use in production code. See set_rdma_receiver_factory_for_test.
     */
    void set_ack_sender_factory_for_test(rdma_sender_factory factory) noexcept;

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
    /**
     * @brief Handles a frame received on the RDMA control channel.
     * @param event RDMA data event whose channel id equals the control channel id.
     */
    void handle_rdma_control_event(rdma_data_event const& event);

    boost::filesystem::path location_;                      ///< filesystem path for datastore
    std::unordered_map<message_type_id, std::function<std::shared_ptr<channel_handler_base>(replication_message_io&)>> handler_factories_;
                                                            ///< factories for creating handlers
    std::unique_ptr<limestone::api::datastore> datastore_;  ///< underlying datastore instance
    std::mutex state_mutex_;                                ///< mutex for thread safety
    int event_fd_{-1};                                      ///< eventfd used to unblock poll()
    int sockfd_{-1};                                        ///< listening socket file descriptor
    std::atomic<bool> control_channel_created_{false};      ///< flag to indicate if control channel is created
    std::unique_ptr<rdma_receiver_base> rdma_receiver_; ///< RDMA data receiver owned for process lifetime
    std::unique_ptr<rdma_sender_base> ack_sender_;      ///< RDMA ACK sender targeting the leader's ACK buffer
    std::mutex rdma_init_mutex_{};                      ///< Protect RDMA stack initialization
    rdma_receiver_factory rdma_receiver_factory_for_test_{}; ///< Optional test override for receiver creation
    rdma_sender_factory ack_sender_factory_for_test_{};      ///< Optional test override for ACK sender creation

    // Control channel id taken from the handshake start payload; -1 until the RDMA
    // session is established. Atomic because the transport's receive thread reads it
    // while the establishment thread publishes it.
    std::atomic<std::int32_t> rdma_control_channel_id_{-1};
    std::uint16_t control_next_sequence_number_{0};     ///< Next expected control frame sequence number
    std::mutex control_channel_mutex_{};                ///< Serializes control frame processing
    
    std::vector<std::future<void>> client_futures_;         ///< futures for client handling threads
    std::mutex futures_mutex_;                              ///< mutex for thread-safe access to client_futures_
    std::unordered_set<int> active_client_fds_;             ///< accepted client sockets currently handled
    std::mutex active_client_fds_mutex_;                    ///< protects active_client_fds_

    enum class poll_result {
        shutdown_event,
        client_event,
        poll_error,
    };

    poll_result poll_shutdown_event_or_client();
    void handle_shutdown_event();

    /**
     * @brief Sends a rejection response on the handshake and logs the reason.
     * @param acceptor Acceptor whose handshake is being rejected.
     * @param reason Rejection reason forwarded to the master.
     */
    void send_rdma_session_rejection(
        handshake_acceptor_base& acceptor, std::string const& reason);

    /**
     * @brief Shuts down and releases the RDMA receiver and the ACK sender.
     */
    void release_rdma_stack() noexcept;
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
     * Validates the channel id and stores the handler in the log_channel_handlers_ slot.
     *
     * @param msg  The received LOG_CHANNEL_CREATE message.
     * @param handler The handler created by the factory for this connection.
     */
    void setup_log_channel_handler(
        replication_message& msg,
        std::shared_ptr<channel_handler_base> const& handler);

    // Use fixed-size arrays to avoid reallocations and allow lock-per-slot access.
    // log_channel_handlers_ is the handler registry for the TCP mode and
    // rdma_log_channel_receivers_ is the frame-dispatch registry for the RDMA
    // modes; both are never populated in one configuration.
    std::array<std::shared_ptr<class log_channel_handler>, max_log_channel_slots>
        log_channel_handlers_{};
    std::array<std::shared_ptr<rdma_log_channel_receiver>, max_log_channel_slots>
        rdma_log_channel_receivers_{};
    mutable std::array<std::mutex, max_log_channel_slots> log_channel_slot_mutexes_{};
};

/**
 * @brief Stream insertion operator for replica_server::register_rdma_receiver_result.
 */
std::ostream& operator<<(std::ostream& out, replica_server::register_rdma_receiver_result result);

} // namespace limestone::replication
