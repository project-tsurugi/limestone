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
#include <rdma/rdma_comm_receiver.h>

#include <rdma_comm/unique_fd.h>

namespace limestone::replication {

namespace {

/// @brief Convert rdma_comm frame header fields to the limestone-internal type.
rdma_frame_header convert_header(rdma::communication::rdma_frame_header const& src) noexcept {
    rdma_frame_header dst{};
    dst.version         = src.version;
    dst.flags           = src.flags;
    dst.sequence_number = src.sequence_number;
    dst.channel_id      = src.channel_id;
    dst.payload_size    = src.payload_size;
    return dst;
}

/// @brief Convert an rdma_comm receive event to the limestone-internal variant.
rdma_receive_event convert_event(rdma::communication::rdma_receive_event const& src) {
    if (auto const* data = std::get_if<rdma::communication::rdma_receive_data_event>(&src)) {
        rdma_data_event dst{};
        dst.header  = convert_header(data->header);
        dst.payload = data->payload;
        return dst;
    }
    auto const* err = std::get_if<rdma::communication::rdma_receive_error_event>(&src);
    rdma_error_event dst{};
    if (err->header.has_value()) {
        dst.header = convert_header(*err->header);
    }
    dst.payload       = err->payload;
    dst.error_message = err->error_message;
    return dst;
}

} // namespace

rdma_comm_receiver::rdma_comm_receiver(rdma::communication::rdma_config config)
    : receiver_(std::move(config))
{}

rdma_receiver_base::operation_result rdma_comm_receiver::initialize(
        rdma_receive_handler handler) noexcept {
    auto adaptor = [h = std::move(handler)](rdma::communication::rdma_receive_event const& ev) {
        h(convert_event(ev));
    };
    auto r = receiver_.initialize(std::move(adaptor));
    return {r.success, r.error_message};
}

rdma_receiver_base::operation_result rdma_comm_receiver::shutdown() noexcept {
    auto r = receiver_.shutdown();
    return {r.success, r.error_message};
}

rdma_receiver_base::operation_result rdma_comm_receiver::register_channel(
        std::uint16_t channel_id,
        int           ack_socket) noexcept {
    auto r = receiver_.register_channel(
        channel_id,
        rdma::communication::unique_fd{ack_socket});
    return {r.success, r.error_message};
}

std::optional<std::uint64_t> rdma_comm_receiver::get_dma_address() const noexcept {
    return receiver_.get_dma_address();
}

} // namespace limestone::replication
