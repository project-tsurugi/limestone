
#pragma once

namespace limestone::grpc::service {

constexpr int64_t session_timeout_seconds = 30;

// Chunk size for gRPC backup object transfer (1MB)
constexpr std::size_t backup_object_chunk_size = static_cast<std::size_t>(1024) * 1024;

// Default timeout (ms) for gRPC requests
constexpr int grpc_timeout_ms = 5000;

// Default interval (ms) for keepalive_session during remote backup
constexpr int keepalive_interval_ms = 1000;

} // namespace limestone::grpc::service
