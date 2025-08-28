#pragma once

namespace limestone::grpc::service {

constexpr int64_t session_timeout_seconds = 30;

// Chunk size for gRPC backup object transfer (1MB)
constexpr std::size_t backup_object_chunk_size = 1024 * 1024;

} // namespace limestone::grpc::service
