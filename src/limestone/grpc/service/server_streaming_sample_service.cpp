#include "server_streaming_sample_service.h"

#include <vector>
#include <algorithm>
#include <glog/logging.h>


namespace limestone::grpc::service {

using std::chrono::steady_clock;
using std::chrono::milliseconds;
using limestone::grpc::proto::FileChunk;
using limestone::grpc::proto::FileSizeResponse;
using limestone::grpc::proto::RandomBytesRequest;
using limestone::grpc::proto::RandomBytesChunk;

::grpc::Status FileSizeServiceImpl::GetFileSize(
    ::grpc::ServerContext* /*context*/,
    ::grpc::ServerReader<FileChunk>* reader,
    FileSizeResponse* response)
{
    LOG(INFO) << "[GetFileSize] start";
    auto t0 = steady_clock::now();

    int64_t total_size = 0;
    FileChunk chunk;
    while (reader->Read(&chunk)) {
        total_size += static_cast<int64_t>(chunk.data().size());
    }
    response->set_size(total_size);

    auto t1 = steady_clock::now();
    auto ms = std::chrono::duration_cast<milliseconds>(t1 - t0).count();
    LOG(INFO) << "[GetFileSize] end: elapsed " << ms << " ms";
    return ::grpc::Status::OK;
}

::grpc::Status RandomBytesServiceImpl::GenerateRandomBytes(
    ::grpc::ServerContext* /*context*/,
    const RandomBytesRequest* request,
    ::grpc::ServerWriter<RandomBytesChunk>* writer)
{
    LOG(INFO) << "[GenerateRandomBytes] start";
    auto t0 = steady_clock::now();

    int64_t size = request->size();
    const int64_t chunk_size = 32LL * 1024 * 1024; // 32MB

    auto state =  static_cast<uint32_t>(0x12345678) ^ static_cast<uint32_t>(size);

    // First, create the entire buffer and fill it with random numbers

    std::string all_data(size, '\0');
    for (int64_t i = 0; i < size; ++i) {
        state ^= state << static_cast<uint32_t>(13);
        state ^= state >> static_cast<uint32_t>(17);
        state ^= state << static_cast<uint32_t>(5);
        all_data[i] = static_cast<char>(state & static_cast<uint32_t>(0xFF));
    }

    auto t1 = steady_clock::now();
    auto ms_gen = std::chrono::duration_cast<milliseconds>(t1 - t0).count();
    LOG(INFO) << "[GenerateRandomBytes] data generated: elapsed " << ms_gen << " ms";

    int64_t sent = 0;
    while (sent < size) {
        int64_t current_chunk_size = std::min(chunk_size, size - sent);
    RandomBytesChunk chunk;
        chunk.set_data(all_data.substr(sent, current_chunk_size));
        writer->Write(chunk);
        sent += current_chunk_size;
    }

    auto t2 = steady_clock::now();
    auto ms_total = std::chrono::duration_cast<milliseconds>(t2 - t0).count();
    LOG(INFO) << "[GenerateRandomBytes] end: elapsed " << ms_total << " ms";
    return ::grpc::Status::OK;
}

} // namespace limestone::grpc::service
