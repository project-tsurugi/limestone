#pragma once

#include <grpcpp/grpcpp.h>
#include "server_streaming_sample.pb.h"
#include "server_streaming_sample.grpc.pb.h"
#include <cstdint>
#include <random>


namespace limestone::grpc::service {

class FileSizeServiceImpl final : public limestone::grpc::proto::FileSizeService::Service {
public:
    ::grpc::Status GetFileSize(::grpc::ServerContext* context,
                               ::grpc::ServerReader<limestone::grpc::proto::FileChunk>* reader,
                               limestone::grpc::proto::FileSizeResponse* response) override;
};

class RandomBytesServiceImpl final : public limestone::grpc::proto::RandomBytesService::Service {
public:
    ::grpc::Status GenerateRandomBytes(::grpc::ServerContext* context,
                                       const limestone::grpc::proto::RandomBytesRequest* request,
                                       ::grpc::ServerWriter<limestone::grpc::proto::RandomBytesChunk>* writer) override;
};

} // namespace limestone::grpc::service
