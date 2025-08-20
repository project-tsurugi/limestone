#pragma once

#include <grpcpp/grpcpp.h>
#include "server_streaming_sample.pb.h"
#include "server_streaming_sample.grpc.pb.h"
#include <cstdint>
#include <random>


namespace limestone::grpc::service {

using FileSizeService = limestone::grpc::proto::FileSizeService;
using FileChunk = limestone::grpc::proto::FileChunk;
using FileSizeResponse = limestone::grpc::proto::FileSizeResponse;
using RandomBytesService = limestone::grpc::proto::RandomBytesService;
using RandomBytesRequest = limestone::grpc::proto::RandomBytesRequest;
using RandomBytesChunk = limestone::grpc::proto::RandomBytesChunk;

class FileSizeServiceImpl final : public FileSizeService::Service {
public:
    ::grpc::Status GetFileSize(::grpc::ServerContext* context,
                               ::grpc::ServerReader<FileChunk>* reader,
                               FileSizeResponse* response) override;
};

class RandomBytesServiceImpl final : public RandomBytesService::Service {
public:
    ::grpc::Status GenerateRandomBytes(::grpc::ServerContext* context,
                                       const RandomBytesRequest* request,
                                       ::grpc::ServerWriter<RandomBytesChunk>* writer) override;
};

} // namespace limestone::grpc::service
