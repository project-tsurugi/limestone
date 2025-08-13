# limestone gRPC Implementation

This directory contains the gRPC implementation for limestone datastore.

## Directory Structure

```
src/limestone/grpc/
├── README.md           # This file (directory overview and conventions)
├── proto/              # Protocol Buffer definitions
├── service/            # gRPC service implementations
└── client/             # gRPC client implementations
```

## Naming Conventions

### C++ Namespaces

- `limestone::grpc::service` - gRPC service implementations
- `limestone::grpc::client` - gRPC client implementations  
- `limestone::grpc::proto` - Protocol Buffer generated code (if needed)

### Protocol Buffer Package

- `limestone.grpc` - Package name for all protobuf definitions

### Example Usage

**Note**: The `echo_service` and `echo_client` implementations are for testing and development purposes only. They are not intended for production use.

**Protocol Buffer Definition (echo_service.proto):**
```protobuf
syntax = "proto3";

package limestone.grpc;

option java_package = "com.tsurugi.limestone.grpc";
option csharp_namespace = "Limestone.Grpc";

service EchoService {
  rpc Echo(EchoRequest) returns (EchoResponse);
}

message EchoRequest {
  string message = 1;
}

message EchoResponse {
  string message = 1;
}
```

**C++ Service Implementation (echo_service_impl):**
```cpp
namespace limestone::grpc::service {
  class echo_service_impl : public limestone::grpc::EchoService::Service {
  public:
    grpc::Status Echo(grpc::ServerContext* context,
                     const limestone::grpc::EchoRequest* request,
                     limestone::grpc::EchoResponse* response) override;
  };
}
```

**C++ Client Implementation (echo_client):**
```cpp
namespace limestone::grpc::client {
  class echo_client {
  public:
    echo_client(std::shared_ptr<grpc::Channel> channel);
    std::string echo(const std::string& message);
  private:
    std::unique_ptr<limestone::grpc::EchoService::Stub> stub_;
  };
}
```

## Development Guidelines

1. **Protocol Definitions**: Place all `.proto` files in the `proto/` subdirectory
2. **Service Implementation**: Implement gRPC services in the `service/` subdirectory
3. **Client Implementation**: Implement gRPC clients in the `client/` subdirectory
4. **Naming**: Follow snake_case for file names and variables, PascalCase for types
5. **Documentation**: Include English comments for all public interfaces
6. **Testing**: Create corresponding test files for each implementation

## Build Integration

The gRPC components are integrated into the main limestone build system through CMake.
Protocol buffer files are automatically compiled during the build process.
