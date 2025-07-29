# grpc_server CMakeLists.txt
# Use limestone-grpc library implementation

add_executable(grpc_server grpc_server.cpp)
target_include_directories(grpc_server PRIVATE 
    ${PROJECT_SOURCE_DIR}/src  # For limestone headers
    ${CMAKE_CURRENT_BINARY_DIR}/../../src  # For generated protobuf headers
)

target_link_libraries(grpc_server PRIVATE
    limestone-impl  # This includes limestone library with gRPC
    glog::glog
    grpc++
    grpc
    libprotobuf
)

# Add grpc_client
add_executable(grpc_client grpc_client.cpp)
target_include_directories(grpc_client PRIVATE 
    ${PROJECT_SOURCE_DIR}/src  # For limestone headers
    ${CMAKE_CURRENT_BINARY_DIR}/../../src  # For generated protobuf headers
)

target_link_libraries(grpc_client PRIVATE
    limestone-impl  # This includes limestone library with gRPC
    glog::glog
    grpc++
    grpc
    libprotobuf
)
