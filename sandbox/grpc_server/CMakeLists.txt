# grpc_server CMakeLists.txt
# Dependent libraries are already searched in the root CMakeLists.txt

# Find Protocol Buffers tools
find_program(PROTOC protoc REQUIRED)
find_program(GRPC_CPP_PLUGIN grpc_cpp_plugin REQUIRED)

# Output directory for generated files (build directory)
set(PROTO_SRCS 
    ${CMAKE_CURRENT_BINARY_DIR}/echo.pb.cc
    ${CMAKE_CURRENT_BINARY_DIR}/echo.grpc.pb.cc
)
set(PROTO_HDRS 
    ${CMAKE_CURRENT_BINARY_DIR}/echo.pb.h
    ${CMAKE_CURRENT_BINARY_DIR}/echo.grpc.pb.h
)

# Generate Protocol Buffers files
add_custom_command(
    OUTPUT ${PROTO_SRCS} ${PROTO_HDRS}
    COMMAND ${PROTOC} 
        --cpp_out=${CMAKE_CURRENT_BINARY_DIR}
        --grpc_out=${CMAKE_CURRENT_BINARY_DIR}
        --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN}
        --proto_path=${CMAKE_CURRENT_SOURCE_DIR}
        ${CMAKE_CURRENT_SOURCE_DIR}/echo.proto
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/echo.proto
    COMMENT "Generating Protocol Buffers and gRPC files"
)

add_executable(grpc_server 
    grpc_server.cpp 
    ${PROTO_SRCS}
)
target_include_directories(grpc_server PRIVATE 
    ${GRPC_INCLUDE_DIRS} 
    ${PROTOBUF_INCLUDE_DIRS} 
    ${CMAKE_CURRENT_BINARY_DIR}  # Path to generated header files
)
target_link_libraries(grpc_server PRIVATE
    ${GRPC_LIBRARIES}
    ${PROTOBUF_LIBRARIES}
    grpc++ grpc
    protobuf
    absl_synchronization
    absl_time
    absl_strings
    absl_status
    absl_hash
    absl_cord
    absl_raw_logging_internal
    absl_log_severity
    absl_throw_delegate
)

# Add grpc_client
add_executable(grpc_client 
    grpc_client.cpp 
    ${PROTO_SRCS}
)
target_include_directories(grpc_client PRIVATE 
    ${GRPC_INCLUDE_DIRS} 
    ${PROTOBUF_INCLUDE_DIRS} 
    ${CMAKE_CURRENT_BINARY_DIR}  # Path to generated header files
)
target_link_libraries(grpc_client PRIVATE
    ${GRPC_LIBRARIES}
    ${PROTOBUF_LIBRARIES}
    grpc++ grpc
    protobuf
    absl_synchronization
    absl_time
    absl_strings
    absl_status
    absl_hash
    absl_cord
    absl_raw_logging_internal
    absl_log_severity
    absl_throw_delegate
)

# Client with AddressSanitizer disabled
target_compile_options(grpc_client PRIVATE -fno-sanitize=address)
target_link_options(grpc_client PRIVATE -fno-sanitize=address)

# Safe client with AddressSanitizer disabled
add_executable(grpc_client_safe 
    grpc_client.cpp 
    ${PROTO_SRCS}
)
target_include_directories(grpc_client_safe PRIVATE 
    ${GRPC_INCLUDE_DIRS} 
    ${PROTOBUF_INCLUDE_DIRS} 
    ${CMAKE_CURRENT_BINARY_DIR}  # Path to generated header files
)
target_compile_options(grpc_client_safe PRIVATE -O2 -DNDEBUG)
target_link_libraries(grpc_client_safe PRIVATE
    ${GRPC_LIBRARIES}
    ${PROTOBUF_LIBRARIES}
    grpc++ grpc
    protobuf
    absl_synchronization
    absl_time
    absl_strings
    absl_status
    absl_hash
    absl_cord
    absl_raw_logging_internal
    absl_log_severity
    absl_throw_delegate
)
