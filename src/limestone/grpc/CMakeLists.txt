# limestone gRPC implementation

# Find required packages for protobuf and gRPC code generation
find_program(PROTOC protoc REQUIRED)
find_program(GRPC_CPP_PLUGIN grpc_cpp_plugin REQUIRED)

# Define proto files
set(PROTO_FILES
    proto/echo_service.proto
)

# Generate protobuf and gRPC source files
set(PROTO_SRCS)
set(PROTO_HDRS)
set(GRPC_SRCS)
set(GRPC_HDRS)

foreach(PROTO_FILE ${PROTO_FILES})
    get_filename_component(PROTO_NAME ${PROTO_FILE} NAME_WE)
    get_filename_component(PROTO_PATH ${PROTO_FILE} PATH)
    
    # Generated files paths (directly in build directory)
    set(PROTO_SRC "${CMAKE_CURRENT_BINARY_DIR}/${PROTO_NAME}.pb.cc")
    set(PROTO_HDR "${CMAKE_CURRENT_BINARY_DIR}/${PROTO_NAME}.pb.h")
    set(GRPC_SRC "${CMAKE_CURRENT_BINARY_DIR}/${PROTO_NAME}.grpc.pb.cc")
    set(GRPC_HDR "${CMAKE_CURRENT_BINARY_DIR}/${PROTO_NAME}.grpc.pb.h")
    
    list(APPEND PROTO_SRCS ${PROTO_SRC})
    list(APPEND PROTO_HDRS ${PROTO_HDR})
    list(APPEND GRPC_SRCS ${GRPC_SRC})
    list(APPEND GRPC_HDRS ${GRPC_HDR})
    
    # Custom command to generate protobuf and gRPC files
    add_custom_command(
        OUTPUT ${PROTO_SRC} ${PROTO_HDR} ${GRPC_SRC} ${GRPC_HDR}
        COMMAND ${PROTOC}
            --cpp_out=${CMAKE_CURRENT_BINARY_DIR}
            --grpc_out=${CMAKE_CURRENT_BINARY_DIR}
            --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN}
            --proto_path=${CMAKE_CURRENT_SOURCE_DIR}/proto
            ${CMAKE_CURRENT_SOURCE_DIR}/${PROTO_FILE}
        DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/${PROTO_FILE}
        COMMENT "Generating protobuf and gRPC files for ${PROTO_FILE}"
    )
endforeach()

# Create limestone-grpc library
add_library(limestone-grpc
    # Generated protobuf/gRPC files
    ${PROTO_SRCS}
    ${GRPC_SRCS}
    
    # Service implementations
    service/echo_service_impl.cpp
    
    # Client implementations
    client/echo_client.cpp
)

# Include directories for limestone-grpc
target_include_directories(limestone-grpc
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${CMAKE_CURRENT_BINARY_DIR}
        ${PROJECT_SOURCE_DIR}/include
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../../..  # for limestone headers
)

# Link libraries
target_link_libraries(limestone-grpc
    PUBLIC
        ${GRPC_LIBRARIES}
        ${PROTOBUF_LIBRARIES}
        grpc++
        grpc
        protobuf
    PRIVATE
        limestone
        glog::glog
)

# Set C++ standard
target_compile_features(limestone-grpc PUBLIC cxx_std_17)

# Install headers
install(
    FILES
        service/echo_service_impl.h
        client/echo_client.h
    DESTINATION
        ${CMAKE_INSTALL_INCLUDEDIR}/limestone/grpc
)

# Install generated headers
install(
    FILES
        ${PROTO_HDRS}
        ${GRPC_HDRS}
    DESTINATION
        ${CMAKE_INSTALL_INCLUDEDIR}/limestone/grpc/proto
)
