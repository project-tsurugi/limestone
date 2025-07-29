file(GLOB SOURCES
        "limestone/*.cpp"
        "limestone/replication/*.cpp"
        "limestone/partitioned_cursor/*.cpp"
        "limestone/grpc/service/*.cpp"
        "limestone/grpc/client/*.cpp"
)

# Generate protobuf and gRPC files
# Use the protoc and grpc_cpp_plugin from the built gRPC
set(_PROTOBUF_PROTOC $<TARGET_FILE:protoc>)
set(_GRPC_CPP_PLUGIN_EXECUTABLE $<TARGET_FILE:grpc_cpp_plugin>)

# Automatically find all .proto files in limestone/grpc/proto/
file(GLOB PROTO_FILES "limestone/grpc/proto/*.proto")
set(PROTO_SRCS)
set(GRPC_SRCS)

foreach(PROTO_FILE ${PROTO_FILES})
    get_filename_component(PROTO_NAME ${PROTO_FILE} NAME_WE)
    get_filename_component(PROTO_PATH ${PROTO_FILE} PATH)
    set(PROTO_SRC "${CMAKE_CURRENT_BINARY_DIR}/${PROTO_NAME}.pb.cc")
    set(GRPC_SRC "${CMAKE_CURRENT_BINARY_DIR}/${PROTO_NAME}.grpc.pb.cc")
    list(APPEND PROTO_SRCS ${PROTO_SRC})
    list(APPEND GRPC_SRCS ${GRPC_SRC})
    
    add_custom_command(
        OUTPUT ${PROTO_SRC} ${CMAKE_CURRENT_BINARY_DIR}/${PROTO_NAME}.pb.h 
               ${GRPC_SRC} ${CMAKE_CURRENT_BINARY_DIR}/${PROTO_NAME}.grpc.pb.h
        COMMAND ${_PROTOBUF_PROTOC}
            --cpp_out=${CMAKE_CURRENT_BINARY_DIR}
            --grpc_out=${CMAKE_CURRENT_BINARY_DIR}
            --plugin=protoc-gen-grpc=${_GRPC_CPP_PLUGIN_EXECUTABLE}
            --proto_path=${CMAKE_CURRENT_SOURCE_DIR}/limestone/grpc/proto
            ${PROTO_FILE}
        DEPENDS ${PROTO_FILE} protoc grpc_cpp_plugin
        COMMENT "Generating protobuf and gRPC files for ${PROTO_FILE}"
    )
endforeach()

# Add generated files to sources
list(APPEND SOURCES ${PROTO_SRCS} ${GRPC_SRCS})

add_library(${package_name}
        ${SOURCES}
)

set_target_properties(${package_name}
        PROPERTIES
                INSTALL_RPATH "\$ORIGIN"
                OUTPUT_NAME ${export_name}
)
target_include_directories(${package_name}
        PRIVATE ./limestone
        PRIVATE ${CMAKE_CURRENT_BINARY_DIR}
)

if(${RECOVERY_SORTER_KVSLIB_UPPERCASE} STREQUAL "ROCKSDB")
    set(sort_lib RocksDB::RocksDB)
    target_compile_options(${package_name} PUBLIC -DSORT_METHOD_USE_ROCKSDB)
else()
    set(sort_lib leveldb)
endif()
if(RECOVERY_SORTER_PUT_ONLY)
    target_compile_options(${package_name} PRIVATE -DSORT_METHOD_PUT_ONLY)
endif()

target_link_libraries(${package_name}
        PUBLIC limestone-api
        PRIVATE Boost::boost
        PRIVATE Boost::filesystem
        PRIVATE Boost::thread
        PRIVATE Boost::container
        PRIVATE glog::glog
        PRIVATE ${sort_lib}
        PRIVATE nlohmann_json::nlohmann_json
        PRIVATE grpc++
        PRIVATE grpc
        PRIVATE libprotobuf
)

if (ENABLE_ALTIMETER)
    target_link_libraries(${package_name}
        PRIVATE altimeter
    )
endif()

set_compile_options(${package_name})

install_custom(${package_name} ${export_name})

# for tests
add_library(limestone-impl INTERFACE)

target_include_directories(limestone-impl
        INTERFACE .
        )

target_link_libraries(limestone-impl
        INTERFACE ${package_name}
        INTERFACE Boost::boost
        INTERFACE Boost::filesystem
        INTERFACE Boost::thread
        INTERFACE Boost::container
        INTERFACE glog::glog
        INTERFACE ${sort_lib}
        INTERFACE nlohmann_json::nlohmann_json
)

# utils
file(GLOB DBLOGUTIL_SOURCES
        "limestone/dblogutil/*.cpp"
)

add_executable(dblogutil ${DBLOGUTIL_SOURCES})
target_include_directories(dblogutil
        PRIVATE ./limestone
        )
target_link_libraries(dblogutil PRIVATE limestone-impl PRIVATE glog::glog gflags::gflags)
set_target_properties(dblogutil
        PROPERTIES
                INSTALL_RPATH "\$ORIGIN/../${CMAKE_INSTALL_LIBDIR}"
                RUNTIME_OUTPUT_NAME "tglogutil"
)
install_custom(dblogutil dblogutil)

# replica command
file(GLOB REPLICA_SOURCES
        "limestone/replica/*.cpp"
)

add_executable(replica ${REPLICA_SOURCES})
target_include_directories(replica
        PRIVATE ./limestone
)
target_link_libraries(replica PRIVATE limestone-impl PRIVATE glog::glog)
set_target_properties(replica
        PROPERTIES
                INSTALL_RPATH "\$ORIGIN/../${CMAKE_INSTALL_LIBDIR}"
                RUNTIME_OUTPUT_NAME "tgreplica"
)
if (INSTALL_EXPERIMENTAL_TOOLS)
    install_custom(replica replica)
endif()
