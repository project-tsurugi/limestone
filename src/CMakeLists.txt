file(GLOB SOURCES
        "limestone/*.cpp"
        "limestone/replication/*.cpp"
        "limestone/partitioned_cursor/*.cpp"
        "limestone/grpc/service/*.cpp"
        "limestone/grpc/backend/*.cpp"
        "limestone/grpc/client/*.cpp"
)

# Generate protobuf and gRPC files
find_program(PROTOC protoc REQUIRED)
find_program(GRPC_CPP_PLUGIN grpc_cpp_plugin REQUIRED)

# Automatically find all .proto files in limestone/grpc/proto/
file(GLOB PROTO_FILES "limestone/grpc/proto/*.proto")
set(PROTO_SRCS)
set(PROTO_HDRS)
set(GRPC_SRCS)
set(GRPC_HDRS)

foreach(PROTO_FILE ${PROTO_FILES})
    get_filename_component(PROTO_NAME ${PROTO_FILE} NAME_WE)
    set(PROTO_SRC "${CMAKE_CURRENT_BINARY_DIR}/${PROTO_NAME}.pb.cc")
    set(PROTO_HDR "${CMAKE_CURRENT_BINARY_DIR}/${PROTO_NAME}.pb.h")
    set(GRPC_SRC "${CMAKE_CURRENT_BINARY_DIR}/${PROTO_NAME}.grpc.pb.cc")
    set(GRPC_HDR "${CMAKE_CURRENT_BINARY_DIR}/${PROTO_NAME}.grpc.pb.h")
    list(APPEND PROTO_SRCS ${PROTO_SRC})
    list(APPEND PROTO_HDRS ${PROTO_HDR})
    list(APPEND GRPC_SRCS ${GRPC_SRC})
    list(APPEND GRPC_HDRS ${GRPC_HDR})

    add_custom_command(
        OUTPUT ${PROTO_SRC} ${PROTO_HDR} ${GRPC_SRC} ${GRPC_HDR}
        COMMAND ${PROTOC}
            --cpp_out=${CMAKE_CURRENT_BINARY_DIR}
            --grpc_out=${CMAKE_CURRENT_BINARY_DIR}
            --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN}
            --proto_path=${CMAKE_CURRENT_SOURCE_DIR}/limestone/grpc/proto
            ${PROTO_FILE}
        DEPENDS ${PROTO_FILE}
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
        PRIVATE protobuf
        PRIVATE OpenSSL::SSL
        PRIVATE OpenSSL::Crypto
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
        INTERFACE grpc++
        INTERFACE grpc
        INTERFACE protobuf
        INTERFACE gpr
        INTERFACE absl_synchronization
        INTERFACE OpenSSL::SSL
        INTERFACE OpenSSL::Crypto
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

# tg-grpc-tpmonitor: gRPC TP monitor service
file(GLOB TG_GRPC_TPMONITOR_SOURCES
        "limestone/cli/tp_monitor_grpc_server_main.cpp"
)
add_executable(tg-grpc-tpmonitor ${TG_GRPC_TPMONITOR_SOURCES})
target_include_directories(tg-grpc-tpmonitor PRIVATE
        ./limestone
        ${CMAKE_BINARY_DIR}/src
)
target_link_libraries(tg-grpc-tpmonitor PRIVATE
        limestone-impl
        glog::glog
        gflags::gflags
)
set_target_properties(tg-grpc-tpmonitor PROPERTIES
        INSTALL_RPATH "\$ORIGIN/../${CMAKE_INSTALL_LIBDIR}"
        RUNTIME_OUTPUT_NAME "tg-grpc-tpmonitor"
)
if (INSTALL_EXPERIMENTAL_TOOLS)
    install_custom(tg-grpc-tpmonitor tg-grpc-tpmonitor)
endif()
