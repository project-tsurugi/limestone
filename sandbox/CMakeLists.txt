set(TOOLS
    startup_speed_benchmark/startup_speed_benchmark.cpp
    wal_dump/wal_dump.cpp
    grpc/grpc_server.cpp
    grpc/grpc_client.cpp
)

foreach(tool_src IN LISTS TOOLS)
    get_filename_component(tool_name ${tool_src} NAME_WE)
    add_executable(${tool_name} ${tool_src})
    target_include_directories(${tool_name} PRIVATE
        ${PROJECT_SOURCE_DIR}/src
        ${PROJECT_SOURCE_DIR}/src/limestone
        ${PROJECT_SOURCE_DIR}/include
        ${CMAKE_BINARY_DIR}/src   # for generated protobuf headers (safe to include always)
    )
    target_link_libraries(${tool_name} PRIVATE limestone-impl)
endforeach()
