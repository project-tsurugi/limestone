set(TOOLS
    startup_speed_benchmark
    wal_dump
)

foreach(tool IN LISTS TOOLS)
    add_executable(${tool} ${tool}/${tool}.cpp)

    target_include_directories(${tool}
        PRIVATE ${CMAKE_SOURCE_DIR}/src/limestone
        PRIVATE ${CMAKE_SOURCE_DIR}/include
    )

    target_link_libraries(${tool}
        PRIVATE limestone-impl
        PRIVATE glog::glog
    )
endforeach()

# Special handling for grpc_server
if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/grpc_server")
    add_subdirectory(grpc_server)
endif()
