
add_executable(run-recovery run-recovery.cpp)

target_include_directories(run-recovery
        PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}
        PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../src/limestone
        )

target_link_libraries(run-recovery
    PRIVATE limestone-impl
    PRIVATE Boost::filesystem
    PRIVATE glog::glog
    PRIVATE gflags::gflags
)
