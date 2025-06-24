add_executable(startup_speed_benchmark
    startup_speed_benchmark/startup_speed_benchmark.cpp
)
target_include_directories(startup_speed_benchmark
    PRIVATE ${CMAKE_SOURCE_DIR}/src/limestone
)
target_link_libraries(startup_speed_benchmark
    PRIVATE limestone-impl
    PRIVATE glog::glog
)
