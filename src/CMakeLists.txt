file(GLOB SOURCES
        "limestone/*.cpp"
)

add_library(${package_name}
        ${SOURCES}
)

set_target_properties(${package_name}
        PROPERTIES
                INSTALL_RPATH "\$ORIGIN"
                OUTPUT_NAME ${export_name}
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
        PRIVATE .
        PRIVATE ./limestone
        )
target_link_libraries(dblogutil PRIVATE limestone-impl PRIVATE glog::glog gflags::gflags)
set_target_properties(dblogutil
        PROPERTIES
                INSTALL_RPATH "\$ORIGIN/../${CMAKE_INSTALL_LIBDIR}"
                RUNTIME_OUTPUT_NAME "tglogutil"
)
install_custom(dblogutil dblogutil)
