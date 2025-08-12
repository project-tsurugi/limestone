set(test_target limestone-test)

add_executable(${test_target}
        main.cpp
        )

target_include_directories(${test_target}
        PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}
        PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../src/limestone
        )

target_link_libraries(${test_target}
        PRIVATE limestone-impl
        PUBLIC gtest
        PRIVATE Boost::filesystem
        PRIVATE glog::glog
        PRIVATE gflags::gflags
        )

function (add_test_executable source_file)
    get_filename_component(test_name "${source_file}" NAME_WE)
    target_sources(${test_target}
            PRIVATE ${source_file}
            )
    add_test(
            NAME ${test_name}
            COMMAND ${test_target} --gtest_filter=${test_name}.* --gtest_output=xml:${test_name}_gtest_result.xml
    )
endfunction (add_test_executable)

file(GLOB SRCS
        "limestone/api/*.cpp"
        "limestone/blob/*.cpp"
        "limestone/compaction/*.cpp"
        "limestone/datastore_impl/*.cpp"
        "limestone/epoch/*.cpp"
        "limestone/log/*.cpp"
        "limestone/replication/*.cpp"
        "limestone/snapshot/*.cpp"
        "limestone/utils/*.cpp"
        "limestone/partitioned_cursor/*.cpp"
        "limestone/wal_sync/*.cpp"
        )

foreach(file ${SRCS})
    if("${file}" MATCHES "limestone/replication/" AND NOT BUILD_REPLICATION_TESTS)
        message(STATUS "Skipping replication test: ${file}")
        continue()
    endif()

    add_test_executable(${file})
endforeach()
  
