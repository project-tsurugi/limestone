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

if(${RECOVERY_SORTER_KVSLIB_UPPERCASE} STREQUAL "ROCKSDB")
    target_compile_definitions(${test_target}
        PRIVATE SORT_METHOD_USE_ROCKSDB
    )
endif()

function (add_test_executable source_file)
    get_filename_component(test_name "${source_file}" NAME_WE)
    target_sources(${test_target}
        PRIVATE ${source_file}
    )
    add_test(
        NAME ${test_name}
        COMMAND ${test_target} --gtest_filter=${test_name}.*
                --gtest_output=xml:${test_name}_gtest_result.xml
    )
endfunction ()

file(GLOB SRCS
    "limestone/api/*.cpp"
    "limestone/blob/*.cpp"
    "limestone/log/*.cpp"
    "limestone/epoch/*.cpp"
    "limestone/utils/*.cpp"
    "limestone/compaction/*.cpp"
    "limestone/snapshot/*.cpp"
)

foreach(file ${SRCS})
    add_test_executable(${file})
endforeach()
