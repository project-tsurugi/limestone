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

if(ENABLE_RDMA)
    target_compile_definitions(${test_target} PRIVATE LIMESTONE_ENABLE_RDMA)
    target_link_libraries(${test_target} PRIVATE rdma_comm rdma_handshake)

    find_program(RDMA_HANDSHAKED_EXECUTABLE rdma_handshaked
        HINTS "${rdma_comm_RUNTIME_DIR}/../bin"
        REQUIRED
    )
    target_compile_definitions(${test_target} PRIVATE
        RDMA_HANDSHAKED_BIN="${RDMA_HANDSHAKED_EXECUTABLE}"
        TGREPLICA_BIN="$<TARGET_FILE:replica>"
    )
endif()

if (ENABLE_ALTIMETER)
    target_link_libraries(${test_target}
        PRIVATE altimeter
    )
endif()

# Test suites that touch the real RDMA stack (GnMock / rdma_handshaked daemons).
# Keep this set() above add_test_executable(), which reads it via IN_LIST: IN_LIST
# silently treats an undefined variable as false, so placing it later would quietly
# drop the RUN_SERIAL properties.
set(rdma_stack_serial_tests
        rdma_comm_handshake_test
        rdma_comm_send_stream_test
        rdma_establish_session_test
        rdma_establish_replica_session_test
        scenario_tcpless_rdma_test
        rdma_sender_test
        replica_server_test
)

# Create one ctest entry per test source file and run only that file's tests
# via a gtest_filter. CMake never looks inside the files, so this selection
# relies on the convention that the suite name (the first argument of the
# TEST-family macros) equals the file name. The convention is verified
# mechanically by the test_suite_naming_check entry (check_suite_names.sh).
# Adding a new test directory also requires extending the GLOB below; a
# forgotten entry leaves that directory's tests unregistered while everything
# stays green.
function (add_test_executable source_file)
    get_filename_component(test_name "${source_file}" NAME_WE)
    target_sources(${test_target}
            PRIVATE ${source_file}
            )
    # Tests instantiated with a non-empty INSTANTIATE_TEST_SUITE_P prefix have full
    # names of the form "<prefix>/<file name>.<test>/<param>", so match both the
    # prefixed and the unprefixed forms (the latter alone matches zero tests and
    # the entry passes vacuously).
    add_test(
            NAME ${test_name}
            COMMAND ${test_target} "--gtest_filter=${test_name}.*:*/${test_name}.*" --gtest_output=xml:${test_name}_gtest_result.xml
    )
    # Suites using the real GnMock stack (fixed-name shared memory) must run fully
    # exclusive. limestone-test wipes the fixed-name shm segments at startup on RDMA
    # builds, so another entry's process start overlapping one of these suites unlinks
    # segments they have mapped and fails them spuriously (observed with ctest -j4).
    if(test_name IN_LIST rdma_stack_serial_tests)
        set_tests_properties(${test_name} PROPERTIES RUN_SERIAL TRUE)
    endif()
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
        )

foreach(file ${SRCS})
    if("${file}" MATCHES "limestone/replication/" AND NOT BUILD_REPLICATION_TESTS)
        message(STATUS "Skipping replication test: ${file}")
        continue()
    endif()

    # replication_test_helper.cpp is a helper source without any test suite, so it is
    # registered as a source only: an add_test entry for it would match zero tests in
    # the gtest_filter and pass vacuously without running anything.
    if("${file}" MATCHES "/replication_test_helper\\.cpp$")
        target_sources(${test_target} PRIVATE ${file})
        continue()
    endif()

    add_test_executable(${file})
endforeach()

add_test(
        NAME test_suite_naming_check
        COMMAND "${CMAKE_CURRENT_SOURCE_DIR}/check_suite_names.sh"
)
  
