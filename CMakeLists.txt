# Copyright 2022-2024 Project Tsurugi.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

cmake_minimum_required(VERSION 3.16)

project(limestone
    VERSION 1.0.0
    DESCRIPTION "limestone datastore engine"
    LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

list(APPEND CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/cmake")

option(ENABLE_SANITIZER "enable sanitizer on debug build" ON)
option(ENABLE_UB_SANITIZER "enable undefined behavior sanitizer on debug build" OFF)
option(ENABLE_COVERAGE "enable coverage on debug build" OFF)
option(BUILD_TESTS "Build test programs" ON)
option(BUILD_EXAMPLES "Build example programs" ON)
option(BUILD_DOCUMENTS "build documents" ON)
option(INSTALL_EXAMPLES "install examples" OFF)
option(BUILD_SHARED_LIBS "build shared libraries instead of static" ON)
option(BUILD_STRICT "build with option strictly determine of success" ON)
option(ENABLE_GOOGLE_PERFTOOLS "Enable Google Perftools" OFF)
option(PERFORMANCE_TOOLS "Enable tooling to measure engine performance" OFF)
option(TRACY_ENABLE "enable tracy profiler" OFF)
option(ENABLE_ALTIMETER "enable altimeter logging" OFF)
option(INSTALL_EXPERIMENTAL_TOOLS "Install experimental tools like tgreplica" OFF)
option(BUILD_REPLICATION_TESTS "Build replication tests" OFF)

if (FORCE_INSTALL_RPATH)
    message(DEPRECATION "FORCE_INSTALL_RPATH is obsoleted")
endif (FORCE_INSTALL_RPATH)

if (ENABLE_GOOGLE_PERFTOOLS)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DENABLE_GOOGLE_PERFTOOLS")
endif()

set(RECOVERY_SORTER_KVSLIB ROCKSDB CACHE STRING "using eKVS library at recovery process")
string(TOUPPER $CACHE{RECOVERY_SORTER_KVSLIB} RECOVERY_SORTER_KVSLIB_UPPERCASE)
option(RECOVERY_SORTER_PUT_ONLY "using put-only method at recovery process (faster)" ON)

# set(ENGINE "engine")

find_package(Doxygen)
find_package(glog REQUIRED)
find_package(gflags REQUIRED)
find_package(Threads REQUIRED)
find_package(Boost
        COMPONENTS filesystem thread system container
        REQUIRED
        )
if(${RECOVERY_SORTER_KVSLIB_UPPERCASE} STREQUAL "ROCKSDB")
    find_package(RocksDB REQUIRED)
elseif(${RECOVERY_SORTER_KVSLIB_UPPERCASE} STREQUAL "LEVELDB")
    find_package(leveldb REQUIRED)
else()
    message(FATAL_ERROR "unsupported RECOVERY_SORTER_KVSLIB value: ${RECOVERY_SORTER_KVSLIB_UPPERCASE}")
endif()
find_package(nlohmann_json 3.7.0 REQUIRED)
if (ENABLE_ALTIMETER)
    find_package(altimeter REQUIRED)
    find_package(fmt REQUIRED)
endif()

add_subdirectory(third_party) # should be before enable_testing()

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)
include(CompileOptions)
include(InstallOptions)

if (BUILD_TESTS OR BUILD_EXAMPLES)
    enable_testing()
endif()

set(export_name "limestone")
set(package_name "limestone")

add_library(limestone-api INTERFACE)

target_include_directories(limestone-api
    INTERFACE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/limestone>
)

add_subdirectory(src)
if(BUILD_TESTS)
    add_subdirectory(test)
endif()
# if(BUILD_EXAMPLES)
#     add_subdirectory(examples)
# endif()
# if (BUILD_DOCUMENTS)
#     add_subdirectory(doxygen)
# endif()

install(
    TARGETS
        limestone-api
    EXPORT
        ${export_name}
    LIBRARY
        DESTINATION ${CMAKE_INSTALL_LIBDIR}
        COMPONENT Runtime
    ARCHIVE
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/${export_name}
        COMPONENT Development
    RUNTIME
        DESTINATION ${CMAKE_INSTALL_BINDIR}
        COMPONENT Runtime
)


set(SCRIPT_DIR "${CMAKE_SOURCE_DIR}/scripts")
set(SCRIPTS
    "${SCRIPT_DIR}/tgcmpct_logs.sh"
    "${SCRIPT_DIR}/tgdel_logs.sh"
)

install(
    FILES ${SCRIPTS}
    DESTINATION ${CMAKE_INSTALL_BINDIR}
    PERMISSIONS OWNER_EXECUTE OWNER_WRITE OWNER_READ GROUP_EXECUTE GROUP_READ WORLD_EXECUTE WORLD_READ
)

install(
    DIRECTORY include/
    DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/${export_name}"
    COMPONENT Development
    FILES_MATCHING PATTERN "*.h"
)

configure_file(
        ${CMAKE_CURRENT_SOURCE_DIR}/cmake/Config.cmake.in
        ${CMAKE_CURRENT_BINARY_DIR}/${package_name}-config.cmake
        @ONLY
)

write_basic_package_version_file(
        "${CMAKE_CURRENT_BINARY_DIR}/${package_name}-config-version.cmake"
        COMPATIBILITY SameMajorVersion
)

install(
    FILES
        ${CMAKE_CURRENT_BINARY_DIR}/${package_name}-config.cmake
        ${CMAKE_CURRENT_BINARY_DIR}/${package_name}-config-version.cmake
    DESTINATION
        ${CMAKE_INSTALL_LIBDIR}/cmake/${package_name}
)

install(
    EXPORT ${package_name}
    FILE ${package_name}-targets.cmake
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/${package_name}
    EXPORT_LINK_INTERFACE_LIBRARIES
)
