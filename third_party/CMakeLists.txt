# Copyright 2018-2018 tsurugi project.
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

# gRPC as a subproject (using locally cloned directory)
# Set gRPC options before adding subdirectory
set(gRPC_INSTALL ON CACHE BOOL "Enable gRPC installation")
set(gRPC_BUILD_TESTS OFF CACHE BOOL "Disable gRPC tests")
set(gRPC_BUILD_CSHARP_EXT OFF CACHE BOOL "Disable C# extension")
set(gRPC_BUILD_GRPC_CSHARP_PLUGIN OFF CACHE BOOL "Disable C# plugin")
set(gRPC_BUILD_GRPC_NODE_PLUGIN OFF CACHE BOOL "Disable Node.js plugin")
set(gRPC_BUILD_GRPC_OBJECTIVE_C_PLUGIN OFF CACHE BOOL "Disable Objective-C plugin")
set(gRPC_BUILD_GRPC_PHP_PLUGIN OFF CACHE BOOL "Disable PHP plugin")
set(gRPC_BUILD_GRPC_PYTHON_PLUGIN OFF CACHE BOOL "Disable Python plugin")
set(gRPC_BUILD_GRPC_RUBY_PLUGIN OFF CACHE BOOL "Disable Ruby plugin")

# Disable additional unnecessary gRPC components
set(gRPC_BUILD_GRPC_WEB_PLUGIN OFF CACHE BOOL "Disable gRPC Web plugin")
# set(gRPC_BUILD_CODEGEN OFF CACHE BOOL "Disable code generation tools") - Required for grpc_cpp_plugin
set(gRPC_BACKWARDS_COMPATIBILITY_MODE OFF CACHE BOOL "Disable backwards compatibility")
set(gRPC_BUILD_GRPC_REFLECTION_PLUGIN OFF CACHE BOOL "Disable reflection plugin")
set(gRPC_BUILD_BENCHMARK OFF CACHE BOOL "Disable benchmarks")

# Disable SSL/TLS and use system OpenSSL properly
set(gRPC_SSL_PROVIDER "package" CACHE STRING "Use system OpenSSL")
set(gRPC_USE_SYSTEM_OPENSSL ON CACHE BOOL "Use system OpenSSL")
set(OPENSSL_ROOT_DIR "/usr" CACHE STRING "OpenSSL root directory")

# Find OpenSSL explicitly
find_package(OpenSSL REQUIRED)

# Force static linking for gRPC dependencies to avoid symbol conflicts
set(BUILD_SHARED_LIBS_ORIGINAL ${BUILD_SHARED_LIBS})
# Force ALL gRPC dependencies to be static to prevent symbol duplication
set(BUILD_SHARED_LIBS OFF CACHE BOOL "Build static libraries for gRPC dependencies" FORCE)

# Minimize Abseil components and force consistent configuration
set(ABSL_ENABLE_INSTALL ON CACHE BOOL "Enable Abseil installation")
set(ABSL_BUILD_TESTING OFF CACHE BOOL "Disable Abseil tests")
set(ABSL_BUILD_TEST_HELPERS OFF CACHE BOOL "Disable Abseil test helpers")
set(ABSL_USE_EXTERNAL_GOOGLETEST OFF CACHE BOOL "Don't use external googletest")
set(ABSL_PROPAGATE_CXX_STD ON CACHE BOOL "Propagate C++ standard")
set(ABSL_BUILD_DLL OFF CACHE BOOL "Don't build Abseil as DLL")

# AddressSanitizer compatibility settings for gRPC
# With static UPB linking, ODR violations should be prevented
if(ENABLE_SANITIZER)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -fsanitize-address-use-after-scope -fno-optimize-sibling-calls")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fsanitize=address")
endif()

# Protobuf compatibility with AddressSanitizer - force static linking
set(protobuf_BUILD_TESTS OFF CACHE BOOL "Disable protobuf tests")
set(protobuf_MSVC_STATIC_RUNTIME OFF CACHE BOOL "")
set(protobuf_WITH_ZLIB OFF CACHE BOOL "Disable zlib in protobuf")
set(protobuf_BUILD_EXAMPLES OFF CACHE BOOL "Disable protobuf examples")
set(protobuf_BUILD_CONFORMANCE OFF CACHE BOOL "Disable protobuf conformance tests")
set(protobuf_BUILD_SHARED_LIBS OFF CACHE BOOL "Build protobuf as static library")

# Force all gRPC dependencies to be static
set(gRPC_ABSL_PROVIDER "module" CACHE STRING "Use gRPC's internal Abseil")
set(gRPC_PROTOBUF_PROVIDER "module" CACHE STRING "Use gRPC's internal protobuf")
set(gRPC_RE2_PROVIDER "module" CACHE STRING "Use gRPC's internal RE2")
set(gRPC_CARES_PROVIDER "module" CACHE STRING "Use gRPC's internal c-ares")
set(gRPC_ZLIB_PROVIDER "module" CACHE STRING "Use gRPC's internal zlib")

# Disable UPB backend in protobuf to avoid conflicts
set(protobuf_UPB_ENABLE OFF CACHE BOOL "Disable protobuf UPB backend")
set(protobuf_ENABLE_EXPERIMENTAL_EDITIONS OFF CACHE BOOL "Disable experimental editions")

# Complete static configuration for gRPC core
set(gRPC_USE_PROTO_LITE OFF CACHE BOOL "Disable proto lite to avoid symbol conflicts")
set(gRPC_BUILD_GRPC_CPP_PLUGIN ON CACHE BOOL "Build C++ plugin")
set(gRPC_BUILD_GRPC_CSHARP_PLUGIN OFF CACHE BOOL "Disable C# plugin")

# Enable code generation tools (required for grpc_cpp_plugin)
set(gRPC_BUILD_CODEGEN ON CACHE BOOL "Enable code generation tools")

# Disable UPB usage in gRPC to prevent ODR violations
set(gRPC_UPB_NAMESPACE_OVERRIDE "" CACHE STRING "No UPB namespace override")
set(gRPC_USE_UPB_JSON_LIBRARY OFF CACHE BOOL "Don't use UPB JSON library")
set(gRPC_USE_UPB_GENERATED_CODE OFF CACHE BOOL "Don't use UPB generated code")
set(gRPC_PREFER_PROTOBUF OFF CACHE BOOL "Use protobuf instead of UPB")

# Complete static linking configuration
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# UPB-related settings to avoid ODR violations - force single static library
set(UPB_BUILD_TESTS OFF CACHE BOOL "Disable UPB tests")
set(UPB_ENABLE_CODEGEN OFF CACHE BOOL "Disable UPB codegen")
set(UPB_ENABLE_WERROR OFF CACHE BOOL "Disable UPB werror")
set(UPB_USE_SYSTEM_GRPC OFF CACHE BOOL "Don't use system gRPC")

# Force single UPB library to avoid symbol duplication - CRITICAL for ODR violation fix
set(gRPC_UPB_PROVIDER "module" CACHE STRING "Use gRPC's internal UPB")

# Disable ALL separate UPB targets to force monolithic build
set(UPB_BUILD_TEXTFORMAT OFF CACHE BOOL "Disable UPB textformat to prevent duplication")
set(UPB_BUILD_JSON OFF CACHE BOOL "Disable UPB JSON")
set(protobuf_UPB_BACKEND OFF CACHE BOOL "Disable protobuf UPB backend")

# Complete UPB deduplication - disable all separate UPB targets
set(UPB_ENABLE_JSON OFF CACHE BOOL "Disable UPB JSON completely")
set(UPB_ENABLE_TEXTFORMAT OFF CACHE BOOL "Disable UPB textformat completely")
set(UPB_ENABLE_DECODE OFF CACHE BOOL "Disable UPB decode")
set(UPB_ENABLE_ENCODE OFF CACHE BOOL "Disable UPB encode")
set(UPB_ENABLE_CONFORMANCE OFF CACHE BOOL "Disable UPB conformance")

# FORCE single monolithic UPB library - this is the key to avoiding ODR violations
set(UPB_BUILD_MONOLITHIC ON CACHE BOOL "Build single monolithic UPB library" FORCE)

# Additional static linking enforcement for UPB
set(UPB_BUILD_SHARED_LIBS OFF CACHE BOOL "Force UPB static libraries" FORCE)

# Force gRPC to link UPB statically by controlling UPB targets before gRPC build
# This prevents the creation of separate UPB shared libraries
set(CMAKE_POLICY_DEFAULT_CMP0077 NEW)  # option() honors normal variables

# Pre-configure UPB to prevent shared library creation
set(CMAKE_FIND_LIBRARY_SUFFIXES_BACKUP ${CMAKE_FIND_LIBRARY_SUFFIXES})
set(CMAKE_FIND_LIBRARY_SUFFIXES .a)  # Force static library preference

# Add gRPC subdirectory if it exists - with static dependencies
if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/grpc/CMakeLists.txt")
    add_subdirectory(grpc EXCLUDE_FROM_ALL)
else()
    message(FATAL_ERROR "gRPC directory not found. Please clone gRPC to third_party/grpc/")
endif()

# Restore library suffixes and shared libs setting after gRPC
set(CMAKE_FIND_LIBRARY_SUFFIXES ${CMAKE_FIND_LIBRARY_SUFFIXES_BACKUP})
set(BUILD_SHARED_LIBS ${BUILD_SHARED_LIBS_ORIGINAL})

# Minimize c-ares (DNS resolver) build  
set(CARES_STATIC ON CACHE BOOL "Build c-ares as static")
set(CARES_SHARED OFF CACHE BOOL "Don't build c-ares shared")
set(CARES_BUILD_TESTS OFF CACHE BOOL "Disable c-ares tests")
set(CARES_BUILD_TOOLS OFF CACHE BOOL "Disable c-ares tools")

# Set protobuf and gRPC plugin paths
set(_PROTOBUF_PROTOC $<TARGET_FILE:protoc>)
set(_GRPC_CPP_PLUGIN_EXECUTABLE $<TARGET_FILE:grpc_cpp_plugin>)

if(BUILD_TESTS)
    add_subdirectory(googletest EXCLUDE_FROM_ALL)
endif()
