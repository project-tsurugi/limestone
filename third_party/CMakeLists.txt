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

# AddressSanitizer compatibility settings for gRPC
set(ABSL_ENABLE_INSTALL ON CACHE BOOL "Enable Abseil installation")
# Force gRPC and Abseil to use compatible AddressSanitizer settings
if(ENABLE_SANITIZER)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -fsanitize-address-use-after-scope -fno-optimize-sibling-calls")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fsanitize=address")
endif()

# Protobuf compatibility with AddressSanitizer
set(protobuf_BUILD_TESTS OFF CACHE BOOL "Disable protobuf tests")
set(protobuf_MSVC_STATIC_RUNTIME OFF CACHE BOOL "")
set(protobuf_WITH_ZLIB OFF CACHE BOOL "Disable zlib in protobuf")

# Add gRPC subdirectory if it exists
if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/grpc/CMakeLists.txt")
    add_subdirectory(grpc EXCLUDE_FROM_ALL)
else()
    message(FATAL_ERROR "gRPC directory not found. Please clone gRPC to third_party/grpc/")
endif()

# Set protobuf and gRPC plugin paths
set(_PROTOBUF_PROTOC $<TARGET_FILE:protoc>)
set(_GRPC_CPP_PLUGIN_EXECUTABLE $<TARGET_FILE:grpc_cpp_plugin>)

if(BUILD_TESTS)
    add_subdirectory(googletest EXCLUDE_FROM_ALL)
endif()
