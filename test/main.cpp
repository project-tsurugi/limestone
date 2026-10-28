/*
 * Copyright 2022-2023 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "test_root.h"

#include <stdlib.h>
#include <errno.h>

#ifdef LIMESTONE_ENABLE_RDMA
#include <filesystem>
#include <string_view>
#include <string>
#include <system_error>
#endif

#include <gflags/gflags.h>

#ifdef LIMESTONE_ENABLE_RDMA
namespace {

// Remove the vendor mock's fixed-name shared-memory segments (GnMock_* and their
// named semaphores sem.GnMock_*). The mock simulates a machine-wide RDMA fabric via
// these segments and does not unlink them on abnormal exit, so residue from a
// previous run (stale ring state, accumulated pid-keyed endpoint slots) breaks the
// next one. Wiping is only safe while no process maps the segments, which is why
// this runs once at binary startup and never between tests: most limestone RDMA
// suites drive the mock in-process and keep it mapped across tests.
void remove_vendor_mock_shm() {
    std::error_code ec{};
    std::filesystem::path const shm_path{"/dev/shm"};
    if (! std::filesystem::exists(shm_path, ec)) {
        return;
    }
    try {
        for (auto const& entry : std::filesystem::directory_iterator{shm_path, ec}) {
            auto const filename = entry.path().filename().string();
            if (filename.rfind("GnMock_", 0) == 0 || filename.rfind("sem.GnMock_", 0) == 0) {
                std::filesystem::remove(entry.path(), ec);
            }
        }
    } catch (std::filesystem::filesystem_error const&) {
        // The error_code constructor does not cover operator++, which throws on
        // iteration errors; the wipe stays best-effort, so leave the residue in place.
    }
}

/**
 * @brief Returns whether the invocation only lists the tests (--gtest_list_tests), without running any.
 *
 * Listing does not touch the mock, so the segments are not wiped for it. An editor's test explorer lists the tests
 * whenever the binary changes, and a wipe at that moment would fail an RDMA suite being run by ctest concurrently.
 */
bool lists_tests_only(int argc, char** argv) {
    for (int i = 1; i < argc; ++i) {
        if (std::string_view(argv[i]).rfind("--gtest_list_tests", 0) == 0) {  // NOLINT(*-pointer-arithmetic)
            return true;
        }
    }
    return false;
}

} // namespace
#endif // LIMESTONE_ENABLE_RDMA

int main(int argc, char **argv) {
    google::InitGoogleLogging("limestone tests");
    FLAGS_logtostderr = true;
#ifdef LIMESTONE_ENABLE_RDMA
    if (! lists_tests_only(argc, argv)) {
        remove_vendor_mock_shm();
    }
#endif
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
