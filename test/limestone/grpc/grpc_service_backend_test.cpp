/*
 * Copyright 2022-2025 Project Tsurugi.
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

#include <gtest/gtest.h>
#include <boost/filesystem.hpp>
#include "limestone/grpc/backend/grpc_service_backend.h"
#include "limestone/api/datastore.h"
#include "test_root.h"
#include "limestone/api/configuration.h"

namespace limestone::testing {

using limestone::api::datastore;
using limestone::grpc::backend::grpc_service_backend;

class grpc_service_backend_test : public ::testing::Test {

public:
    void gen_datastore() {
        std::vector<boost::filesystem::path> data_locations{};
        data_locations.emplace_back(log_dir_);
        boost::filesystem::path metadata_location{log_dir_};
        limestone::api::configuration conf(data_locations, metadata_location);
        datastore_ = std::make_unique<limestone::api::datastore_test>(conf);
    }
    void SetUp() override {
        boost::filesystem::remove_all(log_dir_);
        if (!boost::filesystem::create_directory(log_dir_)) {
            std::cerr << "cannot make directory" << std::endl;
        }
    }

    void TearDown() override {
        datastore_ = nullptr;
        boost::filesystem::remove_all(log_dir_);
    }

protected:
    std::unique_ptr<limestone::api::datastore_test> datastore_{};
    boost::filesystem::path log_dir_ = "/tmp/grpc_service_backend_test";
};

TEST_F(grpc_service_backend_test, create_inproc_returns_instance) {
    gen_datastore();
    auto backend = grpc_service_backend::create_inproc(*datastore_, log_dir_);
    EXPECT_NE(backend, nullptr);
}

TEST_F(grpc_service_backend_test, create_standalone_returns_instance) {
    auto backend = grpc_service_backend::create_standalone(log_dir_);
    EXPECT_NE(backend, nullptr);
}

} // namespace limestone::testing
