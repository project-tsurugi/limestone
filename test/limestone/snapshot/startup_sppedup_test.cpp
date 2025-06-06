/*
 * Copyright 2022-2024 Project Tsurugi.
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
#include "test_root.h"
#include "limestone/api/log_channel.h"

namespace limestone::testing {

using limestone::api::log_channel;

class startup_speedup_test : public ::testing::Test {
protected:
    static constexpr const char* location_compacted = "/home/umegane/work/log.compacted";
    static constexpr const char* location_nocompacted = "/home/umegane/work/log.nocompact";
    std::unique_ptr<limestone::api::datastore_test> datastore_;

    void SetUp() override {
    }

    void gen_datastore(const std::string& loc) {
        std::vector<boost::filesystem::path> data_locations{};
        data_locations.emplace_back(loc);
        boost::filesystem::path metadata_location{loc};
        limestone::api::configuration conf(data_locations, metadata_location);

        datastore_ = std::make_unique<limestone::api::datastore_test>(conf);

        datastore_->ready();
    }

    void TearDown() override {
        datastore_ = nullptr;
    }
};

TEST_F(startup_speedup_test, dummy_test) {
    gen_datastore(location_compacted);
    auto snapshot = datastore_->get_snapshot();
    ASSERT_TRUE(snapshot != nullptr);
    auto cursor = snapshot->get_cursor();
    ASSERT_TRUE(cursor != nullptr);
    int i = 0;
    while (cursor->next()) {
        i++;
    }
    std::cerr << "entry count = " << i << std::endl;
}

}  // namespace limestone::testing
