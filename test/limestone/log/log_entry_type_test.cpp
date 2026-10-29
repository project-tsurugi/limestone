/*
 * Copyright 2022-2026 Project Tsurugi.
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

#include <unistd.h>
#include "test_root.h"

#include <boost/filesystem.hpp>

#include "log_entry.h"
#include "log_entry/log_entry_reader.h"

namespace limestone::testing {

constexpr const char* location = "/tmp/log_entry_type_test";

class log_entry_type_test : public ::testing::Test {
protected:
    const std::string key = "this is a key";
    const std::string value = "this is a value";
    const std::string key2 = "this is a key2";
    const std::string value2 = "this is a value2";
    const std::vector<limestone::api::blob_id_type> blob_ids = {314, 1592, 65358};
    const limestone::api::storage_id_type storage_id = 12345;
    const limestone::api::write_version_type write_version = limestone::api::write_version_type(67898, 76543);
    const limestone::api::write_version_type write_version2 = limestone::api::write_version_type(2236, 1732);
    const limestone::api::epoch_id_type epoch_id = 56873;
    
    virtual void SetUp() {
        if (system("rm -rf /tmp/log_entry_type_test") != 0) {
            std::cerr << "cannot remove directory" << std::endl;
        }
        if (system("mkdir -p /tmp/log_entry_type_test") != 0) {
            std::cerr << "cannot make directory" << std::endl;
        }
        file1_ = boost::filesystem::path(location) / boost::filesystem::path("file1");
        file2_ = boost::filesystem::path(location) / boost::filesystem::path("file2");
    }

    virtual void TearDown() {
        if (system("rm -rf /tmp/log_entry_type_test") != 0) {
            std::cerr << "cannot remove directory" << std::endl;
        }
    }

    limestone::api::log_entry log_entry_begin_{};
    limestone::api::log_entry log_entry_normal_{};
    limestone::api::log_entry log_entry_normal_with_blob_{};
    limestone::api::log_entry log_entry_end_{};
    boost::filesystem::path file1_{};
    boost::filesystem::path file2_{};

    void write_log_entries(const boost::filesystem::path& file_path) {
        FILE* ostrm = fopen(file_path.c_str(), "a");
        limestone::api::log_entry::begin_session(ostrm, epoch_id);
        limestone::api::log_entry::write(ostrm, storage_id, key, value, write_version);
        limestone::api::log_entry::write_with_blob(ostrm, storage_id, key2, value2, write_version2, blob_ids);
        limestone::api::log_entry::end_session(ostrm, epoch_id + 1);
        fclose(ostrm);
    }

    void verify_log_entries(const boost::filesystem::path& file_path) {
        limestone::internal::log_entry_reader reader(file_path);
        for (auto* entry : {&log_entry_begin_, &log_entry_normal_, &log_entry_normal_with_blob_, &log_entry_end_}) {
            const auto read_result = reader.next();
            ASSERT_EQ(read_result.result(), limestone::internal::log_entry_read_result::status::ok);
            *entry = limestone::api::log_entry{read_result.view()};
        }
        EXPECT_EQ(reader.next().result(), limestone::internal::log_entry_read_result::status::end);

        EXPECT_EQ(log_entry_begin_.type(), limestone::api::log_entry::entry_type::marker_begin);
        EXPECT_EQ(log_entry_begin_.epoch_id(), epoch_id);

        EXPECT_EQ(log_entry_normal_.type(), limestone::api::log_entry::entry_type::normal_entry);
        EXPECT_EQ(log_entry_normal_.storage(), storage_id);

        EXPECT_EQ(log_entry_normal_.key(), key);
        EXPECT_EQ(log_entry_normal_.value(), value);
        EXPECT_EQ(log_entry_normal_.write_version(), write_version);

        EXPECT_EQ(log_entry_normal_with_blob_.type(), limestone::api::log_entry::entry_type::normal_with_blob);
        EXPECT_EQ(log_entry_normal_with_blob_.storage(), storage_id);

        EXPECT_EQ(log_entry_normal_with_blob_.key(), key2);
        EXPECT_EQ(log_entry_normal_with_blob_.value(), value2);
        EXPECT_EQ(log_entry_normal_with_blob_.write_version(), write_version2);
        EXPECT_EQ(log_entry_normal_with_blob_.blob_ids(), blob_ids);

        EXPECT_EQ(log_entry_end_.type(), limestone::api::log_entry::entry_type::marker_end);
        EXPECT_EQ(log_entry_end_.epoch_id(), epoch_id + 1);
    }
};

TEST_F(log_entry_type_test, write_and_read) {
    write_log_entries(file1_);
    verify_log_entries(file1_);
}

TEST_F(log_entry_type_test, write_and_read_and_write_and_read) {
    write_log_entries(file1_);

    limestone::internal::log_entry_reader reader(file1_);
    FILE* ostrm2 = fopen(file2_.c_str(), "a");
    while (true) {
        const auto read_result = reader.next();
        if (read_result.result() != limestone::internal::log_entry_read_result::status::ok) {
            EXPECT_EQ(read_result.result(), limestone::internal::log_entry_read_result::status::end);
            break;
        }
        limestone::api::log_entry::write_serialized(ostrm2, read_result.view(),
                                                    limestone::api::log_entry::write_version_handling::keep);
    }
    fclose(ostrm2);

    verify_log_entries(file2_);
}

}  // namespace limestone::testing
