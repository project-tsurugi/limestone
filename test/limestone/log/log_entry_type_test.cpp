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

#include <unistd.h>
#include "test_root.h"

#include <boost/filesystem.hpp>
#include <boost/filesystem/fstream.hpp>

#include "log_entry.h"

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
        limestone::api::log_entry log_entry;
        boost::filesystem::ifstream istrm;
        istrm.open(file_path, std::ios_base::in | std::ios_base::binary);

        EXPECT_TRUE(log_entry_begin_.read(istrm));
        EXPECT_TRUE(log_entry_normal_.read(istrm));
        EXPECT_TRUE(log_entry_normal_with_blob_.read(istrm));
        EXPECT_TRUE(log_entry_end_.read(istrm));
        EXPECT_FALSE(log_entry.read(istrm));
        istrm.close();

        EXPECT_EQ(log_entry_begin_.type(), limestone::api::log_entry::entry_type::marker_begin);
        EXPECT_EQ(log_entry_begin_.epoch_id(), epoch_id);

        EXPECT_EQ(log_entry_normal_.type(), limestone::api::log_entry::entry_type::normal_entry);
        EXPECT_EQ(log_entry_normal_.storage(), storage_id);

        std::string buf_key;
        log_entry_normal_.key(buf_key);
        EXPECT_EQ(buf_key, key);

        std::string buf_value;
        log_entry_normal_.value(buf_value);
        EXPECT_EQ(buf_value, value);

        limestone::api::write_version_type buf_version;
        log_entry_normal_.write_version(buf_version);
        EXPECT_EQ(buf_version, write_version);

        EXPECT_EQ(log_entry_normal_with_blob_.type(), limestone::api::log_entry::entry_type::normal_with_blob);
        EXPECT_EQ(log_entry_normal_with_blob_.storage(), storage_id);

        std::string buf_key2;
        log_entry_normal_with_blob_.key(buf_key2);
        EXPECT_EQ(buf_key2, key2);

        std::string buf_value2;
        log_entry_normal_with_blob_.value(buf_value2);
        EXPECT_EQ(buf_value2, value2);

        limestone::api::write_version_type buf_version2;
        log_entry_normal_with_blob_.write_version(buf_version2);
        EXPECT_EQ(buf_version2, write_version2);

        EXPECT_EQ(log_entry_normal_with_blob_.get_blob_ids(), blob_ids);

        EXPECT_EQ(log_entry_end_.type(), limestone::api::log_entry::entry_type::marker_end);
        EXPECT_EQ(log_entry_end_.epoch_id(), epoch_id + 1);
    }
};

TEST_F(log_entry_type_test, write_and_read) {
    limestone::api::log_entry log_entry;

    write_log_entries(file1_);
    verify_log_entries(file1_);
}

TEST_F(log_entry_type_test, write_and_read_and_write_and_read) {
    limestone::api::log_entry log_entry;
    
    write_log_entries(file1_);

    boost::filesystem::ifstream istrm;
    istrm.open(file1_, std::ios_base::in | std::ios_base::binary);
    FILE* ostrm2 = fopen(file2_.c_str(), "a");
    while(log_entry.read(istrm)) {
        log_entry.write(ostrm2);
    }
    istrm.close();
    fclose(ostrm2);

    verify_log_entries(file2_);
}

}  // namespace limestone::testing
