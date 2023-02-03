/*
 * Copyright 2022-2022 tsurugi project.
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

#include <boost/filesystem/path.hpp>
#include <boost/filesystem/fstream.hpp>

#include "log_entry.h"

namespace limestone::testing {

constexpr const char* location = "/tmp/log_entry_4_LevelDB_test";

class log_entry_4_LevelDB_test : public ::testing::Test {
protected:
    const std::string key = "this is a key";
    const std::string value = "this is a value";
    const limestone::api::storage_id_type storage_id = 12345;
    const limestone::api::write_version_type write_version = limestone::api::write_version_type(67898, 76543);
    const limestone::api::epoch_id_type epoch_id = 56873;
    
    virtual void SetUp() {
        if (system("rm -rf /tmp/log_entry_4_LevelDB_test") != 0) {
            std::cerr << "cannot remove directory" << std::endl;
        }
        if (system("mkdir -p /tmp/log_entry_4_LevelDB_test") != 0) {
            std::cerr << "cannot make directory" << std::endl;
        }
        file1_ = boost::filesystem::path(location) / boost::filesystem::path("file1");
        file2_ = boost::filesystem::path(location) / boost::filesystem::path("file2");
    }

    virtual void TearDown() {
        if (system("rm -rf /tmp/log_entry_4_LevelDB_test") != 0) {
            std::cerr << "cannot remove directory" << std::endl;
        }
    }

    limestone::api::impl::log_entry log_entry_begin_{};
    limestone::api::impl::log_entry log_entry_normal_{};
    limestone::api::impl::log_entry log_entry_end_{};
    boost::filesystem::path file1_{};
    boost::filesystem::path file2_{};
};

TEST_F(log_entry_4_LevelDB_test, write_and_read_and_write_and_read) {
    boost::filesystem::ofstream ostrm;
    limestone::api::impl::log_entry log_entry;
    
    ostrm.open(file1_, std::ios_base::out | std::ios_base::app | std::ios_base::binary);
    limestone::api::impl::log_entry::write(ostrm, storage_id, key, value, write_version);
    ostrm.close();

    boost::filesystem::ifstream istrm;
    istrm.open(file1_, std::ios_base::in | std::ios_base::binary);
    boost::filesystem::ofstream ostrm2;
    ostrm2.open(file2_, std::ios_base::out | std::ios_base::app | std::ios_base::binary);
    while(log_entry.read(istrm)) {
        limestone::api::impl::log_entry::write(ostrm2, log_entry.key_sid(), log_entry.value_etc());
    }
    istrm.close();
    ostrm2.close();

    boost::filesystem::ifstream istrm2;
    istrm2.open(file2_, std::ios_base::in | std::ios_base::binary);
    EXPECT_TRUE(log_entry_normal_.read(istrm2));
    EXPECT_FALSE(log_entry.read(istrm2));
    istrm2.close();

    EXPECT_EQ(log_entry_normal_.type(), limestone::api::impl::log_entry::entry_type::normal_entry);
    EXPECT_EQ(log_entry_normal_.storage(), storage_id);

    std::string buf_key;
    log_entry_normal_.key(buf_key);
    EXPECT_EQ(buf_key, key);

    std::string buf_value;
    log_entry_normal_.value(buf_value);
    EXPECT_EQ(buf_value, value);

    limestone::api::write_version_type buf_version;
    log_entry_normal_.write_version(buf_version);
    EXPECT_TRUE(buf_version == write_version);
}

}  // namespace limestone::testing
