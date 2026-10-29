/*
 * Copyright 2025-2026 Project Tsurugi.
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
#include <algorithm>
#include <cerrno>
#include <fstream>
#include <vector>
#include "test_root.h"
#include "blob_file_resolver.h"
#include "datastore_impl.h"
#include "file_operations.h"
#include "limestone/api/limestone_exception.h"
#include "replication/tcp_replication_message_io.h"
#include "limestone/api/blob_id_type.h"


namespace limestone::testing {

using namespace limestone::replication;
using namespace limestone::internal;
using namespace limestone::api;

constexpr const char* base_directory = "/tmp/tcp_replication_message_io_test";

namespace {

// records the directories passed to fsync_directory, and makes it fail if fail is true
class directory_sync_recorder : public limestone::internal::real_file_operations {
public:
    int fsync_directory(const boost::filesystem::path& dir) override {
        synced_dirs.push_back(dir);
        if (fail) {
            errno = EIO;
            return -1;
        }
        return real_file_operations::fsync_directory(dir);
    }
    [[nodiscard]] bool synced(const boost::filesystem::path& dir) const {
        return std::find(synced_dirs.begin(), synced_dirs.end(), dir) != synced_dirs.end();
    }
    bool fail = false;
    std::vector<boost::filesystem::path> synced_dirs;
};

}  // namespace

class tcp_replication_message_io_test : public ::testing::Test {

// TODO: Add more error-path tests to improve coverage.



protected:
    void SetUp() override {
        [[maybe_unused]] int rm_result = system(("rm -rf " + std::string(base_directory)).c_str());
        [[maybe_unused]] int mkdir_result = system(("mkdir -p " + std::string(base_directory)).c_str());

        limestone::api::configuration conf{};
        conf.set_data_location(base_directory);

        datastore_ = std::make_unique<limestone::api::datastore_test>(conf);        
    }

    void TearDown() override {
        datastore_.reset();
        [[maybe_unused]] int rm_result2 = system(("rm -rf " + std::string(base_directory)).c_str());
    }


    std::unique_ptr<api::datastore_test> datastore_;
};

TEST_F(tcp_replication_message_io_test, round_trip_blob) {
    blob_id_type blob_id = 123456789;
    auto path = datastore_->get_blob_file(blob_id).path();

    // create source file
    boost::filesystem::create_directories(path.parent_path());
    std::ofstream ofs(path.string(), std::ios::binary);
    ofs << "limestone_blob_data";
    ofs.close();

    // send via replication_message_io in string‑mode
    tcp_replication_message_io sender("", *datastore_);
    sender.send_blob(blob_id);
    std::string wire = sender.get_out_string();

    // Remove original file so receive_blob must write it
    boost::filesystem::remove(path);

    // receive into resolver directory
    tcp_replication_message_io receiver(wire, *datastore_);
    EXPECT_EQ(receiver.receive_blob(), blob_id);

    // verify content
    std::ifstream ifs(path.string(), std::ios::binary);
    std::ostringstream oss;
    oss << ifs.rdbuf();
    EXPECT_EQ(oss.str(), "limestone_blob_data");
}

// receive_blob synchronizes the parent of the created BLOB directory and the directory of the created file, and
// throws if it fails
TEST_F(tcp_replication_message_io_test, receive_blob_synchronizes_directories) {
    blob_id_type blob_id = 123456789;
    auto path = datastore_->get_blob_file(blob_id).path();
    boost::filesystem::create_directories(path.parent_path());
    std::ofstream(path.string(), std::ios::binary) << "limestone_blob_data";
    tcp_replication_message_io sender("", *datastore_);
    sender.send_blob(blob_id);
    std::string wire = sender.get_out_string();

    // the receiving datastore uses the recorder; recreate it after removing the BLOB directory
    directory_sync_recorder ops;
    datastore_.reset();
    boost::filesystem::remove_all(path.parent_path());
    limestone::api::datastore_impl::set_file_operations_for_test(&ops);
    limestone::api::configuration conf{};
    conf.set_data_location(base_directory);
    datastore_ = std::make_unique<limestone::api::datastore_test>(conf);
    limestone::api::datastore_impl::set_file_operations_for_test(nullptr);

    {
        tcp_replication_message_io receiver(wire, *datastore_);
        EXPECT_EQ(receiver.receive_blob(), blob_id);
    }
    EXPECT_TRUE(ops.synced(path.parent_path()));
    EXPECT_TRUE(ops.synced(path.parent_path().parent_path()));

    ops.fail = true;
    {
        tcp_replication_message_io receiver(wire, *datastore_);
        EXPECT_THROW(receiver.receive_blob(), limestone_io_exception);
    }
    EXPECT_EQ(ops.synced_dirs.back(), path.parent_path());

    // when the BLOB directory is created again, a failure to synchronize its parent throws
    boost::filesystem::remove_all(path.parent_path());
    {
        tcp_replication_message_io receiver(wire, *datastore_);
        EXPECT_THROW(receiver.receive_blob(), limestone_io_exception);
    }
    EXPECT_EQ(ops.synced_dirs.back(), path.parent_path().parent_path());
    datastore_.reset();
}

TEST_F(tcp_replication_message_io_test, unsupported_path_type_throws) {
    blob_id_type blob_id = 987654321;
    auto dir = datastore_->get_blob_file(blob_id).path();
    boost::filesystem::create_directories(dir);

    tcp_replication_message_io io("", *datastore_);
    EXPECT_THROW(io.send_blob(blob_id), std::runtime_error);
}

TEST_F(tcp_replication_message_io_test, round_trip_large_blob) {
    
    constexpr std::size_t file_size = tcp_replication_message_io::blob_buffer_size * 10 + 1234;

    blob_id_type blob_id = 555555;
    auto path = datastore_->get_blob_file(blob_id).path();

    boost::filesystem::create_directories(path.parent_path());
    std::ofstream ofs(path.string(), std::ios::binary);
    for (std::size_t i = 0; i < file_size; ++i) {
        char c = static_cast<char>((i % 256) ^ 0xAA);
        ofs.put(c);
    }
    ofs.close();

    tcp_replication_message_io sender("", *datastore_);
    sender.send_blob(blob_id);
    std::string wire = sender.get_out_string();

    // Ensure receive_blob actually writes the file
    boost::filesystem::remove(path);
    tcp_replication_message_io receiver(wire, *datastore_);

    EXPECT_EQ(receiver.receive_blob(), blob_id);
    std::ifstream ifs(path.string(), std::ios::binary);
    std::vector<char> read_data(file_size);
    ifs.read(read_data.data(), static_cast<std::streamsize>(file_size));
    ASSERT_TRUE(ifs.good());

    for (std::size_t i = 0; i < file_size; ++i) {
        char expected = static_cast<char>((i % 256) ^ 0xAA);
        EXPECT_EQ(read_data[i], expected) << "Mismatch at byte " << i;
    }
}

TEST_F(tcp_replication_message_io_test, round_trip_boundary_blob) {
    constexpr std::size_t buffer_size = tcp_replication_message_io::blob_buffer_size;
    std::array<uint32_t, 5> sizes = {
        0,
        1,
        static_cast<uint32_t>(buffer_size - 1),
        static_cast<uint32_t>(buffer_size),
        static_cast<uint32_t>(buffer_size + 1)
    };

    for (auto size : sizes) {
        SCOPED_TRACE("file_size=" + std::to_string(size));

        blob_id_type blob_id = static_cast<blob_id_type>(1000 + size);
        auto path = datastore_->get_blob_file(blob_id).path();
        boost::filesystem::create_directories(path.parent_path());

        // Write patterned data
        std::ofstream ofs(path.string(), std::ios::binary);
        for (uint32_t i = 0; i < size; ++i) {
            ofs.put(static_cast<char>((i % 256) ^ 0xAA));
        }
        ofs.close();

        tcp_replication_message_io sender("", *datastore_);
        sender.send_blob(blob_id);
        std::string wire = sender.get_out_string();

        boost::filesystem::remove(path);
        tcp_replication_message_io receiver(wire, *datastore_);
        EXPECT_EQ(receiver.receive_blob(), blob_id);

        std::vector<char> data(size);
        std::ifstream ifs(path.string(), std::ios::binary);
        ifs.read(data.data(), static_cast<std::streamsize>(size));
        ASSERT_EQ(ifs.gcount(), static_cast<std::streamsize>(size));

        for (uint32_t i = 0; i < size; ++i) {
            EXPECT_EQ(data[i], static_cast<char>((i % 256) ^ 0xAA))
                << "Mismatch at byte " << i;
        }
    }
}

TEST_F(tcp_replication_message_io_test, receive_creates_missing_parent_directory) {
    blob_id_type blob_id = 42424242;
    auto path = datastore_->get_blob_file(blob_id).path();
    auto parent = path.parent_path();
    auto grandparent = parent.parent_path();

    // Prepare source file and wire
    boost::filesystem::create_directories(parent);
    std::ofstream ofs(path.string(), std::ios::binary);
    ofs << "test_data";
    ofs.close();

    tcp_replication_message_io sender("", *datastore_);
    sender.send_blob(blob_id);
    std::string wire = sender.get_out_string();

    // Remove only the immediate parent directory
    boost::filesystem::remove_all(parent);
    // Ensure grandparent still exists
    boost::filesystem::create_directories(grandparent);

    tcp_replication_message_io receiver(wire, *datastore_);
    EXPECT_EQ(receiver.receive_blob(), blob_id);

    std::ifstream ifs(path.string(), std::ios::binary);
    std::ostringstream oss;
    oss << ifs.rdbuf();
    EXPECT_EQ(oss.str(), "test_data");
}

TEST_F(tcp_replication_message_io_test, receive_fails_when_grandparent_missing) {
    blob_id_type blob_id = 42424243;
    auto path = datastore_->get_blob_file(blob_id).path();
    auto parent = path.parent_path();
    auto grandparent = parent.parent_path();

    // Prepare source file and wire
    boost::filesystem::create_directories(parent);
    std::ofstream ofs(path.string(), std::ios::binary);
    ofs << "test_data";
    ofs.close();

    tcp_replication_message_io sender("", *datastore_);
    sender.send_blob(blob_id);
    std::string wire = sender.get_out_string();

    // Remove grandparent (and thus the parent)
    boost::filesystem::remove_all(grandparent);

    tcp_replication_message_io receiver(wire, *datastore_);
    EXPECT_THROW(receiver.receive_blob(), std::runtime_error);
}


}  // namespace limestone::testing
