#include <gtest/gtest.h>
#include <boost/filesystem.hpp>
#include <fstream>
#include "test_root.h"
#include "blob_file_resolver.h"
#include "replication/blob_socket_io.h"
#include "limestone/api/blob_id_type.h"


namespace limestone::testing {

using namespace limestone::replication;
using namespace limestone::internal;
using namespace limestone::api;

constexpr const char* base_directory = "/tmp/blob_socket_io_test";

class blob_socket_io_test : public ::testing::Test {

// TODO: エラー系のテストが不足していｔ，カバレッジが低い。    
    
protected:
    void SetUp() override {
        system(("rm -rf " + std::string(base_directory)).c_str());
        system(("mkdir -p " + std::string(base_directory)).c_str());
        resolver_ = std::make_unique<blob_file_resolver>(boost::filesystem::path(base_directory));
    }

    void TearDown() override {
        resolver_.reset();
        system(("rm -rf " + std::string(base_directory)).c_str());
    }

    std::unique_ptr<blob_file_resolver> resolver_;
};

TEST_F(blob_socket_io_test, round_trip_blob) {
    blob_id_type blob_id = 123456789;
    auto path = resolver_->resolve_path(blob_id);

    // create source file
    boost::filesystem::create_directories(path.parent_path());
    std::ofstream ofs(path.string(), std::ios::binary);
    ofs << "limestone_blob_data";
    ofs.close();

    // send via socket_io in string‑mode
    blob_socket_io sender("", *resolver_);
    sender.send_blob(blob_id);
    std::string wire = sender.get_out_string();

    // Remove original file so receive_blob must write it
    boost::filesystem::remove(path);

    // receive into resolver directory
    blob_socket_io receiver(wire, *resolver_);
    EXPECT_EQ(receiver.receive_blob(), blob_id);

    // verify content
    std::ifstream ifs(path.string(), std::ios::binary);
    std::ostringstream oss;
    oss << ifs.rdbuf();
    EXPECT_EQ(oss.str(), "limestone_blob_data");
}

TEST_F(blob_socket_io_test, unsupported_path_type_throws) {
    blob_id_type blob_id = 987654321;
    auto dir = resolver_->resolve_path(blob_id);
    boost::filesystem::create_directories(dir);

    blob_socket_io io("", *resolver_);
    EXPECT_THROW(io.send_blob(blob_id), std::runtime_error);
}

TEST_F(blob_socket_io_test, round_trip_large_blob) {
    
    constexpr std::size_t file_size = blob_socket_io::blob_buffer_size * 10 + 1234;

    blob_id_type blob_id = 555555;
    auto path = resolver_->resolve_path(blob_id);

    boost::filesystem::create_directories(path.parent_path());
    std::ofstream ofs(path.string(), std::ios::binary);
    for (std::size_t i = 0; i < file_size; ++i) {
        char c = static_cast<char>((i % 256) ^ 0xAA);
        ofs.put(c);
    }
    ofs.close();

    blob_socket_io sender("", *resolver_);
    sender.send_blob(blob_id);
    std::string wire = sender.get_out_string();

    // Ensure receive_blob actually writes the file
    boost::filesystem::remove(path);
    blob_socket_io receiver(wire, *resolver_);

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

TEST_F(blob_socket_io_test, round_trip_boundary_blob) {
    constexpr std::size_t buffer_size = blob_socket_io::blob_buffer_size;
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
        auto path = resolver_->resolve_path(blob_id);
        boost::filesystem::create_directories(path.parent_path());

        // Write patterned data
        std::ofstream ofs(path.string(), std::ios::binary);
        for (uint32_t i = 0; i < size; ++i) {
            ofs.put(static_cast<char>((i % 256) ^ 0xAA));
        }
        ofs.close();

        blob_socket_io sender("", *resolver_);
        sender.send_blob(blob_id);
        std::string wire = sender.get_out_string();

        boost::filesystem::remove(path);
        blob_socket_io receiver(wire, *resolver_);
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

TEST_F(blob_socket_io_test, receive_creates_missing_parent_directory) {
    blob_id_type blob_id = 42424242;
    auto path = resolver_->resolve_path(blob_id);
    auto parent = path.parent_path();
    auto grandparent = parent.parent_path();

    // Prepare source file and wire
    boost::filesystem::create_directories(parent);
    std::ofstream ofs(path.string(), std::ios::binary);
    ofs << "test_data";
    ofs.close();

    blob_socket_io sender("", *resolver_);
    sender.send_blob(blob_id);
    std::string wire = sender.get_out_string();

    // Remove only the immediate parent directory
    boost::filesystem::remove_all(parent);
    // Ensure grandparent still exists
    boost::filesystem::create_directories(grandparent);

    blob_socket_io receiver(wire, *resolver_);
    EXPECT_EQ(receiver.receive_blob(), blob_id);

    std::ifstream ifs(path.string(), std::ios::binary);
    std::ostringstream oss;
    oss << ifs.rdbuf();
    EXPECT_EQ(oss.str(), "test_data");
}

TEST_F(blob_socket_io_test, receive_fails_when_grandparent_missing) {
    blob_id_type blob_id = 42424243;
    auto path = resolver_->resolve_path(blob_id);
    auto parent = path.parent_path();
    auto grandparent = parent.parent_path();

    // Prepare source file and wire
    boost::filesystem::create_directories(parent);
    std::ofstream ofs(path.string(), std::ios::binary);
    ofs << "test_data";
    ofs.close();

    blob_socket_io sender("", *resolver_);
    sender.send_blob(blob_id);
    std::string wire = sender.get_out_string();

    // Remove grandparent (and thus the parent)
    boost::filesystem::remove_all(grandparent);

    blob_socket_io receiver(wire, *resolver_);
    EXPECT_THROW(receiver.receive_blob(), std::runtime_error);
}


}  // namespace limestone::testing
