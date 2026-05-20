#include <gtest/gtest.h>

#include <fstream>
#include <utility>
#include <vector>

#include <boost/filesystem.hpp>

#include "test_root.h"
#include "replication/opened_blob_file.h"
#include <limestone/api/blob_file.h>
#include <limestone/api/datastore.h>

namespace limestone::testing {

using namespace limestone::api;
using namespace limestone::replication;

constexpr const char* base_directory = "/tmp/opened_blob_file_test";

class opened_blob_file_test : public ::testing::Test {
protected:
    void SetUp() override {
        [[maybe_unused]] int rm_result = system(("rm -rf " + std::string(base_directory)).c_str());
        [[maybe_unused]] int mkdir_result = system(("mkdir -p " + std::string(base_directory)).c_str());

        configuration conf{};
        conf.set_data_location(base_directory);
        datastore_ = std::make_unique<datastore_test>(conf);
    }

    void TearDown() override {
        datastore_.reset();
        [[maybe_unused]] int rm_result = system(("rm -rf " + std::string(base_directory)).c_str());
    }

    [[nodiscard]] boost::filesystem::path write_blob_file(
            blob_id_type blob_id,
            std::string const& content) const {
        blob_file blob = datastore_->get_blob_file(blob_id);
        boost::filesystem::path path = blob.path();
        boost::filesystem::create_directories(path.parent_path());
        std::ofstream ofs(path.string(), std::ios::binary);
        ofs << content;
        return path;
    }

    std::unique_ptr<datastore_test> datastore_{};
};

TEST_F(opened_blob_file_test, open_for_send_opens_regular_file) {
    blob_id_type blob_id = 1001;
    std::string const content = "blob-send-utils";
    boost::filesystem::path path = write_blob_file(blob_id, content);

    opened_blob_file opened = opened_blob_file::open_for_send(*datastore_, blob_id);
    EXPECT_EQ(opened.path(), path);
    EXPECT_EQ(opened.size(), content.size());

    std::vector<std::uint8_t> buffer(4);
    ASSERT_EQ(opened.read_chunk(buffer.data(), buffer.size()), buffer.size());
    EXPECT_EQ(std::string(buffer.begin(), buffer.end()), content.substr(0, buffer.size()));
}

TEST_F(opened_blob_file_test, open_for_send_resolves_symlink) {
    blob_id_type blob_id = 1002;
    std::string const content = "symlink-blob";
    boost::filesystem::path original_path = write_blob_file(blob_id, content);
    boost::filesystem::path target_path = original_path.parent_path() / "blob-target.dat";

    boost::filesystem::rename(original_path, target_path);
    boost::filesystem::create_symlink(target_path, original_path);

    opened_blob_file opened = opened_blob_file::open_for_send(*datastore_, blob_id);
    EXPECT_EQ(opened.path(), boost::filesystem::canonical(target_path));
    EXPECT_EQ(opened.size(), content.size());
}

TEST_F(opened_blob_file_test, open_for_send_rejects_directory) {
    blob_id_type blob_id = 1003;
    blob_file blob = datastore_->get_blob_file(blob_id);
    boost::filesystem::path path = blob.path();
    boost::filesystem::create_directories(path);

    EXPECT_THROW((void)opened_blob_file::open_for_send(*datastore_, blob_id), std::runtime_error);
}

TEST_F(opened_blob_file_test, read_blob_chunk_reads_exact_bytes) {
    blob_id_type blob_id = 1004;
    std::string const content = "0123456789";
    [[maybe_unused]] boost::filesystem::path path = write_blob_file(blob_id, content);

    opened_blob_file opened = opened_blob_file::open_for_send(*datastore_, blob_id);

    std::vector<std::uint8_t> buffer(6);
    std::size_t bytes_read = opened.read_chunk(buffer.data(), buffer.size());
    EXPECT_EQ(bytes_read, buffer.size());
    EXPECT_EQ(std::string(buffer.begin(), buffer.end()), content.substr(0, buffer.size()));
}

TEST_F(opened_blob_file_test, move_constructor_transfers_open_file) {
    blob_id_type blob_id = 1005;
    std::string const content = "move-constructor";
    boost::filesystem::path path = write_blob_file(blob_id, content);

    opened_blob_file opened = opened_blob_file::open_for_send(*datastore_, blob_id);
    opened_blob_file moved(std::move(opened));

    EXPECT_EQ(moved.path(), path);
    EXPECT_EQ(moved.size(), content.size());

    std::vector<std::uint8_t> buffer(4);
    ASSERT_EQ(moved.read_chunk(buffer.data(), buffer.size()), buffer.size());
    EXPECT_EQ(std::string(buffer.begin(), buffer.end()), content.substr(0, buffer.size()));
}

TEST_F(opened_blob_file_test, move_assignment_transfers_open_file) {
    blob_id_type source_blob_id = 1006;
    blob_id_type target_blob_id = 1007;
    std::string const source_content = "move-assignment-source";
    std::string const target_content = "move-assignment-target";
    boost::filesystem::path source_path = write_blob_file(source_blob_id, source_content);
    [[maybe_unused]] boost::filesystem::path target_path =
            write_blob_file(target_blob_id, target_content);

    opened_blob_file source = opened_blob_file::open_for_send(*datastore_, source_blob_id);
    opened_blob_file target = opened_blob_file::open_for_send(*datastore_, target_blob_id);

    target = std::move(source);

    EXPECT_EQ(target.path(), source_path);
    EXPECT_EQ(target.size(), source_content.size());

    std::vector<std::uint8_t> buffer(4);
    ASSERT_EQ(target.read_chunk(buffer.data(), buffer.size()), buffer.size());
    EXPECT_EQ(std::string(buffer.begin(), buffer.end()), source_content.substr(0, buffer.size()));
}

TEST_F(opened_blob_file_test, read_blob_chunk_throws_on_unexpected_eof) {
    blob_id_type blob_id = 1008;
    std::string const content = "short";
    [[maybe_unused]] boost::filesystem::path path = write_blob_file(blob_id, content);

    opened_blob_file opened = opened_blob_file::open_for_send(*datastore_, blob_id);

    std::vector<std::uint8_t> buffer(content.size() + 1);
    EXPECT_THROW({
        [[maybe_unused]] std::size_t bytes_read =
            opened.read_chunk(buffer.data(), buffer.size());
    }, std::runtime_error);
}

}  // namespace limestone::testing
