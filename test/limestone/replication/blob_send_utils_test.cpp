#include <gtest/gtest.h>

#include <cstdio>
#include <fstream>
#include <vector>

#include <boost/filesystem.hpp>

#include "test_root.h"
#include "replication/blob_send_utils.h"
#include <limestone/api/blob_file.h>
#include <limestone/api/datastore.h>

namespace limestone::testing {

using namespace limestone::api;
using namespace limestone::replication;

constexpr const char* base_directory = "/tmp/blob_send_utils_test";

class blob_send_utils_test : public ::testing::Test {
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

TEST_F(blob_send_utils_test, open_blob_file_for_send_opens_regular_file) {
    blob_id_type blob_id = 1001;
    std::string const content = "blob-send-utils";
    boost::filesystem::path path = write_blob_file(blob_id, content);

    opened_blob_file opened = open_blob_file_for_send(*datastore_, blob_id);
    ASSERT_NE(opened.fp, nullptr);
    EXPECT_EQ(opened.path, path);
    EXPECT_EQ(opened.size, content.size());

    char buffer[4]{};
    ASSERT_EQ(std::fread(buffer, 1, sizeof(buffer), opened.fp), sizeof(buffer));
    EXPECT_EQ(std::string(buffer, sizeof(buffer)), content.substr(0, sizeof(buffer)));

    safe_close_blob_file(opened.fp, "close failed in test");
}

TEST_F(blob_send_utils_test, open_blob_file_for_send_resolves_symlink) {
    blob_id_type blob_id = 1002;
    std::string const content = "symlink-blob";
    boost::filesystem::path original_path = write_blob_file(blob_id, content);
    boost::filesystem::path target_path = original_path.parent_path() / "blob-target.dat";

    boost::filesystem::rename(original_path, target_path);
    boost::filesystem::create_symlink(target_path, original_path);

    opened_blob_file opened = open_blob_file_for_send(*datastore_, blob_id);
    ASSERT_NE(opened.fp, nullptr);
    EXPECT_EQ(opened.path, boost::filesystem::canonical(target_path));
    EXPECT_EQ(opened.size, content.size());

    safe_close_blob_file(opened.fp, "close failed in symlink test");
}

TEST_F(blob_send_utils_test, open_blob_file_for_send_rejects_directory) {
    blob_id_type blob_id = 1003;
    blob_file blob = datastore_->get_blob_file(blob_id);
    boost::filesystem::path path = blob.path();
    boost::filesystem::create_directories(path);

    EXPECT_THROW(open_blob_file_for_send(*datastore_, blob_id), std::runtime_error);
}

TEST_F(blob_send_utils_test, read_blob_chunk_reads_exact_bytes) {
    blob_id_type blob_id = 1004;
    std::string const content = "0123456789";
    [[maybe_unused]] boost::filesystem::path path = write_blob_file(blob_id, content);

    opened_blob_file opened = open_blob_file_for_send(*datastore_, blob_id);
    ASSERT_NE(opened.fp, nullptr);

    std::vector<char> buffer(6);
    std::size_t bytes_read = read_blob_chunk(
        opened.fp, opened.path, buffer.data(), buffer.size());
    EXPECT_EQ(bytes_read, buffer.size());
    EXPECT_EQ(std::string(buffer.begin(), buffer.end()), content.substr(0, buffer.size()));

    safe_close_blob_file(opened.fp, "close failed in read test");
}

TEST_F(blob_send_utils_test, read_blob_chunk_throws_on_unexpected_eof) {
    blob_id_type blob_id = 1005;
    std::string const content = "short";
    [[maybe_unused]] boost::filesystem::path path = write_blob_file(blob_id, content);

    opened_blob_file opened = open_blob_file_for_send(*datastore_, blob_id);
    ASSERT_NE(opened.fp, nullptr);

    std::vector<char> buffer(content.size() + 1);
    EXPECT_THROW({
        [[maybe_unused]] std::size_t bytes_read =
            read_blob_chunk(opened.fp, opened.path, buffer.data(), buffer.size());
    }, std::runtime_error);

    safe_close_blob_file(opened.fp, "close failed in eof test");
}

TEST_F(blob_send_utils_test, safe_close_blob_file_accepts_nullptr) {
    EXPECT_NO_THROW(safe_close_blob_file(nullptr, "close failed in nullptr test"));
}

}  // namespace limestone::testing
