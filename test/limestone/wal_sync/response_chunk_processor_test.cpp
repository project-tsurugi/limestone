#include <algorithm>
#include <fstream>
#include <optional>
#include <string>
#include <vector>

#include <boost/filesystem.hpp>
#include <gtest/gtest.h>

#include "file_operations.h"
#include "wal_sync/response_chunk_processor.h"
#include "wal_sync/backup_object_type.h"

namespace limestone::testing {

using limestone::internal::backup_object_type;
using limestone::internal::real_file_operations;
using limestone::internal::response_chunk_processor;
using limestone::internal::backup_object;

namespace {

struct test_object {
    std::string id;
    std::string path;
    std::string payload;
};

class response_chunk_processor_test : public ::testing::Test {
protected:
    void SetUp() override {
        base_dir_ = boost::filesystem::path{"/tmp/response_chunk_processor_test"};
        boost::filesystem::remove_all(base_dir_);
        ASSERT_TRUE(boost::filesystem::create_directories(base_dir_));
    }

    void TearDown() override {
        boost::filesystem::remove_all(base_dir_);
    }

static limestone::grpc::proto::GetObjectResponse build_chunk(
    std::string const& object_id,
    std::string const& path,
    std::string const& data,
    std::uint64_t offset,
    bool is_first,
    bool is_last,
    std::optional<std::uint64_t> total_size = std::nullopt
) {
        limestone::grpc::proto::GetObjectResponse response;
        auto* object = response.mutable_object();
        object->set_object_id(object_id);
        object->set_path(path);
        response.set_offset(offset);
        response.set_chunk(data);
        response.set_is_first(is_first);
        response.set_is_last(is_last);
        if (is_first && total_size.has_value()) {
            response.set_total_size(total_size.value());
        }
        return response;
    }

    static std::vector<backup_object> to_backup_objects(std::vector<test_object> const& objs) {
        std::vector<backup_object> result;
        result.reserve(objs.size());
        for (auto const& obj : objs) {
            result.emplace_back(backup_object{
                obj.id,
                backup_object_type::metadata,
                obj.path
            });
        }
        return result;
    }

    static std::string load_file(boost::filesystem::path const& path) {
        std::ifstream ifs(path.string(), std::ios::binary);
        return {std::istreambuf_iterator<char>(ifs), std::istreambuf_iterator<char>()};
    }

    static response_chunk_processor::transfer_state_snapshot const* find_state(
        std::vector<response_chunk_processor::transfer_state_snapshot> const& states,
        std::string const& object_id
    ) {
        for (auto const& snapshot : states) {
            if (snapshot.object_id == object_id) {
                return &snapshot;
            }
        }
        return nullptr;
    }

    boost::filesystem::path base_dir_;
    real_file_operations file_ops_;
};

} // namespace

TEST_F(response_chunk_processor_test, known_objects_are_written) {
    std::vector<test_object> objects{{"meta", "meta/info.json", "hello"}};
    auto backup_objects = to_backup_objects(objects);
    response_chunk_processor processor(file_ops_, base_dir_, backup_objects);

    auto response = build_chunk("meta", "meta/info.json", "hello", 0, true, true, 5);
    processor.handle_response(response);

    EXPECT_TRUE(processor.all_completed());
    EXPECT_FALSE(processor.failed());

    auto states = processor.snapshot_states();
    ASSERT_EQ(states.size(), 1U);
    auto const* meta_state = find_state(states, "meta");
    ASSERT_NE(meta_state, nullptr);
    EXPECT_TRUE(meta_state->completed);
    EXPECT_TRUE(meta_state->saw_first_chunk);
    EXPECT_EQ(meta_state->received_bytes, 5U);
    EXPECT_EQ(meta_state->expected_total_size, 5U);
    EXPECT_EQ(meta_state->final_path, base_dir_ / "meta/info.json");

    auto produced = load_file(base_dir_ / "meta/info.json");
    EXPECT_EQ(produced, "hello");
}

TEST_F(response_chunk_processor_test, empty_chunk_is_handled) {
    std::vector<test_object> objects{{"meta", "meta/info", ""}};
    auto backup_objects = to_backup_objects(objects);
    response_chunk_processor processor(file_ops_, base_dir_, backup_objects);

    auto response = build_chunk("meta", "meta/info", "", 0, true, true, std::uint64_t{0});
    processor.handle_response(response);

    EXPECT_TRUE(processor.all_completed());
    EXPECT_TRUE(! processor.failed());

    auto produced = load_file(base_dir_ / "meta/info");
    EXPECT_TRUE(produced.empty());
}

TEST_F(response_chunk_processor_test, missing_object_metadata_fails) {
    std::vector<test_object> objects{{"meta", "meta/info", "data"}};
    auto backup_objects = to_backup_objects(objects);
    response_chunk_processor processor(file_ops_, base_dir_, backup_objects);

    limestone::grpc::proto::GetObjectResponse response;
    processor.handle_response(response);

    EXPECT_TRUE(processor.failed());
    EXPECT_NE(processor.error_message().find("received response without object metadata"), std::string::npos);

    auto states = processor.snapshot_states();
    ASSERT_EQ(states.size(), 1U);
    auto const* meta_state = find_state(states, "meta");
    ASSERT_NE(meta_state, nullptr);
    EXPECT_FALSE(meta_state->saw_first_chunk);
    EXPECT_EQ(meta_state->received_bytes, 0U);
}

TEST_F(response_chunk_processor_test, unknown_child_object_is_accepted) {
    std::vector<test_object> objects{{"parent", "parent/file", "parent-data"}};
    auto backup_objects = to_backup_objects(objects);
    response_chunk_processor processor(file_ops_, base_dir_, backup_objects);

    processor.handle_response(build_chunk("parent", "parent/file", "parent-data", 0, true, true, 11));
    processor.handle_response(build_chunk("child", "parent/blob.bin", "chunk", 0, true, false, 10));
    processor.handle_response(build_chunk("child", "parent/blob.bin", "chunk", 5, false, true));

    EXPECT_TRUE(processor.all_completed());
    EXPECT_FALSE(processor.failed());

    auto states = processor.snapshot_states();
    ASSERT_EQ(states.size(), 2U);
    auto const* parent_state = find_state(states, "parent");
    auto const* child_state = find_state(states, "child");
    ASSERT_NE(parent_state, nullptr);
    ASSERT_NE(child_state, nullptr);
    EXPECT_TRUE(parent_state->completed);
    EXPECT_TRUE(child_state->completed);
    EXPECT_EQ(parent_state->received_bytes, 11U);
    EXPECT_EQ(child_state->received_bytes, 10U);
    EXPECT_EQ(parent_state->final_path, base_dir_ / "parent/file");
    EXPECT_EQ(child_state->final_path, base_dir_ / "parent/blob.bin");

    auto parent_content = load_file(base_dir_ / "parent/file");
    auto child_content = load_file(base_dir_ / "parent/blob.bin");
    EXPECT_EQ(parent_content, "parent-data");
    EXPECT_EQ(child_content, "chunkchunk");
}

TEST_F(response_chunk_processor_test, first_chunk_requires_non_empty_path) {
    std::vector<test_object> objects{{"meta", "meta/info", "abc"}};
    auto backup_objects = to_backup_objects(objects);
    response_chunk_processor processor(file_ops_, base_dir_, backup_objects);

    processor.handle_response(build_chunk("meta", "", "abc", 0, true, false, 3));

    EXPECT_TRUE(processor.failed());
    EXPECT_NE(processor.error_message().find("object path is empty"), std::string::npos);
}

TEST_F(response_chunk_processor_test, first_chunk_flag_must_be_set) {
    std::vector<test_object> objects{{"meta", "meta/info", "abc"}};
    auto backup_objects = to_backup_objects(objects);
    response_chunk_processor processor(file_ops_, base_dir_, backup_objects);

    processor.handle_response(build_chunk("meta", "meta/info", "abc", 0, false, false, 3));

    EXPECT_TRUE(processor.failed());
    EXPECT_NE(processor.error_message().find("first chunk must be marked as is_first"), std::string::npos);
}

TEST_F(response_chunk_processor_test, first_chunk_requires_relative_path) {
    std::vector<test_object> objects{{"meta", "meta/info", "abc"}};
    auto backup_objects = to_backup_objects(objects);
    response_chunk_processor processor(file_ops_, base_dir_, backup_objects);

    processor.handle_response(build_chunk("meta", "/absolute/path", "abc", 0, true, false, 3));

    EXPECT_TRUE(processor.failed());
    EXPECT_NE(processor.error_message().find("object path must be relative"), std::string::npos);
}

TEST_F(response_chunk_processor_test, first_chunk_path_must_not_contain_dotdot) {
    std::vector<test_object> objects{{"meta", "meta/info", "abc"}};
    auto backup_objects = to_backup_objects(objects);
    response_chunk_processor processor(file_ops_, base_dir_, backup_objects);

    processor.handle_response(build_chunk("meta", "../escape", "abc", 0, true, false, 3));

    EXPECT_TRUE(processor.failed());
    EXPECT_NE(processor.error_message().find("object path must not contain '..'"), std::string::npos);
}

TEST_F(response_chunk_processor_test, first_chunk_path_must_match_manifest) {
    std::vector<test_object> objects{{"meta", "meta/info", "abc"}};
    auto backup_objects = to_backup_objects(objects);
    response_chunk_processor processor(file_ops_, base_dir_, backup_objects);

    processor.handle_response(build_chunk("meta", "meta/other", "abc", 0, true, false, 3));

    EXPECT_TRUE(processor.failed());
    EXPECT_NE(processor.error_message().find("object path mismatch"), std::string::npos);
}

TEST_F(response_chunk_processor_test, unknown_object_without_first_chunk_fails) {
    std::vector<test_object> objects{{"meta", "meta/info", "abcdef"}};
    auto backup_objects = to_backup_objects(objects);
    response_chunk_processor processor(file_ops_, base_dir_, backup_objects);

    processor.handle_response(build_chunk("child", "data/file", "chunk", 0, false, false, 5));

    EXPECT_TRUE(processor.failed());
    EXPECT_FALSE(processor.all_completed());
    EXPECT_FALSE(boost::filesystem::exists(base_dir_ / "data/file"));
    EXPECT_NE(processor.error_message().find("received chunk before first for unknown object_id"), std::string::npos);

    auto states = processor.snapshot_states();
    ASSERT_EQ(states.size(), 1U);
    auto const* meta_state = find_state(states, "meta");
    ASSERT_NE(meta_state, nullptr);
    EXPECT_FALSE(meta_state->completed);
    EXPECT_FALSE(meta_state->saw_first_chunk);
    EXPECT_EQ(meta_state->received_bytes, 0U);
}

TEST_F(response_chunk_processor_test, unknown_child_with_invalid_path_fails) {
    std::vector<test_object> objects{{"meta", "meta/info", "abcdef"}};
    auto backup_objects = to_backup_objects(objects);
    response_chunk_processor processor(file_ops_, base_dir_, backup_objects);

    processor.handle_response(build_chunk("child", "../escape", "data", 0, true, false, std::uint64_t{4}));

    EXPECT_TRUE(processor.failed());
    EXPECT_NE(processor.error_message().find("object path must not contain '..'"), std::string::npos);
}

TEST_F(response_chunk_processor_test, create_directories_failure_is_reported) {
    struct mock_ops : public real_file_operations {
        void create_directories(const boost::filesystem::path& path, boost::system::error_code& ec) override {
            ec = boost::system::errc::make_error_code(boost::system::errc::permission_denied);
        }
    } mock_file_ops;

    std::vector<test_object> objects{{"meta", "branch/file", "abc"}};
    auto backup_objects = to_backup_objects(objects);
    response_chunk_processor processor(mock_file_ops, base_dir_, backup_objects);

    processor.handle_response(build_chunk("meta", "branch/file", "abc", 0, true, false, std::uint64_t{3}));

    EXPECT_TRUE(processor.failed());
    EXPECT_NE(processor.error_message().find("failed to create parent directory"), std::string::npos);
}

TEST_F(response_chunk_processor_test, duplicate_first_chunk_fails) {
    std::vector<test_object> objects{{"meta", "meta/info", "abcdef"}};
    auto backup_objects = to_backup_objects(objects);
    response_chunk_processor processor(file_ops_, base_dir_, backup_objects);

    processor.handle_response(build_chunk("meta", "meta/info", "abc", 0, true, false, 6));
    processor.handle_response(build_chunk("meta", "meta/info", "def", 3, true, true));

    EXPECT_TRUE(processor.failed());
    EXPECT_NE(processor.error_message().find("received duplicate first chunk"), std::string::npos);
}

TEST_F(response_chunk_processor_test, chunk_after_completion_fails) {
    std::vector<test_object> objects{{"meta", "meta/info", "abcdef"}};
    auto backup_objects = to_backup_objects(objects);
    response_chunk_processor processor(file_ops_, base_dir_, backup_objects);

    processor.handle_response(build_chunk("meta", "meta/info", "abcdef", 0, true, true, 6));
    processor.handle_response(build_chunk("meta", "meta/info", "extra", 6, false, true));

    EXPECT_TRUE(processor.failed());
    EXPECT_NE(processor.error_message().find("received chunk after completion"), std::string::npos);
}

TEST_F(response_chunk_processor_test, offset_mismatch_triggers_failure) {
    std::vector<test_object> objects{{"meta", "meta/info", "abcdef"}};
    auto backup_objects = to_backup_objects(objects);
    response_chunk_processor processor(file_ops_, base_dir_, backup_objects);

    processor.handle_response(build_chunk("meta", "meta/info", "abc", 0, true, false, 6));
    processor.handle_response(build_chunk("meta", "meta/info", "def", 4, false, true));

    EXPECT_TRUE(processor.failed());
    EXPECT_FALSE(processor.all_completed());
    EXPECT_FALSE(boost::filesystem::exists(base_dir_ / "meta/info"));
    EXPECT_NE(processor.error_message().find("unexpected offset for object_id"), std::string::npos);

    auto states = processor.snapshot_states();
    ASSERT_EQ(states.size(), 1U);
    auto const* meta_state = find_state(states, "meta");
    ASSERT_NE(meta_state, nullptr);
    EXPECT_FALSE(meta_state->completed);
    EXPECT_TRUE(meta_state->saw_first_chunk);
    EXPECT_EQ(meta_state->received_bytes, 3U);
}

TEST_F(response_chunk_processor_test, total_size_mismatch_fails) {
    std::vector<test_object> objects{{"meta", "meta/info", "abcdef"}};
    auto backup_objects = to_backup_objects(objects);
    response_chunk_processor processor(file_ops_, base_dir_, backup_objects);

    processor.handle_response(build_chunk("meta", "meta/info", "abc", 0, true, false, std::uint64_t{6}));
    processor.handle_response(build_chunk("meta", "meta/info", "de", 3, false, true));

    EXPECT_TRUE(processor.failed());
    EXPECT_NE(processor.error_message().find("size mismatch"), std::string::npos);
}

TEST_F(response_chunk_processor_test, flush_failure_is_reported) {
    class local_mock_file_operations : public real_file_operations {
    public:
        void ofs_flush(std::ofstream& ofs) override {
            real_file_operations::ofs_flush(ofs);
            ofs.setstate(std::ios::badbit);
        }
    } mock_ops;

    std::vector<test_object> objects{{"meta", "meta/info", "abcdef"}};
    auto backup_objects = to_backup_objects(objects);
    response_chunk_processor processor(mock_ops, base_dir_, backup_objects);

    processor.handle_response(build_chunk("meta", "meta/info", "abcdef", 0, true, true, std::uint64_t{6}));

    EXPECT_TRUE(processor.failed());
    EXPECT_NE(processor.error_message().find("failed to flush stream"), std::string::npos);
}

TEST_F(response_chunk_processor_test, close_failure_is_reported) {
    class local_mock_file_operations : public real_file_operations {
    public:
        void ofs_close(std::ofstream& ofs) override {
            real_file_operations::ofs_close(ofs);
            ofs.setstate(std::ios::badbit);
        }
    } mock_ops;

    std::vector<test_object> objects{{"meta", "meta/info", "abcdef"}};
    auto backup_objects = to_backup_objects(objects);
    response_chunk_processor processor(mock_ops, base_dir_, backup_objects);

    processor.handle_response(build_chunk("meta", "meta/info", "abcdef", 0, true, true, std::uint64_t{6}));

    EXPECT_TRUE(processor.failed());
    EXPECT_NE(processor.error_message().find("failed to close stream"), std::string::npos);
}

TEST_F(response_chunk_processor_test, open_output_file_failure_is_reported) {
    std::vector<test_object> objects{{"meta", "meta/info", "abc"}};
    auto backup_objects = to_backup_objects(objects);
    response_chunk_processor processor(file_ops_, base_dir_, backup_objects);

    boost::filesystem::path target_path = base_dir_ / "meta/info";
    boost::filesystem::create_directories(target_path);

    processor.handle_response(build_chunk("meta", "meta/info", "abc", 0, true, false, std::uint64_t{3}));

    EXPECT_TRUE(processor.failed());
    EXPECT_NE(processor.error_message().find("failed to open output file"), std::string::npos);
}

TEST_F(response_chunk_processor_test, output_stream_missing_triggers_failure) {
    class local_mock_file_operations : public real_file_operations {
    public:
        std::ofstream* stored_stream = nullptr;

        std::unique_ptr<std::ofstream> open_ofstream(const std::string& path) override {
            auto stream = real_file_operations::open_ofstream(path);
            stored_stream = stream.get();
            return stream;
        }

        void close_stored_stream() {
            if (stored_stream != nullptr) {
                stored_stream->close();
            }
        }
    } mock_ops;

    std::vector<test_object> objects{{"meta", "meta/info", "abcdef"}};
    auto backup_objects = to_backup_objects(objects);
    response_chunk_processor processor(mock_ops, base_dir_, backup_objects);

    processor.handle_response(build_chunk("meta", "meta/info", "abc", 0, true, false, std::uint64_t{6}));
    mock_ops.close_stored_stream();
    processor.handle_response(build_chunk("meta", "meta/info", "def", 3, false, true));

    EXPECT_TRUE(processor.failed());
    EXPECT_NE(processor.error_message().find("output stream missing"), std::string::npos);
}

TEST_F(response_chunk_processor_test, early_return_after_failure) {
    std::vector<test_object> objects{{"meta", "meta/info", "abcdef"}};
    auto backup_objects = to_backup_objects(objects);
    response_chunk_processor processor(file_ops_, base_dir_, backup_objects);

    processor.handle_response(build_chunk("meta", "meta/info", "abc", 0, true, false, std::uint64_t{6}));
    processor.handle_response(build_chunk("meta", "meta/info", "def", 4, false, true));
    std::string first_error = processor.error_message();

    processor.handle_response(build_chunk("meta", "meta/info", "ghi", 6, false, true));

    EXPECT_TRUE(processor.failed());
    EXPECT_EQ(processor.error_message(), first_error);
    EXPECT_FALSE(boost::filesystem::exists(base_dir_ / "meta/info"));
}

TEST_F(response_chunk_processor_test, write_failure_is_reported) {
    class local_mock_file_operations : public real_file_operations {
    public:
        void ofs_write(std::ofstream& ofs, const char* buf, std::streamsize size) override {
            real_file_operations::ofs_write(ofs, buf, size);
            ofs.setstate(std::ios::badbit);
        }
    } mock_ops;

    std::vector<test_object> objects{{"meta", "meta/info", "abcdef"}};
    auto backup_objects = to_backup_objects(objects);
    response_chunk_processor processor(mock_ops, base_dir_, backup_objects);

    processor.handle_response(build_chunk("meta", "meta/info", "abc", 0, true, false, std::uint64_t{6}));
    processor.handle_response(build_chunk("meta", "meta/info", "def", 3, false, true));

    EXPECT_TRUE(processor.failed());
    EXPECT_NE(processor.error_message().find("failed to write chunk"), std::string::npos);
}

TEST_F(response_chunk_processor_test, cleanup_partials_removes_incomplete_files) {
    std::vector<test_object> objects{
        {"meta", "meta/info", "abcdef"},
        {"child", "meta/child", "xyz"}
    };
    auto backup_objects = to_backup_objects(objects);
    response_chunk_processor processor(file_ops_, base_dir_, backup_objects);

    processor.handle_response(build_chunk("meta", "meta/info", "abcdef", 0, true, true, std::uint64_t{6}));
    processor.handle_response(build_chunk("child", "meta/child", "xyz", 0, true, false, std::uint64_t{3}));

    boost::filesystem::path complete_path = base_dir_ / "meta/info";
    boost::filesystem::path partial_path = base_dir_ / "meta/child";
    EXPECT_TRUE(boost::filesystem::exists(complete_path));
    EXPECT_TRUE(boost::filesystem::exists(partial_path));

    processor.cleanup_partials();

    EXPECT_TRUE(boost::filesystem::exists(complete_path));
    EXPECT_TRUE(! boost::filesystem::exists(partial_path));
}

TEST_F(response_chunk_processor_test, incomplete_object_ids_returns_pending_ids) {
    std::vector<test_object> objects{
        {"meta", "meta/info", "abcdef"},
        {"child", "meta/child", "xyz"},
        {"orphan", "meta/orphan", "uvw"}
    };
    auto backup_objects = to_backup_objects(objects);
    response_chunk_processor processor(file_ops_, base_dir_, backup_objects);

    processor.handle_response(build_chunk("meta", "meta/info", "abcdef", 0, true, true, std::uint64_t{6}));
    processor.handle_response(build_chunk("child", "meta/child", "xyz", 0, true, false, std::uint64_t{3}));

    auto incomplete = processor.incomplete_object_ids();
    std::sort(incomplete.begin(), incomplete.end());
    std::vector<std::string> expected{"child", "orphan"};
    EXPECT_EQ(incomplete, expected);
}
} // namespace limestone::testing
