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

#include "limestone/grpc/backend/backend_shared_impl.h"
#include <gtest/gtest.h>
#include <boost/filesystem.hpp>
#include <fstream>
#include <vector>
#include "limestone/grpc/service/message_versions.h"

namespace limestone::testing {

using limestone::grpc::proto::BackupObject;
using limestone::grpc::proto::BackupObjectType;

using namespace limestone::grpc::backend;

using namespace limestone::internal;

class backend_shared_impl_test : public ::testing::Test {
protected:
    boost::filesystem::path temp_dir = "/tmp/backend_shared_impl_test";

    void SetUp() override {
        boost::filesystem::create_directories(temp_dir);
    }
    void TearDown() override {
        boost::filesystem::remove_all(temp_dir);
    }

    class seekg_fail_file_operations : public limestone::internal::real_file_operations {
    public:
        enum fail_timing { FIRST, SECOND };
        explicit seekg_fail_file_operations(fail_timing timing) : fail_timing_(timing), seekg_count_(0) {}

        void ifs_seekg(std::ifstream& ifs, std::streamoff offset, std::ios_base::seekdir way) override {
            ++seekg_count_;
            if ((fail_timing_ == FIRST && seekg_count_ == 1) || (fail_timing_ == SECOND && seekg_count_ == 2)) {
                fail_ = true;
                errno = ENXIO;
            } else {
                fail_ = false;
                ifs.seekg(offset, way);
            }
        }
        bool ifs_fail(std::ifstream&) override { return fail_; }

    private:
        fail_timing fail_timing_;
        int seekg_count_ = 0;
        bool fail_ = false;
    };
};

class dummy_writer : public i_writer {
public:
    std::vector<limestone::grpc::proto::GetObjectResponse> responses;
    bool fail_write = false;
    bool Write(const limestone::grpc::proto::GetObjectResponse& resp) override {
        if (fail_write) return false;
        responses.push_back(resp);
        return true;
    }
};

TEST_F(backend_shared_impl_test, list_wal_history_returns_empty_when_dir_is_empty) {
    backend_shared_impl backend(temp_dir);
    auto result = backend.list_wal_history();
    EXPECT_TRUE(result.empty());
}

TEST_F(backend_shared_impl_test, list_wal_history_matches_wal_history_class) {
    wal_history wh(temp_dir);
    wh.append(123);
    wh.append(456);
    auto expected = wh.list();

    backend_shared_impl backend(temp_dir);
    auto actual = backend.list_wal_history();

    ASSERT_EQ(expected.size(), actual.size());
    for (size_t i = 0; i < expected.size(); ++i) {
        EXPECT_EQ(expected[i].epoch, actual[i].epoch());
        EXPECT_EQ(expected[i].identity, actual[i].identity());
        EXPECT_EQ(expected[i].timestamp, actual[i].timestamp());
    }
}

TEST_F(backend_shared_impl_test, make_backup_object_from_path_metadata_files) {
    std::vector<std::string> files = {
        "compaction_catalog",
        "limestone-manifest.json",
        "wal_history",
        "epoch.1234567890.1"
    };
    for (const auto& fname : files) {
        auto obj = backend_shared_impl::make_backup_object_from_path(fname);
        ASSERT_TRUE(obj.has_value());
        EXPECT_EQ(obj->object_id(), fname);
        EXPECT_EQ(obj->path(), fname);
        EXPECT_EQ(obj->type(), backup_object_type::metadata);
    }
}

TEST_F(backend_shared_impl_test, make_backup_object_from_path_snapshot) {
    std::string fname = "pwal_0000.compacted";
    auto obj = backend_shared_impl::make_backup_object_from_path(fname);
    ASSERT_TRUE(obj.has_value());
    EXPECT_EQ(obj->object_id(), fname);
    EXPECT_EQ(obj->path(), fname);
    EXPECT_EQ(obj->type(), backup_object_type::snapshot);
}

TEST_F(backend_shared_impl_test, make_backup_object_from_path_log) {
    std::string fname = "pwal_0001.1234567890.0";
    auto obj = backend_shared_impl::make_backup_object_from_path(fname);
    ASSERT_TRUE(obj.has_value());
    EXPECT_EQ(obj->object_id(), fname);
    EXPECT_EQ(obj->path(), fname);
    EXPECT_EQ(obj->type(), backup_object_type::log);
}

TEST_F(backend_shared_impl_test, make_backup_object_from_path_not_matched) {
    std::string fname = "random_file.txt";
    auto obj = backend_shared_impl::make_backup_object_from_path(fname);
    EXPECT_FALSE(obj.has_value());
}

TEST_F(backend_shared_impl_test, keep_alive_success_and_not_found) {
    backend_shared_impl backend(temp_dir);
    // Create a session
    auto session_opt = backend.create_and_register_session(0, 0, 60, nullptr);
    ASSERT_TRUE(session_opt.has_value());
    std::string session_id = session_opt->session_id();

    // Normal case: version matches and session is valid
    limestone::grpc::proto::KeepAliveRequest req;
    req.set_version(limestone::grpc::service::keep_alive_message_version);
    req.set_session_id(session_id);
    limestone::grpc::proto::KeepAliveResponse resp;
    auto status = backend.keep_alive(&req, &resp);
    EXPECT_TRUE(status.ok());
    auto session_for_check = backend.get_session_store().get_session(session_id);
    ASSERT_TRUE(session_for_check.has_value());
    EXPECT_EQ(resp.expire_at(), session_for_check->expire_at());

    // Version mismatch
    req.set_version(9999);
    status = backend.keep_alive(&req, &resp);
    EXPECT_EQ(status.error_code(), ::grpc::StatusCode::INVALID_ARGUMENT);
    EXPECT_STREQ(status.error_message().c_str(), "unsupported keep_alive request version");

    // Unregistered session ID
    req.set_version(limestone::grpc::service::keep_alive_message_version);
    req.set_session_id("not_found_id");
    status = backend.keep_alive(&req, &resp);
    EXPECT_EQ(status.error_code(), ::grpc::StatusCode::NOT_FOUND);
    EXPECT_STREQ(status.error_message().c_str(), "session not found or expired");
}

TEST_F(backend_shared_impl_test, end_backup_success_and_not_found) {
    backend_shared_impl backend(temp_dir);
    // Create a session
    auto session_opt = backend.create_and_register_session(0, 0, 60, nullptr);
    ASSERT_TRUE(session_opt.has_value());
    std::string session_id = session_opt->session_id();

    // Normal case: version matches and session is valid
    limestone::grpc::proto::EndBackupRequest req;
    req.set_version(limestone::grpc::service::end_backup_message_version);
    req.set_session_id(session_id);
    limestone::grpc::proto::EndBackupResponse resp;
    auto status = backend.end_backup(&req, &resp);
    EXPECT_TRUE(status.ok());

    // Version mismatch
    req.set_version(9999);
    status = backend.end_backup(&req, &resp);
    EXPECT_EQ(status.error_code(), ::grpc::StatusCode::INVALID_ARGUMENT);
    EXPECT_STREQ(status.error_message().c_str(), "unsupported end_backup request version");

    // Unregistered session ID
    req.set_version(limestone::grpc::service::end_backup_message_version);
    req.set_session_id("not_found_id");
    status = backend.end_backup(&req, &resp);
    EXPECT_TRUE(status.ok());  // remove_session returns OK even if not found
}

TEST_F(backend_shared_impl_test, get_session_store_returns_registered_sessions) {
    backend_shared_impl backend(temp_dir);
    // Register a session
    auto session_opt = backend.create_and_register_session(123, 456, 30, nullptr);
    ASSERT_TRUE(session_opt.has_value());
    std::string session_id = session_opt->session_id();

    // Access the session_store via get_session_store and verify the session can be retrieved
    const auto& store = backend.get_session_store();
    auto found = store.get_session(session_id);
    ASSERT_TRUE(found.has_value());
    EXPECT_EQ(found->session_id(), session_id);
    EXPECT_EQ(found->begin_epoch(), 123);
    EXPECT_EQ(found->end_epoch(), 456);
}

TEST_F(backend_shared_impl_test, make_stream_error_status_errno_mapping) {
    boost::filesystem::path dummy_path = temp_dir / "file.txt";
    std::string context = "test error";
    std::optional<std::streamoff> offset = 42;

    // ENOENT -> NOT_FOUND
    auto status = backend_shared_impl::make_stream_error_status(context, dummy_path, offset, ENOENT);
    EXPECT_EQ(status.error_code(), ::grpc::StatusCode::NOT_FOUND);
    EXPECT_NE(std::string(status.error_message()).find("test error"), std::string::npos);
    EXPECT_NE(std::string(status.error_message()).find("file.txt"), std::string::npos);
    EXPECT_NE(std::string(status.error_message()).find("offset=42"), std::string::npos);

    // EACCES -> PERMISSION_DENIED
    status = backend_shared_impl::make_stream_error_status(context, dummy_path, std::nullopt, EACCES);
    EXPECT_EQ(status.error_code(), ::grpc::StatusCode::PERMISSION_DENIED);

    // EPERM -> PERMISSION_DENIED
    status = backend_shared_impl::make_stream_error_status(context, dummy_path, std::nullopt, EPERM);
    EXPECT_EQ(status.error_code(), ::grpc::StatusCode::PERMISSION_DENIED);

    // Unknown errno -> INTERNAL
    status = backend_shared_impl::make_stream_error_status(context, dummy_path, std::nullopt, 12345);
    EXPECT_EQ(status.error_code(), ::grpc::StatusCode::INTERNAL);
}






TEST_F(backend_shared_impl_test, send_backup_object_data_success_whole_file) {
    // Prepare a file
    std::string fname = "pwal_0000.compacted";
    std::string content = "abcdefghij";
    auto file_path = temp_dir / fname;
    {
        std::ofstream ofs(file_path.string(), std::ios::binary);
        ofs << content;
    }
    backend_shared_impl backend(temp_dir, 4); // chunk_size = 4
    backup_object obj(fname, backup_object_type::snapshot, fname);

    dummy_writer writer;
    auto status = backend.send_backup_object_data(obj, &writer, 0, std::nullopt);
    EXPECT_TRUE(status.ok());
    // Should be split into 3 chunks: 4, 4, 2 bytes
    ASSERT_EQ(writer.responses.size(), 3);
    EXPECT_EQ(writer.responses[0].chunk(), "abcd");
    EXPECT_EQ(writer.responses[1].chunk(), "efgh");
    EXPECT_EQ(writer.responses[2].chunk(), "ij");
    EXPECT_TRUE(writer.responses[0].is_first());
    EXPECT_FALSE(writer.responses[1].is_first());
    EXPECT_FALSE(writer.responses[2].is_first());
    EXPECT_TRUE(writer.responses[2].is_last());
    EXPECT_EQ(writer.responses[0].total_size(), content.size());
    EXPECT_EQ(writer.responses[1].total_size(), 0); // only first has total_size
    EXPECT_EQ(writer.responses[2].total_size(), 0);
    EXPECT_EQ(writer.responses[0].offset(), 0);
    EXPECT_EQ(writer.responses[1].offset(), 4);
    EXPECT_EQ(writer.responses[2].offset(), 8);
}

TEST_F(backend_shared_impl_test, send_backup_object_data_with_offset_and_end_offset) {
    std::string fname = "pwal_0000.compacted";
    std::string content = "abcdefghij";
    auto file_path = temp_dir / fname;
    {
        std::ofstream ofs(file_path.string(), std::ios::binary);
        ofs << content;
    }
    backend_shared_impl backend(temp_dir, 3); // chunk_size = 3
    backup_object obj(fname, backup_object_type::snapshot, fname);

    dummy_writer writer;
    // Only send "cde" (offset 2 to 5)
    auto status = backend.send_backup_object_data(obj, &writer, 2, 5);
    EXPECT_TRUE(status.ok());
    ASSERT_EQ(writer.responses.size(), 1);
    EXPECT_EQ(writer.responses[0].chunk(), "cde");
    EXPECT_TRUE(writer.responses[0].is_first());
    EXPECT_TRUE(writer.responses[0].is_last());
    EXPECT_EQ(writer.responses[0].offset(), 2);
}

TEST_F(backend_shared_impl_test, send_backup_object_data_start_offset_out_of_range) {
    std::string fname = "pwal_0000.compacted";
    std::string content = "abc";
    auto file_path = temp_dir / fname;
    {
        std::ofstream ofs(file_path.string(), std::ios::binary);
        ofs << content;
    }
    backend_shared_impl backend(temp_dir, 2);
    backup_object obj(fname, backup_object_type::snapshot, fname);

    dummy_writer writer;
    // start_offset > file size
    auto status = backend.send_backup_object_data(obj, &writer, 10, std::nullopt);
    EXPECT_EQ(status.error_code(), ::grpc::StatusCode::OUT_OF_RANGE);
    EXPECT_NE(std::string(status.error_message()).find("start_offset out of range"), std::string::npos);
}

TEST_F(backend_shared_impl_test, send_backup_object_data_end_offset_before_start_offset) {
    std::string fname = "pwal_0000.compacted";
    std::string content = "abc";
    auto file_path = temp_dir / fname;
    {
        std::ofstream ofs(file_path.string(), std::ios::binary);
        ofs << content;
    }
    backend_shared_impl backend(temp_dir, 2);
    backup_object obj(fname, backup_object_type::snapshot, fname);

    dummy_writer writer;
    // end_offset < start_offset
    auto status = backend.send_backup_object_data(obj, &writer, 2, 1);
    EXPECT_EQ(status.error_code(), ::grpc::StatusCode::OUT_OF_RANGE);
    EXPECT_NE(std::string(status.error_message()).find("end_offset before start_offset"), std::string::npos);
}

TEST_F(backend_shared_impl_test, send_backup_object_data_file_not_found) {
    std::string fname = "not_exist_file";
    backend_shared_impl backend(temp_dir, 2);
    backup_object obj(fname, backup_object_type::snapshot, fname);

    dummy_writer writer;
    auto status = backend.send_backup_object_data(obj, &writer, 0, std::nullopt);
    EXPECT_EQ(status.error_code(), ::grpc::StatusCode::NOT_FOUND);
    EXPECT_NE(std::string(status.error_message()).find("failed to open file"), std::string::npos);
}

TEST_F(backend_shared_impl_test, send_backup_object_data_writer_write_fails) {
    std::string fname = "pwal_0000.compacted";
    std::string content = "abcdefgh";
    auto file_path = temp_dir / fname;
    {
        std::ofstream ofs(file_path.string(), std::ios::binary);
        ofs << content;
    }
    backend_shared_impl backend(temp_dir, 4);
    backup_object obj(fname, backup_object_type::snapshot, fname);

    dummy_writer writer;
    writer.fail_write = true;
    auto status = backend.send_backup_object_data(obj, &writer, 0, std::nullopt);
    EXPECT_EQ(status.error_code(), ::grpc::StatusCode::UNKNOWN);
    EXPECT_NE(std::string(status.error_message()).find("stream write failed"), std::string::npos);
}

TEST_F(backend_shared_impl_test, send_backup_object_data_file_truncated_during_read) {
    // Mock file_operations that simulates file truncation during read
    class mock_file_operations : public limestone::internal::real_file_operations {
    public:
        explicit mock_file_operations(const std::string& content) : content_(content) {}
        void ifs_read(std::ifstream&, char* buf, std::streamsize size) override {
            std::streamsize to_read = std::min<std::streamsize>(5, size);
            std::memcpy(buf, content_.data(), to_read);
        }
        bool ifs_eof(std::ifstream&) override {
            return true;
        }
    private:
        std::string content_;
    };
    
    std::string fname = "pwal_0000.compacted";
    std::string content = "abcdefgh"; // 8 bytes, but truncated to 5 bytes
    auto file_path = temp_dir / fname;
    {
        std::ofstream ofs(file_path.string(), std::ios::binary);
        ofs << content;
    }
    backend_shared_impl backend(temp_dir, 10); // chunk_size = 10

    // Inject mock file operations that simulates truncation to 5 bytes
    auto mock_ops = std::make_unique<mock_file_operations>(content);
    backend.set_file_operations(mock_ops.get());
    
    backup_object obj(fname, backup_object_type::snapshot, fname);
    
    struct test_writer : public i_writer {
        bool Write(const limestone::grpc::proto::GetObjectResponse& /*resp*/) override {
            return true;
        }
    } writer;

    auto status = backend.send_backup_object_data(obj, &writer, 0, std::nullopt);

    // Should detect file truncation and return DATA_LOSS
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.error_code(), ::grpc::StatusCode::DATA_LOSS);
    EXPECT_NE(std::string(status.error_message()).find("file truncated during read"), std::string::npos);
}

TEST_F(backend_shared_impl_test, send_backup_object_data_seekg_first_fail) {
    std::string fname = "pwal_0000.compacted";
    std::string content = "abcdefgh";
    auto file_path = temp_dir / fname;
    {
        std::ofstream ofs(file_path.string(), std::ios::binary);
        ofs << content;
    }
    backend_shared_impl backend(temp_dir, 4);
    auto mock_ops = std::make_unique<seekg_fail_file_operations>( seekg_fail_file_operations::FIRST);
    backend.set_file_operations(mock_ops.get());
    backup_object obj(fname, backup_object_type::snapshot, fname);
    struct test_writer : public i_writer {
        bool Write(const limestone::grpc::proto::GetObjectResponse&) override { return true; }
    } writer;
    auto status = backend.send_backup_object_data(obj, &writer, 0, std::nullopt);
    EXPECT_EQ(status.error_code(), ::grpc::StatusCode::INTERNAL);
    EXPECT_NE(std::string(status.error_message()).find("failed to seek to end of file"), std::string::npos);
}

TEST_F(backend_shared_impl_test, send_backup_object_data_seekg_second_fail) {
    std::string fname = "pwal_0000.compacted";
    std::string content = "abcdefgh";
    auto file_path = temp_dir / fname;
    {
        std::ofstream ofs(file_path.string(), std::ios::binary);
        ofs << content;
    }
    backend_shared_impl backend(temp_dir, 4);
    auto mock_ops = std::make_unique<seekg_fail_file_operations>( seekg_fail_file_operations::SECOND);
    backend.set_file_operations(mock_ops.get());
    backup_object obj(fname, backup_object_type::snapshot, fname);
    struct test_writer : public i_writer {
        bool Write(const limestone::grpc::proto::GetObjectResponse&) override { return true; }
    } writer;
    auto status = backend.send_backup_object_data(obj, &writer, 0, std::nullopt);
    EXPECT_EQ(status.error_code(), ::grpc::StatusCode::INTERNAL);
    EXPECT_NE(std::string(status.error_message()).find("failed to seek to start_offset"), std::string::npos);
}

TEST_F(backend_shared_impl_test, send_backup_object_data_tellg_fail) {
    std::string fname = "pwal_0000.compacted";
    std::string content = "abcdefgh";
    auto file_path = temp_dir / fname;
    {
        std::ofstream ofs(file_path.string(), std::ios::binary);
        ofs << content;
    }
    backend_shared_impl backend(temp_dir, 4);

    class : public limestone::internal::real_file_operations {
        std::streampos ifs_tellg(std::ifstream&) override {
            errno = EBADF;
            tellg_called = true;
            return static_cast<std::streampos>(-1);
        }
        bool ifs_fail(std::ifstream& ifs) override {
            if (tellg_called) {
                return true;
            }
            return ifs.fail();
        }
        bool tellg_called = false;
    } tellg_fail_ops;

    backend.set_file_operations(&tellg_fail_ops);
    backup_object obj(fname, backup_object_type::snapshot, fname);
    struct test_writer : public i_writer {
        bool Write(const limestone::grpc::proto::GetObjectResponse&) override { return true; }
    } writer;
    auto status = backend.send_backup_object_data(obj, &writer, 0, std::nullopt);
    EXPECT_EQ(status.error_code(), ::grpc::StatusCode::INTERNAL);
    EXPECT_NE(std::string(status.error_message()).find("failed to get file size"), std::string::npos);
    EXPECT_NE(std::string(status.error_message()).find("errno=9"), std::string::npos); // EBADF=9
}

TEST_F(backend_shared_impl_test, send_backup_object_data_read_badbit) {
    std::string fname = "pwal_0000.compacted";
    std::string content = "abcdefgh";
    auto file_path = temp_dir / fname;
    {
        std::ofstream ofs(file_path.string(), std::ios::binary);
        ofs << content;
    }
    backend_shared_impl backend(temp_dir, 4);
    class : public limestone::internal::real_file_operations {
        bool ifs_bad(std::ifstream&) override { 
            errno = EIO;
            return true;
         }
    } bad_ops;
    backend.set_file_operations(&bad_ops);
    backup_object obj(fname, backup_object_type::snapshot, fname);
    struct test_writer : public i_writer {
        bool Write(const limestone::grpc::proto::GetObjectResponse&) override { return true; }
    } writer;
    auto status = backend.send_backup_object_data(obj, &writer, 0, std::nullopt);
    EXPECT_EQ(status.error_code(), ::grpc::StatusCode::INTERNAL);
    EXPECT_NE(std::string(status.error_message()).find("failed to read file chunk"), std::string::npos);
}

TEST_F(backend_shared_impl_test, send_backup_object_data_read_fail_and_bytes_read_zero) {
    std::string fname = "pwal_0000.compacted";
    std::string content = "abcdefgh";
    auto file_path = temp_dir / fname;
    {
        std::ofstream ofs(file_path.string(), std::ios::binary);
        ofs << content;
    }
    backend_shared_impl backend(temp_dir, 4);

    class : public limestone::internal::real_file_operations {
        void ifs_read(std::ifstream& ifs, char* buf, std::streamsize size) override {
            read_called = true;
            ifs.read(buf, 0);
            auto bytes_read = ifs.gcount();
            std::cerr << "Read " << bytes_read << " bytes from file" << std::endl;
        }
        bool ifs_fail(std::ifstream& ifs) override {
            if (read_called) {
                errno = EIO;
                return true;
            }
            return ifs.fail();
        }
        bool read_called = false;
    } fail_ops;

    backend.set_file_operations(&fail_ops);
    backup_object obj(fname, backup_object_type::snapshot, fname);
    struct test_writer : public i_writer {
        bool Write(const limestone::grpc::proto::GetObjectResponse&) override { return true; }
    } writer;
    auto status = backend.send_backup_object_data(obj, &writer, 0, std::nullopt);
    EXPECT_EQ(status.error_code(), ::grpc::StatusCode::INTERNAL);
    EXPECT_NE(std::string(status.error_message()).find("failed to read file chunk"), std::string::npos);
    EXPECT_NE(std::string(status.error_message()).find("errno=5"), std::string::npos); // EIO=5
}

TEST_F(backend_shared_impl_test, reset_file_operations_to_default_restores_default_ops) {
    backend_shared_impl backend(temp_dir, 4);

    // Create a dummy file_operations to inject
    class dummy_file_operations : public limestone::internal::real_file_operations {
    public:
        bool used = false;
        std::unique_ptr<std::ifstream> open_ifstream(const std::string&, std::ios_base::openmode) override {
            used = true;
            return nullptr;
        }
    } dummy_ops;

    // Set dummy ops and verify it's used
    backend.set_file_operations(&dummy_ops);
    backup_object obj("not_exist_file", backup_object_type::snapshot, "not_exist_file");
    struct test_writer : public i_writer {
        bool Write(const limestone::grpc::proto::GetObjectResponse&) override { return true; }
    } writer;
    backend.send_backup_object_data(obj, &writer, 0, std::nullopt);
    EXPECT_TRUE(dummy_ops.used);

    // Reset to default and verify dummy_ops is not used anymore
    dummy_ops.used = false;
    backend.reset_file_operations_to_default();
    backend.send_backup_object_data(obj, &writer, 0, std::nullopt);
    EXPECT_FALSE(dummy_ops.used); // Should use default ops, not dummy_ops
}


} // namespace limestone::testing

