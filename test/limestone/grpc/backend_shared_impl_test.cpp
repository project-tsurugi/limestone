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
#include <unistd.h>

#include <boost/filesystem.hpp>
#include <cstdio>
#include <fstream>
#include <vector>

#include "backend_test_fixture.h"
#include "limestone/grpc/service/message_versions.h"
#include "log_entry.h"



 

namespace limestone::testing {

using namespace limestone::grpc::backend;

using limestone::api::log_entry;
using limestone::grpc::backend::i_writer;
using limestone::grpc::proto::BackupObject;
using limestone::internal::wal_history;
using limestone::grpc::proto::BeginBackupRequest;
using limestone::grpc::proto::BeginBackupResponse;
using limestone::grpc::service::begin_backup_message_version;
using limestone::grpc::service::session_timeout_seconds;

class backend_shared_impl_test : public backend_test_fixture {
protected:
    const char* get_location() const override { return "/tmp/backend_shared_impl_test"; }

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

 protected:

    
    void create_test_pwal_with_blogs() {
        gen_datastore();
        create_blob_file(*resolver_, 100);
        create_blob_file(*resolver_, 200);
        create_blob_file(*resolver_, 300);
        datastore_->switch_epoch(100);
        lc0_->begin_session();
        create_blob_file(*resolver_, 100);
        lc0_->add_entry(1, "key1", "value1", {100, 100}, {100});
        lc0_->end_session();
        datastore_->switch_epoch(101);
        lc0_->begin_session();
        lc0_->add_entry(1, "key1", "value1", {200, 200}, {200, 300});
        lc0_->end_session();
        datastore_->switch_epoch(102);
        lc0_->begin_session();
        lc0_->add_entry(1, "key1", "value1", {300, 300});
        lc0_->end_session();
        datastore_->switch_epoch(103);
    }

    // Define backup_path_list_provider as a lambda
    backup_path_list_provider_type backup_path_list_provider = [&]() {
        backup_detail_and_rotation_result result = datastore_->get_impl()->begin_backup_with_rotation_result(backup_type::transaction);
        std::vector<boost::filesystem::path> paths;
        if (result.detail) {
            for (const auto& entry : result.detail->entries()) {
                paths.push_back(entry.source_path());
            }
        }
        return paths;
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
    backend_shared_impl backend(get_location());
    auto result = backend.list_wal_history();
    EXPECT_TRUE(result.empty());
}

TEST_F(backend_shared_impl_test, list_wal_history_matches_wal_history_class) {
    wal_history wh(get_location());
    wh.append(123);
    wh.append(456);
    auto expected = wh.list();

    backend_shared_impl backend(get_location());
    auto actual = backend.list_wal_history();

    ASSERT_EQ(expected.size(), actual.size());
    for (size_t i = 0; i < expected.size(); ++i) {
        EXPECT_EQ(expected[i].epoch, actual[i].epoch());
        EXPECT_EQ(expected[i].identity, actual[i].identity());
        EXPECT_EQ(expected[i].timestamp, actual[i].timestamp());
    }
}

TEST_F(backend_shared_impl_test, generate_backup_objects_metadata_files) {
    std::vector<std::string> files = {
        "compaction_catalog",
        "limestone-manifest.json",
        "epoch.1234567890.1"
    };
    for (const auto& fname : files) {
        auto objs = backend_shared_impl::generate_backup_objects({fname}, true);
        ASSERT_EQ(objs.size(), 1);
        const auto& obj = objs[0];
        EXPECT_EQ(obj.object_id(), fname);
        EXPECT_EQ(obj.path(), fname);
        EXPECT_EQ(obj.type(), backup_object_type::metadata);

        objs = backend_shared_impl::generate_backup_objects({fname}, false);
        ASSERT_TRUE(objs.empty());
    }
    for (bool is_full_backup : {true, false}) {
        std::string fname = "wal_history";
        auto objs = backend_shared_impl::generate_backup_objects({fname}, is_full_backup);
        ASSERT_EQ(objs.size(), 1);
        const auto& obj = objs[0];
        EXPECT_EQ(obj.object_id(), fname);
        EXPECT_EQ(obj.path(), fname);
        EXPECT_EQ(obj.type(), backup_object_type::metadata);
    }
}

TEST_F(backend_shared_impl_test, generate_backup_objects_snapshot) {
    std::string fname = "pwal_0000.compacted";
    auto objs = backend_shared_impl::generate_backup_objects({fname}, true);
    ASSERT_EQ(objs.size(), 1);
    const auto& obj = objs[0];
    EXPECT_EQ(obj.object_id(), fname);
    EXPECT_EQ(obj.path(), fname);
    EXPECT_EQ(obj.type(), backup_object_type::snapshot);

    objs = backend_shared_impl::generate_backup_objects({fname}, false);
    ASSERT_TRUE(objs.empty());
}

TEST_F(backend_shared_impl_test, generate_backup_objects_log) {
    std::string fname = "pwal_0001.1234567890.0";
    for (bool is_full_backup : {true, false}) {
        auto objs = backend_shared_impl::generate_backup_objects({fname}, is_full_backup);
        ASSERT_EQ(objs.size(), 1);
        const auto& obj = objs[0];
        EXPECT_EQ(obj.object_id(), fname);
        EXPECT_EQ(obj.path(), fname);
        EXPECT_EQ(obj.type(), backup_object_type::log);
    }
}

TEST_F(backend_shared_impl_test, generate_backup_objects_not_matched) {
    std::string fname = "random_file.txt";
    auto objs = backend_shared_impl::generate_backup_objects({fname}, true);
    EXPECT_TRUE(objs.empty());

    objs = backend_shared_impl::generate_backup_objects({fname}, false);
    EXPECT_TRUE(objs.empty());
}

TEST_F(backend_shared_impl_test, generate_backup_objects_multiple_elements) {
    std::vector<boost::filesystem::path> files = {
        "compaction_catalog",
        "pwal_0000.compacted",
        "pwal_0001.1234567890.0"
    };
    auto objs = backend_shared_impl::generate_backup_objects(files, true);
    ASSERT_EQ(objs.size(), 3);

    EXPECT_EQ(objs[0].object_id(), "compaction_catalog");
    EXPECT_EQ(objs[0].type(), backup_object_type::metadata);

    EXPECT_EQ(objs[1].object_id(), "pwal_0000.compacted");
    EXPECT_EQ(objs[1].type(), backup_object_type::snapshot);

    EXPECT_EQ(objs[2].object_id(), "pwal_0001.1234567890.0");
    EXPECT_EQ(objs[2].type(), backup_object_type::log);
}

TEST_F(backend_shared_impl_test, generate_backup_objects_empty_list) {
    std::vector<boost::filesystem::path> files;
    auto objs = backend_shared_impl::generate_backup_objects(files, true);
    EXPECT_TRUE(objs.empty());
}

TEST_F(backend_shared_impl_test, keep_alive_success_and_not_found) {
    backend_shared_impl backend(get_location());
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
    backend_shared_impl backend(get_location());
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
    backend_shared_impl backend(get_location());
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
    boost::filesystem::path dummy_path = boost::filesystem::path(get_location()) / "file.txt";
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
    auto file_path = boost::filesystem::path(get_location()) / fname;
    {
        std::ofstream ofs(file_path.string(), std::ios::binary);
        ofs << content;
    }
    backend_shared_impl backend(get_location(), 4); // chunk_size = 4
    backup_object obj(fname, backup_object_type::snapshot, fname);

    dummy_writer writer;
    auto status = backend.send_backup_object_data(obj, &writer, byte_range{0, std::nullopt});
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
    auto file_path = boost::filesystem::path(get_location()) / fname;
    {
        std::ofstream ofs(file_path.string(), std::ios::binary);
        ofs << content;
    }
    backend_shared_impl backend(get_location(), 3); // chunk_size = 3
    backup_object obj(fname, backup_object_type::snapshot, fname);

    dummy_writer writer;
    // Only send "cde" (offset 2 to 5)
    auto status = backend.send_backup_object_data(obj, &writer, byte_range{2, 5});
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
    auto file_path = boost::filesystem::path(get_location()) / fname;
    {
        std::ofstream ofs(file_path.string(), std::ios::binary);
        ofs << content;
    }
    backend_shared_impl backend(get_location(), 2);
    backup_object obj(fname, backup_object_type::snapshot, fname);

    dummy_writer writer;
    // start_offset > file size
    auto status = backend.send_backup_object_data(obj, &writer, byte_range{10, std::nullopt});
    EXPECT_EQ(status.error_code(), ::grpc::StatusCode::OUT_OF_RANGE);
    EXPECT_NE(std::string(status.error_message()).find("start_offset out of range"), std::string::npos);
}

TEST_F(backend_shared_impl_test, send_backup_object_data_end_offset_before_start_offset) {
    std::string fname = "pwal_0000.compacted";
    std::string content = "abc";
    auto file_path = boost::filesystem::path(get_location()) / fname;
    {
        std::ofstream ofs(file_path.string(), std::ios::binary);
        ofs << content;
    }
    backend_shared_impl backend(get_location(), 2);
    backup_object obj(fname, backup_object_type::snapshot, fname);

    dummy_writer writer;
    // end_offset < start_offset
    auto status = backend.send_backup_object_data(obj, &writer, byte_range{2, 1});
    EXPECT_EQ(status.error_code(), ::grpc::StatusCode::OUT_OF_RANGE);
    EXPECT_NE(std::string(status.error_message()).find("end_offset before start_offset"), std::string::npos);
}

TEST_F(backend_shared_impl_test, send_backup_object_data_file_not_found) {
    std::string fname = "not_exist_file";
    backend_shared_impl backend(get_location(), 2);
    backup_object obj(fname, backup_object_type::snapshot, fname);

    dummy_writer writer;
    auto status = backend.send_backup_object_data(obj, &writer, byte_range{0, std::nullopt});
    EXPECT_EQ(status.error_code(), ::grpc::StatusCode::NOT_FOUND);
    EXPECT_NE(std::string(status.error_message()).find("failed to open file"), std::string::npos);
}

TEST_F(backend_shared_impl_test, send_backup_object_data_writer_write_fails) {
    std::string fname = "pwal_0000.compacted";
    std::string content = "abcdefgh";
    auto file_path = boost::filesystem::path(get_location()) / fname;
    {
        std::ofstream ofs(file_path.string(), std::ios::binary);
        ofs << content;
    }
    backend_shared_impl backend(get_location(), 4);
    backup_object obj(fname, backup_object_type::snapshot, fname);

    dummy_writer writer;
    writer.fail_write = true;
    auto status = backend.send_backup_object_data(obj, &writer, byte_range{0, std::nullopt});
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
    auto file_path = boost::filesystem::path(get_location()) / fname;
    {
        std::ofstream ofs(file_path.string(), std::ios::binary);
        ofs << content;
    }
    backend_shared_impl backend(get_location(), 10); // chunk_size = 10

    // Inject mock file operations that simulates truncation to 5 bytes
    auto mock_ops = std::make_unique<mock_file_operations>(content);
    backend.set_file_operations(mock_ops.get());
    
    backup_object obj(fname, backup_object_type::snapshot, fname);
    
    struct test_writer : public i_writer {
        bool Write(const limestone::grpc::proto::GetObjectResponse& /*resp*/) override {
            return true;
        }
    } writer;

    auto status = backend.send_backup_object_data(obj, &writer, byte_range{0, std::nullopt});

    // Should detect file truncation and return DATA_LOSS
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.error_code(), ::grpc::StatusCode::DATA_LOSS);
    EXPECT_NE(std::string(status.error_message()).find("file truncated during read"), std::string::npos);
}

TEST_F(backend_shared_impl_test, send_backup_object_data_seekg_first_fail) {
    std::string fname = "pwal_0000.compacted";
    std::string content = "abcdefgh";
    auto file_path = boost::filesystem::path(get_location()) / fname;
    {
        std::ofstream ofs(file_path.string(), std::ios::binary);
        ofs << content;
    }
    backend_shared_impl backend(get_location(), 4);
    auto mock_ops = std::make_unique<seekg_fail_file_operations>( seekg_fail_file_operations::FIRST);
    backend.set_file_operations(mock_ops.get());
    backup_object obj(fname, backup_object_type::snapshot, fname);
    struct test_writer : public i_writer {
        bool Write(const limestone::grpc::proto::GetObjectResponse&) override { return true; }
    } writer;
    auto status = backend.send_backup_object_data(obj, &writer, byte_range{0, std::nullopt});
    EXPECT_EQ(status.error_code(), ::grpc::StatusCode::INTERNAL);
    EXPECT_NE(std::string(status.error_message()).find("failed to seek to end of file"), std::string::npos);
}

TEST_F(backend_shared_impl_test, send_backup_object_data_seekg_second_fail) {
    std::string fname = "pwal_0000.compacted";
    std::string content = "abcdefgh";
    auto file_path = boost::filesystem::path(get_location()) / fname;
    {
        std::ofstream ofs(file_path.string(), std::ios::binary);
        ofs << content;
    }
    backend_shared_impl backend(get_location(), 4);
    auto mock_ops = std::make_unique<seekg_fail_file_operations>( seekg_fail_file_operations::SECOND);
    backend.set_file_operations(mock_ops.get());
    backup_object obj(fname, backup_object_type::snapshot, fname);
    struct test_writer : public i_writer {
        bool Write(const limestone::grpc::proto::GetObjectResponse&) override { return true; }
    } writer;
    auto status = backend.send_backup_object_data(obj, &writer, byte_range{0, std::nullopt});
    EXPECT_EQ(status.error_code(), ::grpc::StatusCode::INTERNAL);
    EXPECT_NE(std::string(status.error_message()).find("failed to seek to start_offset"), std::string::npos);
}

TEST_F(backend_shared_impl_test, send_backup_object_data_tellg_fail) {
    std::string fname = "pwal_0000.compacted";
    std::string content = "abcdefgh";
    auto file_path = boost::filesystem::path(get_location()) / fname;
    {
        std::ofstream ofs(file_path.string(), std::ios::binary);
        ofs << content;
    }
    backend_shared_impl backend(get_location(), 4);

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
    auto status = backend.send_backup_object_data(obj, &writer, byte_range{0, std::nullopt});
    EXPECT_EQ(status.error_code(), ::grpc::StatusCode::INTERNAL);
    EXPECT_NE(std::string(status.error_message()).find("failed to get file size"), std::string::npos);
    EXPECT_NE(std::string(status.error_message()).find("errno=9"), std::string::npos); // EBADF=9
}

TEST_F(backend_shared_impl_test, send_backup_object_data_read_badbit) {
    std::string fname = "pwal_0000.compacted";
    std::string content = "abcdefgh";
    auto file_path = boost::filesystem::path(get_location()) / fname;
    {
        std::ofstream ofs(file_path.string(), std::ios::binary);
        ofs << content;
    }
    backend_shared_impl backend(get_location(), 4);
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
    auto status = backend.send_backup_object_data(obj, &writer, byte_range{0, std::nullopt});
    EXPECT_EQ(status.error_code(), ::grpc::StatusCode::INTERNAL);
    EXPECT_NE(std::string(status.error_message()).find("failed to read file chunk"), std::string::npos);
}

TEST_F(backend_shared_impl_test, send_backup_object_data_read_fail_and_bytes_read_zero) {
    std::string fname = "pwal_0000.compacted";
    std::string content = "abcdefgh";
    auto file_path = boost::filesystem::path(get_location()) / fname;
    {
        std::ofstream ofs(file_path.string(), std::ios::binary);
        ofs << content;
    }
    backend_shared_impl backend(get_location(), 4);

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
    auto status = backend.send_backup_object_data(obj, &writer, byte_range{0, std::nullopt});
    EXPECT_EQ(status.error_code(), ::grpc::StatusCode::INTERNAL);
    EXPECT_NE(std::string(status.error_message()).find("failed to read file chunk"), std::string::npos);
    EXPECT_NE(std::string(status.error_message()).find("errno=5"), std::string::npos); // EIO=5
}

TEST_F(backend_shared_impl_test, reset_file_operations_to_default_restores_default_ops) {
    backend_shared_impl backend(get_location(), 4);

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
    backend.send_backup_object_data(obj, &writer, byte_range{0, std::nullopt});
    EXPECT_TRUE(dummy_ops.used);

    // Reset to default and verify dummy_ops is not used anymore
    dummy_ops.used = false;
    backend.reset_file_operations_to_default();
    backend.send_backup_object_data(obj, &writer, byte_range{0, std::nullopt});
    EXPECT_FALSE(dummy_ops.used); // Should use default ops, not dummy_ops
}



TEST_F(backend_shared_impl_test, prepare_log_object_copy_basic_range_and_blob_extraction) {
    // Create test WAL file with BLOBs
    create_test_pwal_with_blogs();

    // Create backup object
    backup_object obj(
        "test_object_id",
        backup_object_type::log,
        "pwal_0000"
    );

    backend_shared_impl backend(get_location(), 4096);

    std::set<blob_id_type> required_blobs;
    ::grpc::Status error_status;
    std::optional<byte_range> result = backend.prepare_log_object_copy(
        obj,
        0,
        999,
        required_blobs,
        error_status
    );

    // Assert the expected range and required_blobs contents
    EXPECT_EQ(required_blobs.size(), 3);
    EXPECT_TRUE(required_blobs.count(blob_id_type{100}) > 0);
    EXPECT_TRUE(required_blobs.count(blob_id_type{200}) > 0);
    EXPECT_TRUE(required_blobs.count(blob_id_type{300}) > 0);
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(result->start_offset, 0);
    EXPECT_EQ(result->end_offset, std::nullopt);
}

TEST_F(backend_shared_impl_test, prepare_log_object_copy_file_open_fail) {
    backup_object obj(
        "test_object_id",
        backup_object_type::log,
        "not_exist_file"
    );
   
    backend_shared_impl backend(get_location(), 4096);
    

    std::set<blob_id_type> required_blobs;
    ::grpc::Status error_status;
    auto result = backend.prepare_log_object_copy(
        obj,
        0,
        999,
        required_blobs,
        error_status
    );

    EXPECT_FALSE(result.has_value());
    EXPECT_EQ(error_status.error_code(), ::grpc::StatusCode::NOT_FOUND);
    EXPECT_NE(std::string(error_status.error_message()).find("failed to open file"), std::string::npos);
}

TEST_F(backend_shared_impl_test, prepare_log_object_copy_read_fail) {
    // Test using a file (symbolic link to /proc/self/mem) that can be opened but fails to read
    // Create a symbolic link for testing
    auto link_path = boost::filesystem::path(get_location()) / "proc_self_mem_link";
    std::string link_name = link_path.string();
    const char* target = "/proc/self/mem";
    int symlink_result = ::symlink(target, link_name.c_str());
    ASSERT_EQ(symlink_result, 0) << "failed to create symlink to /proc/self/mem";

    backup_object obj(
        "test_object_id",
        backup_object_type::log,
        "proc_self_mem_link"
    );

    backend_shared_impl backend(get_location(), 4096);

    std::set<blob_id_type> required_blobs;
    ::grpc::Status error_status;
    auto result = backend.prepare_log_object_copy(
        obj,
        0,
        999,
        required_blobs,
        error_status
    );

    // Confirm that open succeeds but read_entry_from fails, resulting in an INTERNAL error
    EXPECT_FALSE(result.has_value());
    EXPECT_EQ(error_status.error_code(), ::grpc::StatusCode::INTERNAL);
    EXPECT_NE(std::string(error_status.error_message()).find("file is corrupted: failed to read entry"), std::string::npos);
}

TEST_F(backend_shared_impl_test, prepare_log_object_copy_no_blob_ids) {
    std::string fname = "pwal_empty";
    auto file_path = boost::filesystem::path(get_location()) / fname;
    {
        std::ofstream ofs(file_path.string(), std::ios::binary);
    }
    backup_object obj("test_object_id", backup_object_type::log, fname);
    backend_shared_impl backend(get_location(), 4096);
    std::set<blob_id_type> required_blobs;
    ::grpc::Status error_status;
    auto result = backend.prepare_log_object_copy(
        obj,
        0,
        999,
        required_blobs,
        error_status
    );
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(result->start_offset, 0);
    EXPECT_TRUE(result->end_offset.has_value());
    EXPECT_EQ(result->end_offset, 0);
    EXPECT_TRUE(required_blobs.empty());
}


TEST_F(backend_shared_impl_test, prepare_log_object_copy_duplicate_blob_ids) {
    // Create test WAL file with BLOBs
    create_test_pwal_with_blogs();


    // Create backup object
    backup_object obj(
        "test_object_id",
        backup_object_type::log,
        "pwal_0000"
    );

    backend_shared_impl backend(get_location(), 4096);
    std::set<blob_id_type> required_blobs = {blob_id_type{100}, blob_id_type{400}};
    ::grpc::Status error_status;
    auto result = backend.prepare_log_object_copy(
        obj,
        0,
        999,
        required_blobs,
        error_status
    );

    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(result->start_offset, 0);
    EXPECT_EQ(result->end_offset, std::nullopt);
    EXPECT_EQ(required_blobs.size(), 4);
    EXPECT_TRUE(required_blobs.count(blob_id_type{100}) > 0);
    EXPECT_TRUE(required_blobs.count(blob_id_type{200}) > 0);
    EXPECT_TRUE(required_blobs.count(blob_id_type{300}) > 0);
    EXPECT_TRUE(required_blobs.count(blob_id_type{400}) > 0);
}


TEST_F(backend_shared_impl_test, prepare_log_object_copy_begin0_end0) {
    create_test_pwal_with_blogs();
    backup_object obj("test_object_id", backup_object_type::log, "pwal_0000");
    backend_shared_impl backend(get_location(), 4096);
    std::set<blob_id_type> required_blobs;
    ::grpc::Status error_status;
    auto result = backend.prepare_log_object_copy(
        obj,
        0,
        0,
        required_blobs,
        error_status
    );
    // There is no entry with epoch_id=0, so there are 0 entries in the range and the offset is at the beginning of the file

    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(result->start_offset, 0);
    EXPECT_TRUE(result->end_offset.has_value());
    EXPECT_EQ(result->end_offset, 0);
    EXPECT_TRUE(required_blobs.empty());
}

TEST_F(backend_shared_impl_test, prepare_log_object_copy_begin0_end1) {
    create_test_pwal_with_blogs();
    backup_object obj("test_object_id", backup_object_type::log, "pwal_0000");
    backend_shared_impl backend(get_location(), 4096);
    std::set<blob_id_type> required_blobs;
    ::grpc::Status error_status;
    auto result = backend.prepare_log_object_copy(
        obj,
        0,
        1,
        required_blobs,
        error_status
    );
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(result->start_offset, 0);
    EXPECT_TRUE(result->end_offset.has_value());
    EXPECT_EQ(result->end_offset, 0);
    EXPECT_TRUE(required_blobs.empty());
}

TEST_F(backend_shared_impl_test, prepare_log_object_copy_begin0_end99) {
    create_test_pwal_with_blogs();
    backup_object obj("test_object_id", backup_object_type::log, "pwal_0000");
    backend_shared_impl backend(get_location(), 4096);
    std::set<blob_id_type> required_blobs;
    ::grpc::Status error_status;
    auto result = backend.prepare_log_object_copy(
        obj,
        0,
        99,
        required_blobs,
        error_status
    );
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(result->start_offset, 0);
    EXPECT_TRUE(result->end_offset.has_value());
    EXPECT_EQ(result->end_offset, 0);
    EXPECT_TRUE(required_blobs.empty());
}

TEST_F(backend_shared_impl_test, prepare_log_object_copy_begin0_end100) {
    create_test_pwal_with_blogs();
    backup_object obj("test_object_id", backup_object_type::log, "pwal_0000");
    backend_shared_impl backend(get_location(), 4096);
    std::set<blob_id_type> required_blobs;
    ::grpc::Status error_status;
    auto result = backend.prepare_log_object_copy(
        obj,
        0,
        100,
        required_blobs,
        error_status
    );
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(result->start_offset, 0);
    EXPECT_TRUE(result->end_offset.has_value());
    EXPECT_EQ(result->end_offset, 0);
    EXPECT_TRUE(required_blobs.empty());
}

TEST_F(backend_shared_impl_test, prepare_log_object_copy_begin0_end101) {
    create_test_pwal_with_blogs();
    backup_object obj("test_object_id", backup_object_type::log, "pwal_0000");
    backend_shared_impl backend(get_location(), 4096);
    std::set<blob_id_type> required_blobs;
    ::grpc::Status error_status;
    auto result = backend.prepare_log_object_copy(
        obj,
        0,
        101,
        required_blobs,
        error_status
    );
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(result->start_offset, 0);
    EXPECT_EQ(result->end_offset, 74);
    EXPECT_EQ(required_blobs.size(), 1);
    EXPECT_TRUE(required_blobs.count(blob_id_type{100}) > 0);
}

TEST_F(backend_shared_impl_test, prepare_log_object_copy_begin0_end102) {
    create_test_pwal_with_blogs();
    backup_object obj("test_object_id", backup_object_type::log, "pwal_0000");
    backend_shared_impl backend(get_location(), 4096);
    std::set<blob_id_type> required_blobs;
    ::grpc::Status error_status;
    auto result = backend.prepare_log_object_copy(
        obj,
        0,
        102,
        required_blobs,
        error_status
    );
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(result->start_offset, 0);
    EXPECT_EQ(result->end_offset, 156);
    EXPECT_EQ(required_blobs.size(), 3);
    EXPECT_TRUE(required_blobs.count(blob_id_type{100}) > 0);
    EXPECT_TRUE(required_blobs.count(blob_id_type{200}) > 0);
    EXPECT_TRUE(required_blobs.count(blob_id_type{300}) > 0);
}

TEST_F(backend_shared_impl_test, prepare_log_object_copy_begin0_end103) {
    create_test_pwal_with_blogs();
    backup_object obj("test_object_id", backup_object_type::log, "pwal_0000");
    backend_shared_impl backend(get_location(), 4096);
    std::set<blob_id_type> required_blobs;
    ::grpc::Status error_status;
    auto result = backend.prepare_log_object_copy(
        obj,
        0,
        103,
        required_blobs,
        error_status
    );
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(result->start_offset, 0);
    EXPECT_EQ(result->end_offset, std::nullopt);
    EXPECT_EQ(required_blobs.size(), 3);
    EXPECT_TRUE(required_blobs.count(blob_id_type{100}) > 0);
    EXPECT_TRUE(required_blobs.count(blob_id_type{200}) > 0);
    EXPECT_TRUE(required_blobs.count(blob_id_type{300}) > 0);
}

TEST_F(backend_shared_impl_test, prepare_log_object_copy_begin98_end99) {
    create_test_pwal_with_blogs();
    backup_object obj("test_object_id", backup_object_type::log, "pwal_0000");
    backend_shared_impl backend(get_location(), 4096);
    std::set<blob_id_type> required_blobs;
    ::grpc::Status error_status;
    auto result = backend.prepare_log_object_copy(
        obj,
        98,
        99,
        required_blobs,
        error_status
    );
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(result->start_offset, 0);
    EXPECT_TRUE(result->end_offset.has_value());
    EXPECT_EQ(result->end_offset, 0);
    EXPECT_TRUE(required_blobs.empty());
}

TEST_F(backend_shared_impl_test, prepare_log_object_copy_begin99_end100) {
    create_test_pwal_with_blogs();
    backup_object obj("test_object_id", backup_object_type::log, "pwal_0000");
    backend_shared_impl backend(get_location(), 4096);
    std::set<blob_id_type> required_blobs;
    ::grpc::Status error_status;
    auto result = backend.prepare_log_object_copy(
        obj,
        99,
        100,
        required_blobs,
        error_status
    );
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(result->start_offset, 0);
    EXPECT_TRUE(result->end_offset.has_value());
    EXPECT_EQ(result->end_offset, 0);
    EXPECT_TRUE(required_blobs.empty());
}

TEST_F(backend_shared_impl_test, prepare_log_object_copy_begin100_end101) {
    create_test_pwal_with_blogs();
    backup_object obj("test_object_id", backup_object_type::log, "pwal_0000");
    backend_shared_impl backend(get_location(), 4096);
    std::set<blob_id_type> required_blobs;
    ::grpc::Status error_status;
    auto result = backend.prepare_log_object_copy(
        obj,
        100,
        101,
        required_blobs,
        error_status
    );
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(result->start_offset, 0);
    EXPECT_EQ(result->end_offset, 74);
    EXPECT_EQ(required_blobs.size(), 1);
    EXPECT_TRUE(required_blobs.count(blob_id_type{100}) > 0);
}

TEST_F(backend_shared_impl_test, prepare_log_object_copy_begin101_end102) {
    create_test_pwal_with_blogs();
    backup_object obj("test_object_id", backup_object_type::log, "pwal_0000");
    backend_shared_impl backend(get_location(), 4096);
    std::set<blob_id_type> required_blobs;
    ::grpc::Status error_status;
    auto result = backend.prepare_log_object_copy(
        obj,
        101,
        102,
        required_blobs,
        error_status
    );
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(result->start_offset, 74);
    EXPECT_EQ(result->end_offset, 156);
    EXPECT_EQ(required_blobs.size(), 2);
    EXPECT_TRUE(required_blobs.count(blob_id_type{200}) > 0);
    EXPECT_TRUE(required_blobs.count(blob_id_type{300}) > 0);
}

TEST_F(backend_shared_impl_test, prepare_log_object_copy_begin102_end103) {
    create_test_pwal_with_blogs();
    backup_object obj("test_object_id", backup_object_type::log, "pwal_0000");
    backend_shared_impl backend(get_location(), 4096);
    std::set<blob_id_type> required_blobs;
    ::grpc::Status error_status;
    auto result = backend.prepare_log_object_copy(
        obj,
        102,
        103,
        required_blobs,
        error_status
    );
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(result->start_offset, 156);
    EXPECT_EQ(result->end_offset, std::nullopt);
    EXPECT_TRUE(required_blobs.empty());
}

TEST_F(backend_shared_impl_test, prepare_log_object_copy_begin103_end104) {
    create_test_pwal_with_blogs();
    backup_object obj("test_object_id", backup_object_type::log, "pwal_0000");
    backend_shared_impl backend(get_location(), 4096);
    std::set<blob_id_type> required_blobs;
    ::grpc::Status error_status;
    auto result = backend.prepare_log_object_copy(
        obj,
        103,
        104,
        required_blobs,
        error_status
    );
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(result->start_offset, 0);
    EXPECT_TRUE(result->end_offset.has_value());
    EXPECT_EQ(result->end_offset, 0);
    EXPECT_TRUE(required_blobs.empty());
}

TEST_F(backend_shared_impl_test, get_object_snapshot_success) {
    // Prepare test files
    gen_datastore();
    prepare_backup_test_files();
    backend_shared_impl backend(get_location(), 4); // chunk_size=4
    // Create session
    auto session_opt = backend.create_and_register_session(0, 0, 60, nullptr);
    ASSERT_TRUE(session_opt.has_value());
    std::string session_id = session_opt->session_id();

    // Create backup object
    backup_object obj("pwal_0000.compacted", backup_object_type::snapshot, "pwal_0000.compacted");
    backend.get_session_store().add_backup_object_to_session(session_id, obj);

    // Specify object_id list with pwal_0000.compacted
    limestone::grpc::proto::GetObjectRequest req;
    req.set_version(limestone::grpc::service::get_object_message_version);
    req.set_session_id(session_id);
    req.add_object_id("pwal_0000.compacted");

    // dummy_writer inherits i_writer
    dummy_writer writer;
    // Execute
    auto status = backend.get_object(&req, &writer);
    EXPECT_TRUE(status.ok());
    // At least one response should be returned (may be multiple chunks depending on file size)
    EXPECT_FALSE(writer.responses.empty());
    // The first response's object_id/type/path should be correct
    const auto& first = writer.responses[0];
    EXPECT_EQ(first.object().object_id(), "pwal_0000.compacted");
    EXPECT_EQ(first.object().type(), limestone::grpc::proto::BackupObjectType::SNAPSHOT);
    EXPECT_EQ(first.object().path(), "pwal_0000.compacted");
    // Check is_first, is_last, offset, total_size, etc.
    EXPECT_TRUE(first.is_first());
    EXPECT_EQ(first.offset(), 0);
    EXPECT_EQ(first.total_size() > 0, true);
    // The last response should have is_last=true
    EXPECT_TRUE(writer.responses.back().is_last());
}

TEST_F(backend_shared_impl_test, get_object_error_cases) {
    gen_datastore();
    backend_shared_impl backend(get_location(), 4);
    dummy_writer writer;

    // 1. バージョン不正
    {
        limestone::grpc::proto::GetObjectRequest req;
        req.set_version(9999); // 不正なバージョン
        req.set_session_id("dummy");
        req.add_object_id("pwal_0000.compacted");
        auto status = backend.get_object(&req, &writer);
        EXPECT_EQ(status.error_code(), ::grpc::StatusCode::INVALID_ARGUMENT);
        EXPECT_NE(std::string(status.error_message()).find("unsupported get_object request version"), std::string::npos);
    }

    // 2. 存在しないsession_id
    {
        limestone::grpc::proto::GetObjectRequest req;
        req.set_version(limestone::grpc::service::get_object_message_version);
        req.set_session_id("not_found_session");
        req.add_object_id("pwal_0000.compacted");
        auto status = backend.get_object(&req, &writer);
        EXPECT_EQ(status.error_code(), ::grpc::StatusCode::NOT_FOUND);
        EXPECT_NE(std::string(status.error_message()).find("session not found"), std::string::npos);
    }

    // 3. 存在しないobject_id
    {
        // 正常なsessionを作成
        prepare_backup_test_files();
        auto session_opt = backend.create_and_register_session(0, 0, 60, nullptr);
        ASSERT_TRUE(session_opt.has_value());
        std::string session_id = session_opt->session_id();
        limestone::grpc::proto::GetObjectRequest req;
        req.set_version(limestone::grpc::service::get_object_message_version);
        req.set_session_id(session_id);
        req.add_object_id("not_exist_object");
        auto status = backend.get_object(&req, &writer);
        EXPECT_EQ(status.error_code(), ::grpc::StatusCode::NOT_FOUND);
        EXPECT_NE(std::string(status.error_message()).find("backup object not found"), std::string::npos);
    }
}

TEST_F(backend_shared_impl_test, get_object_writer_write_fails)
{
    // This test simulates a write failure in i_writer (dummy_writer), just like send_backup_object_data_writer_write_fails.
    // It covers the error return path in get_object when writer->Write() fails.
    gen_datastore();
    prepare_backup_test_files();
    backend_shared_impl backend(get_location(), 4); // chunk_size=4
    auto session_opt = backend.create_and_register_session(0, 0, 60, nullptr);
    ASSERT_TRUE(session_opt.has_value());
    std::string session_id = session_opt->session_id();
    backup_object obj("pwal_0000.compacted", backup_object_type::snapshot, "pwal_0000.compacted");
    backend.get_session_store().add_backup_object_to_session(session_id, obj);

    // Prepare request
    limestone::grpc::proto::GetObjectRequest req;
    req.set_version(limestone::grpc::service::get_object_message_version);
    req.set_session_id(session_id);
    req.add_object_id("pwal_0000.compacted");

    dummy_writer writer;
    writer.fail_write = true;
    auto status = backend.get_object(&req, &writer);

    // Check that the error is UNKNOWN and the message contains 'stream write failed'
    EXPECT_EQ(status.error_code(), ::grpc::StatusCode::UNKNOWN);
    EXPECT_NE(std::string(status.error_message()).find("stream write failed"), std::string::npos);
}


TEST_F(backend_shared_impl_test, get_object_log_continue_if_no_end_offset) {
    // Create actual file pwal_0001 and use it as a log object
    gen_datastore();
    prepare_backup_test_files();
    backend_shared_impl backend(get_location(), 4);
    // Create session (begin_epoch=999, end_epoch=999)
    auto session_opt = backend.create_and_register_session(999, 9999, 60, nullptr);
    ASSERT_TRUE(session_opt.has_value());
    std::string session_id = session_opt->session_id();

    // Add log object (pwal_0001) to the session
    backup_object obj("pwal_0001", backup_object_type::log, "pwal_0001");
    backend.get_session_store().add_backup_object_to_session(session_id, obj);

    // Create request
    limestone::grpc::proto::GetObjectRequest req;
    req.set_version(limestone::grpc::service::get_object_message_version);
    req.set_session_id(session_id);
    req.add_object_id("pwal_0001");

    dummy_writer writer;
    // Execute: goes through the continue branch, but send_backup_object_data is not called so the response is empty
    auto status = backend.get_object(&req, &writer);
    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(writer.responses.empty());
}

TEST_F(backend_shared_impl_test, get_object_log_corrupted_file_returns_error_status) {

    // Directly generate a 00-filled file (write multiple 0x00 bytes)
    std::string fname = "pwal_00fill";
    auto file_path = boost::filesystem::path(get_location()) / fname;
    {
        std::ofstream ofs(file_path.string(), std::ios::binary);
        std::vector<char> zeros(256, 0x00); // 256 bytes of 0x00
        ofs.write(zeros.data(), zeros.size());
    }

    backend_shared_impl backend(get_location(), 4096);
    // Create a session
    auto session_opt = backend.create_and_register_session(100, 200, 60, nullptr);
    ASSERT_TRUE(session_opt.has_value());
    std::string session_id = session_opt->session_id();

    // Add log object (pwal_00fill) to the session
    backup_object obj(fname, backup_object_type::log, fname);
    backend.get_session_store().add_backup_object_to_session(session_id, obj);

    // Create request
    limestone::grpc::proto::GetObjectRequest req;
    req.set_version(limestone::grpc::service::get_object_message_version);
    req.set_session_id(session_id);
    req.add_object_id(fname);

    dummy_writer writer;
    // Execute: since the file is corrupted, the error_status branch in prepare_log_object_copy is reached
    auto status = backend.get_object(&req, &writer);
    EXPECT_EQ(status.error_code(), ::grpc::StatusCode::INTERNAL);
    EXPECT_NE(std::string(status.error_message()).find("file is corrupted"), std::string::npos);
    EXPECT_TRUE(writer.responses.empty());
}

TEST_F(backend_shared_impl_test, begin_backup_version_unsupported_0) {
    gen_datastore();
    prepare_backup_test_files();

    backend_shared_impl backend(get_location(), 4);

    BeginBackupRequest request;
    BeginBackupResponse response;

    // version=0 (unsupported)
    request.set_version(0);
    request.set_begin_epoch(0);
    request.set_end_epoch(0);
    auto status = backend.begin_backup(*datastore_, &request, &response, backup_path_list_provider);

    EXPECT_FALSE(status.ok());
    EXPECT_FALSE(backend.get_session_store().get_session(response.session_id()).has_value());
    EXPECT_EQ(status.error_code(), ::grpc::StatusCode::INVALID_ARGUMENT);
}

TEST_F(backend_shared_impl_test, begin_backup_version_supported_1) {
    gen_datastore();
    prepare_backup_test_files();

    backend_shared_impl backend(get_location(), 4);

    BeginBackupRequest request;
    BeginBackupResponse response;

    // version=1 (supported, but not implemented)
    request.set_version(begin_backup_message_version);
    auto status = run_with_epoch_switch(
        [&]() { return backend.begin_backup(*datastore_, &request, &response, backup_path_list_provider); }, 7);

    EXPECT_TRUE(status.ok());
}

TEST_F(backend_shared_impl_test, begin_backup_version_unsupported_2) {
    gen_datastore();
    prepare_backup_test_files();

    backend_shared_impl backend(get_location(), 4);

    BeginBackupRequest request;
    BeginBackupResponse response;

    // version=2 (unsupported)
    request.set_version(2);
    request.set_begin_epoch(0);
    request.set_end_epoch(0);
    auto status = backend.begin_backup(*datastore_, &request, &response, backup_path_list_provider);

    EXPECT_FALSE(status.ok());
    EXPECT_FALSE(backend.get_session_store().get_session(response.session_id()).has_value());
    EXPECT_EQ(status.error_code(), ::grpc::StatusCode::INVALID_ARGUMENT);
}

TEST_F(backend_shared_impl_test, begin_backup_overall) {
    gen_datastore();
    prepare_backup_test_files();
    assert_backup_file_conditions([](const backup_condition& c) { return c.pre_rotation_path; });

    // FLAGS_v = 50; // set VLOG level to 50

    backend_shared_impl backend(get_location(), 4);
    BeginBackupRequest request;
    request.set_version(1);
    request.set_begin_epoch(0);
    request.set_end_epoch(0);
    BeginBackupResponse response;
    
    // Call begin_backup via run_with_epoch_switch to synchronize with epoch switch and log rotation if needed
    auto before = static_cast<int64_t>(std::time(nullptr));
    auto status = run_with_epoch_switch([&]() { return backend.begin_backup(*datastore_, &request, &response, backup_path_list_provider); }, 7);
    auto after = static_cast<int64_t>(std::time(nullptr));

    // Check log_dir after begin_backup
    assert_backup_file_conditions([](const backup_condition& c) { return c.post_rotation_path; });

    // Check that session_id is a valid UUID (xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx)
    const std::string& session_id = response.session_id();
    std::regex uuid_regex(R"([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})");
    EXPECT_TRUE(std::regex_match(session_id, uuid_regex)) << "session_id is not a valid UUID: " << session_id;

    // expire_at should be in [before + session_timeout_seconds, after + session_timeout_seconds]
    int64_t expire_at = response.expire_at();
    EXPECT_GE(expire_at, before + session_timeout_seconds);
    EXPECT_LE(expire_at, after + session_timeout_seconds);
    EXPECT_EQ(response.start_epoch(), 0);
    EXPECT_EQ(response.finish_epoch(), 0);


    auto conditions = get_filtered_backup_conditions([](const backup_condition& cond) {
        return cond.object_type != BackupObjectType::UNSPECIFIED;
    });

    auto objects = response.objects();
    ASSERT_EQ(conditions.size(), static_cast<size_t>(objects.size())) << "Mismatch in number of backup objects between conditions and response";
    for(const auto&obj: objects) {
        auto match = find_matching_backup_conditions(obj.object_id(), conditions);
        ASSERT_FALSE(match.empty()) << "No matching backup condition found for object: " << obj.object_id();
        ASSERT_FALSE(match.size() > 1) << "Multiple matching backup conditions found for object: " << obj.object_id();
        auto cond = match[0];
        EXPECT_EQ(obj.type(), cond.object_type) << "Type mismatch for object: " << obj.object_id() << ", expected: " << cond.object_type << ", actual: " << obj.type();
        EXPECT_TRUE(is_path_matching(obj.path(), cond.post_rotation_path)) << "Path mismatch for object: " << obj.object_id() << ", expected: " << cond.post_rotation_path << ", actual: " << obj.path();
    }

    const auto& session_store = backend.get_session_store();
    auto session_opt = session_store.get_session(session_id);
    ASSERT_TRUE(session_opt.has_value()) << "Session not found for session_id: " << session_id;
    const auto& session = session_opt.value();

    // Convert the backup_object map of the session to proto type and compare

    std::vector<limestone::grpc::proto::BackupObject> session_objects;
    for (auto it = session.begin(); it != session.end(); ++it) {
        session_objects.push_back(it->second.to_proto());
    }
    // The number must match
    EXPECT_EQ(session_objects.size(), static_cast<size_t>(response.objects_size()));
    // Each element must match (order does not matter)
    for (const auto& obj : session_objects) {
        bool found = false;
        for (const auto& resp_obj : response.objects()) {
            if (obj.object_id() == resp_obj.object_id() &&
                obj.path() == resp_obj.path() &&
                obj.type() == resp_obj.type()) {
                found = true;
                break;
            }
        }
        EXPECT_TRUE(found) << "Session backup_object not found in response: id=" << obj.object_id() << ", path=" << obj.path() << ", type=" << obj.type();
    }
    // And vice versa
    for (const auto& resp_obj : response.objects()) {
        bool found = false;
        for (const auto& obj : session_objects) {
            if (obj.object_id() == resp_obj.object_id() &&
                obj.path() == resp_obj.path() &&
                obj.type() == resp_obj.type()) {
                found = true;
                break;
            }
        }
        EXPECT_TRUE(found) << "Response backup_object not found in session: id=" << resp_obj.object_id() << ", path=" << resp_obj.path() << ", type=" << resp_obj.type();
    }

    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(backend.get_session_store().get_session(response.session_id()).has_value());
}

// begin_epoch > end_epoch
TEST_F(backend_shared_impl_test, begin_backup_epoch_order_ok) {
    gen_datastore();
    prepare_backup_test_files();
    backend_shared_impl backend(get_location(), 4);
    BeginBackupRequest request;
    BeginBackupResponse response;
    request.set_version(begin_backup_message_version);
    request.set_begin_epoch(3);
    request.set_end_epoch(4);
    auto status = run_with_epoch_switch([&]() { return backend.begin_backup(*datastore_, &request, &response, backup_path_list_provider); }, 7);
    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(backend.get_session_store().get_session(response.session_id()).has_value());
}

TEST_F(backend_shared_impl_test, begin_backup_epoch_order_equal_ng) {
    gen_datastore();
    prepare_backup_test_files();
    backend_shared_impl backend(get_location(), 4);
    BeginBackupRequest request;
    BeginBackupResponse response;
    request.set_version(begin_backup_message_version);
    request.set_begin_epoch(3);
    request.set_end_epoch(3);
    auto status = backend.begin_backup(*datastore_, &request, &response, backup_path_list_provider);
    EXPECT_FALSE(status.ok());
    EXPECT_FALSE(backend.get_session_store().get_session(response.session_id()).has_value());
    EXPECT_EQ(status.error_code(), ::grpc::StatusCode::INVALID_ARGUMENT);
    EXPECT_EQ(status.error_message(), "begin_epoch must be less than end_epoch: begin_epoch=3, end_epoch=3");
}

TEST_F(backend_shared_impl_test, begin_backup_epoch_order_gt_ng) {
    gen_datastore();
    prepare_backup_test_files();
    backend_shared_impl backend(get_location(), 4);
    BeginBackupRequest request;
    BeginBackupResponse response;
    request.set_version(begin_backup_message_version);
    request.set_begin_epoch(4);
    request.set_end_epoch(3);
    auto status = backend.begin_backup(*datastore_, &request, &response, backup_path_list_provider);
    EXPECT_FALSE(status.ok());
    EXPECT_FALSE(backend.get_session_store().get_session(response.session_id()).has_value());
    EXPECT_EQ(status.error_code(), ::grpc::StatusCode::INVALID_ARGUMENT);
    EXPECT_EQ(status.error_message(), "begin_epoch must be less than end_epoch: begin_epoch=4, end_epoch=3");
}

TEST_F(backend_shared_impl_test, begin_backup_begin_epoch_gt_snapshot_ok) {
    gen_datastore();
    prepare_backup_test_files();
    backend_shared_impl backend(get_location(), 4);
    BeginBackupRequest request;
    BeginBackupResponse response;
    request.set_version(begin_backup_message_version);
    EXPECT_EQ(snapshot_epoch_id_, 2);
    request.set_begin_epoch(3);
    request.set_end_epoch(4);
    auto status = run_with_epoch_switch([&]() { return backend.begin_backup(*datastore_, &request, &response, backup_path_list_provider); }, 7);
    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(backend.get_session_store().get_session(response.session_id()).has_value());
}

TEST_F(backend_shared_impl_test, begin_backup_begin_epoch_eq_snapshot_ng) {
    gen_datastore();
    prepare_backup_test_files();
    backend_shared_impl backend(get_location(), 4);
    BeginBackupRequest request;
    BeginBackupResponse response;
    request.set_version(begin_backup_message_version);
    EXPECT_EQ(snapshot_epoch_id_, 2);
    request.set_begin_epoch(2);
    request.set_end_epoch(4);
    auto status = backend.begin_backup(*datastore_, &request, &response, backup_path_list_provider);
    EXPECT_FALSE(status.ok());
    EXPECT_FALSE(backend.get_session_store().get_session(response.session_id()).has_value());
    EXPECT_EQ(status.error_code(), ::grpc::StatusCode::INVALID_ARGUMENT);
    EXPECT_EQ(status.error_message(), "begin_epoch must be strictly greater than the epoch id of the last snapshot: begin_epoch=2, snapshot_epoch_id=2");
}

TEST_F(backend_shared_impl_test, begin_backup_begin_epoch_lt_snapshot_ng) {
    gen_datastore();
    prepare_backup_test_files();
    backend_shared_impl backend(get_location(), 4);
    BeginBackupRequest request;
    BeginBackupResponse response;
    request.set_version(begin_backup_message_version);
    EXPECT_EQ(snapshot_epoch_id_, 2);
    request.set_begin_epoch(1);
    request.set_end_epoch(4);
    auto status = backend.begin_backup(*datastore_, &request, &response, backup_path_list_provider);
    EXPECT_FALSE(status.ok());
    EXPECT_FALSE(backend.get_session_store().get_session(response.session_id()).has_value());
    EXPECT_EQ(status.error_code(), ::grpc::StatusCode::INVALID_ARGUMENT);
    EXPECT_EQ(status.error_message(), "begin_epoch must be strictly greater than the epoch id of the last snapshot: begin_epoch=1, snapshot_epoch_id=2");
}

TEST_F(backend_shared_impl_test, begin_backup_end_epoch_lt_current_ok) {
    gen_datastore();
    prepare_backup_test_files();
    backend_shared_impl backend(get_location(), 4);
    BeginBackupRequest request;
    BeginBackupResponse response;
    request.set_version(begin_backup_message_version);
    request.set_begin_epoch(3);
    request.set_end_epoch(4); 
    auto status = run_with_epoch_switch([&]() { return backend.begin_backup(*datastore_, &request, &response, backup_path_list_provider); }, 7);
    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(backend.get_session_store().get_session(response.session_id()).has_value());
}

TEST_F(backend_shared_impl_test, begin_backup_end_epoch_eq_current_ok) {
    gen_datastore();
    prepare_backup_test_files();
    backend_shared_impl backend(get_location(), 4);
    BeginBackupRequest request;
    BeginBackupResponse response;
    request.set_version(begin_backup_message_version);
    request.set_begin_epoch(3);
    request.set_end_epoch(5);
    auto status = run_with_epoch_switch([&]() { return backend.begin_backup(*datastore_, &request, &response, backup_path_list_provider); }, 7);
    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(backend.get_session_store().get_session(response.session_id()).has_value());
}

TEST_F(backend_shared_impl_test, begin_backup_end_epoch_gt_current_ng) {
    gen_datastore();
    prepare_backup_test_files();
    backend_shared_impl backend(get_location(), 4);
    BeginBackupRequest request;
    BeginBackupResponse response;
    request.set_version(begin_backup_message_version);
    request.set_begin_epoch(3);
    request.set_end_epoch(6); 
    auto status = backend.begin_backup(*datastore_, &request, &response, backup_path_list_provider);
    EXPECT_FALSE(status.ok());
    EXPECT_FALSE(backend.get_session_store().get_session(response.session_id()).has_value());
    EXPECT_EQ(status.error_code(), ::grpc::StatusCode::INVALID_ARGUMENT);
    EXPECT_EQ(status.error_message(), "end_epoch must be less than or equal to the current epoch id: end_epoch=6, current_epoch_id=5");
}

TEST_F(backend_shared_impl_test, begin_backup_end_epoch_lt_boot_durable_epoch_ng) {
    gen_datastore();
    prepare_backup_test_files_without_compaction();
    backend_shared_impl backend(get_location(), 4);
    BeginBackupRequest request;
    BeginBackupResponse response;
    request.set_version(begin_backup_message_version);
    request.set_begin_epoch(1);
    request.set_end_epoch(2); 
    auto status = backend.begin_backup(*datastore_, &request, &response, backup_path_list_provider);
    EXPECT_FALSE(status.ok());
    EXPECT_FALSE(backend.get_session_store().get_session(response.session_id()).has_value());
    EXPECT_EQ(status.error_code(), ::grpc::StatusCode::INVALID_ARGUMENT);
    EXPECT_EQ(status.error_message(), "end_epoch must be strictly greater than the durable epoch id at boot time: end_epoch=2, boot_durable_epoch_id=3");
}

TEST_F(backend_shared_impl_test, begin_backup_end_epoch_eq_boot_durable_epoch_ok) {
    gen_datastore();
    prepare_backup_test_files_without_compaction();
    backend_shared_impl backend(get_location(), 4);
    BeginBackupRequest request;
    BeginBackupResponse response;
    request.set_version(begin_backup_message_version);
    request.set_begin_epoch(1);
    request.set_end_epoch(3); 
    auto status = run_with_epoch_switch([&]() { return backend.begin_backup(*datastore_, &request, &response, backup_path_list_provider); }, 7);
    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(backend.get_session_store().get_session(response.session_id()).has_value());
}

TEST_F(backend_shared_impl_test, begin_backup_end_epoch_gt_boot_durable_epoch_ok) {
    gen_datastore();
    prepare_backup_test_files_without_compaction();
    backend_shared_impl backend(get_location(), 4);
    BeginBackupRequest request;
    BeginBackupResponse response;
    request.set_version(begin_backup_message_version);
    request.set_begin_epoch(1);
    request.set_end_epoch(4); 
    auto status = run_with_epoch_switch([&]() { return backend.begin_backup(*datastore_, &request, &response, backup_path_list_provider); }, 7);
    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(backend.get_session_store().get_session(response.session_id()).has_value());
}

TEST_F(backend_shared_impl_test, begin_backup_exception_handling) {
    gen_datastore(call_ready_mode::call_ready_manual);
    backend_shared_impl backend(get_location(), 4);
    backend.set_exception_hook([]() { throw std::runtime_error("test exception"); });
    BeginBackupRequest request;
    request.set_version(begin_backup_message_version);
    request.set_begin_epoch(1);
    request.set_end_epoch(2);
    BeginBackupResponse response;
    auto status = backend.begin_backup(*datastore_, &request, &response, backup_path_list_provider);
    EXPECT_EQ(status.error_code(), ::grpc::StatusCode::INTERNAL);
    EXPECT_STREQ(status.error_message().c_str(), "test exception");
}




} // namespace limestone::testing

