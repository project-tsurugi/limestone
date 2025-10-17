/*
 * Copyright 2023-2025 Project Tsurugi.
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

#include "wal_sync/wal_sync_client.h"

#include <chrono>
#include <regex>
#include <sstream>
#include <unordered_set>
#include <set>

#include <boost/filesystem.hpp>
#include <boost/system/error_code.hpp>
#include <grpcpp/grpcpp.h>
#include <gtest/gtest.h>

#include "limestone/grpc/grpc_test_helper.h"
#include "limestone/grpc/backend_test_fixture.h"
#include "manifest.h"
#include "test_root.h"
#include "limestone/grpc/service/wal_history_service_impl.h"
#include "limestone/grpc/service/backup_service_impl.h"
#include "limestone/grpc/service/grpc_constants.h"
#include "backup.pb.h"

namespace {

class scripted_backup_service final : public limestone::grpc::proto::BackupService::Service {
public:
    void set_responses(std::vector<limestone::grpc::proto::GetObjectResponse> responses) {
        responses_ = std::move(responses);
    }

    ::grpc::Status GetObject(
        ::grpc::ServerContext* /*context*/,
        const limestone::grpc::proto::GetObjectRequest* /*request*/,
        ::grpc::ServerWriter<limestone::grpc::proto::GetObjectResponse>* writer
    ) override {
        for (auto const& response : responses_) {
            writer->Write(response);
        }
        return ::grpc::Status::OK;
    }

private:
    std::vector<limestone::grpc::proto::GetObjectResponse> responses_;
};

} // namespace

namespace limestone::testing {
using namespace limestone::internal;
using limestone::grpc::service::session_timeout_seconds;
using limestone::internal::backup_object_type_helper::from_proto;


class wal_sync_client_test : public backend_test_fixture {
protected:
    boost::filesystem::path test_dir = "/tmp/wal_sync_client_test";
	boost::filesystem::path locale_dir = "/tmp/wal_sync_client_test/local";
	boost::filesystem::path remote_dir = "/tmp/wal_sync_client_test/remote";
    limestone::grpc::testing::grpc_test_helper helper_;

    char const* get_location() const override {
        return remote_dir.string().c_str();
    }

	void SetUp() override {
        boost::filesystem::remove_all(locale_dir);
        if (!boost::filesystem::create_directories(locale_dir)) {
            std::cerr << "cannot make directory" << std::endl;
        }	
		backend_test_fixture::SetUp();
        helper_.set_backend_factory([this]() {
            return limestone::grpc::backend::grpc_service_backend::create_standalone(remote_dir);
        });
        helper_.add_service_factory([](limestone::grpc::backend::grpc_service_backend& backend) {
            return std::make_unique<limestone::grpc::service::wal_history_service_impl>(backend);
        });
        helper_.add_service_factory([](limestone::grpc::backend::grpc_service_backend& backend) {
            return std::make_unique<limestone::grpc::service::backup_service_impl>(backend);
        });
        helper_.setup();
    }

    void TearDown() override {
        helper_.tear_down();

        backend_test_fixture::TearDown();
        boost::filesystem::remove_all(test_dir);
    }
};

TEST_F(wal_sync_client_test, init_creates_manifest_when_dir_not_exist_and_allowed) {
	wal_sync_client client(locale_dir, helper_.create_channel());
	boost::filesystem::remove_all(locale_dir);
	std::string error;
	EXPECT_TRUE(client.init(error, true));
	boost::filesystem::path manifest_path = locale_dir / "limestone-manifest.json";
	EXPECT_TRUE(boost::filesystem::exists(manifest_path));
}

TEST_F(wal_sync_client_test, init_fails_when_dir_not_exist_and_not_allowed) {
	wal_sync_client client(locale_dir, helper_.create_channel());
    boost::filesystem::remove_all(locale_dir);
	std::string error;
	EXPECT_FALSE(client.init(error, false));
	EXPECT_NE(error.find("log_dir does not exist"), std::string::npos);
}

TEST_F(wal_sync_client_test, init_acquires_and_releases_manifest_lock) {
	// Prepare: create directory and manifest
	limestone::internal::manifest::create_initial(locale_dir);

	// 1. Acquire lock by wal_sync_client::init
	{
		wal_sync_client client(locale_dir, helper_.create_channel());
		std::string error;
		ASSERT_TRUE(client.init(error, false));

		// 2. Try to acquire the same lock (should fail: flock is exclusive in the same process)
		int fd = limestone::internal::manifest::acquire_lock(locale_dir);
		EXPECT_EQ(fd, -1) << "lock should be held by wal_sync_client";
	}
	// 3. After client destruction, lock should be released and can be acquired again
	int fd2 = limestone::internal::manifest::acquire_lock(locale_dir);
	EXPECT_GE(fd2, 0) << "lock should be released after wal_sync_client destruction";
	if (fd2 >= 0) {
		::close(fd2);
	}
}

TEST_F(wal_sync_client_test, init_fails_when_dir_creation_fails) {
    boost::filesystem::remove_all(test_dir);
	wal_sync_client client(locale_dir, helper_.create_channel());
	std::string error;
	EXPECT_FALSE(client.init(error, true));
	EXPECT_NE(error.find("failed to create log_dir"), std::string::npos);
}

TEST_F(wal_sync_client_test, init_fails_when_log_dir_is_a_file) {
    boost::filesystem::remove_all(locale_dir);
	// Create a file at the log_dir path
	std::ofstream ofs(locale_dir.string());
	ofs << "dummy";
	ofs.close();
	wal_sync_client client(locale_dir, helper_.create_channel());
	std::string error;
	EXPECT_FALSE(client.init(error, true));
	EXPECT_NE(error.find("log_dir is not a directory"), std::string::npos);
}

TEST_F(wal_sync_client_test, init_fails_when_dir_is_empty_and_not_allowed) {
	wal_sync_client client(locale_dir, helper_.create_channel());
	std::string error;
	EXPECT_FALSE(client.init(error, false));
	EXPECT_NE(error.find("log_dir is empty"), std::string::npos);
}

TEST_F(wal_sync_client_test, init_fails_when_dir_not_exist_and_not_allowed_2) {
	// Redundant with init_fails_when_dir_not_exist_and_not_allowed, but explicit for allow_initialize=false
    boost::filesystem::remove_all(locale_dir);
	wal_sync_client client(locale_dir, helper_.create_channel());
	std::string error;
	EXPECT_FALSE(client.init(error, false));
	EXPECT_NE(error.find("log_dir does not exist"), std::string::npos);
}

TEST_F(wal_sync_client_test, init_fails_when_manifest_not_found) {
	boost::filesystem::create_directory(locale_dir);
	// Place a dummy file so the directory is not empty
	std::ofstream dummy((locale_dir / "dummy.txt").string());
	dummy << "dummy";
	dummy.close();
	// Do not create manifest file
	wal_sync_client client(locale_dir, helper_.create_channel());
	std::string error;
	EXPECT_FALSE(client.init(error, false));
	EXPECT_NE(error.find("manifest file not found"), std::string::npos);
}

TEST_F(wal_sync_client_test, init_fails_when_manifest_format_version_is_invalid) {
	boost::filesystem::path manifest_path = locale_dir / "limestone-manifest.json";
	std::ofstream ofs(manifest_path.string());
	ofs << "{\"format_version\":\"bad\",\"instance_uuid\":\"ddf87e86-08b8-4577-a21e-250e3a0f652e\",\"persistent_format_version\":7}";
	ofs.close();
	wal_sync_client client(locale_dir, helper_.create_channel());
	std::string error;
	EXPECT_FALSE(client.init(error, false));
	EXPECT_NE(error.find("unsupported manifest format_version: 'bad'"), std::string::npos);
}

TEST_F(wal_sync_client_test, init_fails_when_manifest_persistent_format_version_is_invalid) {
	boost::filesystem::create_directory(locale_dir);
	boost::filesystem::path manifest_path = locale_dir / "limestone-manifest.json";
	std::ofstream ofs(manifest_path.string());
	ofs << "{\"format_version\":\"1.1\",\"instance_uuid\":\"ddf87e86-08b8-4577-a21e-250e3a0f652e\",\"persistent_format_version\":1}";
	ofs.close();
	wal_sync_client client(locale_dir, helper_.create_channel());
	std::string error;
	EXPECT_FALSE(client.init(error, false));
	EXPECT_NE(error.find("unsupported manifest persistent_format_version"), std::string::npos);
}

TEST_F(wal_sync_client_test, init_fails_when_manifest_is_broken) {
	boost::filesystem::create_directory(locale_dir);
	boost::filesystem::path manifest_path = locale_dir / "limestone-manifest.json";
	std::ofstream ofs(manifest_path.string());
	ofs << "{ broken";
	ofs.close();
	wal_sync_client client(locale_dir, helper_.create_channel());
	std::string error;
	EXPECT_FALSE(client.init(error, false));
	EXPECT_NE(error.find("manifest file not found or invalid"), std::string::npos);
}

TEST_F(wal_sync_client_test, init_fails_when_lock_cannot_be_acquired) {
	limestone::internal::manifest::create_initial(locale_dir);
	// Acquire lock manually
	int fd = limestone::internal::manifest::acquire_lock(locale_dir);
	ASSERT_GE(fd, 0);
	wal_sync_client client(locale_dir, helper_.create_channel());
	std::string error;
	EXPECT_FALSE(client.init(error, false));
	EXPECT_NE(error.find("failed to acquire manifest lock"), std::string::npos);
	::close(fd);
}

TEST_F(wal_sync_client_test, get_local_epoch_returns_zero_when_no_wal_files) {
    limestone::internal::manifest::create_initial(locale_dir);

    wal_sync_client client(locale_dir, helper_.create_channel());
    std::string error;
    ASSERT_TRUE(client.init(error, false));

    // No WAL files present, should return 0
    EXPECT_EQ(client.get_local_epoch(), 0);
}

TEST_F(wal_sync_client_test, get_local_epoch_returns_last_durable_epoch) {
    gen_datastore();
    prepare_backup_test_files();
    datastore_=nullptr;

    wal_sync_client client(remote_dir, helper_.create_channel());
    std::string error;
    ASSERT_TRUE(client.init(error, false));

    // Should return the highest epoch (5)
    EXPECT_EQ(client.get_local_epoch(), 5);
}

TEST_F(wal_sync_client_test, get_remote_epoch_success) {
    gen_datastore();
    prepare_backup_test_files();
    datastore_=nullptr;
    helper_.start_server();

	wal_sync_client client(locale_dir, helper_.create_channel());

    // Should return the highest epoch (5)
    auto epoch = client.get_remote_epoch();
    ASSERT_TRUE(epoch.has_value());
    EXPECT_EQ(epoch.value(), 5);
}

TEST_F(wal_sync_client_test, get_remote_epoch_failure) {
    wal_sync_client client(locale_dir, helper_.create_channel());

    auto epoch = client.get_remote_epoch();
    ASSERT_FALSE(epoch.has_value());
}

TEST_F(wal_sync_client_test, get_remote_wal_compatibility_success) {
    helper_.start_server();

    // prepare wal history on disk
    limestone::internal::wal_history wh(remote_dir);
    wh.append(42);
    wh.append(84);
    auto expected = wh.list();
    // set last epoch file
    FILE* fp = std::fopen((remote_dir / "epoch").c_str(), "wb");
    ASSERT_TRUE(fp != nullptr);
    limestone::api::log_entry::durable_epoch(fp, 100);
    std::fclose(fp);

    wal_sync_client client(locale_dir, helper_.create_channel());
    auto branch_epochs = client.get_remote_wal_compatibility();
    ASSERT_TRUE(branch_epochs.has_value());
    ASSERT_EQ(branch_epochs->size(), expected.size());
    for (std::size_t i = 0; i < branch_epochs->size(); ++i) {
        const auto& branch_epoch = branch_epochs->at(i);
        const auto& exp = expected[i];
        EXPECT_EQ(branch_epoch.epoch, exp.epoch);
        EXPECT_EQ(branch_epoch.identity, exp.identity);
        EXPECT_EQ(branch_epoch.timestamp, static_cast<unix_timestamp_seconds>(exp.timestamp));
    }
}

TEST_F(wal_sync_client_test, get_remote_wal_compatibility_failure) {
    wal_sync_client client(locale_dir, helper_.create_channel());

    auto branch_epochs = client.get_remote_wal_compatibility();
    ASSERT_FALSE(branch_epochs.has_value());
}

TEST_F(wal_sync_client_test, keepalive_session_success) {
    gen_datastore();
    prepare_backup_test_files();
    datastore_->shutdown();
    datastore_ = nullptr;

    helper_.start_server();

    wal_sync_client client(locale_dir, helper_.create_channel());
    std::string error;
    ASSERT_TRUE(client.init(error, true));

    auto begin_result = client.begin_backup(0, 0);
    ASSERT_TRUE(begin_result.has_value());
    EXPECT_TRUE(client.keepalive_session(begin_result->session_token));
}

TEST_F(wal_sync_client_test, keepalive_session_failure) {
    helper_.start_server();

    wal_sync_client client(locale_dir, helper_.create_channel());
    std::string error;
    ASSERT_TRUE(client.init(error, true));

    EXPECT_FALSE(client.keepalive_session("invalid-session-token"));
}

TEST_F(wal_sync_client_test, end_backup_success) {
    gen_datastore();
    prepare_backup_test_files();
    datastore_->shutdown();
    datastore_ = nullptr;

    helper_.start_server();

    wal_sync_client client(locale_dir, helper_.create_channel());
    std::string error;
    ASSERT_TRUE(client.init(error, true));

    auto begin_result = client.begin_backup(0, 0);
    ASSERT_TRUE(begin_result.has_value());
    EXPECT_TRUE(client.end_backup(begin_result->session_token));
}

TEST_F(wal_sync_client_test, end_backup_failure) {
    helper_.start_server();

    wal_sync_client client(locale_dir, helper_.create_channel());
    std::string error;
    ASSERT_TRUE(client.init(error, true));

    helper_.tear_down();

    EXPECT_FALSE(client.end_backup("invalid-session-token"));
}

TEST_F(wal_sync_client_test, execute_remote_backup_success) {
    gen_datastore();
    prepare_backup_test_files();
    datastore_->shutdown();
    datastore_ = nullptr;

    helper_.start_server();

    wal_sync_client client(locale_dir, helper_.create_channel());
    std::string error;
    ASSERT_TRUE(client.init(error, true));

    auto filtered_conditions = get_filtered_backup_conditions([](const backup_condition& condition) {
        return condition.is_offline_backup_target;
    });

    boost::filesystem::path output_dir = locale_dir / "remote_backup_success";
    boost::filesystem::remove_all(output_dir);

    auto backup_result = client.execute_remote_backup(0, 0, output_dir);
    ASSERT_TRUE(backup_result.success);
    EXPECT_TRUE(backup_result.error_message.empty());
    EXPECT_TRUE(backup_result.incomplete_object_ids.empty());
    EXPECT_TRUE(boost::filesystem::exists(output_dir));

    std::set<std::string> actual_paths;
    for (boost::filesystem::recursive_directory_iterator it(output_dir), end; it != end; ++it) {
        if (boost::filesystem::is_regular_file(it->path())) {
            auto rel = boost::filesystem::relative(it->path(), output_dir);
            actual_paths.insert(rel.generic_string());
        }
    }

    EXPECT_FALSE(actual_paths.empty());
    for (auto const& path : actual_paths) {
        bool matched = false;
        for (auto const& condition : filtered_conditions) {
            if (condition.object_path.empty()) {
                continue;
            }
            if (is_path_matching(path, condition.object_path)) {
                matched = true;
                break;
            }
        }
        EXPECT_TRUE(matched) << "unexpected file copied: " << path;
    }
}

TEST_F(wal_sync_client_test, execute_remote_backup_begin_failure) {
    wal_sync_client client(locale_dir, helper_.create_channel());
    std::string error;
    ASSERT_TRUE(client.init(error, true));

    boost::filesystem::path output_dir = locale_dir / "remote_backup_begin_failure";
    auto backup_result = client.execute_remote_backup(0, 0, output_dir);

    EXPECT_FALSE(backup_result.success);
    EXPECT_FALSE(backup_result.error_message.empty());
    EXPECT_TRUE(backup_result.incomplete_object_ids.empty());
    EXPECT_FALSE(boost::filesystem::exists(output_dir));
}

class wal_sync_client_processor_error_test : public ::testing::Test {
protected:
    void SetUp() override {
        boost::filesystem::remove_all(base_dir_);
        ASSERT_TRUE(boost::filesystem::create_directories(base_dir_));
        ::grpc::ServerBuilder builder;
        builder.AddListeningPort("127.0.0.1:0", ::grpc::InsecureServerCredentials(), &port_);
        builder.RegisterService(&service_);
        server_ = builder.BuildAndStart();
        ASSERT_TRUE(server_ != nullptr);
        actual_address_ = "127.0.0.1:" + std::to_string(port_);
        channel_ = ::grpc::CreateChannel(actual_address_, ::grpc::InsecureChannelCredentials());
        ASSERT_TRUE(channel_->WaitForConnected(std::chrono::system_clock::now() + std::chrono::seconds(1)));
        client_ = std::make_unique<wal_sync_client>(base_dir_, channel_);
        std::string error;
        ASSERT_TRUE(client_->init(error, true));
    }

    void TearDown() override {
        client_.reset();
        if (server_ != nullptr) {
            server_->Shutdown();
            server_->Wait();
        }
        boost::filesystem::remove_all(base_dir_);
    }

    static limestone::grpc::proto::GetObjectResponse make_single_chunk(
        std::string const& object_id,
        std::string const& path,
        std::string const& data,
        bool is_last = true
    ) {
        limestone::grpc::proto::GetObjectResponse response;
        auto* object = response.mutable_object();
        object->set_object_id(object_id);
        object->set_path(path);
        response.set_is_first(true);
        response.set_is_last(is_last);
        response.set_offset(0);
        response.set_total_size(static_cast<std::uint64_t>(data.size()));
        response.set_chunk(data);
        return response;
    }

    struct failing_open_file_operations : public real_file_operations {
        std::unique_ptr<std::ofstream> open_ofstream(const std::string& /*path*/) override {
            return std::make_unique<std::ofstream>();
        }
    };

    boost::filesystem::path base_dir_ = boost::filesystem::path{"/tmp/wal_sync_client_processor_test"};
    std::shared_ptr<::grpc::Channel> channel_;
    std::unique_ptr<::grpc::Server> server_;
    int port_ = 0;
    std::string actual_address_;
    scripted_backup_service service_;
    std::unique_ptr<wal_sync_client> client_;
    failing_open_file_operations failing_ops_;
};

TEST_F(wal_sync_client_test, begin_backup_success) {
    gen_datastore();
    prepare_backup_test_files();
    datastore_->shutdown();
    datastore_ = nullptr;
    assert_backup_file_conditions([](const backup_condition& c) { return c.pre_rotation_path; });

    helper_.start_server();

    wal_sync_client client(locale_dir, helper_.create_channel());
    std::string error;
    ASSERT_TRUE(client.init(error, true));

    auto before = std::chrono::system_clock::now();
    auto result_opt = client.begin_backup(0, 0);
    auto after = std::chrono::system_clock::now();

    ASSERT_TRUE(result_opt.has_value());
    const auto& result = result_opt.value();
    ASSERT_FALSE(result.objects.empty());

    std::regex uuid_regex(R"([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})");
    EXPECT_TRUE(std::regex_match(result.session_token, uuid_regex)) << "session token is not UUID: " << result.session_token;

    auto expire_seconds = std::chrono::duration_cast<std::chrono::seconds>(result.expire_at.time_since_epoch()).count();
    auto before_seconds = std::chrono::duration_cast<std::chrono::seconds>(before.time_since_epoch()).count();
    auto after_seconds = std::chrono::duration_cast<std::chrono::seconds>(after.time_since_epoch()).count();
    EXPECT_GE(expire_seconds, before_seconds + session_timeout_seconds);
    EXPECT_LE(expire_seconds, after_seconds + session_timeout_seconds);

    auto filtered_conditions = get_filtered_backup_conditions([](const backup_condition& c) {
        return c.is_offline_backup_target;
    });
    std::unordered_set<std::string> remaining_ids;
    for (auto const& cond : filtered_conditions) {
        remaining_ids.insert(cond.object_id);
    }

    for (auto const& object : result.objects) {
        auto matched = find_matching_backup_conditions(object.id, filtered_conditions);
        ASSERT_FALSE(matched.empty()) << "no expected condition for object id: " << object.id;
        ASSERT_LT(matched.size(), static_cast<std::size_t>(2)) << "multiple conditions matched object id: " << object.id;
        auto const& cond = matched.front();
        EXPECT_EQ(object.type, from_proto(cond.object_type));
        EXPECT_TRUE(is_path_matching(object.path, cond.object_path))
            << "object path mismatch for id " << object.id << ": " << object.path << " expected pattern " << cond.object_path;
        remaining_ids.erase(cond.object_id);
    }

    EXPECT_TRUE(remaining_ids.empty()) << "missing expected objects: " << [&]() {
        std::ostringstream oss;
        for (auto const& id : remaining_ids) {
            oss << id << ", ";
        }
        return oss.str();
    }();
}

TEST_F(wal_sync_client_test, copy_backup_objects_success) {
    // Build remote datastore so begin_backup() enumerates actual backup targets.
    gen_datastore();
    prepare_backup_test_files();
    datastore_->shutdown();
    datastore_ = nullptr;

    helper_.start_server();

    wal_sync_client client(locale_dir, helper_.create_channel());
    std::string error;
    ASSERT_TRUE(client.init(error, true));

    // Fetch session token and backup object list from the remote backup service.
    auto begin_result_opt = client.begin_backup(0, 0);
    ASSERT_TRUE(begin_result_opt.has_value());
    auto const& begin_result = begin_result_opt.value();
    ASSERT_FALSE(begin_result.objects.empty());

    // Derive the list of expected objects from the fixture configuration.
    auto filtered_conditions = get_filtered_backup_conditions([](const backup_condition& condition) {
        return condition.is_offline_backup_target;
    });

    // Keep track of expected IDs that must appear in the begin_backup() result.
    std::unordered_set<std::string> remaining_ids;
    for (auto const& condition : filtered_conditions) {
        if (!condition.object_id.empty()) {
            remaining_ids.insert(condition.object_id);
        }
    }

    // Record the expected relative paths so we can later verify the files produced by copy_backup_objects().
    std::unordered_set<std::string> expected_paths;
    expected_paths.reserve(begin_result.objects.size());

    // Ensure every returned object corresponds to exactly one expected condition.
    for (auto const& object : begin_result.objects) {
        auto matched = find_matching_backup_conditions(object.id, filtered_conditions);
        ASSERT_FALSE(matched.empty()) << "no expected condition for object id: " << object.id;
        ASSERT_LT(matched.size(), static_cast<std::size_t>(2)) << "multiple conditions matched object id: " << object.id;
        auto const& condition = matched.front();
        EXPECT_TRUE(is_path_matching(object.path, condition.object_path))
            << "object path mismatch for id " << object.id << ": " << object.path
            << " expected pattern " << condition.object_path;
        if (!condition.object_id.empty()) {
            remaining_ids.erase(condition.object_id);
        }
        expected_paths.insert(object.path);
    }

    // All expected IDs must have been matched by the begin_backup() response.
    EXPECT_TRUE(remaining_ids.empty()) << "missing expected objects: " << [&]() {
        std::ostringstream oss;
        for (auto const& id : remaining_ids) {
            oss << id << ", ";
        }
        return oss.str();
    }();

    boost::filesystem::path output_dir = locale_dir / "copied_backup";
    boost::filesystem::remove_all(output_dir);

    // Execute the copy and confirm the destination directory exists.
    auto copy_result = client.copy_backup_objects(begin_result.session_token, begin_result.objects, output_dir);
    ASSERT_TRUE(copy_result.success);
    EXPECT_TRUE(copy_result.error_message.empty());
    EXPECT_TRUE(copy_result.incomplete_object_ids.empty());
    EXPECT_TRUE(boost::filesystem::exists(output_dir));

    std::set<std::string> actual_paths;
    // Collect relative paths of produced files to compare with the expected manifest.
    for (boost::filesystem::recursive_directory_iterator it(output_dir), end; it != end; ++it) {
        if (boost::filesystem::is_regular_file(it->path())) {
            auto rel = boost::filesystem::relative(it->path(), output_dir);
            actual_paths.insert(rel.generic_string());
        }
    }

    // The set of files produced locally must match the expected list exactly.
    EXPECT_EQ(actual_paths.size(), expected_paths.size());
    for (auto const& path : actual_paths) {
        EXPECT_TRUE(expected_paths.find(path) != expected_paths.end())
            << "unexpected copied file: " << path;
    }

    // Optionally compare file sizes with the remote source as an additional sanity check.
    for (auto const& object : begin_result.objects) {
        boost::filesystem::path local_path = output_dir / object.path;
        EXPECT_TRUE(boost::filesystem::exists(local_path))
            << "missing copied file: " << local_path.string();

        boost::filesystem::path remote_path = remote_dir / object.path;
        if (boost::filesystem::exists(remote_path)
            && boost::filesystem::is_regular_file(remote_path)
            && boost::filesystem::is_regular_file(local_path)) {
            EXPECT_EQ(
                boost::filesystem::file_size(local_path),
                boost::filesystem::file_size(remote_path)
            ) << "size mismatch for copied file: " << object.path;
        }
    }
}

TEST_F(wal_sync_client_test, copy_backup_objects_returns_true_when_no_objects) {
    // With no objects to copy, the method should short-circuit without touching the filesystem.
    wal_sync_client client(locale_dir, helper_.create_channel());
    std::string error;
    ASSERT_TRUE(client.init(error, true));

    boost::filesystem::path output_dir = locale_dir / "no_objects";
    boost::filesystem::remove_all(output_dir);

    auto copy_result = client.copy_backup_objects("unused_session", {}, output_dir);
    EXPECT_TRUE(copy_result.success);
    EXPECT_TRUE(copy_result.error_message.empty());
    EXPECT_TRUE(copy_result.incomplete_object_ids.empty());
    EXPECT_FALSE(boost::filesystem::exists(output_dir));
}

TEST_F(wal_sync_client_test, copy_backup_objects_fails_when_directory_creation_fails) {
    class failing_file_operations : public real_file_operations {
    public:
        void create_directories(const boost::filesystem::path& path, boost::system::error_code& ec) override {
            (void)path;
            ec = boost::system::errc::make_error_code(boost::system::errc::permission_denied);
        }
    };
    failing_file_operations mock_ops;

    wal_sync_client client(locale_dir, helper_.create_channel());
    std::string error;
    ASSERT_TRUE(client.init(error, true));

    client.set_file_operations(mock_ops);

    std::vector<backup_object> objects{
        {"meta", backup_object_type::metadata, "meta/info"}
    };
    boost::filesystem::path output_dir = locale_dir / "dir_creation_failure";
    boost::filesystem::remove_all(output_dir);

    auto copy_result = client.copy_backup_objects("session", objects, output_dir);
    EXPECT_FALSE(copy_result.success);
    EXPECT_FALSE(copy_result.error_message.empty());
    EXPECT_TRUE(copy_result.incomplete_object_ids.empty());
    EXPECT_FALSE(boost::filesystem::exists(output_dir));
}

TEST_F(wal_sync_client_test, copy_backup_objects_fails_when_rpc_error) {
    // Prepare objects and session token while the server is available.
    gen_datastore();
    prepare_backup_test_files();
    datastore_->shutdown();
    datastore_ = nullptr;

    helper_.start_server();

    wal_sync_client client(locale_dir, helper_.create_channel());
    std::string error;
    ASSERT_TRUE(client.init(error, true));

    auto begin_result_opt = client.begin_backup(0, 0);
    ASSERT_TRUE(begin_result_opt.has_value());
    auto begin_result = begin_result_opt.value();
    ASSERT_FALSE(begin_result.objects.empty());

    // Simulate RPC failure by stopping the server before issuing copy_backup_objects().
    helper_.tear_down();

    boost::filesystem::path output_dir = locale_dir / "rpc_failure";
    boost::filesystem::remove_all(output_dir);

    auto copy_result = client.copy_backup_objects(begin_result.session_token, begin_result.objects, output_dir);
    EXPECT_FALSE(copy_result.success);
    EXPECT_FALSE(copy_result.error_message.empty());
    // Could not finish copying, but processor cleanup already removed partial files.

    // Directories may have been created, but no files should remain on disk.
    if (boost::filesystem::exists(output_dir)) {
        bool const empty = boost::filesystem::directory_iterator(output_dir) == boost::filesystem::directory_iterator();
        EXPECT_TRUE(empty) << "output directory should be empty after failure";
    }
}

TEST_F(wal_sync_client_test, begin_backup_failure) {
    wal_sync_client client(locale_dir, helper_.create_channel());
    std::string error;
    ASSERT_TRUE(client.init(error, true));

    auto result = client.begin_backup(0, 0);
    ASSERT_FALSE(result.has_value());
}

TEST_F(wal_sync_client_test, get_local_wal_compatibility) {
    // prepare wal history on disk
    limestone::internal::wal_history wh(locale_dir);
    wh.append(42);
    wh.append(84);
    auto expected = wh.list();
    // set last epoch file
    FILE* fp = std::fopen((locale_dir / "epoch").c_str(), "wb");
    ASSERT_TRUE(fp != nullptr);
    limestone::api::log_entry::durable_epoch(fp, 100);
    std::fclose(fp);

    wal_sync_client client(locale_dir, helper_.create_channel());
    auto branch_epochs = client.get_local_wal_compatibility();

    ASSERT_EQ(branch_epochs.size(), expected.size());
    for (int i = 0; i < branch_epochs.size(); ++i) {
        const auto& branch_epoch = branch_epochs[i];
        const auto& exp = expected[i];
        EXPECT_EQ(branch_epoch.epoch, exp.epoch);
        EXPECT_EQ(branch_epoch.identity, exp.identity);
        EXPECT_EQ(branch_epoch.timestamp, static_cast<unix_timestamp_seconds>(exp.timestamp));
    }
}

TEST_F(wal_sync_client_test, check_wal_compatibility) {
    // Arrange
    std::vector<branch_epoch> local = {
        {1, 100, 1633024800},
        {2, 101, 1633028400}
    };

    std::vector<branch_epoch> remote = {
        {1, 100, 1633024800},
        {2, 101, 1633028400},
        {3, 102, 1633032000}
    };

    wal_sync_client client(locale_dir, helper_.create_channel());

    // Act & Assert
    EXPECT_TRUE(client.check_wal_compatibility(local, remote));

    // Modify local to make it incompatible
    local[1].identity = 999;
    EXPECT_FALSE(client.check_wal_compatibility(local, remote));

    // Modify local size to make it larger than remote
    local.push_back({4, 103, 1633035600});
    EXPECT_FALSE(client.check_wal_compatibility(local, remote));
}

TEST_F(wal_sync_client_test, check_wal_compatibility_empty_vectors) {
    // Arrange
    std::vector<branch_epoch> local;
    std::vector<branch_epoch> remote;

    wal_sync_client client(locale_dir, helper_.create_channel());

    // Act & Assert
    EXPECT_FALSE(client.check_wal_compatibility(local, remote));

    remote.push_back({1, 100, 1633024800});
    EXPECT_FALSE(client.check_wal_compatibility(local, remote));

    local.push_back({1, 100, 1633024800});
    remote.clear();
    EXPECT_FALSE(client.check_wal_compatibility(local, remote));
}

TEST_F(wal_sync_client_test, check_wal_compatibility_partial_match) {
    // Arrange
    std::vector<branch_epoch> local = {
        {1, 100, 1633024800},
        {2, 101, 1633028400}
    };

    std::vector<branch_epoch> remote = {
        {1, 100, 1633024800},
        {2, 999, 1633028400}, // Mismatch in identity
        {3, 102, 1633032000}
    };

    wal_sync_client client(locale_dir, helper_.create_channel());

    // Act & Assert
    EXPECT_FALSE(client.check_wal_compatibility(local, remote));
}

TEST_F(wal_sync_client_test, check_wal_compatibility_remote_contains_local_with_differences) {
    // Arrange
    std::vector<branch_epoch> local = {
        {1, 100, 1633024800},
        {2, 101, 1633028400}
    };

    std::vector<branch_epoch> remote = {
        {1, 100, 1633024800},
        {2, 101, 1633028400},
        {3, 102, 1633032000},
        {4, 103, 1633035600} // Extra entry in remote
    };

    wal_sync_client client(locale_dir, helper_.create_channel());

    // Act & Assert
    EXPECT_TRUE(client.check_wal_compatibility(local, remote));

    // Modify remote to make it incompatible
    remote[1].identity = 999;
    EXPECT_FALSE(client.check_wal_compatibility(local, remote));
}

TEST_F(wal_sync_client_test, check_wal_compatibility_identical_vectors) {
    // Arrange
    std::vector<branch_epoch> local = {
        {1, 100, 1633024800},
        {2, 101, 1633028400},
        {3, 102, 1633032000}
    };

    std::vector<branch_epoch> remote = {
        {1, 100, 1633024800},
        {2, 101, 1633028400},
        {3, 102, 1633032000}
    };

    wal_sync_client client(locale_dir, helper_.create_channel());

    // Act & Assert
    EXPECT_TRUE(client.check_wal_compatibility(local, remote));
}

TEST_F(wal_sync_client_processor_error_test, copy_backup_objects_reports_processor_failure) {
    service_.set_responses({make_single_chunk("meta", "meta/info", "data")});

    client_->set_file_operations(failing_ops_);

    std::vector<backup_object> objects{
        {"meta", backup_object_type::metadata, "meta/info"}
    };
    boost::filesystem::path output_dir = base_dir_ / "processor_failure";
    boost::filesystem::remove_all(output_dir);

    auto copy_result = client_->copy_backup_objects("session", objects, output_dir);

    EXPECT_FALSE(copy_result.success);
    EXPECT_TRUE(copy_result.error_message.find("failed to open output file") != std::string::npos);
    EXPECT_TRUE(copy_result.incomplete_object_ids.empty());
    EXPECT_FALSE(boost::filesystem::exists(output_dir / "meta/info"));
}

TEST_F(wal_sync_client_processor_error_test, copy_backup_objects_reports_incomplete_objects) {
    service_.set_responses({make_single_chunk("meta", "meta/info", "data")});

    std::vector<backup_object> objects{
        {"meta", backup_object_type::metadata, "meta/info"},
        {"orphan", backup_object_type::metadata, "orphan/info"}
    };
    boost::filesystem::path output_dir = base_dir_ / "incomplete_copy";
    boost::filesystem::remove_all(output_dir);

    auto copy_result = client_->copy_backup_objects("session", objects, output_dir);

    EXPECT_FALSE(copy_result.success);
    EXPECT_EQ(copy_result.error_message, "copy incomplete for one or more objects");
    ASSERT_EQ(copy_result.incomplete_object_ids.size(), 1U);
    EXPECT_EQ(copy_result.incomplete_object_ids.front(), "orphan");
    EXPECT_TRUE(boost::filesystem::exists(output_dir / "meta/info"));
    EXPECT_FALSE(boost::filesystem::exists(output_dir / "orphan/info"));
}


} // namespace limestone::testing
