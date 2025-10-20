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

#include <array>
#include <chrono>
#include <fstream>
#include <regex>
#include <sstream>
#include <unordered_set>
#include <set>
#include <stdexcept>
#include <future>

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
#include "limestone/limestone_exception_helper.h"
#include "log_entry.h"

namespace {

void expect_epoch_file(const boost::filesystem::path& dir, std::uint64_t expected_epoch) {
    boost::filesystem::path epoch_path = dir / "epoch";
    ASSERT_TRUE(boost::filesystem::exists(epoch_path))
        << "epoch file not found: " << epoch_path.string();

    std::ifstream ifs(epoch_path.string(), std::ios::binary);
    ASSERT_TRUE(ifs.is_open()) << "failed to open epoch file: " << epoch_path.string();

    unsigned char marker_type = 0;
    ifs.read(reinterpret_cast<char*>(&marker_type), 1);
    ASSERT_EQ(ifs.gcount(), 1) << "failed to read epoch marker type";
    EXPECT_EQ(marker_type, static_cast<unsigned char>(limestone::api::log_entry::entry_type::marker_durable))
        << "unexpected epoch marker type";

    std::array<unsigned char, sizeof(std::uint64_t)> buffer{};
    ifs.read(reinterpret_cast<char*>(buffer.data()), static_cast<std::streamsize>(buffer.size()));
    ASSERT_EQ(ifs.gcount(), static_cast<std::streamsize>(buffer.size()))
        << "failed to read epoch marker payload";

    std::uint64_t epoch_value = 0;
    for (std::size_t i = 0; i < buffer.size(); ++i) {
        epoch_value |= static_cast<std::uint64_t>(buffer[i]) << (8U * i);
    }
    EXPECT_EQ(epoch_value, expected_epoch) << "epoch marker value mismatch";

    ifs.get();
    EXPECT_TRUE(ifs.eof()) << "epoch file contains unexpected extra data";
}

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

class wal_sync_client_testable : public wal_sync_client {
public:
    using wal_sync_client::wal_sync_client;
    using wal_sync_client::copy_backup_objects;
    using wal_sync_client::keepalive_session;
    using wal_sync_client::end_backup;
    using wal_sync_client::create_rotation_aware_datastore;
    using wal_sync_client::prepare_for_compaction;
    using wal_sync_client::run_compaction_with_rotation;
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

TEST_F(wal_sync_client_test, get_local_epoch_returns_zero_when_no_wal_files) {
    limestone::internal::manifest::create_initial(locale_dir);

    wal_sync_client_testable client(locale_dir, helper_.create_channel());
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
    wal_sync_client_testable client(locale_dir, helper_.create_channel());

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

    wal_sync_client_testable client(locale_dir, helper_.create_channel());
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
    wal_sync_client_testable client(locale_dir, helper_.create_channel());

    auto branch_epochs = client.get_remote_wal_compatibility();
    ASSERT_FALSE(branch_epochs.has_value());
}

TEST_F(wal_sync_client_test, keepalive_session_success) {
    gen_datastore();
    prepare_backup_test_files();
    datastore_->shutdown();
    datastore_ = nullptr;

    helper_.start_server();

    wal_sync_client_testable client(locale_dir, helper_.create_channel());
    std::string error;
    ASSERT_TRUE(client.init(error, true));

    auto begin_result = client.begin_backup(0, 0);
    ASSERT_TRUE(begin_result.has_value());
    EXPECT_TRUE(client.keepalive_session(begin_result->session_token));
}

TEST_F(wal_sync_client_test, keepalive_session_failure) {
    helper_.start_server();

    wal_sync_client_testable client(locale_dir, helper_.create_channel());
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

    wal_sync_client_testable client(locale_dir, helper_.create_channel());
    std::string error;
    ASSERT_TRUE(client.init(error, true));

    auto begin_result = client.begin_backup(0, 0);
    ASSERT_TRUE(begin_result.has_value());
    EXPECT_TRUE(client.end_backup(begin_result->session_token));
}

TEST_F(wal_sync_client_test, end_backup_failure) {
    helper_.start_server();

    wal_sync_client_testable client(locale_dir, helper_.create_channel());
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

    wal_sync_client_testable client(locale_dir, helper_.create_channel());
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
    wal_sync_client_testable client(locale_dir, helper_.create_channel());
    std::string error;
    ASSERT_TRUE(client.init(error, true));

    boost::filesystem::path output_dir = locale_dir / "remote_backup_begin_failure";
    auto backup_result = client.execute_remote_backup(0, 0, output_dir);

    EXPECT_FALSE(backup_result.success);
    EXPECT_FALSE(backup_result.error_message.empty());
    EXPECT_TRUE(backup_result.incomplete_object_ids.empty());
    EXPECT_FALSE(boost::filesystem::exists(output_dir));
}

TEST_F(wal_sync_client_test, create_rotation_aware_datastore_initializes_with_log_dir) {
    wal_sync_client_testable client(locale_dir, helper_.create_channel());

    auto datastore = client.create_rotation_aware_datastore();
    ASSERT_NE(datastore, nullptr);
}

namespace {

class fake_rotation_datastore final : public rotation_aware_datastore {
public:
    fake_rotation_datastore(limestone::api::configuration const& conf)
        : rotation_aware_datastore(conf) {}

    void set_ready_behavior(bool should_throw) {
        throw_on_ready_ = should_throw;
    }

    void set_last_epoch(epoch_id_type value) {
        epoch_ = value;
    }

    void set_compaction_behavior(std::function<void()> behavior) {
        compaction_behavior_ = std::move(behavior);
    }

    void set_epoch_switch_behavior(std::function<void(epoch_id_type)> behavior) {
        epoch_switch_behavior_ = std::move(behavior);
    }

    void simulate_ready() {
        if (throw_on_ready_) {
            throw std::runtime_error("ready failed");
        }
    }

    epoch_id_type simulated_last_epoch() const {
        return epoch_;
    }

    void perform_compaction() override {
        if (compaction_behavior_) {
            compaction_behavior_();
        }
    }

    void perform_switch_epoch(epoch_id_type value) override {
        if (epoch_switch_behavior_) {
            epoch_switch_behavior_(value);
        }
    }

private:
    epoch_id_type epoch_{0};
    bool throw_on_ready_ = false;
    std::function<void()> compaction_behavior_{};
    std::function<void(epoch_id_type)> epoch_switch_behavior_{};
};

class wal_sync_client_prepare_testable : public wal_sync_client_testable {
public:
    using wal_sync_client_testable::wal_sync_client_testable;

    void set_fake_datastore(std::unique_ptr<rotation_aware_datastore> datastore) {
        fake_datastore_ = std::move(datastore);
    }

protected:
    std::unique_ptr<rotation_aware_datastore> create_rotation_aware_datastore() override {
        if (fake_datastore_) {
            return std::move(fake_datastore_);
        }
        return wal_sync_client_testable::create_rotation_aware_datastore();
    }

    void ready_datastore(rotation_aware_datastore& datastore) override {
        auto& fake = static_cast<fake_rotation_datastore&>(datastore);
        fake.simulate_ready();
    }

    epoch_id_type query_last_epoch(rotation_aware_datastore const& datastore) const override {
        auto const& fake = static_cast<fake_rotation_datastore const&>(datastore);
        return fake.simulated_last_epoch();
    }

private:
    std::unique_ptr<rotation_aware_datastore> fake_datastore_{};
};

class wal_sync_client_prepare_unknown_testable : public wal_sync_client_prepare_testable {
public:
    using wal_sync_client_prepare_testable::wal_sync_client_prepare_testable;

protected:
    void ready_datastore(rotation_aware_datastore& /*datastore*/) override {
        throw 42;
    }
};

class wal_sync_client_run_compaction_testable : public wal_sync_client_prepare_testable {
public:
    using wal_sync_client_prepare_testable::wal_sync_client_prepare_testable;

    void set_compaction_behavior(std::function<void()> behavior) {
        compaction_behavior_ = std::move(behavior);
    }

    void set_epoch_switch_behavior(std::function<void(epoch_id_type)> behavior) {
        epoch_switch_behavior_ = std::move(behavior);
    }

protected:
    std::unique_ptr<rotation_aware_datastore> create_rotation_aware_datastore() override {
        auto datastore = wal_sync_client_prepare_testable::create_rotation_aware_datastore();
        auto* fake = static_cast<fake_rotation_datastore*>(datastore.get());
        fake->set_compaction_behavior(compaction_behavior_);
        fake->set_epoch_switch_behavior(epoch_switch_behavior_);
        return datastore;
    }

private:
    std::function<void()> compaction_behavior_{};
    std::function<void(epoch_id_type)> epoch_switch_behavior_{};
};

class wal_sync_client_compact_testable : public wal_sync_client_prepare_testable {
public:
    using wal_sync_client_prepare_testable::wal_sync_client_prepare_testable;
    using wal_sync_client_prepare_testable::compact_wal;

    void set_prepare_result(epoch_id_type epoch, bool success) {
        prepare_result_ = {epoch, success};
    }

    void set_run_result(bool value) {
        run_result_ = value;
    }

    void set_create_datastore_should_fail(bool value) {
        create_datastore_should_fail_ = value;
    }

    bool prepare_called() const {
        return prepare_called_;
    }

    bool run_called() const {
        return run_called_;
    }

protected:
    std::pair<epoch_id_type, bool> prepare_for_compaction(
        rotation_aware_datastore& /*datastore*/,
        std::atomic<bool>& /*rotation_triggered*/,
        std::condition_variable& /*rotation_cv*/,
        std::mutex& /*rotation_mutex*/
    ) override {
        prepare_called_ = true;
        return prepare_result_;
    }

    bool run_compaction_with_rotation(
        rotation_aware_datastore& /*datastore*/,
        epoch_id_type /*current_epoch*/,
        std::atomic<bool>& /*rotation_triggered*/,
        std::condition_variable& /*rotation_cv*/,
        std::mutex& /*rotation_mutex*/,
        std::exception_ptr& /*compaction_error*/
    ) override {
        run_called_ = true;
        return run_result_;
    }

    std::unique_ptr<rotation_aware_datastore> create_rotation_aware_datastore() override {
        if (create_datastore_should_fail_) {
            return {};
        }
        return wal_sync_client_prepare_testable::create_rotation_aware_datastore();
    }

private:
    std::pair<epoch_id_type, bool> prepare_result_{1, true};
    bool run_result_ = true;
    bool prepare_called_ = false;
    bool run_called_ = false;
    bool create_datastore_should_fail_ = false;
};

} // namespace

TEST_F(wal_sync_client_test, prepare_for_compaction_success_sets_rotation_handler) {
    wal_sync_client_prepare_testable client(locale_dir, helper_.create_channel());

    auto fake = std::make_unique<fake_rotation_datastore>(
        limestone::api::configuration({locale_dir}, locale_dir));
    fake->set_last_epoch(42);
    fake_rotation_datastore* fake_ptr = fake.get();
    client.set_fake_datastore(std::move(fake));

    std::atomic<bool> rotation_triggered{false};
    std::condition_variable rotation_cv;
    std::mutex rotation_mutex;

    auto result = client.prepare_for_compaction(*fake_ptr, rotation_triggered, rotation_cv, rotation_mutex);
    EXPECT_TRUE(result.second);
    EXPECT_EQ(result.first, 42);

    fake_ptr->trigger_rotation_handler_for_tests();
    EXPECT_TRUE(rotation_triggered.load());
}

TEST_F(wal_sync_client_test, prepare_for_compaction_fails_on_zero_epoch) {
    wal_sync_client_prepare_testable client(locale_dir, helper_.create_channel());
    auto fake = std::make_unique<fake_rotation_datastore>(
        limestone::api::configuration({locale_dir}, locale_dir));
    fake->set_last_epoch(0);
    fake_rotation_datastore* fake_ptr = fake.get();
    client.set_fake_datastore(std::move(fake));

    std::atomic<bool> rotation_triggered{false};
    std::condition_variable rotation_cv;
    std::mutex rotation_mutex;

    auto result = client.prepare_for_compaction(*fake_ptr, rotation_triggered, rotation_cv, rotation_mutex);
    EXPECT_FALSE(result.second);
    EXPECT_EQ(result.first, 0);
}

TEST_F(wal_sync_client_test, prepare_for_compaction_fails_on_ready_exception) {
    wal_sync_client_prepare_testable client(locale_dir, helper_.create_channel());
    auto fake = std::make_unique<fake_rotation_datastore>(
        limestone::api::configuration({locale_dir}, locale_dir));
    fake->set_last_epoch(10);
    fake->set_ready_behavior(true);
    fake_rotation_datastore* fake_ptr = fake.get();
    client.set_fake_datastore(std::move(fake));

    std::atomic<bool> rotation_triggered{false};
    std::condition_variable rotation_cv;
    std::mutex rotation_mutex;

    auto result = client.prepare_for_compaction(*fake_ptr, rotation_triggered, rotation_cv, rotation_mutex);
    EXPECT_FALSE(result.second);
    EXPECT_EQ(result.first, 0);
}

TEST_F(wal_sync_client_test, run_compaction_with_rotation_success) {
    wal_sync_client_run_compaction_testable client(locale_dir, helper_.create_channel());
    auto fake = std::make_unique<fake_rotation_datastore>(
        limestone::api::configuration({locale_dir}, locale_dir));
    fake->set_last_epoch(5);
    auto* fake_ptr = fake.get();
    client.set_fake_datastore(std::move(fake));

    std::atomic<bool> rotation_triggered{false};
    std::condition_variable rotation_cv;
    std::mutex rotation_mutex;
    std::exception_ptr compaction_error{};

    auto prep = client.prepare_for_compaction(*fake_ptr, rotation_triggered, rotation_cv, rotation_mutex);
    ASSERT_TRUE(prep.second);

    bool epoch_switched = false;
    epoch_id_type switched_epoch = 0;

    fake_ptr->set_compaction_behavior([&]() {
        fake_ptr->trigger_rotation_handler_for_tests();
    });
    fake_ptr->set_epoch_switch_behavior([&](epoch_id_type value) {
        epoch_switched = true;
        switched_epoch = value;
    });

    bool const result = client.run_compaction_with_rotation(
        *fake_ptr,
        prep.first,
        rotation_triggered,
        rotation_cv,
        rotation_mutex,
        compaction_error
    );

    EXPECT_TRUE(result);
    EXPECT_TRUE(epoch_switched);
    EXPECT_EQ(switched_epoch, prep.first + 1);
    EXPECT_FALSE(static_cast<bool>(compaction_error));
}

TEST_F(wal_sync_client_test, run_compaction_with_rotation_handles_compaction_exception) {
    wal_sync_client_run_compaction_testable client(locale_dir, helper_.create_channel());
    auto fake = std::make_unique<fake_rotation_datastore>(
        limestone::api::configuration({locale_dir}, locale_dir));
    fake->set_last_epoch(5);
    auto* fake_ptr = fake.get();
    client.set_fake_datastore(std::move(fake));

    std::atomic<bool> rotation_triggered{false};
    std::condition_variable rotation_cv;
    std::mutex rotation_mutex;
    std::exception_ptr compaction_error{};

    auto prep = client.prepare_for_compaction(*fake_ptr, rotation_triggered, rotation_cv, rotation_mutex);
    ASSERT_TRUE(prep.second);

    fake_ptr->set_compaction_behavior([]() {
        throw std::runtime_error("compaction failure");
    });

    bool const result = client.run_compaction_with_rotation(
        *fake_ptr,
        prep.first,
        rotation_triggered,
        rotation_cv,
        rotation_mutex,
        compaction_error
    );

    EXPECT_FALSE(result);
    EXPECT_TRUE(static_cast<bool>(compaction_error));
}

TEST_F(wal_sync_client_test, run_compaction_with_rotation_handles_epoch_switch_exception) {
    wal_sync_client_run_compaction_testable client(locale_dir, helper_.create_channel());
    auto fake = std::make_unique<fake_rotation_datastore>(
        limestone::api::configuration({locale_dir}, locale_dir));
    fake->set_last_epoch(7);
    auto* fake_ptr = fake.get();
    client.set_fake_datastore(std::move(fake));

    std::atomic<bool> rotation_triggered{false};
    std::condition_variable rotation_cv;
    std::mutex rotation_mutex;
    std::exception_ptr compaction_error{};

    auto prep = client.prepare_for_compaction(*fake_ptr, rotation_triggered, rotation_cv, rotation_mutex);
    ASSERT_TRUE(prep.second);

    fake_ptr->set_compaction_behavior([&]() {
        fake_ptr->trigger_rotation_handler_for_tests();
    });
    fake_ptr->set_epoch_switch_behavior([](epoch_id_type) {
        throw std::runtime_error("switch failure");
    });

    bool const result = client.run_compaction_with_rotation(
        *fake_ptr,
        prep.first,
        rotation_triggered,
        rotation_cv,
        rotation_mutex,
        compaction_error
    );

    EXPECT_FALSE(result);
    EXPECT_FALSE(static_cast<bool>(compaction_error));
}

TEST_F(wal_sync_client_test, run_compaction_with_rotation_without_rotation_trigger) {
    wal_sync_client_run_compaction_testable client(locale_dir, helper_.create_channel());
    auto fake = std::make_unique<fake_rotation_datastore>(
        limestone::api::configuration({locale_dir}, locale_dir));
    fake->set_last_epoch(9);
    auto* fake_ptr = fake.get();
    client.set_fake_datastore(std::move(fake));

    std::atomic<bool> rotation_triggered{false};
    std::condition_variable rotation_cv;
    std::mutex rotation_mutex;
    std::exception_ptr compaction_error{};

    auto prep = client.prepare_for_compaction(*fake_ptr, rotation_triggered, rotation_cv, rotation_mutex);
    ASSERT_TRUE(prep.second);

    bool epoch_switched = false;
    fake_ptr->set_compaction_behavior([]() {
        // No rotation trigger to simulate non-rotation compaction path.
    });
    fake_ptr->set_epoch_switch_behavior([&](epoch_id_type) {
        epoch_switched = true;
    });

    bool const result = client.run_compaction_with_rotation(
        *fake_ptr,
        prep.first,
        rotation_triggered,
        rotation_cv,
        rotation_mutex,
        compaction_error
    );

    EXPECT_TRUE(result);
    EXPECT_FALSE(epoch_switched);
    EXPECT_FALSE(rotation_triggered.load());
    EXPECT_FALSE(static_cast<bool>(compaction_error));
}

TEST_F(wal_sync_client_test, run_compaction_with_rotation_handles_unknown_compaction_exception) {
    wal_sync_client_run_compaction_testable client(locale_dir, helper_.create_channel());
    auto fake = std::make_unique<fake_rotation_datastore>(
        limestone::api::configuration({locale_dir}, locale_dir));
    fake->set_last_epoch(4);
    auto* fake_ptr = fake.get();
    client.set_fake_datastore(std::move(fake));

    std::atomic<bool> rotation_triggered{false};
    std::condition_variable rotation_cv;
    std::mutex rotation_mutex;
    std::exception_ptr compaction_error{};

    auto prep = client.prepare_for_compaction(*fake_ptr, rotation_triggered, rotation_cv, rotation_mutex);
    ASSERT_TRUE(prep.second);

    fake_ptr->set_compaction_behavior([]() {
        throw 123;
    });

    bool const result = client.run_compaction_with_rotation(
        *fake_ptr,
        prep.first,
        rotation_triggered,
        rotation_cv,
        rotation_mutex,
        compaction_error
    );

    EXPECT_FALSE(result);
}

TEST_F(wal_sync_client_test, run_compaction_with_rotation_handles_unknown_switch_exception) {
    wal_sync_client_run_compaction_testable client(locale_dir, helper_.create_channel());
    auto fake = std::make_unique<fake_rotation_datastore>(
        limestone::api::configuration({locale_dir}, locale_dir));
    fake->set_last_epoch(8);
    auto* fake_ptr = fake.get();
    client.set_fake_datastore(std::move(fake));

    std::atomic<bool> rotation_triggered{false};
    std::condition_variable rotation_cv;
    std::mutex rotation_mutex;
    std::exception_ptr compaction_error{};

    auto prep = client.prepare_for_compaction(*fake_ptr, rotation_triggered, rotation_cv, rotation_mutex);
    ASSERT_TRUE(prep.second);

    fake_ptr->set_compaction_behavior([&]() {
        fake_ptr->trigger_rotation_handler_for_tests();
    });
    fake_ptr->set_epoch_switch_behavior([](epoch_id_type) {
        throw 456;
    });

    bool const result = client.run_compaction_with_rotation(
        *fake_ptr,
        prep.first,
        rotation_triggered,
        rotation_cv,
        rotation_mutex,
        compaction_error
    );

    EXPECT_FALSE(result);
    EXPECT_TRUE(rotation_triggered.load());
    EXPECT_FALSE(static_cast<bool>(compaction_error));
}

TEST_F(wal_sync_client_test, run_compaction_with_rotation_propagates_compaction_error_after_thread_completion) {
    wal_sync_client_run_compaction_testable client(locale_dir, helper_.create_channel());
    auto fake = std::make_unique<fake_rotation_datastore>(
        limestone::api::configuration({locale_dir}, locale_dir));
    fake->set_last_epoch(11);
    auto* fake_ptr = fake.get();
    client.set_fake_datastore(std::move(fake));

    std::atomic<bool> rotation_triggered{false};
    std::condition_variable rotation_cv;
    std::mutex rotation_mutex;
    std::exception_ptr compaction_error{};

    auto prep = client.prepare_for_compaction(*fake_ptr, rotation_triggered, rotation_cv, rotation_mutex);
    ASSERT_TRUE(prep.second);

    fake_ptr->set_compaction_behavior([]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        throw std::runtime_error("delayed compaction failure");
    });

    bool const result = client.run_compaction_with_rotation(
        *fake_ptr,
        prep.first,
        rotation_triggered,
        rotation_cv,
        rotation_mutex,
        compaction_error
    );

    EXPECT_FALSE(result);
    ASSERT_TRUE(static_cast<bool>(compaction_error));
    try {
        std::rethrow_exception(compaction_error);
    } catch (std::runtime_error const& ex) {
        EXPECT_STREQ(ex.what(), "delayed compaction failure");
    } catch (...) {
        FAIL() << "unexpected exception type";
    }
}

TEST_F(wal_sync_client_test, run_compaction_with_rotation_handles_rotation_then_compaction_failure) {
    wal_sync_client_run_compaction_testable client(locale_dir, helper_.create_channel());
    auto fake = std::make_unique<fake_rotation_datastore>(
        limestone::api::configuration({locale_dir}, locale_dir));
    fake->set_last_epoch(12);
    auto* fake_ptr = fake.get();
    client.set_fake_datastore(std::move(fake));

    std::atomic<bool> rotation_triggered{false};
    std::condition_variable rotation_cv;
    std::mutex rotation_mutex;
    std::exception_ptr compaction_error{};

    auto prep = client.prepare_for_compaction(*fake_ptr, rotation_triggered, rotation_cv, rotation_mutex);
    ASSERT_TRUE(prep.second);

    bool epoch_switched = false;
    fake_ptr->set_compaction_behavior([&]() {
        fake_ptr->trigger_rotation_handler_for_tests();
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        throw std::runtime_error("post rotation failure");
    });
    fake_ptr->set_epoch_switch_behavior([&](epoch_id_type) {
        epoch_switched = true;
    });

    bool const result = client.run_compaction_with_rotation(
        *fake_ptr,
        prep.first,
        rotation_triggered,
        rotation_cv,
        rotation_mutex,
        compaction_error
    );

    EXPECT_FALSE(result);
    EXPECT_TRUE(epoch_switched);
    ASSERT_TRUE(static_cast<bool>(compaction_error));
    try {
        std::rethrow_exception(compaction_error);
    } catch (std::runtime_error const& ex) {
        EXPECT_STREQ(ex.what(), "post rotation failure");
    } catch (...) {
        FAIL() << "unexpected exception type";
    }
}

TEST_F(wal_sync_client_test, compact_wal_success) {
    wal_sync_client_compact_testable client(locale_dir, helper_.create_channel());
    client.set_prepare_result(5, true);
    client.set_run_result(true);

    EXPECT_TRUE(client.compact_wal());
    EXPECT_TRUE(client.prepare_called());
    EXPECT_TRUE(client.run_called());
}

TEST_F(wal_sync_client_test, compact_wal_returns_false_when_prepare_fails) {
    wal_sync_client_compact_testable client(locale_dir, helper_.create_channel());
    client.set_prepare_result(0, false);

    EXPECT_FALSE(client.compact_wal());
    EXPECT_TRUE(client.prepare_called());
    EXPECT_FALSE(client.run_called());
}

TEST_F(wal_sync_client_test, compact_wal_returns_false_when_run_compaction_fails) {
    wal_sync_client_compact_testable client(locale_dir, helper_.create_channel());
    client.set_prepare_result(6, true);
    client.set_run_result(false);

    EXPECT_FALSE(client.compact_wal());
    EXPECT_TRUE(client.prepare_called());
    EXPECT_TRUE(client.run_called());
}

TEST_F(wal_sync_client_test, compact_wal_returns_false_when_datastore_creation_fails) {
    wal_sync_client_compact_testable client(locale_dir, helper_.create_channel());
    client.set_create_datastore_should_fail(true);

    EXPECT_FALSE(client.compact_wal());
    EXPECT_FALSE(client.prepare_called());
    EXPECT_FALSE(client.run_called());
}

TEST_F(wal_sync_client_test, prepare_for_compaction_handles_unknown_exception) {
    wal_sync_client_prepare_unknown_testable client(locale_dir, helper_.create_channel());
    auto fake = std::make_unique<fake_rotation_datastore>(
        limestone::api::configuration({locale_dir}, locale_dir));
    fake->set_last_epoch(10);
    fake_rotation_datastore* fake_ptr = fake.get();
    client.set_fake_datastore(std::move(fake));

    std::atomic<bool> rotation_triggered{false};
    std::condition_variable rotation_cv;
    std::mutex rotation_mutex;

    auto result = client.prepare_for_compaction(*fake_ptr, rotation_triggered, rotation_cv, rotation_mutex);
    EXPECT_FALSE(result.second);
    EXPECT_EQ(result.first, 0);
}

TEST_F(wal_sync_client_test, restore_full_success) {
    bool const prev_throwing = limestone::testing::enable_exception_throwing;
    limestone::testing::enable_exception_throwing = true;

    if (!boost::filesystem::exists(test_dir)) {
        ASSERT_TRUE(boost::filesystem::create_directories(test_dir));
    }

    wal_sync_client client(locale_dir, helper_.create_channel());

    boost::filesystem::path backup_dir = test_dir / "restore_full_success";
    boost::filesystem::remove_all(backup_dir);
    ASSERT_TRUE(boost::filesystem::create_directories(backup_dir));
    limestone::internal::manifest::create_initial(backup_dir);

    EXPECT_TRUE(client.restore(0, 0, backup_dir));

    boost::filesystem::path manifest_path = locale_dir / std::string(limestone::internal::manifest::file_name);
    EXPECT_TRUE(boost::filesystem::exists(manifest_path));
    EXPECT_TRUE(boost::filesystem::is_empty(backup_dir));

    limestone::testing::enable_exception_throwing = prev_throwing;
}

TEST_F(wal_sync_client_test, restore_full_failure_without_manifest) {
    bool const prev_throwing = limestone::testing::enable_exception_throwing;
    limestone::testing::enable_exception_throwing = true;

    if (!boost::filesystem::exists(test_dir)) {
        ASSERT_TRUE(boost::filesystem::create_directories(test_dir));
    }

    wal_sync_client client(locale_dir, helper_.create_channel());

    boost::filesystem::path backup_dir = test_dir / "restore_full_missing_manifest";
    boost::filesystem::remove_all(backup_dir);
    ASSERT_TRUE(boost::filesystem::create_directories(backup_dir));

    EXPECT_FALSE(client.restore(0, 0, backup_dir));

    limestone::testing::enable_exception_throwing = prev_throwing;
}

TEST_F(wal_sync_client_test, restore_incremental_success_when_compaction_succeeds) {
    bool const prev_throwing = limestone::testing::enable_exception_throwing;
    limestone::testing::enable_exception_throwing = true;

    if (!boost::filesystem::exists(test_dir)) {
        ASSERT_TRUE(boost::filesystem::create_directories(test_dir));
    }

    wal_sync_client_compact_testable client(locale_dir, helper_.create_channel());
    client.set_prepare_result(5, true);
    client.set_run_result(true);

    boost::filesystem::path backup_dir = test_dir / "restore_incremental_success";
    boost::filesystem::remove_all(backup_dir);
    ASSERT_TRUE(boost::filesystem::create_directories(backup_dir));
    limestone::internal::manifest::create_initial(backup_dir);

    EXPECT_TRUE(client.restore(1, 0, backup_dir));
    EXPECT_TRUE(client.prepare_called());
    EXPECT_TRUE(client.run_called());

    limestone::testing::enable_exception_throwing = prev_throwing;
}

TEST_F(wal_sync_client_test, restore_incremental_fails_when_compaction_fails) {
    bool const prev_throwing = limestone::testing::enable_exception_throwing;
    limestone::testing::enable_exception_throwing = true;

    wal_sync_client_compact_testable client(locale_dir, helper_.create_channel());
    client.set_prepare_result(3, true);
    client.set_run_result(false);

    EXPECT_FALSE(client.restore(1, 0, remote_dir));
    EXPECT_TRUE(client.prepare_called());
    EXPECT_TRUE(client.run_called());

    limestone::testing::enable_exception_throwing = prev_throwing;
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
        client_ = std::make_unique<wal_sync_client_testable>(base_dir_, channel_);
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
    std::unique_ptr<wal_sync_client_testable> client_;
    failing_open_file_operations failing_ops_;
};

class failing_keepalive_wal_sync_client : public wal_sync_client_testable {
public:
    using wal_sync_client_testable::wal_sync_client_testable;

    bool keepalive_session(std::string const& /*session_token*/) override {
        return false;
    }
};

TEST_F(wal_sync_client_test, execute_remote_backup_keepalive_failure) {
    gen_datastore();
    prepare_backup_test_files();
    datastore_->shutdown();
    datastore_ = nullptr;

    helper_.start_server();

    failing_keepalive_wal_sync_client client(locale_dir, helper_.create_channel());
    std::string error;
    ASSERT_TRUE(client.init(error, true));

    boost::filesystem::path output_dir = locale_dir / "remote_backup_keepalive_failure";
    boost::filesystem::remove_all(output_dir);

    auto backup_result = client.execute_remote_backup(0, 0, output_dir);
    EXPECT_TRUE(backup_result.success);
    EXPECT_TRUE(backup_result.error_message.empty());
    EXPECT_TRUE(backup_result.incomplete_object_ids.empty());
    EXPECT_TRUE(boost::filesystem::exists(output_dir));
}

TEST_F(wal_sync_client_test, begin_backup_success) {
    gen_datastore();
    prepare_backup_test_files();
    datastore_->shutdown();
    datastore_ = nullptr;
    assert_backup_file_conditions([](const backup_condition& c) { return c.pre_rotation_path; });

    helper_.start_server();

    wal_sync_client_testable client(locale_dir, helper_.create_channel());
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

    wal_sync_client_testable client(locale_dir, helper_.create_channel());
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
    wal_sync_client_testable client(locale_dir, helper_.create_channel());
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

    wal_sync_client_testable client(locale_dir, helper_.create_channel());
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

    wal_sync_client_testable client(locale_dir, helper_.create_channel());
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


TEST_F(wal_sync_client_test, wal_sync_full_scenario_copy_backup) {
    // Build remote datastore so begin_backup() enumerates actual backup targets.
    gen_datastore();
    prepare_backup_test_files();
    datastore_->shutdown();
    datastore_ = nullptr;

    helper_.start_server();

    wal_sync_client_testable client(locale_dir, helper_.create_channel());
    std::string error;
    ASSERT_TRUE(client.init(error, true));

    // Validate local and remote epochs before running the scenario.
    auto remote_epoch = client.get_remote_epoch();
    ASSERT_TRUE(remote_epoch.has_value());
    EXPECT_EQ(remote_epoch.value(), 5);
    EXPECT_EQ(client.get_local_epoch(), 0);

    // Fetch session token and backup object list from the remote backup service.
    boost::filesystem::path output_dir = test_dir / "copied_backup";
    boost::filesystem::remove_all(output_dir);

    // Execute the end-to-end backup and confirm success.
    auto execute_result = client.execute_remote_backup(0, 0, output_dir);
    if (!execute_result.success) {
        std::cerr << "execute_remote_backup failed; incomplete_object_ids:";
        for (auto const& id : execute_result.incomplete_object_ids) {
            std::cerr << ' ' << id;
        }
        std::cerr << std::endl;
    }
    ASSERT_TRUE(execute_result.success);
    EXPECT_TRUE(execute_result.error_message.empty());
    EXPECT_TRUE(execute_result.incomplete_object_ids.empty());
    EXPECT_TRUE(boost::filesystem::exists(output_dir));

    bool restore_result = client.restore(0, 0, output_dir);
    if (!restore_result) {
        // reproduce internal call to obtain detailed status for debugging
        std::vector<boost::filesystem::path> data_locations{locale_dir};
        limestone::api::configuration conf(data_locations, locale_dir);
        limestone::api::datastore ds(conf);
        limestone::status s = ds.restore(output_dir.string(), false, /*purge_destination=*/true);
        std::cerr << "datastore::restore returned status: " << s << std::endl;
    }
    EXPECT_TRUE(restore_result);

    // Verify that the local files and remote files for backup targets are identical
    auto filtered_conditions = get_filtered_backup_conditions([](const backup_condition& c) {
        return c.is_offline_backup_target;
    });

    for (boost::filesystem::recursive_directory_iterator it(remote_dir), end; it != end; ++it) {
        if (!boost::filesystem::is_regular_file(it->path())) {
            continue;
        }
        boost::filesystem::path relative_path = boost::filesystem::relative(it->path(), remote_dir);
        // Only consider files that are declared as offline backup targets
        bool is_target = false;
        for (auto const& cond : filtered_conditions) {
            if (cond.object_path.empty()) {
                continue;
            }
            if (is_path_matching(relative_path.generic_string(), cond.object_path)) {
                is_target = true;
                break;
            }
        }
        if (!is_target) {
            continue;
        }

        boost::filesystem::path local_path = locale_dir / relative_path;
        EXPECT_TRUE(boost::filesystem::exists(local_path))
            << "missing local file: " << local_path.string();
    }

    EXPECT_EQ(client.get_local_epoch(), 5);

    // update the remote database
    helper_.tear_down();
    gen_datastore();
    datastore_->switch_epoch(7);
    lc0_->begin_session();
    lc0_->add_entry(1, "key6", "value6", {7, 7});
    lc0_->end_session();
    datastore_->switch_epoch(8);
    lc0_->begin_session();
    lc0_->add_entry(1, "key7", "value7", {8, 8});
    lc0_->end_session();
    datastore_->switch_epoch(9);
    lc0_->begin_session();
    lc0_->add_entry(1, "key8", "value8", {9, 9});
    lc0_->end_session();
    datastore_->switch_epoch(10);
    datastore_->shutdown();
    datastore_ = nullptr;

    // Check backup conditions
    helper_.start_server();
    remote_epoch = client.get_remote_epoch();
    ASSERT_TRUE(remote_epoch.has_value());
    EXPECT_EQ(remote_epoch.value(), 9);
    EXPECT_EQ(client.get_local_epoch(), 5);

    auto remote_wal_compatibility = client.get_remote_wal_compatibility();
    auto local_wal_compatibility = client.get_local_wal_compatibility();
    EXPECT_TRUE(remote_wal_compatibility.has_value());
    EXPECT_TRUE(client.check_wal_compatibility(local_wal_compatibility, remote_wal_compatibility.value()));
    EXPECT_FALSE(client.check_wal_compatibility(remote_wal_compatibility.value(), local_wal_compatibility));

    // Execute incremental backup
    boost::filesystem::remove_all(output_dir);
    auto begin = client.get_local_epoch();
    auto end = client.get_remote_epoch();
    ASSERT_TRUE(end.has_value());

    execute_result = client.execute_remote_backup(begin, end.value(), output_dir);
    ASSERT_TRUE(execute_result.success);
    EXPECT_TRUE(execute_result.error_message.empty());
    EXPECT_TRUE(execute_result.incomplete_object_ids.empty());
    EXPECT_TRUE(boost::filesystem::exists(output_dir));
    expect_epoch_file(output_dir, end.value());

    restore_result = client.restore(begin, end.value(), output_dir);
    EXPECT_TRUE(restore_result);

    // Also verify datastore-level equality: last_epoch and snapshot contents must match
    // Create a temporary datastore for the remote (original) and restored output and compare.
    {
        helper_.tear_down();
        // Remote datastore: open and create snapshot
        std::vector<std::pair<std::string, std::string>> remote_kv;
        {
            // construct datastore_test for remote_dir
            std::vector<boost::filesystem::path> data_locations{remote_dir};
            boost::filesystem::path metadata_location{remote_dir};
            limestone::api::configuration conf(data_locations, metadata_location);
            auto remote_ds = std::make_unique<limestone::api::datastore_test>(conf);
            // ensure snapshot exists
            remote_ds->ready();
            epoch_id_type remote_epoch = remote_ds->last_epoch();

            // read snapshot contents
            std::unique_ptr<limestone::api::snapshot> remote_snapshot = remote_ds->get_snapshot();
            std::unique_ptr<limestone::api::cursor> remote_cursor = remote_snapshot->get_cursor();
            while (remote_cursor->next()) {
                std::string key, value;
                remote_cursor->key(key);
                remote_cursor->value(value);
                remote_kv.emplace_back(key, value);
            }

            // Restored datastore
            std::vector<std::pair<std::string, std::string>> restored_kv;
            std::vector<boost::filesystem::path> restored_data_locations{locale_dir};
            boost::filesystem::path restored_metadata_location{locale_dir};
            limestone::api::configuration restored_conf(restored_data_locations, restored_metadata_location);
            auto restored_ds = std::make_unique<limestone::api::datastore_test>(restored_conf);
            restored_ds->ready();
            epoch_id_type restored_epoch = restored_ds->last_epoch();

            std::unique_ptr<limestone::api::snapshot> restored_snapshot = restored_ds->get_snapshot();
            std::unique_ptr<limestone::api::cursor> restored_cursor = restored_snapshot->get_cursor();
            while (restored_cursor->next()) {
                std::string key, value;
                restored_cursor->key(key);
                restored_cursor->value(value);
                restored_kv.emplace_back(key, value);
            }

            // Compare epochs
            EXPECT_EQ(remote_epoch, restored_epoch) << "last_epoch mismatch between remote and restored datastore";

            // Compare snapshot key/value sets (order-insensitive)
            std::multiset<std::pair<std::string, std::string>> a(remote_kv.begin(), remote_kv.end());
            std::multiset<std::pair<std::string, std::string>> b(restored_kv.begin(), restored_kv.end());
            EXPECT_EQ(a, b) << "snapshot contents differ between remote and restored datastore";
        }
    }
}

} // namespace limestone::testing
