
#include "wal_sync/wal_sync_client.h"

#include <chrono>
#include <regex>
#include <sstream>
#include <unordered_set>

#include <boost/filesystem.hpp>
#include <gtest/gtest.h>

#include "limestone/grpc/grpc_test_helper.h"
#include "limestone/grpc/backend_test_fixture.h"
#include "manifest.h"
#include "test_root.h"
#include "wal_sync/remote_exception.h"
#include "limestone/grpc/service/wal_history_service_impl.h"
#include "limestone/grpc/service/backup_service_impl.h"
#include "limestone/grpc/service/grpc_constants.h"

namespace limestone::testing {
using namespace limestone::internal;
using limestone::grpc::service::session_timeout_seconds;


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
    EXPECT_EQ(client.get_remote_epoch(), 5);
}

TEST_F(wal_sync_client_test, get_remote_epoch_failure) {
    wal_sync_client client(locale_dir, helper_.create_channel());

    // Call the method and expect an exception
    try {
        client.get_remote_epoch();
        FAIL() << "Expected remote_exception to be thrown";
    } catch (const remote_exception& ex) {
        EXPECT_EQ(ex.code(), remote_error_code::unavailable);
        EXPECT_EQ(ex.method(), "WalHistoryService/GetWalHistory");
        EXPECT_NE(std::string(ex.what()).find("failed to connect to all addresses"), std::string::npos);
    } catch (...) {
        FAIL() << "Expected remote_exception, but caught a different exception";
    }
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

    ASSERT_EQ(branch_epochs.size(), expected.size());
    for (int i = 0; i < branch_epochs.size(); ++i) {
        const auto& branch_epoch = branch_epochs[i];
        const auto& exp = expected[i];
        EXPECT_EQ(branch_epoch.epoch, exp.epoch);
        EXPECT_EQ(branch_epoch.identity, exp.identity);
        EXPECT_EQ(branch_epoch.timestamp, static_cast<unix_timestamp_seconds>(exp.timestamp));
    }
}

TEST_F(wal_sync_client_test, get_remote_wal_compatibility_failure) {
    wal_sync_client client(locale_dir, helper_.create_channel());

    // Call the method and expect an exception
    try {
        client.get_remote_wal_compatibility();
        FAIL() << "Expected remote_exception to be thrown";
    } catch (const remote_exception& ex) {
        EXPECT_EQ(ex.code(), remote_error_code::unavailable);
        EXPECT_EQ(ex.method(), "WalHistoryService/GetWalHistory");
        EXPECT_NE(std::string(ex.what()).find("failed to connect to all addresses"), std::string::npos);
    } catch (...) {
        FAIL() << "Expected remote_exception, but caught a different exception";
    }
}

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
    auto result = client.begin_backup(0, 0);
    auto after = std::chrono::system_clock::now();

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
        EXPECT_EQ(object.type, static_cast<limestone::grpc::backend::backup_object_type>(cond.object_type));
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

TEST_F(wal_sync_client_test, begin_backup_failure) {
    wal_sync_client client(locale_dir, helper_.create_channel());
    std::string error;
    ASSERT_TRUE(client.init(error, true));

    try {
        static_cast<void>(client.begin_backup(0, 0));
        FAIL() << "Expected remote_exception to be thrown";
    } catch (const remote_exception& ex) {
        EXPECT_EQ(ex.code(), remote_error_code::unavailable);
        EXPECT_EQ(ex.method(), "BackupService/BeginBackup");
    } catch (...) {
        FAIL() << "Expected remote_exception, but caught different exception";
    }
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


} // namespace limestone::testing
