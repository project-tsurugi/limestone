#include <gtest/gtest.h>
#include <boost/filesystem.hpp>
#include "test_root.h"
#include "wal_sync/wal_sync_client.h"
#include "manifest.h"
#include "limestone/grpc/backend_test_fixture.h"

namespace limestone::testing {
using namespace limestone::internal;


class wal_sync_client_test : public backend_test_fixture {
protected:
    boost::filesystem::path test_dir = "/tmp/wal_sync_client_test";
	boost::filesystem::path locale_dir = "/tmp/wal_sync_client_test/local";
	boost::filesystem::path remote_dir = "/tmp/wal_sync_client_test/remote";

    char const* get_location() const override {
        return remote_dir.string().c_str();
    }

	void SetUp() override {
        boost::filesystem::remove_all(locale_dir);
        if (!boost::filesystem::create_directories(locale_dir)) {
            std::cerr << "cannot make directory" << std::endl;
        }	
		backend_test_fixture::SetUp();
    }

	void TearDown() override {
		backend_test_fixture::TearDown();
        boost::filesystem::remove_all(test_dir);
	}
};

TEST_F(wal_sync_client_test, init_creates_manifest_when_dir_not_exist_and_allowed) {
	wal_sync_client client(locale_dir);
	std::string error;
	EXPECT_TRUE(client.init(error, true));
	boost::filesystem::path manifest_path = locale_dir / "limestone-manifest.json";
	EXPECT_TRUE(boost::filesystem::exists(manifest_path));
}

TEST_F(wal_sync_client_test, init_fails_when_dir_not_exist_and_not_allowed) {
	wal_sync_client client(locale_dir);
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
		wal_sync_client client(locale_dir);
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
	wal_sync_client client(locale_dir);
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
	wal_sync_client client(locale_dir);
	std::string error;
	EXPECT_FALSE(client.init(error, true));
	EXPECT_NE(error.find("log_dir is not a directory"), std::string::npos);
}

TEST_F(wal_sync_client_test, init_fails_when_dir_is_empty_and_not_allowed) {
	wal_sync_client client(locale_dir);
	std::string error;
	EXPECT_FALSE(client.init(error, false));
	EXPECT_NE(error.find("log_dir is empty"), std::string::npos);
}

TEST_F(wal_sync_client_test, init_fails_when_dir_not_exist_and_not_allowed_2) {
	// Redundant with init_fails_when_dir_not_exist_and_not_allowed, but explicit for allow_initialize=false
    boost::filesystem::remove_all(locale_dir);
	wal_sync_client client(locale_dir);
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
	wal_sync_client client(locale_dir);
	std::string error;
	EXPECT_FALSE(client.init(error, false));
	EXPECT_NE(error.find("manifest file not found"), std::string::npos);
}

TEST_F(wal_sync_client_test, init_fails_when_manifest_format_version_is_invalid) {
	boost::filesystem::path manifest_path = locale_dir / "limestone-manifest.json";
	std::ofstream ofs(manifest_path.string());
	ofs << "{\"format_version\":\"bad\",\"instance_uuid\":\"ddf87e86-08b8-4577-a21e-250e3a0f652e\",\"persistent_format_version\":7}";
	ofs.close();
	wal_sync_client client(locale_dir);
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
	wal_sync_client client(locale_dir);
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
	wal_sync_client client(locale_dir);
	std::string error;
	EXPECT_FALSE(client.init(error, false));
	EXPECT_NE(error.find("manifest file not found or invalid"), std::string::npos);
}

TEST_F(wal_sync_client_test, init_fails_when_lock_cannot_be_acquired) {
	limestone::internal::manifest::create_initial(locale_dir);
	// Acquire lock manually
	int fd = limestone::internal::manifest::acquire_lock(locale_dir);
	ASSERT_GE(fd, 0);
	wal_sync_client client(locale_dir);
	std::string error;
	EXPECT_FALSE(client.init(error, false));
	EXPECT_NE(error.find("failed to acquire manifest lock"), std::string::npos);
	::close(fd);
}

TEST_F(wal_sync_client_test, get_local_epoch_returns_zero_when_no_wal_files) {
    limestone::internal::manifest::create_initial(locale_dir);

    wal_sync_client client(locale_dir);
    std::string error;
    ASSERT_TRUE(client.init(error, false));

    // No WAL files present, should return 0
    EXPECT_EQ(client.get_local_epoch(), 0);
}

TEST_F(wal_sync_client_test, get_local_epoch_returns_last_durable_epoch) {
    gen_datastore();
    prepare_backup_test_files();
    datastore_=nullptr;

    wal_sync_client client(remote_dir);
    std::string error;
    ASSERT_TRUE(client.init(error, false));

    // Should return the highest epoch (5)
    EXPECT_EQ(client.get_local_epoch(), 5);
}

} // namespace limestone::testing

