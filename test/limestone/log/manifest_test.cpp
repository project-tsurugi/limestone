#include <gtest/gtest.h>
#include <boost/filesystem.hpp>
#include <fstream>
#include <nlohmann/json.hpp>

#include "manifest.h"
#include "limestone/api/limestone_exception.h"

namespace limestone::testing {

using namespace limestone::internal;

constexpr const char* k_manifest_test_dir = "/tmp/manifest_test";

class manifest_test : public ::testing::Test {
protected:
    boost::filesystem::path logdir;

    void SetUp() override {
        logdir = k_manifest_test_dir;
        boost::filesystem::remove_all(logdir);
        boost::filesystem::create_directories(logdir);
    }

    void TearDown() override {
        boost::filesystem::remove_all(logdir);
    }
};

// Tests that create_initial() writes a valid JSON manifest file with expected fields.
TEST_F(manifest_test, create_initial_creates_manifest_file_with_correct_content) {
    manifest::create_initial(logdir);

    boost::filesystem::path manifest_path = logdir / std::string(manifest::file_name);
    ASSERT_TRUE(boost::filesystem::exists(manifest_path));

    std::ifstream ifs(manifest_path.string());
    ASSERT_TRUE(ifs) << "Failed to open manifest file for reading";
    nlohmann::json j;
    ASSERT_NO_THROW(ifs >> j);

    ASSERT_TRUE(j.contains("format_version"));
    EXPECT_EQ(j["format_version"].get<std::string>(), "1.0");

    ASSERT_TRUE(j.contains("persistent_format_version"));
    EXPECT_EQ(j["persistent_format_version"].get<int>(), 4);
}

// Tests that create_initial() throws limestone_io_exception when logdir path is a file
TEST_F(manifest_test, create_initial_throws_when_logdir_is_file) {
    boost::filesystem::remove_all(logdir);
    std::ofstream ofs(logdir.string());
    ASSERT_TRUE(ofs) << "Failed to create file at logdir path";
    ofs.close();

    EXPECT_THROW(
        manifest::create_initial(logdir),
        limestone::api::limestone_io_exception
    );
}

// Tests for failure scenarios using a custom file_operations stub
class failing_file_ops : public real_file_operations {
public:
    enum class fail_step_type { none, fopen, fwrite, fflush, fsync, fclose };
    fail_step_type fail_step = fail_step_type::none;

    FILE* fopen(const char* path, const char* mode) override {
        if (fail_step == fail_step_type::fopen) return nullptr;
        return real_file_operations::fopen(path, mode);
    }
    size_t fwrite(const void* ptr, size_t size, size_t nmemb, FILE* stream) override {
        if (fail_step == fail_step_type::fwrite) return 0;
        return real_file_operations::fwrite(ptr, size, nmemb, stream);
    }
    int fflush(FILE* stream) override {
        if (fail_step == fail_step_type::fflush) return EOF;
        return real_file_operations::fflush(stream);
    }
    int fsync(int fd) override {
        if (fail_step == fail_step_type::fsync) return -1;
        return real_file_operations::fsync(fd);
    }
    int fclose(FILE* stream) override {
        if (fail_step == fail_step_type::fclose) return EOF;
        return real_file_operations::fclose(stream);
    }
};

TEST_F(manifest_test, create_initial_fails_on_fwrite_error) {
    failing_file_ops ops;
    ops.fail_step = failing_file_ops::fail_step_type::fwrite;
    try {
        manifest::create_initial(logdir, ops);
        FAIL() << "Expected limestone_io_exception";
    } catch (const limestone::api::limestone_io_exception& e) {
        // Ensure the error message indicates write failure and includes manifest filename
        std::string what = e.what();
        EXPECT_NE(what.find("Failed to write to file"), std::string::npos);
        EXPECT_NE(what.find(std::string(manifest::file_name)), std::string::npos);
    }
}

TEST_F(manifest_test, create_initial_fails_on_fflush_error) {
    failing_file_ops ops;
    ops.fail_step = failing_file_ops::fail_step_type::fflush;
    try {
        manifest::create_initial(logdir, ops);
        FAIL() << "Expected limestone_io_exception";
    } catch (const limestone::api::limestone_io_exception& e) {
        std::string what = e.what();
        EXPECT_NE(what.find("Failed to flush file buffer"), std::string::npos);
        EXPECT_NE(what.find(std::string(manifest::file_name)), std::string::npos);
    }
}


TEST_F(manifest_test, create_initial_fails_on_fsync_error) {
    failing_file_ops ops;
    ops.fail_step = failing_file_ops::fail_step_type::fsync;
    try {
        manifest::create_initial(logdir, ops);
        FAIL() << "Expected limestone_io_exception";
    } catch (const limestone::api::limestone_io_exception& e) {
        std::string what = e.what();
        EXPECT_NE(what.find("Failed to sync file to disk"), std::string::npos);
        EXPECT_NE(what.find(std::string(manifest::file_name)), std::string::npos);
    }
}


TEST_F(manifest_test, create_initial_fails_on_fclose_error) {
    failing_file_ops ops;
    ops.fail_step = failing_file_ops::fail_step_type::fclose;
    try {
        manifest::create_initial(logdir, ops);
        FAIL() << "Expected limestone_io_exception";
    } catch (const limestone::api::limestone_io_exception& e) {
        std::string what = e.what();
        EXPECT_NE(what.find("Failed to close file"), std::string::npos);
        EXPECT_NE(what.find(std::string(manifest::file_name)), std::string::npos);
    }
}

// Tests for acquire_lock()
TEST_F(manifest_test, acquire_lock_success_and_close) {
    // Prepare: create manifest file so open succeeds
    manifest::create_initial(logdir);
    // Call default overload
    int fd = manifest::acquire_lock(logdir);
    ASSERT_GE(fd, 0) << "Expected non-negative fd on success";
    // Release lock and close
    EXPECT_EQ(0, ::close(fd));
}

// Tests that acquire_lock enforces exclusive locking
TEST_F(manifest_test, acquire_lock_exclusive) {
    // Prepare the manifest file
    manifest::create_initial(logdir);

    // First locker: should succeed
    int fd1 = manifest::acquire_lock(logdir);
    ASSERT_GE(fd1, 0) << "First lock acquisition should succeed";

    // Second locker: should fail immediately (LOCK_NB)
    int fd2 = manifest::acquire_lock(logdir);
    EXPECT_EQ(fd2, -1) << "Second lock acquisition should fail when already held";

    // Release the first lock
    EXPECT_EQ(0, ::close(fd1)) << "Failed to close first lock fd";

    // Third locker: now that the file is unlocked, it should succeed again
    int fd3 = manifest::acquire_lock(logdir);
    ASSERT_GE(fd3, 0) << "Lock acquisition should succeed after releasing the first lock";
    EXPECT_EQ(0, ::close(fd3));
}


// Stub to simulate open, flock and close failures
class lock_file_ops : public real_file_operations {
public:
    bool simulate_open_fail = false;
    bool simulate_flock_fail = false;
    bool simulate_close_fail = false;

    int open(const char* filename, int flags) override {
        if (simulate_open_fail) return -1;
        return real_file_operations::open(filename, flags);
    }
    int flock(int fd, int operation) override {
        if (simulate_flock_fail) return -1;
        return real_file_operations::flock(fd, operation);
    }
    int close(int fd) override {
        if (simulate_close_fail) return -1;
        return real_file_operations::close(fd);
    }
};

TEST_F(manifest_test, acquire_lock_open_fails) {
    lock_file_ops ops;
    ops.simulate_open_fail = true;
    int ret = manifest::acquire_lock(logdir, ops);
    EXPECT_EQ(ret, -1);
}

TEST_F(manifest_test, acquire_lock_flock_fails) {
    manifest::create_initial(logdir);
    lock_file_ops ops;
    ops.simulate_flock_fail = true;
    int ret = manifest::acquire_lock(logdir, ops);
    EXPECT_EQ(ret, -1);
}

TEST_F(manifest_test, acquire_lock_close_fails_after_flock_fail) {
    manifest::create_initial(logdir);
    lock_file_ops ops;
    ops.simulate_flock_fail = true;
    ops.simulate_close_fail = true;
    int ret = manifest::acquire_lock(logdir, ops);
    EXPECT_EQ(ret, -1);
}

// Tests for is_supported_version()
TEST_F(manifest_test, is_supported_version_returns_supported_version) {
    // Create a manifest file with persistent_format_version = 3
    nlohmann::json j = { {"format_version", "1.0"}, {"persistent_format_version", 3} };
    auto path = logdir / std::string(manifest::file_name);
    std::ofstream ofs(path.string());
    ofs << j.dump();
    ofs.close();

    std::string errmsg;
    int v = manifest::is_supported_version(path, errmsg);
    EXPECT_EQ(v, 3);
    EXPECT_TRUE(errmsg.empty());
}

TEST_F(manifest_test, is_supported_version_returns_zero_and_message_on_unsupported_version) {
    // persistent_format_version = 10 (unsupported)
    nlohmann::json j = { {"format_version", "1.0"}, {"persistent_format_version", 10} };
    auto path = logdir / std::string(manifest::file_name);
    std::ofstream(path.string()) << j.dump();

    std::string errmsg;
    int v = manifest::is_supported_version(path, errmsg);
    EXPECT_EQ(v, 0);
    EXPECT_NE(errmsg.find("version mismatch"), std::string::npos);
}

TEST_F(manifest_test, is_supported_version_returns_negative_on_invalid_type) {
    // persistent_format_version is a string
    nlohmann::json j = { {"format_version", "1.0"}, {"persistent_format_version", "bad"} };
    auto path = logdir / std::string(manifest::file_name);
    std::ofstream ofs(path.string());
    ofs << j.dump();
    ofs.close();

    std::string errmsg;
    int v = manifest::is_supported_version(path, errmsg);
    EXPECT_LT(v, 0);
    EXPECT_NE(errmsg.find("invalid manifest file"), std::string::npos);
}

TEST_F(manifest_test, is_supported_version_returns_negative_on_json_parse_error) {
    // Write malformed JSON
    auto path = logdir / std::string(manifest::file_name);
    std::ofstream ofs(path.string());
    ofs << "{ not json ";
    ofs.close();

    std::string errmsg;
    int v = manifest::is_supported_version(path, errmsg);
    EXPECT_LT(v, 0);
    EXPECT_NE(errmsg.find("parse error"), std::string::npos);
}

TEST_F(manifest_test, is_supported_version_returns_zero_when_file_not_openable) {
    // path does not exist
    auto path = logdir / std::string(manifest::file_name);
    std::string errmsg;
    int v = manifest::is_supported_version(path, errmsg);
    EXPECT_EQ(v, 0);
    EXPECT_NE(errmsg.find("cannot open for read"), std::string::npos);
}


TEST_F(manifest_test, check_and_migrate_uses_backup_when_manifest_missing) {
    // Prepare: only backup file exists
    auto backup_path = logdir / std::string(manifest::backup_file_name);
    nlohmann::json j = { {"format_version", "1.0"}, {"persistent_format_version", 4} };
    std::ofstream(backup_path.string()) << j.dump();
    ASSERT_FALSE(boost::filesystem::exists(logdir / std::string(manifest::file_name)));
    ASSERT_TRUE(boost::filesystem::exists(backup_path));

    // Act: should rename backup -> manifest, and remove backup
    EXPECT_NO_THROW(manifest::check_and_migrate(logdir));
    EXPECT_TRUE(boost::filesystem::exists(logdir / std::string(manifest::file_name)));
    EXPECT_FALSE(boost::filesystem::exists(backup_path));
}

TEST_F(manifest_test, check_and_migrate_removes_backup_when_both_exist) {
    // Prepare: both manifest and backup exist
    auto manifest_path = logdir / std::string(manifest::file_name);
    auto backup_path   = logdir / std::string(manifest::backup_file_name);
    nlohmann::json j = { {"format_version", "1.0"}, {"persistent_format_version", 4} };
    std::ofstream(manifest_path.string()) << j.dump();
    std::ofstream(backup_path.string())   << j.dump();
    ASSERT_TRUE(boost::filesystem::exists(manifest_path));
    ASSERT_TRUE(boost::filesystem::exists(backup_path));

    // Act: should remove backup, leave manifest untouched
    EXPECT_NO_THROW(manifest::check_and_migrate(logdir));
    EXPECT_TRUE(boost::filesystem::exists(manifest_path));
    EXPECT_FALSE(boost::filesystem::exists(backup_path));
}

TEST_F(manifest_test, check_and_migrate_throws_when_no_manifest_or_backup) {
    // Nothing in logdir
    ASSERT_FALSE(boost::filesystem::exists(logdir / std::string(manifest::file_name)));
    ASSERT_FALSE(boost::filesystem::exists(logdir / std::string(manifest::backup_file_name)));

    // Act & Assert: should throw fatal limestone_exception due to missing manifest
    EXPECT_THROW(
        manifest::check_and_migrate(logdir),
        limestone::api::limestone_exception
    );
}

TEST_F(manifest_test, check_and_migrate_throws_on_unsupported_version) {
    // Write manifest with version 0 (unsupported)
    auto manifest_path = logdir / std::string(manifest::file_name);
    nlohmann::json j = { {"format_version", "1.0"}, {"persistent_format_version", 0} };
    std::ofstream(manifest_path.string()) << j.dump();
    std::string errmsg;
    int vc = manifest::is_supported_version(manifest_path, errmsg);
    ASSERT_EQ(vc, 0);

    // Act & Assert: version mismatch leads to limestone_exception
    EXPECT_THROW(
        manifest::check_and_migrate(logdir),
        limestone::api::limestone_exception
    );
}

TEST_F(manifest_test, check_and_migrate_migrates_old_version_to_latest) {
    // Write manifest with persistent_format_version = 2
    auto manifest_path = logdir / std::string(manifest::file_name);
    nlohmann::json old = { {"format_version", "1.0"}, {"persistent_format_version", 2} };
    std::ofstream(manifest_path.string()) << old.dump();

    // Act: migration should succeed
    EXPECT_NO_THROW(manifest::check_and_migrate(logdir));

    // After migration: new manifest has version 4, backup removed
    std::ifstream ifs((logdir / std::string(manifest::file_name)).string());
    nlohmann::json updated; ifs >> updated;
    EXPECT_EQ(updated["persistent_format_version"].get<int>(), 4);

    auto backup_path = logdir / std::string(manifest::backup_file_name);
    EXPECT_FALSE(boost::filesystem::exists(backup_path));
}

// Stub to simulate rename/remove failures
class migrate_file_ops : public real_file_operations {
public:
    bool fail_rename_backup_to_manifest = false;
    bool fail_remove_backup_after_both_exist = false;
    bool fail_rename_manifest_to_backup = false;
    bool fail_remove_backup_after_migration = false;

    void rename(const boost::filesystem::path& old_path,
                const boost::filesystem::path& new_path,
                boost::system::error_code& ec) override {
        if ((fail_rename_backup_to_manifest && old_path.string().find(manifest::backup_file_name) != std::string::npos)
         || (fail_rename_manifest_to_backup && old_path.string().find(manifest::file_name) != std::string::npos
             && new_path.string().find(manifest::backup_file_name) != std::string::npos)) {
            ec = boost::system::errc::make_error_code(boost::system::errc::io_error);
            return;
        }
        real_file_operations::rename(old_path, new_path, ec);
    }

    void remove(const boost::filesystem::path& path,
                boost::system::error_code& ec) override {
        if ((fail_remove_backup_after_both_exist && path.string().find(manifest::backup_file_name) != std::string::npos)
         || (fail_remove_backup_after_migration && path.string().find(manifest::backup_file_name) != std::string::npos)) {
            ec = boost::system::errc::make_error_code(boost::system::errc::io_error);
            return;
        }
        real_file_operations::remove(path, ec);
    }
};

// rename backup->manifest fails
TEST_F(manifest_test, check_and_migrate_rename_backup_to_manifest_failure) {
    // Prepare: only backup exists
    auto backup_path = logdir / std::string(manifest::backup_file_name);
    nlohmann::json j = { {"format_version", "1.0"}, {"persistent_format_version", 4} };
    std::ofstream(backup_path.string()) << j.dump();

    migrate_file_ops ops;
    ops.fail_rename_backup_to_manifest = true;
    try {
        manifest::check_and_migrate(logdir, ops);
        FAIL() << "Expected limestone_io_exception";
    } catch (const limestone::api::limestone_io_exception& e) {
        std::string what = e.what();
        EXPECT_NE(what.find("Failed to rename manifest backup"), std::string::npos);
        EXPECT_NE(what.find(std::string(manifest::backup_file_name)), std::string::npos);
    }
}

// remove backup when both exist fails
TEST_F(manifest_test, check_and_migrate_remove_backup_failure_when_both_exist) {
    // Prepare: both manifest and backup exist
    auto manifest_path = logdir / std::string(manifest::file_name);
    auto backup_path   = logdir / std::string(manifest::backup_file_name);
    nlohmann::json j = { {"format_version", "1.0"}, {"persistent_format_version", 4} };
    std::ofstream(manifest_path.string()) << j.dump();
    std::ofstream(backup_path.string())   << j.dump();

    migrate_file_ops ops;
    ops.fail_remove_backup_after_both_exist = true;
    try {
        manifest::check_and_migrate(logdir, ops);
        FAIL() << "Expected limestone_io_exception";
    } catch (const limestone::api::limestone_io_exception& e) {
        std::string what = e.what();
        EXPECT_NE(what.find("Failed to remove backup manifest file"), std::string::npos);
        EXPECT_NE(what.find(std::string(manifest::backup_file_name)), std::string::npos);
    }
}

// rename manifest->backup fails during migration
TEST_F(manifest_test, check_and_migrate_rename_manifest_to_backup_failure) {
    // Prepare: manifest with older version
    auto manifest_path = logdir / std::string(manifest::file_name);
    nlohmann::json j = { {"format_version", "1.0"}, {"persistent_format_version", 2} };
    std::ofstream(manifest_path.string()) << j.dump();

    migrate_file_ops ops;
    ops.fail_rename_manifest_to_backup = true;
    try {
        manifest::check_and_migrate(logdir, ops);
        FAIL() << "Expected limestone_io_exception";
    } catch (const limestone::api::limestone_io_exception& e) {
        std::string what = e.what();
        EXPECT_NE(what.find("Failed to rename manifest file"), std::string::npos);
        EXPECT_NE(what.find(std::string(manifest::file_name)), std::string::npos);
    }
}

// remove backup after migration fails
TEST_F(manifest_test, check_and_migrate_remove_backup_after_migration_failure) {
    // Prepare: manifest with older version
    auto manifest_path = logdir / std::string(manifest::file_name);
    nlohmann::json j = { {"format_version", "1.0"}, {"persistent_format_version", 2} };
    std::ofstream(manifest_path.string()) << j.dump();

    migrate_file_ops ops;
    ops.fail_remove_backup_after_migration = true;
    try {
        manifest::check_and_migrate(logdir, ops);
        FAIL() << "Expected limestone_io_exception";
    } catch (const limestone::api::limestone_io_exception& e) {
        std::string what = e.what();
        EXPECT_NE(what.find("Failed to remove backup manifest file"), std::string::npos);
        EXPECT_NE(what.find(std::string(manifest::backup_file_name)), std::string::npos);
    }
}

// Test check_and_migrate throws when is_supported_version returns negative (corrupted manifest)
TEST_F(manifest_test, check_and_migrate_throws_on_corrupted_manifest) {
    // Write invalid JSON to cause parse error
    auto manifest_path = logdir / std::string(manifest::file_name);
    std::ofstream ofs(manifest_path.string());
    ofs << "{ invalid json }";
    ofs.close();

    // Expect: corrupted manifest leads to limestone_exception("logdir corrupted")
    try {
        manifest::check_and_migrate(logdir);
        FAIL() << "Expected limestone_exception for corrupted manifest";
    } catch (const limestone::api::limestone_exception& e) {
        std::string what = e.what();
        EXPECT_NE(what.find("logdir corrupted"), std::string::npos);
    }
}

class exists_error_file_ops : public limestone::internal::real_file_operations {
public:
    bool exists(const boost::filesystem::path& path, boost::system::error_code& ec) override {
        ec = boost::system::errc::make_error_code(boost::system::errc::permission_denied); // 例: permission_denied
        return false;
    }
};

class manifest_test_access : public limestone::internal::manifest {
public:
    using limestone::internal::manifest::exists_path_with_ops;
};

TEST_F(manifest_test, exists_path_throws_on_filesystem_error) {
    boost::filesystem::path some_path = "/tmp/xxx";
    exists_error_file_ops ops;
    EXPECT_THROW(
        manifest_test_access::exists_path_with_ops(some_path, ops),
        limestone::api::limestone_io_exception
    );
}

// Test that getter/setter for format_version work correctly
TEST_F(manifest_test, format_version_getter_setter) {
    manifest m;
    m.set_format_version("1.5");
    EXPECT_EQ(m.get_format_version(), "1.5");
}

// Test that getter/setter for persistent_format_version work correctly
TEST_F(manifest_test, persistent_format_version_getter_setter) {
    manifest m;
    m.set_persistent_format_version(42);
    EXPECT_EQ(m.get_persistent_format_version(), 42);
}

// Test to_json_string produces valid JSON string
TEST_F(manifest_test, to_json_string_outputs_valid_json) {
    manifest m;
    m.set_format_version("A.B.C");
    m.set_persistent_format_version(777);

    std::string json_str = m.to_json_string();
    nlohmann::json j = nlohmann::json::parse(json_str);

    ASSERT_TRUE(j.contains("format_version"));
    EXPECT_EQ(j["format_version"].get<std::string>(), "A.B.C");

    ASSERT_TRUE(j.contains("persistent_format_version"));
    EXPECT_EQ(j["persistent_format_version"].get<int>(), 777);
}

// Test from_json_string parses valid JSON and populates fields
TEST_F(manifest_test, from_json_string_parses_json_and_sets_fields) {
    nlohmann::json j = {
        {"format_version", "2.3.4"},
        {"persistent_format_version", 1234}
    };
    std::string json_str = j.dump();

    manifest m = manifest::from_json_string(json_str);

    EXPECT_EQ(m.get_format_version(), "2.3.4");
    EXPECT_EQ(m.get_persistent_format_version(), 1234);
}

// Test to_json_string and from_json_string are consistent (round-trip)
TEST_F(manifest_test, to_json_string_and_from_json_string_round_trip) {
    manifest m1;
    m1.set_format_version("9.9.9");
    m1.set_persistent_format_version(999);

    std::string json_str = m1.to_json_string();
    manifest m2 = manifest::from_json_string(json_str);

    EXPECT_EQ(m1.get_format_version(), m2.get_format_version());
    EXPECT_EQ(m1.get_persistent_format_version(), m2.get_persistent_format_version());
}

// Test from_json_string throws when format_version is missing
TEST_F(manifest_test, from_json_string_throws_if_format_version_missing) {
    nlohmann::json j = {{"persistent_format_version", 4}};
    std::string json_str = j.dump();

    EXPECT_THROW(
        manifest::from_json_string(json_str),
        limestone::api::limestone_exception
    );
}

// Test from_json_string throws when persistent_format_version is missing
TEST_F(manifest_test, from_json_string_throws_if_persistent_format_version_missing) {
    nlohmann::json j = {{"format_version", "X.Y.Z"}};
    std::string json_str = j.dump();

    EXPECT_THROW(
        manifest::from_json_string(json_str),
        limestone::api::limestone_exception
    );
}

// Test from_json_string throws when JSON is invalid
TEST_F(manifest_test, from_json_string_throws_on_invalid_json) {
    std::string json_str = "{ not: json }";
    EXPECT_THROW(
        manifest::from_json_string(json_str),
        limestone::api::limestone_exception
    );
}

// Test from_json_string throws when types are invalid
TEST_F(manifest_test, from_json_string_throws_on_invalid_type) {
    nlohmann::json j = {
        {"format_version", 123}, // not a string
        {"persistent_format_version", "abc"} // not an int
    };
    std::string json_str = j.dump();

    EXPECT_THROW(
        manifest::from_json_string(json_str),
        limestone::api::limestone_exception
    );
}


}  // namespace limestone::testing
