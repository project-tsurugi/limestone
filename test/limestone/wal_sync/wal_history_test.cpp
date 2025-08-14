#include <gtest/gtest.h>
#include <boost/filesystem.hpp>
#include <fstream>
#include "test_root.h"
#include "wal_sync/wal_history.h"
#include "limestone_exception_helper.h"

namespace limestone::testing {
using namespace limestone::internal;
using namespace limestone::api;

constexpr const char* base_directory = "/tmp/wal_history_test";

// Use subclass to access protected set_file_operations
class wal_history_testable : public wal_history {
public:
    using wal_history::wal_history;
    using wal_history::set_file_operations;
    using wal_history::record_size;
    using wal_history::write_record; 
    using wal_history::parse_record;
};

class wal_history_test : public ::testing::Test {
public:
    class failing_file_ops : public real_file_operations {
    public:
        bool remove_failed = false;
        bool rename_failed = false;
        int unlink(const char* filename) override {
            if (remove_failed) return -1;
            return real_file_operations::unlink(filename);
        }
        int rename(const char* oldname, const char* newname) override {
            if (rename_failed) return -1;
            return real_file_operations::rename(oldname, newname);
        }
    };

    class failing_read_file_ops : public real_file_operations {
    public:
        bool fail_open = false;
        std::unique_ptr<std::ofstream> open_ofstream(const std::string& filename) override {
            if (fail_open) {
                errno = ENOENT;
                return std::make_unique<std::ofstream>();  // is_open() == false
            }
            return real_file_operations::open_ofstream(filename);
        }
    };

protected:
    boost::filesystem::path test_dir;

    void SetUp() override {
        int rm_result = system((std::string("rm -rf ") + base_directory).c_str());
        int mkdir_result = system((std::string("mkdir -p ") + base_directory).c_str());
        test_dir = base_directory;
    }
    void TearDown() override {
        int rm_result2 = system((std::string("rm -rf ") + base_directory).c_str());
    }
};

TEST_F(wal_history_test, append_and_list) {
    wal_history wh(test_dir);
    wh.append(100);
    wh.append(200);
    auto records = wh.list();
    ASSERT_EQ(records.size(), 2u);
    EXPECT_EQ(records[0].epoch, 100u);
    EXPECT_EQ(records[1].epoch, 200u);
}

TEST_F(wal_history_test, write_record_and_list_consistency) {
    wal_history_testable wh(test_dir);
    boost::filesystem::path file_path = test_dir / "wal_history";

    FILE* fp = fopen(file_path.string().c_str(), "wb");
    ASSERT_TRUE(fp != nullptr);
    std::vector<epoch_id_type> epochs = {42, 43, 44};
    std::vector<std::array<std::uint8_t, 16>> unique_ids;
    std::vector<std::int64_t> timestamps;
    for (size_t i = 0; i < epochs.size(); ++i) {
        boost::uuids::uuid uuid = {};
        std::array<std::uint8_t, 16> unique_id;
        std::memcpy(unique_id.data(), uuid.data, 16);
        std::int64_t timestamp = 1234567890 + i;
        wh.write_record(fp, epochs[i], unique_id, timestamp);
        unique_ids.push_back(unique_id);
        timestamps.push_back(timestamp);
    }
    fclose(fp);

    auto records = wh.list();
    ASSERT_EQ(records.size(), epochs.size());
    for (size_t i = 0; i < epochs.size(); ++i) {
        EXPECT_EQ(records[i].epoch, epochs[i]);
        EXPECT_EQ(records[i].unique_id, unique_ids[i]);
        EXPECT_EQ(records[i].timestamp, timestamps[i]);
    }
}

TEST_F(wal_history_test, check_and_recover_tmp_only) {
    wal_history wh(test_dir);
    wh.append(1);
    boost::filesystem::path main_path = test_dir / "wal_history";
    boost::filesystem::path tmp_path = test_dir / "wal_history.tmp";
    boost::filesystem::rename(main_path, tmp_path);
    ASSERT_FALSE(boost::filesystem::exists(main_path));
    ASSERT_TRUE(boost::filesystem::exists(tmp_path));
    wh.check_and_recover();
    ASSERT_TRUE(boost::filesystem::exists(main_path));
    ASSERT_FALSE(boost::filesystem::exists(tmp_path));
}

TEST_F(wal_history_test, check_and_recover_both_exist) {
    wal_history wh(test_dir);
    wh.append(1);
    boost::filesystem::path main_path = test_dir / "wal_history";
    boost::filesystem::path tmp_path = test_dir / "wal_history.tmp";
    boost::filesystem::copy_file(main_path, tmp_path);
    ASSERT_TRUE(boost::filesystem::exists(main_path));
    ASSERT_TRUE(boost::filesystem::exists(tmp_path));
    wh.check_and_recover();
    ASSERT_TRUE(boost::filesystem::exists(main_path));
    ASSERT_FALSE(boost::filesystem::exists(tmp_path));
}



TEST_F(wal_history_test, check_and_recover_remove_tmp_fail) {
    wal_history_testable wh(test_dir);
    wh.append(1);
    boost::filesystem::path main_path = test_dir / "wal_history";
    boost::filesystem::path tmp_path = test_dir / "wal_history.tmp";
    boost::filesystem::copy_file(main_path, tmp_path);
    auto failing_ops = std::make_unique<failing_file_ops>();
    failing_ops->remove_failed = true;
    failing_ops->unlink(tmp_path.string().c_str());
    wh.set_file_operations(std::move(failing_ops));
    // Exception should be thrown when remove fails

    try {
        wh.check_and_recover();
        FAIL() << "Exception was not thrown";
    } catch (const limestone_exception& ex) {
        EXPECT_TRUE(std::string(ex.what()).find("Failed to remove wal_history.tmp during recovery:") != std::string::npos);
    }
}

TEST_F(wal_history_test, check_and_recover_rename_tmp_fail) {
    wal_history_testable wh(test_dir);
    wh.append(1);
    boost::filesystem::path main_path = test_dir / "wal_history";
    boost::filesystem::path tmp_path = test_dir / "wal_history.tmp";
    boost::filesystem::rename(main_path, tmp_path);
    auto failing_ops = std::make_unique<failing_file_ops>();
    failing_ops->rename_failed = true;
    wh.set_file_operations(std::move(failing_ops));
    // Exception should be thrown when rename fails

    try {
        wh.check_and_recover();
        FAIL() << "Exception was not thrown";
    } catch (const limestone_exception& ex) {
        EXPECT_TRUE(std::string(ex.what()).find("Failed to recover wal_history from wal_history.tmp:") != std::string::npos);
    }
}

TEST_F(wal_history_test, read_all_records_throws_on_open_failure) {
    boost::filesystem::path not_exist_path = test_dir / "not_exist_file";
    class FailingOpenFileOps : public limestone::internal::real_file_operations {
    public:
        std::unique_ptr<std::ifstream> open_ifstream(const std::string& filename) override {
            errno = EACCES;
            return std::make_unique<std::ifstream>(); // is_open() == false
        }
    };
    wal_history_testable wh(test_dir);
    auto failing_ops = std::make_unique<FailingOpenFileOps>();
    wh.set_file_operations(std::move(failing_ops));
    boost::filesystem::path file_path = test_dir / "wal_history";
    std::ofstream ofs(file_path.string());
    ofs << "dummy";
    ofs.close();
    try {
        auto _ = wh.list();
        FAIL() << "Exception was not thrown";
    } catch (const limestone_exception& ex) {
        EXPECT_TRUE(std::string(ex.what()).find("Failed to open wal_history for read:") != std::string::npos);
    }
}

TEST_F(wal_history_test, read_all_records_throws_on_exists_error) {
    class FailingExistsFileOps : public limestone::internal::real_file_operations {
    public:
        bool exists(const boost::filesystem::path&, boost::system::error_code& ec) override {
            ec = boost::system::errc::make_error_code(boost::system::errc::io_error);
            return false;
        }
    };
    wal_history_testable wh(test_dir);
    auto failing_ops = std::make_unique<FailingExistsFileOps>();
    wh.set_file_operations(std::move(failing_ops));
    try {
        auto _ = wh.list();
        FAIL() << "Exception was not thrown";
    } catch (const limestone_exception& ex) {
        EXPECT_TRUE(std::string(ex.what()).find("Failed to check existence of wal_history:") != std::string::npos);
    }
}

TEST_F(wal_history_test, read_all_records_throws_on_partial_record_read) {
    wal_history_testable wh(test_dir);

    boost::filesystem::path file_path = test_dir / "wal_history";
    std::ofstream ofs(file_path.string(), std::ios::binary);
    ofs.put('x');
    ofs.close();
    try {
        auto _ = wh.list();
        FAIL() << "Exception was not thrown";
    } catch (const limestone_exception& ex) {
        EXPECT_TRUE(std::string(ex.what()).find("Failed to read wal_history file: partial record read:") != std::string::npos);
    }
}

TEST_F(wal_history_test, read_all_records_throws_on_stream_error) {
    wal_history_testable wh(test_dir);
    wh.append(1); 
    class FailingFileOps : public limestone::internal::real_file_operations {
    public:
        bool has_error(std::ifstream&) override { return true; }
        bool is_eof(std::ifstream&) override { return false; }
    };
    auto failing_ops = std::make_unique<FailingFileOps>();
    wh.set_file_operations(std::move(failing_ops));
    try {
        auto _ = wh.list();
        FAIL() << "Exception was not thrown";
    } catch (const limestone_exception& ex) {
        EXPECT_TRUE(std::string(ex.what()).find("Failed to read wal_history file: stream error:") != std::string::npos);
    }
}

TEST_F(wal_history_test, read_all_records_returns_empty_when_file_not_exist) {
    wal_history_testable wh(test_dir);
    boost::filesystem::path file_path = test_dir / "wal_history";
    if (boost::filesystem::exists(file_path)) {
        boost::filesystem::remove(file_path);
    }
    auto records = wh.list();
    EXPECT_TRUE(records.empty());
}

TEST_F(wal_history_test, read_all_records_returns_empty_when_file_is_empty) {
    wal_history_testable wh(test_dir);
    boost::filesystem::path file_path = test_dir / "wal_history";
    std::ofstream ofs(file_path.string(), std::ios::binary);
    ofs.close();
    auto records = wh.list();
    EXPECT_TRUE(records.empty());
}

TEST_F(wal_history_test, read_all_records_returns_empty_when_exists_false_and_ec_zero) {
    class ExistsFalseFileOps : public limestone::internal::real_file_operations {
    public:
        bool exists(const boost::filesystem::path&, boost::system::error_code& ec) override {
            ec.clear(); // ec = 0
            return false;
        }
    };
    wal_history_testable wh(test_dir);
    auto custom_ops = std::make_unique<ExistsFalseFileOps>();
    wh.set_file_operations(std::move(custom_ops));
    auto records = wh.list();
    EXPECT_TRUE(records.empty());
}

TEST_F(wal_history_test, write_record_throws_on_write_failure) {
    wal_history_testable wh(test_dir);
    class failing_write_file_ops : public limestone::internal::real_file_operations {
    public:
        size_t fwrite(const void* /*ptr*/, size_t /*size*/, size_t /*count*/, FILE* /*stream*/) override {
            errno = EIO;
            return 0; // Simulate write failure
        }
    };
    auto failing_ops = std::make_unique<failing_write_file_ops>();
    wh.set_file_operations(std::move(failing_ops));
    boost::filesystem::path file_path = test_dir / "wal_history";
    FILE* fp = fopen(file_path.string().c_str(), "wb");
    ASSERT_TRUE(fp != nullptr);
    boost::uuids::uuid uuid{};
    std::array<std::uint8_t, 16> unique_id;
    std::memcpy(unique_id.data(), uuid.data, 16);
    try {
        wh.write_record(fp, 1, unique_id, 123);
        FAIL() << "Exception was not thrown";
    } catch (const limestone_exception& ex) {
        EXPECT_TRUE(std::string(ex.what()).find("Failed to write wal_history record") != std::string::npos);
    }
    fclose(fp);
}

TEST_F(wal_history_test, write_record_partial_fwrite_success) {
    wal_history_testable wh(test_dir);
    class partial_write_ops_t : public limestone::internal::real_file_operations {
    public:
        size_t call_count = 0;
        size_t fwrite(const void* ptr, size_t size, size_t count, FILE* stream) override {
            ++call_count;
            if (call_count == 1) {
                // 1回目は半分だけ書く
                size_t half = count / 2 > 0 ? count / 2 : 1;
                return ::fwrite(ptr, size, half, stream);
            } else {
                // 2回目は残りすべて書く
                return ::fwrite(ptr, size, count, stream);
            }
        }
    };
    auto partial_ops = std::make_unique<partial_write_ops_t>();
    wh.set_file_operations(std::move(partial_ops));
    boost::filesystem::path file_path = test_dir / "wal_history";
    FILE* fp = fopen(file_path.string().c_str(), "wb");
    ASSERT_TRUE(fp != nullptr);
    boost::uuids::uuid uuid{};
    std::array<std::uint8_t, 16> unique_id;
    std::memcpy(unique_id.data(), uuid.data, 16);
    wh.write_record(fp, 1, unique_id, 123);
    fclose(fp);
    // ファイル内容が正しいか検証
    fp = fopen(file_path.string().c_str(), "rb");
    ASSERT_TRUE(fp != nullptr);
    std::array<std::byte, wal_history_testable::record_size> buf{};
    size_t n = fread(buf.data(), 1, buf.size(), fp);
    fclose(fp);
    ASSERT_EQ(n, buf.size());
    // パースして値を検証
    auto rec = wh.parse_record(buf);
    EXPECT_EQ(rec.epoch, 1u);
    EXPECT_EQ(rec.unique_id, unique_id);
    EXPECT_EQ(rec.timestamp, 123);
}


TEST_F(wal_history_test, exists_and_get_file_path) {
    wal_history wh(test_dir);
    // The file should not exist initially
    EXPECT_FALSE(wh.exists());
    // Create the file with append
    wh.append(1);
    EXPECT_TRUE(wh.exists());
    // Check if get_file_path returns the correct path
    boost::filesystem::path expected = test_dir / "wal_history";
    EXPECT_EQ(wh.get_file_path(), expected);
    // Remove the file and check again
    boost::filesystem::remove(expected);
    EXPECT_FALSE(wh.exists());
}

TEST_F(wal_history_test, append_throws_on_fopen_failure) {
    wal_history_testable wh(test_dir);
    class failing_ops_t : public limestone::internal::real_file_operations {
    public:
        FILE* fopen(const char*, const char*) override {
            errno = EACCES;
            return nullptr;
        }
    };
    auto failing_ops = std::make_unique<failing_ops_t>();
    wh.set_file_operations(std::move(failing_ops));
    try {
        wh.append(1);
        FAIL() << "Exception was not thrown";
    } catch (const limestone_exception& ex) {
        EXPECT_TRUE(std::string(ex.what()).find("Failed to open wal_history.tmp for write:") != std::string::npos);
    }
}

TEST_F(wal_history_test, append_throws_on_fwrite_failure) {
    wal_history_testable wh(test_dir);
    class failing_ops_t : public limestone::internal::real_file_operations {
    public:
        size_t fwrite(const void*, size_t, size_t, FILE*) override {
            errno = EIO;
            return 0;
        }
    };
    auto failing_ops = std::make_unique<failing_ops_t>();
    wh.set_file_operations(std::move(failing_ops));
    try {
        wh.append(1);
        FAIL() << "Exception was not thrown";
    } catch (const limestone_exception& ex) {
        EXPECT_TRUE(std::string(ex.what()).find("Failed to write wal_history record") != std::string::npos);
    }
}

TEST_F(wal_history_test, append_throws_on_fflush_failure) {
    wal_history_testable wh(test_dir);
    class failing_ops_t : public limestone::internal::real_file_operations {
    public:
        int fflush(FILE*) override {
            errno = EIO;
            return -1;
        }
    };
    auto failing_ops = std::make_unique<failing_ops_t>();
    wh.set_file_operations(std::move(failing_ops));
    try {
        wh.append(1);
        FAIL() << "Exception was not thrown";
    } catch (const limestone_exception& ex) {
        EXPECT_TRUE(std::string(ex.what()).find("Failed to flush wal_history.tmp:") != std::string::npos);
    }
}

TEST_F(wal_history_test, append_throws_on_fileno_failure) {
    wal_history_testable wh(test_dir);
    class failing_ops_t : public limestone::internal::real_file_operations {
    public:
        int fileno(FILE*) override {
            errno = EBADF;
            return -1;
        }
    };
    auto failing_ops = std::make_unique<failing_ops_t>();
    wh.set_file_operations(std::move(failing_ops));
    try {
        wh.append(1);
        FAIL() << "Exception was not thrown";
    } catch (const limestone_exception& ex) {
        EXPECT_TRUE(std::string(ex.what()).find("Failed to get file descriptor for wal_history.tmp:") != std::string::npos);
    }
}

TEST_F(wal_history_test, append_throws_on_fsync_failure) {
    wal_history_testable wh(test_dir);
    class failing_ops_t : public limestone::internal::real_file_operations {
    public:
        int fsync(int) override {
            errno = EIO;
            return -1;
        }
    };
    auto failing_ops = std::make_unique<failing_ops_t>();
    wh.set_file_operations(std::move(failing_ops));
    try {
        wh.append(1);
        FAIL() << "Exception was not thrown";
    } catch (const limestone_exception& ex) {
        EXPECT_TRUE(std::string(ex.what()).find("Failed to fsync wal_history.tmp:") != std::string::npos);
    }
}

TEST_F(wal_history_test, append_throws_on_fclose_failure) {
    wal_history_testable wh(test_dir);
    class failing_ops_t : public limestone::internal::real_file_operations {
    public:
        int fclose(FILE*) override {
            errno = EIO;
            return -1;
        }
    };
    auto failing_ops = std::make_unique<failing_ops_t>();
    wh.set_file_operations(std::move(failing_ops));
    try {
        wh.append(1);
        FAIL() << "Exception was not thrown";
    } catch (const limestone_exception& ex) {
        EXPECT_TRUE(std::string(ex.what()).find("Failed to close wal_history.tmp:") != std::string::npos);
    }
}

TEST_F(wal_history_test, append_throws_on_rename_failure) {
    wal_history_testable wh(test_dir);
    class failing_ops_t : public limestone::internal::real_file_operations {
    public:
        void rename(const boost::filesystem::path&, const boost::filesystem::path&, boost::system::error_code& ec) override {
            ec = boost::system::errc::make_error_code(boost::system::errc::io_error);
        }
    };
    auto failing_ops = std::make_unique<failing_ops_t>();
    wh.set_file_operations(std::move(failing_ops));
    try {
        wh.append(1);
        FAIL() << "Exception was not thrown";
    } catch (const limestone_exception& ex) {
        EXPECT_TRUE(std::string(ex.what()).find("Failed to rename wal_history.tmp to wal_history:") != std::string::npos);
    }
}

TEST_F(wal_history_test, file_name_returns_expected_value) {
    EXPECT_STREQ(wal_history::file_name(), "wal_history");
}

} // namespace limestone::testing

