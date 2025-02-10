/*
 * Copyright 2022-2024 Project Tsurugi.
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

#include <gtest/gtest.h>
#include <boost/filesystem.hpp>
#include "compaction_catalog.h"
#include "limestone/api/limestone_exception.h"
#include "limestone/api/blob_file.h"

namespace limestone::testing {

using limestone::api::epoch_id_type;
using limestone::api::blob_id_type;
using limestone::api::limestone_io_exception;
using limestone::api::limestone_exception;
using limestone::internal::file_operations;
using limestone::internal::compacted_file_info;
using limestone::internal::compaction_catalog;



constexpr const char* COMPACTION_CATALOG_CONTENT = R"(COMPACTION_CATALOG_HEADER
COMPACTED_FILE file1 1
COMPACTED_FILE file2 2
DETACHED_PWAL pwal1
DETACHED_PWAL pwal2
MAX_EPOCH_ID 123
COMPACTION_CATALOG_FOOTER
)";


constexpr const char* COMPACTION_CATALOG_CONTENT_WITH_EMPTY_LINES = R"(COMPACTION_CATALOG_HEADER
COMPACTED_FILE file1 1
COMPACTED_FILE file2 2
DETACHED_PWAL pwal1

DETACHED_PWAL pwal2
MAX_EPOCH_ID 123

COMPACTION_CATALOG_FOOTER

)";

constexpr const char* COMPACTION_CATALOG_MISSING_FOOTER = R"(COMPACTION_CATALOG_HEADER
COMPACTED_FILE file1 1
COMPACTED_FILE file2 2
DETACHED_PWAL pwal1
DETACHED_PWAL pwal2
MAX_EPOCH_ID 123
)";


constexpr const char* COMPACTION_CATALOG_MISSING_MAX_EPOCH_ID = R"(COMPACTION_CATALOG_HEADER
COMPACTED_FILE file1 1
COMPACTED_FILE file2 2
DETACHED_PWAL pwal1
DETACHED_PWAL pwal2
COMPACTION_CATALOG_FOOTER
)";

constexpr const char* COMPACTION_CATALOG_INVALID_HEADER = R"(Wrong Header
COMPACTED_FILE file1 1
COMPACTED_FILE file2 2
DETACHED_PWAL pwal1
DETACHED_PWAL pwal2
MAX_EPOCH_ID 123
COMPACTION_CATALOG_FOOTER
)";



static const boost::filesystem::path test_dir = "/tmp/comapction_catalog";
static const boost::filesystem::path catalog_file_path = test_dir / "compaction_catalog";
static const boost::filesystem::path backup_file_path = test_dir / "compaction_catalog.back";

class compaction_catalog_test : public ::testing::Test {
protected:
    void SetUp() override {
        boost::filesystem::create_directory(test_dir);
    }

    void TearDown() override {
        boost::filesystem::remove_all(test_dir);
    }
};


class test_file_writer {
public:
    explicit test_file_writer(const std::string& file_path) : file_path_(file_path) {}

    template <typename T>
    test_file_writer& operator<<(const T& data) {
        std::ofstream file(file_path_, std::ios::app); 
        if (!file.is_open()) {
            throw std::runtime_error("Failed to open file: " + file_path_);
        }
        file << data;
        file.close();
        return *this;
    }

    void clear() {
        std::ofstream file(file_path_, std::ios::trunc);
        if (!file.is_open()) {
            throw std::runtime_error("Failed to open file: " + file_path_);
        }
        file.close();
    }

private:
    std::string file_path_;
};



// Test helper class for accessing protected methods of compaction_catalog
class testable_compaction_catalog : public compaction_catalog {
public:
    // Constructor forwarding the directory_path to the base class constructor
    explicit testable_compaction_catalog(const boost::filesystem::path& directory_path)
        : compaction_catalog(directory_path) {}

    using compaction_catalog::create_catalog_content;    
    using compaction_catalog::load;
    using compaction_catalog::load_catalog_file;
    using compaction_catalog::restore_from_backup;
    using compaction_catalog::parse_catalog_entry;
    using compaction_catalog::set_file_operations;
    using compaction_catalog::reset_file_operations;
};

TEST_F(compaction_catalog_test, create_catalog) {
    compaction_catalog catalog(test_dir);

    EXPECT_EQ(catalog.get_max_epoch_id(), 0);
    EXPECT_TRUE(catalog.get_compacted_files().empty());
    EXPECT_TRUE(catalog.get_detached_pwals().empty());
}



TEST_F(compaction_catalog_test, update_catalog) {
    testable_compaction_catalog catalog(test_dir);

    epoch_id_type max_epoch_id = 123;
    blob_id_type max_blob_id = 456;
    std::set<compacted_file_info> compacted_files = {
        {"file1", 1},
        {"file2", 2}
    };
    std::set<std::string> detached_pwals = {"pwal1", "pwal2"};

    catalog.update_catalog_file(max_epoch_id, max_blob_id, compacted_files, detached_pwals);
    EXPECT_EQ(catalog.get_max_epoch_id(), max_epoch_id);
    EXPECT_EQ(catalog.get_max_blob_id(), max_blob_id);
    EXPECT_EQ(catalog.get_compacted_files(), compacted_files);
    EXPECT_EQ(catalog.get_detached_pwals(), detached_pwals);

    // update existing catalog

    max_epoch_id = 456;
    max_blob_id = 789;
    compacted_files = {
        {"file3", 3},
        {"file4", 4}
    };
    detached_pwals = {};
    catalog.update_catalog_file(max_epoch_id, max_blob_id, compacted_files, detached_pwals);
    EXPECT_EQ(catalog.get_max_epoch_id(), max_epoch_id);
    EXPECT_EQ(catalog.get_max_blob_id(), max_blob_id);
    EXPECT_EQ(catalog.get_compacted_files(), compacted_files);
    EXPECT_EQ(catalog.get_detached_pwals(), detached_pwals);

    // file_ops_->exists failure
    class mock_exists : public limestone::internal::real_file_operations {
    public:
        bool exists(const boost::filesystem::path& p, boost::system::error_code& ec) override {
            ec = make_error_code(boost::system::errc::permission_denied);
            return false;
        }
    };
    catalog.set_file_operations(std::make_unique<mock_exists>());
    EXPECT_THROW(
        {
            try {
                catalog.update_catalog_file(max_epoch_id, max_blob_id, compacted_files, detached_pwals);
            } catch (const limestone_io_exception& e) {
                EXPECT_EQ(e.error_code(), EACCES);  
                throw;
            }
        },
        limestone_io_exception);

    // file_ops_->rename failure
    class mock_rename : public limestone::internal::real_file_operations {
    public:
        int rename(const char* oldname, const char* newname) override {
            errno = EACCES;
            return -1;
        }
    };
    catalog.set_file_operations(std::make_unique<mock_rename>());
    EXPECT_THROW(
        {
            try {
                catalog.update_catalog_file(max_epoch_id, max_blob_id, compacted_files, detached_pwals);
            } catch (const limestone_io_exception& e) {
                EXPECT_EQ(e.error_code(), EACCES);  
                throw;
            }
        },
        limestone_io_exception);

    // file_ops_->fclose failure
    class mock_fclose : public limestone::internal::real_file_operations {
    public:
        int fclose(FILE* stream) override {
            errno = EBADF;
            return -1;
        }
    };
    catalog.set_file_operations(std::make_unique<mock_fclose>());
    catalog.update_catalog_file(max_epoch_id, max_blob_id, compacted_files, detached_pwals);  // no exception expected

    // file_ops_->fopen failure
    class mock_fopen : public limestone::internal::real_file_operations {
    public:
        FILE* fopen(const char* filename, const char* mode) override {
            errno = ENOSPC;
            return nullptr;
        }
    };
    catalog.set_file_operations(std::make_unique<mock_fopen>());
    EXPECT_THROW(
        {
            try {
                catalog.update_catalog_file(max_epoch_id, max_blob_id, compacted_files, detached_pwals);
            } catch (const limestone_io_exception& e) {
                EXPECT_EQ(e.error_code(), ENOSPC);  
                throw;
            }
        },
        limestone_io_exception);

    // file_ops_->fwrite failure
    class mock_fwrite : public limestone::internal::real_file_operations {  
    public:
        size_t fwrite(const void* ptr, size_t size, size_t count, FILE* stream) override {
            errno = ENOSPC;
            return 0;
        }
        int ferror(FILE* stream) override {
            return ENOSPC;
        }

    };
    catalog.set_file_operations(std::make_unique<mock_fwrite>());
    EXPECT_THROW(
        {
            try {
                catalog.update_catalog_file(max_epoch_id, max_blob_id, compacted_files, detached_pwals);
            } catch (const limestone_io_exception& e) {
                EXPECT_EQ(e.error_code(), ENOSPC);  
                throw;
            }
        },
        limestone_io_exception);

    // file_ops_->fwrite returns 0 without error
    class mock_fwrite_no_error : public limestone::internal::real_file_operations {
    public:
        size_t fwrite(const void* ptr, size_t size, size_t count, FILE* stream) override {
            return 0;
        }
        int ferror(FILE* stream) override {
            return 0;
        }
    };
    catalog.set_file_operations(std::make_unique<mock_fwrite_no_error>());
    EXPECT_THROW(
        {
            try {
                catalog.update_catalog_file(max_epoch_id, max_blob_id, compacted_files, detached_pwals);
            } catch (const limestone_exception& e) {
                EXPECT_EQ(e.error_code(), 0);  
                throw;
            }
        },
        limestone_exception);

    // file_ops_->fflush failure
    class mock_fflush : public limestone::internal::real_file_operations {
    public:
        int fflush(FILE* stream) override {
            errno = ENOSPC;
            return -1;
        }
    };
    catalog.set_file_operations(std::make_unique<mock_fflush>());
    EXPECT_THROW(
        {
            try {
                catalog.update_catalog_file(max_epoch_id, max_blob_id, compacted_files, detached_pwals);
            } catch (const limestone_io_exception& e) {
                EXPECT_EQ(e.error_code(), ENOSPC);  
                throw;
            }
        },
        limestone_io_exception);

    // file_ops_->fileno failure
    class mock_fileno : public limestone::internal::real_file_operations {
    public:
        int fileno(FILE* stream) override {
            errno = EBADF;
            return -1;
        }
    };
    catalog.set_file_operations(std::make_unique<mock_fileno>());
    EXPECT_THROW(
        {
            try {
                catalog.update_catalog_file(max_epoch_id, max_blob_id, compacted_files, detached_pwals);
            } catch (const limestone_io_exception& e) {
                EXPECT_EQ(e.error_code(), EBADF);  
                throw;
            }
        },
        limestone_io_exception);        

    // file_ops_->fsync failure
    class mock_fsync : public limestone::internal::real_file_operations {
    public:
        int fsync(int fd) override {
            errno = ENOSPC;
            return -1;
        }
    };
    catalog.set_file_operations(std::make_unique<mock_fsync>());
    EXPECT_THROW(
        {
            try {
                catalog.update_catalog_file(max_epoch_id, max_blob_id, compacted_files, detached_pwals);
            } catch (const limestone_io_exception& e) {
                EXPECT_EQ(e.error_code(), ENOSPC);  
                throw;
            }
        },
        limestone_io_exception);
}

TEST_F(compaction_catalog_test, update_and_load_catalog_file) {
    testable_compaction_catalog catalog(test_dir);

    epoch_id_type max_epoch_id = 123;
    blob_id_type max_blob_id = 456;
    std::set<compacted_file_info> compacted_files = {
        {"file1", 1},
        {"file2", 2}
    };
    std::set<std::string> detached_pwals = {"pwal1", "pwal2"};

    catalog.update_catalog_file(max_epoch_id, max_blob_id, compacted_files, detached_pwals);

    compaction_catalog loaded_catalog = compaction_catalog::from_catalog_file(test_dir);

    EXPECT_EQ(loaded_catalog.get_max_epoch_id(), max_epoch_id);
    EXPECT_EQ(loaded_catalog.get_max_blob_id(), max_blob_id);
    EXPECT_EQ(loaded_catalog.get_compacted_files(), compacted_files);
    EXPECT_EQ(loaded_catalog.get_detached_pwals(), detached_pwals);

    // file_ops_->fwrite writes only 1 byte
    class mock_fwrite_one_byte : public limestone::internal::real_file_operations {
    public:
        size_t fwrite(const void* ptr, size_t size, size_t count, FILE* stream) override {
            if (size * count > 0) {
                size_t written = ::fwrite(ptr, 1, 1, stream);
                return written;
            }
            return 0;
        }
    };

    max_epoch_id = 456;
    max_blob_id = 789;
    compacted_files = {
        {"file3", 3},
        {"file4", 4}
    };
    detached_pwals = {};
    catalog.update_catalog_file(max_epoch_id, max_blob_id, compacted_files, detached_pwals);
    catalog.set_file_operations(std::make_unique<mock_fwrite_one_byte>());
    catalog.update_catalog_file(max_epoch_id, max_blob_id, compacted_files, detached_pwals);

    loaded_catalog = compaction_catalog::from_catalog_file(test_dir);
    EXPECT_EQ(loaded_catalog.get_max_epoch_id(), max_epoch_id);
    EXPECT_EQ(loaded_catalog.get_max_blob_id(), max_blob_id);
    EXPECT_EQ(loaded_catalog.get_compacted_files(), compacted_files);
    EXPECT_EQ(loaded_catalog.get_detached_pwals(), detached_pwals);
}

TEST_F(compaction_catalog_test, load_catalog_file) {
    test_file_writer writer(catalog_file_path.string());
    testable_compaction_catalog catalog(test_dir);

    // normal case
    writer.clear();
    writer << COMPACTION_CATALOG_CONTENT;
    epoch_id_type max_epoch_id = 123;
    std::set<compacted_file_info> compacted_files = {
        {"file1", 1},
        {"file2", 2}
    };
    std::set<std::string> detached_pwals = {"pwal1", "pwal2"};

    catalog.load_catalog_file(catalog_file_path);
    EXPECT_EQ(catalog.get_max_epoch_id(), max_epoch_id);
    EXPECT_EQ(catalog.get_compacted_files(), compacted_files);
    EXPECT_EQ(catalog.get_detached_pwals(), detached_pwals);

    // testing skip empty line
    writer.clear();
    writer << COMPACTION_CATALOG_CONTENT_WITH_EMPTY_LINES;
    catalog.load_catalog_file(catalog_file_path);
    EXPECT_EQ(catalog.get_max_epoch_id(), max_epoch_id);
    EXPECT_EQ(catalog.get_compacted_files(), compacted_files);
    EXPECT_EQ(catalog.get_detached_pwals(), detached_pwals);

    // unexpected end of file while reading header line
    writer.clear();
    EXPECT_THROW(
        {
            try {
                catalog.load_catalog_file(catalog_file_path);
            } catch (const limestone_exception& e) {
                EXPECT_EQ(e.error_code(), 0);
                EXPECT_TRUE(std::string(e.what()).find("Unexpected end of file while reading header line") != std::string::npos)
                    << "Actual message: " << e.what();
                throw;
            }
        },
        limestone_exception);

    // invalid header line
    writer.clear();
    writer << "Wrong Header";
    EXPECT_THROW(
        {
            try {
                catalog.load_catalog_file(catalog_file_path);
            } catch (const limestone_exception& e) {
                EXPECT_EQ(e.error_code(), 0);
                EXPECT_TRUE(std::string(e.what()).find("Invalid header line:") != std::string::npos)
                    << "Actual message: " << e.what();
                throw;
            }
        },
        limestone_exception);

    writer.clear();
    writer << COMPACTION_CATALOG_MISSING_FOOTER;
    EXPECT_THROW(
        {
            try {
                catalog.load_catalog_file(catalog_file_path);
            } catch (const limestone_exception& e) {
                EXPECT_EQ(e.error_code(), 0);
                EXPECT_TRUE(std::string(e.what()).find("Missing footer line") != std::string::npos) << "Actual message: " << e.what();
                throw;
            }
        },
        limestone_exception);

    writer.clear();
    writer << COMPACTION_CATALOG_MISSING_MAX_EPOCH_ID;
    EXPECT_THROW(
        {
            try {
                catalog.load_catalog_file(catalog_file_path);
            } catch (const limestone_exception& e) {
                EXPECT_EQ(e.error_code(), 0);
                EXPECT_TRUE(std::string(e.what()).find("MAX_EPOCH_ID entry not found") != std::string::npos) << "Actual message: " << e.what();
                throw;
            }
        },
        limestone_exception);

    // error in reading header line
    class mock_header_read_error : public limestone::internal::real_file_operations {
    public:
        bool getline(std::ifstream& file, std::string& line) {
            int ret = static_cast<bool>(std::getline(file, line));
            if (ret && line == "COMPACTION_CATALOG_HEADER") {
                errno = ENOSPC;
                has_eror_ = true;
                return false;
            }
            return ret;
        }
        bool has_error(std::ifstream& file) {
            return has_eror_;
        }
    private:
        bool has_eror_ = false;        
    };
    catalog.set_file_operations(std::make_unique<mock_header_read_error>());
    EXPECT_THROW(
        {
            try {
                catalog.load_catalog_file(catalog_file_path);
            } catch (const limestone_io_exception& e) {
                EXPECT_EQ(e.error_code(), ENOSPC);
                EXPECT_TRUE(std::string(e.what()).find("Failed to read line from file") != std::string::npos) << "Actual message: " << e.what();
                throw;
            }
        },
        limestone_io_exception);

    // error in reading footer line
    class mock_footer_read_error : public limestone::internal::real_file_operations {
    public:
        bool getline(std::ifstream& file, std::string& line) {
            int ret = static_cast<bool>(std::getline(file, line));
            if (ret && line == "COMPACTION_CATALOG_FOOTER") {
                errno = ENOSPC;
                has_eror_ = true;
                return false;
            }
            return ret;
        }
  bool has_error(std::ifstream& file) {
            return has_eror_;
        }
    private:
        bool has_eror_ = false;        
    };
    catalog.set_file_operations(std::make_unique<mock_footer_read_error>());
    EXPECT_THROW(
        {
            try {
                catalog.load_catalog_file(catalog_file_path);
            } catch (const limestone_io_exception& e) {
                EXPECT_EQ(e.error_code(), ENOSPC);
                EXPECT_TRUE(std::string(e.what()).find("Failed to read line from file") != std::string::npos) << "Actual message: " << e.what();
                throw;
            }
        },
        limestone_io_exception);
    
    // error in is_open
    class mock_is_open_error : public limestone::internal::real_file_operations {
    public:
        std::unique_ptr<std::ifstream> open_ifstream(const std::string& path) override {
            errno = EIO;
            return nullptr;
        }
        bool is_open(std::ifstream& file) override {
            errno = 0;
            return false;
        }
    };
    writer.clear();
    catalog.set_file_operations(std::make_unique<mock_is_open_error>());
    EXPECT_THROW(
        {
            try {
                catalog.load_catalog_file(catalog_file_path);
            } catch (const limestone_exception& e) {
                EXPECT_EQ(e.error_code(), EIO);
                EXPECT_TRUE(std::string(e.what()).find("Failed to open compaction catalog file") != std::string::npos) << "Actual message: " << e.what();
                throw;
            }
        },
        limestone_exception);



    catalog.reset_file_operations();
}

TEST_F(compaction_catalog_test, load_from_backup) {
    {
        compaction_catalog catalog(test_dir);

        epoch_id_type max_epoch_id = 123;
        blob_id_type blob_id = 456;
        std::set<compacted_file_info> compacted_files = {
            {"file1", 1},
            {"file2", 2}
        };
        std::set<std::string> detached_pwals = {"pwal1", "pwal2"};

        catalog.update_catalog_file(max_epoch_id, blob_id, compacted_files, detached_pwals);

    }

    boost::filesystem::rename(catalog_file_path, backup_file_path);

    boost::filesystem::remove(catalog_file_path);

    compaction_catalog loaded_catalog = compaction_catalog::from_catalog_file(test_dir);

    EXPECT_EQ(loaded_catalog.get_max_epoch_id(), 123);
    EXPECT_EQ(loaded_catalog.get_max_blob_id(), 456);
    EXPECT_EQ(loaded_catalog.get_compacted_files().size(), 2);
    EXPECT_EQ(loaded_catalog.get_detached_pwals().size(), 2);
}

TEST_F(compaction_catalog_test, parse_catalog_entry) {

    // Test valid COMPACTED_FILE entry
    {
        std::string compacted_file_entry = "COMPACTED_FILE file1 1";
        bool max_epoch_id_found = false;
        testable_compaction_catalog catalog(test_dir);
        catalog.parse_catalog_entry(compacted_file_entry, max_epoch_id_found);
        EXPECT_EQ(catalog.get_compacted_files().size(), 1);
        EXPECT_EQ(catalog.get_compacted_files().begin()->get_file_name(), "file1");
        EXPECT_EQ(catalog.get_compacted_files().begin()->get_version(), 1);
        EXPECT_FALSE(max_epoch_id_found);
    }

    // Test valid DETACHED_PWAL entry
    {
        std::string detached_pwal_entry = "DETACHED_PWAL pwal1";
        bool max_epoch_id_found = false;
        testable_compaction_catalog catalog(test_dir);
        catalog.parse_catalog_entry(detached_pwal_entry, max_epoch_id_found);
        EXPECT_EQ(catalog.get_detached_pwals().size(), 1);
        EXPECT_EQ(*catalog.get_detached_pwals().begin(), "pwal1");
        EXPECT_FALSE(max_epoch_id_found);
    }

    // Test valid MAX_EPOCH_ID entry
    {
        std::string max_epoch_id_entry = "MAX_EPOCH_ID 123";
        bool max_epoch_id_found = false;
        testable_compaction_catalog catalog(test_dir);
        catalog.parse_catalog_entry(max_epoch_id_entry, max_epoch_id_found);
        EXPECT_EQ(catalog.get_max_epoch_id(), 123);
        EXPECT_TRUE(max_epoch_id_found);
    }

    // Test empty line
    {
        std::string empty_line = "";
        bool max_epoch_id_found = false;
        testable_compaction_catalog catalog(test_dir);
        catalog.parse_catalog_entry(empty_line, max_epoch_id_found);
        EXPECT_EQ(catalog.get_compacted_files().size(), 0);
        EXPECT_EQ(catalog.get_detached_pwals().size(), 0);
        EXPECT_EQ(catalog.get_max_epoch_id(), 0);
        EXPECT_FALSE(max_epoch_id_found);
    }

    // Test line with only whitespace
    {
        std::string whitespace_line = "   ";
        bool max_epoch_id_found = false;
        testable_compaction_catalog catalog(test_dir);
        catalog.parse_catalog_entry(whitespace_line, max_epoch_id_found);
        EXPECT_EQ(catalog.get_compacted_files().size(), 0);
        EXPECT_EQ(catalog.get_detached_pwals().size(), 0);
        EXPECT_EQ(catalog.get_max_epoch_id(), 0);
        EXPECT_FALSE(max_epoch_id_found);
    }

    // Test invalid COMPACTED_FILE entry
    std::string invalid_compacted_file_entry = "COMPACTED_FILE file1";
    EXPECT_THROW(
        {
            try {
                bool max_epoch_id_found = false;
                testable_compaction_catalog catalog(test_dir);
                catalog.parse_catalog_entry(invalid_compacted_file_entry, max_epoch_id_found);
            } catch (const limestone_exception& e) {
                EXPECT_TRUE(std::string(e.what()).find("Invalid format for COMPACTED_FILE:") != std::string::npos);
                throw;
            }
        },
        limestone_exception);

    // Test invalid DETACHED_PWAL entry
    std::string invalid_detached_pwal_entry = "DETACHED_PWAL";
    EXPECT_THROW(
        {
            try {
                bool max_epoch_id_found = false;
                testable_compaction_catalog catalog(test_dir);
                catalog.parse_catalog_entry(invalid_detached_pwal_entry, max_epoch_id_found);
            } catch (const limestone_exception& e) {
                EXPECT_TRUE(std::string(e.what()).find("Invalid format for DETACHED_PWAL:") != std::string::npos);
                throw;
            }
        },
        limestone_exception);

    // Test invalid MAX_EPOCH_ID entry
    std::string invalid_max_epoch_id_entry = "MAX_EPOCH_ID";
    EXPECT_THROW(
        {
            try {
                bool max_epoch_id_found = false;
                testable_compaction_catalog catalog(test_dir);
                catalog.parse_catalog_entry(invalid_max_epoch_id_entry, max_epoch_id_found);
            } catch (const limestone_exception& e) {
                EXPECT_TRUE(std::string(e.what()).find("Invalid format for MAX_EPOCH_ID:") != std::string::npos);
                throw;
            }
        },
        limestone_exception);

    // Test unknown entry type
    std::string unknown_entry = "UNKNOWN_ENTRY_TYPE data";
    EXPECT_THROW(
        {
            try {
                bool max_epoch_id_found = false;
                testable_compaction_catalog catalog(test_dir);
                catalog.parse_catalog_entry(unknown_entry, max_epoch_id_found);
            } catch (const limestone_exception& e) {
                EXPECT_TRUE(std::string(e.what()).find("Unknown entry type:") != std::string::npos);
                throw;
            }
        },
        limestone_exception);
}

TEST_F(compaction_catalog_test, restore_from_backup_exceptions) {
    test_file_writer writer(backup_file_path.string());
    testable_compaction_catalog catalog(test_dir);

    // Normal case
    writer.clear();
    writer << COMPACTION_CATALOG_CONTENT;
    catalog.reset_file_operations();
    catalog.restore_from_backup();
    epoch_id_type max_epoch_id = 123;
    std::set<compacted_file_info> compacted_files = {
        {"file1", 1},
        {"file2", 2}
    };
    std::set<std::string> detached_pwals = {"pwal1", "pwal2"};
    EXPECT_EQ(catalog.get_max_epoch_id(), max_epoch_id);
    EXPECT_EQ(catalog.get_compacted_files(), compacted_files);
    EXPECT_EQ(catalog.get_detached_pwals(), detached_pwals);


    // Backup file does not exist
    boost::filesystem::remove(backup_file_path);
    EXPECT_THROW(
        {
            try {
                catalog.restore_from_backup();
            } catch (const limestone_exception& e) {
                EXPECT_TRUE(std::string(e.what()).find("Failed to load compaction catalog file and no backup available.") != std::string::npos);
                throw;
            }
        },
        limestone_exception);

    // Error in rename backup file
    class mock_rename_error : public limestone::internal::real_file_operations {
    public:
        int rename(const char* oldname, const char* newname) override {
            errno = EACCES;
            return -1;
        }
    };
    writer.clear();
    writer << COMPACTION_CATALOG_CONTENT;
    catalog.set_file_operations(std::make_unique<mock_rename_error>());
    EXPECT_THROW(
        {
            try {
                catalog.restore_from_backup();
            } catch (const limestone_io_exception& e) {
                EXPECT_EQ(e.error_code(), EACCES);
                EXPECT_TRUE(std::string(e.what()).find("Failed to rename backup file") != std::string::npos);
                throw;
            }
        },
        limestone_io_exception);

    // Error in remove existing catalog file
    class mock_remove_error : public limestone::internal::real_file_operations {
    public:
        int unlink(const char* pathname) override {
            errno = EACCES;
            return -1;
        }
    };
    writer.clear();
    writer << COMPACTION_CATALOG_CONTENT;
    boost::filesystem::copy_file(backup_file_path, catalog_file_path);
    catalog.set_file_operations(std::make_unique<mock_remove_error>());
    EXPECT_THROW(
        {
            try {
                catalog.restore_from_backup();
            } catch (const limestone_io_exception& e) {
                EXPECT_EQ(e.error_code(), EACCES);
                EXPECT_TRUE(std::string(e.what()).find("Failed to remove existing catalog file") != std::string::npos);
                throw;
            }
        },
    limestone_io_exception);

    // Error in checking exists backup file
    class mock_exists_backup_error : public limestone::internal::real_file_operations {
    public:
        bool exists(const boost::filesystem::path& p, boost::system::error_code& ec) override {
            if (p == backup_file_path) {
                return real_file_operations::exists(p, ec);
            }
            ec = make_error_code(boost::system::errc::permission_denied);
            return false;
        }
    };
    writer.clear();
    writer << COMPACTION_CATALOG_CONTENT;
    catalog.set_file_operations(std::make_unique<mock_exists_backup_error>());
    EXPECT_THROW(
        {
            try {
                catalog.restore_from_backup();
            } catch (const limestone_io_exception& e) {
                EXPECT_EQ(e.error_code(), EACCES);
                EXPECT_TRUE(std::string(e.what()).find("Error checking catalog file existence") != std::string::npos);
                throw;
            }
        },
        limestone_io_exception);

    // Error in checking exists backup file        
    class mock_exists_catalog_error : public limestone::internal::real_file_operations {
    public:
        bool exists(const boost::filesystem::path& p, boost::system::error_code& ec) override {
            if (p == catalog_file_path) {
                return real_file_operations::exists(p, ec);
            }
            ec = make_error_code(boost::system::errc::permission_denied);
            return false;
        }
    };
    writer.clear();
    writer << COMPACTION_CATALOG_CONTENT;
    catalog.set_file_operations(std::make_unique<mock_exists_catalog_error>());
    EXPECT_THROW(
        {
            try {
                catalog.restore_from_backup();
            } catch (const limestone_io_exception& e) {
                EXPECT_EQ(e.error_code(), EACCES);
                EXPECT_TRUE(std::string(e.what()).find("Error checking backup file existence") != std::string::npos);
                throw;
            }
        },
        limestone_io_exception);
}



}  // namespace limestone::testing

