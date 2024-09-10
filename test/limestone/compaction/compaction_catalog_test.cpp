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

namespace limestone::testing {

using limestone::api::epoch_id_type;
using limestone::api::limestone_io_exception;
using limestone::api::limestone_exception;
using limestone::internal::compacted_file_info;
using limestone::internal::compaction_catalog;

class compaction_catalog_test : public ::testing::Test {
protected:
    void SetUp() override {
        test_dir = boost::filesystem::path("/tmp/comapction_catalog");
        boost::filesystem::create_directory(test_dir);

        catalog_file_path = test_dir / "compaction_catalog";
        backup_file_path = test_dir / "compaction_catalog.back";
    }

    void TearDown() override {
        boost::filesystem::remove_all(test_dir);
    }

    boost::filesystem::path test_dir;
    boost::filesystem::path catalog_file_path;
    boost::filesystem::path backup_file_path;
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

    // Public methods to access protected members
    void load_catalog_file_for_testing(const boost::filesystem::path& directory_path) {
        load_catalog_file(directory_path);
    }

    void parse_catalog_entry_for_testing(const std::string& line, bool& max_epoch_id_found) {
        parse_catalog_entry(line, max_epoch_id_found);
    }

    [[nodiscard]] std::string create_catalog_content_for_testing() const {
        return create_catalog_content();
    }
};

TEST_F(compaction_catalog_test, create_catalog) {
    compaction_catalog catalog(test_dir);

    EXPECT_EQ(catalog.get_max_epoch_id(), 0);
    EXPECT_TRUE(catalog.get_compacted_files().empty());
    EXPECT_TRUE(catalog.get_detached_pwals().empty());
}



TEST_F(compaction_catalog_test, update_catalog) {
    compaction_catalog catalog(test_dir);

    epoch_id_type max_epoch_id = 123;
    std::set<compacted_file_info> compacted_files = {
        {"file1", 1},
        {"file2", 2}
    };
    std::set<std::string> detached_pwals = {"pwal1", "pwal2"};

    catalog.update_catalog_file(max_epoch_id, compacted_files, detached_pwals);
    EXPECT_EQ(catalog.get_max_epoch_id(), max_epoch_id);
    EXPECT_EQ(catalog.get_compacted_files(), compacted_files);
    EXPECT_EQ(catalog.get_detached_pwals(), detached_pwals);

    // update existing catalog

    max_epoch_id = 456;
    compacted_files = {
        {"file3", 3},
        {"file4", 4}
    };
    detached_pwals = {};
    catalog.update_catalog_file(max_epoch_id, compacted_files, detached_pwals);
    EXPECT_EQ(catalog.get_max_epoch_id(), max_epoch_id);
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
                catalog.update_catalog_file(max_epoch_id, compacted_files, detached_pwals);
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
                catalog.update_catalog_file(max_epoch_id, compacted_files, detached_pwals);
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
    catalog.update_catalog_file(max_epoch_id, compacted_files, detached_pwals);  // no exception expected

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
                catalog.update_catalog_file(max_epoch_id, compacted_files, detached_pwals);
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
                catalog.update_catalog_file(max_epoch_id, compacted_files, detached_pwals);
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
                catalog.update_catalog_file(max_epoch_id, compacted_files, detached_pwals);
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
                catalog.update_catalog_file(max_epoch_id, compacted_files, detached_pwals);
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
                catalog.update_catalog_file(max_epoch_id, compacted_files, detached_pwals);
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
                catalog.update_catalog_file(max_epoch_id, compacted_files, detached_pwals);
            } catch (const limestone_io_exception& e) {
                EXPECT_EQ(e.error_code(), ENOSPC);  
                throw;
            }
        },
        limestone_io_exception);
}

TEST_F(compaction_catalog_test, update_and_load_catalog_file) {
    compaction_catalog catalog(test_dir);

    epoch_id_type max_epoch_id = 123;
    std::set<compacted_file_info> compacted_files = {
        {"file1", 1},
        {"file2", 2}
    };
    std::set<std::string> detached_pwals = {"pwal1", "pwal2"};

    catalog.update_catalog_file(max_epoch_id, compacted_files, detached_pwals);

    compaction_catalog loaded_catalog = compaction_catalog::from_catalog_file(test_dir);

    EXPECT_EQ(loaded_catalog.get_max_epoch_id(), max_epoch_id);
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
    compacted_files = {
        {"file3", 3},
        {"file4", 4}
    };
    detached_pwals = {};
    catalog.update_catalog_file(max_epoch_id, compacted_files, detached_pwals);
    catalog.set_file_operations(std::make_unique<mock_fwrite_one_byte>());
    catalog.update_catalog_file(max_epoch_id, compacted_files, detached_pwals);

    loaded_catalog = compaction_catalog::from_catalog_file(test_dir);
    EXPECT_EQ(loaded_catalog.get_max_epoch_id(), max_epoch_id);
    EXPECT_EQ(loaded_catalog.get_compacted_files(), compacted_files);
    EXPECT_EQ(loaded_catalog.get_detached_pwals(), detached_pwals);
}

TEST_F(compaction_catalog_test, load_catalog_file) {
    test_file_writer writer(catalog_file_path.string());
    testable_compaction_catalog catalog(test_dir);

    // normal case
    writer.clear();
    writer << "COMPACTION_CATALOG_HEADER\n"
           << "COMPACTED_FILE file1 1\n"
           << "COMPACTED_FILE file2 2\n"
           << "DETACHED_PWAL pwal1\n"
           << "DETACHED_PWAL pwal2\n"
           << "MAX_EPOCH_ID 123\n"
           << "COMPACTION_CATALOG_FOOTER\n";

    epoch_id_type max_epoch_id = 123;
    std::set<compacted_file_info> compacted_files = {
        {"file1", 1},
        {"file2", 2}
    };
    std::set<std::string> detached_pwals = {"pwal1", "pwal2"};

    catalog.load_catalog_file_for_testing(catalog_file_path);
    EXPECT_EQ(catalog.get_max_epoch_id(), max_epoch_id);
    EXPECT_EQ(catalog.get_compacted_files(), compacted_files);
    EXPECT_EQ(catalog.get_detached_pwals(), detached_pwals);

    // testing skip empty line
    writer.clear();
    writer << "COMPACTION_CATALOG_HEADER\n"
           << "COMPACTED_FILE file1 1\n"
           << "COMPACTED_FILE file2 2\n"
           << "DETACHED_PWAL pwal1\n\n"
           << "DETACHED_PWAL pwal2\n"
           << "MAX_EPOCH_ID 123\n\n"
           << "COMPACTION_CATALOG_FOOTER\n\n";

    catalog.load_catalog_file_for_testing(catalog_file_path);
    EXPECT_EQ(catalog.get_max_epoch_id(), max_epoch_id);
    EXPECT_EQ(catalog.get_compacted_files(), compacted_files);
    EXPECT_EQ(catalog.get_detached_pwals(), detached_pwals);

    // unexpected end of file while reading header line
    writer.clear();
    EXPECT_THROW(
        {
            try {
                catalog.load_catalog_file_for_testing(catalog_file_path);
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
                catalog.load_catalog_file_for_testing(catalog_file_path);
            } catch (const limestone_exception& e) {
                EXPECT_EQ(e.error_code(), 0);
                EXPECT_TRUE(std::string(e.what()).find("Invalid header line:") != std::string::npos)
                    << "Actual message: " << e.what();
                throw;
            }
        },
        limestone_exception);

writer.clear();
writer << "COMPACTION_CATALOG_HEADER\n"
       << "COMPACTED_FILE file1 1\n"
       << "COMPACTED_FILE file2 2\n"
       << "DETACHED_PWAL pwal1\n"
       << "DETACHED_PWAL pwal2\n"
       << "MAX_EPOCH_ID 123\n";
EXPECT_THROW(
    {
        try {
            catalog.load_catalog_file_for_testing(catalog_file_path);
        } catch (const limestone_exception& e) {
            EXPECT_EQ(e.error_code(), 0);
            EXPECT_TRUE(std::string(e.what()).find("Missing footer line") != std::string::npos) << "Actual message: " << e.what();
            throw;
        }
    },
    limestone_exception);

    writer.clear();
    writer << "COMPACTION_CATALOG_HEADER\n"
           << "COMPACTED_FILE file1 1\n"
           << "COMPACTED_FILE file2 2\n"
           << "DETACHED_PWAL pwal1\n"
           << "DETACHED_PWAL pwal2\n"
           << "COMPACTION_CATALOG_FOOTER\n";
    EXPECT_THROW(
        {
            try {
                catalog.load_catalog_file_for_testing(catalog_file_path);
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
                catalog.load_catalog_file_for_testing(catalog_file_path);
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
                catalog.load_catalog_file_for_testing(catalog_file_path);
            } catch (const limestone_io_exception& e) {
                EXPECT_EQ(e.error_code(), ENOSPC);
                EXPECT_TRUE(std::string(e.what()).find("Failed to read line from file") != std::string::npos) << "Actual message: " << e.what();
                throw;
            }
        },
        limestone_io_exception);
    
    catalog.reset_file_operations();
}

TEST_F(compaction_catalog_test, load_from_backup) {
    {
        compaction_catalog catalog(test_dir);

        epoch_id_type max_epoch_id = 123;
        std::set<compacted_file_info> compacted_files = {
            {"file1", 1},
            {"file2", 2}
        };
        std::set<std::string> detached_pwals = {"pwal1", "pwal2"};

        catalog.update_catalog_file(max_epoch_id, compacted_files, detached_pwals);

    }

    boost::filesystem::rename(catalog_file_path, backup_file_path);

    boost::filesystem::remove(catalog_file_path);

    compaction_catalog loaded_catalog = compaction_catalog::from_catalog_file(test_dir);

    EXPECT_EQ(loaded_catalog.get_max_epoch_id(), 123);
    EXPECT_EQ(loaded_catalog.get_compacted_files().size(), 2);
    EXPECT_EQ(loaded_catalog.get_detached_pwals().size(), 2);
}

}  // namespace limestone::testing
