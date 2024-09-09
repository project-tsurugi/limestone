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

#include <glog/logging.h>
#include <fstream>
#include <stdexcept>
#include <sstream>
#include <fcntl.h> // for open, O_WRONLY
#include <unistd.h> // for fsync, close
#include <system_error> // for std::system_error
#include <boost/filesystem.hpp>


#include "compaction_catalog.h"
#include "logging_helper.h"
#include "limestone/api/epoch_id_type.h"
#include "limestone/api/limestone_exception.h"

namespace limestone::internal {

using limestone::api::epoch_id_type;

// Constructor that takes a directory path and initializes file paths
compaction_catalog::compaction_catalog(const boost::filesystem::path &directory_path)
    : catalog_file_path_(directory_path / COMPACTION_CATALOG_FILENAME),
      backup_file_path_(directory_path / COMPACTION_CATALOG_BACKUP_FILENAME) {}

// Static method to create a compaction_catalog from a catalog file
compaction_catalog compaction_catalog::from_catalog_file(const boost::filesystem::path& directory_path) {
    compaction_catalog catalog(directory_path);
    catalog.load();
    return catalog;
}

void compaction_catalog::load() {
    try {
        // Load the main catalog file
        load_catalog_file(catalog_file_path_);
    } catch (const limestone_exception& e) {
        // Handle error and attempt to restore from backup
        boost::system::error_code ec;

        // Check if the backup file exists
        if (file_ops_->exists(backup_file_path_, ec)) {
            try {
                // Load the backup file
                load_catalog_file(backup_file_path_);

                // Restore the backup file as the main catalog file
                if (file_ops_->exists(catalog_file_path_, ec)) {
                    if (file_ops_->unlink(catalog_file_path_.c_str()) != 0) {
                        int error_num = errno;
                        THROW_LIMESTONE_IO_EXCEPTION("Failed to remove existing catalog file: " + catalog_file_path_.string(), error_num);
                    }
                } else if (ec && ec != boost::system::errc::no_such_file_or_directory) {
                    THROW_LIMESTONE_IO_EXCEPTION("Error checking catalog file existence", ec.value());
                }

                // Rename the backup file to catalog file
                if (file_ops_->rename(backup_file_path_.c_str(), catalog_file_path_.c_str()) != 0) {
                    int error_num = errno;
                    THROW_LIMESTONE_IO_EXCEPTION("Failed to rename backup file: " + backup_file_path_.string() + " to catalog file: " + catalog_file_path_.string(), error_num);
                }
            } catch (const limestone_exception& backup_error) {
                THROW_LIMESTONE_EXCEPTION("Failed to restore from backup compaction catalog file: " + std::string(backup_error.what()));
            }
        } else if (ec && ec != boost::system::errc::no_such_file_or_directory) {
            THROW_LIMESTONE_IO_EXCEPTION("Error checking backup file existence", ec.value());
        } else {
            THROW_LIMESTONE_EXCEPTION("Failed to load compaction catalog file and no backup available.");
        }
    }
}


// Helper method to load the catalog file
void compaction_catalog::load_catalog_file(const boost::filesystem::path& path) {
    // file_operations を使って ifstream を開く
    auto file = file_ops_->open_ifstream(path.string());
    int error_num = errno;
    if (!file->is_open()) {
        THROW_LIMESTONE_IO_EXCEPTION("Failed to open compaction catalog file: " + path.string(), error_num);
    }

    std::string line;
    if (!file_ops_->read_line(*file, line)) {
        error_num = errno;
        if (file_ops_->is_eof(*file)) {
            THROW_LIMESTONE_EXCEPTION("Unexpected end of file while reading header line");
        } 
        if (file_ops_->has_error(*file)) {
            THROW_LIMESTONE_IO_EXCEPTION("Failed to read line from file", error_num);
        }
    }

    if (line != HEADER_LINE) {
        THROW_LIMESTONE_EXCEPTION("Invalid header line: " + line);
    }

    bool max_epoch_id_found = false;
    while (true) {
        if (!file_ops_->read_line(*file, line)) {
            error_num = errno;
            if (file_ops_->is_eof(*file)) {
                break;
            }
            THROW_LIMESTONE_IO_EXCEPTION("Failed to read line from file", error_num);
        }
        if (line == FOOTER_LINE) {
            if (!max_epoch_id_found) {
                THROW_LIMESTONE_EXCEPTION("MAX_EPOCH_ID entry not found");
            }
            return;  // Footer found, exit successfully
        }
        parse_catalog_entry(line, max_epoch_id_found);
    }

    // If the footer line was not found, throw an error
    THROW_LIMESTONE_EXCEPTION("Missing footer line");
}


// Helper method to parse a catalog entry
void compaction_catalog::parse_catalog_entry(const std::string& line, bool& max_epoch_id_found) {
    std::istringstream iss(line);
    std::string type;
    if (!(iss >> type)) {
        return; // Skip empty lines
    }

    if (type == COMPACTED_FILE_KEY) {
        std::string file_name;
        int version = 0;
        if (iss >> file_name >> version) {
            compacted_files_.insert({file_name, version});
        } else {
            THROW_LIMESTONE_EXCEPTION("Invalid format for " + std::string(COMPACTED_FILE_KEY) + ": " + line);
        }
    } else if (type == DETACHED_PWAL_KEY) {
        std::string pwal;
        if (iss >> pwal) {
            detached_pwals_.insert(pwal);
        } else {
            THROW_LIMESTONE_EXCEPTION("Invalid format for " + std::string(DETACHED_PWAL_KEY) + ": " + line);
        }
    } else if (type == MAX_EPOCH_ID_KEY) {
        size_t epoch_id = 0; 
        if (iss >> epoch_id) {
            max_epoch_id_ = epoch_id;
            max_epoch_id_found = true;
        } else {
            THROW_LIMESTONE_EXCEPTION("Invalid format for " + std::string(MAX_EPOCH_ID_KEY) + ": " + line);
        }
    } else {
        THROW_LIMESTONE_EXCEPTION("Unknown entry type: " + type);
    }
    
}

// Method to update the compaction catalog
void compaction_catalog::update_catalog_file(epoch_id_type max_epoch_id, const std::set<compacted_file_info>& compacted_files, const std::set<std::string>& detached_pwals) {
    // Update internal state
    max_epoch_id_ = max_epoch_id;
    compacted_files_ = compacted_files;
    detached_pwals_ = detached_pwals;

    // Create the catalog using std::string
    std::string catalog = create_catalog_content();

    // Rename the current catalog file to a backup if it exists
    boost::system::error_code ec;
    if (file_ops_->exists(catalog_file_path_, ec)) {
        if (file_ops_->rename(catalog_file_path_.c_str(), backup_file_path_.c_str()) != 0) {
            int error_num = errno;
            THROW_LIMESTONE_IO_EXCEPTION("Failed to rename catalog file: " + catalog_file_path_.string() + " to backup file: " + backup_file_path_.string(), error_num);
        }
    }
    if (ec && ec != boost::system::errc::no_such_file_or_directory) {
        THROW_LIMESTONE_IO_EXCEPTION("Error checking catalog file existence", ec.value());
    }

    // Open the new catalog file using fopen and manage it with std::unique_ptr
    auto file_closer = [this](FILE* file) {
        // NOLINTNEXTLINE(cppcoreguidelines-owning-memory)
        if (file && file_ops_->fclose(file) != 0) {
            int error_num = errno;
            LOG_LP(ERROR) << "fclose failed for file, errno = " << error_num;
        }
    };
    std::unique_ptr<FILE, decltype(file_closer)> file_ptr(file_ops_->fopen(catalog_file_path_.c_str(), "w"), file_closer);

    if (!file_ptr) {
        int error_num = errno;
        THROW_LIMESTONE_IO_EXCEPTION("Failed to open compaction catalog file: " + catalog_file_path_.string(), error_num);
    }

    // Write the data to the file using fwrite
    size_t total_written = 0;
    size_t catalog_size = catalog.size();
    while (total_written < catalog_size) {
        size_t remaining_size = catalog_size - total_written;
        std::string chunk = catalog.substr(total_written, remaining_size);  
        size_t written = file_ops_->fwrite(chunk.data(), 1, remaining_size, file_ptr.get());
        int error_num = errno;
        if (written == 0) {
            if (ferror(file_ptr.get()) != 0) {
                THROW_LIMESTONE_IO_EXCEPTION("Failed to write complete data to compaction catalog file '" + catalog_file_path_.string() + "'", error_num);
            }
            THROW_LIMESTONE_EXCEPTION("Failed to write complete data to compaction catalog file '" + catalog_file_path_.string() + "'");
        }
        total_written += written;
    }

    // Perform fflush to ensure all data is written to the file
    if (file_ops_->fflush(file_ptr.get()) != 0) {
        int error_num = errno;
        THROW_LIMESTONE_IO_EXCEPTION("Failed to flush the output buffer to file '" + catalog_file_path_.string() + "'", error_num);
    }

    // Perform fsync to ensure data is written to disk
    int fd = fileno(file_ptr.get());
    if (fd == -1) {
        int error_num = errno;
        THROW_LIMESTONE_IO_EXCEPTION("Failed to get file descriptor for file '" + catalog_file_path_.string() + "'", error_num);
    }
    if (file_ops_->fsync(fd) != 0) {
        int error_num = errno;
        THROW_LIMESTONE_IO_EXCEPTION("Failed to fsync compaction catalog file '" + catalog_file_path_.string() + "'", error_num);
    }

    // The file will be automatically closed by unique_ptr when going out of scope
}



// Helper function to create the catalog content from instance fields
std::string compaction_catalog::create_catalog_content() const {
    std::string catalog;
    catalog += HEADER_LINE;
    catalog += "\n";

    for (const auto &file_info : compacted_files_) {
        catalog += COMPACTED_FILE_KEY;
        catalog += " " + file_info.get_file_name();
        catalog += " " + std::to_string(file_info.get_version());
        catalog += "\n";
    }

    for (const auto &pwal : detached_pwals_) {
        catalog += DETACHED_PWAL_KEY;
        catalog += " " + pwal;
        catalog += "\n";
    }

    catalog += MAX_EPOCH_ID_KEY;
    catalog += " " + std::to_string(max_epoch_id_);
    catalog += "\n";

    catalog += FOOTER_LINE;
    catalog += "\n";

    return catalog;
}


// Getter methods
epoch_id_type compaction_catalog::get_max_epoch_id() const {
    return max_epoch_id_;
}

const std::set<compacted_file_info>& compaction_catalog::get_compacted_files() const {
    return compacted_files_;
}

const std::set<std::string>& compaction_catalog::get_detached_pwals() const {
    return detached_pwals_;
}

// for Unit Testing

void compaction_catalog::set_file_operations(std::unique_ptr<file_operations> file_ops) {
    file_ops_ = std::move(file_ops);
}

void compaction_catalog::reset_file_operations() {
    file_ops_ = std::make_unique<real_file_operations>();
}

} // namespace limestone::internal
