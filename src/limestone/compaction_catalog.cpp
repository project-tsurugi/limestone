#include "compaction_catalog.h"
#include <fstream>
#include <stdexcept>
#include <sstream>
#include <fcntl.h> // for open, O_WRONLY
#include <unistd.h> // for fsync, close
#include <system_error> // for std::system_error
#include <boost/filesystem.hpp>

namespace limestone::api {

// Define constants for file names
const std::string compaction_catalog::COMPACTION_CATALOG_FILENAME = "compaction_catalog";
const std::string compaction_catalog::COMPACTION_CATALOG_BACKUP_FILENAME = "compaction_catalog.bak";
const std::string compaction_catalog::HEADER_LINE = "COMPACTION_CATALOG_HEADER";
const std::string compaction_catalog::FOOTER_LINE = "COMPACTION_CATALOG_FOOTER";
const std::string compaction_catalog::COMPACTED_FILE_KEY = "COMPACTED_FILE";
const std::string compaction_catalog::MIGRATED_PWAL_KEY = "MIGRATED_PWAL";
const std::string compaction_catalog::MAX_EPOCH_ID_KEY = "MAX_EPOCH_ID";

// Default constructor
compaction_catalog::compaction_catalog()
    : max_epoch_id_(0) {}

// Static method to create a compaction_catalog from a catalog file
compaction_catalog compaction_catalog::from_catalog_file(const boost::filesystem::path& directory_path) {
    compaction_catalog catalog;
    catalog.catalog_file_path_ = directory_path / COMPACTION_CATALOG_FILENAME;
    catalog.backup_file_path_ = directory_path / COMPACTION_CATALOG_BACKUP_FILENAME;

    try {
        catalog.load_catalog_file(directory_path);
    } catch (const std::runtime_error& e) {
        // Handle error and attempt to restore from backup
        if (boost::filesystem::exists(catalog.backup_file_path_)) {
            try {
                // Load the backup file
                catalog.load_catalog_file(catalog.backup_file_path_);

                // Restore the backup file as the main catalog file
                if (boost::filesystem::exists(catalog.catalog_file_path_)) {
                    if (unlink(catalog.catalog_file_path_.c_str()) != 0) {
                        throw std::runtime_error("Failed to remove existing catalog file: " + std::string(strerror(errno)));
                    }
                }
                if (rename(catalog.backup_file_path_.c_str(), catalog.catalog_file_path_.c_str()) != 0) {
                    throw std::runtime_error("Failed to rename backup file to catalog file: " + std::string(strerror(errno)));
                }
            } catch (const std::runtime_error& backup_error) {
                throw std::runtime_error("Failed to restore from backup compaction catalog file: " + std::string(backup_error.what()));
            }
        } else {
            throw std::runtime_error("Failed to load compaction catalog file and no backup available: " + std::string(e.what()));
        }
    }
    return catalog;
}


// Helper method to load the catalog file
void compaction_catalog::load_catalog_file(const boost::filesystem::path& directory_path) {
    catalog_file_path_ = directory_path / COMPACTION_CATALOG_FILENAME;
    backup_file_path_ = directory_path / COMPACTION_CATALOG_BACKUP_FILENAME;

    std::ifstream file(catalog_file_path_.string());
    if (!file.is_open()) {
        throw std::runtime_error("Failed to open compaction catalog file: " + catalog_file_path_.string());
    }

    std::string line;

    // Check header line
    if (!std::getline(file, line) || line != HEADER_LINE) {
        throw std::runtime_error("Invalid or missing header line: " + line);
    }

    bool max_epoch_id_found = false;

    while (std::getline(file, line)) {
        if (line == FOOTER_LINE) {
            if (!max_epoch_id_found) {
                throw std::runtime_error("MAX_EPOCH_ID entry not found");
            }
            return; // Footer found, exit successfully
        }

        std::istringstream iss(line);
        std::string type;
        if (!(iss >> type)) {
            continue; // Skip empty lines
        }

        if (type == COMPACTED_FILE_KEY) {
            std::string file_name;
            int version;
            if (iss >> file_name >> version) {
                compacted_files_.insert({file_name, version});
            } else {
                throw std::runtime_error("Invalid format for " + COMPACTED_FILE_KEY + ": " + line);
            }
        } else if (type == MIGRATED_PWAL_KEY) {
            std::string pwal;
            if (iss >> pwal) {
                migrated_pwals_.insert(pwal);
            } else {
                throw std::runtime_error("Invalid format for " + MIGRATED_PWAL_KEY + ": " + line);
            }
        } else if (type == MAX_EPOCH_ID_KEY) {
            epoch_id_type epoch_id;
            if (iss >> epoch_id) {
                max_epoch_id_ = epoch_id;
                max_epoch_id_found = true;
            } else {
                throw std::runtime_error("Invalid format for " + MAX_EPOCH_ID_KEY + ": " + line);
            }
        } else {
            throw std::runtime_error("Unknown entry type: " + type);
        }
    }

    // If the footer line was not found, throw an error
    throw std::runtime_error("Missing footer line");
}

// Method to update the compaction catalog
void compaction_catalog::update_catalog(epoch_id_type max_epoch_id, const std::set<compacted_file_info>& compacted_files, const std::set<std::string>& migrated_pwals) {
    // Update internal state
    max_epoch_id_ = max_epoch_id;
    compacted_files_ = compacted_files;
    migrated_pwals_ = migrated_pwals;
}

// Method to write the compaction catalog to a file
void compaction_catalog::update_catalog_file() const {
    // Rename the current catalog file to a backup if it exists
    try {
        if (boost::filesystem::exists(catalog_file_path_)) {
            if (rename(catalog_file_path_.c_str(), backup_file_path_.c_str()) != 0) {
                throw std::runtime_error("Failed to rename catalog file to backup: " + std::string(strerror(errno)));
            }
        }
    } catch (const boost::filesystem::filesystem_error& e) {
        throw std::runtime_error("Failed to check existence of catalog file: " + std::string(e.what()));
    }

    // Write to the new catalog file
    std::ofstream catalog_file(catalog_file_path_.string(), std::ios::trunc);
    if (!catalog_file.is_open()) {
        throw std::runtime_error("Failed to open compaction catalog file for writing: " + catalog_file_path_.string());
    }

    // Write header
    catalog_file << HEADER_LINE << "\n";

    // Write compacted files
    for (const auto& file_info : compacted_files_) {
        catalog_file << COMPACTED_FILE_KEY << " " << file_info.file_name << " " << file_info.version << "\n";
    }

    // Write migrated PWALs
    for (const auto& pwal : migrated_pwals_) {
        catalog_file << MIGRATED_PWAL_KEY << " " << pwal << "\n";
    }

    // Write max epoch ID
    catalog_file << MAX_EPOCH_ID_KEY << " " << max_epoch_id_ << "\n";

    // Write footer
    catalog_file << FOOTER_LINE << "\n";

    catalog_file.close();

    // Ensure the catalog file is fully written to disk
    int catalog_fd = ::open(catalog_file_path_.string().c_str(), O_WRONLY);
    if (catalog_fd == -1) {
        throw std::runtime_error("Failed to open catalog file descriptor for fsync");
    }
    if (::fsync(catalog_fd) == -1) {
        ::close(catalog_fd);
        throw std::runtime_error("Failed to fsync catalog file");
    }
    ::close(catalog_fd);

    // Remove the backup file after successful write
    if (boost::filesystem::exists(backup_file_path_)) {
        boost::filesystem::remove(backup_file_path_);
    }
}

// Getter methods
epoch_id_type compaction_catalog::get_max_epoch_id() const {
    return max_epoch_id_;
}

const std::set<compacted_file_info>& compaction_catalog::get_compacted_files() const {
    return compacted_files_;
}

const std::set<std::string>& compaction_catalog::get_migrated_pwals() const {
    return migrated_pwals_;
}

} // namespace limestone::api
