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

namespace limestone::api {


// Constructor that takes a directory path and initializes file paths
compaction_catalog::compaction_catalog(const boost::filesystem::path &directory_path)
    : catalog_file_path_(directory_path / COMPACTION_CATALOG_FILENAME),
      backup_file_path_(directory_path / COMPACTION_CATALOG_BACKUP_FILENAME) {}

// Static method to create a compaction_catalog from a catalog file
compaction_catalog compaction_catalog::from_catalog_file(const boost::filesystem::path& directory_path) {
    compaction_catalog catalog(directory_path);
    try {
        catalog.load_catalog_file(catalog.catalog_file_path_);
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
void compaction_catalog::load_catalog_file(const boost::filesystem::path& path) {

    std::ifstream file(path.string());
    if (!file.is_open()) {
        throw std::runtime_error("Failed to open compaction catalog file: " + path.string());
    }

    std::string line;

    // Check header line
    if (!std::getline(file, line) || line != HEADER_LINE) {
        throw std::runtime_error("Invalid or missing header{} line: " + line);
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
            int version = 0;
            if (iss >> file_name >> version) {
                compacted_files_.insert({file_name, version});
            } else {
                throw std::runtime_error("Invalid format for " + std::string(COMPACTED_FILE_KEY) + ": " + line);
            }
        } else if (type == MIGRATED_PWAL_KEY) {
            std::string pwal;
            if (iss >> pwal) {
                migrated_pwals_.insert(pwal);
            } else {
                throw std::runtime_error("Invalid format for " + std::string(MIGRATED_PWAL_KEY) + ": " + line);
            }
        } else if (type == MAX_EPOCH_ID_KEY) {
            epoch_id_type epoch_id = 0;
            if (iss >> epoch_id) {
                max_epoch_id_ = epoch_id;
                max_epoch_id_found = true;
            } else {
                throw std::runtime_error("Invalid format for " + std::string(MAX_EPOCH_ID_KEY) + ": " + line);
            }
        } else {
            throw std::runtime_error("Unknown entry type: " + type);
        }
    }

    // If the footer line was not found, throw an error
    throw std::runtime_error("Missing footer line");
}

// Method to update the compaction catalog
void compaction_catalog::update_catalog_file(epoch_id_type max_epoch_id, const std::set<compacted_file_info>& compacted_files, const std::set<std::string>& migrated_pwals) {
    // Update internal state
    max_epoch_id_ = max_epoch_id;
    compacted_files_ = compacted_files;
    migrated_pwals_ = migrated_pwals;

    // Create the catalog using std::string
    std::string catalog = create_catalog_content();

    // Rename the current catalog file to a backup if it exists
    try {
        if (boost::filesystem::exists(catalog_file_path_)) {
            if (rename(catalog_file_path_.c_str(), backup_file_path_.c_str()) != 0) {
                throw std::runtime_error("Failed to rename catalog file to backup '" + backup_file_path_.string() +
                                         "': " + std::string(strerror(errno)));
            }
        }
    } catch (const boost::filesystem::filesystem_error &e) {
        throw std::runtime_error("Failed to check existence of catalog file '" + catalog_file_path_.string() +
                                 "': " + std::string(e.what()));
    }


    // Open the new catalog file using fopen
    FILE *file = fopen(catalog_file_path_.c_str(), "w"); // NOLINT(*-owning-memory)

    if (!file) {
        throw std::runtime_error("Failed to open compaction catalog file '" + catalog_file_path_.string() +
                                 "': " + std::string(strerror(errno)));
    }

    try {

        // Write the data to the file using fwrite
        size_t written = fwrite(catalog.data(), 1, catalog.size(), file);
        if (written != catalog.size()) {
            throw std::runtime_error("Failed to write to compaction catalog file '" + catalog_file_path_.string() +
                                     "'");
        }

        // Perform fflush to ensure all data is written to the file
        if (fflush(file) != 0) {
            throw std::runtime_error("Failed to flush the output buffer to file '" + catalog_file_path_.string() +
                                     "': " + std::string(strerror(errno)));
        }

        // Perform fsync to ensure data is written to disk
        int fd = fileno(file);
        if (fd == -1) {
            throw std::runtime_error("Failed to get file descriptor for file '" + catalog_file_path_.string() +
                                     "': " + std::string(strerror(errno)));
        }
        if (fsync(fd) != 0) {
            throw std::runtime_error("Failed to fsync compaction catalog file '" + catalog_file_path_.string() +
                                     "': " + std::string(strerror(errno)));
        }

    } catch (...) {
        // If an exception occurs, ensure the file is closed and retain the original exception
        try {
            if (fclose(file) != 0) { // NOLINT(*-owning-memory)
                LOG_LP(ERROR) << "fclose failed for file '" << catalog_file_path_.string() << "', errno = " << errno;
                throw std::runtime_error("I/O error occurred while closing file '" + catalog_file_path_.string() +
                                         "': " + std::string(strerror(errno)));
            }
        } catch (...) {
            // Throw the original exception and the exception from fclose
            std::throw_with_nested(std::runtime_error("Failed to close the file after an error occurred"));
        }
        throw; // Re-throw the caught exception
    }

    // Close the file descriptor
    if (fclose(file) != 0) { // NOLINT(*-owning-memory)
        LOG_LP(ERROR) << "fclose failed for file '" << catalog_file_path_.string() << "', errno = " << errno;
        throw std::runtime_error("I/O error occurred while closing file '" + catalog_file_path_.string() +
                                 "': " + std::string(strerror(errno)));
    }
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

    for (const auto &pwal : migrated_pwals_) {
        catalog += MIGRATED_PWAL_KEY;
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

const std::set<std::string>& compaction_catalog::get_migrated_pwals() const {
    return migrated_pwals_;
}

} // namespace limestone::api
