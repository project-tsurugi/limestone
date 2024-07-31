#ifndef COMPACTION_CATALOG_H
#define COMPACTION_CATALOG_H

#include <boost/filesystem.hpp>
#include <set>
#include <string>
#include <utility>

#include "limestone/api/epoch_id_type.h"

namespace limestone::api {

// Structure to hold information about compacted files
struct compacted_file_info {
private:
    std::string file_name;
    int version;

public:
    // Constructor
    compacted_file_info(std::string file_name, int version) : file_name(std::move(file_name)), version(version) {}

    // Accessor methods
    [[nodiscard]] const std::string &get_file_name() const { return file_name; }
    [[nodiscard]] int get_version() const { return version; }

    // Define less-than operator for
    // comparison
    bool operator<(const compacted_file_info &other) const {
        if (file_name != other.file_name) {
            return file_name < other.file_name;
        }
        return version < other.version;
    }

    // Define equality operator for
    // comparison
    bool operator==(const compacted_file_info &other) const {
        return file_name == other.file_name && version == other.version;
    }
};

class compaction_catalog {
public:
    // Constructor
    explicit compaction_catalog(const boost::filesystem::path &directory_path);

    // Static method to create a compaction_catalog from a catalog file
    static compaction_catalog from_catalog_file(const boost::filesystem::path &directory_path);

    // Method to update the compaction catalog and write it to a file
    void update_catalog_file(epoch_id_type max_epoch_id, const std::set<compacted_file_info> &compacted_files,
                        const std::set<std::string> &migrated_pwals);

    // Getter methods
    [[nodiscard]] epoch_id_type get_max_epoch_id() const;
    [[nodiscard]] const std::set<compacted_file_info> &get_compacted_files() const;
    [[nodiscard]] const std::set<std::string> &get_migrated_pwals() const;

private:
    // Constants
    static constexpr const char *COMPACTION_CATALOG_FILENAME = "compaction_catalog";
    static constexpr const char *COMPACTION_CATALOG_BACKUP_FILENAME = "compaction_catalog.back";
    static constexpr const char *HEADER_LINE = "COMPACTION_CATALOG_HEADER";
    static constexpr const char *FOOTER_LINE = "COMPACTION_CATALOG_FOOTER";
    static constexpr const char *COMPACTED_FILE_KEY = "COMPACTED_FILE";
    static constexpr const char *MIGRATED_PWAL_KEY = "MIGRATED_PWAL";
    static constexpr const char *MAX_EPOCH_ID_KEY = "MAX_EPOCH_ID";

    // Member variables
    boost::filesystem::path catalog_file_path_;     // Path of the compaction catalog file
    boost::filesystem::path backup_file_path_;      // Path of the backup file
    std::set<compacted_file_info> compacted_files_; // Set of compacted files
    std::set<std::string> migrated_pwals_;          // Set of migrated PWALs
    epoch_id_type max_epoch_id_ = 0;                // Maximum epoch ID included in the compacted files

    // Helper method to load the catalog file
    void load_catalog_file(const boost::filesystem::path &directory_path);


    // Helper function to create the catalog content from instance fields
    [[nodiscard]] std::string create_catalog_content() const;
};

} // namespace limestone::api

#endif // COMPACTION_CATALOG_H
