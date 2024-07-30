#ifndef COMPACTION_CATALOG_H
#define COMPACTION_CATALOG_H

#include <string>
#include <set>

#include <boost/filesystem.hpp>
#include <limestone/api/epoch_id_type.h>

namespace limestone::api {

    // Structure to hold information about compacted files
    struct compacted_file_info {
        std::string file_name;
        int version;

        // Define less-than operator for comparison
        bool operator<(const compacted_file_info &other) const {
            if (file_name != other.file_name)
                return file_name < other.file_name;
            return version < other.version;
        }

        // Define equality operator for comparison
        bool operator==(const compacted_file_info &other) const{
            return file_name == other.file_name && version == other.version;
        }
    };

    class compaction_catalog {
    public:
public:
        // Constructor
        compaction_catalog(const boost::filesystem::path& directory_path);

        // Static method to create a compaction_catalog from a catalog file
        static compaction_catalog from_catalog_file(const boost::filesystem::path& directory_path);

        // Method to update the compaction catalog
        void update_catalog(
            epoch_id_type max_epoch_id,
            const std::set<compacted_file_info>& compacted_files,
            const std::set<std::string>& migrated_pwals);

        // Method to write the compaction catalog to a file
        void update_catalog_file() const;

        // Getter methods
        epoch_id_type get_max_epoch_id() const;
        const std::set<compacted_file_info>& get_compacted_files() const;
        const std::set<std::string>& get_migrated_pwals() const;

    private:
        // Constants
        static const std::string COMPACTION_CATALOG_FILENAME;
        static const std::string COMPACTION_CATALOG_BACKUP_FILENAME;
        static const std::string HEADER_LINE;
        static const std::string FOOTER_LINE;
        static const std::string COMPACTED_FILE_KEY;
        static const std::string MIGRATED_PWAL_KEY;
        static const std::string MAX_EPOCH_ID_KEY;

        // Member variables
        boost::filesystem::path catalog_file_path_;     // Path of the compaction catalog file
        boost::filesystem::path backup_file_path_;      // Path of the backup file
        std::set<compacted_file_info> compacted_files_; // Set of compacted files
        std::set<std::string> migrated_pwals_;          // Set of migrated PWALs
        epoch_id_type max_epoch_id_;                    // Maximum epoch ID included in the compacted files

        // Helper method to load the catalog file
        void load_catalog_file(const boost::filesystem::path& directory_path);
    };

} // namespace limestone::api

#endif // COMPACTION_CATALOG_H
