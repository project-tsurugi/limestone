#ifndef COMPACTION_CATALOG_H
#define COMPACTION_CATALOG_H

#include <boost/filesystem.hpp>
#include <set>
#include <string>
#include <utility>

#include "limestone/api/epoch_id_type.h"

namespace limestone::api {

/**
 * @brief Structure to hold information about compacted files.
 * 
 * This structure stores the filename and version information for files
 * that have been compacted. It also provides comparison operators to
 * facilitate sorting and equality checks.
 */
struct compacted_file_info {
private:
    std::string file_name; ///< Name of the compacted file
    int version; ///< Version of the compacted file

public:
    /**
     * @brief Constructs a new compacted_file_info object.
     * 
     * @param file_name Name of the compacted file.
     * @param version Version number of the compacted file.
     */
    compacted_file_info(std::string file_name, int version) : file_name(std::move(file_name)), version(version) {}

    /**
     * @brief Gets the name of the compacted file.
     * 
     * @return const std::string& Reference to the name of the file.
     */
    [[nodiscard]] const std::string &get_file_name() const { return file_name; }

    /**
     * @brief Gets the version of the compacted file.
     * 
     * @return int Version number of the file.
     */
    [[nodiscard]] int get_version() const { return version; }

    /**
     * @brief Comparison operator to determine the order of compacted files.
     * 
     * Files are first compared by their name and then by their version.
     * 
     * @param other Another compacted_file_info object to compare against.
     * @return true If this file is considered less than the other file.
     * @return false Otherwise.
     */
    bool operator<(const compacted_file_info &other) const {
        if (file_name != other.file_name) {
            return file_name < other.file_name;
        }
        return version < other.version;
    }

    /**
     * @brief Equality operator to check if two compacted_file_info objects are identical.
     * 
     * Files are considered equal if both their name and version match.
     * 
     * @param other Another compacted_file_info object to compare against.
     * @return true If both objects are equal.
     * @return false Otherwise.
     */
    bool operator==(const compacted_file_info &other) const {
        return file_name == other.file_name && version == other.version;
    }
};

/**
 * @brief Class to manage the compaction catalog.
 * 
 * This class handles the cataloging of compacted files within a specific directory.
 * It provides methods for updating, loading, and retrieving information about
 * the results of the compaction process.
 */
class compaction_catalog {
public:
    /**
     * @brief Constructs a new compaction_catalog object.
     * 
     * @param directory_path Path to the directory where the catalog is located.
     */
    explicit compaction_catalog(const boost::filesystem::path &directory_path);

    compaction_catalog(compaction_catalog&& other) noexcept = default;
    compaction_catalog(const compaction_catalog& other) = delete;
    compaction_catalog& operator=(compaction_catalog const& other) = delete;
    compaction_catalog& operator=(compaction_catalog&& other) noexcept = default;

    /**
     * @brief Destroys the compaction_catalog object.
     */
    ~compaction_catalog() = default;

    /**
     * @brief Creates a compaction_catalog object from an existing catalog file.
     * 
     * This static method loads the catalog data from a file in the specified directory.
     * 
     * @param directory_path Path to the directory containing the catalog file.
     * @return compaction_catalog A compaction_catalog object with the loaded data.
     */
    static compaction_catalog from_catalog_file(const boost::filesystem::path &directory_path);

    /**
     * @brief Updates the compaction catalog and writes the changes to a file.
     * 
     * This method updates the catalog with new compacted files, migrated PWALs, and the maximum epoch ID,
     * then writes the updated catalog to a file.
     * 
     * @param max_epoch_id The maximum epoch ID to be recorded in the catalog.
     * @param compacted_files Set of compacted files to be included in the catalog.
     * @param migrated_pwals Set of migrated PWALs to be included in the catalog.
     */
    void update_catalog_file(epoch_id_type max_epoch_id, const std::set<compacted_file_info> &compacted_files,
                        const std::set<std::string> &migrated_pwals);

    /**
     * @brief Gets the maximum epoch ID from the catalog.
     * 
     * @return epoch_id_type The maximum epoch ID recorded in the catalog.
     */
    [[nodiscard]] epoch_id_type get_max_epoch_id() const;

    /**
     * @brief Gets the set of migrated PWALs from the catalog.
     *
     * PWAL is not compacted directly but migrated for other purposes.
     * This method retrieves the set of PWALs that have been moved for further use after compaction.
     *
     * @return const std::set<std::string>& Reference to the set of migrated PWALs.
     */
    [[nodiscard]] const std::set<compacted_file_info> &get_compacted_files() const;

    /**
     * @brief Gets the set of migrated PWALs from the catalog.
     * 
     * @return const std::set<std::string>& Reference to the set of migrated PWALs.
     */
    [[nodiscard]] const std::set<std::string> &get_migrated_pwals() const;

    /**
     * @brief Returns the filename of the compaction catalog.
     * 
     * @return The filename of the compaction catalog.
     */
    [[nodiscard]] static inline std::string_view get_catalog_filename() { return COMPACTION_CATALOG_FILENAME; }

private:
    // Constants
    static constexpr const char *COMPACTION_CATALOG_FILENAME = "compaction_catalog"; ///< Name of the catalog file
    static constexpr const char *COMPACTION_CATALOG_BACKUP_FILENAME = "compaction_catalog.back"; ///< Name of the backup catalog file
    static constexpr const char *HEADER_LINE = "COMPACTION_CATALOG_HEADER"; ///< Header identifier for the catalog file
    static constexpr const char *FOOTER_LINE = "COMPACTION_CATALOG_FOOTER"; ///< Footer identifier for the catalog file
    static constexpr const char *COMPACTED_FILE_KEY = "COMPACTED_FILE"; ///< Key for compacted files in the catalog file
    static constexpr const char *MIGRATED_PWAL_KEY = "MIGRATED_PWAL"; ///< Key for migrated PWALs in the catalog file
    static constexpr const char *MAX_EPOCH_ID_KEY = "MAX_EPOCH_ID"; ///< Key for maximum epoch ID in the catalog file

    // Member variables
    boost::filesystem::path catalog_file_path_; ///< Path of the compaction catalog file
    boost::filesystem::path backup_file_path_; ///< Path of the backup file
    std::set<compacted_file_info> compacted_files_{}; ///< Set of compacted files
    std::set<std::string> migrated_pwals_{}; ///< Set of migrated PWALs
    epoch_id_type max_epoch_id_ = 0; ///< Maximum epoch ID included in the compacted files

    // Helper methods
    void load_catalog_file(const boost::filesystem::path &directory_path);
    void parse_catalog_entry(const std::string& line, bool& max_epoch_id_found);
    [[nodiscard]] std::string create_catalog_content() const;
};

} // namespace limestone::api

#endif // COMPACTION_CATALOG_H
