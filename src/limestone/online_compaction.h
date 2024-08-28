#ifndef ONLINE_COMPACTION_H
#define ONLINE_COMPACTION_H

#include <boost/filesystem.hpp>
#include <set>
#include <string>

namespace limestone::internal {

/**
 * @brief Safely renames a file or directory.
 * 
 * This function attempts to rename a file or directory from one path to another.
 * If the operation fails, it logs an error message and throws a runtime exception.
 * 
 * @param from The current path of the file or directory.
 * @param to The new path to rename the file or directory to.
 * @throws std::runtime_error if the renaming operation fails.
 */
void safe_rename(const boost::filesystem::path& from, const boost::filesystem::path& to);

/**
 * @brief Selects files for compaction based on specific criteria.
 * 
 * This function iterates through a set of files that have reached the end of their rotation
 * and selects those that are eligible for compaction. It adds selected files to a list and
 * updates a set of detached files.
 * 
 * @param rotation_end_files A set of file paths that have reached the end of their rotation.
 * @param detached_pwals A set of filenames that are already detached and should not be selected.
 * @return A set of filenames that have been selected for compaction.
 */
std::set<std::string> select_files_for_compaction(const std::set<boost::filesystem::path>& rotation_end_files, std::set<std::string>& detached_pwals);

/**
 * @brief Ensures that a directory exists, creating it if necessary.
 * 
 * This function checks if a directory exists at the specified path. If it does not exist,
 * the function attempts to create it. If the path exists but is not a directory, or if 
 * directory creation fails, the function logs an error and throws an exception.
 * 
 * @param dir The path of the directory to check or create.
 * @throws std::runtime_error if the path exists but is not a directory, or if directory creation fails.
 */
void ensure_directory_exists(const boost::filesystem::path& dir);

/**
 * @brief Handles an existing compacted file by renaming it if necessary.
 * 
 * This function checks for the existence of a compacted file in a specified location. If a compacted
 * file already exists and a backup file does not, the function renames the compacted file to a backup
 * name. If both files exist, it logs an error and throws an exception.
 * 
 * @param location The directory path where the compacted file is located.
 * @throws std::runtime_error if both the compacted file and backup file already exist.
 */
void handle_existing_compacted_file(const boost::filesystem::path& location);

/**
 * @brief Retrieves a list of all regular files in a specified directory.
 * 
 * This function iterates over all entries in a given directory and returns a set containing
 * the names of all regular files found.
 * 
 * @param directory The path of the directory to search.
 * @return A set of strings representing the names of all regular files in the directory.
 */
std::set<std::string> get_files_in_directory(const boost::filesystem::path& directory);

/**
 * @brief Safely removes a file.
 * 
 * This function attempts to remove a file at the specified path. If the removal fails, it logs
 * an error message and throws a runtime exception.
 * 
 * @param file The path of the file to be removed.
 * @throws std::runtime_error if the file removal operation fails.
 */
void remove_file_safely(const boost::filesystem::path& file);

}  // namespace limestone::internal

#endif  // ONLINE_COMPACTION_H
