#include "online_compaction.h"

#include <glog/logging.h>
#include <limestone/api/compaction_catalog.h>
#include <limestone/logging.h>

#include <boost/filesystem.hpp>
#include "logging_helper.h"

namespace limestone::internal {

using namespace limestone::api;    

void safe_rename(const boost::filesystem::path& from, const boost::filesystem::path& to) {
    boost::system::error_code error;
    boost::filesystem::rename(from, to, error);
    if (error) {
        LOG_LP(ERROR) << "fail to rename file: error_code: " << error << ", from: " << from << ", to: " << to;
        throw std::runtime_error("fail to rename the file");
    }
}

std::set<std::string> select_files_for_compaction(const std::set<boost::filesystem::path>& rotation_end_files, std::set<std::string>& detached_pwals) {
    std::set<std::string> need_compaction_filenames;
    for (const boost::filesystem::path& path : rotation_end_files) {
        std::string filename = path.filename().string();
        if (filename.substr(0, 4) == "pwal" && filename.length() > 9 && detached_pwals.find(filename) == detached_pwals.end()) {
            need_compaction_filenames.insert(filename);
            detached_pwals.insert(filename);
            LOG_LP(INFO) << "Selected file for compaction: " << filename;
        } else {
            LOG_LP(INFO) << "File skipped for compaction: " << filename << " (Reason: " 
                         << (filename.substr(0, 4) != "pwal" ? "does not start with 'pwal'" :
                            filename.length() <= 9 ? "filename length is 9 or less" : 
                            "file is already detached") << ")";
        }
    }
    return need_compaction_filenames;
}

void ensure_directory_exists(const boost::filesystem::path& dir) {
    if (boost::filesystem::exists(dir)) {
        if (!boost::filesystem::is_directory(dir)) {
            LOG_LP(ERROR) << "the path exists but is not a directory: " << dir;
            throw std::runtime_error("The path exists but is not a directory: " + dir.string());
        }
    } else {
        boost::system::error_code error;
        const bool result_mkdir = boost::filesystem::create_directory(dir, error);
        if (!result_mkdir || error) {
            LOG_LP(ERROR) << "failed to create directory: result_mkdir: " << result_mkdir << ", error_code: " << error << ", path: " << dir;
            throw std::runtime_error("Failed to create the directory");
        }
    }
}

void handle_existing_compacted_file(const boost::filesystem::path& location) {
    boost::filesystem::path compacted_file = location / compaction_catalog::get_compacted_filename();
    boost::filesystem::path compacted_prev_file = location / compaction_catalog::get_compacted_backup_filename();

    if (boost::filesystem::exists(compacted_file)) {
        if (boost::filesystem::exists(compacted_prev_file)) {
            LOG_LP(ERROR) << "the file already exists: " << compacted_prev_file;
            throw std::runtime_error("The file already exists: " + compacted_prev_file.string());
        }
        safe_rename(compacted_file, compacted_prev_file);
    }
}

std::set<std::string> get_files_in_directory(const boost::filesystem::path& directory) {
    std::set<std::string> files;
    boost::system::error_code error; 

    if (!boost::filesystem::exists(directory, error)) {
        LOG_LP(ERROR) << "Directory does not exist: " << directory << ", error_code: " << error.message();
        throw std::runtime_error("Directory does not exist: " + directory.string());
    }

    if (!boost::filesystem::is_directory(directory, error)) {
        LOG_LP(ERROR) << "The path exists but is not a directory: " << directory << ", error_code: " << error.message();
        throw std::runtime_error("The path exists but is not a directory: " + directory.string());
    }

    for (boost::filesystem::directory_iterator it(directory, error), end; it != end && !error; it.increment(error)) {
        if (boost::filesystem::is_regular_file(it->path(), error)) {
            files.insert(it->path().filename().string());
        }
    }

    if (error) {
        LOG_LP(ERROR) << "Error while iterating directory: " << directory << ", error_code: " << error.message();
        throw std::runtime_error("Error while iterating directory: " + directory.string());
    }

    return files;
}


void remove_file_safely(const boost::filesystem::path& file) {
    boost::system::error_code error;
    boost::filesystem::remove(file, error);
    if (error) {
        LOG_LP(ERROR) << "failed to remove file: error_code: " << error << ", path: " << file;
        throw std::runtime_error("Failed to remove the file");
    }
}

}