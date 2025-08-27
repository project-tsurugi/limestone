#pragma once

#include <string>
#include <boost/filesystem.hpp>
#include "backup.pb.h"

namespace limestone {

/**
 * @brief Type of backup object.
 * Corresponds to BackupObjectType in backup.proto.
 */
enum class backup_object_type {
    unspecified = 0, /**< Unknown object type. */
    log = 1,         /**< WAL file. */
    snapshot = 2,    /**< Piece of snapshot files. */
    blob = 3,        /**< BLOB files. */
    metadata = 4,    /**< Metadata files. */
};

/**
 * @brief Backup object information.
 *
 * Represents a backup object as defined in backup.proto.
 */

class backup_object {
public:
    /**
     * @brief Construct a backup_object.
     * @param object_id Object ID.
     * @param type Object type.
     * @param path Relative path of the object, relative to the container root of its object type.
     */
    explicit backup_object(
        std::string object_id,
        backup_object_type type,
        boost::filesystem::path path
    );

    /**
     * @brief Get the object ID.
     * @return Object ID.
     */
    [[nodiscard]] const std::string& object_id() const noexcept;

    /**
     * @brief Get the object type.
     * @return Object type.
     */
    [[nodiscard]] backup_object_type type() const noexcept;

    /**
     * @brief Get the relative path of the object.
     * @return Relative path of the object, relative to the container root of its object type.
     */
    [[nodiscard]] const boost::filesystem::path& path() const noexcept;

    /**
     * @brief Create a backup_object from a proto message.
     * @param src Source proto message.
     * @return backup_object instance.
     */
    static backup_object from_proto(const limestone::grpc::proto::BackupObject& src);

    /**
     * @brief Convert this object to a proto message.
     * @return Proto message.
     */
    limestone::grpc::proto::BackupObject to_proto() const;

private:
    std::string object_id_;
    backup_object_type type_;
    boost::filesystem::path path_;
};

} // namespace limestone
