#include "backup_object.h"

namespace limestone {

backup_object::backup_object(
    std::string object_id,
    backup_object_type type,
    boost::filesystem::path path
) : object_id_(std::move(object_id)), type_(type), path_(std::move(path)) {}

[[nodiscard]] const std::string& backup_object::object_id() const noexcept {
    return object_id_;
}

[[nodiscard]] backup_object_type backup_object::type() const noexcept {
    return type_;
}

[[nodiscard]] const boost::filesystem::path& backup_object::path() const noexcept {
    return path_;
}

backup_object backup_object::from_proto(const limestone::grpc::proto::BackupObject& src) {
    backup_object_type type = static_cast<backup_object_type>(src.type());
    return backup_object(src.object_id(), type, boost::filesystem::path(src.path()));
}

limestone::grpc::proto::BackupObject backup_object::to_proto() const {
    limestone::grpc::proto::BackupObject dst;
    dst.set_object_id(object_id_);
    dst.set_type(static_cast<limestone::grpc::proto::BackupObjectType>(type_));
    dst.set_path(path_.string());
    return dst;
}

} // namespace limestone
