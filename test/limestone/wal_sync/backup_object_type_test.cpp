#include "wal_sync/backup_object_type.h"

#include <gtest/gtest.h>

#include <backup.pb.h>
#include <sstream>

namespace limestone::testing {

using limestone::grpc::proto::BackupObjectType;
using limestone::internal::backup_object_type;
namespace backup_helper = limestone::internal::backup_object_type_helper;

TEST(backup_object_type_test, to_string_view_returns_expected) {
    EXPECT_STREQ(backup_helper::to_string_view(backup_object_type::unspecified).data(), "unspecified");
    EXPECT_STREQ(backup_helper::to_string_view(backup_object_type::log).data(), "log");
    EXPECT_STREQ(backup_helper::to_string_view(backup_object_type::snapshot).data(), "snapshot");
    EXPECT_STREQ(backup_helper::to_string_view(backup_object_type::blob).data(), "blob");
    EXPECT_STREQ(backup_helper::to_string_view(backup_object_type::metadata).data(), "metadata");

    std::stringstream ss;
    ss << backup_object_type::blob;
    EXPECT_EQ(ss.str(), "blob");
}

TEST(backup_object_type_test, to_string_view_out_of_range_returns_unspecified) {
    auto invalid_value = static_cast<backup_object_type>(999);
    EXPECT_STREQ(backup_helper::to_string_view(invalid_value).data(), "unspecified");
}

TEST(backup_object_type_test, converts_to_proto_and_back) {
    auto roundtrip = [](backup_object_type value) {
        auto proto = backup_helper::to_proto(value);
        auto converted = backup_helper::from_proto(proto);
        EXPECT_EQ(converted, value);
    };

    roundtrip(backup_object_type::unspecified);
    roundtrip(backup_object_type::log);
    roundtrip(backup_object_type::snapshot);
    roundtrip(backup_object_type::blob);
    roundtrip(backup_object_type::metadata);
}

TEST(backup_object_type_test, from_proto_returns_unspecified_for_unknown_value) {
    auto unknown = static_cast<BackupObjectType>(999);
    EXPECT_EQ(backup_helper::from_proto(unknown), backup_object_type::unspecified);
}

TEST(backup_object_type_test, from_proto_handles_sentinel_values) {
    EXPECT_EQ(backup_helper::from_proto(BackupObjectType::BackupObjectType_INT_MIN_SENTINEL_DO_NOT_USE_),
              backup_object_type::unspecified);
    EXPECT_EQ(backup_helper::from_proto(BackupObjectType::BackupObjectType_INT_MAX_SENTINEL_DO_NOT_USE_),
              backup_object_type::unspecified);
}

} // namespace limestone::testing
