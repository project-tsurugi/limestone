/*
 * Copyright 2022-2025 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "limestone/grpc/backend/backup_object.h"
#include <gtest/gtest.h>

namespace limestone::testing {

using limestone::grpc::backend::backup_object;
using limestone::grpc::backend::backup_object_type;

class backup_object_test : public ::testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(backup_object_test, construct_and_getters) {
    std::string id = "obj1";
    backup_object_type type = backup_object_type::log;
    boost::filesystem::path path = "foo/bar";
    backup_object obj(id, type, path);
    ASSERT_EQ(obj.object_id(), id);
    ASSERT_EQ(obj.type(), type);
    ASSERT_EQ(obj.path(), path);
}

TEST_F(backup_object_test, proto_conversion) {
    std::string id = "obj2";
    backup_object_type type = backup_object_type::snapshot;
    boost::filesystem::path path = "snap/path";
    backup_object obj(id, type, path);
    auto proto = obj.to_proto();
    ASSERT_EQ(proto.object_id(), id);
    ASSERT_EQ(proto.type(), limestone::grpc::proto::BackupObjectType::SNAPSHOT);
    ASSERT_EQ(proto.path(), path.string());

    auto obj2 = backup_object::from_proto(proto);
    ASSERT_EQ(obj2.object_id(), id);
    ASSERT_EQ(obj2.type(), type);
    ASSERT_EQ(obj2.path(), path);
}

TEST_F(backup_object_test, backup_object_type_enum_matches_proto) {
    using proto_type = limestone::grpc::proto::BackupObjectType;
    ASSERT_EQ(static_cast<int>(backup_object_type::unspecified), static_cast<int>(proto_type::UNSPECIFIED));
    ASSERT_EQ(static_cast<int>(backup_object_type::log), static_cast<int>(proto_type::LOG));
    ASSERT_EQ(static_cast<int>(backup_object_type::snapshot), static_cast<int>(proto_type::SNAPSHOT));
    ASSERT_EQ(static_cast<int>(backup_object_type::blob), static_cast<int>(proto_type::BLOB));
    ASSERT_EQ(static_cast<int>(backup_object_type::metadata), static_cast<int>(proto_type::METADATA));
}

} // namespace limestone::testing
