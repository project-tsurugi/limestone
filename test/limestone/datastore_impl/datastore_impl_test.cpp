#include <gtest/gtest.h>
#include <limestone/datastore_impl.h>
#include <limestone/manifest.h>

namespace limestone::api {

class datastore_impl_test : public ::testing::Test {
protected:
    datastore_impl_test() = default;
    ~datastore_impl_test() override = default;
};

TEST_F(datastore_impl_test, migration_info_getter_setter) {
    datastore_impl datastore;
    
    // Initially, migration_info should not have a value
    EXPECT_FALSE(datastore.get_migration_info().has_value());
    
    // Create a migration_info object and set it
    limestone::internal::manifest::migration_info info(5, 6);
    datastore.set_migration_info(info);
    
    // Verify that the migration_info is now set
    EXPECT_TRUE(datastore.get_migration_info().has_value());
    
    // Verify the values are correct
    auto& stored_info = datastore.get_migration_info().value();
    EXPECT_EQ(stored_info.get_old_version(), 5);
    EXPECT_EQ(stored_info.get_new_version(), 6);
    EXPECT_TRUE(stored_info.requires_rotation());
}

TEST_F(datastore_impl_test, migration_info_no_rotation_required) {
    datastore_impl datastore;
    
    // Create a migration_info that doesn't require rotation
    limestone::internal::manifest::migration_info info(6, 7);
    datastore.set_migration_info(info);
    
    // Verify the migration_info is set correctly
    EXPECT_TRUE(datastore.get_migration_info().has_value());
    auto& stored_info = datastore.get_migration_info().value();
    EXPECT_EQ(stored_info.get_old_version(), 6);
    EXPECT_EQ(stored_info.get_new_version(), 7);
    EXPECT_FALSE(stored_info.requires_rotation());
}

TEST_F(datastore_impl_test, migration_info_multiple_sets) {
    datastore_impl datastore;
    
    // Set first migration_info
    limestone::internal::manifest::migration_info info1(3, 4);
    datastore.set_migration_info(info1);
    
    EXPECT_TRUE(datastore.get_migration_info().has_value());
    EXPECT_EQ(datastore.get_migration_info().value().get_old_version(), 3);
    EXPECT_EQ(datastore.get_migration_info().value().get_new_version(), 4);
    
    // Overwrite with second migration_info
    limestone::internal::manifest::migration_info info2(7, 8);
    datastore.set_migration_info(info2);
    
    EXPECT_TRUE(datastore.get_migration_info().has_value());
    EXPECT_EQ(datastore.get_migration_info().value().get_old_version(), 7);
    EXPECT_EQ(datastore.get_migration_info().value().get_new_version(), 8);
}

TEST_F(datastore_impl_test, generate_reference_tag_deterministic_and_unique) {
    datastore_impl datastore;

    blob_id_type const blob_id1 = 100;
    blob_id_type const blob_id2 = 200;
    std::uint64_t const txid1 = 1000;
    std::uint64_t const txid2 = 2000;

    auto const tag1a = datastore.generate_reference_tag(blob_id1, txid1);
    auto const tag1b = datastore.generate_reference_tag(blob_id1, txid1);
    EXPECT_EQ(tag1a, tag1b);

    auto const tag2 = datastore.generate_reference_tag(blob_id2, txid1);
    EXPECT_NE(tag1a, tag2);

    auto const tag3 = datastore.generate_reference_tag(blob_id1, txid2);
    EXPECT_NE(tag1a, tag3);
}

} // namespace limestone::api
