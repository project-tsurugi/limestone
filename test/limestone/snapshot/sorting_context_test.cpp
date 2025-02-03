#include <gtest/gtest.h>
#include <vector>
#include <cstdint>
#include <algorithm>
#include "sorting_context.h"


namespace limestone::testing {

using internal::sorting_context;
using api::storage_id_type;
using api::blob_id_type;
using api::write_version_type;
using api::sortdb_wrapper;

TEST(sorting_context_test, update_max_blob_id) {
    sorting_context ctx;

    // Verify that the initial state is 0
    EXPECT_EQ(ctx.get_max_blob_id(), 0);

    // Create a vector for updating and test updating the maximum value
    std::vector<blob_id_type> blob_ids_1 = {10, 20, 5};
    ctx.update_max_blob_id(blob_ids_1);
    // The new maximum value is 20
    EXPECT_EQ(ctx.get_max_blob_id(), 20);

    // Update with a vector containing values larger than the existing maximum
    std::vector<blob_id_type> blob_ids_2 = {15, 25};
    ctx.update_max_blob_id(blob_ids_2);
    // The maximum value is updated to 25
    EXPECT_EQ(ctx.get_max_blob_id(), 25);

    // When only values smaller than the existing maximum are present, it is not updated
    std::vector<blob_id_type> blob_ids_3 = {3, 7, 24};
    ctx.update_max_blob_id(blob_ids_3);
    // The maximum value remains 25
    EXPECT_EQ(ctx.get_max_blob_id(), 25);

    // Verify that nothing is updated with an empty vector
    std::vector<blob_id_type> empty_blob_ids;
    ctx.update_max_blob_id(empty_blob_ids);
    EXPECT_EQ(ctx.get_max_blob_id(), 25);
}

TEST(sorting_context_test, clear_storage_update_and_find) {
    sorting_context ctx;
    storage_id_type sid = 1;
    write_version_type wv_initial = {100, 4};
    write_version_type wv_updated = {150, 4};


    // First update: set the initial write_version for sid
    ctx.clear_storage_update(sid, wv_initial);
    auto opt_wv = ctx.clear_storage_find(sid);
    ASSERT_TRUE(opt_wv.has_value());
    EXPECT_EQ(opt_wv.value(), wv_initial);

    // Update with a larger value for the same sid: the update should be reflected
    ctx.clear_storage_update(sid, wv_updated);
    opt_wv = ctx.clear_storage_find(sid);
    ASSERT_TRUE(opt_wv.has_value());
    EXPECT_EQ(opt_wv.value(), wv_updated);

    // Even if a smaller value is passed for the same sid, it should not be updated
    ctx.clear_storage_update(sid, {120, 4});
    opt_wv = ctx.clear_storage_find(sid);
    ASSERT_TRUE(opt_wv.has_value());
    EXPECT_EQ(opt_wv.value(), wv_updated);
}

TEST(sorting_context_test, get_clear_storage_returns_map) {
    sorting_context ctx;
    storage_id_type sid1 = 1;
    storage_id_type sid2 = 2;
    write_version_type wv1 = {100, 4};
    write_version_type wv2 = {150, 4};

    ctx.clear_storage_update(sid1, wv1);
    ctx.clear_storage_update(sid2, wv2);

    auto storage_map = ctx.get_clear_storage();
    ASSERT_EQ(storage_map.size(), 2);
    EXPECT_EQ(storage_map[sid1], wv1);
    EXPECT_EQ(storage_map[sid2], wv2);
}

TEST(sorting_context_test, get_sortdb_default) {
    sorting_context ctx;
    // When generated with the default constructor, sortdb should be nullptr
    EXPECT_EQ(ctx.get_sortdb(), nullptr);
}

}  // namespace limestone::testinge