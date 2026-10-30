/*
 * Copyright 2026-2026 Project Tsurugi.
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
#include <gtest/gtest.h>
#include "datastore_impl.h"

namespace limestone::api {

// Unit tests of the live pool registry (register_live_blob_pool / unregister_live_blob_pool /
// get_live_blob_pool_min_next_blob_id). The registry uses the key only as an identifier and
// never dereferences it, so dummy addresses stand in for real pools.
class live_blob_pool_registry_test : public ::testing::Test {
protected:
    using pool_key = limestone::internal::blob_pool_impl const*;

    static pool_key key(int const& marker) {
        return reinterpret_cast<pool_key>(&marker);  // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
    }

    int marker_a_{};
    int marker_b_{};
    int marker_c_{};
};

TEST_F(live_blob_pool_registry_test, returns_argument_when_no_pool_is_live) {
    datastore_impl impl;

    EXPECT_EQ(impl.get_live_blob_pool_min_next_blob_id(0), 0);
    EXPECT_EQ(impl.get_live_blob_pool_min_next_blob_id(500), 500);
}

TEST_F(live_blob_pool_registry_test, live_pool_lowers_the_minimum) {
    datastore_impl impl;
    impl.register_live_blob_pool(key(marker_a_), 100);

    // An argument above the registered value yields the registered value
    EXPECT_EQ(impl.get_live_blob_pool_min_next_blob_id(500), 100);
    // An argument below the registered value is returned as it is (the argument takes part in the minimum)
    EXPECT_EQ(impl.get_live_blob_pool_min_next_blob_id(50), 50);
}

TEST_F(live_blob_pool_registry_test, minimum_over_multiple_pools_and_unregister) {
    datastore_impl impl;
    impl.register_live_blob_pool(key(marker_a_), 300);
    impl.register_live_blob_pool(key(marker_b_), 100);
    impl.register_live_blob_pool(key(marker_c_), 200);

    EXPECT_EQ(impl.get_live_blob_pool_min_next_blob_id(500), 100);

    // Removing the pool with the minimum moves to the next smallest value
    impl.unregister_live_blob_pool(key(marker_b_));
    EXPECT_EQ(impl.get_live_blob_pool_min_next_blob_id(500), 200);

    // Removing a pool other than the minimum leaves the minimum unchanged
    impl.unregister_live_blob_pool(key(marker_a_));
    EXPECT_EQ(impl.get_live_blob_pool_min_next_blob_id(500), 200);

    // Removing every pool returns the argument again
    impl.unregister_live_blob_pool(key(marker_c_));
    EXPECT_EQ(impl.get_live_blob_pool_min_next_blob_id(500), 500);
}

TEST_F(live_blob_pool_registry_test, unregister_of_unknown_pool_is_a_no_op) {
    datastore_impl impl;
    impl.register_live_blob_pool(key(marker_a_), 100);

    impl.unregister_live_blob_pool(key(marker_b_));
    EXPECT_EQ(impl.get_live_blob_pool_min_next_blob_id(500), 100);

    // Removing twice is harmless
    impl.unregister_live_blob_pool(key(marker_a_));
    impl.unregister_live_blob_pool(key(marker_a_));
    EXPECT_EQ(impl.get_live_blob_pool_min_next_blob_id(500), 500);

    // Re-registering the same key after its removal (address reuse) is an ordinary registration
    impl.register_live_blob_pool(key(marker_a_), 200);
    EXPECT_EQ(impl.get_live_blob_pool_min_next_blob_id(500), 200);
}

}  // namespace limestone::api
