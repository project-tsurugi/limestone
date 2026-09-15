
#include "internal.h"

#include "test_root.h"

namespace limestone::testing {

using limestone::internal::is_unrotated_pwal_name;

TEST(unrotated_pwal_name_test, matches_only_unrotated_pwal_names) { // NOLINT
    EXPECT_TRUE(is_unrotated_pwal_name("pwal_0000"));
    EXPECT_TRUE(is_unrotated_pwal_name("pwal_9999"));
    EXPECT_FALSE(is_unrotated_pwal_name("pwal_0000.00000000000001.0"));  // rotated naming
    EXPECT_FALSE(is_unrotated_pwal_name("pwal_000"));    // too short
    EXPECT_FALSE(is_unrotated_pwal_name("pwal_00000"));  // too long
    EXPECT_FALSE(is_unrotated_pwal_name("pwal0000"));    // prefix "pwal_" does not match
    EXPECT_FALSE(is_unrotated_pwal_name("qwal_0000"));
    EXPECT_FALSE(is_unrotated_pwal_name("epoch"));
    EXPECT_FALSE(is_unrotated_pwal_name(""));
}

}  // namespace limestone::testing
