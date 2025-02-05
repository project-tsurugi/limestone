#include "blob_item_container.h"
#include <gtest/gtest.h>
#include <vector>

namespace limestone::testing {

using namespace limestone::api;

// Helper function to extract all blob_id values from a blob_item_container.
std::vector<blob_id_type> get_blob_ids(const limestone::api::blob_item_container &container) {
    std::vector<blob_id_type> ids;
    for (const auto &item : container) {
        ids.push_back(item.get_blob_id());
    }
    return ids;
}



TEST(blob_item_container_test, add_and_iteration) {
    // Create container and add blob_items with unsorted blob IDs.
    blob_item_container container;
    container.add_blob_item(blob_item(3));
    container.add_blob_item(blob_item(1));
    container.add_blob_item(blob_item(2));

    // When begin() is called, container is sorted.
    std::vector<blob_id_type> result = get_blob_ids(container);

    // Expected sorted order: 1, 2, 3.
    std::vector<blob_id_type> expected {1, 2, 3};
    EXPECT_EQ(result, expected);
}

TEST(blob_item_container_test, add_and_iteration_empty) {
    // Create an empty container and do not call add_blob_item.
    blob_item_container container;

    // Begin and end should be equal for an empty container.
    auto it = container.begin();
    EXPECT_EQ(it, container.end());
}

TEST(blob_item_container_test, add_and_iteration_single) {
    // Create container and add a single blob_item.
    blob_item_container container;
    container.add_blob_item(blob_item(42));

    // Obtain an iterator and collect blob IDs.
    std::vector<blob_id_type> result = get_blob_ids(container);

    // Expected sorted order is a single element: 42.
    std::vector<blob_id_type> expected {42};
    EXPECT_EQ(result, expected);
}


TEST(blob_item_container_test, diff_removes_matching_items) {
    // Create a container with blob IDs: 1, 2, 3, 4.
    blob_item_container container;
    container.add_blob_item(blob_item(1));
    container.add_blob_item(blob_item(2));
    container.add_blob_item(blob_item(3));
    container.add_blob_item(blob_item(4));

    // Create an 'other' container with blob IDs: 2, 4.
    blob_item_container other;
    other.add_blob_item(blob_item(2));
    other.add_blob_item(blob_item(4));

    // Execute diff: remove from container all items that are present in other.
    container.diff(other);

    // After diff, container should contain blob IDs: 1 and 3.
    std::vector<blob_id_type> result = get_blob_ids(container);

    std::vector<blob_id_type> expected {1, 3};
    EXPECT_EQ(result, expected);
}

TEST(blob_item_container_test, diff_with_our_container_empty) {
    // Our container is empty.
    blob_item_container container;
    
    // Other container has some items.
    blob_item_container other;
    other.add_blob_item(blob_item(1));
    other.add_blob_item(blob_item(2));
    
    // Perform diff: since our container is empty, result remains empty.
    container.diff(other);
    
    EXPECT_EQ(container.begin(), container.end());
}

TEST(blob_item_container_test, diff_with_other_container_empty) {
    // Our container has some items.
    blob_item_container container;
    container.add_blob_item(blob_item(5));
    container.add_blob_item(blob_item(3));
    
    // Other container is empty.
    blob_item_container other;
    
    // Perform diff: since other is empty, none of our items match, so container remains unchanged.
    container.diff(other);
    
    // After diff, the container should still contain the original items in sorted order.
    std::vector<blob_id_type> result = get_blob_ids(container);
    std::vector<blob_id_type> expected {3, 5}; 
    EXPECT_EQ(result, expected);
}

TEST(blob_item_container_test, diff_with_both_empty) {
    // Both containers are empty.
    blob_item_container container;
    blob_item_container other;
    
    // Perform diff: result should remain empty.
    container.diff(other);
    
    EXPECT_EQ(container.begin(), container.end());
}

TEST(blob_item_container_test, diff_when_other_contains_all_our_items_and_more) {
    // Our container has some items.
    blob_item_container container;
    container.add_blob_item(blob_item(2));
    container.add_blob_item(blob_item(4));
    container.add_blob_item(blob_item(6));

    // Other container has all items of our container plus additional items.
    blob_item_container other;
    other.add_blob_item(blob_item(1));  // extra
    other.add_blob_item(blob_item(2));  // common
    other.add_blob_item(blob_item(3));  // extra
    other.add_blob_item(blob_item(4));  // common
    other.add_blob_item(blob_item(5));  // extra
    other.add_blob_item(blob_item(6));  // common
    other.add_blob_item(blob_item(7));  // extra

    // Execute diff: all items in 'container' are present in 'other', so container should be empty.
    container.diff(other);

    EXPECT_EQ(container.begin(), container.end());
}


TEST(blob_item_container_test, diff_with_self_removes_all) {
    // Create a container and add several blob_items.
    blob_item_container container;
    container.add_blob_item(blob_item(10));
    container.add_blob_item(blob_item(20));
    container.add_blob_item(blob_item(30));

    // Call diff with the same container.
    // According to the specification, since every item in 'container' is present in itself,
    // diff() should remove all items, resulting in an empty container.
    container.diff(container);

    // Verify that the container is empty.
    EXPECT_EQ(container.begin(), container.end());
}

TEST(blob_item_container_test, diff_between_containers_with_same_items) {
    // Create container_a with blob IDs: 1, 2, 3.
    blob_item_container container_a;
    container_a.add_blob_item(blob_item(1));
    container_a.add_blob_item(blob_item(2));
    container_a.add_blob_item(blob_item(3));

    // Create container_b with the same blob IDs: 1, 2, 3 (order may be arbitrary).
    blob_item_container container_b;
    container_b.add_blob_item(blob_item(1));
    container_b.add_blob_item(blob_item(2));
    container_b.add_blob_item(blob_item(3));

    // Execute diff: Since container_b contains all items in container_a,
    // container_a should become empty.
    container_a.diff(container_b);

    // Verify that container_a is empty.
    EXPECT_EQ(container_a.begin(), container_a.end());
}

TEST(blob_item_container_test, diff_partial_overlap) {
    // our container: {1, 2, 3, 4}
    blob_item_container container;
    container.add_blob_item(blob_item(1));
    container.add_blob_item(blob_item(2));
    container.add_blob_item(blob_item(3));
    container.add_blob_item(blob_item(4));

    // other container: {2, 4, 5}
    blob_item_container other;
    other.add_blob_item(blob_item(2));
    other.add_blob_item(blob_item(4));
    other.add_blob_item(blob_item(5));

    // Execute diff: Remove items present in other.
    container.diff(other);

    // Expect: {1, 3}
    std::vector<blob_id_type> result = get_blob_ids(container);
    std::vector<blob_id_type> expected {1, 3};
    EXPECT_EQ(result, expected);
}

TEST(blob_item_container_test, diff_with_duplicates_in_our_container) {
    // our container: {1, 1, 2, 3}
    blob_item_container container;
    container.add_blob_item(blob_item(1));
    container.add_blob_item(blob_item(1));
    container.add_blob_item(blob_item(2));
    container.add_blob_item(blob_item(3));

    // other container: {1}
    blob_item_container other;
    other.add_blob_item(blob_item(1));

    // Execute diff: All occurrences of blob_id 1 should be removed.
    container.diff(other);

    // Expect: {2, 3}
    std::vector<blob_id_type> result = get_blob_ids(container);
    std::vector<blob_id_type> expected {2, 3};
    EXPECT_EQ(result, expected);
}

TEST(blob_item_container_test, diff_with_no_overlap) {
    // our container: {1, 2, 3}
    blob_item_container container;
    container.add_blob_item(blob_item(1));
    container.add_blob_item(blob_item(2));
    container.add_blob_item(blob_item(3));

    // other container: {4, 5}
    blob_item_container other;
    other.add_blob_item(blob_item(4));
    other.add_blob_item(blob_item(5));

    // Execute diff: Since no items overlap, container remains unchanged.
    container.diff(other);

    // Expect: {1, 2, 3}
    std::vector<blob_id_type> result = get_blob_ids(container);
    std::vector<blob_id_type> expected {1, 2, 3};
    EXPECT_EQ(result, expected);
}

TEST(blob_item_container_test, diff_called_multiple_times) {
    // our container: {1, 2, 3, 4}
    blob_item_container container;
    container.add_blob_item(blob_item(1));
    container.add_blob_item(blob_item(2));
    container.add_blob_item(blob_item(3));
    container.add_blob_item(blob_item(4));

    // other container: {2, 4}
    blob_item_container other;
    other.add_blob_item(blob_item(2));
    other.add_blob_item(blob_item(4));

    // Call diff twice before any iterator is obtained.
    container.diff(other);
    container.diff(other);

    // Now obtain the iterator and verify the result.
    std::vector<blob_id_type> result = get_blob_ids(container);
    std::vector<blob_id_type> expected {1, 3};
    EXPECT_EQ(result, expected);
}

TEST(blob_item_container_test, diff_preserves_sorted_order) {
    // our container: {4, 1, 3, 2} (unsorted order)
    blob_item_container container;
    container.add_blob_item(blob_item(4));
    container.add_blob_item(blob_item(1));
    container.add_blob_item(blob_item(3));
    container.add_blob_item(blob_item(2));

    // other container is empty, so diff triggers sort but no removals.
    blob_item_container other;
    container.diff(other);

    // Expect sorted order: {1, 2, 3, 4}
    std::vector<blob_id_type> result = get_blob_ids(container);
    std::vector<blob_id_type> expected {1, 2, 3, 4};
    EXPECT_EQ(result, expected);
}


TEST(blob_item_container_test, merge_adds_items) {
    // Create an initially empty container.
    blob_item_container container;

    // Create first container with blob IDs: 5, 3.
    blob_item_container container1;
    container1.add_blob_item(blob_item(5));
    container1.add_blob_item(blob_item(3));

    // Create second container with blob IDs: 7, 1.
    blob_item_container container2;
    container2.add_blob_item(blob_item(7));
    container2.add_blob_item(blob_item(1));

    // Merge container1 and container2 into container.
    container.merge(container1);
    container.merge(container2);

    // After merge, container should contain blob IDs: 1, 3, 5, 7 (sorted).
    std::vector<blob_id_type> result = get_blob_ids(container);
    std::vector<blob_id_type> expected {1, 3, 5, 7};
    EXPECT_EQ(result, expected);
}

TEST(blob_item_container_test, merge_with_no_overlap) {
    // our container initially has: {3, 5}
    blob_item_container container;
    container.add_blob_item(blob_item(3));
    container.add_blob_item(blob_item(5));
    
    // other container has: {1, 2}
    blob_item_container other;
    other.add_blob_item(blob_item(1));
    other.add_blob_item(blob_item(2));
    
    // Merge other into container.
    container.merge(other);
    
    // Expect: {1, 2, 3, 5} (sorted order)
    std::vector<blob_id_type> result = get_blob_ids(container);
    std::vector<blob_id_type> expected {1, 2, 3, 5};
    EXPECT_EQ(result, expected);
}

TEST(blob_item_container_test, merge_with_overlap) {
    // our container initially has: {1, 3}
    blob_item_container container;
    container.add_blob_item(blob_item(1));
    container.add_blob_item(blob_item(3));
    
    // other container has: {1, 2}
    blob_item_container other;
    other.add_blob_item(blob_item(1));
    other.add_blob_item(blob_item(2));
    
    // Merge other into container.
    container.merge(other);
    
    // Expected result:
    // our container had {1, 3}, other has {1, 2} → merged raw: {1, 3, 1, 2},
    // then sorted: {1, 1, 2, 3}.
    std::vector<blob_id_type> result = get_blob_ids(container);
    std::vector<blob_id_type> expected {1, 1, 2, 3};
    EXPECT_EQ(result, expected);
}

TEST(blob_item_container_test, merge_with_other_empty) {
    // our container initially has: {4, 2} (unsorted)
    blob_item_container container;
    container.add_blob_item(blob_item(4));
    container.add_blob_item(blob_item(2));
    
    // other container is empty.
    blob_item_container empty;
    
    // Merge empty container into our container.
    container.merge(empty);
    
    // Expect: {2, 4} (sorted order, same as original)
    std::vector<blob_id_type> result = get_blob_ids(container);
    std::vector<blob_id_type> expected {2, 4};
    EXPECT_EQ(result, expected);
}

TEST(blob_item_container_test, merge_with_our_empty) {
    // our container is empty.
    blob_item_container container;

    // other container has: {3, 1, 2}
    blob_item_container other;
    other.add_blob_item(blob_item(3));
    other.add_blob_item(blob_item(1));
    other.add_blob_item(blob_item(2));

    // Merge other into our container.
    container.merge(other);

    // Expect: {1, 2, 3} (sorted)
    std::vector<blob_id_type> result = get_blob_ids(container);
    std::vector<blob_id_type> expected {1, 2, 3};
    EXPECT_EQ(result, expected);
}

TEST(blob_item_container_test, merge_with_both_empty) {
    // Both our container and other container are empty.
    blob_item_container container;
    blob_item_container other;

    // Merge other into our container.
    container.merge(other);

    // Expect: empty container.
    EXPECT_EQ(container.begin(), container.end());
}


TEST(blob_item_container_test, modification_after_iterator_throws) {
    blob_item_container container;
    container.add_blob_item(blob_item(1));

    // Obtain an iterator; this locks the container for modifications.
    auto it = container.begin();

    // Subsequent modification operations should throw std::logic_error.
    EXPECT_THROW(container.add_blob_item(blob_item(2)), std::logic_error);
    EXPECT_THROW(container.diff(container), std::logic_error);

    // Create a dummy container to use as the argument for merge().
    blob_item_container dummy;
    dummy.add_blob_item(blob_item(3));
    EXPECT_THROW(container.merge(dummy), std::logic_error);
}

}  // namespace limestone::testing
