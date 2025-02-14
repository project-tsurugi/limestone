#include "blob_id_container.h" 
#include <gtest/gtest.h>
#include <vector>

namespace limestone::testing {

using namespace limestone::internal;
using namespace limestone::api;

// Helper function to extract all blob_id values from a blob_id_container.
std::vector<blob_id_type> get_blob_ids(const blob_id_container &container) {
    std::vector<blob_id_type> ids;
    for (const auto &id : container) {
        ids.push_back(id);
    }
    return ids;
}

TEST(blob_id_container_test, add_and_iteration) {
    // Create a container and add unsorted blob IDs.
    blob_id_container container;
    container.add_blob_id(3);
    container.add_blob_id(1);
    container.add_blob_id(2);

    // Calling begin() sorts the container.
    std::vector<blob_id_type> result = get_blob_ids(container);

    // Expected sorted order: 1, 2, 3.
    std::vector<blob_id_type> expected {1, 2, 3};
    EXPECT_EQ(result, expected);
}

TEST(blob_id_container_test, add_and_iteration_empty) {
    // Create an empty container.
    blob_id_container container;

    // For an empty container, begin() and end() should be equal.
    auto it = container.begin();
    EXPECT_EQ(it, container.end());
}

TEST(blob_id_container_test, add_and_iteration_single) {
    // Create a container and add a single blob ID.
    blob_id_container container;
    container.add_blob_id(42);

    // Retrieve blob IDs using an iterator.
    std::vector<blob_id_type> result = get_blob_ids(container);

    // Expected order is a single element: 42.
    std::vector<blob_id_type> expected {42};
    EXPECT_EQ(result, expected);
}

TEST(blob_id_container_test, diff_removes_matching_items) {
    // Create a container with blob IDs: 1, 2, 3, 4.
    blob_id_container container;
    container.add_blob_id(1);
    container.add_blob_id(2);
    container.add_blob_id(3);
    container.add_blob_id(4);

    // Create another container with blob IDs: 2, 4.
    blob_id_container other;
    other.add_blob_id(2);
    other.add_blob_id(4);

    // Execute diff: remove blob IDs present in the other container.
    container.diff(other);

    // After diff, container should contain blob IDs: 1 and 3.
    std::vector<blob_id_type> result = get_blob_ids(container);
    std::vector<blob_id_type> expected {1, 3};
    EXPECT_EQ(result, expected);
}

TEST(blob_id_container_test, diff_with_our_container_empty) {
    // Our container is empty.
    blob_id_container container;
    
    // Other container has some blob IDs.
    blob_id_container other;
    other.add_blob_id(1);
    other.add_blob_id(2);
    
    // Performing diff should leave our container empty.
    container.diff(other);
    
    EXPECT_EQ(container.begin(), container.end());
}

TEST(blob_id_container_test, diff_with_other_container_empty) {
    // Our container has some blob IDs.
    blob_id_container container;
    container.add_blob_id(5);
    container.add_blob_id(3);
    
    // Other container is empty.
    blob_id_container other;
    
    // Performing diff should leave our container unchanged.
    container.diff(other);
    
    // Expected sorted order: 3, 5.
    std::vector<blob_id_type> result = get_blob_ids(container);
    std::vector<blob_id_type> expected {3, 5}; 
    EXPECT_EQ(result, expected);
}

TEST(blob_id_container_test, diff_with_both_empty) {
    // Both containers are empty.
    blob_id_container container;
    blob_id_container other;
    
    // Diff on empty containers should remain empty.
    container.diff(other);
    
    EXPECT_EQ(container.begin(), container.end());
}

TEST(blob_id_container_test, diff_when_other_contains_all_our_items_and_more) {
    // Our container has blob IDs.
    blob_id_container container;
    container.add_blob_id(2);
    container.add_blob_id(4);
    container.add_blob_id(6);

    // Other container has all our blob IDs plus additional ones.
    blob_id_container other;
    other.add_blob_id(1);  // extra
    other.add_blob_id(2);  // common
    other.add_blob_id(3);  // extra
    other.add_blob_id(4);  // common
    other.add_blob_id(5);  // extra
    other.add_blob_id(6);  // common
    other.add_blob_id(7);  // extra

    // Execute diff: all items in our container are present in the other container,
    // so our container should become empty.
    container.diff(other);

    EXPECT_EQ(container.begin(), container.end());
}

TEST(blob_id_container_test, diff_with_self_removes_all) {
    // Create a container and add several blob IDs.
    blob_id_container container;
    container.add_blob_id(10);
    container.add_blob_id(20);
    container.add_blob_id(30);

    // Calling diff with itself should remove all items.
    container.diff(container);

    // Verify that the container is empty.
    EXPECT_EQ(container.begin(), container.end());
}

TEST(blob_id_container_test, diff_between_containers_with_same_items) {
    // Create container_a with blob IDs: 1, 2, 3.
    blob_id_container container_a;
    container_a.add_blob_id(1);
    container_a.add_blob_id(2);
    container_a.add_blob_id(3);

    // Create container_b with the same blob IDs.
    blob_id_container container_b;
    container_b.add_blob_id(1);
    container_b.add_blob_id(2);
    container_b.add_blob_id(3);

    // Execute diff: container_a should become empty.
    container_a.diff(container_b);

    EXPECT_EQ(container_a.begin(), container_a.end());
}

TEST(blob_id_container_test, diff_partial_overlap) {
    // Our container: {1, 2, 3, 4}
    blob_id_container container;
    container.add_blob_id(1);
    container.add_blob_id(2);
    container.add_blob_id(3);
    container.add_blob_id(4);

    // Other container: {2, 4, 5}
    blob_id_container other;
    other.add_blob_id(2);
    other.add_blob_id(4);
    other.add_blob_id(5);

    // Execute diff: remove blob IDs present in the other container.
    container.diff(other);

    // Expected result: {1, 3}
    std::vector<blob_id_type> result = get_blob_ids(container);
    std::vector<blob_id_type> expected {1, 3};
    EXPECT_EQ(result, expected);
}

TEST(blob_id_container_test, diff_with_duplicates_in_our_container) {
    // Our container: {1, 1, 2, 3}
    blob_id_container container;
    container.add_blob_id(1);
    container.add_blob_id(1);
    container.add_blob_id(2);
    container.add_blob_id(3);

    // Other container: {1}
    blob_id_container other;
    other.add_blob_id(1);

    // Execute diff: all occurrences of 1 should be removed.
    container.diff(other);

    // Expected result: {2, 3}
    std::vector<blob_id_type> result = get_blob_ids(container);
    std::vector<blob_id_type> expected {2, 3};
    EXPECT_EQ(result, expected);
}

TEST(blob_id_container_test, diff_with_no_overlap) {
    // Our container: {1, 2, 3}
    blob_id_container container;
    container.add_blob_id(1);
    container.add_blob_id(2);
    container.add_blob_id(3);

    // Other container: {4, 5}
    blob_id_container other;
    other.add_blob_id(4);
    other.add_blob_id(5);

    // Execute diff: no overlapping blob IDs, so container remains unchanged.
    container.diff(other);

    // Expected result: {1, 2, 3}
    std::vector<blob_id_type> result = get_blob_ids(container);
    std::vector<blob_id_type> expected {1, 2, 3};
    EXPECT_EQ(result, expected);
}

TEST(blob_id_container_test, diff_called_multiple_times) {
    // Our container: {1, 2, 3, 4}
    blob_id_container container;
    container.add_blob_id(1);
    container.add_blob_id(2);
    container.add_blob_id(3);
    container.add_blob_id(4);

    // Other container: {2, 4}
    blob_id_container other;
    other.add_blob_id(2);
    other.add_blob_id(4);

    // Call diff twice before obtaining an iterator.
    container.diff(other);
    container.diff(other);

    // Verify the result.
    std::vector<blob_id_type> result = get_blob_ids(container);
    std::vector<blob_id_type> expected {1, 3};
    EXPECT_EQ(result, expected);
}

TEST(blob_id_container_test, diff_preserves_sorted_order) {
    // Our container: {4, 1, 3, 2} in unsorted order.
    blob_id_container container;
    container.add_blob_id(4);
    container.add_blob_id(1);
    container.add_blob_id(3);
    container.add_blob_id(2);

    // Diff triggers sorting even if no deletion occurs.
    blob_id_container other;
    container.diff(other);

    // Expected sorted order: {1, 2, 3, 4}
    std::vector<blob_id_type> result = get_blob_ids(container);
    std::vector<blob_id_type> expected {1, 2, 3, 4};
    EXPECT_EQ(result, expected);
}

TEST(blob_id_container_test, merge_adds_items) {
    // Create an initially empty container.
    blob_id_container container;

    // Create container1 with blob IDs: 5, 3.
    blob_id_container container1;
    container1.add_blob_id(5);
    container1.add_blob_id(3);

    // Create container2 with blob IDs: 7, 1.
    blob_id_container container2;
    container2.add_blob_id(7);
    container2.add_blob_id(1);

    // Merge container1 and container2 into container.
    container.merge(container1);
    container.merge(container2);

    // Expected result after merge: {1, 3, 5, 7} in sorted order.
    std::vector<blob_id_type> result = get_blob_ids(container);
    std::vector<blob_id_type> expected {1, 3, 5, 7};
    EXPECT_EQ(result, expected);
}

TEST(blob_id_container_test, merge_with_no_overlap) {
    // Our container initially has: {3, 5}.
    blob_id_container container;
    container.add_blob_id(3);
    container.add_blob_id(5);
    
    // Other container has: {1, 2}.
    blob_id_container other;
    other.add_blob_id(1);
    other.add_blob_id(2);
    
    // Merge other into our container.
    container.merge(other);
    
    // Expected result: {1, 2, 3, 5} in sorted order.
    std::vector<blob_id_type> result = get_blob_ids(container);
    std::vector<blob_id_type> expected {1, 2, 3, 5};
    EXPECT_EQ(result, expected);
}

TEST(blob_id_container_test, merge_with_overlap) {
    // Our container initially has: {1, 3}.
    blob_id_container container;
    container.add_blob_id(1);
    container.add_blob_id(3);
    
    // Other container has: {1, 2}.
    blob_id_container other;
    other.add_blob_id(1);
    other.add_blob_id(2);
    
    // Merge other into our container.
    container.merge(other);
    
    // Expected result: raw merge gives {1, 3, 1, 2} then sorted to {1, 1, 2, 3}.
    std::vector<blob_id_type> result = get_blob_ids(container);
    std::vector<blob_id_type> expected {1, 1, 2, 3};
    EXPECT_EQ(result, expected);
}

TEST(blob_id_container_test, merge_with_other_empty) {
    // Our container initially has: {4, 2} (unsorted).
    blob_id_container container;
    container.add_blob_id(4);
    container.add_blob_id(2);
    
    // Other container is empty.
    blob_id_container empty;
    
    // Merge empty container into our container.
    container.merge(empty);
    
    // Expected result: {2, 4} in sorted order.
    std::vector<blob_id_type> result = get_blob_ids(container);
    std::vector<blob_id_type> expected {2, 4};
    EXPECT_EQ(result, expected);
}

TEST(blob_id_container_test, merge_with_our_empty) {
    // Our container is empty.
    blob_id_container container;

    // Other container has: {3, 1, 2}.
    blob_id_container other;
    other.add_blob_id(3);
    other.add_blob_id(1);
    other.add_blob_id(2);

    // Merge other into our container.
    container.merge(other);

    // Expected result: {1, 2, 3} in sorted order.
    std::vector<blob_id_type> result = get_blob_ids(container);
    std::vector<blob_id_type> expected {1, 2, 3};
    EXPECT_EQ(result, expected);
}

TEST(blob_id_container_test, merge_with_both_empty) {
    // Both our container and the other container are empty.
    blob_id_container container;
    blob_id_container other;

    // Merge other into our container.
    container.merge(other);

    // Expected result: an empty container.
    EXPECT_EQ(container.begin(), container.end());
}

TEST(blob_id_container_test, modification_after_iterator_throws) {
    blob_id_container container;
    container.add_blob_id(1);

    // Once an iterator is obtained, the container becomes read-only.
    auto it = container.begin();

    // Subsequent modification operations should throw std::logic_error.
    EXPECT_THROW(container.add_blob_id(2), std::logic_error);
    EXPECT_THROW(container.diff(container), std::logic_error);

    // Create a dummy container for merge() argument.
    blob_id_container dummy;
    dummy.add_blob_id(3);
    EXPECT_THROW(container.merge(dummy), std::logic_error);
}

}  // namespace limestone::testing
