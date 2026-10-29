/*
 * Copyright 2025-2026 Project Tsurugi.
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
#include <cstdio>
#include <fstream>
#include <cstdlib>
#include <string>
#include <stdexcept>
#include <vector>
#include <boost/filesystem.hpp>

#include "log_entry.h"
#include "blob_gc_entry.h"
#include "log_entry/log_entry_format.h"
#include "blob_gc_entry_container.h"
#include "blob_gc_entry_comparator.h"

// Use the namespaces defined for the API and internal classes.
using namespace limestone::api;
using namespace limestone::internal;

namespace limestone {
namespace testing {

class blob_gc_entry_container_test : public ::testing::Test {
protected:
    // creates a blob_gc_entry from a view of a normal_with_blob
    static blob_gc_entry create_blob_gc_entry(storage_id_type storage,
                                              const std::string &key,
                                              const write_version_type &wversion) {
        return blob_gc_entry{log_entry::make_normal_with_blob(storage, key, "", wversion, {}).view()};
    }

    // checks the key_sid (the storage_id and the key) and the write_version of a blob_gc_entry
    static void check_blob_gc_entry(const blob_gc_entry& entry,
        storage_id_type expected_storage,
        const std::string& expected_key,
        const write_version_type& expected_wv) {
        const std::string_view key_sid = entry.serialized_key_sid();
        EXPECT_EQ(log_entry_format::load_uint64le(key_sid), expected_storage);
        EXPECT_EQ(key_sid.substr(log_entry_format::storage_id_size), expected_key);
        EXPECT_EQ(entry.write_version(), expected_wv);
    }
};

// Test that appending log_entry instances increases container size.
TEST_F(blob_gc_entry_container_test, append_and_size) {
    blob_gc_entry_container container;
    EXPECT_EQ(container.size(), 0u);

    blob_gc_entry entry1 = create_blob_gc_entry(100, "keyA", write_version_type(10, 1));
    blob_gc_entry entry2 = create_blob_gc_entry(100, "keyB", write_version_type(5, 1));
    
    container.append(entry1);
    container.append(entry2);
    EXPECT_EQ(container.size(), 2u);
}

TEST_F(blob_gc_entry_container_test, sort_order) {
    blob_gc_entry_container container;
    // Two entries with the same key ("keyA") have different versions,
    // and one entry with a different key ("keyB").
    blob_gc_entry entry1 = create_blob_gc_entry(100, "keyB", write_version_type(1, 1));
    blob_gc_entry entry2 = create_blob_gc_entry(100, "keyA", write_version_type(3, 1)); // higher version
    blob_gc_entry entry3 = create_blob_gc_entry(100, "keyA", write_version_type(2, 1)); // lower version

    container.append(entry1);
    container.append(entry2);
    container.append(entry3);

    // Before sort, container should not be marked sorted.
    EXPECT_FALSE(container.is_sorted());
    EXPECT_EQ(container.size(), 3u);

    container.sort_descending();
    EXPECT_TRUE(container.is_sorted());
    EXPECT_EQ(container.size(), 3u);

    // Verify sorted order using the helper function.
    // Sorted order (descending):
    //   1st: key "keyB", write_version (1,1)
    //   2nd: key "keyA", write_version (3,1)
    //   3rd: key "keyA", write_version (2,1)
    auto it = container.begin();
    ASSERT_NE(it, container.end());
    check_blob_gc_entry(*it, 100, "keyB", write_version_type(1, 1));

    ++it;
    ASSERT_NE(it, container.end());
    check_blob_gc_entry(*it, 100, "keyA", write_version_type(3, 1));

    ++it;
    ASSERT_NE(it, container.end());
    check_blob_gc_entry(*it, 100, "keyA", write_version_type(2, 1));

    ++it;
    EXPECT_EQ(it, container.end());
}

// Test merging three sorted containers.
TEST_F(blob_gc_entry_container_test, merge_sorted_collections) {
    // Create three containers.
    blob_gc_entry_container container1, container2, container3;

    // --- Container 1 ---
    // Two entries:
    //   Entry 1: storage=100, key="D", value="val1", write_version=(1,0)
    //   Entry 2: storage=100, key="B", value="val2", write_version=(2,0)
    blob_gc_entry c1_e1 = create_blob_gc_entry(100, "D", write_version_type(1, 0));
    blob_gc_entry c1_e2 = create_blob_gc_entry(100, "B", write_version_type(2, 0));
    container1.append(c1_e1);
    container1.append(c1_e2);

    // --- Container 2 ---
    // Two entries:
    //   Entry 1: storage=100, key="C", value="val3", write_version=(3,0)
    //   Entry 2: storage=100, key="A", value="val4", write_version=(4,0)
    blob_gc_entry c2_e1 = create_blob_gc_entry(100, "C", write_version_type(3, 0));
    blob_gc_entry c2_e2 = create_blob_gc_entry(100, "A", write_version_type(4, 0));
    container2.append(c2_e1);
    container2.append(c2_e2);

    // --- Container 3 ---
    // Three entries:
    //   Entry 1: storage=100, key="E", value="val5", write_version=(5,0)
    //   Entry 2: storage=100, key="B", value="val6", write_version=(6,0)
    //   Entry 3: storage=100, key="A", value="val7", write_version=(7,0)
    blob_gc_entry c3_e1 = create_blob_gc_entry(100, "E", write_version_type(5, 0));
    blob_gc_entry c3_e2 = create_blob_gc_entry(100, "B", write_version_type(6, 0));
    blob_gc_entry c3_e3 = create_blob_gc_entry(100, "A", write_version_type(7, 0));
    container3.append(c3_e1);
    container3.append(c3_e2);
    container3.append(c3_e3);

    // Prepare a vector of containers to merge using shared_ptr.
    std::vector<std::shared_ptr<blob_gc_entry_container>> containers;
    containers.push_back(std::make_shared<blob_gc_entry_container>(std::move(container1)));
    containers.push_back(std::make_shared<blob_gc_entry_container>(std::move(container2)));
    containers.push_back(std::make_shared<blob_gc_entry_container>(std::move(container3)));

    // Perform the merge.
    blob_gc_entry_container merged = blob_gc_entry_container::merge_sorted_collections(containers);

    // Expected merged order (descending):
    //   1. ("E", (5,0))      -- container3
    //   2. ("D", (1,0))      -- container1
    //   3. ("C", (3,0))      -- container2
    //   4. ("B", (6,0))      -- container3
    //   5. ("B", (2,0))      -- container1
    //   6. ("A", (7,0))      -- container3
    //   7. ("A", (4,0))      -- container2
    auto it = merged.begin();
    ASSERT_NE(it, merged.end());
    check_blob_gc_entry(*it, 100, "E", write_version_type(5, 0));

    ++it;
    ASSERT_NE(it, merged.end());
    check_blob_gc_entry(*it, 100, "D", write_version_type(1, 0));

    ++it;
    ASSERT_NE(it, merged.end());
    check_blob_gc_entry(*it, 100, "C", write_version_type(3, 0));

    ++it;
    ASSERT_NE(it, merged.end());
    check_blob_gc_entry(*it, 100, "B", write_version_type(6, 0));

    ++it;
    ASSERT_NE(it, merged.end());
    check_blob_gc_entry(*it, 100, "B", write_version_type(2, 0));

    ++it;
    ASSERT_NE(it, merged.end());
    check_blob_gc_entry(*it, 100, "A", write_version_type(7, 0));

    ++it;
    ASSERT_NE(it, merged.end());
    check_blob_gc_entry(*it, 100, "A", write_version_type(4, 0));

    ++it;
    EXPECT_EQ(it, merged.end());

    // Verify that all original containers have been cleared.
    for (auto& uptr : containers) {
        EXPECT_EQ(uptr->size(), 0u);
    }
}



// Test case: Merge sorted collections with an empty container included.
TEST_F(blob_gc_entry_container_test, merge_sorted_collections_with_empty_container) {
    // Create two non-empty containers and one empty container.
    blob_gc_entry_container container1, container2, container_empty;
    
    // --- Container 1 ---
    blob_gc_entry c1_e1 = create_blob_gc_entry(100, "B", write_version_type(2, 0));
    blob_gc_entry c1_e2 = create_blob_gc_entry(100, "A", write_version_type(1, 0));
    container1.append(c1_e1);
    container1.append(c1_e2);
    
    // --- Container 2 ---
    blob_gc_entry c2_e1 = create_blob_gc_entry(100, "C", write_version_type(3, 0));
    container2.append(c2_e1);
    
    // Prepare vector using shared_ptr.
    std::vector<std::shared_ptr<blob_gc_entry_container>> containers;
    containers.push_back(std::make_unique<blob_gc_entry_container>(std::move(container1)));
    containers.push_back(std::make_unique<blob_gc_entry_container>(std::move(container_empty)));
    containers.push_back(std::make_unique<blob_gc_entry_container>(std::move(container2)));
    
    // Perform the merge.
    blob_gc_entry_container merged = blob_gc_entry_container::merge_sorted_collections(containers);
    
    // Expected descending order: "C" > "B" > "A"
    auto it = merged.begin();
    ASSERT_NE(it, merged.end());
    check_blob_gc_entry(*it, 100, "C", write_version_type(3, 0));
    
    ++it;
    ASSERT_NE(it, merged.end());
    check_blob_gc_entry(*it, 100, "B", write_version_type(2, 0));
    
    ++it;
    ASSERT_NE(it, merged.end());
    check_blob_gc_entry(*it, 100, "A", write_version_type(1, 0));
    
    ++it;
    EXPECT_EQ(it, merged.end());
    
    // Verify that all original containers have been cleared.
    for (auto& uptr : containers) {
        EXPECT_EQ(uptr->size(), 0u);
    }
}

// Test case: All containers are empty.
TEST_F(blob_gc_entry_container_test, merge_all_empty_containers) {
    // Create three empty containers.
    blob_gc_entry_container container1, container2, container3;
    
    // Prepare vector using shared_ptr.
    std::vector<std::shared_ptr<blob_gc_entry_container>> containers;
    containers.push_back(std::make_unique<blob_gc_entry_container>(std::move(container1)));
    containers.push_back(std::make_unique<blob_gc_entry_container>(std::move(container2)));
    containers.push_back(std::make_unique<blob_gc_entry_container>(std::move(container3)));
    
    // Perform the merge.
    blob_gc_entry_container merged = blob_gc_entry_container::merge_sorted_collections(containers);
    
    // Expect the merged container to be empty.
    EXPECT_EQ(merged.size(), 0u);
    
    // Also, each original container should be cleared.
    for (auto& uptr : containers) {
        EXPECT_EQ(uptr->size(), 0u);
    }
}

// Test case: Container list is empty.
TEST_F(blob_gc_entry_container_test, merge_empty_container_list) {
    // Prepare an empty vector of containers using shared_ptr.
    std::vector<std::shared_ptr<blob_gc_entry_container>> containers;
    
    // Perform the merge.
    blob_gc_entry_container merged = blob_gc_entry_container::merge_sorted_collections(containers);
    
    // Expect the merged container to be empty.
    EXPECT_EQ(merged.size(), 0u);
}


// Test case: Each container contains a single entry.
TEST_F(blob_gc_entry_container_test, merge_single_entry_containers) {
    // Create three containers, each with one entry.
    blob_gc_entry_container container1, container2, container3;
    
    blob_gc_entry c1_e1 = create_blob_gc_entry(100, "A", write_version_type(1, 0));
    container1.append(c1_e1);
    
    blob_gc_entry c2_e1 = create_blob_gc_entry(100, "B", write_version_type(2, 0));
    container2.append(c2_e1);
    
    blob_gc_entry c3_e1 = create_blob_gc_entry(100, "C", write_version_type(3, 0));
    container3.append(c3_e1);
    
    // Prepare vector using shared_ptr.
    std::vector<std::shared_ptr<blob_gc_entry_container>> containers;
    containers.push_back(std::make_unique<blob_gc_entry_container>(std::move(container1)));
    containers.push_back(std::make_unique<blob_gc_entry_container>(std::move(container2)));
    containers.push_back(std::make_unique<blob_gc_entry_container>(std::move(container3)));
    
    // Perform the merge.
    blob_gc_entry_container merged = blob_gc_entry_container::merge_sorted_collections(containers);
    
    // Expected descending order: "C" > "B" > "A"
    auto it = merged.begin();
    ASSERT_NE(it, merged.end());
    check_blob_gc_entry(*it, 100, "C", write_version_type(3, 0));
    
    ++it;
    ASSERT_NE(it, merged.end());
    check_blob_gc_entry(*it, 100, "B", write_version_type(2, 0));
    
    ++it;
    ASSERT_NE(it, merged.end());
    check_blob_gc_entry(*it, 100, "A", write_version_type(1, 0));
    
    ++it;
    EXPECT_EQ(it, merged.end());
    
    // Verify that all original containers have been cleared.
    for (auto& uptr : containers) {
        EXPECT_EQ(uptr->size(), 0u);
    }
}


// Test case: Merge sorted collections with duplicate entries.
TEST_F(blob_gc_entry_container_test, merge_with_duplicate_entries) {
    // Create two containers with duplicate entries.
    blob_gc_entry_container container1, container2;
    
    blob_gc_entry dup_entry1 = create_blob_gc_entry(100, "X", write_version_type(5, 0));
    blob_gc_entry dup_entry2 = create_blob_gc_entry(100, "X", write_version_type(5, 0));
    blob_gc_entry dup_entry3 = create_blob_gc_entry(100, "X", write_version_type(5, 0));
    
    container1.append(dup_entry1);
    container1.append(dup_entry2);
    container2.append(dup_entry3);
    
    // Prepare vector using shared_ptr.
    std::vector<std::shared_ptr<blob_gc_entry_container>> containers;
    containers.push_back(std::make_unique<blob_gc_entry_container>(std::move(container1)));
    containers.push_back(std::make_unique<blob_gc_entry_container>(std::move(container2)));
    
    // Perform the merge.
    blob_gc_entry_container merged = blob_gc_entry_container::merge_sorted_collections(containers);
    
    // Since all entries are identical, merged container should contain all 3 entries.
    int count = 0;
    for (auto it = merged.begin(); it != merged.end(); ++it) {
        check_blob_gc_entry(*it, 100, "X", write_version_type(5, 0));
        count++;
    }
    EXPECT_EQ(count, 3);
    
    // Verify that all original containers have been cleared.
    for (auto& uptr : containers) {
        EXPECT_EQ(uptr->size(), 0u);
    }
}

} // namespace testing
} // namespace limestone