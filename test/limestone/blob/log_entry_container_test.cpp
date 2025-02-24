#include <gtest/gtest.h>
#include <cstdio>
#include <fstream>
#include <cstdlib>
#include <string>
#include <stdexcept>
#include <vector>
#include <boost/filesystem.hpp>

#include "log_entry.h"
#include "log_entry_container.h"
#include "log_entry_comparator.h"

// Use the namespaces defined for the API and internal classes.
using namespace limestone::api;
using namespace limestone::internal;

namespace limestone {
namespace testing {

class log_entry_container_test : public ::testing::Test {
protected:
    std::string temp_dir;
    int file_counter = 0;

    // Set up a temporary directory for creating log_entry instances.
    void SetUp() override {
        if (system("rm -rf /tmp/limestone_log_entry_container_test") != 0) {
            std::cerr << "Unable to remove /tmp/limestone_log_entry_container_test" 
                      << std::endl;
        }
        if (system("mkdir -p /tmp/limestone_log_entry_container_test") != 0) {
            std::cerr << "Unable to create /tmp/limestone_log_entry_container_test" 
                      << std::endl;
        }
        temp_dir = "/tmp/limestone_log_entry_container_test";
        file_counter = 0;
    }

    void TearDown() override {
        boost::filesystem::remove_all(temp_dir);
    }

    // Generate a unique temporary file name in temp_dir.
    std::string get_temp_file_name() {
        file_counter++;
        return temp_dir + "/temp_file_" + std::to_string(file_counter);
    }

    // Create a log_entry instance by writing a normal entry into a temporary file
    // and reading it back. Uses the log_entry::write() and log_entry::read() methods.
    log_entry create_normal_log_entry(storage_id_type storage,
                                      const std::string &key,
                                      const std::string &value,
                                      const write_version_type &wversion) {
        std::string temp_file = get_temp_file_name();
        FILE* out = std::fopen(temp_file.c_str(), "wb");
        if (!out) {
            throw std::runtime_error("Failed to open temporary file for writing.");
        }
        // Write a normal log entry.
        log_entry::write(out, storage, key, value, wversion);
        std::fclose(out);

        std::ifstream in(temp_file, std::ios::binary);
        if (!in.is_open()) {
            throw std::runtime_error("Failed to open temporary file for reading.");
        }
        log_entry entry;
        bool rc = entry.read(in);
        in.close();
        boost::filesystem::remove(temp_file);
        if (!rc) {
            throw std::runtime_error("Failed to read log entry from temporary file.");
        }
        return entry;
    }

    // Helper function to check log_entry contents.
    void check_log_entry(const log_entry& entry,
        storage_id_type expected_storage,
        const std::string& expected_key,
        const std::string& expected_value,
        const write_version_type& expected_wv) {
        EXPECT_EQ(entry.storage(), expected_storage);

        std::string key_buf;
        entry.key(key_buf);
        EXPECT_EQ(key_buf, expected_key);

        std::string value_buf;
        entry.value(value_buf);
        EXPECT_EQ(value_buf, expected_value);

        write_version_type actual_wv;
        entry.write_version(actual_wv);
        EXPECT_EQ(actual_wv, expected_wv);
    }
};

// Test that appending log_entry instances increases container size.
TEST_F(log_entry_container_test, append_and_size) {
    log_entry_container container;
    EXPECT_EQ(container.size(), 0u);

    log_entry entry1 = create_normal_log_entry(100, "keyA", "valueA", write_version_type(10, 1));
    log_entry entry2 = create_normal_log_entry(100, "keyB", "valueB", write_version_type(5, 1));
    
    container.append(entry1);
    container.append(entry2);
    EXPECT_EQ(container.size(), 2u);
}

TEST_F(log_entry_container_test, sort_order) {
    log_entry_container container;
    // Two entries with the same key ("keyA") have different versions,
    // and one entry with a different key ("keyB").
    log_entry entry1 = create_normal_log_entry(100, "keyB", "value", write_version_type(1, 1));
    log_entry entry2 = create_normal_log_entry(100, "keyA", "value", write_version_type(3, 1)); // higher version
    log_entry entry3 = create_normal_log_entry(100, "keyA", "value", write_version_type(2, 1)); // lower version

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
    check_log_entry(*it, 100, "keyB", "value", write_version_type(1, 1));

    ++it;
    ASSERT_NE(it, container.end());
    check_log_entry(*it, 100, "keyA", "value", write_version_type(3, 1));

    ++it;
    ASSERT_NE(it, container.end());
    check_log_entry(*it, 100, "keyA", "value", write_version_type(2, 1));

    ++it;
    EXPECT_EQ(it, container.end());
}

// Test merging three sorted containers.
TEST_F(log_entry_container_test, merge_sorted_collections) {
    // Create three containers.
    log_entry_container container1, container2, container3;

    // --- Container 1 ---
    // Two entries:
    //   Entry 1: storage=100, key="D", value="val1", write_version=(1,0)
    //   Entry 2: storage=100, key="B", value="val2", write_version=(2,0)
    log_entry c1_e1 = create_normal_log_entry(100, "D", "val1", write_version_type(1, 0));
    log_entry c1_e2 = create_normal_log_entry(100, "B", "val2", write_version_type(2, 0));
    container1.append(c1_e1);
    container1.append(c1_e2);

    // --- Container 2 ---
    // Two entries:
    //   Entry 1: storage=100, key="C", value="val3", write_version=(3,0)
    //   Entry 2: storage=100, key="A", value="val4", write_version=(4,0)
    log_entry c2_e1 = create_normal_log_entry(100, "C", "val3", write_version_type(3, 0));
    log_entry c2_e2 = create_normal_log_entry(100, "A", "val4", write_version_type(4, 0));
    container2.append(c2_e1);
    container2.append(c2_e2);

    // --- Container 3 ---
    // Three entries:
    //   Entry 1: storage=100, key="E", value="val5", write_version=(5,0)
    //   Entry 2: storage=100, key="B", value="val6", write_version=(6,0)
    //   Entry 3: storage=100, key="A", value="val7", write_version=(7,0)
    log_entry c3_e1 = create_normal_log_entry(100, "E", "val5", write_version_type(5, 0));
    log_entry c3_e2 = create_normal_log_entry(100, "B", "val6", write_version_type(6, 0));
    log_entry c3_e3 = create_normal_log_entry(100, "A", "val7", write_version_type(7, 0));
    container3.append(c3_e1);
    container3.append(c3_e2);
    container3.append(c3_e3);

    // Prepare a vector of containers to merge using shared_ptr.
    std::vector<std::shared_ptr<log_entry_container>> containers;
    containers.push_back(std::make_shared<log_entry_container>(std::move(container1)));
    containers.push_back(std::make_shared<log_entry_container>(std::move(container2)));
    containers.push_back(std::make_shared<log_entry_container>(std::move(container3)));

    // Perform the merge.
    log_entry_container merged = log_entry_container::merge_sorted_collections(containers);

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
    check_log_entry(*it, 100, "E", "val5", write_version_type(5, 0));

    ++it;
    ASSERT_NE(it, merged.end());
    check_log_entry(*it, 100, "D", "val1", write_version_type(1, 0));

    ++it;
    ASSERT_NE(it, merged.end());
    check_log_entry(*it, 100, "C", "val3", write_version_type(3, 0));

    ++it;
    ASSERT_NE(it, merged.end());
    check_log_entry(*it, 100, "B", "val6", write_version_type(6, 0));

    ++it;
    ASSERT_NE(it, merged.end());
    check_log_entry(*it, 100, "B", "val2", write_version_type(2, 0));

    ++it;
    ASSERT_NE(it, merged.end());
    check_log_entry(*it, 100, "A", "val7", write_version_type(7, 0));

    ++it;
    ASSERT_NE(it, merged.end());
    check_log_entry(*it, 100, "A", "val4", write_version_type(4, 0));

    ++it;
    EXPECT_EQ(it, merged.end());

    // Verify that all original containers have been cleared.
    for (auto& uptr : containers) {
        EXPECT_EQ(uptr->size(), 0u);
    }
}



// Test case: Merge sorted collections with an empty container included.
TEST_F(log_entry_container_test, merge_sorted_collections_with_empty_container) {
    // Create two non-empty containers and one empty container.
    log_entry_container container1, container2, container_empty;
    
    // --- Container 1 ---
    log_entry c1_e1 = create_normal_log_entry(100, "B", "val1", write_version_type(2, 0));
    log_entry c1_e2 = create_normal_log_entry(100, "A", "val2", write_version_type(1, 0));
    container1.append(c1_e1);
    container1.append(c1_e2);
    
    // --- Container 2 ---
    log_entry c2_e1 = create_normal_log_entry(100, "C", "val3", write_version_type(3, 0));
    container2.append(c2_e1);
    
    // Prepare vector using shared_ptr.
    std::vector<std::shared_ptr<log_entry_container>> containers;
    containers.push_back(std::make_unique<log_entry_container>(std::move(container1)));
    containers.push_back(std::make_unique<log_entry_container>(std::move(container_empty)));
    containers.push_back(std::make_unique<log_entry_container>(std::move(container2)));
    
    // Perform the merge.
    log_entry_container merged = log_entry_container::merge_sorted_collections(containers);
    
    // Expected descending order: "C" > "B" > "A"
    auto it = merged.begin();
    ASSERT_NE(it, merged.end());
    check_log_entry(*it, 100, "C", "val3", write_version_type(3, 0));
    
    ++it;
    ASSERT_NE(it, merged.end());
    check_log_entry(*it, 100, "B", "val1", write_version_type(2, 0));
    
    ++it;
    ASSERT_NE(it, merged.end());
    check_log_entry(*it, 100, "A", "val2", write_version_type(1, 0));
    
    ++it;
    EXPECT_EQ(it, merged.end());
    
    // Verify that all original containers have been cleared.
    for (auto& uptr : containers) {
        EXPECT_EQ(uptr->size(), 0u);
    }
}

// Test case: All containers are empty.
TEST_F(log_entry_container_test, merge_all_empty_containers) {
    // Create three empty containers.
    log_entry_container container1, container2, container3;
    
    // Prepare vector using shared_ptr.
    std::vector<std::shared_ptr<log_entry_container>> containers;
    containers.push_back(std::make_unique<log_entry_container>(std::move(container1)));
    containers.push_back(std::make_unique<log_entry_container>(std::move(container2)));
    containers.push_back(std::make_unique<log_entry_container>(std::move(container3)));
    
    // Perform the merge.
    log_entry_container merged = log_entry_container::merge_sorted_collections(containers);
    
    // Expect the merged container to be empty.
    EXPECT_EQ(merged.size(), 0u);
    
    // Also, each original container should be cleared.
    for (auto& uptr : containers) {
        EXPECT_EQ(uptr->size(), 0u);
    }
}

// Test case: Container list is empty.
TEST_F(log_entry_container_test, merge_empty_container_list) {
    // Prepare an empty vector of containers using shared_ptr.
    std::vector<std::shared_ptr<log_entry_container>> containers;
    
    // Perform the merge.
    log_entry_container merged = log_entry_container::merge_sorted_collections(containers);
    
    // Expect the merged container to be empty.
    EXPECT_EQ(merged.size(), 0u);
}


// Test case: Each container contains a single entry.
TEST_F(log_entry_container_test, merge_single_entry_containers) {
    // Create three containers, each with one entry.
    log_entry_container container1, container2, container3;
    
    log_entry c1_e1 = create_normal_log_entry(100, "A", "val1", write_version_type(1, 0));
    container1.append(c1_e1);
    
    log_entry c2_e1 = create_normal_log_entry(100, "B", "val2", write_version_type(2, 0));
    container2.append(c2_e1);
    
    log_entry c3_e1 = create_normal_log_entry(100, "C", "val3", write_version_type(3, 0));
    container3.append(c3_e1);
    
    // Prepare vector using shared_ptr.
    std::vector<std::shared_ptr<log_entry_container>> containers;
    containers.push_back(std::make_unique<log_entry_container>(std::move(container1)));
    containers.push_back(std::make_unique<log_entry_container>(std::move(container2)));
    containers.push_back(std::make_unique<log_entry_container>(std::move(container3)));
    
    // Perform the merge.
    log_entry_container merged = log_entry_container::merge_sorted_collections(containers);
    
    // Expected descending order: "C" > "B" > "A"
    auto it = merged.begin();
    ASSERT_NE(it, merged.end());
    check_log_entry(*it, 100, "C", "val3", write_version_type(3, 0));
    
    ++it;
    ASSERT_NE(it, merged.end());
    check_log_entry(*it, 100, "B", "val2", write_version_type(2, 0));
    
    ++it;
    ASSERT_NE(it, merged.end());
    check_log_entry(*it, 100, "A", "val1", write_version_type(1, 0));
    
    ++it;
    EXPECT_EQ(it, merged.end());
    
    // Verify that all original containers have been cleared.
    for (auto& uptr : containers) {
        EXPECT_EQ(uptr->size(), 0u);
    }
}


// Test case: Merge sorted collections with duplicate entries.
TEST_F(log_entry_container_test, merge_with_duplicate_entries) {
    // Create two containers with duplicate entries.
    log_entry_container container1, container2;
    
    log_entry dup_entry1 = create_normal_log_entry(100, "X", "dup", write_version_type(5, 0));
    log_entry dup_entry2 = create_normal_log_entry(100, "X", "dup", write_version_type(5, 0));
    log_entry dup_entry3 = create_normal_log_entry(100, "X", "dup", write_version_type(5, 0));
    
    container1.append(dup_entry1);
    container1.append(dup_entry2);
    container2.append(dup_entry3);
    
    // Prepare vector using shared_ptr.
    std::vector<std::shared_ptr<log_entry_container>> containers;
    containers.push_back(std::make_unique<log_entry_container>(std::move(container1)));
    containers.push_back(std::make_unique<log_entry_container>(std::move(container2)));
    
    // Perform the merge.
    log_entry_container merged = log_entry_container::merge_sorted_collections(containers);
    
    // Since all entries are identical, merged container should contain all 3 entries.
    int count = 0;
    for (auto it = merged.begin(); it != merged.end(); ++it) {
        check_log_entry(*it, 100, "X", "dup", write_version_type(5, 0));
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