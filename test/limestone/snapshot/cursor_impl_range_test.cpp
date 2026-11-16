/*
 * Copyright 2022-2026 Project Tsurugi.
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
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <memory>
#include <optional>
#include <string>
#include <tuple>
#include <vector>

#include <boost/filesystem.hpp>

#include "test_root.h"

#include <limestone/api/limestone_exception.h>

#include "cursor_impl.h"
#include "limestone/log/log_entry_bytes.h"
#include "locator/locator.h"
#include "locator/locator_format.h"
#include "locator/locator_writer.h"
#include "region_allocator.h"

namespace limestone::testing {

using limestone::api::limestone_exception;
using limestone::api::limestone_io_exception;
using limestone::internal::cursor_impl;
using limestone::internal::locator;
using limestone::internal::locator_writer;
using limestone::internal::region_allocator;
namespace lf = limestone::internal::locator_format;

using log_entry_bytes::marker;
using log_entry_bytes::normal_entry;
using log_entry_bytes::remove_entry;

/// a class to call the protected members of cursor_impl
class cursor_impl_range_testable : public cursor_impl {
public:
    using cursor_impl::cursor_impl;
    using cursor_impl::next;
    using cursor_impl::close;
    using cursor_impl::storage;
    using cursor_impl::key;
    using cursor_impl::value;

    ~cursor_impl_range_testable() override {
        close();
    }
};

class cursor_impl_range_test : public ::testing::Test {
protected:
    static constexpr const char* location = "/tmp/cursor_impl_range_test";

    /// one row returned by a cursor (storage, key, value)
    using row = std::tuple<std::uint64_t, std::string, std::string>;
    using rows = std::vector<row>;

    void SetUp() override {
        boost::filesystem::remove_all(location);
        boost::filesystem::create_directories(location);
    }

    void TearDown() override {
        boost::filesystem::remove_all(location);
    }

    static boost::filesystem::path path(const std::string& name) {
        return boost::filesystem::path(location) / name;
    }

    // makes a file with the contents, and returns its path
    static boost::filesystem::path write_file(const std::string& name, const std::string& bytes) {
        boost::filesystem::path p = path(name);
        std::ofstream out(p.string(), std::ios::binary | std::ios::trunc);
        out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
        out.close();
        if (! out) {
            ADD_FAILURE() << "cannot write " << p;
        }
        return p;
    }

    // makes a file with the contents and its locator file (with the record interval), and returns the locator
    static locator make_file(const std::string& name, const std::string& bytes, std::uint64_t interval) {
        boost::filesystem::path locatee = write_file(name, bytes);
        boost::filesystem::path file = lf::locator_file_path(locatee);
        locator_writer::build_from_file(locatee, file, interval);
        return locator{file, bytes.size()};
    }

    // a normal_entry of storage 1. The value tells which file the output comes from
    static std::string entry(std::string_view key, std::string_view value) {
        return normal_entry(1, key, value, 7, 1);
    }

    static std::string entries(const std::vector<std::pair<std::string, std::string>>& key_values) {
        std::string bytes{};
        for (const auto& [key, value] : key_values) {
            bytes += entry(key, value);
        }
        return bytes;
    }

    // reads all the rest the cursor returns
    static rows read_all(cursor_impl_range_testable& cursor) {
        rows result{};
        while (cursor.next()) {
            std::string key{};
            std::string value{};
            cursor.key(key);
            cursor.value(value);
            result.emplace_back(cursor.storage(), key, value);
        }
        return result;
    }

    // the output of a cursor that reads without regions (the reference)
    static rows read_full(const std::optional<boost::filesystem::path>& compacted) {
        if (compacted.has_value()) {
            cursor_impl_range_testable cursor(path("snapshot"), *compacted, std::map<api::storage_id_type, api::write_version_type>{});
            return read_all(cursor);
        }
        cursor_impl_range_testable cursor(path("snapshot"), std::map<api::storage_id_type, api::write_version_type>{});
        return read_all(cursor);
    }

    static std::unique_ptr<cursor_impl_range_testable> make_cursor(const std::optional<boost::filesystem::path>& compacted,
                                                                   const std::shared_ptr<region_allocator>& allocator) {
        return std::make_unique<cursor_impl_range_testable>(path("snapshot"), compacted,
                                                            std::map<api::storage_id_type, api::write_version_type>{},
                                                            allocator);
    }

    // returns the output of each region. Makes as many cursors as the regions, first lets each cursor read one row so
    // that each takes one region, and then reads the rest of each. The output of cursor i is the output of region i
    static std::vector<rows> read_by_region(const std::optional<boost::filesystem::path>& compacted,
                                            const std::shared_ptr<region_allocator>& allocator) {
        const std::size_t n = allocator->region_count();
        std::vector<std::unique_ptr<cursor_impl_range_testable>> cursors{};
        std::vector<rows> result(n);
        for (std::size_t i = 0; i < n; ++i) {
            cursors.push_back(make_cursor(compacted, allocator));
            if (cursors[i]->next()) {
                std::string key{};
                std::string value{};
                cursors[i]->key(key);
                cursors[i]->value(value);
                result[i].emplace_back(cursors[i]->storage(), key, value);
            }
        }
        for (std::size_t i = 0; i < n; ++i) {
            rows rest = read_all(*cursors[i]);
            result[i].insert(result[i].end(), rest.begin(), rest.end());
        }
        return result;
    }
};

// one cursor reading the regions one after another gives the same output as reading without regions. Includes R that
// divides the number of records of the primary, R that does not, and a single region. Includes a small key only in the
// secondary file (a00), the same key in both (k02, the snapshot side is taken), and a middle key only in the secondary
// file (k05)
TEST_F(cursor_impl_range_test, one_cursor_reads_all_regions) {
    locator snapshot = make_file("snapshot", entries({{"k00", "s"}, {"k02", "s"}, {"k04", "s"}, {"k06", "s"}}), 1);
    locator compacted = make_file("compacted", entries({{"a00", "c"}, {"k02", "c"}, {"k05", "c"}}), 1);
    const rows expected{{1, "a00", "c"}, {1, "k00", "s"}, {1, "k02", "s"}, {1, "k04", "s"}, {1, "k05", "c"}, {1, "k06", "s"}};
    ASSERT_EQ(read_full(path("compacted")), expected);
    for (std::size_t r = 1; r <= 5; ++r) {
        auto allocator = std::make_shared<region_allocator>(snapshot, compacted, r);
        std::unique_ptr<cursor_impl_range_testable> cursor = make_cursor(path("compacted"), allocator);
        EXPECT_EQ(read_all(*cursor), expected) << "R = " << r;
        // stays false after nothing is left
        EXPECT_FALSE(cursor->next()) << "R = " << r;
    }
}

// the output of each region. The ranges of the secondary file overlap across the boundary keys of the regions, but
// the check of the key range makes each entry appear once
TEST_F(cursor_impl_range_test, output_of_each_region) {
    locator snapshot = make_file("snapshot", entries({{"k00", "s"}, {"k02", "s"}, {"k04", "s"}, {"k06", "s"}}), 1);
    locator compacted = make_file("compacted", entries({{"a00", "c"}, {"k02", "c"}, {"k05", "c"}}), 1);
    auto allocator = std::make_shared<region_allocator>(snapshot, compacted, 1);
    ASSERT_EQ(allocator->region_count(), 4U);
    const std::vector<rows> expected{
        {{1, "a00", "c"}, {1, "k00", "s"}},  // region 0: no start key, the end key is k02
        {{1, "k02", "s"}},                   // region 1: from k02 below k04. The secondary range starts at a00
        {{1, "k04", "s"}, {1, "k05", "c"}},  // region 2: from k04 below k06. The secondary range is from k02 to the end
        {{1, "k06", "s"}},                   // region 3: from k06, no end key. The secondary range is from k05 to the end
    };
    EXPECT_EQ(read_by_region(path("compacted"), allocator), expected);
}

// if the secondary file has only one record, the range of every region covers the secondary file from the beginning
// to the end, and only the check of the key range decides the output. A key equal to the start key is included (k02,
// k04), and reading stops at a key equal to the end key
TEST_F(cursor_impl_range_test, key_equal_to_boundary) {
    locator snapshot = make_file("snapshot", entries({{"k00", "s"}, {"k02", "s"}, {"k04", "s"}, {"k06", "s"}}), 1);
    // a large record interval leaves only one record, at position 0
    locator compacted = make_file("compacted", entries({{"a00", "c"}, {"k02", "c"}, {"k04", "c"}, {"k05", "c"}, {"k07", "c"}}), 1000);
    ASSERT_EQ(compacted.record_count(), 1U);
    auto allocator = std::make_shared<region_allocator>(snapshot, compacted, 1);
    const std::vector<rows> expected{
        {{1, "a00", "c"}, {1, "k00", "s"}},
        {{1, "k02", "s"}},
        {{1, "k04", "s"}, {1, "k05", "c"}},
        {{1, "k06", "s"}, {1, "k07", "c"}},
    };
    EXPECT_EQ(read_by_region(path("compacted"), allocator), expected);
}

// without a compacted WAL file
TEST_F(cursor_impl_range_test, snapshot_only) {
    locator snapshot = make_file("snapshot", entries({{"k00", "s"}, {"k01", "s"}, {"k02", "s"}, {"k03", "s"}, {"k04", "s"}}), 1);
    const rows expected = read_full(std::nullopt);
    ASSERT_EQ(expected.size(), 5U);
    auto allocator = std::make_shared<region_allocator>(snapshot, std::nullopt, 2);
    ASSERT_EQ(allocator->region_count(), 3U);
    std::unique_ptr<cursor_impl_range_testable> cursor = make_cursor(std::nullopt, allocator);
    EXPECT_EQ(read_all(*cursor), expected);

    auto allocator2 = std::make_shared<region_allocator>(snapshot, std::nullopt, 2);
    const std::vector<rows> by_region{
        {{1, "k00", "s"}, {1, "k01", "s"}},
        {{1, "k02", "s"}, {1, "k03", "s"}},
        {{1, "k04", "s"}},
    };
    EXPECT_EQ(read_by_region(std::nullopt, allocator2), by_region);
}

// if the primary has no record, next() is false from the first call
TEST_F(cursor_impl_range_test, no_records) {
    locator snapshot = make_file("snapshot", "", 1);
    auto allocator = std::make_shared<region_allocator>(snapshot, std::nullopt, 1);
    std::unique_ptr<cursor_impl_range_testable> cursor = make_cursor(std::nullopt, allocator);
    EXPECT_FALSE(cursor->next());
}

// even when several cursors share the regions, their outputs together are the same as reading without regions, and
// the output of each cursor is in ascending order of the key. The compacted WAL file is the primary
TEST_F(cursor_impl_range_test, cursors_share_regions) {
    std::vector<std::pair<std::string, std::string>> snapshot_entries{};
    std::vector<std::pair<std::string, std::string>> compacted_entries{};
    for (int i = 0; i < 30; ++i) {
        const std::string key = "k" + std::to_string(10 + i);
        compacted_entries.emplace_back(key, "c");
        if (i % 3 == 0) {
            snapshot_entries.emplace_back(key, "s");
        }
    }
    locator snapshot = make_file("snapshot", entries(snapshot_entries), 1);
    locator compacted = make_file("compacted", entries(compacted_entries), 1);
    rows expected = read_full(path("compacted"));
    ASSERT_EQ(expected.size(), 30U);

    auto allocator = std::make_shared<region_allocator>(snapshot, compacted, 4);
    ASSERT_EQ(allocator->region_count(), 8U);
    std::vector<std::unique_ptr<cursor_impl_range_testable>> cursors{};
    for (int i = 0; i < 3; ++i) {
        cursors.push_back(make_cursor(path("compacted"), allocator));
    }
    // advance the 3 cursors in turn, a little at a time
    rows all{};
    std::vector<rows> each(3);
    bool progressed = true;
    while (progressed) {
        progressed = false;
        for (std::size_t i = 0; i < 3; ++i) {
            for (int k = 0; k < 2 && cursors[i]->next(); ++k) {
                std::string key{};
                std::string value{};
                cursors[i]->key(key);
                cursors[i]->value(value);
                each[i].emplace_back(cursors[i]->storage(), key, value);
                progressed = true;
            }
        }
    }
    for (const rows& r : each) {
        EXPECT_TRUE(std::is_sorted(r.begin(), r.end()));
        all.insert(all.end(), r.begin(), r.end());
    }
    std::sort(all.begin(), all.end());
    EXPECT_EQ(all, expected);
}

// a remove_entry of the snapshot cancels the same key of the compacted WAL file, also across regions
TEST_F(cursor_impl_range_test, remove_entry_overrides_compacted) {
    std::string snapshot_bytes = entry("k00", "s") + remove_entry(1, "k02", 7, 1) + entry("k04", "s") + remove_entry(1, "k05", 7, 1);
    locator snapshot = make_file("snapshot", snapshot_bytes, 1);
    locator compacted = make_file("compacted", entries({{"k02", "c"}, {"k05", "c"}}), 1);
    const rows expected{{1, "k00", "s"}, {1, "k04", "s"}};
    ASSERT_EQ(read_full(path("compacted")), expected);
    auto allocator = std::make_shared<region_allocator>(snapshot, compacted, 1);
    ASSERT_EQ(allocator->region_count(), 4U);
    std::unique_ptr<cursor_impl_range_testable> cursor = make_cursor(path("compacted"), allocator);
    EXPECT_EQ(read_all(*cursor), expected);
}

// if a probe of the allocator fails (an entry at the position of a record is not a data entry), next() throws
// limestone_exception
TEST_F(cursor_impl_range_test, probe_failure_in_next) {
    locator snapshot = make_file("snapshot", entries({{"k00", "s"}, {"k02", "s"}, {"k04", "s"}}), 1);
    // put a marker at the position of record 1 of the secondary file (the locator is written by hand)
    std::string e0 = entry("k01", "c");
    std::string compacted_bytes = e0 + marker(2, 7) + entry("k03", "c");
    boost::filesystem::path locatee = write_file("compacted", compacted_bytes);
    boost::filesystem::path file = lf::locator_file_path(locatee);
    {
        locator_writer writer(file, 1, false);
        writer.note_entry(0);
        writer.note_entry(e0.size());
        writer.close(compacted_bytes.size());
    }
    locator compacted(file, compacted_bytes.size());
    ASSERT_EQ(compacted.record_count(), 2U);
    auto allocator = std::make_shared<region_allocator>(snapshot, compacted, 1);
    std::unique_ptr<cursor_impl_range_testable> cursor = make_cursor(path("compacted"), allocator);
    // the bisection for the end position of the secondary of region 0 probes the marker
    try {
        (void) cursor->next();
        FAIL() << "no exception";
    } catch (const limestone_io_exception& e) {
        FAIL() << "unexpected I/O exception: " << e.what();
    } catch (const limestone_exception& e) {
        EXPECT_NE(std::string(e.what()).find("is not a data entry"), std::string::npos) << e.what();
    }
}

// an invalid use aborts: an empty allocator
TEST_F(cursor_impl_range_test, no_allocator_aborts) {
    (void) make_file("snapshot", entries({{"k00", "s"}}), 1);
    EXPECT_DEATH({
        cursor_impl_range_testable cursor(path("snapshot"), std::nullopt,
                                          std::map<api::storage_id_type, api::write_version_type>{}, nullptr);
    }, "no region allocator");
}

}  // namespace limestone::testing
