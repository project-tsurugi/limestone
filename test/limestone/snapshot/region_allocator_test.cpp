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
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include <boost/filesystem.hpp>

#include "test_root.h"

#include <limestone/api/limestone_exception.h>

#include "file_operations.h"
#include "limestone/log/log_entry_bytes.h"
#include "locator/locator.h"
#include "locator/locator_format.h"
#include "locator/locator_writer.h"
#include "log_entry/log_entry_reader.h"
#include "region_allocator.h"

namespace limestone::testing {

using limestone::api::limestone_exception;
using limestone::api::limestone_io_exception;
using limestone::internal::locator;
using limestone::internal::locator_writer;
using limestone::internal::log_entry_reader;
using limestone::internal::real_file_operations;
using limestone::internal::region_allocator;
using limestone::internal::region_range;
namespace lf = limestone::internal::locator_format;

using log_entry_bytes::marker;
using log_entry_bytes::normal_entry;
using log_entry_bytes::normal_with_blob;
using log_entry_bytes::u64le;
using log_entry_bytes::u8;

namespace {

// file operations that record the sizes requested by pread
class recording_file_operations : public real_file_operations {
public:
    ssize_t pread(int fd, void* buf, size_t count, off_t offset) override {
        requests_.push_back(count);
        return real_file_operations::pread(fd, buf, count, offset);
    }
    [[nodiscard]] const std::vector<std::size_t>& requests() const noexcept { return requests_; }
    void clear() { requests_.clear(); }

private:
    std::vector<std::size_t> requests_{};
};

}  // namespace

class region_allocator_test : public ::testing::Test {
protected:
    static constexpr const char* location = "/tmp/region_allocator_test";
    /// the size of one entry (made by entry())
    static constexpr std::uint64_t entry_size = 38;

    void SetUp() override {
        boost::filesystem::remove_all(location);
        boost::filesystem::create_directories(location);
    }

    void TearDown() override {
        boost::filesystem::remove_all(location);
    }

    // a normal_entry of size entry_size (the key is 3 characters)
    static std::string entry(std::string_view key) {
        return normal_entry(1, key, "vv", 7, 1);
    }

    // the serialized key_sid of entry()
    static std::string key_sid(std::string_view key) {
        return u64le(1) + std::string(key);
    }

    // makes the contents of a file of entry() of the keys
    static std::string entries(const std::vector<std::string>& keys) {
        std::string bytes{};
        for (const std::string& key : keys) {
            bytes += entry(key);
        }
        return bytes;
    }

    // makes a file with the contents, and returns its path
    static boost::filesystem::path write_file(const std::string& name, const std::string& bytes) {
        boost::filesystem::path p = boost::filesystem::path(location) / name;
        std::ofstream out(p.string(), std::ios::binary | std::ios::trunc);
        out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
        out.close();
        if (! out) {
            ADD_FAILURE() << "cannot write " << p;
        }
        return p;
    }

    // makes the locator file of the file with the record interval, and returns the locator read
    static locator make_locator(const boost::filesystem::path& locatee, std::uint64_t interval) {
        const boost::filesystem::path file = lf::locator_file_path(locatee);
        locator_writer::build_from_file(locatee, file, interval);
        return locator{file, boost::filesystem::file_size(locatee)};
    }

    // makes a file with the contents and its locator file (with the record interval), and returns the locator
    static locator make_file(const std::string& name, const std::string& bytes, std::uint64_t interval) {
        return make_locator(write_file(name, bytes), interval);
    }

    static boost::filesystem::path path(const std::string& name) {
        return boost::filesystem::path(location) / name;
    }

    static void expect_key(const std::optional<std::string>& actual, const std::optional<std::string>& expected) {
        ASSERT_EQ(actual.has_value(), expected.has_value());
        if (expected.has_value()) {
            EXPECT_EQ(*actual, *expected);
        }
    }

    // checks that limestone_exception (not limestone_io_exception) is thrown and its message contains expected
    template <class F>
    static void expect_throws(F&& f, const std::string& expected) {
        try {
            f();
            FAIL() << "no exception";
        } catch (const limestone_io_exception& e) {
            FAIL() << "unexpected I/O exception: " << e.what();
        } catch (const limestone_exception& e) {
            EXPECT_NE(std::string(e.what()).find(expected), std::string::npos) << e.what();
        }
    }
};

// the snapshot file with more records is the primary. For each region, the start and end positions of the primary, the
// start and end keys, and the start and end positions of the secondary are computed. The first region has no start
// key and the last region has no end key (10 records are not divisible by R = 4)
TEST_F(region_allocator_test, snapshot_is_primary) {
    // snapshot: 10 entries k00..k09, every entry is a record. compacted: 4 entries k01, k03, k05, k07
    locator snapshot = make_file("snapshot", entries({"k00", "k01", "k02", "k03", "k04", "k05", "k06", "k07", "k08", "k09"}), 1);
    locator compacted = make_file("compacted", entries({"k01", "k03", "k05", "k07"}), 1);
    ASSERT_EQ(snapshot.record_count(), 10U);
    ASSERT_EQ(compacted.record_count(), 4U);
    region_allocator allocator(snapshot, compacted, 4);
    EXPECT_EQ(allocator.region_count(), 3U);
    log_entry_reader snapshot_reader(path("snapshot"));
    log_entry_reader compacted_reader(path("compacted"));

    // region 0: k00..k03. The secondary is from the beginning to the first record at or above k04 (k05, number 2)
    std::optional<region_range> r0 = allocator.allocate(snapshot_reader, &compacted_reader);
    ASSERT_TRUE(r0.has_value());
    EXPECT_EQ(r0->snapshot_start, 0U);
    EXPECT_EQ(r0->snapshot_end, 4 * entry_size);
    expect_key(r0->start_key, std::nullopt);
    expect_key(r0->end_key, key_sid("k04"));
    EXPECT_EQ(r0->compacted_start, 0U);
    EXPECT_EQ(r0->compacted_end, 2 * entry_size);

    // region 1: k04..k07. The secondary is from the last record below k04 (k03, number 1) to the end, as there is no
    // record at or above k08
    std::optional<region_range> r1 = allocator.allocate(snapshot_reader, &compacted_reader);
    ASSERT_TRUE(r1.has_value());
    EXPECT_EQ(r1->snapshot_start, 4 * entry_size);
    EXPECT_EQ(r1->snapshot_end, 8 * entry_size);
    expect_key(r1->start_key, key_sid("k04"));
    expect_key(r1->end_key, key_sid("k08"));
    EXPECT_EQ(r1->compacted_start, 1 * entry_size);
    EXPECT_EQ(r1->compacted_end, 4 * entry_size);

    // region 2 (the last): k08..k09. The end position of the primary is the end of the file. The secondary is from the
    // last record below k08 (k07, number 3) to the end
    std::optional<region_range> r2 = allocator.allocate(snapshot_reader, &compacted_reader);
    ASSERT_TRUE(r2.has_value());
    EXPECT_EQ(r2->snapshot_start, 8 * entry_size);
    EXPECT_EQ(r2->snapshot_end, 10 * entry_size);
    expect_key(r2->start_key, key_sid("k08"));
    expect_key(r2->end_key, std::nullopt);
    EXPECT_EQ(r2->compacted_start, 3 * entry_size);
    EXPECT_EQ(r2->compacted_end, 4 * entry_size);

    // none is left
    EXPECT_FALSE(allocator.allocate(snapshot_reader, &compacted_reader).has_value());
    EXPECT_FALSE(allocator.allocate(snapshot_reader, &compacted_reader).has_value());
}

// the compacted WAL file with more records is the primary. The range is returned in terms of snapshot and compacted
TEST_F(region_allocator_test, compacted_is_primary) {
    locator snapshot = make_file("snapshot", entries({"k01", "k03", "k05", "k07"}), 1);
    locator compacted = make_file("compacted", entries({"k00", "k01", "k02", "k03", "k04", "k05", "k06", "k07", "k08", "k09"}), 1);
    region_allocator allocator(snapshot, compacted, 4);
    EXPECT_EQ(allocator.region_count(), 3U);
    log_entry_reader snapshot_reader(path("snapshot"));
    log_entry_reader compacted_reader(path("compacted"));

    std::optional<region_range> r0 = allocator.allocate(snapshot_reader, &compacted_reader);
    ASSERT_TRUE(r0.has_value());
    EXPECT_EQ(r0->compacted_start, 0U);
    EXPECT_EQ(r0->compacted_end, 4 * entry_size);
    expect_key(r0->start_key, std::nullopt);
    expect_key(r0->end_key, key_sid("k04"));
    EXPECT_EQ(r0->snapshot_start, 0U);
    EXPECT_EQ(r0->snapshot_end, 2 * entry_size);

    std::optional<region_range> r1 = allocator.allocate(snapshot_reader, &compacted_reader);
    ASSERT_TRUE(r1.has_value());
    EXPECT_EQ(r1->compacted_start, 4 * entry_size);
    EXPECT_EQ(r1->compacted_end, 8 * entry_size);
    expect_key(r1->start_key, key_sid("k04"));
    expect_key(r1->end_key, key_sid("k08"));
    EXPECT_EQ(r1->snapshot_start, 1 * entry_size);
    EXPECT_EQ(r1->snapshot_end, 4 * entry_size);

    std::optional<region_range> r2 = allocator.allocate(snapshot_reader, &compacted_reader);
    ASSERT_TRUE(r2.has_value());
    EXPECT_EQ(r2->compacted_start, 8 * entry_size);
    EXPECT_EQ(r2->compacted_end, 10 * entry_size);
    expect_key(r2->start_key, key_sid("k08"));
    expect_key(r2->end_key, std::nullopt);
    EXPECT_EQ(r2->snapshot_start, 3 * entry_size);
    EXPECT_EQ(r2->snapshot_end, 4 * entry_size);

    EXPECT_FALSE(allocator.allocate(snapshot_reader, &compacted_reader).has_value());
}

// with the same number of records, the compacted WAL file is the primary
TEST_F(region_allocator_test, same_record_count_compacted_is_primary) {
    locator snapshot = make_file("snapshot", entries({"k00", "k02", "k04", "k06"}), 1);
    locator compacted = make_file("compacted", entries({"k01", "k03", "k05", "k07"}), 1);
    region_allocator allocator(snapshot, compacted, 2);
    EXPECT_EQ(allocator.region_count(), 2U);
    log_entry_reader snapshot_reader(path("snapshot"));
    log_entry_reader compacted_reader(path("compacted"));

    // region 0: the primary (compacted) is k01, k03. The end key is k05. The secondary is up to the first record at or
    // above k05 (k06, number 3)
    std::optional<region_range> r0 = allocator.allocate(snapshot_reader, &compacted_reader);
    ASSERT_TRUE(r0.has_value());
    EXPECT_EQ(r0->compacted_start, 0U);
    EXPECT_EQ(r0->compacted_end, 2 * entry_size);
    expect_key(r0->end_key, key_sid("k05"));
    EXPECT_EQ(r0->snapshot_start, 0U);
    EXPECT_EQ(r0->snapshot_end, 3 * entry_size);

    // region 1: the start key is k05. The secondary is from the last record below k05 (k04, number 2) to the end
    std::optional<region_range> r1 = allocator.allocate(snapshot_reader, &compacted_reader);
    ASSERT_TRUE(r1.has_value());
    EXPECT_EQ(r1->compacted_start, 2 * entry_size);
    EXPECT_EQ(r1->compacted_end, 4 * entry_size);
    expect_key(r1->start_key, key_sid("k05"));
    expect_key(r1->end_key, std::nullopt);
    EXPECT_EQ(r1->snapshot_start, 2 * entry_size);
    EXPECT_EQ(r1->snapshot_end, 4 * entry_size);
}

// without a compacted WAL file, the snapshot file is the primary and there is no secondary, whose range is 0, 0
TEST_F(region_allocator_test, no_compacted_file) {
    locator snapshot = make_file("snapshot", entries({"k00", "k01", "k02", "k03", "k04", "k05", "k06", "k07", "k08", "k09"}), 1);
    region_allocator allocator(snapshot, std::nullopt, 3);
    EXPECT_EQ(allocator.region_count(), 4U);
    log_entry_reader snapshot_reader(path("snapshot"));

    for (std::size_t j = 0; j < 4; ++j) {
        std::optional<region_range> r = allocator.allocate(snapshot_reader, nullptr);
        ASSERT_TRUE(r.has_value()) << j;
        EXPECT_EQ(r->snapshot_start, 3 * j * entry_size) << j;
        EXPECT_EQ(r->snapshot_end, std::min<std::uint64_t>(3 * (j + 1), 10) * entry_size) << j;
        EXPECT_EQ(r->compacted_start, 0U) << j;
        EXPECT_EQ(r->compacted_end, 0U) << j;
        expect_key(r->start_key, j == 0 ? std::nullopt : std::optional<std::string>(key_sid("k0" + std::to_string(3 * j))));
        expect_key(r->end_key, j == 3 ? std::nullopt : std::optional<std::string>(key_sid("k0" + std::to_string(3 * (j + 1)))));
    }
    EXPECT_FALSE(allocator.allocate(snapshot_reader, nullptr).has_value());
}

// with R = 1, the number of regions is the number of records
TEST_F(region_allocator_test, one_record_per_region) {
    locator snapshot = make_file("snapshot", entries({"k00", "k01", "k02"}), 1);
    region_allocator allocator(snapshot, std::nullopt, 1);
    EXPECT_EQ(allocator.region_count(), 3U);
    log_entry_reader snapshot_reader(path("snapshot"));
    std::optional<region_range> r1 = allocator.allocate(snapshot_reader, nullptr);
    ASSERT_TRUE(r1.has_value());
    r1 = allocator.allocate(snapshot_reader, nullptr);
    ASSERT_TRUE(r1.has_value());
    EXPECT_EQ(r1->snapshot_start, entry_size);
    EXPECT_EQ(r1->snapshot_end, 2 * entry_size);
    expect_key(r1->start_key, key_sid("k01"));
    expect_key(r1->end_key, key_sid("k02"));
}

// if the record interval is larger than an entry, the records skip entries. The positions of the regions follow the
// positions of the records
TEST_F(region_allocator_test, record_interval_larger_than_entry) {
    // record interval 100 bytes: the positions 0, 114 (the 3rd), 228 (the 6th) and 342 (the 9th) are the records
    locator snapshot = make_file("snapshot", entries({"k00", "k01", "k02", "k03", "k04", "k05", "k06", "k07", "k08", "k09"}), 100);
    ASSERT_EQ(snapshot.record_count(), 4U);
    region_allocator allocator(snapshot, std::nullopt, 2);
    EXPECT_EQ(allocator.region_count(), 2U);
    log_entry_reader snapshot_reader(path("snapshot"));
    std::optional<region_range> r0 = allocator.allocate(snapshot_reader, nullptr);
    ASSERT_TRUE(r0.has_value());
    EXPECT_EQ(r0->snapshot_start, 0U);
    EXPECT_EQ(r0->snapshot_end, 6 * entry_size);
    expect_key(r0->end_key, key_sid("k06"));
    std::optional<region_range> r1 = allocator.allocate(snapshot_reader, nullptr);
    ASSERT_TRUE(r1.has_value());
    EXPECT_EQ(r1->snapshot_start, 6 * entry_size);
    EXPECT_EQ(r1->snapshot_end, 10 * entry_size);
    expect_key(r1->start_key, key_sid("k06"));
    expect_key(r1->end_key, std::nullopt);
}

// if the primary has no record, there is no region, and nothing is left from the first allocate()
TEST_F(region_allocator_test, no_records) {
    locator snapshot = make_file("snapshot", "", 1);
    ASSERT_EQ(snapshot.record_count(), 0U);
    region_allocator allocator(snapshot, std::nullopt, 1);
    EXPECT_EQ(allocator.region_count(), 0U);
    log_entry_reader snapshot_reader(path("snapshot"));
    EXPECT_FALSE(allocator.allocate(snapshot_reader, nullptr).has_value());
}

// if all the keys of the secondary are at or above the start key, the start position of the secondary is 0. If all of
// them are below the end key, the end position of the secondary is the end of the file
TEST_F(region_allocator_test, secondary_without_matching_record) {
    locator snapshot = make_file("snapshot", entries({"k00", "k01", "k02", "k03", "k04", "k05"}), 1);
    region_allocator allocator(snapshot, std::nullopt, 2);
    log_entry_reader snapshot_reader(path("snapshot"));
    {
        // all the keys of the secondary are at or above the start key k02 of region 1, and the first record at or above
        // the end key k04 of region 1 is number 0
        locator compacted = make_file("compacted", entries({"k07", "k08"}), 1);
        region_allocator a(snapshot, compacted, 2);
        log_entry_reader compacted_reader(path("compacted"));
        (void) a.allocate(snapshot_reader, &compacted_reader);
        std::optional<region_range> r1 = a.allocate(snapshot_reader, &compacted_reader);
        ASSERT_TRUE(r1.has_value());
        expect_key(r1->start_key, key_sid("k02"));
        expect_key(r1->end_key, key_sid("k04"));
        EXPECT_EQ(r1->compacted_start, 0U);
        EXPECT_EQ(r1->compacted_end, 0U);
    }
    {
        // all the keys of the secondary are below the end key k04 of region 1. The last record below the start key k02
        // is number 1
        locator compacted = make_file("compacted2", entries({"a00", "a01"}), 1);
        region_allocator a(snapshot, compacted, 2);
        log_entry_reader compacted_reader(path("compacted2"));
        (void) a.allocate(snapshot_reader, &compacted_reader);
        std::optional<region_range> r1 = a.allocate(snapshot_reader, &compacted_reader);
        ASSERT_TRUE(r1.has_value());
        EXPECT_EQ(r1->compacted_start, 1 * entry_size);
        EXPECT_EQ(r1->compacted_end, 2 * entry_size);
    }
}

// when the key of a record of the secondary equals a boundary key: the start position is "the last below the start
// key", so the record before the equal one. The end position is "the first at or above the end key", so the equal one
TEST_F(region_allocator_test, secondary_record_equal_to_boundary_key) {
    locator snapshot = make_file("snapshot", entries({"k00", "k01", "k02", "k03", "k04", "k05"}), 1);
    locator compacted = make_file("compacted", entries({"k01", "k02", "k03", "k04"}), 1);
    region_allocator allocator(snapshot, compacted, 2);
    log_entry_reader snapshot_reader(path("snapshot"));
    log_entry_reader compacted_reader(path("compacted"));
    (void) allocator.allocate(snapshot_reader, &compacted_reader);
    // region 1: the start key is k02 and the end key is k04. The secondary starts at k01 (number 0) and ends at k04
    // (number 3)
    std::optional<region_range> r1 = allocator.allocate(snapshot_reader, &compacted_reader);
    ASSERT_TRUE(r1.has_value());
    EXPECT_EQ(r1->compacted_start, 0U);
    EXPECT_EQ(r1->compacted_end, 3 * entry_size);
}

// even if allocate() is called from several threads at the same time, each region is handed out exactly once
TEST_F(region_allocator_test, allocate_concurrently) {
    std::vector<std::string> keys{};
    for (int i = 0; i < 40; ++i) {
        keys.push_back("k" + std::to_string(10 + i));
    }
    locator snapshot = make_file("snapshot", entries(keys), 1);
    locator compacted = make_file("compacted", entries({"k15", "k25", "k35"}), 1);
    region_allocator allocator(snapshot, compacted, 3);
    ASSERT_EQ(allocator.region_count(), 14U);

    constexpr int thread_count = 4;
    std::vector<std::vector<std::uint64_t>> starts(thread_count);
    std::vector<std::thread> threads{};
    for (int t = 0; t < thread_count; ++t) {
        threads.emplace_back([&, t] {
            log_entry_reader snapshot_reader(path("snapshot"));
            log_entry_reader compacted_reader(path("compacted"));
            while (true) {
                std::optional<region_range> r = allocator.allocate(snapshot_reader, &compacted_reader);
                if (! r.has_value()) {
                    break;
                }
                starts[t].push_back(r->snapshot_start);
            }
        });
    }
    for (std::thread& th : threads) {
        th.join();
    }
    std::multiset<std::uint64_t> all{};
    for (const std::vector<std::uint64_t>& s : starts) {
        all.insert(s.begin(), s.end());
    }
    std::multiset<std::uint64_t> expected{};
    for (std::size_t j = 0; j < 14; ++j) {
        expected.insert(3 * j * entry_size);
    }
    EXPECT_EQ(all, expected);
}

// a probe is one pread (4 KiB or the rest of the file, whichever is smaller) if the entry fits in 4 KiB
TEST_F(region_allocator_test, probe_small_entry) {
    std::string bytes = entries({"k00", "k01", "k02"});
    boost::filesystem::path p = write_file("snapshot", bytes);
    recording_file_operations ops;
    log_entry_reader reader(p, 0, nullptr, ops);
    EXPECT_EQ(region_allocator::probe(reader, entry_size, bytes.size()), key_sid("k01"));
    EXPECT_EQ(ops.requests(), (std::vector<std::size_t>{2 * entry_size}));
    ops.clear();
    std::string large = bytes + std::string(8192, '\0');  // the garbage at the end is not read
    p = write_file("snapshot2", large);
    log_entry_reader reader2(p, 0, nullptr, ops);
    EXPECT_EQ(region_allocator::probe(reader2, entry_size, large.size()), key_sid("k01"));
    EXPECT_EQ(ops.requests(), (std::vector<std::size_t>{4096}));
}

// an entry larger than 4 KiB is read with 4 KiB and then what it needs. normal_with_blob has one more step
TEST_F(region_allocator_test, probe_large_entry) {
    std::string e0 = entry("k00");
    std::string e1 = normal_entry(1, "k01", std::string(10000, 'v'), 7, 1);
    std::string e2 = normal_with_blob(1, "k02", std::string(10000, 'v'), 7, 1, {11, 12});
    std::string bytes = e0 + e1 + e2;
    boost::filesystem::path p = write_file("snapshot", bytes);
    recording_file_operations ops;
    log_entry_reader reader(p, 0, nullptr, ops);
    EXPECT_EQ(region_allocator::probe(reader, e0.size(), bytes.size()), key_sid("k01"));
    EXPECT_EQ(ops.requests(), (std::vector<std::size_t>{4096, e1.size()}));
    ops.clear();
    EXPECT_EQ(region_allocator::probe(reader, e0.size() + e1.size(), bytes.size()), key_sid("k02"));
    EXPECT_EQ(ops.requests(), (std::vector<std::size_t>{4096, e2.size() - 2 * 8, e2.size()}));
}

// failures of a probe: an entry that is not a data entry, an invalid type, a position at or beyond the size of the file
TEST_F(region_allocator_test, probe_fails_on_invalid_entry) {
    std::string e0 = entry("k00");
    std::string bytes = e0 + marker(2, 7) + u8(99);
    boost::filesystem::path p = write_file("snapshot", bytes);
    log_entry_reader reader(p);
    expect_throws([&] { (void) region_allocator::probe(reader, e0.size(), bytes.size()); }, "is not a data entry");
    expect_throws([&] { (void) region_allocator::probe(reader, bytes.size() - 1, bytes.size()); }, "is broken");
    expect_throws([&] { (void) region_allocator::probe(reader, bytes.size(), bytes.size()); }, "is not within the file size");
}

// failures of a probe: the file is smaller than the size recorded in the locator (the entry is cut in the middle, or
// there is no data at the position). Fails without looping
TEST_F(region_allocator_test, probe_fails_on_file_shorter_than_locator) {
    std::string e0 = entry("k00");
    std::string e1 = normal_entry(1, "k01", std::string(10000, 'v'), 7, 1);
    const std::uint64_t claimed_size = e0.size() + e1.size() + e0.size();
    // e1 is cut beyond 4 KiB
    boost::filesystem::path p = write_file("snapshot", e0 + e1.substr(0, 5000));
    log_entry_reader reader(p);
    expect_throws([&] { (void) region_allocator::probe(reader, e0.size(), claimed_size); }, "is cut by the file size");
    // e1 is cut in the middle of the header
    p = write_file("snapshot2", e0 + e1.substr(0, 5));
    log_entry_reader reader2(p);
    expect_throws([&] { (void) region_allocator::probe(reader2, e0.size(), claimed_size); }, "is cut by the file size");
    // no data at the position
    p = write_file("snapshot3", e0);
    log_entry_reader reader3(p);
    expect_throws([&] { (void) region_allocator::probe(reader3, e0.size(), claimed_size); }, "is shorter than the locator says");
    // what the entry needs exceeds the size of the file
    expect_throws([&] { (void) region_allocator::probe(reader, e0.size(), e0.size() + 5000); }, "is cut by the file size");
}

// invalid uses abort: R = 0, and no reader while there is a locator of the compacted WAL file
TEST_F(region_allocator_test, invalid_use_aborts) {
    locator snapshot = make_file("snapshot", entries({"k00", "k01"}), 1);
    locator compacted = make_file("compacted", entries({"k00"}), 1);
    EXPECT_DEATH({ region_allocator a(snapshot, std::nullopt, 0); }, "region records is 0");
    region_allocator allocator(snapshot, compacted, 1);
    log_entry_reader snapshot_reader(path("snapshot"));
    EXPECT_DEATH((void) allocator.allocate(snapshot_reader, nullptr), "no reader for the compacted file");
}

}  // namespace limestone::testing
