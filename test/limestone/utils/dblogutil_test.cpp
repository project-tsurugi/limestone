#include <algorithm>
#include <sstream>
#include <limestone/logging.h>

#include <boost/filesystem.hpp>

#include "dblog_scan.h"
#include "manifest.h"
#include "log_entry.h"
#include "limestone/log/testdata.h"
#include "test_root.h"

namespace limestone::testing {

using namespace std::literals;
using namespace limestone::api;
using namespace limestone::internal;


#define UTIL_COMMAND "../src/tglogutil"

int invoke(const std::string& command, std::string& out) {
    FILE* fp;
    fp = popen(command.c_str(), "r");
    char buf[4096];
    std::ostringstream ss;
    std::size_t rc;
    while ((rc = fread(buf, 1, 4095, fp)) > 0) {
        ss.write(buf, rc);
    }
    out.assign(ss.str());
    LOG(INFO) << "command output:\n--begin\n" << out << "--end";
    return pclose(fp);
}

class dblogutil_test : public ::testing::Test {
public:
static constexpr const char* location = "/tmp/dblogutil_test";
static constexpr const char* metadata_location = "/tmp/dblogutil_test/metadata";

    void SetUp() {
        boost::filesystem::remove_all(location);
        if (!boost::filesystem::create_directory(location)) {
            std::cerr << "cannot make directory" << std::endl;
        }
    }

    void TearDown() {
        boost::filesystem::remove_all(location);
    }

    bool starts_with(std::string a, std::string b) { return a.substr(0, b.length()) == b; }
    bool contains(std::string a, std::string b) { return a.find(b) != a.npos; }
    bool contains_line_starts_with(std::string a, std::string b) { return starts_with(a, b) || contains(a, "\n" + b); }

    std::vector<boost::filesystem::path> list_dir() {
        std::vector<boost::filesystem::path> ret;
        for (const boost::filesystem::path& p : boost::filesystem::directory_iterator(boost::filesystem::path(location))) {
            if (dblog_scan::is_wal(p)) {
                ret.emplace_back(p);
            }
        }
        return ret;
    }

    std::pair<int, std::string> inspect(
        std::string pwal_fname,
        std::string_view data,
        std::string_view epoch = epoch_0x100_str
    ) {
        boost::filesystem::path dir{location};
        create_file(dir / "epoch", epoch);
        create_file(dir / std::string(manifest::file_name), data_manifest());
        auto pwal = dir / pwal_fname;
        create_file(pwal, data);
        std::string command = UTIL_COMMAND " inspect " + dir.string() + " 2>&1";
        std::string out;
        int rc = invoke(command, out);
        return make_pair(rc, out);
    }

    std::pair<int, std::string> repairm(
        std::string pwal_fname,
        std::string_view data,
        std::string_view epoch = epoch_0x100_str
    ) {
        boost::filesystem::path dir{location};
        create_file(dir / "epoch", epoch);
        create_file(dir / std::string(manifest::file_name), data_manifest());
        auto pwal = dir / pwal_fname;
        create_file(pwal, data);
        std::string command = UTIL_COMMAND " repair --cut=false " + dir.string() + " 2>&1";
        std::string out;
        int rc = invoke(command, out);
        auto files = list_dir();
        assert(files.size() == 1);
        assert(dblog_scan::is_detached_wal(files.at(0)));
        return make_pair(rc, out);
    }

    std::tuple<int, std::string, int, std::string> repairm_twice(
        std::string pwal_fname,
        std::string_view data,
        std::string_view epoch = epoch_0x100_str
    ) {
        boost::filesystem::path dir{location};
        create_file(dir / "epoch", epoch);
        create_file(dir / std::string(manifest::file_name), data_manifest());
        auto pwal = dir / pwal_fname;
        create_file(pwal, data);
        std::string command = UTIL_COMMAND " repair --cut=false " + dir.string() + " 2>&1";
        std::string out;
        int rc = invoke(command, out);
        auto files = list_dir();
        assert(files.size() == 1);
        assert(dblog_scan::is_detached_wal(files.at(0)));
        auto data1 = read_entire_file(list_dir()[0]);

        std::string out2;
        int rc2 = invoke(command, out2);
        auto files2 = list_dir();
        assert(files2.size() == 1);
        assert(dblog_scan::is_detached_wal(files2.at(0)));
        auto data2 = read_entire_file(list_dir()[0]);

        assert(data1.size() == data.size());
        assert(data1 == data2);
        return std::make_tuple(rc, out, rc2, out2);
    }

    std::pair<int, std::string> repairc(
        std::string pwal_fname,
        std::string_view data,
        std::string_view epoch = epoch_0x100_str
    ) {
        boost::filesystem::path dir{location};
        create_file(dir / "epoch", epoch);
        create_file(dir / std::string(manifest::file_name), data_manifest());
        auto pwal = dir / pwal_fname;
        create_file(pwal, data);
        std::string command = UTIL_COMMAND " repair --cut=true " + dir.string() + " 2>&1";
        std::string out;
        int rc = invoke(command, out);
        auto files = list_dir();
        assert(files.size() == 1);
        assert(dblog_scan::is_detached_wal(files.at(0)));
        return make_pair(rc, out);
    }

    std::tuple<int, std::string, int, std::string> repairc_twice(
        std::string pwal_fname,
        std::string_view data,
        std::string_view epoch = epoch_0x100_str
    ) {
        boost::filesystem::path dir{location};
        create_file(dir / "epoch", epoch);
        create_file(dir / std::string(manifest::file_name), data_manifest());
        auto pwal = dir / pwal_fname;
        create_file(pwal, data);
        std::string command = UTIL_COMMAND " repair --cut=true " + dir.string() + " 2>&1";
        std::string out;
        int rc = invoke(command, out);
        auto files = list_dir();
        assert(files.size() == 1);
        assert(dblog_scan::is_detached_wal(files.at(0)));
        auto data1 = read_entire_file(list_dir()[0]);

        std::string out2;
        int rc2 = invoke(command, out2);
        auto files2 = list_dir();
        assert(files2.size() == 1);
        assert(dblog_scan::is_detached_wal(files2.at(0)));
        auto data2 = read_entire_file(list_dir()[0]);

        assert(data1.size() <= data.size());
        assert(data1 == data2);
        return std::make_tuple(rc, out, rc2, out2);
    }

    void expect_no_change(std::string_view& from) {
        auto to = read_entire_file(list_dir()[0]);
        EXPECT_EQ(from, to);  // no change
    }

    void expect_mark_at(std::size_t offset, std::string_view& from) {
        auto to = read_entire_file(list_dir()[0]);
        ASSERT_EQ(from.at(offset), '\x02');
        EXPECT_EQ(to.at(offset), '\x06');  // marked
        EXPECT_EQ(from.substr(0, offset), to.substr(0, offset));  // no change before mark
        EXPECT_EQ(from.substr(offset + 1), to.substr(offset + 1));
    }

    void expect_mark_at_from_zero(std::size_t offset, std::string_view& from) {
        auto to = read_entire_file(list_dir()[0]);
        ASSERT_EQ(from.at(offset), '\x00');
        EXPECT_EQ(to.at(offset), '\x06');                         // marked
        EXPECT_EQ(from.substr(0, offset), to.substr(0, offset));  // no change before mark
        EXPECT_EQ(from.substr(offset + 1), to.substr(offset + 1));
    }

    void expect_cut_at(std::size_t offset, std::string_view& from) {
        auto to = read_entire_file(list_dir()[0]);
        ASSERT_TRUE(from.at(offset) == '\x02' || from.at(offset) == '\x06');
        EXPECT_EQ(to.size(), offset);  // cut
        EXPECT_EQ(from.substr(0, offset), to.substr(0, offset));  // no change before cut
    }

};

TEST_F(dblogutil_test, inspect_normal) {
    auto [rc, out] = inspect("pwal_0000", data_normal);
    EXPECT_EQ(rc, 0);
    EXPECT_NE(out.find("\n" "status: OK"), out.npos);
}

TEST_F(dblogutil_test, inspect_normal2) {
    auto [rc, out] = inspect("pwal_0000", data_normal2);
    EXPECT_EQ(rc, 0 << 8);
    EXPECT_NE(out.find("\n" "status: OK"), out.npos);
    EXPECT_NE(out.find("\n" "count-durable-wal-entries: 3"), out.npos);
}

TEST_F(dblogutil_test, inspect_nondurable) {
    auto [rc, out] = inspect("pwal_0000", data_nondurable);
    EXPECT_EQ(rc, 1 << 8);
    EXPECT_NE(out.find("\n" "status: auto-repairable"), out.npos);
}

TEST_F(dblogutil_test, inspect_repaired_nondurable) {
    auto [rc, out] = inspect("pwal_0000", data_repaired_nondurable);
    EXPECT_EQ(rc, 0 << 8);
    EXPECT_NE(out.find("\n" "status: OK"), out.npos);
    EXPECT_NE(out.find("\n" "count-durable-wal-entries: 2"), out.npos);
}

TEST_F(dblogutil_test, inspect_zerofill) {
    auto [rc, out] = inspect("pwal_0000", data_zerofill);
    EXPECT_EQ(rc, 1 << 8);
    EXPECT_NE(out.find("\n" "status: auto-repairable"), out.npos);
}

TEST_F(dblogutil_test, inspect_truncated_normal_entry) {
    auto [rc, out] = inspect("pwal_0000", data_truncated_normal_entry);
    EXPECT_EQ(rc, 1 << 8);
    EXPECT_NE(out.find("\n" "status: auto-repairable"), out.npos);
}

TEST_F(dblogutil_test, inspect_truncated_epoch_header) {
    auto [rc, out] = inspect("pwal_0000", data_truncated_epoch_header);
    EXPECT_EQ(rc, 2 << 8);
    EXPECT_NE(out.find("\n" "status: unrepairable"), out.npos);
}

TEST_F(dblogutil_test, inspect_truncated_invalidated_normal_entry) {
    auto [rc, out] = inspect("pwal_0000", data_truncated_invalidated_normal_entry);
    EXPECT_EQ(rc, 1 << 8);
    EXPECT_NE(out.find("\n" "status: auto-repairable"), out.npos);
}

TEST_F(dblogutil_test, inspect_truncated_invalidated_epoch_header) {
    auto [rc, out] = inspect("pwal_0000", data_truncated_invalidated_epoch_header);
    EXPECT_EQ(rc, 0);
    EXPECT_NE(out.find("\n" "status: OK"), out.npos);
}

TEST_F(dblogutil_test, inspect_allzero) {
    auto [rc, out] = inspect("pwal_0000", data_allzero);
    EXPECT_EQ(rc, 1 << 8);
    EXPECT_NE(out.find("\n" "status: auto-repairable"), out.npos);
}

TEST_F(dblogutil_test, repairm_normal) {
    auto orig_data = data_normal;
    auto [rc, out] = repairm("pwal_0000", orig_data);
    EXPECT_EQ(rc, 0);
    EXPECT_NE(out.find("\n" "status: OK"), out.npos);
    expect_no_change(orig_data);
}

TEST_F(dblogutil_test, repairm_nondurable) {
    auto orig_data = data_nondurable;
    auto [rc, out, rc2, out2] = repairm_twice("pwal_0000", orig_data);
    EXPECT_EQ(rc, 0);
    EXPECT_NE(out.find("\n" "status: repaired"), out.npos);
    EXPECT_EQ(rc2, 0);
    EXPECT_NE(out2.find("\n" "status: OK"), out2.npos);
    expect_mark_at(9, orig_data);
}

TEST_F(dblogutil_test, repairm_nondurable_detached) {
    auto orig_data = data_nondurable;
    auto [rc, out] = repairm("pwal_0000.rotated", orig_data);
    EXPECT_EQ(rc, 0);
    EXPECT_NE(out.find("\n" "status: repaired"), out.npos);
    expect_mark_at(9, orig_data);
}

TEST_F(dblogutil_test, repairm_repaired_nondurable) {
    auto orig_data = data_repaired_nondurable;
    auto [rc, out] = repairm("pwal_0000", orig_data);
    EXPECT_EQ(rc, 0);
    EXPECT_NE(out.find("\n" "status: OK"), out.npos);
    expect_no_change(orig_data);
}

TEST_F(dblogutil_test, repairm_zerofill) {
    auto orig_data = data_zerofill;
    auto [rc, out, rc2, out2] = repairm_twice("pwal_0000", orig_data);
    EXPECT_EQ(rc, 0);
    EXPECT_NE(out.find("\n" "status: repaired"), out.npos);
    EXPECT_EQ(rc2, 0);
    EXPECT_NE(out2.find("\n" "status: OK"), out2.npos);
    expect_mark_at(9, orig_data);
}

TEST_F(dblogutil_test, repairm_zerofill_detached) {
    auto orig_data = data_zerofill;
    auto [rc, out] = repairm("pwal_0000.rotated", orig_data);
    EXPECT_EQ(rc, 0);
    EXPECT_NE(out.find("\n" "status: repaired"), out.npos);
    expect_mark_at(9, orig_data);
}

TEST_F(dblogutil_test, repairm_truncated_normal_entry) {
    auto orig_data = data_truncated_normal_entry;
    auto [rc, out, rc2, out2] = repairm_twice("pwal_0000", orig_data);
    EXPECT_EQ(rc, 0);
    EXPECT_NE(out.find("\n" "status: repaired"), out.npos);
    EXPECT_EQ(rc2, 0);
    EXPECT_NE(out2.find("\n" "status: OK"), out2.npos);
    expect_mark_at(9, orig_data);
}

TEST_F(dblogutil_test, repairm_truncated_normal_entry_detached) {
    auto orig_data = data_truncated_normal_entry;
    auto [rc, out] = repairm("pwal_0000.rotated", orig_data);
    EXPECT_EQ(rc, 0);
    EXPECT_NE(out.find("\n" "status: repaired"), out.npos);
    expect_mark_at(9, orig_data);
}

TEST_F(dblogutil_test, repairm_truncated_epoch_header) {
    auto orig_data = data_truncated_epoch_header;
    auto [rc, out, rc2, out2] = repairm_twice("pwal_0000", orig_data);
    EXPECT_EQ(rc, 16 << 8);
    EXPECT_NE(out.find("\n" "status: unrepairable"), out.npos);
    EXPECT_EQ(rc2, 16 << 8);
    EXPECT_NE(out2.find("\n" "status: unrepairable"), out2.npos);
    expect_no_change(orig_data);
}

TEST_F(dblogutil_test, repairm_truncated_epoch_header_detached) {
    auto orig_data = data_truncated_epoch_header;
    auto [rc, out] = repairm("pwal_0000.rotated", orig_data);
    EXPECT_EQ(rc, 16 << 8);
    EXPECT_NE(out.find("\n" "status: unrepairable"), out.npos);
    expect_no_change(orig_data);
}

TEST_F(dblogutil_test, repairm_truncated_invalidated_normal_entry) {
    auto orig_data = data_truncated_invalidated_normal_entry;
    auto [rc, out] = repairm("pwal_0000", orig_data);
    EXPECT_EQ(rc, 0);
    EXPECT_NE(out.find("\n" "status: OK"), out.npos);
    expect_no_change(orig_data);
}

TEST_F(dblogutil_test, repairm_truncated_invalidated_normal_entry_detached) {
    auto orig_data = data_truncated_invalidated_normal_entry;
    auto [rc, out] = repairm("pwal_0000.rotated", orig_data);
    EXPECT_EQ(rc, 0);
    EXPECT_NE(out.find("\n" "status: OK"), out.npos);
    expect_no_change(orig_data);
}

TEST_F(dblogutil_test, repairm_truncated_invalidated_epoch_header) {
    auto orig_data = data_truncated_invalidated_epoch_header;
    auto [rc, out] = repairm("pwal_0000", orig_data);
    EXPECT_EQ(rc, 0);
    EXPECT_NE(out.find("\n" "status: OK"), out.npos);
    expect_no_change(orig_data);
}

TEST_F(dblogutil_test, repairm_truncated_invalidated_epoch_header_detached) {
    auto orig_data = data_truncated_invalidated_epoch_header;
    auto [rc, out] = repairm("pwal_0000.rotated", orig_data);
    EXPECT_EQ(rc, 0);
    EXPECT_NE(out.find("\n" "status: OK"), out.npos);
    expect_no_change(orig_data);
}

TEST_F(dblogutil_test, repairm_allzero) {
    auto orig_data = data_allzero;
    auto [rc, out] = repairm("pwal_0000", orig_data);
    EXPECT_EQ(rc, 0);
    EXPECT_NE(out.find("\n" "status: repaired"), out.npos);
    expect_mark_at_from_zero(0, orig_data);
}

TEST_F(dblogutil_test, repairc_zerofill) {
    auto orig_data = data_zerofill;
    auto [rc, out, rc2, out2] = repairc_twice("pwal_0000", orig_data);
    EXPECT_EQ(rc, 0);
    EXPECT_NE(out.find("\n" "status: repaired"), out.npos);
    EXPECT_EQ(rc2, 0);
    EXPECT_NE(out2.find("\n" "status: OK"), out2.npos);
    expect_cut_at(9, orig_data);
}

TEST_F(dblogutil_test, repairc_truncated_normal_entry) {
    auto orig_data = data_truncated_normal_entry;
    auto [rc, out, rc2, out2] = repairc_twice("pwal_0000", orig_data);
    EXPECT_EQ(rc, 0);
    EXPECT_NE(out.find("\n" "status: repaired"), out.npos);
    EXPECT_EQ(rc2, 0);
    EXPECT_NE(out2.find("\n" "status: OK"), out2.npos);
    expect_cut_at(9, orig_data);
}

TEST_F(dblogutil_test, repairc_truncated_epoch_header) {
    auto orig_data = data_truncated_epoch_header;
    auto [rc, out, rc2, out2] = repairc_twice("pwal_0000", orig_data);
    EXPECT_EQ(rc,16 << 8);
    EXPECT_NE(out.find("\n" "status: unrepairable"), out.npos);
    EXPECT_EQ(rc2, 16 << 8);
    EXPECT_NE(out2.find("\n" "status: unrepairable"), out2.npos);
    expect_no_change(orig_data);
}

TEST_F(dblogutil_test, repairc_truncated_invalidated_normal_entry) {
    auto orig_data = data_truncated_invalidated_normal_entry;
    auto [rc, out, rc2, out2] = repairc_twice("pwal_0000", orig_data);
    EXPECT_EQ(rc, 0);
    EXPECT_NE(out.find("\n" "status: repaired"), out.npos);
    EXPECT_EQ(rc2, 0);
    EXPECT_NE(out2.find("\n" "status: OK"), out2.npos);
    expect_cut_at(9, orig_data);
}

TEST_F(dblogutil_test, repairc_truncated_invalidated_epoch_header) {
    auto orig_data = data_truncated_invalidated_epoch_header;
    auto [rc, out, rc2, out2] = repairc_twice("pwal_0000", orig_data);
    EXPECT_EQ(rc, 0);
    EXPECT_NE(out.find("\n" "status: OK"), out.npos);
    EXPECT_EQ(rc2, 0);
    EXPECT_NE(out2.find("\n" "status: OK"), out2.npos);
    expect_no_change(orig_data);
}

TEST_F(dblogutil_test, repairc_allzero) {
    auto orig_data = data_allzero;
    auto [rc, out] = repairc("pwal_0000", orig_data);
    EXPECT_EQ(rc, 0);
    EXPECT_TRUE(contains_line_starts_with(out, "status: repaired"));
    auto to = read_entire_file(list_dir()[0]);
    EXPECT_TRUE(to.empty()); // allzero file is cut to empty
}

TEST_F(dblogutil_test, repair_nonexistent) {
    boost::filesystem::path dir{location};
    dir /= "nonexistent";
    std::string command;
    command = UTIL_COMMAND " repair " + dir.string() + " 2>&1";
    std::string out;
    int rc = invoke(command, out);
    EXPECT_GE(rc, 64 << 8);
    EXPECT_TRUE(contains_line_starts_with(out, "E"));  // LOG(ERROR)
    EXPECT_TRUE(contains(out, "not exist"));
}

TEST_F(dblogutil_test, repair_unreadable) {
    // root can read directories w/o permissions
    if (geteuid() == 0) { GTEST_SKIP() << "skip when run by root"; }

    boost::filesystem::path dir{location};
    dir /= "unreadable";
    boost::filesystem::create_directory(dir);
    boost::filesystem::permissions(dir, boost::filesystem::no_perms);  // drop dir permission
    std::string command;
    command = UTIL_COMMAND " repair " + dir.string() + " 2>&1";
    std::string out;
    int rc = invoke(command, out);
    EXPECT_GE(rc, 64 << 8);
    EXPECT_TRUE(contains_line_starts_with(out, "E"));  // LOG(ERROR)
    EXPECT_TRUE(contains(out, "Permission denied"));
    boost::filesystem::permissions(dir, boost::filesystem::owner_all);
}

TEST_F(dblogutil_test, repair_nondblogdir) {
    boost::filesystem::path dir{location};  // assume empty dir
    std::string command;
    command = UTIL_COMMAND " repair " + dir.string() + " 2>&1";
    std::string out;
    int rc = invoke(command, out);
    EXPECT_GE(rc, 64 << 8);
    EXPECT_TRUE(contains_line_starts_with(out, "E"));  // LOG(ERROR)
    EXPECT_TRUE(contains(out, "unsupport"));
}

TEST_F(dblogutil_test, repair_cannot_rotate) {
    // root can write directories w/o permissions
    if (geteuid() == 0) { GTEST_SKIP() << "skip when run by root"; }

    boost::filesystem::path dir{location};
    dir /= "unwriteable";
    boost::filesystem::create_directory(dir);
    create_file(dir / "epoch", epoch_0x100_str);
    create_file(dir / std::string(manifest::file_name), data_manifest());
    create_file(dir / "pwal_0000", data_zerofill);
    boost::filesystem::permissions(dir, boost::filesystem::owner_read | boost::filesystem::owner_exe);  // drop dir write permission
    std::string command;
    command = UTIL_COMMAND " repair " + dir.string() + " 2>&1";
    std::string out;
    int rc = invoke(command, out);
    EXPECT_GE(rc, 64 << 8);
    EXPECT_TRUE(contains_line_starts_with(out, "E"));  // LOG(ERROR)
    EXPECT_TRUE(contains(out, "Permission denied"));
    boost::filesystem::permissions(dir, boost::filesystem::owner_all);
}

TEST_F(dblogutil_test, repair_cannot_modify) {
    // root can write directories w/o permissions
    if (geteuid() == 0) { GTEST_SKIP() << "skip when run by root"; }

    boost::filesystem::path dir{location};
    dir /= "unwriteable";
    auto pwal = dir / "pwal_0000.rotated";
    boost::filesystem::create_directory(dir);
    create_file(dir / "epoch", epoch_0x100_str);
    create_file(dir / std::string(manifest::file_name), data_manifest());
    create_file(pwal, data_zerofill);
    boost::filesystem::permissions(pwal, boost::filesystem::owner_read);  // drop file write permission
    std::string command;
    command = UTIL_COMMAND " repair " + dir.string() + " 2>&1";
    std::string out;
    int rc = invoke(command, out);
    EXPECT_GE(rc, 64 << 8);
    EXPECT_TRUE(contains_line_starts_with(out, "E"));  // LOG(ERROR)
    EXPECT_TRUE(contains(out, "Permission denied") || contains(out, "cannot open"));
    boost::filesystem::permissions(dir, boost::filesystem::owner_all);
}

TEST_F(dblogutil_test, invalid_epoch_option1) {
    boost::filesystem::path dir{location};
    std::string command;
    command = UTIL_COMMAND " repair --epoch=Z " + dir.string() + " 2>&1";
    std::string out;
    int rc = invoke(command, out);
    EXPECT_GE(rc, 64 << 8);
    EXPECT_TRUE(contains_line_starts_with(out, "E"));  // LOG(ERROR)
    EXPECT_TRUE(contains(out, "invalid"));
}

TEST_F(dblogutil_test, invalid_epoch_option2) {
    boost::filesystem::path dir{location};
    std::string command;
    command = UTIL_COMMAND " repair --epoch=0x100 " + dir.string() + " 2>&1";
    std::string out;
    int rc = invoke(command, out);
    EXPECT_GE(rc, 64 << 8);
    EXPECT_TRUE(contains_line_starts_with(out, "E"));  // LOG(ERROR)
    EXPECT_TRUE(contains(out, "invalid"));
}

TEST_F(dblogutil_test, invalid_epoch_option3) {
    boost::filesystem::path dir{location};
    std::string command;
    command = UTIL_COMMAND " repair --epoch=99999999999999999999 " + dir.string() + " 2>&1";
    std::string out;
    int rc = invoke(command, out);
    EXPECT_GE(rc, 64 << 8);
    EXPECT_TRUE(contains_line_starts_with(out, "E"));  // LOG(ERROR)
    EXPECT_TRUE(contains(out, "invalid"));
}

TEST_F(dblogutil_test, execution_fails_while_active_datastore) {
    // Inactive datastore
    auto [rc, out] = inspect("pwal_0000", data_normal);
    EXPECT_EQ(rc, 0);
    EXPECT_NE(out.find("\n" "status: OK"), out.npos);

    // Activate datastore
    std::vector<boost::filesystem::path> data_locations{};
    data_locations.emplace_back(location);
    boost::filesystem::path metadata_location_path{metadata_location};
    limestone::api::configuration conf(data_locations, metadata_location_path);
    auto ds1 = std::make_unique<limestone::api::datastore_test>(conf);
    ds1->ready();

    // Attempt to run inspect while datastore is active
    auto [rc_active, out_active] = inspect("pwal_0000", data_normal);
    EXPECT_NE(rc_active, 0);
    std::cerr << out_active << std::endl;
    EXPECT_TRUE(contains(out_active, "Log directory \"/tmp/dblogutil_test\" is already in use by another process. Operation aborted."));

    // Inactive datastore
    ds1->shutdown();
    ds1 = nullptr;
    auto [rc_inactive, out_inacive] = inspect("pwal_0000", data_normal);
    EXPECT_EQ(rc_inactive, 0);
    EXPECT_NE(out_inacive.find("\n" "status: OK"), out.npos);
}


TEST_F(dblogutil_test, inspect_marker_end_only) {
    auto [rc, out] = inspect("pwal_0000", data_marker_end_only);
    EXPECT_EQ(rc, 0);
    EXPECT_NE(out.find("\n" "status: OK"), out.npos);
    EXPECT_NE(out.find("\n" "count-durable-wal-entries: 1"), out.npos);
}

TEST_F(dblogutil_test, repairm_marker_end_only) {
    auto orig_data = data_marker_end_only;
    auto [rc, out] = repairm("pwal_0000", orig_data);
    EXPECT_EQ(rc, 0);
    EXPECT_NE(out.find("\n" "status: OK"), out.npos);
    expect_no_change(orig_data);
}

TEST_F(dblogutil_test, repairc_marker_end_only) {
    auto orig_data = data_marker_end_only;
    auto [rc, out] = repairc("pwal_0000", orig_data);
    EXPECT_EQ(rc, 0);
    EXPECT_NE(out.find("\n" "status: OK"), out.npos);
    expect_no_change(orig_data);
}

TEST_F(dblogutil_test, inspect_marker_end_followed_by_normal_entry) {
    auto [rc, out] = inspect("pwal_0000", data_marker_end_followed_by_normal_entry);
    EXPECT_EQ(rc, 2 << 8);
    EXPECT_NE(out.find("\n" "status: unrepairable"), out.npos);
}

TEST_F(dblogutil_test, repairm_marker_end_followed_by_normal_entry) {
    auto orig_data = data_marker_end_followed_by_normal_entry;
    auto [rc, out] = repairm("pwal_0000", orig_data);
    EXPECT_EQ(rc, 16 << 8);
    EXPECT_NE(out.find("\n" "status: unrepairable"), out.npos);
    expect_no_change(orig_data);
}

TEST_F(dblogutil_test, repairc_marker_end_followed_by_normal_entry) {
    auto orig_data = data_marker_end_followed_by_normal_entry;
    auto [rc, out] = repairc("pwal_0000", orig_data);
    EXPECT_EQ(rc, 16 << 8);
    EXPECT_NE(out.find("\n" "status: unrepairable"), out.npos);
    expect_no_change(orig_data);
}

TEST_F(dblogutil_test, inspect_marker_end_followed_by_marker_begin) {
    auto [rc, out] = inspect("pwal_0000", data_marker_end_followed_by_marker_begin);
    EXPECT_EQ(rc, 1 << 8);
    EXPECT_NE(out.find("\n" "status: auto-repairable"), out.npos);
}

TEST_F(dblogutil_test, repairm_marker_end_followed_by_marker_begin) {
    auto orig_data = data_marker_end_followed_by_marker_begin;
    auto [rc, out, rc2, out2] = repairm_twice("pwal_0000", orig_data);
    EXPECT_EQ(rc, 0);
    EXPECT_NE(out.find("\n" "status: repaired"), out.npos);
    EXPECT_EQ(rc2, 0);
    EXPECT_NE(out2.find("\n" "status: OK"), out2.npos);
    expect_mark_at(59, orig_data);
}

TEST_F(dblogutil_test, repairc_marker_end_followed_by_marker_begin) {
    auto orig_data = data_marker_end_followed_by_marker_begin;
    auto [rc, out, rc2, out2] = repairc_twice("pwal_0000", orig_data);
    EXPECT_EQ(rc, 0);
    EXPECT_NE(out.find("\n" "status: repaired"), out.npos);
    EXPECT_EQ(rc2, 0);
    EXPECT_NE(out2.find("\n" "status: OK"), out2.npos);
    expect_mark_at(59, orig_data);
}

TEST_F(dblogutil_test, inspect_marker_end_followed_by_marker_inv_begin) {
    auto [rc, out] = inspect("pwal_0000", data_marker_end_followed_by_marker_inv_begin);
    EXPECT_EQ(rc, 0);
    EXPECT_NE(out.find("\n" "status: OK"), out.npos);
}

TEST_F(dblogutil_test, repairm_marker_end_followed_by_marker_inv_begin) {
    auto orig_data = data_marker_end_followed_by_marker_inv_begin;
    auto [rc, out] = repairm("pwal_0000", orig_data);
    EXPECT_EQ(rc, 0);
    EXPECT_NE(out.find("\n" "status: OK"), out.npos);
    expect_no_change(orig_data);
}

TEST_F(dblogutil_test, repairc_marker_end_followed_by_marker_inv_begin) {
    auto orig_data = data_marker_end_followed_by_marker_inv_begin;
    auto [rc, out] = repairc("pwal_0000", orig_data);
    EXPECT_EQ(rc, 0);
    EXPECT_NE(out.find("\n" "status: OK"), out.npos);
    expect_no_change(orig_data);
}

TEST_F(dblogutil_test, inspect_marker_end_followed_by_short_entry) {
    auto [rc, out] = inspect("pwal_0000", data_marker_end_followed_by_short_entry);
    EXPECT_EQ(rc, 2 << 8);
    EXPECT_NE(out.find("\n" "status: unrepairable"), out.npos);
}

TEST_F(dblogutil_test, repairm_marker_end_followed_by_short_entry) {
    auto orig_data = data_marker_end_followed_by_short_entry;
    auto [rc, out] = repairm("pwal_0000", orig_data);
    EXPECT_EQ(rc, 16 << 8);
    EXPECT_NE(out.find("\n" "status: unrepairable"), out.npos);
    expect_no_change(orig_data);
}

TEST_F(dblogutil_test, repairc_marker_end_followed_by_short_entry) {
    auto orig_data = data_marker_end_followed_by_short_entry;
    auto [rc, out] = repairc("pwal_0000", orig_data);
    EXPECT_EQ(rc, 16 << 8);
    EXPECT_NE(out.find("\n" "status: unrepairable"), out.npos);
    expect_no_change(orig_data);
}

TEST_F(dblogutil_test, inspect_short_marker_end_only) {
    auto [rc, out] = inspect("pwal_0000", data_short_marker_end_only);
    EXPECT_EQ(rc, 2 << 8);
    EXPECT_NE(out.find("\n" "status: unrepairable"), out.npos);
}

TEST_F(dblogutil_test, repairm_short_marker_end_only) {
    auto orig_data = data_short_marker_end_only;
    auto [rc, out] = repairm("pwal_0000", orig_data);
    EXPECT_EQ(rc, 16 << 8);
    EXPECT_NE(out.find("\n" "status: unrepairable"), out.npos);
    expect_no_change(orig_data);
}

TEST_F(dblogutil_test, repairc_short_marker_end_only) {
    auto orig_data = data_short_marker_end_only;
    auto [rc, out] = repairc("pwal_0000", orig_data);
    EXPECT_EQ(rc, 16 << 8);
    EXPECT_NE(out.find("\n" "status: unrepairable"), out.npos);
    expect_no_change(orig_data);
}

TEST_F(dblogutil_test, inspect_marker_begin_partial_zerofill) {
    auto [rc, out] = inspect("pwal_0000", data_marker_begin_partial_zerofill);
    EXPECT_EQ(rc, 1 << 8);
    EXPECT_NE(out.find("\n" "status: auto-repairable"), out.npos);
}

TEST_F(dblogutil_test, repairm_marker_begin_partial_zerofill) {
    auto orig_data = data_marker_begin_partial_zerofill;
    auto [rc, out, rc2, out2] = repairm_twice("pwal_0000", orig_data);
    EXPECT_EQ(rc, 0);
    EXPECT_NE(out.find("\n" "status: repaired"), out.npos);
    EXPECT_EQ(rc2, 0);
    EXPECT_NE(out2.find("\n" "status: OK"), out2.npos);
    expect_mark_at(0, orig_data);
}

TEST_F(dblogutil_test, repairc_marker_begin_partial_zerofill) {
    auto orig_data = data_marker_begin_partial_zerofill;
    auto [rc, out, rc2, out2] = repairc_twice("pwal_0000", orig_data);
    EXPECT_EQ(rc, 0);
    EXPECT_NE(out.find("\n" "status: repaired"), out.npos);
    EXPECT_EQ(rc2, 0);
    EXPECT_NE(out2.find("\n" "status: OK"), out2.npos);
    expect_cut_at(0, orig_data);
}

TEST_F(dblogutil_test, inspect_marker_begin_followed_by_zerofill) {
    auto [rc, out] = inspect("pwal_0000", data_marker_begin_followed_by_zerofill);
    EXPECT_EQ(rc, 2 << 8);
    EXPECT_NE(out.find("\n" "status: unrepairable"), out.npos);
}

TEST_F(dblogutil_test, repairm_marker_begin_followed_by_zerofill) {
    auto orig_data = data_marker_begin_followed_by_zerofill;
    auto [rc, out] = repairm("pwal_0000", orig_data);
    EXPECT_EQ(rc, 16 << 8);
    EXPECT_NE(out.find("\n" "status: unrepairable"), out.npos);
    expect_no_change(orig_data);
}

TEST_F(dblogutil_test, repairc_marker_begin_followed_by_zerofill) {
    auto orig_data = data_marker_begin_followed_by_zerofill;
    auto [rc, out] = repairc("pwal_0000", orig_data);
    EXPECT_EQ(rc, 16 << 8);
    EXPECT_NE(out.find("\n" "status: unrepairable"), out.npos);
    expect_no_change(orig_data);
}

TEST_F(dblogutil_test, inspect_marker_begin_normal_entry_partial_zerofill) {
    auto [rc, out] = inspect("pwal_0000", data_marker_begin_normal_entry_partial_zerofill);
    EXPECT_EQ(rc, 2 << 8);
    EXPECT_NE(out.find("\n" "status: unrepairable"), out.npos);
}

TEST_F(dblogutil_test, repairm_marker_begin_normal_entry_partial_zerofill) {
    auto orig_data = data_marker_begin_normal_entry_partial_zerofill;
    auto [rc, out] = repairm("pwal_0000", orig_data);
    EXPECT_EQ(rc, 16 << 8);
    EXPECT_NE(out.find("\n" "status: unrepairable"), out.npos);
    expect_no_change(orig_data);
}

TEST_F(dblogutil_test, repairc_marker_begin_normal_entry_partial_zerofill) {
    auto orig_data = data_marker_begin_normal_entry_partial_zerofill;
    auto [rc, out] = repairc("pwal_0000", orig_data);
    EXPECT_EQ(rc, 16 << 8);
    EXPECT_NE(out.find("\n" "status: unrepairable"), out.npos);
    expect_no_change(orig_data);
}

TEST_F(dblogutil_test, inspect_marker_begin_normal_entry_followed_by_zerofill) {
    auto [rc, out] = inspect("pwal_0000", data_marker_begin_normal_entry_followed_by_zerofill);
    EXPECT_EQ(rc, 2 << 8);
    EXPECT_NE(out.find("\n" "status: unrepairable"), out.npos);
}

TEST_F(dblogutil_test, repairm_marker_begin_normal_entry_followed_by_zerofill) {
    auto orig_data = data_marker_begin_normal_entry_followed_by_zerofill;
    auto [rc, out] = repairm("pwal_0000", orig_data);
    EXPECT_EQ(rc, 16 << 8);
    EXPECT_NE(out.find("\n" "status: unrepairable"), out.npos);
    expect_no_change(orig_data);
}

TEST_F(dblogutil_test, repairc_marker_begin_normal_entry_followed_by_zerofill) {
    auto orig_data = data_marker_begin_normal_entry_followed_by_zerofill;
    auto [rc, out] = repairc("pwal_0000", orig_data);
    EXPECT_EQ(rc, 16 << 8);
    EXPECT_NE(out.find("\n" "status: unrepairable"), out.npos);
    expect_no_change(orig_data);
}

TEST_F(dblogutil_test, inspect_marker_end_partial_zerofill) {
    auto [rc, out] = inspect("pwal_0000", data_marker_end_partial_zerofill);
    EXPECT_EQ(rc, 2 << 8);
    EXPECT_NE(out.find("\n" "status: unrepairable"), out.npos);
}

TEST_F(dblogutil_test, repairm_marker_end_partial_zerofill) {
    auto orig_data = data_marker_end_partial_zerofill;
    auto [rc, out] = repairm("pwal_0000", orig_data);
    EXPECT_EQ(rc, 16 << 8);
    EXPECT_NE(out.find("\n" "status: unrepairable"), out.npos);
    expect_no_change(orig_data);
}

TEST_F(dblogutil_test, repairc_marker_end_partial_zerofill) {
    auto orig_data = data_marker_end_partial_zerofill;
    auto [rc, out] = repairc("pwal_0000", orig_data);
    EXPECT_EQ(rc, 16 << 8);
    EXPECT_NE(out.find("\n" "status: unrepairable"), out.npos);
    expect_no_change(orig_data);
}

TEST_F(dblogutil_test, inspect_marker_begin_followed_by_zerofill_epoch_ff) {
    auto [rc, out] = inspect("pwal_0000", data_marker_begin_followed_by_zerofill, epoch_0xff_str);
    EXPECT_EQ(rc, 1 << 8);
    EXPECT_NE(out.find("\n" "status: auto-repairable"), out.npos);
}

TEST_F(dblogutil_test, repairm_marker_begin_followed_by_zerofill_epoch_ff) {
    auto orig_data = data_marker_begin_followed_by_zerofill;
    auto [rc, out, rc2, out2] = repairm_twice("pwal_0000", orig_data, epoch_0xff_str);
    EXPECT_EQ(rc, 0);
    EXPECT_NE(out.find("\n" "status: repaired"), out.npos);
    EXPECT_EQ(rc2, 0);
    EXPECT_NE(out2.find("\n" "status: OK"), out2.npos);
    expect_mark_at(0, orig_data);
}

TEST_F(dblogutil_test, repairc_marker_begin_followed_by_zerofill_epoch_ff) {
    auto orig_data = data_marker_begin_followed_by_zerofill;
    auto [rc, out, rc2, out2] = repairc_twice("pwal_0000", orig_data, epoch_0xff_str);
    EXPECT_EQ(rc, 0);
    EXPECT_NE(out.find("\n" "status: repaired"), out.npos);
    EXPECT_EQ(rc2, 0);
    EXPECT_NE(out2.find("\n" "status: OK"), out2.npos);
    expect_cut_at(0, orig_data);
}

TEST_F(dblogutil_test, inspect_all_zerofill_epoch_ff) {
    auto [rc, out] = inspect("pwal_0000", data_all_zerofill, epoch_0xff_str);
    EXPECT_EQ(rc, 1 << 8);
    EXPECT_NE(out.find("\n" "status: auto-repairable"), out.npos);
}

TEST_F(dblogutil_test, repairm_all_zerofill_epoch_ff) {
    auto orig_data = data_all_zerofill;
    auto [rc, out, rc2, out2] = repairm_twice("pwal_0000", orig_data, epoch_0xff_str);
    EXPECT_EQ(rc, 0);
    EXPECT_NE(out.find("\n" "status: repaired"), out.npos);
    EXPECT_EQ(rc2, 0);
    EXPECT_NE(out2.find("\n" "status: OK"), out2.npos);
    expect_mark_at_from_zero(0, orig_data);
}

TEST_F(dblogutil_test, repairc_all_zerofill_epoch_ff) {
    auto orig_data = data_all_zerofill;
    auto [rc, out] = repairc("pwal_0000", orig_data, epoch_0xff_str);
    EXPECT_EQ(rc, 0);
    EXPECT_TRUE(contains_line_starts_with(out, "status: repaired"));
    auto to = read_entire_file(list_dir()[0]);
    EXPECT_TRUE(to.empty()); // all zero file is cut to empty
}

TEST_F(dblogutil_test, inspect_marker_begin_partial_zerofill_epoch_ff) {
    auto [rc, out] = inspect("pwal_0000", data_marker_begin_partial_zerofill, epoch_0xff_str);
    EXPECT_EQ(rc, 1 << 8);
    EXPECT_NE(out.find("\n" "status: auto-repairable"), out.npos);
}

TEST_F(dblogutil_test, repairm_marker_begin_partial_zerofill_epoch_ff) {
    auto orig_data = data_marker_begin_partial_zerofill;
    auto [rc, out, rc2, out2] = repairm_twice("pwal_0000", orig_data, epoch_0xff_str);
    EXPECT_EQ(rc, 0);
    EXPECT_NE(out.find("\n" "status: repaired"), out.npos);
    EXPECT_EQ(rc2, 0);
    EXPECT_NE(out2.find("\n" "status: OK"), out2.npos);
    expect_mark_at(0, orig_data);
}

TEST_F(dblogutil_test, repairc_marker_begin_partial_zerofill_epoch_ff) {
    auto orig_data = data_marker_begin_partial_zerofill;
    auto [rc, out, rc2, out2] = repairc_twice("pwal_0000", orig_data, epoch_0xff_str);
    EXPECT_EQ(rc, 0);
    EXPECT_NE(out.find("\n" "status: repaired"), out.npos);
    EXPECT_EQ(rc2, 0);
    EXPECT_NE(out2.find("\n" "status: OK"), out2.npos);
    expect_cut_at(0, orig_data);
}

TEST_F(dblogutil_test, inspect_marker_begin_normal_entry_partial_zerofill_epoch_ff) {
    auto [rc, out] = inspect("pwal_0000", data_marker_begin_normal_entry_partial_zerofill, epoch_0xff_str);
    EXPECT_EQ(rc, 1 << 8);
    EXPECT_NE(out.find("\n" "status: auto-repairable"), out.npos);
}

TEST_F(dblogutil_test, repairm_marker_begin_normal_entry_partial_zerofill_epoch_ff) {
    auto orig_data = data_marker_begin_normal_entry_partial_zerofill;
    auto [rc, out, rc2, out2] = repairm_twice("pwal_0000", orig_data, epoch_0xff_str);
    EXPECT_EQ(rc, 0);
    EXPECT_NE(out.find("\n" "status: repaired"), out.npos);
    EXPECT_EQ(rc2, 0);
    EXPECT_NE(out2.find("\n" "status: OK"), out2.npos);
    expect_mark_at(0, orig_data);
}

TEST_F(dblogutil_test, repairc_marker_begin_normal_entry_partial_zerofill_epoch_ff) {
    auto orig_data = data_marker_begin_normal_entry_partial_zerofill;
    auto [rc, out, rc2, out2] = repairc_twice("pwal_0000", orig_data, epoch_0xff_str);
    EXPECT_EQ(rc, 0);
    EXPECT_NE(out.find("\n" "status: repaired"), out.npos);
    EXPECT_EQ(rc2, 0);
    EXPECT_NE(out2.find("\n" "status: OK"), out2.npos);
    expect_cut_at(0, orig_data);
}

TEST_F(dblogutil_test, inspect_marker_begin_normal_entry_followed_by_zerofill_epoch_ff) {
    auto [rc, out] = inspect("pwal_0000", data_marker_begin_normal_entry_followed_by_zerofill, epoch_0xff_str);
    EXPECT_EQ(rc, 1 << 8);
    EXPECT_NE(out.find("\n" "status: auto-repairable"), out.npos);
}

TEST_F(dblogutil_test, repairm_marker_begin_normal_entry_followed_by_zerofill_epoch_ff) {
    auto orig_data = data_marker_begin_normal_entry_followed_by_zerofill;
    auto [rc, out, rc2, out2] = repairm_twice("pwal_0000", orig_data, epoch_0xff_str);
    EXPECT_EQ(rc, 0);
    EXPECT_NE(out.find("\n" "status: repaired"), out.npos);
    EXPECT_EQ(rc2, 0);
    EXPECT_NE(out2.find("\n" "status: OK"), out2.npos);
    expect_mark_at(0, orig_data);
}

TEST_F(dblogutil_test, repairc_marker_begin_normal_entry_followed_by_zerofill_epoch_ff) {
    auto orig_data = data_marker_begin_normal_entry_followed_by_zerofill;
    auto [rc, out, rc2, out2] = repairc_twice("pwal_0000", orig_data, epoch_0xff_str);
    EXPECT_EQ(rc, 0);
    EXPECT_NE(out.find("\n" "status: repaired"), out.npos);
    EXPECT_EQ(rc2, 0);
    EXPECT_NE(out2.find("\n" "status: OK"), out2.npos);
    expect_cut_at(0, orig_data);
}

TEST_F(dblogutil_test, inspect_marker_end_partial_zerofill_epoch_ff) {
    auto [rc, out] = inspect("pwal_0000", data_marker_end_partial_zerofill, epoch_0xff_str);
    EXPECT_EQ(rc, 1 << 8);
    EXPECT_NE(out.find("\n" "status: auto-repairable"), out.npos);
}

TEST_F(dblogutil_test, repairm_marker_end_partial_zerofill_epoch_ff) {
    auto orig_data = data_marker_end_partial_zerofill;
    auto [rc, out, rc2, out2] = repairm_twice("pwal_0000", orig_data, epoch_0xff_str);
    EXPECT_EQ(rc, 0);
    EXPECT_NE(out.find("\n" "status: repaired"), out.npos);
    EXPECT_EQ(rc2, 0);
    EXPECT_NE(out2.find("\n" "status: OK"), out2.npos);
    expect_mark_at(0, orig_data);
}

TEST_F(dblogutil_test, repairc_marker_end_partial_zerofill_epoch_ff) {
    auto orig_data = data_marker_end_partial_zerofill;
    auto [rc, out, rc2, out2] = repairc_twice("pwal_0000", orig_data, epoch_0xff_str);
    EXPECT_EQ(rc, 0);
    EXPECT_NE(out.find("\n" "status: repaired"), out.npos);
    EXPECT_EQ(rc2, 0);
    EXPECT_NE(out2.find("\n" "status: OK"), out2.npos);
    expect_cut_at(0, orig_data);
}

} // namespace limestone::testing
