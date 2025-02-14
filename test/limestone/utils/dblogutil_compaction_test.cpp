
#include <algorithm>
#include <sstream>
#include <limestone/logging.h>

#include <boost/filesystem.hpp>

#include "dblog_scan.h"
#include "internal.h"
#include "log_entry.h"

#include "test_root.h"

namespace limestone::testing {

using namespace std::literals;
using namespace limestone::api;
using namespace limestone::internal;

extern void create_file(const boost::filesystem::path& path, std::string_view content);
extern std::string read_entire_file(const boost::filesystem::path& path);
extern std::string data_manifest(int persistent_format_version = 1);

extern const std::string_view epoch_0x100_str;

#define UTIL_COMMAND "../src/tglogutil"

static int invoke(const std::string& command, std::string& out) {
    FILE* fp;
    fp = popen(command.c_str(), "r");
    char buf[4096];
    std::ostringstream ss;
    std::size_t rc;
    while ((rc = fread(buf, 1, 4095, fp)) > 0) {
        ss.write(buf, rc);
    }
    out.assign(ss.str());
    LOG(INFO) << "\n" << out;
    return pclose(fp);
}

class dblogutil_compaction_test : public ::testing::Test {
public:
static constexpr const char* location = "/tmp/dblogutil_compaction_test";

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
        return list_dir(boost::filesystem::path(location));
    }

    std::vector<boost::filesystem::path> list_dir(boost::filesystem::path dir) {
        std::vector<boost::filesystem::path> ret;
        for (const boost::filesystem::path& p : boost::filesystem::directory_iterator(dir)) {
            if (dblog_scan::is_wal(p)) {
                ret.emplace_back(p);
            }
        }
        return ret;
    }

};

extern constexpr const std::string_view data_case1_epoch =
    "\x04\x00\x00\x00\x00\x00\x00\x00\x00"  // epoch 0
    "\x04\x00\x01\x00\x00\x00\x00\x00\x00"  // epoch 0x100
    ""sv;

extern constexpr const std::string_view data_case1_pwal0 =
    "\x02\xf0\x00\x00\x00\x00\x00\x00\x00"  // marker_begin 0xf0
    "\x01\x01\x00\x00\x00\x01\x00\x00\x00" "storage1" "A" "\xf0\0\0\0\0\0\0\0" "verminor" "0"  // normal_entry
    "\x01\x01\x00\x00\x00\x01\x00\x00\x00" "storage1" "B" "\xf0\0\0\0\0\0\0\0" "verminor" "0"  // normal_entry
    "\x01\x01\x00\x00\x00\x01\x00\x00\x00" "storage1" "C" "\xf0\0\0\0\0\0\0\0" "verminor" "0"  // normal_entry
    // XXX: epoch footer...
    ""sv;

extern constexpr const std::string_view data_case1_pwal1 =
    "\x02\xf1\x00\x00\x00\x00\x00\x00\x00"  // marker_begin 0xf1
    "\x01\x01\x00\x00\x00\x01\x00\x00\x00" "storage1" "A" "\xf1\0\0\0\0\0\0\0" "verminor" "1"  // normal_entry
    // XXX: epoch footer...
    "\x02\xf2\x00\x00\x00\x00\x00\x00\x00"  // marker_begin 0xf2
    "\x05\x01\x00\x00\x00"                 "storage1" "C" "\xf2\0\0\0\0\0\0\0" "verminor"  // remove_entry
    // XXX: epoch footer...
    ""sv;

extern constexpr const std::string_view data_case1_pwalcompact =
    "\x02\x00\x00\x00\x00\x00\x00\x00\x00"
    "\x01\x01\x00\x00\x00\x01\x00\x00\x00" "storage1" "A" "\xf1\0\0\0\0\0\0\0" "verminor" "1"  // normal_entry
    "\x01\x01\x00\x00\x00\x01\x00\x00\x00" "storage1" "B" "\xf0\0\0\0\0\0\0\0" "verminor" "0"  // normal_entry
    // XXX: epoch footer...
    ""sv;

extern const std::string_view data_case1_epochcompact = epoch_0x100_str;

TEST_F(dblogutil_compaction_test, case1force) {
    boost::filesystem::path dir{location};
    dir /= "log";
    boost::filesystem::create_directory(dir);
    create_file(dir / "epoch", data_case1_epoch);
    create_file(dir / std::string(manifest_file_name), data_manifest());
    create_file(dir / "pwal_0000", data_case1_pwal0);
    create_file(dir / "pwal_0001", data_case1_pwal1);
    std::string command;
    command = UTIL_COMMAND " compaction --force " + dir.string() + " 2>&1";
    std::string out;
    int rc = invoke(command, out);
    EXPECT_GE(rc, 0 << 8);
    EXPECT_TRUE(contains(out, "compaction was successfully completed: "));
    EXPECT_EQ(read_entire_file(list_dir(dir)[0]), data_case1_pwalcompact);
    EXPECT_EQ(read_entire_file(dir / "epoch"), data_case1_epochcompact);
}

TEST_F(dblogutil_compaction_test, case1prompt) {
    boost::filesystem::path dir{location};
    dir /= "log";
    boost::filesystem::create_directory(dir);
    create_file(dir / "epoch", data_case1_epoch);
    create_file(dir / std::string(manifest_file_name), data_manifest());
    create_file(dir / "pwal_0000", data_case1_pwal0);
    create_file(dir / "pwal_0001", data_case1_pwal1);
    std::string command;
    command = "echo y | " UTIL_COMMAND " compaction " + dir.string() + " 2>&1";
    std::string out;
    int rc = invoke(command, out);
    EXPECT_GE(rc, 0 << 8);
    EXPECT_TRUE(contains(out, "y/N"));
    EXPECT_TRUE(contains(out, "compaction was successfully completed: "));
    EXPECT_EQ(read_entire_file(list_dir(dir)[0]), data_case1_pwalcompact);
    EXPECT_EQ(read_entire_file(dir / "epoch"), data_case1_epochcompact);
}

TEST_F(dblogutil_compaction_test, unreadable) {
    // root can read directories w/o permissions
    if (geteuid() == 0) { GTEST_SKIP() << "skip when run by root"; }

    boost::filesystem::path dir{location};
    dir /= "unreadable";
    boost::filesystem::create_directory(dir);
    boost::filesystem::permissions(dir, boost::filesystem::no_perms);  // drop dir permission
    std::string command;
    command = UTIL_COMMAND " compaction --force " + dir.string() + " 2>&1";
    std::string out;
    int rc = invoke(command, out);
    EXPECT_GE(rc, 64 << 8);
    EXPECT_TRUE(contains_line_starts_with(out, "E"));  // LOG(ERROR)
    EXPECT_TRUE(contains(out, "Permission denied"));
    boost::filesystem::permissions(dir, boost::filesystem::owner_all);
}

TEST_F(dblogutil_compaction_test, nondblogdir) {
    boost::filesystem::path dir{location};  // assume empty dir
    std::string command;
    command = UTIL_COMMAND " compaction --force " + dir.string() + " 2>&1";
    std::string out;
    int rc = invoke(command, out);
    EXPECT_GE(rc, 64 << 8);
    EXPECT_TRUE(contains_line_starts_with(out, "E"));  // LOG(ERROR)
    EXPECT_TRUE(contains(out, "unsupport"));
}

TEST_F(dblogutil_compaction_test, rejects_symlink) {
    // setup normal logdir as same as case1
    boost::filesystem::path dir{location};
    dir /= "log";
    boost::filesystem::create_directory(dir);
    create_file(dir / "epoch", data_case1_epoch);
    create_file(dir / std::string(manifest_file_name), data_manifest());
    create_file(dir / "pwal_0000", data_case1_pwal0);
    create_file(dir / "pwal_0001", data_case1_pwal1);

    // make symlink to data
    boost::filesystem::path symlink{location};
    symlink /= "symbolic_link";
    boost::filesystem::create_directory_symlink(dir, symlink);

    std::string command;
    command = UTIL_COMMAND " compaction --force " + symlink.string() + " 2>&1";
    std::string out;
    int rc = invoke(command, out);
    EXPECT_GE(rc, 64 << 8);
    EXPECT_TRUE(contains_line_starts_with(out, "E"));  // LOG(ERROR)
    EXPECT_TRUE(contains(out, "must not be symlink"));
}

}  // namespace limestone::testing
