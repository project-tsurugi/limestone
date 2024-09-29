
#include <boost/filesystem.hpp>

#include "dblog_scan.h"
#include "internal.h"
#include "log_entry.h"

#include "test_root.h"

namespace limestone::testing {

using namespace std::literals;
using namespace limestone::api;
using namespace limestone::internal;

class make_tmp_test : public ::testing::Test {
public:
static constexpr const char* location = "/tmp/make_tmp_test";

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

};

TEST_F(make_tmp_test, remove_trailing_dir_separators) {
    boost::filesystem::path p0{"/tmp/nonexist/0"};
    remove_trailing_dir_separators(p0);
    ASSERT_EQ(p0.string(), "/tmp/nonexist/0");
    ASSERT_EQ(p0.filename().string(), "0");

    boost::filesystem::path p1{"/tmp/nonexist/1/"};
    remove_trailing_dir_separators(p1);
    ASSERT_EQ(p1.string(), "/tmp/nonexist/1");
    ASSERT_EQ(p1.filename().string(), "1");

    boost::filesystem::path p2{"/tmp/nonexist/2//"};
    remove_trailing_dir_separators(p2);
    ASSERT_EQ(p2.string(), "/tmp/nonexist/2");
    ASSERT_EQ(p2.filename().string(), "2");
}

TEST_F(make_tmp_test, make_tmp_dir_next_to_0slash) {
    std::string p = std::string(location) + "/test0";
    boost::filesystem::create_directory(p);
    auto tmp = make_tmp_dir_next_to({p}, ".suffix_XXXXXX");
    ASSERT_TRUE(starts_with(tmp.filename().string(), "test0.suffix_"));
}

// check removing trailig slashes

TEST_F(make_tmp_test, make_tmp_dir_next_to_1slash) {
    std::string p = std::string(location) + "/test1/";
    boost::filesystem::create_directory(p);
    auto tmp = make_tmp_dir_next_to({p}, ".suffix_XXXXXX");
    ASSERT_TRUE(starts_with(tmp.filename().string(), "test1.suffix_"));
}

TEST_F(make_tmp_test, make_tmp_dir_next_to_2slash) {
    std::string p = std::string(location) + "/test2//";
    boost::filesystem::create_directory(p);
    auto tmp = make_tmp_dir_next_to({p}, ".suffix_XXXXXX");
    ASSERT_TRUE(starts_with(tmp.filename().string(), "test2.suffix_"));
}

}
