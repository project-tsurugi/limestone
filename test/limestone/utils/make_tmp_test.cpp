
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
