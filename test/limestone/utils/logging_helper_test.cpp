
#include <sstream>
#include <limestone/logging.h>
#include "logging_helper.h"

#include "test_root.h"

namespace limestone::testing {

class logging_helper_test : public ::testing::Test {
public:
    void SetUp() {
    }

    void TearDown() {
    }

};

TEST_F(logging_helper_test, find_fullname) { // NOLINT
    // check constexpr-ness (if fail, then compile error)
    constexpr std::string_view a = find_fullname(__PRETTY_FUNCTION__, __FUNCTION__);

    ASSERT_EQ(find_fullname("int foo(int)", "foo"), "foo");
    ASSERT_EQ(find_fullname("limestone::api::datastore::recover()", "recover"), "limestone::api::datastore::recover");
    ASSERT_EQ(find_fullname("myclass::myclass()", "myclass"), "myclass::myclass");

    // TODO: closure
    // g++-9
    // ASSERT_EQ(find_fullname("main(int, char**)::<lambda()>", "operator()"), ???);
    // clang++-11
    // ASSERT_EQ(find_fullname("auto main(int, char **)::(anonymous class)::operator()()", "operator()"), ???);
}

TEST_F(logging_helper_test, location_prefix_sv) { // NOLINT
    // check constexpr-ness (if fail, then compile error)
    constexpr auto a = location_prefix<2>(std::string_view("a"));

    // TODO: operators, eg. operator==
    ASSERT_EQ(std::string(location_prefix<50>("limestone::api::datastore::recover").data()), "/:limestone:api:datastore:recover ");
}

TEST_F(logging_helper_test, location_prefix_constchar) { // NOLINT
    // check constexpr-ness (if fail, then compile error)
    constexpr auto a = location_prefix(__PRETTY_FUNCTION__, __FUNCTION__);

    ASSERT_EQ(std::string(location_prefix("limestone::api::datastore::recover()", "recover").data()), "/:limestone:api:datastore:recover ");
    ASSERT_EQ(std::string(location_prefix("foo<myclass>::func(int)", "func").data()), "/:foo:func ");
}

TEST_F(logging_helper_test, shrink_prettyname) { // NOLINT
    // check constexpr-ness (if fail, then compile error)
    constexpr auto a = shrink_prettyname(__PRETTY_FUNCTION__);

#define __CHECK(prettyname, expected) EXPECT_EQ(std::string(shrink_prettyname(prettyname).data()), expected)
    __CHECK("int foo(int)", "foo");
    __CHECK("limestone::api::datastore::recover()", "limestone:api:datastore:recover");
    __CHECK("foo<myclass>::func(int)", "foo:func");

    /// lambda
    // g++-9 lambda
    __CHECK("aaaa::bbbb<T, n>::func<long unsigned int, 99>::<lambda(int)>",                               "aaaa:bbbb:func:lambda");
    // clang++-11 lambda
    __CHECK("auto aaaa::bbbb<unsigned long, 99>::func(int &, int &)::(anonymous class)::operator()(int)", "aaaa:bbbb:func:lambda");

    /// operator
    // g++-9
    __CHECK("long double aa::bb<T, n>::operator<<=(const char*) [with T = long unsigned int; int n = 99]", "aa:bb:operator");

    /// cast
    // g++-9
    __CHECK("aa::bb<T, n>::operator std::vector<aa::bb<char* const, -5> >() [with T = long unsigned int; int n = 99]", "aa:bb:operator");
    __CHECK("aa::bb<T, n>::operator std::vector<aa::bb<char* const, -5> >() [with T = long unsigned int; int n = 99]::<lambda(int)>", "aa:bb:operator:lambda");
#undef __CHECK
}

TEST_F(logging_helper_test, DISABLED_shrinked_length) { // NOLINT
    constexpr size_t n = shrinked_length(__PRETTY_FUNCTION__);
    std::cout << n << " " << std::string_view(__PRETTY_FUNCTION__).length() << " " << __PRETTY_FUNCTION__;
    constexpr auto lp = location_prefix_v2(__PRETTY_FUNCTION__);
    std::cout << " " << lp.data() << lp.size() << std::endl;
}

// redirect LOG to local-scope lbuf
#ifdef LOG
#undef LOG
#endif

#define LOG(_ignored) lbuf

class logging_helper_test_foo1 {
public:
    int foo(int& p) {
        std::ostringstream lbuf;
        lbuf.str("");
        LOG_LP(0) << "TEST";
        assert(lbuf.str() == "/:limestone:testing:logging_helper_test_foo1:foo TEST");
        return 0;
    }
};
template<class T, int n>
class logging_helper_test_foo2 {
public:
    T foo(int& p, int &p2) {
        std::ostringstream lbuf;
        // limestone::logging_helper_test_foo2<T, n>::foo
        lbuf.str("");
        LOG_LP(0) << "TEST";
        EXPECT_EQ(lbuf.str(), "/:limestone:testing:logging_helper_test_foo2:foo TEST");
        auto lambda1 = [&lbuf](int u){
            // g++-9:      limestone::testing::logging_helper_test_foo2<T, n>::foo<long unsigned int, 99>::<lambda(int)>
            // clang++-11: auto limestone::testing::logging_helper_test_foo2<unsigned long, 99>::foo(int &, int &)::(anonymous class)::operator()(int)
            lbuf.str("");
            LOG_LP(0) << "TEST";
#if AUTO_LOCATION_PREFIX_VERSION == 2
            EXPECT_EQ(lbuf.str(), "/:limestone:testing:logging_helper_test_foo2:foo:lambda TEST");
#endif
        };
        lambda1(1);
        std::function<long double(int)> lambda2 = [&lbuf](int u){
            // g++-9:      limestone::testing::logging_helper_test_foo2<T, n>::foo<long unsigned int, 99>::<lambda(int)>
            // clang++-11: auto limestone::testing::logging_helper_test_foo2<unsigned long, 99>::foo(int &, int &)::(anonymous class)::operator()(int)
            lbuf.str("");
            LOG_LP(0) << "TEST";
#if AUTO_LOCATION_PREFIX_VERSION == 2
            EXPECT_EQ(lbuf.str(), "/:limestone:testing:logging_helper_test_foo2:foo:lambda TEST");
#endif
            return 0.0;
        };
        lambda2(1);
        return 0;
    }
    long double operator() (const char *p) {
        std::ostringstream lbuf;
        LOG_LP(0) << "TEST";
#if AUTO_LOCATION_PREFIX_VERSION == 2
        EXPECT_EQ(lbuf.str(), "/:limestone:testing:logging_helper_test_foo2:operator TEST");
#endif
        auto lambda1 = [&lbuf](int u){
            //std::cout << "PF:" << __PRETTY_FUNCTION__ << " F:" << __FUNCTION__ << std::endl;
            // g++-9:      limestone::testing::logging_helper_test_foo2<T, n>::operator()(const char*) [with T = long unsigned int; int n = 99]::<lambda(int)>
            // clang++-11: auto limestone::testing::logging_helper_test_foo2<unsigned long, 99>::operator()(const char *)::(anonymous class)::operator()(int) const [T = unsigned long, n = 99]
            lbuf.str("");
            LOG_LP(0) << "TEST";
#if AUTO_LOCATION_PREFIX_VERSION == 2
            EXPECT_EQ(lbuf.str(), "/:limestone:testing:logging_helper_test_foo2:operator:lambda TEST");
#endif
        };
        lambda1(1);
        return 0.0;
    }
    long double operator<<= (const char *p) {
        std::ostringstream lbuf;
        //std::cout << "PF:" << __PRETTY_FUNCTION__ << " F:" << __FUNCTION__ << std::endl;
        // g++-9:      long double limestone::testing::logging_helper_test_foo2<T, n>::operator<<=(const char*) [with T = long unsigned int; int n = 99]
        LOG_LP(0) << "TEST";
#if AUTO_LOCATION_PREFIX_VERSION == 2
        EXPECT_EQ(lbuf.str(), "/:limestone:testing:logging_helper_test_foo2:operator TEST");
#endif
        auto lambda1 = [&lbuf](int u){
            //std::cout << "PF:" << __PRETTY_FUNCTION__ << " F:" << __FUNCTION__ << std::endl;
            // g++-9:      limestone::testing::logging_helper_test_foo2<T, n>::operator<<=(const char*) [with T = long unsigned int; int n = 99]::<lambda(int)>
            lbuf.str("");
            LOG_LP(0) << "TEST";
#if AUTO_LOCATION_PREFIX_VERSION == 2
            EXPECT_EQ(lbuf.str(), "/:limestone:testing:logging_helper_test_foo2:operator:lambda TEST");
#endif
        };
        lambda1(1);
        return 0.0;
    }
    operator std::vector<logging_helper_test_foo2<char * const, -5>> () {
        std::ostringstream lbuf;
        //std::cout << "PF:" << __PRETTY_FUNCTION__ << " F:" << __FUNCTION__ << std::endl;
        // g++-9:      limestone::testing::logging_helper_test_foo2<T, n>::operator std::vector<limestone::testing::logging_helper_test_foo2<char* const, -5> >() [with T = long unsigned int; int n = 99] F:operator std::vector<limestone::testing::logging_helper_test_foo2<char* const, -5> >
        LOG_LP(0) << "TEST";
#if AUTO_LOCATION_PREFIX_VERSION == 2
        EXPECT_EQ(lbuf.str(), "/:limestone:testing:logging_helper_test_foo2:operator TEST");
#endif
        auto lambda1 = [&lbuf](int u){
            //std::cout << "PF:" << __PRETTY_FUNCTION__ << " F:" << __FUNCTION__ << std::endl;
            // g++-9:      limestone::testing::logging_helper_test_foo2<T, n>::operator std::vector<limestone::testing::logging_helper_test_foo2<char* const, -5> >() [with T = long unsigned int; int n = 99]::<lambda(int)>
            lbuf.str("");
            LOG_LP(0) << "TEST";
#if AUTO_LOCATION_PREFIX_VERSION == 2
            EXPECT_EQ(lbuf.str(), "/:limestone:testing:logging_helper_test_foo2:operator:lambda TEST");
#endif
        };
        lambda1(1);
        return std::vector<logging_helper_test_foo2<char * const, -5>>{};
    }
};

TEST_F(logging_helper_test, assert_in_other_methods) { // NOLINT
    int dummy = 1234;
    logging_helper_test_foo1 foo1;
    foo1.foo(dummy);
    logging_helper_test_foo2<unsigned long, 99> foo2;
    foo2.foo(dummy, dummy);
    foo2("a");
    foo2 <<= "a";
    (std::vector<logging_helper_test_foo2<char * const, -5>>)foo2;
}

}  // namespace limestone::testing
