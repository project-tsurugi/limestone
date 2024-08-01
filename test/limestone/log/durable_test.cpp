
#include <algorithm>
#include <sstream>
#include <limestone/logging.h>

#include <boost/filesystem.hpp>

#include "dblog_scan.h"
#include "internal.h"
#include "log_entry.h"

#include "test_root.h"

namespace limestone::testing {

class durable_test : public ::testing::Test {
public:
static constexpr const char* location = "/tmp/durable_test";

    void SetUp() {
        boost::filesystem::remove_all(location);
        if (!boost::filesystem::create_directory(location)) {
            std::cerr << "cannot make directory" << std::endl;
        }

        regen_datastore();
    }

    void regen_datastore() {
        std::vector<boost::filesystem::path> data_locations{};
        data_locations.emplace_back(location);
        boost::filesystem::path metadata_location{location};
        limestone::api::configuration conf(data_locations, metadata_location);

        datastore_ = std::make_unique<limestone::api::datastore_test>(conf);
    }

    void TearDown() {
        datastore_ = nullptr;
        boost::filesystem::remove_all(location);
    }

    bool starts_with(std::string a, std::string b) { return a.substr(0, b.length()) == b; }

protected:
    std::unique_ptr<limestone::api::datastore_test> datastore_{};
};

TEST_F(durable_test, last_record_will_ignored) {
    using namespace limestone::api;

    datastore_->ready();
    log_channel& channel = datastore_->create_channel(boost::filesystem::path(location));
    log_channel& channel2 = datastore_->create_channel(boost::filesystem::path(location));
    datastore_->switch_epoch(42);
    channel.begin_session();
    channel.add_entry(3, "k1", "v1", {42, 4});
    channel.end_session();
    datastore_->switch_epoch(43);
    channel2.begin_session();  // another channel running
    channel.begin_session();
    channel.add_entry(3, "k2", "v2", {43, 5});
    channel.end_session();
    //datastore_->switch_epoch is not called

    datastore_->shutdown();
    regen_datastore();
    // setup done

    datastore_->recover();
    datastore_->ready();
    auto snapshot = datastore_->get_snapshot();
    auto cursor = snapshot->get_cursor();
    std::string buf;

    ASSERT_TRUE(cursor->next());
    EXPECT_EQ(cursor->storage(), 3);
    EXPECT_EQ((cursor->key(buf), buf), "k1");
    EXPECT_EQ((cursor->value(buf), buf), "v1");
    EXPECT_FALSE(cursor->next());  // only 1 entry
    datastore_->shutdown();
}

TEST_F(durable_test, invalidated_entries_are_never_reused) {
    using namespace limestone::api;

    datastore_->ready();
    log_channel& channel = datastore_->create_channel(boost::filesystem::path(location));
    datastore_->switch_epoch(42);
    channel.begin_session();
    channel.add_entry(3, "k1", "v1", {42, 4});
    channel.add_entry(3, "k2", "v2", {42, 4});
    channel.end_session();
    datastore_->switch_epoch(43);
    channel.begin_session();
    channel.add_entry(3, "k3", "v3", {43, 4});
    channel.add_entry(3, "k4", "v4", {43, 4});
    channel.end_session();
    // no switch_epoch, broken

    datastore_->shutdown();
    regen_datastore();
    // setup done

    datastore_->recover();
    datastore_->ready();
    auto snapshot = datastore_->get_snapshot();
    auto cursor = snapshot->get_cursor();
    std::map<std::string, std::string> m;
    while (cursor->next()) {
        std::string key;
        std::string value;
        cursor->key(key);
        cursor->value(value);
        m[key] = value;
    }
    EXPECT_EQ(m.size(), 2);
    EXPECT_EQ(m["k1"], "v1");
    EXPECT_EQ(m["k2"], "v2");

    log_channel& channel2 = datastore_->create_channel(boost::filesystem::path(location));
    datastore_->switch_epoch(46);
    channel2.begin_session();
    channel2.add_entry(3, "k5", "v5", {46, 4});
    channel2.add_entry(3, "k6", "v6", {46, 4});
    channel2.end_session();
    datastore_->switch_epoch(47);

    datastore_->shutdown();

    regen_datastore();
    // setup done

    datastore_->recover();
    datastore_->ready();
    snapshot = datastore_->get_snapshot();
    cursor = snapshot->get_cursor();
    m.clear();
    while (cursor->next()) {
        std::string key;
        std::string value;
        cursor->key(key);
        cursor->value(value);
        m[key] = value;
    }
    EXPECT_EQ(m.size(), 4);
    EXPECT_EQ(m["k1"], "v1");
    EXPECT_EQ(m["k2"], "v2");
    EXPECT_EQ(m["k5"], "v5");
    EXPECT_EQ(m["k6"], "v6");
    datastore_->shutdown();
}

limestone::api::epoch_id_type scan_one_pwal_file(const boost::filesystem::path& p, limestone::api::epoch_id_type ld_epoch, const std::function<void(limestone::api::log_entry&)>& add_entry) {
    using limestone::internal::dblog_scan;
    dblog_scan ds{""};  // dummy
    dblog_scan::parse_error ec;
    ds.set_fail_fast(true);
    ds.set_process_at_nondurable_epoch_snippet(dblog_scan::process_at_nondurable::repair_by_mark);
    ds.set_process_at_truncated_epoch_snippet(dblog_scan::process_at_truncated::report);
    ds.set_process_at_damaged_epoch_snippet(dblog_scan::process_at_damaged::report);
    auto rc = ds.scan_one_pwal_file(p, ld_epoch, add_entry, [](limestone::api::log_entry::read_error& e) -> bool {
        LOG(ERROR) << "this pwal file is broken: " << e.message();
        throw std::runtime_error("pwal file read error");
    }, ec);
    return rc;
}

TEST_F(durable_test, ut_scan_one_pwal_file_nondurable_entry) {
    using namespace limestone::api;

    boost::filesystem::path pwal(location);
    pwal /= "pwal";
    {  // make pwal file for test
        FILE *f = fopen(pwal.c_str(), "w");
        log_entry::begin_session(f, 42);
        log_entry::write(f, 1, "k1", "v1", {42, 1});
        log_entry::begin_session(f, 43);  // after this entry; not durable
        log_entry::write(f, 1, "k2", "v2", {43, 1});
        fclose(f);
    }
    std::vector<log_entry> entries;
    auto add_entry = [&entries](log_entry& e){
        entries.emplace_back(e);
    };

    epoch_id_type last_epoch = scan_one_pwal_file(pwal, 42, add_entry);
    EXPECT_EQ(last_epoch, 43);
    EXPECT_EQ(entries.size(), 1);
    {  // check (marked) pwal file after scan
        std::ifstream in;
        log_entry e;
        in.open(pwal, std::ios::in | std::ios::binary);
        ASSERT_TRUE(e.read(in));
        EXPECT_EQ(e.type(), log_entry::entry_type::marker_begin);
        ASSERT_TRUE(e.read(in));
        EXPECT_EQ(e.type(), log_entry::entry_type::normal_entry);
        ASSERT_TRUE(e.read(in));
        EXPECT_EQ(e.type(), log_entry::entry_type::marker_invalidated_begin);
        ASSERT_TRUE(e.read(in));
        EXPECT_EQ(e.type(), log_entry::entry_type::normal_entry);
        ASSERT_FALSE(e.read(in));
        in.close();
    }
}

// broken entry in non durable epoch is ignored
TEST_F(durable_test, ut_scan_one_pwal_file_broken_entry_nondurable_trimmed) {
    using namespace limestone::api;

    boost::filesystem::path pwal(location);
    pwal /= "pwal";
    {  // make pwal file for test
        FILE *f = fopen(pwal.c_str(), "w");
        log_entry::begin_session(f, 42);
        log_entry::write(f, 1, "k1", "v1", {42, 1});
        log_entry::begin_session(f, 43);  // after this entry; not durable
        // make broken entry
        fputc(static_cast<int>(log_entry::entry_type::normal_entry), f);
        fputc(99, f);  // the end of file is missing
        fclose(f);
    }
    auto add_entry = [](log_entry&){ /* nop */ };

    EXPECT_EQ(scan_one_pwal_file(pwal, 42, add_entry), 43);
}

// broken entry in durable epoch is error
TEST_F(durable_test, ut_scan_one_pwal_file_broken_entry_trimmed) {
    using namespace limestone::api;

    boost::filesystem::path pwal(location);
    pwal /= "pwal";
    {  // make pwal file for test
        FILE *f = fopen(pwal.c_str(), "w");
        log_entry::begin_session(f, 42);
        log_entry::write(f, 1, "k1", "v1", {42, 1});
        log_entry::begin_session(f, 43);
        // make broken entry
        fputc(static_cast<int>(log_entry::entry_type::normal_entry), f);
        fputc(99, f);  // the end of file is missing
        fclose(f);
    }
    auto add_entry = [](log_entry&){ /* nop */ };

    EXPECT_THROW({
        scan_one_pwal_file(pwal, 43, add_entry);
    }, std::exception);
}

TEST_F(durable_test, ut_scan_one_pwal_file_broken_entry_type0) {
    using namespace limestone::api;

    boost::filesystem::path pwal(location);
    pwal /= "pwal";
    {  // make pwal file for test
        FILE *f = fopen(pwal.c_str(), "w");
        log_entry::begin_session(f, 42);
        // make broken entry
        fputc(static_cast<int>(log_entry::entry_type::this_id_is_not_used), f);
        fclose(f);
    }
    auto add_entry = [](log_entry&){ /* nop */ };

    EXPECT_THROW({
        scan_one_pwal_file(pwal, 42, add_entry);
    }, std::exception);
}

TEST_F(durable_test, ut_scan_one_pwal_file_broken_entry_type99) {
    using namespace limestone::api;

    boost::filesystem::path pwal(location);
    pwal /= "pwal";
    {  // make pwal file for test
        FILE *f = fopen(pwal.c_str(), "w");
        log_entry::begin_session(f, 42);
        // make broken entry
        fputc(0x99, f);  // undefined entry_type
        fclose(f);
    }
    auto add_entry = [](log_entry&){ /* nop */ };

    EXPECT_THROW({
        scan_one_pwal_file(pwal, 42, add_entry);
    }, std::exception);
}

TEST_F(durable_test, ut_last_durable_epoch_normal) {
    using namespace limestone::api;

    boost::filesystem::path epoch_file(location);
    epoch_file /= "epoch";
    {  // make pwal file for test
        FILE *f = fopen(epoch_file.c_str(), "w");
        log_entry::durable_epoch(f, 1);
        log_entry::durable_epoch(f, 42);
        fclose(f);
    }

    EXPECT_EQ(limestone::internal::last_durable_epoch(epoch_file), 42);
}

TEST_F(durable_test, ut_last_durable_epoch_broken_trimmed) {
    using namespace limestone::api;

    boost::filesystem::path epoch_file(location);
    epoch_file /= "epoch";
    {  // make pwal file for test
        FILE *f = fopen(epoch_file.c_str(), "w");
        log_entry::durable_epoch(f, 1);
        log_entry::durable_epoch(f, 42);
        // make broken entry
        fputc(static_cast<int>(log_entry::entry_type::marker_durable), f);
        fputc(99, f);  // the end of file is missing
        fclose(f);
    }

    EXPECT_THROW({
        limestone::internal::last_durable_epoch(epoch_file);
    }, std::exception);
}

TEST_F(durable_test, ut_last_durable_epoch_broken_entry_type0) {
    using namespace limestone::api;

    boost::filesystem::path epoch_file(location);
    epoch_file /= "epoch";
    {  // make pwal file for test
        FILE *f = fopen(epoch_file.c_str(), "w");
        log_entry::durable_epoch(f, 1);
        log_entry::durable_epoch(f, 42);
        // make broken entry
        fputc(static_cast<int>(log_entry::entry_type::this_id_is_not_used), f);
        fclose(f);
    }

    EXPECT_THROW({
        limestone::internal::last_durable_epoch(epoch_file);
    }, std::exception);
}

TEST_F(durable_test, ut_last_durable_epoch_broken_entry_type1) {
    using namespace limestone::api;

    boost::filesystem::path epoch_file(location);
    epoch_file /= "epoch";
    {  // make pwal file for test
        FILE *f = fopen(epoch_file.c_str(), "w");
        log_entry::durable_epoch(f, 1);
        log_entry::durable_epoch(f, 42);
        log_entry::write(f, 1, "k1", "v1", {42, 1});  // wrong: normal entry is never in epoch-file
        fclose(f);
    }

    EXPECT_THROW({
        limestone::internal::last_durable_epoch(epoch_file);
    }, std::exception);
}

}  // namespace limestone::testing
