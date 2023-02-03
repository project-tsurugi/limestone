
#include <algorithm>
#include <sstream>
#include <limestone/logging.h>

#include <boost/filesystem/path.hpp>
#include <boost/filesystem/fstream.hpp>

#include "test_root.h"

namespace limestone::testing {

inline constexpr const char* location = "/tmp/rotate_test";

class rotate_test : public ::testing::Test {
public:
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

    bool starts_with(std::string a, std::string b) { return a.substr(0, b.length()) == b; };

protected:
    std::unique_ptr<limestone::api::datastore_test> datastore_{};
};

void create_file(boost::filesystem::path path, std::string_view content) {
    boost::filesystem::ofstream strm{};
    strm.open(path, std::ios_base::out | std::ios_base::app | std::ios_base::binary);
    strm.write(content.data(), content.size());
    strm.flush();
    ASSERT_FALSE(!strm || !strm.is_open() || strm.bad() || strm.fail());
    strm.close();
}

TEST_F(rotate_test, log_is_rotated) { // NOLINT
    using namespace limestone::api;

    log_channel& channel = datastore_->create_channel(boost::filesystem::path(location));
    log_channel& unused_channel = datastore_->create_channel(boost::filesystem::path(location));
    datastore_->switch_epoch(42);
    channel.begin_session();
    channel.add_entry(42, "k1", "v1", {100, 4});
    channel.end_session();
    datastore_->switch_epoch(43);

    {
        auto& backup = datastore_->begin_backup();  // const function
        auto files = backup.files();

        EXPECT_EQ(files.size(), 2);
        EXPECT_EQ(files.at(0).string(), std::string(location) + "/epoch");
        EXPECT_EQ(files.at(1).string(), std::string(location) + "/pwal_0000");
    }
    // setup done

    std::unique_ptr<backup_detail> bd = datastore_->begin_backup(backup_type::standard);
    auto entries = bd->entries();

    {  // result check
        auto v(entries);
        std::sort(v.begin(), v.end(), [](auto& a, auto& b){
            return a.destination_path().string() < b.destination_path().string();
        });
        for (auto & e : v) {
            //std::cout << e.source_path() << std::endl;  // print debug
        }
        EXPECT_EQ(v.size(), 2);
        EXPECT_TRUE(starts_with(v[0].destination_path().string(), "epoch"));  // relative
        EXPECT_TRUE(starts_with(v[0].source_path().string(), location));  // absolute
        //EXPECT_EQ(v[0].is_detached(), false);
        EXPECT_EQ(v[0].is_mutable(), false);
        EXPECT_TRUE(starts_with(v[1].destination_path().string(), "pwal"));  // relative
        EXPECT_TRUE(starts_with(v[1].source_path().string(), location));  // absolute
        EXPECT_EQ(v[1].is_detached(), false);
        EXPECT_EQ(v[1].is_mutable(), false);
    }

    {  // log dir check (by using old backup)
        auto& backup = datastore_->begin_backup();  // const function
        auto files = backup.files();
        std::sort(files.begin(), files.end(), [](auto& a, auto& b){
            return a.string() < b.string();
        });

        // not contains active pwal just after rotate
        EXPECT_EQ(files.size(), 3);
        EXPECT_EQ(files.at(0).string(), std::string(location) + "/epoch");  // active epoch
        EXPECT_TRUE(starts_with(files[1].string(), std::string(location) + "/epoch."));  // rotated epoch
        EXPECT_TRUE(starts_with(files[2].string(), std::string(location) + "/pwal_0000."));  // rotated pwal
    }

}

// why in this file??
TEST_F(rotate_test, restore_prusik_all_abs) { // NOLINT
    using namespace limestone::api;
    auto location_path = boost::filesystem::path(location);

    auto pwal1fn = "pwal_0000.1.1";
    auto pwal2fn = "pwal_0000.2.2";
    auto epochfn = "epoch";
    auto pwal1d = location_path / "bk1";
    auto pwal2d = location_path / "bk2";
    auto epochd = location_path / "bk3";
    boost::filesystem::create_directories(pwal1d);
    boost::filesystem::create_directories(pwal2d);
    boost::filesystem::create_directories(epochd);

    create_file(pwal1d / pwal1fn, "1");
    create_file(pwal2d / pwal2fn, "2");
    create_file(epochd / epochfn, "e");
    // setup done

    std::vector<file_set_entry> data{};
    data.emplace_back(pwal1d / pwal1fn, pwal1fn, false);
    data.emplace_back(pwal2d / pwal2fn, pwal2fn, false);
    data.emplace_back(epochd / epochfn, epochfn, false);

    datastore_->restore(location, data);

    EXPECT_TRUE(boost::filesystem::exists(location_path / pwal1fn));
    EXPECT_TRUE(boost::filesystem::exists(location_path / pwal2fn));
    EXPECT_TRUE(boost::filesystem::exists(location_path / epochfn));

    // file count check, using newly created datastore
    regen_datastore();

    auto& backup = datastore_->begin_backup();  // const function
    auto files = backup.files();
    EXPECT_EQ(files.size(), 3);
}

TEST_F(rotate_test, restore_prusik_all_rel) { // NOLINT
    using namespace limestone::api;
    auto location_path = boost::filesystem::path(location);

    std::string pwal1fn = "pwal_0000.1.1";
    std::string pwal2fn = "pwal_0000.2.2";
    std::string epochfn = "epoch";
    auto pwal1d = location_path / "bk1";
    auto pwal2d = location_path / "bk2";
    auto epochd = location_path / "bk3";
    boost::filesystem::create_directories(pwal1d);
    boost::filesystem::create_directories(pwal2d);
    boost::filesystem::create_directories(epochd);

    create_file(pwal1d / pwal1fn, "1");
    create_file(pwal2d / pwal2fn, "2");
    create_file(epochd / epochfn, "e");
    // setup done

    std::vector<file_set_entry> data{};
    data.emplace_back("bk1/" + pwal1fn, pwal1fn, false);
    data.emplace_back("bk2/" + pwal2fn, pwal2fn, false);
    data.emplace_back("bk3/" + epochfn, epochfn, false);

    datastore_->restore(location, data);

    EXPECT_TRUE(boost::filesystem::exists(location_path / pwal1fn));
    EXPECT_TRUE(boost::filesystem::exists(location_path / pwal2fn));
    EXPECT_TRUE(boost::filesystem::exists(location_path / epochfn));

    // file count check, using newly created datastore
    regen_datastore();

    auto& backup = datastore_->begin_backup();  // const function
    auto files = backup.files();
    EXPECT_EQ(files.size(), 3);
}

TEST_F(rotate_test, get_snapshot_works) { // NOLINT
    using namespace limestone::api;

    datastore_->ready();
    log_channel& channel = datastore_->create_channel(boost::filesystem::path(location));
    log_channel& unused_channel = datastore_->create_channel(boost::filesystem::path(location));
    datastore_->switch_epoch(42);
    channel.begin_session();
    channel.add_entry(3, "k1", "v1", {100, 4});
    channel.end_session();
    datastore_->switch_epoch(43);

    datastore_->begin_backup(backup_type::standard);  // rotate files

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
    EXPECT_FALSE(cursor->next());
    datastore_->shutdown();
}

}  // namespace limestone::testing
