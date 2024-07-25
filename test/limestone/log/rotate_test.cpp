
#include <atomic>
#include <thread>
#include <chrono>
#include <sstream>
#include <limestone/logging.h>

#include <boost/filesystem.hpp>
#include <boost/filesystem/fstream.hpp>

#include "internal.h"

#include "test_root.h"

#define LOGFORMAT_VER 2


namespace limestone::testing {

inline constexpr const char* location = "/tmp/rotate_test";

class rotate_test : public ::testing::Test {
public:
    void SetUp() {
        boost::filesystem::remove_all(location);
        if (!boost::filesystem::create_directory(location)) {
            std::cerr << "cannot make directory" << std::endl;
        }
#if LOGFORMAT_VER >=1
        limestone::internal::setup_initial_logdir(boost::filesystem::path(location));
#endif

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

    std::unique_ptr<limestone::api::backup_detail> run_backup_with_epoch_switch(limestone::api::backup_type type, int initial_epoch) {
        std::atomic<bool> backup_completed(false);
        std::atomic<int> epoch_value(initial_epoch);  

        // Repeatally call switch_epoch until backup is completed in another thread
        std::thread switch_epoch_thread([&]() {
            while (!backup_completed.load()) {
                datastore_->switch_epoch(epoch_value++);
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
        });

        // Begin backup and wait for completion in main thread
        std::unique_ptr<limestone::api::backup_detail> bd = datastore_->begin_backup(type);

        // Set flag to notify backup completion
        backup_completed.store(true);

        // Wait for switch_epoch_thread to finish
        switch_epoch_thread.join();

        return bd;
    }
};

extern void create_file(const boost::filesystem::path& path, std::string_view content);
extern std::string data_manifest(int persistent_format_version = 1);

TEST_F(rotate_test, log_is_rotated) { // NOLINT
    using namespace limestone::api;

    log_channel& channel = datastore_->create_channel(boost::filesystem::path(location));
    log_channel& unused_channel = datastore_->create_channel(boost::filesystem::path(location));
    datastore_->switch_epoch(42);
    channel.begin_session();
    channel.add_entry(42, "k1", "v1", {100, 4});
    channel.end_session();
    datastore_->switch_epoch(43);

#if LOGFORMAT_VER ==1
    int manifest_file_num = 1;
#elif LOGFORMAT_VER >= 2
    int manifest_file_num = 2;
#else
    int manifest_file_num = 0;
#endif
    {
        auto& backup = datastore_->begin_backup();  // const function
        auto files = backup.files();

        ASSERT_EQ(files.size(), 2 + manifest_file_num);
        int i = 0;
#if LOGFORMAT_VER >=2        
        ASSERT_EQ(files[i++].string(), std::string(location) + "/compaction_catalog");
#endif
        ASSERT_EQ(files[i++].string(), std::string(location) + "/epoch");
#if LOGFORMAT_VER >=1
        ASSERT_EQ(files[i++].string(), std::string(location) + "/" + std::string(limestone::internal::manifest_file_name));
#endif
        ASSERT_EQ(files[i++].string(), std::string(location) + "/pwal_0000");
    }
    // setup done

    std::unique_ptr<backup_detail> bd = run_backup_with_epoch_switch(backup_type::standard, 44);
    auto entries = bd->entries();

    {  // result check
        auto v(entries);
        std::sort(v.begin(), v.end(), [](auto& a, auto& b){
            return a.destination_path().string() < b.destination_path().string();
        });
        for (auto & e : v) {
            //std::cout << e.source_path() << std::endl;  // print debug
        }
        EXPECT_EQ(v.size(), 2 + manifest_file_num);
        int i = 0;
#if LOGFORMAT_VER >=2
        EXPECT_TRUE(starts_with(v[i].destination_path().string(), "compaction_catalog"));  // relative
        EXPECT_TRUE(starts_with(v[i].source_path().string(), location));  // absolute
        EXPECT_EQ(v[i].is_detached(), false);
        EXPECT_EQ(v[i].is_mutable(), false);
        i++;
#endif        
        EXPECT_TRUE(starts_with(v[i].destination_path().string(), "epoch"));  // relative
        EXPECT_TRUE(starts_with(v[i].source_path().string(), location));  // absolute
        //EXPECT_EQ(v[i].is_detached(), false);
        EXPECT_EQ(v[i].is_mutable(), false);
        i++;
#if LOGFORMAT_VER >=1
        EXPECT_EQ(v[i].destination_path().string(), limestone::internal::manifest_file_name);  // relative
        EXPECT_TRUE(starts_with(v[i].source_path().string(), location));  // absolute
        EXPECT_EQ(v[i].is_detached(), false);
        EXPECT_EQ(v[i].is_mutable(), true);
        i++;
#endif
        EXPECT_TRUE(starts_with(v[i].destination_path().string(), "pwal"));  // relative
        EXPECT_TRUE(starts_with(v[i].source_path().string(), location));  // absolute
        EXPECT_EQ(v[i].is_detached(), false);
        EXPECT_EQ(v[i].is_mutable(), false);
    }

    {  // log dir check (by using old backup)
        auto& backup = datastore_->begin_backup();  // const function
        auto files = backup.files();
        std::sort(files.begin(), files.end(), [](auto& a, auto& b){
            return a.string() < b.string();
        });

        // not contains active pwal just after rotate
        EXPECT_EQ(files.size(), 3 + manifest_file_num);
        int i = 0;
#if LOGFORMAT_VER >=2        
        ASSERT_EQ(files[i++].string(), std::string(location) + "/compaction_catalog");
#endif
        EXPECT_EQ(files[i++].string(), std::string(location) + "/epoch");  // active epoch
        EXPECT_TRUE(starts_with(files[i++].string(), std::string(location) + "/epoch."));  // rotated epoch
#if LOGFORMAT_VER >=1
        ASSERT_EQ(files[i++].string(), std::string(location) + "/" + std::string(limestone::internal::manifest_file_name));
#endif
        EXPECT_TRUE(starts_with(files[i++].string(), std::string(location) + "/pwal_0000."));  // rotated pwal
    }

}

// implementation note:
// in another design, rotate_all_file on shutdown or startup
TEST_F(rotate_test, inactive_files_are_also_backed_up) { // NOLINT
    using namespace limestone::api;
    // scenario:
    // a. server start
    // b. write log with many channels
    // c. server shutdown (or crash)
    // d. server start
    // e. write nothing or with fewer channels (than num of b.)
    // f. rotate and backup
    //    CHECK: are all files in the backup target??
    // g. server shutdown
    // h. restore files from f.
    //    DATA LOST if step f. is wrong

    {
        log_channel& channel1_0 = datastore_->create_channel(boost::filesystem::path(location));  // pwal_0000
        log_channel& channel1_1 = datastore_->create_channel(boost::filesystem::path(location));  // pwal_0001
        log_channel& unused_1_2 = datastore_->create_channel(boost::filesystem::path(location));  // pwal_0002 unused
        datastore_->ready();
        datastore_->switch_epoch(42);
        channel1_0.begin_session();
        channel1_0.add_entry(2, "k0", "v0", {42, 4});
        channel1_0.end_session();
        channel1_1.begin_session();
        channel1_1.add_entry(2, "k1", "v1", {42, 4});
        channel1_1.end_session();
        datastore_->switch_epoch(43);
    }
    regen_datastore();
    {
        log_channel& channel2_0 = datastore_->create_channel(boost::filesystem::path(location));  // pwal_0000
        log_channel& unused_2_1 = datastore_->create_channel(boost::filesystem::path(location));  // pwal_0001 unused
        log_channel& unused_2_2 = datastore_->create_channel(boost::filesystem::path(location));  // pwal_0002 unused
        datastore_->ready();
        datastore_->switch_epoch(44);
        channel2_0.begin_session();
        channel2_0.add_entry(2, "k3", "v3", {44, 4});
        channel2_0.end_session();
        datastore_->switch_epoch(45);
    }

    // setup done

    std::unique_ptr<backup_detail> bd = run_backup_with_epoch_switch(backup_type::standard,46);
    auto entries = bd->entries();

    {  // result check
        auto v(entries);
        std::sort(v.begin(), v.end(), [](auto& a, auto& b){
            return a.destination_path().string() < b.destination_path().string();
        });
        for (auto & e : v) {
            //std::cout << e.source_path() << std::endl;  // print debug
        }
#if LOGFORMAT_VER == 1
        int manifest_file_num = 1;
#elif LOGFORMAT_VER >= 2
        int manifest_file_num = 2;
#else
        int manifest_file_num = 0;
#endif
       EXPECT_EQ(v.size(), 3 + manifest_file_num);
        int i = 0;
#if LOGFORMAT_VER >=2
        EXPECT_TRUE(starts_with(v[i].destination_path().string(), "compaction_catalog"));  // relative
        EXPECT_TRUE(starts_with(v[i].source_path().string(), location));  // absolute
        EXPECT_EQ(v[i].is_detached(), false);
        EXPECT_EQ(v[i].is_mutable(), false);
        i++;
#endif            
        EXPECT_TRUE(starts_with(v[i].destination_path().string(), "epoch."));  // relative
        EXPECT_TRUE(starts_with(v[i].source_path().string(), location));  // absolute
        //EXPECT_EQ(v[i].is_detached(), false);
        EXPECT_EQ(v[i].is_mutable(), false);
        i++;
#if LOGFORMAT_VER >=1
        EXPECT_EQ(v[i].destination_path().string(), limestone::internal::manifest_file_name);  // relative
        EXPECT_TRUE(starts_with(v[i].source_path().string(), location));  // absolute
        EXPECT_EQ(v[i].is_detached(), false);
        EXPECT_EQ(v[i].is_mutable(), true);
        i++;
#endif
        EXPECT_TRUE(starts_with(v[i].destination_path().string(), "pwal_0000."));  // relative
        EXPECT_TRUE(starts_with(v[i].source_path().string(), location));  // absolute
        EXPECT_EQ(v[i].is_detached(), false);
        EXPECT_EQ(v[i].is_mutable(), false);
        i++;
        EXPECT_TRUE(starts_with(v[i].destination_path().string(), "pwal_0001."));  // relative
        EXPECT_TRUE(starts_with(v[i].source_path().string(), location));  // absolute
        EXPECT_EQ(v[i].is_detached(), false);
        EXPECT_EQ(v[i].is_mutable(), false);
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
#if LOGFORMAT_VER >=1
    auto conffn = std::string(limestone::internal::manifest_file_name);
    auto confd = location_path / "bk0";
    boost::filesystem::create_directories(confd);
    create_file(confd / conffn, data_manifest(1));
    data.emplace_back(confd / conffn, conffn, false);
#endif

    datastore_->restore(location, data);

    EXPECT_TRUE(boost::filesystem::exists(location_path / pwal1fn));
    EXPECT_TRUE(boost::filesystem::exists(location_path / pwal2fn));
    EXPECT_TRUE(boost::filesystem::exists(location_path / epochfn));

    // file count check, using newly created datastore
    regen_datastore();

    auto& backup = datastore_->begin_backup();  // const function
    auto files = backup.files();
#if LOGFORMAT_VER ==1
    int manifest_file_num = 1;
#elif LOGFORMAT_VER >= 2
    int manifest_file_num = 2;
#else
    int manifest_file_num = 0;
#endif
    EXPECT_EQ(files.size(), 3 + manifest_file_num);
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
#if LOGFORMAT_VER >=1
    std::string conffn(limestone::internal::manifest_file_name);
    auto confd = location_path / "bk0";
    boost::filesystem::create_directories(confd);
    create_file(confd / conffn, data_manifest(1));
    data.emplace_back("bk0/" + conffn, conffn, false);
#endif

    datastore_->restore(location, data);

    EXPECT_TRUE(boost::filesystem::exists(location_path / pwal1fn));
    EXPECT_TRUE(boost::filesystem::exists(location_path / pwal2fn));
    EXPECT_TRUE(boost::filesystem::exists(location_path / epochfn));

    // file count check, using newly created datastore
    regen_datastore();

    auto& backup = datastore_->begin_backup();  // const function
    auto files = backup.files();
#if LOGFORMAT_VER == 1
    int manifest_file_num = 1;
#elif LOGFORMAT_VER >= 2
    int manifest_file_num = 2;
#else
    int manifest_file_num = 0;
#endif
    EXPECT_EQ(files.size(), 3 + manifest_file_num);
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

    run_backup_with_epoch_switch(backup_type::standard,46);

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
