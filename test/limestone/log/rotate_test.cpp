
#include <limestone/logging.h>

#include <atomic>
#include <boost/filesystem.hpp>
#include <boost/filesystem/fstream.hpp>
#include <chrono>
#include <sstream>
#include <thread>

#include "internal.h"
#include "limestone/api/limestone_exception.h"
#include "limestone_exception_helper.h"
#include "manifest.h"
#include "test_root.h"

#define LOGFORMAT_VER 7

namespace limestone::testing {

using namespace limestone::api;

inline constexpr const char* location = "/tmp/rotate_test";
inline constexpr const char* location_backup = "/tmp/rotate_test_backup";

class rotate_test : public ::testing::Test {
public:
    void SetUp() {
        limestone::testing::enable_exception_throwing = true;
        boost::filesystem::remove_all(location);
        boost::filesystem::remove_all(location_backup);
        if (!boost::filesystem::create_directory(location)) {
            std::cerr << "cannot make directory" << std::endl;
        }
        if (!boost::filesystem::create_directory(location_backup)) {
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
        datastore_= nullptr;
        datastore_ = std::make_unique<limestone::api::datastore_test>(conf);
    }

    void TearDown() {
        limestone::testing::enable_exception_throwing = false;
        datastore_ = nullptr;
        boost::filesystem::remove_all(location);
        boost::filesystem::remove_all(location_backup);
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
        std::unique_ptr<limestone::api::backup_detail> bd;

        try {
            // Begin backup and wait for completion in main thread
            bd = datastore_->begin_backup(type);
        } catch (const std::exception& e) {
            // Ensure the thread is joined even in case of an exception
            std::cerr << "Exception during backup: " << e.what() << std::endl;
            backup_completed.store(true);  
            switch_epoch_thread.join();    
            throw;                        
        }

        // Set flag to notify backup completion
        backup_completed.store(true);

        // Wait for switch_epoch_thread to finish
        switch_epoch_thread.join();

        return bd;
    }
};

extern void create_file(const boost::filesystem::path& path, std::string_view content);
extern std::string data_manifest(int persistent_format_version = 1);


TEST_F(rotate_test, rotate_fails_with_io_error) {
    using namespace limestone::api;
    
    log_channel& channel = datastore_->create_channel(boost::filesystem::path(location));
    log_channel& unused_channel = datastore_->create_channel(boost::filesystem::path(location));
    datastore_->switch_epoch(42);
    channel.begin_session();
    channel.add_entry(42, "k1", "v1", {100, 4});
    channel.end_session();
    datastore_->switch_epoch(43);

    // Force an exception to be thrown by removing the directory
    if (system("rm -rf /tmp/rotate_test") != 0) {
        std::cerr << "Cannot remove directory" << std::endl;
    }

    // Check that an exception is thrown and verify its details
    try {
        run_backup_with_epoch_switch(backup_type::standard, 44);
        FAIL() << "Expected limestone_exception to be thrown";  // Fails the test if no exception is thrown
    } catch (const limestone_exception& e) {
        // Verify the exception details
        std::cerr << "Caught exception: " << e.what() << std::endl;
        EXPECT_TRUE(std::string(e.what()).rfind("I/O Error (No such file or directory): Failed to rename epoch_file from /tmp", 0) == 0);
        EXPECT_EQ(e.error_code(), ENOENT);
    } catch (const std::exception& e) {
        // Handle non-limestone_exception std::exception types
        std::cerr << "Caught exception: " << e.what() << std::endl;
        FAIL() << "Expected limestone_exception but caught a different std: " << e.what();
    } catch (...) {
        // Handle unknown exception types
        FAIL() << "Expected limestone_exception but caught an unknown exception type.";
    }

}


TEST_F(rotate_test, log_is_rotated) { // NOLINT
    using namespace limestone::api;
    datastore_->ready();

    log_channel& channel = datastore_->create_channel(boost::filesystem::path(location));
    log_channel& unused_channel = datastore_->create_channel(boost::filesystem::path(location));

    datastore_->switch_epoch(42);
    channel.begin_session();
    channel.add_entry(42, "k1", "v1", {100, 4});
    channel.end_session();
    datastore_->switch_epoch(43);

#if LOGFORMAT_VER >= 7
    int manifest_file_num = 3;
#elif LOGFORMAT_VER >= 2
    int manifest_file_num = 2;
#elif LOGFORMAT_VER ==1
    int manifest_file_num = 1;
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
        ASSERT_EQ(files[i++].string(), std::string(location) + "/" + std::string(limestone::internal::manifest::file_name));
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
        EXPECT_EQ(v[i].destination_path().string(), limestone::internal::manifest::file_name);  // relative
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
        ASSERT_EQ(files[i++].string(), std::string(location) + "/" + std::string(limestone::internal::manifest::file_name));
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
        datastore_->shutdown();
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
        datastore_->shutdown();
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
#if LOGFORMAT_VER >= 7
    int manifest_file_num = 3;
#elif LOGFORMAT_VER >= 2
    int manifest_file_num = 2;
#elif LOGFORMAT_VER ==1
    int manifest_file_num = 1;
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
        EXPECT_EQ(v[i].destination_path().string(), limestone::internal::manifest::file_name);  // relative
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
    auto conffn = std::string(limestone::internal::manifest::file_name);
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
    std::string conffn(limestone::internal::manifest::file_name);
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

TEST_F(rotate_test, begin_backup_includes_blob_entries) {
    datastore_->ready();
    datastore_->switch_epoch(1);
    // Acquire the blob pool from datastore and register data via datastore API.
    auto pool = datastore_->acquire_blob_pool();
    std::string data1 = "test data";
    std::string data2 = "more test data";
    blob_id_type blob_id1 = pool->register_data(data1);
    blob_id_type blob_id2 = pool->register_data(data2);

    // Retrieve the corresponding BLOB file paths using the datastore API.
    boost::filesystem::path blob_path1 = datastore_->get_blob_file(blob_id1).path();
    boost::filesystem::path blob_path2 = datastore_->get_blob_file(blob_id2).path();


    // Execute backup using the helper method to run backup while switching epochs.
    std::unique_ptr<backup_detail> bd = run_backup_with_epoch_switch(backup_type::standard, 44);

    // Get blob_root.
    boost::filesystem::path blob_root = boost::filesystem::path(location) / "blob";
    ASSERT_TRUE(boost::filesystem::exists(blob_root));

    // Verify: Check that the backup_detail entries include the expected BLOB file entries.
    auto entries = bd->entries();
    bool found_blob1 = false;
    bool found_blob2 = false;
    for (const auto &entry : entries) {
        if (entry.source_path() == blob_path1) {
            found_blob1 = true;
            EXPECT_FALSE(boost::filesystem::path(entry.destination_path()).is_absolute());
            EXPECT_EQ(blob_path1.filename(), entry.destination_path());
            // Verify that is_mutable and is_detached are false.
            EXPECT_FALSE(entry.is_mutable());
            EXPECT_FALSE(entry.is_detached());
        } else if (entry.source_path() == blob_path2) {
            found_blob2 = true;
            EXPECT_FALSE(boost::filesystem::path(entry.destination_path()).is_absolute());
            EXPECT_EQ(blob_path2.filename(), entry.destination_path());
            EXPECT_FALSE(entry.is_mutable());
            EXPECT_FALSE(entry.is_detached());
        }
    }
    EXPECT_TRUE(found_blob1) << "BLOB file entry for blob_id1 was not found in backup_detail entries.";
    EXPECT_TRUE(found_blob2) << "BLOB file entry for blob_id2 was not found in backup_detail entries.";
}

TEST_F(rotate_test, begin_backup_without_argument_includes_blob_entries) {
    datastore_->ready();

    // Prepare: Acquire the blob pool and register data via datastore API.
    auto pool = datastore_->acquire_blob_pool();
    std::string data1 = "test data";
    std::string data2 = "more test data";
    blob_id_type blob_id1 = pool->register_data(data1);
    blob_id_type blob_id2 = pool->register_data(data2);

    // Retrieve the corresponding BLOB file paths using the datastore API.
    boost::filesystem::path blob_path1 = datastore_->get_blob_file(blob_id1).path();
    boost::filesystem::path blob_path2 = datastore_->get_blob_file(blob_id2).path();

    // Get blob_root from the blob_file_resolver.
    boost::filesystem::path blob_root = boost::filesystem::path(location) / "blob";
    ASSERT_TRUE(boost::filesystem::exists(blob_root)) << "Blob root does not exist.";

    // Execute: Directly call begin_backup() without arguments.
    backup& bk = datastore_->begin_backup();

    // Get the list of files from the backup object.
    auto files = bk.files();

    // Verify: Check that the backup files include the expected BLOB file entries.
    bool found_blob1 = false;
    bool found_blob2 = false;
    for (const auto &file : files) {
        if (file == blob_path1) {
            found_blob1 = true;
        } else if (file == blob_path2) {
            found_blob2 = true;
        }
    }
    EXPECT_TRUE(found_blob1) << "BLOB file for blob_id1 was not found in backup files.";
    EXPECT_TRUE(found_blob2) << "BLOB file for blob_id2 was not found in backup files.";
}


TEST_F(rotate_test, restore_file_set_entries_with_blob) {
    // Scenario:
    // 1. Server start, create channels and write some log entries.
    // 2. Server restart with fewer channels.
    // 3. Rotate logs and generate the backup_detail.
    //    => Check: All files, including BLOB files, are included in the backup target.
    // 4. Create a backup directory at 'location_backup/bk1' to match the expected backup state.
    // 5. Restore files from backup using file_set_entry vector built from backup_entries.
    // 6. Verify that the restored files in 'location' match those in 'location_backup/bk1'.

    // Step 1: Setup log channels, write log entries, and register a BLOB file.
    {
        log_channel& channel1_0 = datastore_->create_channel(boost::filesystem::path(location));  // pwal_0000
        log_channel& channel1_1 = datastore_->create_channel(boost::filesystem::path(location));  // pwal_0001
        log_channel& unused_1_2 = datastore_->create_channel(boost::filesystem::path(location));   // pwal_0002 unused

        datastore_->ready();
        datastore_->switch_epoch(42);

        channel1_0.begin_session();
        channel1_0.add_entry(2, "k0", "v0", {42, 4});
        channel1_0.end_session();

        channel1_1.begin_session();
        channel1_1.add_entry(2, "k1", "v1", {42, 4});
        channel1_1.end_session();

        // Also, register a BLOB file via datastore API.
        auto blob_pool = datastore_->acquire_blob_pool();
        std::string blob_data = "blob initial data";
        blob_id_type blob_id_initial = blob_pool->register_data(blob_data);
        // The BLOB file is created internally via the API.

        datastore_->switch_epoch(43);
        datastore_->shutdown();
    }

    // Step 2: Simulate server restart with fewer channels.
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
        datastore_->shutdown();
    }

    // Step 3: Perform backup (rotate and backup) using run_backup_with_epoch_switch.
    std::unique_ptr<backup_detail> bd = run_backup_with_epoch_switch(backup_type::standard, 46);
    auto backup_entries = bd->entries();

    // Step 4: Manually create the expected backup state for validation.
    // In this test, we assume what `backup_detail` would contain after `begin_backup()`,
    // but no actual backup operation has been performed yet.
    // To validate the restore process, we manually copy the expected files from `location`
    // to `location_backup/bk1`, ensuring that `location_backup/bk1` mimics the expected backup state.

    // Move the database data directory to the backup destination (`location_backup/bk1`).
    boost::filesystem::path backup_src = boost::filesystem::path(location);
    boost::filesystem::path backup_dest = boost::filesystem::path(location_backup) / "bk1";
    boost::filesystem::remove_all(backup_dest);
    boost::filesystem::rename(backup_src, backup_dest);

    // Collect only files that are inside `bk1/blob/`
    std::vector<boost::filesystem::path> files_to_move;
    boost::filesystem::path blob_dir = backup_dest / "blob";
    
    for (boost::filesystem::recursive_directory_iterator it(backup_dest), end; it != end; ++it) {
        // Ensure the file is inside `blob/` by checking if its path starts with `blob_dir`
        if (boost::filesystem::is_regular_file(*it) && it->path().string().find(blob_dir.string()) == 0) {
            files_to_move.push_back(it->path());
        }
    }

    // Move all collected files to `bk1/` root
    for (const auto& file : files_to_move) {
        boost::filesystem::path new_path = backup_dest / file.filename();
        std::cerr << "Moving file: " << file << " to " << new_path << std::endl;
        boost::filesystem::rename(file, new_path);
    }

    // Remove all subdirectories inside `bk1/`
    for (boost::filesystem::directory_iterator it(backup_dest), end; it != end; ++it) {
        if (boost::filesystem::is_directory(*it)) {
            boost::filesystem::remove_all(it->path());
        }
    }

    // Step 5: Build file_set_entry vector based on backup_entries.
    // This ensures we are using the backup file list obtained in Step 3.
    std::vector<file_set_entry> fs_entries;
    for (const auto& entry : backup_entries) {
        // For restore, the source file is located at backup_dest / entry.destination_path()
        boost::filesystem::path src = backup_dest / entry.destination_path();
        // The destination remains the relative path.
        std::string dst = entry.destination_path().string();
        std::cerr << "src: " << src << " dst: " << dst << std::endl;
        fs_entries.emplace_back(src, dst, false);
    }

    // Before restore, recreate the original location directory.
    ASSERT_TRUE(boost::filesystem::create_directory(location));

    // Call the restore API using the file_set_entry vector.
    status st = datastore_->restore(backup_dest.string(), fs_entries);
    EXPECT_EQ(st, status::ok) << "Restore operation failed.";

    // Step 6: Verify that the restored files in 'location' match those in 'backup_entries'.
    std::set<std::string> restored_files;
    std::set<std::string> backup_files;
    // Recursively scan the restored directory.
    for (const auto& entry : boost::filesystem::recursive_directory_iterator(boost::filesystem::path(location))) {
        if (boost::filesystem::is_regular_file(entry)) {
            restored_files.insert(entry.path().string());
        }
    }

    // Create backup_files set from the backup_entries.
    for (const auto& entry : backup_entries) {
        backup_files.insert(entry.source_path().string());
    }
    EXPECT_EQ(restored_files, backup_files) << "The restored files do not match the backup files.";
}

TEST_F(rotate_test, restore_from_directory_with_blob) {
    // Scenario:
    // 1. Server start: Create channels, write log entries, and register a BLOB file.
    // 2. Server restart with fewer channels.
    // 3. Perform backup using the no-argument begin_backup().
    //    => Verify: All files, including BLOB files, are included in the backup target.
    // 4. Create a backup directory at 'location_backup/bk1' to match the expected backup state.
    // 5. Restore files from the backup directory using the restore API.
    // 6. Verify that the restored files in 'location' match those in 'location_backup/bk1'.

    // Step 1: Setup log channels, write log entries, and register a BLOB file.
    {
        log_channel& channel1_0 = datastore_->create_channel(boost::filesystem::path(location)); // pwal_0000
        log_channel& channel1_1 = datastore_->create_channel(boost::filesystem::path(location)); // pwal_0001
        log_channel& unused_1_2 = datastore_->create_channel(boost::filesystem::path(location));  // pwal_0002 (unused)

        datastore_->ready();
        datastore_->switch_epoch(42);

        channel1_0.begin_session();
        channel1_0.add_entry(2, "k0", "v0", {42, 4});
        channel1_0.end_session();

        channel1_1.begin_session();
        channel1_1.add_entry(2, "k1", "v1", {42, 4});
        channel1_1.end_session();

        // Also, register a BLOB file via datastore API.
        auto blob_pool = datastore_->acquire_blob_pool();
        std::string blob_data = "blob initial data";
        blob_id_type blob_id_initial = blob_pool->register_data(blob_data);
        // To ensure the BLOB file isn't GC-deleted, add an entry referencing it.
        channel1_1.begin_session();
        channel1_1.add_entry(2, "k2", "v2", {42, 5}, {blob_id_initial});
        channel1_1.end_session();

        datastore_->switch_epoch(43);
        datastore_->shutdown();
    }

    // Step 2: Simulate server restart with fewer channels.
    regen_datastore();
    {
        log_channel& channel2_0 = datastore_->create_channel(boost::filesystem::path(location)); // pwal_0000
        log_channel& unused_2_1 = datastore_->create_channel(boost::filesystem::path(location)); // pwal_0001 (unused)
        log_channel& unused_2_2 = datastore_->create_channel(boost::filesystem::path(location)); // pwal_0002 (unused)

        datastore_->ready();
        datastore_->switch_epoch(44);

        channel2_0.begin_session();
        channel2_0.add_entry(2, "k3", "v3", {44, 4});
        channel2_0.end_session();

        datastore_->switch_epoch(45);
        datastore_->shutdown();
    }

    // Step 3: Perform backup using the no-argument begin_backup().
    backup& backup_obj = datastore_->begin_backup();
    // Obtain the backup file list from the backup object.
    // (backup_obj.files() returns a std::vector<boost::filesystem::path>)
    auto backup_files_list = backup_obj.files();

    // Step 4: Manually create the expected backup state for validation.
    // In this test, we assume what `backup_detail` would contain after `begin_backup()`,
    // but no actual backup operation has been performed yet.
    // To validate the restore process, we manually copy the expected files from `location`
    // to `location_backup/bk1`, ensuring that `location_backup/bk1` mimics the expected backup state.

    // Move the database data directory to the backup destination (`location_backup/bk1`).
    boost::filesystem::path backup_src = boost::filesystem::path(location);
    boost::filesystem::path backup_dest = boost::filesystem::path(location_backup) / "bk1";
    boost::filesystem::remove_all(backup_dest);
    boost::filesystem::rename(backup_src, backup_dest);

    // Collect only files that are inside `bk1/blob/`
    std::vector<boost::filesystem::path> files_to_move;
    boost::filesystem::path blob_dir = backup_dest / "blob";
    
    for (boost::filesystem::recursive_directory_iterator it(backup_dest), end; it != end; ++it) {
        // Ensure the file is inside `blob/` by checking if its path starts with `blob_dir`
        if (boost::filesystem::is_regular_file(*it) && it->path().string().find(blob_dir.string()) == 0) {
            files_to_move.push_back(it->path());
        }
    }

    // Move all collected files to `bk1/` root
    for (const auto& file : files_to_move) {
        boost::filesystem::path new_path = backup_dest / file.filename();
        std::cerr << "Moving file: " << file << " to " << new_path << std::endl;
        boost::filesystem::rename(file, new_path);
    }

    // Remove all subdirectories inside `bk1/`
    for (boost::filesystem::directory_iterator it(backup_dest), end; it != end; ++it) {
        if (boost::filesystem::is_directory(*it)) {
            boost::filesystem::remove_all(it->path());
        }
    }

    // Step 5: Restore files from the backup directory using the restore API.
    // Remove the original 'location' and recreate it.
    boost::filesystem::remove_all(location);
    ASSERT_TRUE(boost::filesystem::create_directory(location));

    // Note: restore(from, bool) expects the backup directory path and a flag to keep the backup.
    // Here, we pass the backup directory (backup_dest.string()).
    status st = datastore_->restore(backup_dest.string(), true);
    EXPECT_EQ(st, status::ok) << "Restore operation failed.";

    // Step 6: Verify that the restored files in 'location' match those in 'backup_dest'.
    std::set<std::string> restored_files;
    std::set<std::string> expected_files;

    // Recursively scan the restored directory.
    for (const auto& entry : boost::filesystem::recursive_directory_iterator(boost::filesystem::path(location))) {
        if (boost::filesystem::is_regular_file(entry)) {
            std::string rel = boost::filesystem::relative(entry.path(), location).string();
            // Exclude files that are not part of the backup target.
            if (rel.find("datas/snapshot") == 0)
                continue;
            restored_files.insert(rel);
        }
    }

    for (auto p: backup_files_list) {
        expected_files.insert(boost::filesystem::relative(p, backup_src).string());
    }
    EXPECT_EQ(restored_files, expected_files) << "The restored files do not match the backup files.";

}


}  // namespace limestone::testing
