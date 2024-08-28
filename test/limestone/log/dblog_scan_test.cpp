
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

class dblog_scan_test : public ::testing::Test {
public:
static constexpr const char* location = "/tmp/dblog_scan_test";

    void SetUp() {
        boost::filesystem::remove_all(location);
        if (!boost::filesystem::create_directory(location)) {
            std::cerr << "cannot make directory" << std::endl;
        }
    }

    void TearDown() {
        boost::filesystem::remove_all(location);
    }

    void set_inspect_mode(dblog_scan& ds) {
        ds.set_process_at_nondurable_epoch_snippet(dblog_scan::process_at_nondurable::report);
        ds.set_process_at_truncated_epoch_snippet(dblog_scan::process_at_truncated::report);
        ds.set_process_at_damaged_epoch_snippet(dblog_scan::process_at_damaged::report);
        ds.set_fail_fast(false);
    }
    void set_repair_by_mark_mode(dblog_scan& ds) {
        ds.set_process_at_nondurable_epoch_snippet(dblog_scan::process_at_nondurable::repair_by_mark);
        ds.set_process_at_truncated_epoch_snippet(dblog_scan::process_at_truncated::repair_by_mark);
        ds.set_process_at_damaged_epoch_snippet(dblog_scan::process_at_damaged::repair_by_mark);
        ds.set_fail_fast(false);
    }
    void set_repair_by_cut_mode(dblog_scan& ds) {
        ds.set_process_at_nondurable_epoch_snippet(dblog_scan::process_at_nondurable::repair_by_mark);
        ds.set_process_at_truncated_epoch_snippet(dblog_scan::process_at_truncated::repair_by_cut);
        ds.set_process_at_damaged_epoch_snippet(dblog_scan::process_at_damaged::repair_by_cut);
        ds.set_fail_fast(false);
    }
    bool starts_with(std::string a, std::string b) { return a.substr(0, b.length()) == b; }

    std::vector<boost::filesystem::path> list_dir() {
        std::vector<boost::filesystem::path> ret;
        for (const boost::filesystem::path& p : boost::filesystem::directory_iterator(boost::filesystem::path(location))) {
            if (dblog_scan::is_wal(p)) {
                ret.emplace_back(p);
            }
        }
        return ret;
    }

    void scan_one_pwal_file_inspect(const std::string_view data, std::function<void(const boost::filesystem::path&, epoch_id_type, const std::vector<log_entry::read_error>&, const dblog_scan::parse_error&)> check) {
        auto p = boost::filesystem::path(location) / "pwal_0000";
        create_file(p, data);
        ASSERT_EQ(boost::filesystem::file_size(p), data.size());

        dblog_scan ds{boost::filesystem::path(location)};
        ds.set_thread_num(1);
        set_inspect_mode(ds);
        dblog_scan::parse_error pe;
        std::vector<log_entry::read_error> errors;

        epoch_id_type max_epoch = ds.scan_one_pwal_file(p, 0x100, [](void*, const log_entry& e){
            VLOG(30) << static_cast<int>(e.type());
        }, [&errors](const log_entry::read_error& re){
            VLOG(30) << re.message();
            errors.emplace_back(re);
            return false;
        }, pe, 0);

        check(p, max_epoch, errors, pe);
        EXPECT_EQ(boost::filesystem::file_size(p), data.size());  // no size change
    }

    void scan_one_pwal_file_repairm(const std::string_view data, std::function<void(const boost::filesystem::path&, epoch_id_type, const std::vector<log_entry::read_error>&, const dblog_scan::parse_error&)> check) {
        auto p = boost::filesystem::path(location) / "pwal_0000";
        create_file(p, data);
        ASSERT_EQ(boost::filesystem::file_size(p), data.size());

        dblog_scan ds{boost::filesystem::path(location)};
        ds.set_thread_num(1);
        set_repair_by_mark_mode(ds);
        dblog_scan::parse_error pe;
        std::vector<log_entry::read_error> errors;

        epoch_id_type max_epoch = ds.scan_one_pwal_file(p, 0x100, [](void*, const log_entry& e){
            VLOG(30) << static_cast<int>(e.type());
        }, [&errors](const log_entry::read_error& re){
            VLOG(30) << re.message();
            errors.emplace_back(re);
            return false;
        }, pe, 0);

        check(p, max_epoch, errors, pe);
        EXPECT_EQ(boost::filesystem::file_size(p), data.size());  // no size change
    }

    void scan_one_pwal_file_repairc(const std::string_view data, std::function<void(const boost::filesystem::path&, epoch_id_type, const std::vector<log_entry::read_error>&, const dblog_scan::parse_error&)> check) {
        auto p = boost::filesystem::path(location) / "pwal_0000";
        create_file(p, data);
        ASSERT_EQ(boost::filesystem::file_size(p), data.size());

        dblog_scan ds{boost::filesystem::path(location)};
        ds.set_thread_num(1);
        set_repair_by_cut_mode(ds);
        dblog_scan::parse_error pe;
        std::vector<log_entry::read_error> errors;

        epoch_id_type max_epoch = ds.scan_one_pwal_file(p, 0x100, [](void*, const log_entry& e){
            VLOG(30) << static_cast<int>(e.type());
        }, [&errors](const log_entry::read_error& re){
            VLOG(30) << re.message();
            errors.emplace_back(re);
            return false;
        }, pe, 0);

        check(p, max_epoch, errors, pe);
    }

};

// combination test
// {inspect-mode, repair(mark)-mode, repair(cut)-mode}
//   x
// {normal, nondurable, zerofill, truncated_normal_entry, truncated_epoch_header, truncated_invalidated_normal_entry, truncated_invalidated_epoch_header}

extern const std::string_view data_normal;
extern const std::string_view data_nondurable;
extern const std::string_view data_zerofill;
extern const std::string_view data_truncated_normal_entry;
extern const std::string_view data_truncated_epoch_header;
extern const std::string_view data_truncated_invalidated_normal_entry;
extern const std::string_view data_truncated_invalidated_epoch_header;

// unit-test scan_one_pwal_file
// inspect the normal file; returns ok
TEST_F(dblog_scan_test, scan_one_pwal_file_inspect_normal) {
    scan_one_pwal_file_inspect(data_normal,
                               [](const boost::filesystem::path& p, epoch_id_type max_epoch, const std::vector<log_entry::read_error>& errors, const dblog_scan::parse_error& pe) {
        EXPECT_EQ(max_epoch, 0x100);
        EXPECT_EQ(errors.size(), 0);
        EXPECT_EQ(pe.value(), dblog_scan::parse_error::ok);
    });
}

// unit-test scan_one_pwal_file
// inspect the file including nondurable epoch snippet
TEST_F(dblog_scan_test, scan_one_pwal_file_inspect_nondurable) {
    scan_one_pwal_file_inspect(data_nondurable,
                               [](const boost::filesystem::path& p, epoch_id_type max_epoch, const std::vector<log_entry::read_error>& errors, const dblog_scan::parse_error& pe) {
        EXPECT_EQ(max_epoch, 0x101);
        EXPECT_EQ(errors.size(), 1);  // nondurable
        EXPECT_EQ(pe.value(), dblog_scan::parse_error::nondurable_entries);
    });
}

// unit-test scan_one_pwal_file
// inspect the file filled zero
TEST_F(dblog_scan_test, scan_one_pwal_file_inspect_zerofill) {
    scan_one_pwal_file_inspect(data_zerofill,
                               [](const boost::filesystem::path& p, epoch_id_type max_epoch, const std::vector<log_entry::read_error>& errors, const dblog_scan::parse_error& pe) {
        EXPECT_EQ(max_epoch, 0x101);
        EXPECT_EQ(errors.size(), 1);
        EXPECT_EQ(pe.value(), dblog_scan::parse_error::broken_after);
        EXPECT_EQ(pe.fpos(), 9);
    });
}

// unit-test scan_one_pwal_file
// inspect the file truncated on log_entries
TEST_F(dblog_scan_test, scan_one_pwal_file_inspect_truncated_normal_entry) {
    scan_one_pwal_file_inspect(data_truncated_normal_entry,
                               [](const boost::filesystem::path& p, epoch_id_type max_epoch, const std::vector<log_entry::read_error>& errors, const dblog_scan::parse_error& pe) {
        EXPECT_EQ(max_epoch, 0x101);
        //EXPECT_EQ(errors.size(), 1);  // nondurable
        EXPECT_EQ(pe.value(), dblog_scan::parse_error::broken_after);
        EXPECT_EQ(pe.fpos(), 9);
    });
}

// unit-test scan_one_pwal_file
// inspect the file truncated on epoch_snippet_header
TEST_F(dblog_scan_test, scan_one_pwal_file_inspect_truncated_epoch_header) {
    scan_one_pwal_file_inspect(data_truncated_epoch_header,
                               [](const boost::filesystem::path& p, epoch_id_type max_epoch, const std::vector<log_entry::read_error>& errors, const dblog_scan::parse_error& pe) {
        EXPECT_EQ(max_epoch, 0xff);
        EXPECT_EQ(errors.size(), 1);  // truncated
        EXPECT_EQ(pe.value(), dblog_scan::parse_error::broken_after);
        EXPECT_EQ(pe.fpos(), 50);  // after correct epoch snippet
    });
}

// unit-test scan_one_pwal_file
// inspect the file truncated on log_entries in invalidated epoch snippet
TEST_F(dblog_scan_test, scan_one_pwal_file_inspect_truncated_invalidated_normal_entry) {
    scan_one_pwal_file_inspect(data_truncated_invalidated_normal_entry,
                               [](const boost::filesystem::path& p, epoch_id_type max_epoch, const std::vector<log_entry::read_error>& errors, const dblog_scan::parse_error& pe) {
        EXPECT_EQ(max_epoch, 0x101);
        // EXPECT_EQ(errors.size(), 1);  0 or 1 // ??
        EXPECT_EQ(pe.value(), dblog_scan::parse_error::broken_after_marked);
        EXPECT_EQ(pe.fpos(), 9);
    });
}

// unit-test scan_one_pwal_file
// inspect the file truncated on invalidated epoch_snippet_header
TEST_F(dblog_scan_test, scan_one_pwal_file_inspect_truncated_invalidated_epoch_header) {
    scan_one_pwal_file_inspect(data_truncated_invalidated_epoch_header,
                               [](const boost::filesystem::path& p, epoch_id_type max_epoch, const std::vector<log_entry::read_error>& errors, const dblog_scan::parse_error& pe) {
        EXPECT_EQ(max_epoch, 0xff);
        //EXPECT_EQ(errors.size(), 1);  // ?
        EXPECT_EQ(pe.value(), dblog_scan::parse_error::broken_after_marked);
        EXPECT_EQ(pe.fpos(), 50);  // after correct epoch snippet
    });
}

// unit-test scan_one_pwal_file
// repair(mark) the normal file; returns ok
TEST_F(dblog_scan_test, scan_one_pwal_file_repairm_normal) {
    auto orig_data = data_normal;
    scan_one_pwal_file_repairm(orig_data,
                               [&orig_data](const boost::filesystem::path& p, epoch_id_type max_epoch, const std::vector<log_entry::read_error>& errors, const dblog_scan::parse_error& pe) {
        EXPECT_EQ(max_epoch, 0x100);
        EXPECT_EQ(pe.value(), dblog_scan::parse_error::ok);
        EXPECT_EQ(read_entire_file(p), orig_data);  // no change
    });
}

// unit-test scan_one_pwal_file
// repair(mark) the file including nondurable epoch snippet
TEST_F(dblog_scan_test, scan_one_pwal_file_repairm_nondurable) {
    auto orig_data = data_nondurable;
    scan_one_pwal_file_repairm(orig_data,
                               [&orig_data](const boost::filesystem::path& p, epoch_id_type max_epoch, const std::vector<log_entry::read_error>& errors, const dblog_scan::parse_error& pe) {
        EXPECT_EQ(max_epoch, 0x101);
        EXPECT_EQ(pe.value(), dblog_scan::parse_error::repaired);
        auto data = read_entire_file(p);
        ASSERT_EQ(orig_data.at(9), '\x02');
        EXPECT_EQ(data.at(9), '\x06');  // marked
        EXPECT_EQ(data.substr(0, 9), orig_data.substr(0, 9));  // no change before mark
    });
}

// unit-test scan_one_pwal_file
// repair(mark) the file filled zero
TEST_F(dblog_scan_test, scan_one_pwal_file_repairm_zerofill) {
    auto orig_data = data_zerofill;
    scan_one_pwal_file_repairm(orig_data,
                               [&orig_data](const boost::filesystem::path& p, epoch_id_type max_epoch, const std::vector<log_entry::read_error>& errors, const dblog_scan::parse_error& pe) {
        EXPECT_EQ(max_epoch, 0x101);
        EXPECT_EQ(pe.value(), dblog_scan::parse_error::broken_after_marked);
        EXPECT_EQ(pe.fpos(), 9);
        auto data = read_entire_file(p);
        ASSERT_EQ(orig_data.at(9), '\x02');
        EXPECT_EQ(data.at(9), '\x06');  // marked
        EXPECT_EQ(data.substr(0, 9), orig_data.substr(0, 9));  // no change before mark
    });
}

// unit-test scan_one_pwal_file
// repair(mark) the file truncated on log_entries
TEST_F(dblog_scan_test, scan_one_pwal_file_repairm_truncated_normal_entry) {
    auto orig_data = data_truncated_normal_entry;
    scan_one_pwal_file_repairm(orig_data,
                               [&orig_data](const boost::filesystem::path& p, epoch_id_type max_epoch, const std::vector<log_entry::read_error>& errors, const dblog_scan::parse_error& pe) {
        EXPECT_EQ(max_epoch, 0x101);
        EXPECT_EQ(pe.value(), dblog_scan::parse_error::broken_after_marked);
        EXPECT_EQ(pe.fpos(), 9);
        auto data = read_entire_file(p);
        ASSERT_EQ(orig_data.at(9), '\x02');
        EXPECT_EQ(data.at(9), '\x06');  // marked
        EXPECT_EQ(data.substr(0, 9), orig_data.substr(0, 9));  // no change before mark
    });
}

// unit-test scan_one_pwal_file
// repair(mark) the file truncated on epoch_snippet_header
TEST_F(dblog_scan_test, scan_one_pwal_file_repairm_truncated_epoch_header) {
    auto orig_data = data_truncated_epoch_header;
    scan_one_pwal_file_repairm(orig_data,
                               [&orig_data](const boost::filesystem::path& p, epoch_id_type max_epoch, const std::vector<log_entry::read_error>& errors, const dblog_scan::parse_error& pe) {
        EXPECT_EQ(max_epoch, 0xff);
        EXPECT_EQ(pe.value(), dblog_scan::parse_error::broken_after_marked);
        EXPECT_EQ(pe.fpos(), 50);  // after correct epoch snippet
        auto data = read_entire_file(p);
        ASSERT_EQ(orig_data.at(50), '\x02');
        EXPECT_EQ(data.at(50), '\x06');  // marked
        EXPECT_EQ(data.substr(0, 50), orig_data.substr(0, 50));  // no change before mark
    });
}

// unit-test scan_one_pwal_file
// repair(mark) the file truncated on log_entries in invalidated epoch snippet
TEST_F(dblog_scan_test, scan_one_pwal_file_repairm_truncated_invalidated_normal_entry) {
    auto orig_data = data_truncated_invalidated_normal_entry;
    scan_one_pwal_file_repairm(orig_data,
                               [&orig_data](const boost::filesystem::path& p, epoch_id_type max_epoch, const std::vector<log_entry::read_error>& errors, const dblog_scan::parse_error& pe) {
        EXPECT_EQ(max_epoch, 0x101);
        EXPECT_EQ(pe.value(), dblog_scan::parse_error::broken_after_marked);
        EXPECT_EQ(pe.fpos(), 9);
        // already marked in orig_data
        EXPECT_EQ(read_entire_file(p), orig_data);  // no change
    });
}

// unit-test scan_one_pwal_file
// repair(mark) the file truncated on invalidated epoch_snippet_header
TEST_F(dblog_scan_test, scan_one_pwal_file_repairm_truncated_invalidated_epoch_header) {
    auto orig_data = data_truncated_invalidated_epoch_header;
    scan_one_pwal_file_repairm(orig_data,
                               [&orig_data](const boost::filesystem::path& p, epoch_id_type max_epoch, const std::vector<log_entry::read_error>& errors, const dblog_scan::parse_error& pe) {
        EXPECT_EQ(max_epoch, 0xff);
        EXPECT_EQ(pe.value(), dblog_scan::parse_error::broken_after_marked);
        EXPECT_EQ(pe.fpos(), 50);  // after correct epoch snippet
        // already marked in orig_data
        auto data = read_entire_file(p);
        EXPECT_EQ(data.at(50), '\x06');  // marked
        EXPECT_EQ(read_entire_file(p), orig_data);  // no change
    });
}

// unit-test scan_one_pwal_file
// repair(cut) the normal file; returns ok
// same as avobe

// unit-test scan_one_pwal_file
// repair(cut) is not supported

// unit-test scan_one_pwal_file
// repair(mark) the file filled zero
TEST_F(dblog_scan_test, scan_one_pwal_file_repairc_zerofill) {
    scan_one_pwal_file_repairc(data_zerofill,
                               [](const boost::filesystem::path& p, epoch_id_type max_epoch, const std::vector<log_entry::read_error>& errors, const dblog_scan::parse_error& pe) {
        EXPECT_EQ(max_epoch, 0x101);
        EXPECT_EQ(pe.value(), dblog_scan::parse_error::repaired);
        EXPECT_EQ(pe.fpos(), 9);
        EXPECT_EQ(boost::filesystem::file_size(p), 9);
    });
}

// unit-test scan_one_pwal_file
// repair(cut) the file truncated on log_entries
TEST_F(dblog_scan_test, scan_one_pwal_file_repairc_truncated_normal_entry) {
    scan_one_pwal_file_repairc(data_truncated_normal_entry,
                               [](const boost::filesystem::path& p, epoch_id_type max_epoch, const std::vector<log_entry::read_error>& errors, const dblog_scan::parse_error& pe) {
        EXPECT_EQ(max_epoch, 0x101);
        EXPECT_EQ(pe.value(), dblog_scan::parse_error::repaired);
        EXPECT_EQ(pe.fpos(), 9);
        EXPECT_EQ(boost::filesystem::file_size(p), 9);
    });
}

// unit-test scan_one_pwal_file
// repair(cut) the file truncated on epoch_snippet_header
TEST_F(dblog_scan_test, scan_one_pwal_file_repairc_truncated_epoch_header) {
    scan_one_pwal_file_repairc(data_truncated_epoch_header,
                               [](const boost::filesystem::path& p, epoch_id_type max_epoch, const std::vector<log_entry::read_error>& errors, const dblog_scan::parse_error& pe) {
        EXPECT_EQ(max_epoch, 0xff);
        EXPECT_EQ(pe.value(), dblog_scan::parse_error::repaired);
        EXPECT_EQ(pe.fpos(), 50);
        EXPECT_EQ(boost::filesystem::file_size(p), 50);
    });
}

// unit-test scan_one_pwal_file
// repair(cut) the file truncated on log_entries in invalidated epoch snippet
TEST_F(dblog_scan_test, scan_one_pwal_file_repairc_truncated_invalidated_normal_entry) {
    scan_one_pwal_file_repairc(data_truncated_invalidated_normal_entry,
                               [](const boost::filesystem::path& p, epoch_id_type max_epoch, const std::vector<log_entry::read_error>& errors, const dblog_scan::parse_error& pe) {
        EXPECT_EQ(max_epoch, 0x101);
        EXPECT_EQ(pe.value(), dblog_scan::parse_error::repaired);
        EXPECT_EQ(pe.fpos(), 9);
        EXPECT_EQ(boost::filesystem::file_size(p), 9);
    });
}

// unit-test scan_one_pwal_file
// repair(cut) the file truncated on invalidated epoch_snippet_header
TEST_F(dblog_scan_test, scan_one_pwal_file_repairc_truncated_invalidated_epoch_header) {
    scan_one_pwal_file_repairc(data_truncated_epoch_header,
                               [](const boost::filesystem::path& p, epoch_id_type max_epoch, const std::vector<log_entry::read_error>& errors, const dblog_scan::parse_error& pe) {
        EXPECT_EQ(max_epoch, 0xff);
        EXPECT_EQ(pe.value(), dblog_scan::parse_error::repaired);
        EXPECT_EQ(pe.fpos(), 50);  // after correct epoch snippet
        EXPECT_EQ(boost::filesystem::file_size(p), 50);
    });
}

// unit-test detach_wal_files; normal non-detached pwal files are renamed (rotated)
TEST_F(dblog_scan_test, detach_wal_files_renamne_pwal_0000) {
    auto p0_attached = boost::filesystem::path(location) / "pwal_0000";
    create_file(p0_attached,
                "\x02\xff\x00\x00\x00\x00\x00\x00\x00"  // marger_begin 0xff
                // XXX: epoch footer...
                ""sv);
    {
        auto wal_files = list_dir();
        ASSERT_EQ(wal_files.size(), 1);
        ASSERT_EQ(wal_files.at(0), p0_attached);
    }
    dblog_scan ds{boost::filesystem::path(location)};
    ds.detach_wal_files();
    {  // rotated
        auto wal_files = list_dir();
        EXPECT_EQ(wal_files.size(), 1);
        EXPECT_NE(wal_files.at(0), p0_attached);
        EXPECT_GT(wal_files.at(0).filename().string().length(), 10);
    }
}

// unit-test detach_wal_files; empty pwal files are skipped
TEST_F(dblog_scan_test, detach_wal_files_skip_rename_empty_pwal) {
    auto p0_attached_empty = boost::filesystem::path(location) / "pwal_0000";
    create_file(p0_attached_empty, ""sv);
    {
        auto wal_files = list_dir();
        ASSERT_EQ(wal_files.size(), 1);
        ASSERT_EQ(wal_files.at(0), p0_attached_empty);
    }
    dblog_scan ds{boost::filesystem::path(location)};
    ds.detach_wal_files();
    {  // no change
        auto wal_files = list_dir();
        EXPECT_EQ(wal_files.size(), 1);
        EXPECT_EQ(wal_files.at(0), p0_attached_empty);
    }
}

// unit-test detach_wal_files; detached (rotated) pwal files are skipped
TEST_F(dblog_scan_test, detach_wal_files_skip_rename_pwal_0000_somewhat) {
    auto p0_detached = boost::filesystem::path(location) / "pwal_0000.somewhat";
    create_file(p0_detached,
                "\x02\xff\x00\x00\x00\x00\x00\x00\x00"  // marger_begin 0xff
                // XXX: epoch footer...
                ""sv);
    {
        auto wal_files = list_dir();
        ASSERT_EQ(wal_files.size(), 1);
        ASSERT_EQ(wal_files.at(0), p0_detached);
    }
    dblog_scan ds{boost::filesystem::path(location)};
    ds.detach_wal_files();
    {  // no change
        auto wal_files = list_dir();
        EXPECT_EQ(wal_files.size(), 1);
        EXPECT_EQ(wal_files.at(0), p0_detached);
    }
}

}  // namespace limestone::testing
