
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

    void scan_one_pwal_file_inspect(
        const std::string_view data,
        std::function<void(const boost::filesystem::path&, epoch_id_type, const std::vector<log_entry::read_error>&, const dblog_scan::parse_error&)> check,
        epoch_id_type ld_epoch = 0x100) {
        auto p = boost::filesystem::path(location) / "pwal_0000";
        create_file(p, data);
        ASSERT_EQ(boost::filesystem::file_size(p), data.size());

        dblog_scan ds{boost::filesystem::path(location)};
        ds.set_thread_num(1);
        set_inspect_mode(ds);
        dblog_scan::parse_error pe;
        std::vector<log_entry::read_error> errors;

        epoch_id_type max_epoch = ds.scan_one_pwal_file(p, ld_epoch, [](const log_entry& e){
            VLOG(30) << static_cast<int>(e.type());
        }, [&errors](const log_entry::read_error& re){
            VLOG(30) << re.message();
            errors.emplace_back(re);
            return false;
        }, pe);

        check(p, max_epoch, errors, pe);
        EXPECT_EQ(boost::filesystem::file_size(p), data.size());  // no size change
    }

    void scan_one_pwal_file_repairm(
        const std::string_view data,
        std::function<void(const boost::filesystem::path&, epoch_id_type, const std::vector<log_entry::read_error>&, const dblog_scan::parse_error&)> check,
        epoch_id_type ld_epoch = 0x100) {
        auto p = boost::filesystem::path(location) / "pwal_0000";
        create_file(p, data);
        ASSERT_EQ(boost::filesystem::file_size(p), data.size());

        dblog_scan ds{boost::filesystem::path(location)};
        ds.set_thread_num(1);
        set_repair_by_mark_mode(ds);
        dblog_scan::parse_error pe;
        std::vector<log_entry::read_error> errors;

        epoch_id_type max_epoch = ds.scan_one_pwal_file(p, ld_epoch, [](const log_entry& e){
            VLOG(30) << static_cast<int>(e.type());
        }, [&errors](const log_entry::read_error& re){
            VLOG(30) << re.message();
            errors.emplace_back(re);
            return false;
        }, pe);

        check(p, max_epoch, errors, pe);
    }

    void scan_one_pwal_file_repairc(
        const std::string_view data,
        std::function<void(const boost::filesystem::path&, epoch_id_type, const std::vector<log_entry::read_error>&, const dblog_scan::parse_error&)> check,
        epoch_id_type ld_epoch = 0x100) {
        auto p = boost::filesystem::path(location) / "pwal_0000";
        create_file(p, data);
        ASSERT_EQ(boost::filesystem::file_size(p), data.size());

        dblog_scan ds{boost::filesystem::path(location)};
        ds.set_thread_num(1);
        set_repair_by_cut_mode(ds);
        dblog_scan::parse_error pe;
        std::vector<log_entry::read_error> errors;

        epoch_id_type max_epoch = ds.scan_one_pwal_file(p, ld_epoch, [](const log_entry& e){
            VLOG(30) << static_cast<int>(e.type());
        }, [&errors](const log_entry::read_error& re){
            VLOG(30) << re.message();
            errors.emplace_back(re);
            return false;
        }, pe);

        check(p, max_epoch, errors, pe);
    }
    void hexdump(std::string_view data, const std::string& name = "") {
        const size_t bytes_per_line = 16;

        if (!name.empty()) {
            std::cerr << name << ":\n";
        }

        for (size_t i = 0; i < data.size(); i += bytes_per_line) {
            std::cerr << std::setw(4) << std::setfill('0') << std::hex << i << ": ";

            // Output bytes in hexadecimal
            for (size_t j = 0; j < bytes_per_line; ++j) {
                if (i + j < data.size()) {
                    std::cerr << std::setw(2) << static_cast<unsigned>(static_cast<unsigned char>(data[i + j])) << " ";
                } else {
                    std::cerr << "   ";
                }
            }

            std::cerr << " ";

            // Output bytes as ASCII
            for (size_t j = 0; j < bytes_per_line; ++j) {
                if (i + j < data.size()) {
                    unsigned char c = static_cast<unsigned char>(data[i + j]);
                    if (std::isprint(c)) {
                        std::cerr << c;
                    } else {
                        std::cerr << ".";
                    }
                }
            }

            std::cerr << "\n";
        }
        std::cerr << std::dec;  
    }

    std::string concat_binary(std::string_view a, std::string_view b) {
        std::string result;
        result.resize(a.size() + b.size());
        std::memcpy(result.data(), a.data(), a.size());
        std::memcpy(result.data() + a.size(), b.data(), b.size());
        return result;
    }
};

// combination test
// {inspect-mode, repair(mark)-mode, repair(cut)-mode}
//   x
// {normal, nondurable, zerofill, truncated_normal_entry, truncated_epoch_header, truncated_invalidated_normal_entry, truncated_invalidated_epoch_header}

// Existing data
extern const std::string_view data_normal;
extern const std::string_view data_nondurable;
extern const std::string_view data_zerofill;
extern const std::string_view data_truncated_normal_entry;
extern const std::string_view data_truncated_epoch_header;
extern const std::string_view data_truncated_invalidated_normal_entry;
extern const std::string_view data_truncated_invalidated_epoch_header;

extern const std::string_view data_marker_end_only;      
extern const std::string_view data_marker_end_followed_by_normal_entry; 
extern const std::string_view data_marker_end_followed_by_marker_begin; 
extern const std::string_view data_marker_end_followed_by_marker_inv_begin; 
extern const std::string_view data_marker_end_followed_by_short_entry;   

// SHORT_marker_end patterns
extern const std::string_view data_short_marker_end_only;  
extern const std::string_view data_short_marker_end_followed_by_normal_entry;
extern const std::string_view data_short_marker_end_followed_by_marker_begin;
extern const std::string_view data_short_marker_end_followed_by_marker_inv_begin;
extern const std::string_view data_short_marker_end_followed_by_short_entry; 


// 0fill variations
extern const std::string_view data_all_zerofill;
extern const std::string_view data_marker_begin_partial_zerofill;
extern const std::string_view data_marker_begin_followed_by_zerofill;
extern const std::string_view data_marker_begin_normal_entry_partial_zerofill;
extern const std::string_view data_marker_begin_normal_entry_followed_by_zerofill;
extern const std::string_view data_marker_end_partial_zerofill;
extern const std::string_view valid_snippet;

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
    auto orig_data = data_zerofill;

    // Case 1: durable_epoch == 0x101 -> durable -> corrupted_durable_entries
    scan_one_pwal_file_inspect(
        orig_data,
        [](const boost::filesystem::path& p, epoch_id_type max_epoch,
           const std::vector<log_entry::read_error>& errors,
           const dblog_scan::parse_error& pe) {
            EXPECT_EQ(max_epoch, 0x101);
            EXPECT_EQ(errors.size(), 1);
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::corrupted_durable_entries);
            EXPECT_EQ(pe.fpos(), 9);
        },
        0x101
    );

    // Case 2: durable_epoch < 0x101 -> nondurable -> broken_after
    scan_one_pwal_file_inspect(
        orig_data,
        [](const boost::filesystem::path& p, epoch_id_type max_epoch,
           const std::vector<log_entry::read_error>& errors,
           const dblog_scan::parse_error& pe) {
            EXPECT_EQ(max_epoch, 0x101);
            EXPECT_EQ(errors.size(), 1);
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::broken_after);
            EXPECT_EQ(pe.fpos(), 9);
        },
        0x100
    );

    // Case 3: durable_epoch > 0x101 -> durable -> corrupted_durable_entries
    scan_one_pwal_file_inspect(
        orig_data,
        [](const boost::filesystem::path& p, epoch_id_type max_epoch,
           const std::vector<log_entry::read_error>& errors,
           const dblog_scan::parse_error& pe) {
            EXPECT_EQ(max_epoch, 0x101);
            EXPECT_EQ(errors.size(), 1);
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::corrupted_durable_entries);
            EXPECT_EQ(pe.fpos(), 9);
        },
        0x102
    );
}

// unit-test scan_one_pwal_file
// inspect the file truncated on log_entries
TEST_F(dblog_scan_test, scan_one_pwal_file_inspect_truncated_normal_entry) {
    auto orig_data = data_truncated_normal_entry;

    // Case 1: durable_epoch == 0x101 -> durable -> corrupted_durable_entries
    scan_one_pwal_file_inspect(
        orig_data,
        [](const boost::filesystem::path& p, epoch_id_type max_epoch, const std::vector<log_entry::read_error>& errors,
           const dblog_scan::parse_error& pe) {
            EXPECT_EQ(max_epoch, 0x101);
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::corrupted_durable_entries);
            EXPECT_EQ(pe.fpos(), 9);
        },
        0x101
    );

    // Case 2: durable_epoch < 0x101 -> nondurable -> broken_after
    scan_one_pwal_file_inspect(
        orig_data,
        [](const boost::filesystem::path& p, epoch_id_type max_epoch, const std::vector<log_entry::read_error>& errors,
           const dblog_scan::parse_error& pe) {
            EXPECT_EQ(max_epoch, 0x101);
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::broken_after);
            EXPECT_EQ(pe.fpos(), 9);
        },
        0x100
    );

    // Case 3: durable_epoch > 0x101 -> durable -> corrupted_durable_entries
    scan_one_pwal_file_inspect(
        orig_data,
        [](const boost::filesystem::path& p, epoch_id_type max_epoch, const std::vector<log_entry::read_error>& errors,
           const dblog_scan::parse_error& pe) {
            EXPECT_EQ(max_epoch, 0x101);
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::corrupted_durable_entries);
            EXPECT_EQ(pe.fpos(), 9);
        },
        0x102
    );
}

// unit-test scan_one_pwal_file
// inspect the file truncated on epoch_snippet_header
TEST_F(dblog_scan_test, scan_one_pwal_file_inspect_truncated_epoch_header) {
    // durable_epoch = 0xff (== epoch)
    scan_one_pwal_file_inspect(
        data_truncated_epoch_header,
        [](const boost::filesystem::path& p, epoch_id_type max_epoch, const std::vector<log_entry::read_error>& errors, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(max_epoch, 0xff);
            EXPECT_EQ(errors.size(), 1);  // corrupted durable
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::corrupted_durable_entries);
            EXPECT_EQ(pe.fpos(), 50);  // after correct epoch snippet
        },
        0xff  // durable epoch exactly matches epoch => durable
    );

    // durable_epoch = 0xfe ( < epoch )
    scan_one_pwal_file_inspect(
        data_truncated_epoch_header,
        [](const boost::filesystem::path& p, epoch_id_type max_epoch, const std::vector<log_entry::read_error>& errors, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(max_epoch, 0xff);
            EXPECT_EQ(errors.size(), 2);  // 2 entries: not durable and short
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::broken_after);
            EXPECT_EQ(pe.fpos(), 50);
        },
        0xfe  // durable epoch below => still durable
    );

    // durable_epoch = 0x100 ( > epoch )
    scan_one_pwal_file_inspect(
        data_truncated_epoch_header,
        [](const boost::filesystem::path& p, epoch_id_type max_epoch, const std::vector<log_entry::read_error>& errors, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(max_epoch, 0xff);
            EXPECT_EQ(errors.size(), 1);  // short, not durable
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::corrupted_durable_entries);
            EXPECT_EQ(pe.fpos(), 50);
        },
        0x100  // durable epoch above => not durable
    );
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
    auto orig_data = data_truncated_invalidated_epoch_header;

    // Case 1: durable_epoch == current_epoch -> valid snippet -> SHORT_marker_inv_begin is ignored -> no error
    scan_one_pwal_file_inspect(
        orig_data,
        [](const boost::filesystem::path& p, epoch_id_type max_epoch,
           const std::vector<log_entry::read_error>& errors,
           const dblog_scan::parse_error& pe) {
            EXPECT_EQ(max_epoch, 0xff);
            EXPECT_EQ(errors.size(), 0); 
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::ok);
            EXPECT_EQ(pe.fpos(), -1);
        },
        0xff
    );

    // Case 2: durable_epoch < current_epoch -> snippet is non-durable -> report nondurable_entries
    scan_one_pwal_file_inspect(
        orig_data,
        [](const boost::filesystem::path& p, epoch_id_type max_epoch,
           const std::vector<log_entry::read_error>& errors,
           const dblog_scan::parse_error& pe) {
            EXPECT_EQ(max_epoch, 0xff);
            EXPECT_EQ(errors.size(), 1);  
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::nondurable_entries);
            EXPECT_EQ(pe.fpos(), -1);
        },
        0xfe
    );

    // Case 3: durable_epoch > current_epoch -> valid snippet -> no error
    scan_one_pwal_file_inspect(
        orig_data,
        [](const boost::filesystem::path& p, epoch_id_type max_epoch,
           const std::vector<log_entry::read_error>& errors,
           const dblog_scan::parse_error& pe) {
            EXPECT_EQ(max_epoch, 0xff);
            EXPECT_EQ(errors.size(), 0);  
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::ok);
            EXPECT_EQ(pe.fpos(), -1);
        },
        0x100
    );
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

    // Case 1: durable_epoch == 0x101 -> durable -> no repair
    scan_one_pwal_file_repairm(
        orig_data,
        [&orig_data](const boost::filesystem::path& p, epoch_id_type max_epoch,
                     const std::vector<log_entry::read_error>& errors,
                     const dblog_scan::parse_error& pe) {
            EXPECT_EQ(max_epoch, 0x101);
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::ok);
            auto data = read_entire_file(p);
            EXPECT_EQ(data, orig_data);  // no change
        },
        0x101
    );

    // Case 2: durable_epoch < 0x101 -> nondurable -> repair by mark
    scan_one_pwal_file_repairm(
        orig_data,
        [&orig_data](const boost::filesystem::path& p, epoch_id_type max_epoch,
                     const std::vector<log_entry::read_error>& errors,
                     const dblog_scan::parse_error& pe) {
            EXPECT_EQ(max_epoch, 0x101);
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::repaired);
            auto data = read_entire_file(p);
            ASSERT_EQ(orig_data.at(9), '\x02');
            EXPECT_EQ(data.at(9), '\x06');  // marked
            EXPECT_EQ(data.substr(0, 9), orig_data.substr(0, 9));  // unchanged before
        },
        0x100
    );

    // Case 3: durable_epoch > 0x101 -> durable -> no repair
    scan_one_pwal_file_repairm(
        orig_data,
        [&orig_data](const boost::filesystem::path& p, epoch_id_type max_epoch,
                     const std::vector<log_entry::read_error>& errors,
                     const dblog_scan::parse_error& pe) {
            EXPECT_EQ(max_epoch, 0x101);
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::ok);
            auto data = read_entire_file(p);
            EXPECT_EQ(data, orig_data);  // no change
        },
        0x102
    );
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

    // Case 1: durable_epoch == 0x101 -> durable -> no mark
    scan_one_pwal_file_repairm(
        orig_data,
        [&orig_data](const boost::filesystem::path& p, epoch_id_type max_epoch,
                     const std::vector<log_entry::read_error>& errors, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(max_epoch, 0x101);
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::corrupted_durable_entries);
            EXPECT_EQ(pe.fpos(), 9);
            auto data = read_entire_file(p);
            EXPECT_EQ(data, orig_data);  // no change
        },
        0x101
    );

    // Case 2: durable_epoch < 0x101 -> nondurable -> mark
    scan_one_pwal_file_repairm(
        orig_data,
        [&orig_data](const boost::filesystem::path& p, epoch_id_type max_epoch,
                     const std::vector<log_entry::read_error>& errors, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(max_epoch, 0x101);
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::broken_after_marked);
            EXPECT_EQ(pe.fpos(), 9);
            auto data = read_entire_file(p);
            ASSERT_EQ(orig_data.at(9), '\x02');
            EXPECT_EQ(data.at(9), '\x06');  // marker invalidated
            EXPECT_EQ(data.substr(0, 9), orig_data.substr(0, 9));  // no change before mark
        },
        0x100
    );

    // Case 3: durable_epoch > 0x101 -> durable -> no mark
    scan_one_pwal_file_repairm(
        orig_data,
        [&orig_data](const boost::filesystem::path& p, epoch_id_type max_epoch,
                     const std::vector<log_entry::read_error>& errors, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(max_epoch, 0x101);
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::corrupted_durable_entries);
            EXPECT_EQ(pe.fpos(), 9);
            auto data = read_entire_file(p);
            EXPECT_EQ(data, orig_data);  // no change
        },
        0x102
    );
}

// unit-test scan_one_pwal_file
// repair(mark) the file truncated on epoch_snippet_header
TEST_F(dblog_scan_test, scan_one_pwal_file_repairm_truncated_epoch_header) {
    auto orig_data = data_truncated_epoch_header;

    // Check with boundary values for durable epoch
    scan_one_pwal_file_repairm(
        orig_data,
        [&orig_data](const boost::filesystem::path& p, epoch_id_type max_epoch, const std::vector<log_entry::read_error>& errors, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(max_epoch, 0xff);
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::corrupted_durable_entries);
            EXPECT_EQ(pe.fpos(), 50);

            // Not repaired, so content remains unchanged
            EXPECT_EQ(boost::filesystem::file_size(p), orig_data.size());
            EXPECT_EQ(read_entire_file(p), orig_data);
        },
        0xff // durable epoch = epoch, so durable -> corrupted_durable_entries -> no repair
    );

    scan_one_pwal_file_repairm(
        orig_data,
        [&orig_data](const boost::filesystem::path& p, epoch_id_type max_epoch, const std::vector<log_entry::read_error>& errors, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(max_epoch, 0xff);
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::broken_after_marked);
            EXPECT_EQ(pe.fpos(), 50);

            // Should be repaired, replaced with invalid marker
            auto data = read_entire_file(p);
            ASSERT_EQ(data.at(0), '\x06'); // The first marker_begin is also marked
            EXPECT_EQ(data.at(50), '\x06');  // SHORT_marker_begin is also marked
            EXPECT_EQ(data.substr(1, 49), orig_data.substr(1, 49));
        },
        0xfe // durable epoch < epoch -> not durable -> mark repair applies
    );

    scan_one_pwal_file_repairm(
        orig_data,
        [&orig_data](const boost::filesystem::path& p, epoch_id_type max_epoch, const std::vector<log_entry::read_error>& errors, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(max_epoch, 0xff);
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::corrupted_durable_entries);
            EXPECT_EQ(pe.fpos(), 50);

            auto data = read_entire_file(p);
            ASSERT_EQ(orig_data.at(50), '\x02');
            EXPECT_EQ(data.at(50), '\x02');  // marked
            EXPECT_EQ(data.substr(0, 50), orig_data.substr(0, 50));
        },
        0x100 // durable epoch > epoch -> not durable -> mark repair applies
    );
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

    // Case 1: durable_epoch == current_epoch -> valid snippet -> SHORT_marker_inv_begin is ignored -> no repair, file unchanged
    scan_one_pwal_file_repairm(
        orig_data,
        [&orig_data](const boost::filesystem::path& p, epoch_id_type max_epoch,
                     const std::vector<log_entry::read_error>& errors,
                     const dblog_scan::parse_error& pe) {
            EXPECT_EQ(max_epoch, 0xff);
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::ok);
            EXPECT_EQ(pe.fpos(), -1);
            auto data = read_entire_file(p);
            EXPECT_EQ(data, orig_data);  // no change
        },
        0xff
    );

    // Case 2: durable_epoch < current_epoch -> snippet is nondurable -> repair_by_mark -> leading marker invalidated
    scan_one_pwal_file_repairm(
        orig_data,
        [&orig_data](const boost::filesystem::path& p, epoch_id_type max_epoch, const std::vector<log_entry::read_error>& errors,
                     const dblog_scan::parse_error& pe) {
            EXPECT_EQ(max_epoch, 0xff);
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::repaired);
            EXPECT_EQ(pe.fpos(), -1);

            auto data = read_entire_file(p);
            EXPECT_EQ(data.at(0), '\x06');   // leading marker is invalidated
            EXPECT_EQ(data.at(50), '\x06');  // SHORT_marker_inv_begin already invalidated
            EXPECT_EQ(data.substr(1, 49), orig_data.substr(1, 49));
        },
        0xfe);

    // Case 3: durable_epoch > current_epoch -> valid snippet -> SHORT_marker_inv_begin is ignored -> no repair, file unchanged
    scan_one_pwal_file_repairm(
        orig_data,
        [&orig_data](const boost::filesystem::path& p, epoch_id_type max_epoch,
                     const std::vector<log_entry::read_error>& errors,
                     const dblog_scan::parse_error& pe) {
            EXPECT_EQ(max_epoch, 0xff);
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::ok);
            EXPECT_EQ(pe.fpos(), -1);
            auto data = read_entire_file(p);
            EXPECT_EQ(data, orig_data);  // no change
        },
        0x100
    );
}



// unit-test scan_one_pwal_file
// repair(cut) the normal file; returns ok
// same as avobe

// unit-test scan_one_pwal_file
// repair(cut) is not supported

// unit-test scan_one_pwal_file
// repair(mark) the file filled zero
TEST_F(dblog_scan_test, scan_one_pwal_file_repairc_zerofill) {
    auto orig_data = data_zerofill;

    // Case 1: durable_epoch == 0x101 -> durable -> corrupted_durable_entries -> cutされない
    scan_one_pwal_file_repairc(
        orig_data,
        [&orig_data](const boost::filesystem::path& p, epoch_id_type max_epoch,
                     const std::vector<log_entry::read_error>& errors,
                     const dblog_scan::parse_error& pe) {
            EXPECT_EQ(max_epoch, 0x101);
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::corrupted_durable_entries);
            EXPECT_EQ(pe.fpos(), 9);
            EXPECT_EQ(boost::filesystem::file_size(p), orig_data.size());
        },
        0x101
    );

    // Case 2: durable_epoch < 0x101 -> nondurable -> repaired -> cutされる
    scan_one_pwal_file_repairc(
        orig_data,
        [](const boost::filesystem::path& p, epoch_id_type max_epoch,
           const std::vector<log_entry::read_error>& errors,
           const dblog_scan::parse_error& pe) {
            EXPECT_EQ(max_epoch, 0x101);
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::repaired);
            EXPECT_EQ(pe.fpos(), 9);
            EXPECT_EQ(boost::filesystem::file_size(p), 9);
        },
        0x100
    );

    // Case 3: durable_epoch > 0x101 -> durable -> corrupted_durable_entries -> cutされない
    scan_one_pwal_file_repairc(
        orig_data,
        [&orig_data](const boost::filesystem::path& p, epoch_id_type max_epoch,
                     const std::vector<log_entry::read_error>& errors,
                     const dblog_scan::parse_error& pe) {
            EXPECT_EQ(max_epoch, 0x101);
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::corrupted_durable_entries);
            EXPECT_EQ(pe.fpos(), 9);
            EXPECT_EQ(boost::filesystem::file_size(p), orig_data.size());
        },
        0x102
    );
}


// unit-test scan_one_pwal_file
// repair(cut) the file truncated on log_entries
TEST_F(dblog_scan_test, scan_one_pwal_file_repairc_truncated_normal_entry) {
    auto orig_data = data_truncated_normal_entry;

    // Case 1: durable_epoch == 0x101 -> durable -> corrupted_durable_entries -> cutされない
    scan_one_pwal_file_repairc(
        orig_data,
        [&orig_data](const boost::filesystem::path& p, epoch_id_type max_epoch,
           const std::vector<log_entry::read_error>& errors, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(max_epoch, 0x101);
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::corrupted_durable_entries);
            EXPECT_EQ(pe.fpos(), 9);
            EXPECT_EQ(boost::filesystem::file_size(p), orig_data.size());  // cutされない
        },
        0x101
    );

    // Case 2: durable_epoch < 0x101 -> nondurable -> repaired -> cutされる
    scan_one_pwal_file_repairc(
        orig_data,
        [](const boost::filesystem::path& p, epoch_id_type max_epoch,
           const std::vector<log_entry::read_error>& errors, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(max_epoch, 0x101);
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::repaired);
            EXPECT_EQ(pe.fpos(), 9);
            EXPECT_EQ(boost::filesystem::file_size(p), 9);
        },
        0x100
    );

    // Case 3: durable_epoch > 0x101 -> durable -> corrupted_durable_entries -> cutされない
    scan_one_pwal_file_repairc(
        orig_data,
        [&orig_data](const boost::filesystem::path& p, epoch_id_type max_epoch,
           const std::vector<log_entry::read_error>& errors, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(max_epoch, 0x101);
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::corrupted_durable_entries);
            EXPECT_EQ(pe.fpos(), 9);
            EXPECT_EQ(boost::filesystem::file_size(p), orig_data.size());  // cutされない
        },
        0x102
    );
}


// unit-test scan_one_pwal_file
// repair(cut) the file truncated on epoch_snippet_header
TEST_F(dblog_scan_test, scan_one_pwal_file_repairc_truncated_epoch_header) {
    auto orig_data = data_truncated_epoch_header;

    // durable_epoch == current_epoch -> durable -> corrupted_durable_entries -> no cut
    scan_one_pwal_file_repairc(
        orig_data,
        [&orig_data](const boost::filesystem::path& p, epoch_id_type max_epoch, const std::vector<log_entry::read_error>& errors, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(max_epoch, 0xff);
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::corrupted_durable_entries);
            EXPECT_EQ(pe.fpos(), 50);

            // Not cut -> original size
            EXPECT_EQ(boost::filesystem::file_size(p), orig_data.size());
            EXPECT_EQ(read_entire_file(p), orig_data);
        },
        0xff
    );

    // durable_epoch < current_epoch -> nondurable -> cut applies
    scan_one_pwal_file_repairc(
        orig_data,
        [&orig_data](const boost::filesystem::path& p, epoch_id_type max_epoch, const std::vector<log_entry::read_error>& errors, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(max_epoch, 0xff);
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::repaired);
            EXPECT_EQ(pe.fpos(), 50);

            // Cut -> file size reduced
            EXPECT_EQ(boost::filesystem::file_size(p), 50);
        },
        0xfe
    );

    // durable_epoch > current_epoch -> durable -> corrupted_durable_entries -> no cut
    scan_one_pwal_file_repairc(
        orig_data,
        [&orig_data](const boost::filesystem::path& p, epoch_id_type max_epoch, const std::vector<log_entry::read_error>& errors, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(max_epoch, 0xff);
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::corrupted_durable_entries);
            EXPECT_EQ(pe.fpos(), 50);

            EXPECT_EQ(boost::filesystem::file_size(p), orig_data.size());
            EXPECT_EQ(read_entire_file(p), orig_data);
        },
        0x100
    );
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
// verify that SHORT_marker_inv_begin is ignored without physical repair
TEST_F(dblog_scan_test, scan_one_pwal_file_repairc_truncated_invalidated_epoch_header) {
    auto orig_data = data_truncated_invalidated_epoch_header;

    // Case 1: durable_epoch == current_epoch -> SHORT_marker_inv_begin -> not cut
    scan_one_pwal_file_repairc(
        orig_data,
        [&orig_data](const boost::filesystem::path& p, epoch_id_type max_epoch,
                     const std::vector<log_entry::read_error>& errors,
                     const dblog_scan::parse_error& pe) {
            EXPECT_EQ(max_epoch, 0xff);
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::ok);
            EXPECT_EQ(pe.fpos(), -1);
            EXPECT_EQ(boost::filesystem::file_size(p), 58);  // not trimmed
        },
        0xff
    );

    // Case 2: durable_epoch < current_epoch -> SHORT_marker_inv_begin -> not cut
    scan_one_pwal_file_repairc(
        orig_data,
        [&orig_data](const boost::filesystem::path& p, epoch_id_type max_epoch,
                     const std::vector<log_entry::read_error>& errors,
                     const dblog_scan::parse_error& pe) {
            EXPECT_EQ(max_epoch, 0xff);
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::repaired);
            EXPECT_EQ(pe.fpos(), -1);
            EXPECT_EQ(boost::filesystem::file_size(p), 58);  // not trimmed
        },
        0xfe
    );

    // Case 3: durable_epoch > current_epoch -> SHORT_marker_inv_begin -> not cut
    scan_one_pwal_file_repairc(
        orig_data,
        [&orig_data](const boost::filesystem::path& p, epoch_id_type max_epoch,
                     const std::vector<log_entry::read_error>& errors,
                     const dblog_scan::parse_error& pe) {
            EXPECT_EQ(max_epoch, 0xff);
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::ok);
            EXPECT_EQ(pe.fpos(), -1);
            EXPECT_EQ(boost::filesystem::file_size(p), 58);  // not trimmed
        },
        0x100
    );
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

TEST_F(dblog_scan_test, scan_one_pwal_file_inspect_marker_end_only) {
    auto orig_data = data_marker_end_only;
    // Case 1: durable_epoch < epoch (0x0FF < 0x100) -> broken_after
    scan_one_pwal_file_inspect(
        orig_data,
        [](const boost::filesystem::path& p,
           epoch_id_type max_epoch,
           const std::vector<log_entry::read_error>& errors,
           const dblog_scan::parse_error& pe) {
            EXPECT_EQ(max_epoch, 0x100);
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::nondurable_entries);
            EXPECT_EQ(errors.size(), 1); 
        },
        0x0FF
    );

    // Case 2: durable_epoch == epoch (0x100 == 0x100) -> ok
    scan_one_pwal_file_inspect(
        orig_data,
        [](const boost::filesystem::path& p,
           epoch_id_type max_epoch,
           const std::vector<log_entry::read_error>& errors,
           const dblog_scan::parse_error& pe) {
            EXPECT_EQ(max_epoch, 0x100);
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::ok);
            EXPECT_EQ(errors.size(), 0);
        },
        0x100
    );

    // Case 3: durable_epoch > epoch (0x101 > 0x100) -> ok
    scan_one_pwal_file_inspect(
        orig_data,
        [](const boost::filesystem::path& p,
           epoch_id_type max_epoch,
           const std::vector<log_entry::read_error>& errors,
           const dblog_scan::parse_error& pe) {
            EXPECT_EQ(max_epoch, 0x100);
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::ok);
            EXPECT_EQ(errors.size(), 0);
        },
        0x101
    );
}

TEST_F(dblog_scan_test, scan_one_pwal_file_inspect_marker_end_followed_by_normal_entry) {
    auto orig_data = data_marker_end_followed_by_normal_entry;

    // Case 1: durable_epoch < epoch
    scan_one_pwal_file_inspect(
        orig_data,
        [](const boost::filesystem::path& p,
           epoch_id_type max_epoch,
           const std::vector<log_entry::read_error>& errors,
           const dblog_scan::parse_error& pe) {
            EXPECT_EQ(max_epoch, 0x100);
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::unexpected);
            EXPECT_EQ(errors.size(), 2);
        },
        0x0FF
    );

    // Case 2: durable_epoch == epoch
    scan_one_pwal_file_inspect(
        orig_data,
        [](const boost::filesystem::path& p,
           epoch_id_type max_epoch,
           const std::vector<log_entry::read_error>& errors,
           const dblog_scan::parse_error& pe) {
            EXPECT_EQ(max_epoch, 0x100);
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::unexpected);
            EXPECT_EQ(errors.size(), 1);
        },
        0x100
    );

    // Case 3: durable_epoch > epoch
    scan_one_pwal_file_inspect(
        orig_data,
        [](const boost::filesystem::path& p,
           epoch_id_type max_epoch,
           const std::vector<log_entry::read_error>& errors,
           const dblog_scan::parse_error& pe) {
            EXPECT_EQ(max_epoch, 0x100);
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::unexpected);
            EXPECT_EQ(errors.size(), 1);
        },
        0x101
    );
}

TEST_F(dblog_scan_test, scan_one_pwal_file_inspect_marker_end_followed_by_marker_inv_begin) {
    auto orig_data = data_marker_end_followed_by_marker_inv_begin;

    // Case 1: durable_epoch < epoch (0x0FF < 0x100) -> nondurable_entries (first half)
    scan_one_pwal_file_inspect(
        orig_data,
        [](const boost::filesystem::path&, epoch_id_type max_epoch, const auto& errors, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(max_epoch, 0x101);  // max_epoch should be updated by the latter inv_begin
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::nondurable_entries);
            EXPECT_EQ(errors.size(), 1);  // first half is nondurable
        },
        0x0FF
    );

    // Case 2: durable_epoch == epoch (0x100 == 0x100) -> ok (the latter inv is treated as invalid)
    scan_one_pwal_file_inspect(
        orig_data,
        [](const boost::filesystem::path&, epoch_id_type max_epoch, const auto& errors, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(max_epoch, 0x101);  // inv_begin epoch is only updated
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::ok);
            EXPECT_EQ(errors.size(), 0);
        },
        0x100
    );

    // Case 3: durable_epoch > epoch (0x102 > 0x100) -> ok (the latter inv is treated as invalid)
    scan_one_pwal_file_inspect(
        orig_data,
        [](const boost::filesystem::path&, epoch_id_type max_epoch, const auto& errors, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(max_epoch, 0x101);
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::ok);
            EXPECT_EQ(errors.size(), 0);
        },
        0x102
    );
}

TEST_F(dblog_scan_test, scan_one_pwal_file_inspect_marker_end_followed_by_short_entry) {
    auto orig_data = data_marker_end_followed_by_short_entry;

    hexdump(orig_data);

    // Case 1: durable_epoch < epoch -> nondurable_entries + unexpected
    scan_one_pwal_file_inspect(
        orig_data,
        [](const boost::filesystem::path&, epoch_id_type max_epoch,
           const std::vector<log_entry::read_error>& errors,
           const dblog_scan::parse_error& pe) {
            EXPECT_EQ(max_epoch, 0x100);
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::unexpected);
            EXPECT_EQ(errors.size(), 2);
        },
        0x0FF
    );

    // Case 2: durable_epoch == epoch -> unexpected
    scan_one_pwal_file_inspect(
        orig_data,
        [](const boost::filesystem::path&, epoch_id_type max_epoch,
           const std::vector<log_entry::read_error>& errors,
           const dblog_scan::parse_error& pe) {
            EXPECT_EQ(max_epoch, 0x100);
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::unexpected);
            EXPECT_EQ(errors.size(), 1);
        },
        0x100
    );

    // Case 3: durable_epoch > epoch -> unexpected
    scan_one_pwal_file_inspect(
        orig_data,
        [](const boost::filesystem::path&, epoch_id_type max_epoch,
           const std::vector<log_entry::read_error>& errors,
           const dblog_scan::parse_error& pe) {
            EXPECT_EQ(max_epoch, 0x100);
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::unexpected);
            EXPECT_EQ(errors.size(), 1);
        },
        0x101
    );
}

TEST_F(dblog_scan_test, scan_one_pwal_file_inspect_short_marker_end_only) {
    auto orig_data = data_short_marker_end_only;

    // Case 1: durable_epoch < epoch (0x0FF < 0x100) -> broken_after
    scan_one_pwal_file_inspect(
        orig_data,
        [](const boost::filesystem::path& p,
           epoch_id_type max_epoch,
           const std::vector<log_entry::read_error>& errors,
           const dblog_scan::parse_error& pe) {
            EXPECT_EQ(max_epoch, 0x100);
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::broken_after);
            EXPECT_GE(errors.size(), 1);
        },
        0x0FF
    );

    // Case 2: durable_epoch == epoch (0x100 == 0x100) -> corrupted_durable_entries
    scan_one_pwal_file_inspect(
        orig_data,
        [](const boost::filesystem::path& p,
           epoch_id_type max_epoch,
           const std::vector<log_entry::read_error>& errors,
           const dblog_scan::parse_error& pe) {
            EXPECT_EQ(max_epoch, 0x100);
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::corrupted_durable_entries);
            EXPECT_GE(errors.size(), 1);
        },
        0x100
    );

    // Case 3: durable_epoch > epoch (0x101 > 0x100) -> corrupted_durable_entries
    scan_one_pwal_file_inspect(
        orig_data,
        [](const boost::filesystem::path& p,
           epoch_id_type max_epoch,
           const std::vector<log_entry::read_error>& errors,
           const dblog_scan::parse_error& pe) {
            EXPECT_EQ(max_epoch, 0x100);
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::corrupted_durable_entries);
            EXPECT_GE(errors.size(), 1);
        },
        0x101
    );
}


TEST_F(dblog_scan_test, scan_one_pwal_file_repairm_short_marker_end_only) {
    auto orig_data = data_short_marker_end_only;

    // durable_epoch < epoch: → SHORT_marker_end is treated as nondurable and gets repaired by mark
    scan_one_pwal_file_repairm(
        orig_data,
        [&orig_data](const boost::filesystem::path& p,
                     epoch_id_type max_epoch,
                     const std::vector<log_entry::read_error>& errors,
                     const dblog_scan::parse_error& pe) {
            EXPECT_EQ(max_epoch, 0x100);
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::broken_after_marked);
            auto data = read_entire_file(p);
            // 最初の marker_begin が mark されていること
            ASSERT_EQ(orig_data.at(0), '\x02');
            EXPECT_EQ(data.at(0), '\x06');  // 0x02 → 0x06 に書き換わっている
        },
        0x0FF
    );

    // durable_epoch == epoch: → SHORT_marker_end is durable → cannot be repaired, ends with corrupted_durable_entries, not marked
    scan_one_pwal_file_repairm(
        orig_data,
        [&orig_data](const boost::filesystem::path& p,
                     epoch_id_type max_epoch,
                     const std::vector<log_entry::read_error>& errors,
                     const dblog_scan::parse_error& pe) {
            EXPECT_EQ(max_epoch, 0x100);
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::corrupted_durable_entries);
            auto data = read_entire_file(p);
            EXPECT_EQ(data, orig_data);  // no change
        },
        0x100
    );

    // durable_epoch > epoch: → SHORT_marker_end is durable → cannot be repaired, ends with corrupted_durable_entries, not marked
    scan_one_pwal_file_repairm(
        orig_data,
        [&orig_data](const boost::filesystem::path& p,
                     epoch_id_type max_epoch,
                     const std::vector<log_entry::read_error>& errors,
                     const dblog_scan::parse_error& pe) {
            EXPECT_EQ(max_epoch, 0x100);
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::corrupted_durable_entries);
            auto data = read_entire_file(p);
            EXPECT_EQ(data, orig_data);  // no change
        },
        0x101
    );
}

// === 2 === After marker_end, normal_entry
TEST_F(dblog_scan_test, scan_one_pwal_file_repairm_marker_end_followed_by_normal_entry) {
    auto orig_data = data_marker_end_followed_by_normal_entry;

    // durable_epoch < epoch → first half is nondurable, second half is unexpected → first half gets marked but unexpected takes priority
    scan_one_pwal_file_repairm(
        orig_data,
        [&orig_data](const boost::filesystem::path& p, epoch_id_type max_epoch,
                     const auto& errors, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(max_epoch, 0x100);
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::unexpected);
            auto data = read_entire_file(p);
            EXPECT_EQ(data.at(0), '\x06');  // first half is marked
            EXPECT_EQ(data.substr(1), orig_data.substr(1));
        },
        0x0FF
    );

    // durable_epoch == epoch → first half is durable, second half is unexpected → cannot be repaired
    scan_one_pwal_file_repairm(
        orig_data,
        [&orig_data](const boost::filesystem::path& p, epoch_id_type max_epoch,
                     const auto& errors, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(max_epoch, 0x100);
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::unexpected);
            EXPECT_EQ(read_entire_file(p), orig_data);  // unchanged
        },
        0x100
    );

    // durable_epoch > epoch → first half is durable, second half is unexpected → cannot be repaired
    scan_one_pwal_file_repairm(
        orig_data,
        [&orig_data](const boost::filesystem::path& p, epoch_id_type max_epoch,
                     const auto& errors, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(max_epoch, 0x100);
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::unexpected);
            EXPECT_EQ(read_entire_file(p), orig_data);  // unchanged
        },
        0x101
    );
}
// === 3 === After marker_end, marker_begin
TEST_F(dblog_scan_test, scan_one_pwal_file_repairm_marker_end_followed_by_marker_begin) {
    auto orig_data = data_marker_end_followed_by_marker_begin;

    EXPECT_EQ(orig_data.at(59), '\x02'); 

    // Case 1: durable_epoch < first half epoch → first half nondurable + second half also nondurable → both get marked
    scan_one_pwal_file_repairm(
        orig_data,
        [&orig_data](const boost::filesystem::path& p, epoch_id_type max_epoch,
                     const auto& errors, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(max_epoch, 0x101);
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::repaired);
            auto data = read_entire_file(p);
            EXPECT_EQ(data.at(0), '\x06');   // first half
            EXPECT_EQ(data.at(59), '\x06');   // second half
        },
        0x0FF
    );

    // Case 2: durable_epoch == first half epoch → first half is durable, second half is nondurable → only second half gets marked
    scan_one_pwal_file_repairm(
        orig_data,
        [&orig_data](const boost::filesystem::path& p, epoch_id_type max_epoch,
                     const auto& errors, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(max_epoch, 0x101);
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::repaired);
            auto data = read_entire_file(p);
            EXPECT_EQ(data.at(0), '\x02');   // first half remains unchanged
            EXPECT_EQ(data.at(59), '\x06');   // only second half gets marked
        },
        0x100
    );

    // Case 3: durable_epoch > second half epoch → both durable → no repair
    scan_one_pwal_file_repairm(
        orig_data,
        [&orig_data](const boost::filesystem::path& p, epoch_id_type max_epoch,
                     const auto& errors, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(max_epoch, 0x101);
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::ok);
            auto data = read_entire_file(p);
            EXPECT_EQ(data, orig_data);  // both remain unchanged
        },
        0x102
    );
}


// === 4 === After marker_end, marker_inv_begin
TEST_F(dblog_scan_test, scan_one_pwal_file_repairm_marker_end_followed_by_marker_inv_begin) {
    auto orig_data = data_marker_end_followed_by_marker_inv_begin;

    // durable_epoch < epoch → first half nondurable → first half gets marked
    scan_one_pwal_file_repairm(
        orig_data,
        [&orig_data](const boost::filesystem::path& p, epoch_id_type max_epoch,
                     const auto& errors, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(max_epoch, 0x101);
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::repaired);
            auto data = read_entire_file(p);
            EXPECT_EQ(data.at(0), '\x06');  // marker_begin → invalid
        },
        0x0FF
    );

    // durable_epoch == epoch → second half inv is treated as invalid → no mark needed
    scan_one_pwal_file_repairm(
        orig_data,
        [&orig_data](const boost::filesystem::path& p, epoch_id_type max_epoch,
                     const auto& errors, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(max_epoch, 0x101);
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::ok);
            EXPECT_EQ(read_entire_file(p), orig_data);
        },
        0x100
    );

    // durable_epoch > second half inv epoch → both treated as durable → no mark needed
    scan_one_pwal_file_repairm(
        orig_data,
        [&orig_data](const boost::filesystem::path& p, epoch_id_type max_epoch,
                    const auto& errors, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(max_epoch, 0x101);
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::ok);
            EXPECT_EQ(read_entire_file(p), orig_data);
        },
        0x102  // second half inv epoch is 0x101, so greater than that
    );

}

// === 5 === After marker_end, SHORT_entry
TEST_F(dblog_scan_test, scan_one_pwal_file_repairm_marker_end_followed_by_short_entry) {
    auto orig_data = data_marker_end_followed_by_short_entry;

    // Case 1: durable_epoch < epoch → first half nondurable + SHORT → gets marked while result is broken_after_marked
    scan_one_pwal_file_repairm(
        orig_data,
        [&orig_data](const boost::filesystem::path& p, epoch_id_type max_epoch,
                     const auto& errors, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(max_epoch, 0x100);
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::unexpected);
            auto data = read_entire_file(p);
            EXPECT_EQ(data.at(0), '\x06');  // first half marker_begin is marked
        },
        0x0FF
    );

    // Case 2: durable_epoch == epoch → SHORT is unexpected → cannot be repaired
    scan_one_pwal_file_repairm(
        orig_data,
        [&orig_data](const boost::filesystem::path& p, epoch_id_type max_epoch,
                     const auto& errors, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(max_epoch, 0x100);
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::unexpected);
            EXPECT_EQ(read_entire_file(p), orig_data);
        },
        0x100
    );

    // Case 3: durable_epoch > epoch → SHORT is unexpected → cannot be repaired
    scan_one_pwal_file_repairm(
        orig_data,
        [&orig_data](const boost::filesystem::path& p, epoch_id_type max_epoch,
                     const auto& errors, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(max_epoch, 0x100);
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::unexpected);
            EXPECT_EQ(read_entire_file(p), orig_data);
        },
        0x101
    );
}


// === 6 === Only SHORT_marker_end
TEST_F(dblog_scan_test, scan_one_pwal_file_repairm_short_marker_end_case) {
    auto orig_data = data_short_marker_end_only;

    // Case 1: durable_epoch < epoch → nondurable → gets marked
    scan_one_pwal_file_repairm(
        orig_data,
        [&orig_data](const boost::filesystem::path& p, epoch_id_type max_epoch,
                     const auto& errors, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(max_epoch, 0x100);
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::broken_after_marked);
            auto data = read_entire_file(p);
            EXPECT_EQ(data.at(0), '\x06');  // marker_begin is invalidated
        },
        0x0FF
    );

    // Case 2: durable_epoch == epoch → durable → corrupted_durable_entries
    scan_one_pwal_file_repairm(
        orig_data,
        [&orig_data](const boost::filesystem::path& p, epoch_id_type max_epoch,
                     const auto& errors, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(max_epoch, 0x100);
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::corrupted_durable_entries);
            EXPECT_EQ(read_entire_file(p), orig_data);  // no change
        },
        0x100
    );

    // Case 3: durable_epoch > epoch → durable → corrupted_durable_entries
    scan_one_pwal_file_repairm(
        orig_data,
        [&orig_data](const boost::filesystem::path& p, epoch_id_type max_epoch,
                     const auto& errors, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(max_epoch, 0x100);
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::corrupted_durable_entries);
            EXPECT_EQ(read_entire_file(p), orig_data);  // no change
        },
        0x101
    );
}

TEST_F(dblog_scan_test, scan_one_pwal_file_repairc_short_marker_end_only) {
    auto orig_data = data_short_marker_end_only;

    // Case 1: durable_epoch < epoch → cut
    scan_one_pwal_file_repairc(
        orig_data,
        [](const boost::filesystem::path& p, epoch_id_type max_epoch,
           const auto& errors, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::repaired);
            EXPECT_EQ(boost::filesystem::file_size(p), 0);  // Expect full cut
        },
        0x0FF
        );

        // Case 2: durable_epoch == epoch → corrupted_durable_entries → cannot cut
        scan_one_pwal_file_repairc(
        orig_data,
        [&orig_data](const auto& p, epoch_id_type, const auto&, const auto& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::corrupted_durable_entries);
            EXPECT_EQ(read_entire_file(p), orig_data);
        },
        0x100
        );

        // Case 3: durable_epoch > epoch → corrupted_durable_entries → cannot cut
        scan_one_pwal_file_repairc(
        orig_data,
        [&orig_data](const auto& p, epoch_id_type, const auto&, const auto& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::corrupted_durable_entries);
            EXPECT_EQ(read_entire_file(p), orig_data);
        },
        0x101
    );
}

TEST_F(dblog_scan_test, scan_one_pwal_file_repairc_marker_end_followed_by_normal_entry) {
    auto orig_data = data_marker_end_followed_by_normal_entry;

    // Case 1: durable_epoch < epoch → first half nondurable, second half unexpected → first half mark, unexpected takes priority
    scan_one_pwal_file_repairc(
        orig_data,
        [&orig_data](const boost::filesystem::path& p,
                     epoch_id_type max_epoch,
                     const std::vector<log_entry::read_error>& errors,
                     const dblog_scan::parse_error& pe) {
            EXPECT_EQ(max_epoch, 0x100);
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::unexpected);
            auto data = read_entire_file(p);
            EXPECT_EQ(data.at(0), '\x06');  // marker_begin → invalid
            EXPECT_EQ(data.substr(1), orig_data.substr(1));  // second half is unchanged
        },
        0x0FF
    );

    // Case 2: durable_epoch == epoch → first half durable, second half unexpected → cannot be repaired
    scan_one_pwal_file_repairc(
        orig_data,
        [&orig_data](const boost::filesystem::path& p,
                     epoch_id_type max_epoch,
                     const std::vector<log_entry::read_error>& errors,
                     const dblog_scan::parse_error& pe) {
            EXPECT_EQ(max_epoch, 0x100);
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::unexpected);
            EXPECT_EQ(read_entire_file(p), orig_data);  // unchanged
        },
        0x100
    );

    // Case 3: durable_epoch > epoch → first half durable, second half unexpected → cannot be repaired
    scan_one_pwal_file_repairc(
        orig_data,
        [&orig_data](const boost::filesystem::path& p,
                     epoch_id_type max_epoch,
                     const std::vector<log_entry::read_error>& errors,
                     const dblog_scan::parse_error& pe) {
            EXPECT_EQ(max_epoch, 0x100);
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::unexpected);
            EXPECT_EQ(read_entire_file(p), orig_data);  // unchanged
        },
        0x101
    );
}

TEST_F(dblog_scan_test, scan_one_pwal_file_repairc_marker_end_followed_by_marker_begin) {
    auto orig_data = data_marker_end_followed_by_marker_begin;

    // Position verification
    EXPECT_EQ(orig_data.at(59), '\x02');

    // Case 1: durable_epoch < first half epoch → both first and second half are nondurable → both get marked
    scan_one_pwal_file_repairc(
        orig_data,
        [&orig_data](const auto& p, epoch_id_type, const auto&, const auto& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::repaired);
            auto data = read_entire_file(p);
            EXPECT_EQ(data.at(0), '\x06');   // first half mark
            EXPECT_EQ(data.at(59), '\x06');  // second half mark
        },
        0x0FF
    );

    // Case 2: durable_epoch == first half epoch → first half is durable, second half is nondurable → only second half gets marked
    scan_one_pwal_file_repairc(
        orig_data,
        [&orig_data](const auto& p, epoch_id_type, const auto&, const auto& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::repaired);
            auto data = read_entire_file(p);
            EXPECT_EQ(data.at(0), '\x02');   // first half remains unchanged
            EXPECT_EQ(data.at(59), '\x06');  // only second half gets marked
        },
        0x100
    );

    // Case 3: durable_epoch > 後半の epoch → 両方 durable → 修復なし
    scan_one_pwal_file_repairc(
        orig_data,
        [&orig_data](const auto& p, epoch_id_type, const auto&, const auto& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::ok);
            auto data = read_entire_file(p);
            EXPECT_EQ(data, orig_data);  // no change
        },
        0x102
    );
}

TEST_F(dblog_scan_test, scan_one_pwal_file_repairc_marker_end_followed_by_marker_inv_begin) {
    auto orig_data = data_marker_end_followed_by_marker_inv_begin;

    // Case 1: durable_epoch < epoch → first half is nondurable → mark
    scan_one_pwal_file_repairc(
        orig_data,
        [&orig_data](const auto& p, epoch_id_type max_epoch, const auto&, const auto& pe) {
            EXPECT_EQ(max_epoch, 0x101);
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::repaired);
            auto data = read_entire_file(p);
            EXPECT_EQ(data.at(0), '\x06');  // mark
        },
        0x0FF
    );

    // Case 2: durable_epoch == epoch → the latter inv is not treated as valid → no mark
    scan_one_pwal_file_repairc(
        orig_data,
        [&orig_data](const auto& p, epoch_id_type max_epoch, const auto&, const auto& pe) {
            EXPECT_EQ(max_epoch, 0x101);
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::ok);
            EXPECT_EQ(read_entire_file(p), orig_data);
        },
        0x100
    );

    // Case 3: durable_epoch > latter epoch → both are durable → no mark
    scan_one_pwal_file_repairc(
        orig_data,
        [&orig_data](const auto& p, epoch_id_type max_epoch, const auto&, const auto& pe) {
            EXPECT_EQ(max_epoch, 0x101);
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::ok);
            EXPECT_EQ(read_entire_file(p), orig_data);
        },
        0x102
    );
}

TEST_F(dblog_scan_test, scan_one_pwal_file_repairc_marker_end_followed_by_short_entry) {
    auto orig_data = data_marker_end_followed_by_short_entry;

    // Case 1: durable_epoch < epoch → first half nondurable so gets marked, second half SHORT cannot be cut → remains unexpected
    scan_one_pwal_file_repairc(
        orig_data,
        [&orig_data](const auto& p, epoch_id_type max_epoch, const auto&, const auto& pe) {
            EXPECT_EQ(max_epoch, 0x100);
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::unexpected);
            auto data = read_entire_file(p);
            EXPECT_EQ(data.at(0), '\x06');  // marked
        },
        0x0FF
    );

    // Case 2: durable_epoch == epoch → SHORT は unexpected → 修復不可
    scan_one_pwal_file_repairc(
        orig_data,
        [&orig_data](const auto& p, epoch_id_type max_epoch, const auto&, const auto& pe) {
            EXPECT_EQ(max_epoch, 0x100);
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::unexpected);
            EXPECT_EQ(read_entire_file(p), orig_data);
        },
        0x100
    );

    // Case 3: durable_epoch > epoch → same unexpected
    scan_one_pwal_file_repairc(
        orig_data,
        [&orig_data](const auto& p, epoch_id_type max_epoch, const auto&, const auto& pe) {
            EXPECT_EQ(max_epoch, 0x100);
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::unexpected);
            EXPECT_EQ(read_entire_file(p), orig_data);
        },
        0x101
    );
}


// === 0F-1: The entire file is 0-filled ===

TEST_F(dblog_scan_test, scan_one_pwal_file_inspect_all_zerofill) {
    auto orig_data = data_all_zerofill;

    // durable_epoch = 0x0FF
    scan_one_pwal_file_inspect(
        orig_data,
        [](const boost::filesystem::path&, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::unexpected);
        },
        0x0FF
    );

    // durable_epoch = 0x100
    scan_one_pwal_file_inspect(
        orig_data,
        [](const boost::filesystem::path&, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::unexpected);
        },
        0x100
    );

    // durable_epoch = 0x101
    scan_one_pwal_file_inspect(
        orig_data,
        [](const boost::filesystem::path&, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::unexpected);
        },
        0x101
    );
}

// === 0F-2: 0fill starts in the middle of marker_begin ===
TEST_F(dblog_scan_test, scan_one_pwal_file_inspect_marker_begin_partial_zerofill) {
    auto orig_data = data_marker_begin_partial_zerofill;

    scan_one_pwal_file_inspect(
        orig_data,
        [](const auto&, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::broken_after);
        },
        0x0FF
    );

    scan_one_pwal_file_inspect(
        orig_data,
        [](const auto&, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::broken_after);
        },
        0x100
    );

    scan_one_pwal_file_inspect(
        orig_data,
        [](const auto&, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::broken_after);
        },
        0x101
    );
}

// === 0F-3: marker_begin の直後から 0fill ===
TEST_F(dblog_scan_test, scan_one_pwal_file_inspect_marker_begin_followed_by_zerofill) {
    auto orig_data = data_marker_begin_followed_by_zerofill;

    scan_one_pwal_file_inspect(
        orig_data,
        [](const auto&, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::broken_after);
        },
        0x0FF
    );

    scan_one_pwal_file_inspect(
        orig_data,
        [](const auto&, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::corrupted_durable_entries);
        },
        0x100
    );

    scan_one_pwal_file_inspect(
        orig_data,
        [](const auto&, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::corrupted_durable_entries);
        },
        0x101
    );
}

// === 0F-4: marker_begin + normal_entry の途中から 0fill ===
TEST_F(dblog_scan_test, scan_one_pwal_file_inspect_marker_begin_normal_entry_partial_zerofill) {
    auto orig_data = data_marker_begin_normal_entry_partial_zerofill;

    scan_one_pwal_file_inspect(
        orig_data,
        [](const auto&, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::broken_after);
        },
        0x0FF
    );

    scan_one_pwal_file_inspect(
        orig_data,
        [](const auto&, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::corrupted_durable_entries);
        },
        0x100
    );

    scan_one_pwal_file_inspect(
        orig_data,
        [](const auto&, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::corrupted_durable_entries);
        },
        0x101
    );
}

// === 0F-5: marker_begin + normal_entry の後から 0fill ===
TEST_F(dblog_scan_test, scan_one_pwal_file_inspect_marker_begin_normal_entry_followed_by_zerofill) {
    auto orig_data = data_marker_begin_normal_entry_followed_by_zerofill;

    scan_one_pwal_file_inspect(
        orig_data,
        [](const auto&, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::broken_after);
        },
        0x0FF
    );

    scan_one_pwal_file_inspect(
        orig_data,
        [](const auto&, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::corrupted_durable_entries);
        },
        0x100
    );

    scan_one_pwal_file_inspect(
        orig_data,
        [](const auto&, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::corrupted_durable_entries);
        },
        0x101
    );
}

// === 0F-6: marker_end の途中から 0fill ===
TEST_F(dblog_scan_test, scan_one_pwal_file_inspect_marker_end_partial_zerofill) {
    auto orig_data = data_marker_end_partial_zerofill;

    scan_one_pwal_file_inspect(
        orig_data,
        [](const auto&, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::broken_after);
        },
        0x0FF
    );

    scan_one_pwal_file_inspect(
        orig_data,
        [](const auto&, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::corrupted_durable_entries);
        },
        0x100
    );

    scan_one_pwal_file_inspect(
        orig_data,
        [](const auto&, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::corrupted_durable_entries);
        },
        0x101
    );
}

// === 0F-1 + valid_snippet ===
TEST_F(dblog_scan_test, scan_one_pwal_file_inspect_valid_snippet_followed_by_all_zerofill) {
    auto orig_data = concat_binary(valid_snippet, data_all_zerofill);

    hexdump(orig_data);
    scan_one_pwal_file_inspect(
        orig_data,
        [](const auto&, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::unexpected);
        },
        0x0FF
    );

    scan_one_pwal_file_inspect(
        orig_data,
        [](const auto&, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::unexpected);
        },
        0x100
    );

    scan_one_pwal_file_inspect(
        orig_data,
        [](const auto&, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::unexpected);
        },
        0x101
    );
}

// === 0F-2 + valid_snippet ===
TEST_F(dblog_scan_test, scan_one_pwal_file_inspect_valid_snippet_followed_by_marker_begin_partial_zerofill) {
    auto orig_data = concat_binary(valid_snippet, data_marker_begin_partial_zerofill);

    scan_one_pwal_file_inspect(
        orig_data,
        [](const auto&, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::broken_after);
        },
        0x0FF
    );

    scan_one_pwal_file_inspect(
        orig_data,
        [](const auto&, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::broken_after);
        },
        0x100
    );

    scan_one_pwal_file_inspect(
        orig_data,
        [](const auto&, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::broken_after);
        },
        0x101
    );
}

// === 0F-3 + valid_snippet ===
TEST_F(dblog_scan_test, scan_one_pwal_file_inspect_valid_snippet_followed_by_marker_begin_followed_by_zerofill) {
    auto orig_data = concat_binary(valid_snippet, data_marker_begin_followed_by_zerofill);

    scan_one_pwal_file_inspect(
        orig_data,
        [](const auto&, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::broken_after);
        },
        0x0FF
    );

    scan_one_pwal_file_inspect(
        orig_data,
        [](const auto&, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::corrupted_durable_entries);
        },
        0x100
    );

    scan_one_pwal_file_inspect(
        orig_data,
        [](const auto&, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::corrupted_durable_entries);
        },
        0x101
    );
}

// === 0F-4 + valid_snippet ===
TEST_F(dblog_scan_test, scan_one_pwal_file_inspect_valid_snippet_followed_by_marker_begin_normal_entry_partial_zerofill) {
    auto orig_data = concat_binary(valid_snippet, data_marker_begin_normal_entry_partial_zerofill);

    scan_one_pwal_file_inspect(
        orig_data,
        [](const auto&, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::broken_after);
        },
        0x0FF
    );

    scan_one_pwal_file_inspect(
        orig_data,
        [](const auto&, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::corrupted_durable_entries);
        },
        0x100
    );

    scan_one_pwal_file_inspect(
        orig_data,
        [](const auto&, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::corrupted_durable_entries);
        },
        0x101
    );
}

// === 0F-5 + valid_snippet ===
TEST_F(dblog_scan_test, scan_one_pwal_file_inspect_valid_snippet_followed_by_marker_begin_normal_entry_followed_by_zerofill) {
    auto orig_data = concat_binary(valid_snippet, data_marker_begin_normal_entry_followed_by_zerofill);

    scan_one_pwal_file_inspect(
        orig_data,
        [](const auto&, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::broken_after);
        },
        0x0FF
    );

    scan_one_pwal_file_inspect(
        orig_data,
        [](const auto&, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::corrupted_durable_entries);
        },
        0x100
    );

    scan_one_pwal_file_inspect(
        orig_data,
        [](const auto&, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::corrupted_durable_entries);
        },
        0x101
    );
}

// === 0F-6 + valid_snippet ===
TEST_F(dblog_scan_test, scan_one_pwal_file_inspect_valid_snippet_followed_by_marker_end_partial_zerofill) {
    auto orig_data = concat_binary(valid_snippet, data_marker_end_partial_zerofill);

    scan_one_pwal_file_inspect(
        orig_data,
        [](const auto&, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::broken_after);
        },
        0x0FF
    );

    scan_one_pwal_file_inspect(
        orig_data,
        [](const auto&, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::corrupted_durable_entries);
        },
        0x100
    );

    scan_one_pwal_file_inspect(
        orig_data,
        [](const auto&, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::corrupted_durable_entries);
        },
        0x101
    );
}

// === 0F-1 ===
TEST_F(dblog_scan_test, scan_one_pwal_file_repairm_all_zerofill) {
    auto orig_data = data_all_zerofill;
    
    // durable_epoch = 0x0FF
    scan_one_pwal_file_repairm(
        orig_data,
        [](const boost::filesystem::path&, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::broken_after_marked);
        },
        0x0FF
    );

    // durable_epoch = 0x100
    scan_one_pwal_file_repairm(
        orig_data,
        [](const boost::filesystem::path&, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::broken_after_marked);
        },
        0x100
    );

    // durable_epoch = 0x101
    scan_one_pwal_file_repairm(
        orig_data,
        [](const boost::filesystem::path&, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::broken_after_marked);
        },
        0x101
    );
}

// === 0F-2 ===
TEST_F(dblog_scan_test, scan_one_pwal_file_repairm_marker_begin_partial_zerofill) {
    auto orig_data = data_marker_begin_partial_zerofill;

    // durable_epoch = 0x0FF
    scan_one_pwal_file_repairm(
        orig_data,
        [](const auto&, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::broken_after_marked);
        },
        0x0FF
    );

    // durable_epoch = 0x100
    scan_one_pwal_file_repairm(
        orig_data,
        [](const auto&, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::broken_after_marked);
        },
        0x100
    );

    // durable_epoch = 0x101
    scan_one_pwal_file_repairm(
        orig_data,
        [](const auto&, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::broken_after_marked);
        },
        0x101
    );
}

// === 0F-3 ===
TEST_F(dblog_scan_test, scan_one_pwal_file_repairm_marker_begin_followed_by_zerofill) {
    auto orig_data = data_marker_begin_followed_by_zerofill;

    scan_one_pwal_file_repairm(orig_data,
                               [](const auto&, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
                                   EXPECT_EQ(pe.value(), dblog_scan::parse_error::broken_after_marked);
                               },
                               0x0FF);

    // durable_epoch = 0x100
    scan_one_pwal_file_repairm(
        orig_data,
        [](const auto&, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::corrupted_durable_entries);
        },
        0x100
    );

    // durable_epoch = 0x101
    scan_one_pwal_file_repairm(
        orig_data,
        [](const auto&, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::corrupted_durable_entries);
        },
        0x101
    );
}

// === 0F-4 ===
TEST_F(dblog_scan_test, scan_one_pwal_file_repairm_marker_begin_normal_entry_partial_zerofill) {
    auto orig_data = data_marker_begin_normal_entry_partial_zerofill;

    scan_one_pwal_file_repairm(orig_data,
                               [](const auto&, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
                                   EXPECT_EQ(pe.value(), dblog_scan::parse_error::broken_after_marked);
                               },
                               0x0FF);

    // durable_epoch = 0x100
    scan_one_pwal_file_repairm(
        orig_data,
        [](const auto&, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::corrupted_durable_entries);
        },
        0x100
    );

    // durable_epoch = 0x101
    scan_one_pwal_file_repairm(
        orig_data,
        [](const auto&, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::corrupted_durable_entries);
        },
        0x101
    );
}

// === 0F-5 ===
TEST_F(dblog_scan_test, scan_one_pwal_file_repairm_marker_begin_normal_entry_followed_by_zerofill) {
    auto orig_data = data_marker_begin_normal_entry_followed_by_zerofill;

    scan_one_pwal_file_repairm(orig_data,
                               [](const auto&, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
                                   EXPECT_EQ(pe.value(), dblog_scan::parse_error::broken_after_marked);
                               },
                               0x0FF);

    // durable_epoch = 0x100
    scan_one_pwal_file_repairm(
        orig_data,
        [](const auto&, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::corrupted_durable_entries);
        },
        0x100
    );

    // durable_epoch = 0x101
    scan_one_pwal_file_repairm(
        orig_data,
        [](const auto&, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::corrupted_durable_entries);
        },
        0x101
    );
}

// === 0F-6 ===
TEST_F(dblog_scan_test, scan_one_pwal_file_repairm_marker_end_partial_zerofill) {
    auto orig_data = data_marker_end_partial_zerofill;

    scan_one_pwal_file_repairm(orig_data,
                               [](const auto&, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
                                   EXPECT_EQ(pe.value(), dblog_scan::parse_error::broken_after_marked);
                               },
                               0x0FF);

    // durable_epoch = 0x100
    scan_one_pwal_file_repairm(
        orig_data,
        [](const auto&, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::corrupted_durable_entries);
        },
        0x100
    );

    // durable_epoch = 0x101
    scan_one_pwal_file_repairm(
        orig_data,
        [](const auto&, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::corrupted_durable_entries);
        },
        0x101
    );
}

// === valid_snippet + 0F-1 ===
TEST_F(dblog_scan_test, scan_one_pwal_file_repairm_valid_snippet_followed_by_all_zerofill) {
    auto orig_data = concat_binary(valid_snippet, data_all_zerofill);

    // durable_epoch = 0x0FF
    scan_one_pwal_file_repairm(
        orig_data,
        [](const auto&, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::broken_after_marked);
        },
        0x0FF
    );

    // durable_epoch = 0x100
    scan_one_pwal_file_repairm(
        orig_data,
        [](const auto&, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::broken_after_marked);
        },
        0x100
    );

    // durable_epoch = 0x101
    scan_one_pwal_file_repairm(
        orig_data,
        [](const auto&, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::broken_after_marked);
        },
        0x101
    );
}

// === valid_snippet + 0F-2 ===
TEST_F(dblog_scan_test, scan_one_pwal_file_repairm_valid_snippet_followed_by_marker_begin_partial_zerofill) {
    auto orig_data = concat_binary(valid_snippet, data_marker_begin_partial_zerofill);

    // durable_epoch = 0x0FF
    scan_one_pwal_file_repairm(
        orig_data,
        [](const auto&, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::broken_after_marked);
        },
        0x0FF
    );

    // durable_epoch = 0x100
    scan_one_pwal_file_repairm(
        orig_data,
        [](const auto&, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::broken_after_marked);
        },
        0x100
    );

    // durable_epoch = 0x101
    scan_one_pwal_file_repairm(
        orig_data,
        [](const auto&, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::broken_after_marked);
        },
        0x101
    );
}

// === valid_snippet + 0F-3 ===
TEST_F(dblog_scan_test, scan_one_pwal_file_repairm_valid_snippet_followed_by_marker_begin_followed_by_zerofill) {
    auto orig_data = concat_binary(valid_snippet, data_marker_begin_followed_by_zerofill);

    scan_one_pwal_file_repairm(orig_data,
                               [](const auto&, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
                                   EXPECT_EQ(pe.value(), dblog_scan::parse_error::broken_after_marked);
                               },
                               0x0FF);

    // durable_epoch = 0x100
    scan_one_pwal_file_repairm(
        orig_data,
        [](const auto&, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::corrupted_durable_entries);
        },
        0x100
    );

    // durable_epoch = 0x101
    scan_one_pwal_file_repairm(
        orig_data,
        [](const auto&, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::corrupted_durable_entries);
        },
        0x101
    );
}

// === valid_snippet + 0F-4 ===
TEST_F(dblog_scan_test, scan_one_pwal_file_repairm_valid_snippet_followed_by_marker_begin_normal_entry_partial_zerofill) {
    auto orig_data = concat_binary(valid_snippet, data_marker_begin_normal_entry_partial_zerofill);

    scan_one_pwal_file_repairm(orig_data,
                               [](const auto&, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
                                   EXPECT_EQ(pe.value(), dblog_scan::parse_error::broken_after_marked);
                               },
                               0x0FF);

    // durable_epoch = 0x100
    scan_one_pwal_file_repairm(
        orig_data,
        [](const auto&, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::corrupted_durable_entries);
        },
        0x100
    );

    // durable_epoch = 0x101
    scan_one_pwal_file_repairm(
        orig_data,
        [](const auto&, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::corrupted_durable_entries);
        },
        0x101
    );
}

// === valid_snippet + 0F-5 ===
TEST_F(dblog_scan_test, scan_one_pwal_file_repairm_valid_snippet_followed_by_marker_begin_normal_entry_followed_by_zerofill) {
    auto orig_data = concat_binary(valid_snippet, data_marker_begin_normal_entry_followed_by_zerofill);

    scan_one_pwal_file_repairm(orig_data,
                               [](const auto&, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
                                   EXPECT_EQ(pe.value(), dblog_scan::parse_error::broken_after_marked);
                               },
                               0x0FF);

    // durable_epoch = 0x100
    scan_one_pwal_file_repairm(
        orig_data,
        [](const auto&, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::corrupted_durable_entries);
        },
        0x100
    );

    // durable_epoch = 0x101
    scan_one_pwal_file_repairm(
        orig_data,
        [](const auto&, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::corrupted_durable_entries);
        },
        0x101
    );
}

// === valid_snippet + 0F-6 ===
TEST_F(dblog_scan_test, scan_one_pwal_file_repairm_valid_snippet_followed_by_marker_end_partial_zerofill) {
    auto orig_data = concat_binary(valid_snippet, data_marker_end_partial_zerofill);

    scan_one_pwal_file_repairm(orig_data,
                               [](const auto&, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
                                   EXPECT_EQ(pe.value(), dblog_scan::parse_error::broken_after_marked);
                               },
                               0x0FF);

    // durable_epoch = 0x100
    scan_one_pwal_file_repairm(
        orig_data,
        [](const auto&, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::corrupted_durable_entries);
        },
        0x100
    );

    // durable_epoch = 0x101
    scan_one_pwal_file_repairm(
        orig_data,
        [](const auto&, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::corrupted_durable_entries);
        },
        0x101
    );
}

// === 0F-1 ===
TEST_F(dblog_scan_test, scan_one_pwal_file_repairc_all_zerofill) {
    auto orig_data = data_all_zerofill;

    scan_one_pwal_file_repairc(
        orig_data,
        [&](const boost::filesystem::path& p, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::repaired);
            EXPECT_LT(boost::filesystem::file_size(p), orig_data.size());
        },
        0x0FF
    );

    scan_one_pwal_file_repairc(
        orig_data,
        [&](const boost::filesystem::path& p, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::repaired);
            EXPECT_LT(boost::filesystem::file_size(p), orig_data.size());
        },
        0x100
    );

    scan_one_pwal_file_repairc(
        orig_data,
        [&](const boost::filesystem::path& p, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::repaired);
            EXPECT_LT(boost::filesystem::file_size(p), orig_data.size());
        },
        0x101
    );
}

// === 0F-2 ===
TEST_F(dblog_scan_test, scan_one_pwal_file_repairc_marker_begin_partial_zerofill) {
    auto orig_data = data_marker_begin_partial_zerofill;

    scan_one_pwal_file_repairc(
        orig_data,
        [&](const boost::filesystem::path& p, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::repaired);
            EXPECT_LT(boost::filesystem::file_size(p), orig_data.size());
        },
        0x0FF
    );

    scan_one_pwal_file_repairc(
        orig_data,
        [&](const boost::filesystem::path& p, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::repaired);
            EXPECT_LT(boost::filesystem::file_size(p), orig_data.size());
        },
        0x100
    );

    scan_one_pwal_file_repairc(
        orig_data,
        [&](const boost::filesystem::path& p, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::repaired);
            EXPECT_LT(boost::filesystem::file_size(p), orig_data.size());
        },
        0x101
    );
}

// === 0F-3 ===
TEST_F(dblog_scan_test, scan_one_pwal_file_repairc_marker_begin_followed_by_zerofill) {
    auto orig_data = data_marker_begin_followed_by_zerofill;

    scan_one_pwal_file_repairc(
        orig_data,
        [&](const boost::filesystem::path& p, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::repaired);
            EXPECT_LT(boost::filesystem::file_size(p), orig_data.size());
        },
        0x0FF
    );

    scan_one_pwal_file_repairc(
        orig_data,
        [&](const boost::filesystem::path& p, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::corrupted_durable_entries);
            EXPECT_EQ(boost::filesystem::file_size(p), orig_data.size());
        },
        0x100
    );

    scan_one_pwal_file_repairc(
        orig_data,
        [&](const boost::filesystem::path& p, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::corrupted_durable_entries);
            EXPECT_EQ(boost::filesystem::file_size(p), orig_data.size());
        },
        0x101
    );
}

// === 0F-4 ===
TEST_F(dblog_scan_test, scan_one_pwal_file_repairc_marker_begin_normal_entry_partial_zerofill) {
    auto orig_data = data_marker_begin_normal_entry_partial_zerofill;

    scan_one_pwal_file_repairc(
        orig_data,
        [&](const boost::filesystem::path& p, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::repaired);
            EXPECT_LT(boost::filesystem::file_size(p), orig_data.size());
        },
        0x0FF
    );

    scan_one_pwal_file_repairc(
        orig_data,
        [&](const boost::filesystem::path& p, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::corrupted_durable_entries);
            EXPECT_EQ(boost::filesystem::file_size(p), orig_data.size());
        },
        0x100
    );

    scan_one_pwal_file_repairc(
        orig_data,
        [&](const boost::filesystem::path& p, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::corrupted_durable_entries);
            EXPECT_EQ(boost::filesystem::file_size(p), orig_data.size());
        },
        0x101
    );
}

// === 0F-5 ===
TEST_F(dblog_scan_test, scan_one_pwal_file_repairc_marker_begin_normal_entry_followed_by_zerofill) {
    auto orig_data = data_marker_begin_normal_entry_followed_by_zerofill;

    scan_one_pwal_file_repairc(
        orig_data,
        [&](const boost::filesystem::path& p, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::repaired);
            EXPECT_LT(boost::filesystem::file_size(p), orig_data.size());
        },
        0x0FF
    );

    scan_one_pwal_file_repairc(
        orig_data,
        [&](const boost::filesystem::path& p, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::corrupted_durable_entries);
            EXPECT_EQ(boost::filesystem::file_size(p), orig_data.size());
        },
        0x100
    );

    scan_one_pwal_file_repairc(
        orig_data,
        [&](const boost::filesystem::path& p, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::corrupted_durable_entries);
            EXPECT_EQ(boost::filesystem::file_size(p), orig_data.size());
        },
        0x101
    );
}

// === 0F-6 ===
TEST_F(dblog_scan_test, scan_one_pwal_file_repairc_marker_end_partial_zerofill) {
    auto orig_data = data_marker_end_partial_zerofill;

    scan_one_pwal_file_repairc(
        orig_data,
        [&](const boost::filesystem::path& p, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::repaired);
            EXPECT_LT(boost::filesystem::file_size(p), orig_data.size());
        },
        0x0FF
    );

    scan_one_pwal_file_repairc(
        orig_data,
        [&](const boost::filesystem::path& p, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::corrupted_durable_entries);
            EXPECT_EQ(boost::filesystem::file_size(p), orig_data.size());
        },
        0x100
    );

    scan_one_pwal_file_repairc(
        orig_data,
        [&](const boost::filesystem::path& p, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::corrupted_durable_entries);
            EXPECT_EQ(boost::filesystem::file_size(p), orig_data.size());
        },
        0x101
    );
}

// === valid_snippet + 0F-1 ===
TEST_F(dblog_scan_test, scan_one_pwal_file_repairc_valid_snippet_followed_by_all_zerofill) {
    auto orig_data = concat_binary(valid_snippet, data_all_zerofill);

    scan_one_pwal_file_repairc(
        orig_data,
        [&](const boost::filesystem::path& p, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::repaired);
            EXPECT_LT(boost::filesystem::file_size(p), orig_data.size());
        },
        0x0FF
    );

    scan_one_pwal_file_repairc(
        orig_data,
        [&](const boost::filesystem::path& p, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::repaired);
            EXPECT_LT(boost::filesystem::file_size(p), orig_data.size());
        },
        0x100
    );

    scan_one_pwal_file_repairc(
        orig_data,
        [&](const boost::filesystem::path& p, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::repaired);
            EXPECT_LT(boost::filesystem::file_size(p), orig_data.size());
        },
        0x101
    );
}

// === valid_snippet + 0F-2 ===
TEST_F(dblog_scan_test, scan_one_pwal_file_repairc_valid_snippet_followed_by_marker_begin_partial_zerofill) {
    auto orig_data = concat_binary(valid_snippet, data_marker_begin_partial_zerofill);

    scan_one_pwal_file_repairc(
        orig_data,
        [&](const boost::filesystem::path& p, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::repaired);
            EXPECT_LT(boost::filesystem::file_size(p), orig_data.size());
        },
        0x0FF
    );

    scan_one_pwal_file_repairc(
        orig_data,
        [&](const boost::filesystem::path& p, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::repaired);
            EXPECT_LT(boost::filesystem::file_size(p), orig_data.size());
        },
        0x100
    );

    scan_one_pwal_file_repairc(
        orig_data,
        [&](const boost::filesystem::path& p, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::repaired);
            EXPECT_LT(boost::filesystem::file_size(p), orig_data.size());
        },
        0x101
    );
}

// === valid_snippet + 0F-3 ===
TEST_F(dblog_scan_test, scan_one_pwal_file_repairc_valid_snippet_followed_by_marker_begin_followed_by_zerofill) {
    auto orig_data = concat_binary(valid_snippet, data_marker_begin_followed_by_zerofill);

    scan_one_pwal_file_repairc(
        orig_data,
        [&](const boost::filesystem::path& p, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::repaired);
            EXPECT_LT(boost::filesystem::file_size(p), orig_data.size());
        },
        0x0FF
    );

    scan_one_pwal_file_repairc(
        orig_data,
        [&](const boost::filesystem::path& p, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::corrupted_durable_entries);
            EXPECT_EQ(boost::filesystem::file_size(p), orig_data.size());
        },
        0x100
    );

    scan_one_pwal_file_repairc(
        orig_data,
        [&](const boost::filesystem::path& p, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::corrupted_durable_entries);
            EXPECT_EQ(boost::filesystem::file_size(p), orig_data.size());
        },
        0x101
    );
}

// === valid_snippet + 0F-4 ===
TEST_F(dblog_scan_test, scan_one_pwal_file_repairc_valid_snippet_followed_by_marker_begin_normal_entry_partial_zerofill) {
    auto orig_data = concat_binary(valid_snippet, data_marker_begin_normal_entry_partial_zerofill);

    scan_one_pwal_file_repairc(
        orig_data,
        [&](const boost::filesystem::path& p, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::repaired);
            EXPECT_LT(boost::filesystem::file_size(p), orig_data.size());
        },
        0x0FF
    );

    scan_one_pwal_file_repairc(
        orig_data,
        [&](const boost::filesystem::path& p, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::corrupted_durable_entries);
            EXPECT_EQ(boost::filesystem::file_size(p), orig_data.size());
        },
        0x100
    );

    scan_one_pwal_file_repairc(
        orig_data,
        [&](const boost::filesystem::path& p, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::corrupted_durable_entries);
            EXPECT_EQ(boost::filesystem::file_size(p), orig_data.size());
        },
        0x101
    );
}

// === valid_snippet + 0F-5 ===
TEST_F(dblog_scan_test, scan_one_pwal_file_repairc_valid_snippet_followed_by_marker_begin_normal_entry_followed_by_zerofill) {
    auto orig_data = concat_binary(valid_snippet, data_marker_begin_normal_entry_followed_by_zerofill);

    scan_one_pwal_file_repairc(
        orig_data,
        [&](const boost::filesystem::path& p, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::repaired);
            EXPECT_LT(boost::filesystem::file_size(p), orig_data.size());
        },
        0x0FF
    );

    scan_one_pwal_file_repairc(
        orig_data,
        [&](const boost::filesystem::path& p, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::corrupted_durable_entries);
            EXPECT_EQ(boost::filesystem::file_size(p), orig_data.size());
        },
        0x100
    );

    scan_one_pwal_file_repairc(
        orig_data,
        [&](const boost::filesystem::path& p, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::corrupted_durable_entries);
            EXPECT_EQ(boost::filesystem::file_size(p), orig_data.size());
        },
        0x101
    );
}

// === valid_snippet + 0F-6 ===
TEST_F(dblog_scan_test, scan_one_pwal_file_repairc_valid_snippet_followed_by_marker_end_partial_zerofill) {
    auto orig_data = concat_binary(valid_snippet, data_marker_end_partial_zerofill);

    scan_one_pwal_file_repairc(
        orig_data,
        [&](const boost::filesystem::path& p, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::repaired);
            EXPECT_LT(boost::filesystem::file_size(p), orig_data.size());
        },
        0x0FF
    );

    scan_one_pwal_file_repairc(
        orig_data,
        [&](const boost::filesystem::path& p, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::corrupted_durable_entries);
            EXPECT_EQ(boost::filesystem::file_size(p), orig_data.size());
        },
        0x100
    );

    scan_one_pwal_file_repairc(
        orig_data,
        [&](const boost::filesystem::path& p, epoch_id_type, const auto&, const dblog_scan::parse_error& pe) {
            EXPECT_EQ(pe.value(), dblog_scan::parse_error::corrupted_durable_entries);
            EXPECT_EQ(boost::filesystem::file_size(p), orig_data.size());
        },
        0x101
    );
}


}  // namespace limestone::testing
