/*
 * Copyright 2022-2024 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <thread>
#include <sys/stat.h>  
#include <iomanip>
#include <sstream>
#include <string>

#include <boost/filesystem.hpp>

#include <limestone/logging.h>
#include <limestone/api/datastore.h>

#include "dblog_scan.h"
#include "internal.h"
#include "log_entry.h"
#include "online_compaction.h"
#include "compaction_catalog.h"

#include "test_root.h"

using namespace std::literals;
using namespace limestone::api;
using namespace limestone::internal;
namespace limestone::testing {

extern void create_file(const boost::filesystem::path& path, std::string_view content);
extern const std::string_view epoch_0_str;
extern const std::string_view epoch_0x100_str;
extern std::string data_manifest(int persistent_format_version = 1);
extern const std::string_view data_normal;
extern const std::string_view data_nondurable;

class test_1034 : public ::testing::Test {
public:
    void SetUp() {}

    void TearDown() {}

protected:
    std::string byte_to_hex(unsigned char byte) {
        std::ostringstream oss;
        oss << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(byte);
        return oss.str();
    }


    std::string to_hexdump(const std::string& data) {
        std::ostringstream oss;
        const size_t bytes_per_line = 16; // 1行に表示するバイト数

        for (size_t i = 0; i < data.size(); ++i) {
            if (i % bytes_per_line == 0) {
                // 前の行のASCII部分を出力し、改行
                if (i > 0) {
                    size_t line_start = (i / bytes_per_line - 1) * bytes_per_line;
                    oss << " ";
                    for (size_t j = line_start; j < i; ++j) {
                        char c = data[j];
                        oss << (std::isprint(static_cast<unsigned char>(c)) ? c : '.');
                    }
                    oss << "\n";
                }

                // 新しい行のオフセットを出力
                oss << std::setw(8) << std::setfill('0') << std::hex << i << ": ";
            }

            // バイトを16進数で表示
            oss << byte_to_hex(static_cast<unsigned char>(data[i])) << " ";
        }

        // 最後の行の処理（データ長が16の倍数の場合も含む）
        if (!data.empty()) {
            size_t line_start = (data.size() / bytes_per_line) * bytes_per_line;
            if (data.size() % bytes_per_line == 0) {
                line_start -= bytes_per_line; // 最後のフル行の開始位置を取得
            }

            size_t i = data.size();
            oss << " ";
            for (size_t j = line_start; j < i; ++j) {
                char c = data[j];
                oss << (std::isprint(static_cast<unsigned char>(c)) ? c : '.');
            }
        }

        oss << "\n";
        return oss.str();
    }





    void print_write_version(const log_entry& entry, std::ostream& os) {
        os << ", Write Version: Epoch: " 
        << log_entry::write_version_epoch_number(entry.value_etc())
        << ", Minor: " 
        << log_entry::write_version_minor_write_version(entry.value_etc());
    }

    std::string to_string(log_entry::entry_type type) {
        switch (type) {
            case log_entry::entry_type::normal_entry: return "normal_entry";
            case log_entry::entry_type::remove_entry: return "remove_entry";
            case log_entry::entry_type::clear_storage: return "clear_storage";
            case log_entry::entry_type::add_storage: return "add_storage";
            case log_entry::entry_type::remove_storage: return "remove_storage";
            case log_entry::entry_type::marker_begin: return "marker_begin";
            case log_entry::entry_type::marker_end: return "marker_end";
            case log_entry::entry_type::marker_durable: return "marker_durable";
            case log_entry::entry_type::marker_invalidated_begin: return "marker_invalidated_begin";
            default: return "unknown";
        }
    }

    void print_log_entry(const log_entry& entry, std::ostream& os = std::cout) {
        std::string key, value;
        log_entry::entry_type type = entry.type();

        storage_id_type storage_id = entry.storage();
        // 解析に不要なエントリをスキップ
        if (storage_id != 42949672960 && type == log_entry::entry_type::normal_entry) {
            return;
        }

        // Key または Value の取得
        if (type == log_entry::entry_type::normal_entry || type == log_entry::entry_type::remove_entry) {
            entry.key(key); // Key の取得
            // 解析に不要なエントリをスキップ
            // if (key.rfind("00000000146", 0) != 0 && key.rfind("00000000147", 0) != 0) {
            //     return;
            // }
        }
        if (type == log_entry::entry_type::normal_entry) {
            entry.value(value); // Value の取得
        }

        // 1行で基本情報を出力
        os << "Entry Type: " << to_string(type)
        << ", Storage ID: " << storage_id;

        if (type == log_entry::entry_type::normal_entry) {
            os << ", Contains Key and Value";
        } else if (type == log_entry::entry_type::remove_entry) {
            os << ", Contains Key";
        } else if (type == log_entry::entry_type::clear_storage ||
                type == log_entry::entry_type::add_storage ||
                type == log_entry::entry_type::remove_storage) {
            write_version_type write_version;
            entry.write_version(write_version);
            os << ", Write Version: ";
            print_write_version(entry, os);
        } else if (type == log_entry::entry_type::marker_begin ||
                type == log_entry::entry_type::marker_end ||
                type == log_entry::entry_type::marker_durable ||
                type == log_entry::entry_type::marker_invalidated_begin) {
            os << ", Epoch ID: " << entry.epoch_id();
        }

        os << std::endl; // 基本情報の行を終える

        // Key と Value の Hexdump を表示（必要なら）
        if (!key.empty()) {
            os << "Key (Hexdump):\n" << to_hexdump(key) << std::endl;
        }
        if (!value.empty()) {
            os << "Value (Hexdump):\n" << to_hexdump(value) << std::endl;
        }

        // 必要に応じて空行を挿入
        if (!key.empty() || !value.empty()) {
            os << std::endl;
        }
    }



    /**
     * @brief Reads a specified log file (PWAL, compacted_file, snapshot) and returns a list of log entries.
     * @param log_file The path to the log file to be scanned.
     * @return A vector of log_entry objects read from the log file.
     */
    void print_log_entries(const std::string& log_file, const boost::filesystem::path& log_dir) {
        boost::filesystem::path log_path = log_dir / log_file;
        std::cout << std::endl << "Log entries read from " << log_path.string() << ":" << std::endl;

        // 出力ファイルの名前を生成
        boost::filesystem::path output_path = log_dir / ("out-" + log_file + ".txt");
        boost::filesystem::ofstream output_stream(output_path, std::ios_base::out | std::ios_base::trunc);

        if (!output_stream) {
            LOG_AND_THROW_IO_EXCEPTION("cannot open output file: " + output_path.string(), errno);
        }

        

        boost::filesystem::fstream strm;
        strm.open(log_path, std::ios_base::in | std::ios_base::out | std::ios_base::binary);
        if (!strm) {
            LOG_AND_THROW_IO_EXCEPTION("cannot open pwal file: " + log_path.string(), errno);
        }
        while (true) {
            log_entry e;
            log_entry::read_error ec;
            bool data_remains = e.read_entry_from(strm, ec);
            if (data_remains) {
                print_log_entry(e, output_stream);
            } else {
                break;
            }
        }
        return;
    }
};

TEST_F(test_1034, DISABLED_parse_pwals) {
    boost::filesystem::path location = boost::filesystem::path("../../test_data");

    // ディレクトリ内のファイルを列挙
    for (const auto& entry : boost::filesystem::directory_iterator(location)) {
        if (boost::filesystem::is_regular_file(entry)) {
            auto filename = entry.path().filename().string();

            // ファイル名が "pwal" で始まるかを確認
            if (filename.find("pwal") == 0) {
                print_log_entries(filename, location);
            }
        }
    }
}


}  // namespace limestone::testing
