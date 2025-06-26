#include <iostream>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <log_entry.h>

const char* to_string(limestone::api::log_entry::entry_type type) {
    using E = limestone::api::log_entry::entry_type;
    switch (type) {
    case E::this_id_is_not_used: return "unused";
    case E::normal_entry: return "normal";
    case E::normal_with_blob: return "normal_with_blob";
    case E::remove_entry: return "remove";
    case E::marker_begin: return "marker_begin";
    case E::marker_end: return "marker_end";
    case E::marker_durable: return "marker_durable";
    case E::marker_invalidated_begin: return "invalidated_begin";
    case E::clear_storage: return "clear_storage";
    case E::add_storage: return "add_storage";
    case E::remove_storage: return "remove_storage";
    default: return "unknown";
    }
}

bool is_valid_utf8(const std::string& str) {
    int expected = 0;
    for (unsigned char c : str) {
        if (expected == 0) {
            if ((c & 0x80) == 0x00) continue;
            if ((c & 0xE0) == 0xC0) expected = 1;
            else if ((c & 0xF0) == 0xE0) expected = 2;
            else if ((c & 0xF8) == 0xF0) expected = 3;
            else return false;
        } else {
            if ((c & 0xC0) != 0x80) return false;
            --expected;
        }
    }
    return expected == 0;
}

std::string format_preview_ascii(const std::string& data, std::size_t limit = 20) {
    std::ostringstream oss;
    std::size_t count = 0;
    for (unsigned char c : data) {
        if (count++ >= limit) break;
        if (c >= 0x21 && c <= 0x7E) {
            oss << static_cast<char>(c);  // printable ASCII
        } else if (c == 0x20) {
            oss << '_';  // space
        } else {
            oss << '.';  // control or non-ASCII
        }
    }
    return oss.str();
}


std::string escape_for_output(const std::string& str, std::size_t limit = 20) {
    std::ostringstream oss;
    std::size_t count = 0;

    for (unsigned char c : str) {
        if (count >= limit) break;

        if (std::isprint(c) && c != ' ' && c != '\\' && c != '\"') {
            oss << c;
        } else if (c == ' ') {
            oss << "_";  // スペースはアンダースコアに変換（シェル/スクリプト処理しやすさ）
        } else if (c == '\\') {
            oss << "\\\\";
        } else if (c == '\"') {
            oss << "\\\"";
        } else if (c == '\n') {
            oss << "\\n";
        } else if (c == '\t') {
            oss << "\\t";
        } else {
            oss << "\\x" << std::hex << std::setw(2) << std::setfill('0')
                << static_cast<int>(c) << std::dec;
        }

        ++count;
    }

    return oss.str();
}

std::string format_data_field(const std::string& data, std::size_t limit = 20) {
    if (is_valid_utf8(data)) {
        return "\"" + escape_for_output(data, limit) + "\"";
    } else {
        std::ostringstream oss;
        oss << "0x";
        std::size_t count = 0;
        for (unsigned char c : data) {
            if (count++ >= limit) break;
            oss << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(c);
        }
        return oss.str();
    }
}

void print_entry(const limestone::api::log_entry& entry) {
    using E = limestone::api::log_entry::entry_type;

    std::cout << to_string(entry.type());

    bool has_epoch_id = false;
    bool has_storage_id = false;

    switch (entry.type()) {
    case E::marker_begin:
    case E::marker_end:
    case E::marker_durable:
    case E::marker_invalidated_begin:
        has_epoch_id = true;
        break;

    case E::normal_entry:
    case E::normal_with_blob:
    case E::remove_entry:
    case E::clear_storage:
    case E::add_storage:
    case E::remove_storage:
        has_storage_id = true;
        break;

    default:
        break;
    }

    if (has_epoch_id) {
        std::cout << " epoch_id=" << entry.epoch_id();
    }

    if (has_storage_id) {
        std::cout << " storage_id=" << entry.storage();
    }

    switch (entry.type()) {
case E::normal_entry:
case E::normal_with_blob: {
    std::string k, v;
    entry.key(k);
    entry.value(v);
    std::cout << " key=" << format_preview_ascii(k)
              << " value=" << format_preview_ascii(v);
    break;
}

case E::remove_entry: {
    std::string k;
    entry.key(k);
    std::cout << " key=" << format_preview_ascii(k);
    break;
}

    default:
        break;
    }

    std::cout << std::endl;
}




void dump_wal(const std::filesystem::path& file_path) {
    std::ifstream in(file_path, std::ios::binary);
    if (!in) {
        std::cerr << "Error: Cannot open file '" << file_path << "'" << std::endl;
        return;
    }

    std::size_t count = 0;
    while (true) {
        limestone::api::log_entry entry;
        bool success = entry.read(in);
        if (!success) {
            break;
        }

        print_entry(entry);
        ++count;
    }

    std::cerr << "Total entries: " << count << std::endl;
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <argument>" << std::endl;
        return 1;
    }
    
    std::filesystem::path file_path(argv[1]);
    
    if (!std::filesystem::exists(file_path)) {
        std::cerr << "Error: File '" << argv[1] << "' does not exist" << std::endl;
        return 1;
    }
    
    if (!std::filesystem::is_regular_file(file_path)) {
        std::cerr << "Error: '" << argv[1] << "' is not a regular file" << std::endl;
        return 1;
    }
    
    std::cout << "First argument: " << argv[1] << std::endl;
    dump_wal(file_path);
    return 0;
}
