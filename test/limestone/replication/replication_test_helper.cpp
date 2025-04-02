#include "replication_test_helper.h"
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include "dblog_scan.h"
#include "internal.h"
#include "log_entry.h"
#include <limestone/api/storage_id_type.h>
#include <limestone/api/write_version_type.h>
namespace limestone::testing
{

using limestone::api::log_entry;    
using limestone::api::epoch_id_type;
using limestone::api::storage_id_type;
using limestone::api::write_version_type;

uint16_t get_free_port() {
    int sock = ::socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;
    ::bind(sock, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    socklen_t len = sizeof(addr);
    ::getsockname(sock, reinterpret_cast<sockaddr*>(&addr), &len);
    uint16_t port = ntohs(addr.sin_port);
    ::close(sock);
    return port;
}

int start_test_server(uint16_t port, bool echo_message, bool close_immediately) {
    int listen_fd = ::socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(port);

    int opt = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    bind(listen_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    listen(listen_fd, 1);
    return listen_fd;
}

sockaddr_in make_listen_addr(uint16_t port) {
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(port);
    return addr;
}




void print_log_entry(const log_entry& entry) {
    std::string key;
    storage_id_type storage_id = entry.storage();
    log_entry::entry_type type = entry.type();

    if (type == log_entry::entry_type::normal_entry || type == log_entry::entry_type::remove_entry) {
        entry.key(key);
    }

    switch (type) {
        case log_entry::entry_type::normal_entry: {
            std::string value;
            entry.value(value);
            std::cout << "Entry Type: normal_entry, Storage ID: " << storage_id << ", Key: " << key << ", Value: " << value
                      << ", Write Version: Epoch: " << log_entry::write_version_epoch_number(entry.value_etc())
                      << ", Minor: " << log_entry::write_version_minor_write_version(entry.value_etc()) << std::endl;
            break;
        }
        case log_entry::entry_type::normal_with_blob: {
            std::string value;
            entry.value(value);
            std::cout << "Entry Type: normal_with_blob, Storage ID: " << storage_id << ", Key: " << key << ", Value: " << value
                      << ", Write Version: Epoch: " << log_entry::write_version_epoch_number(entry.value_etc())
                      << ", Minor: " << log_entry::write_version_minor_write_version(entry.value_etc()) 
                      << ", Blob IDs: ";
            for (const auto& blob_id : entry.get_blob_ids()) {
                std::cout << blob_id << " ";
            }
            std::cout << std::endl;
            break;
        }
        case log_entry::entry_type::remove_entry: {
            std::cout << "Entry Type: remove_entry, Storage ID: " << storage_id << ", Key: " << key
                      << ", Write Version: Epoch: " << log_entry::write_version_epoch_number(entry.value_etc())
                      << ", Minor: " << log_entry::write_version_minor_write_version(entry.value_etc()) << std::endl;
            break;
        }
        case log_entry::entry_type::clear_storage:
        case log_entry::entry_type::add_storage:
        case log_entry::entry_type::remove_storage: {
            write_version_type write_version;
            entry.write_version(write_version);
            std::cout << "Entry Type: " << static_cast<int>(type) << ", Storage ID: " << storage_id
                      << ", Write Version: Epoch: " << log_entry::write_version_epoch_number(entry.value_etc())
                      << ", Minor: " << log_entry::write_version_minor_write_version(entry.value_etc()) << std::endl;
            break;
        }
        case log_entry::entry_type::marker_begin:
            std::cout << "Entry Type: marker_begin, Epoch ID: " << entry.epoch_id() << std::endl;
            break;
        case log_entry::entry_type::marker_end:
            std::cout << "Entry Type: marker_end, Epoch ID: " << entry.epoch_id() << std::endl;
            break;
        case log_entry::entry_type::marker_durable:
            std::cout << "Entry Type: marker_durable, Epoch ID: " << entry.epoch_id() << std::endl;
            break;
        case log_entry::entry_type::marker_invalidated_begin:
            std::cout << "Entry Type: marker_invalidated_begin, Epoch ID: " << entry.epoch_id() << std::endl;
            break;
        default:
            std::cout << "Entry Type: unknown" << std::endl;
            break;
    }
}


std::vector<log_entry> read_log_file(boost::filesystem::path log_path) {
    std::vector<log_entry> log_entries;
    limestone::internal::dblog_scan::parse_error pe;

    // Define a lambda function to capture and store log entries
    auto add_entry = [&](log_entry& e) { log_entries.push_back(e); };

    // Error reporting function, returning bool as expected by error_report_func_t
    auto report_error = [](log_entry::read_error& error) -> bool {
        std::cerr << "Error during log file scan: " << error.message() << std::endl;
        return false;  // Return false to indicate an error occurred
    };

    // Initialize a dblog_scan instance with the log directory
    limestone::internal::dblog_scan scanner(log_path.parent_path());

    // Scan the specified log file
    epoch_id_type max_epoch = scanner.scan_one_pwal_file(log_path, UINT64_MAX, add_entry, report_error, pe);

    if (pe.value() != limestone::internal::dblog_scan::parse_error::ok) {
        std::cerr << "Parse error occurred while reading the log file: " << log_path.string() << std::endl;
    }

    // Iterate over the log entries and print relevant information
    std::cout << std::endl << "Log entries read from " << log_path.string() << ":" << std::endl;
    for (const auto& entry : log_entries) {
        print_log_entry(entry);
    }

    return log_entries;
}

::testing::AssertionResult AssertLogEntry(const log_entry& entry, const std::optional<storage_id_type>& expected_storage_id, const std::optional<std::string>& expected_key,
                                          const std::optional<std::string>& expected_value, const std::optional<epoch_id_type>& expected_epoch_number,
                                          const std::optional<std::uint64_t>& expected_minor_version, const std::vector<blob_id_type>& expected_blob_ids,
                                          log_entry::entry_type expected_type) {
    // Check the entry type
    if (entry.type() != expected_type) {
        return ::testing::AssertionFailure() << "Expected entry type: " << static_cast<int>(expected_type) << ", but got: " << static_cast<int>(entry.type());
    }

    // Check the storage ID if it exists
    if (expected_storage_id.has_value()) {
        if (entry.storage() != expected_storage_id.value()) {
            return ::testing::AssertionFailure() << "Expected storage ID: " << expected_storage_id.value() << ", but got: " << entry.storage();
        }
    }

    // Check the key if it exists
    if (expected_key.has_value()) {
        std::string actual_key;
        entry.key(actual_key);
        if (actual_key != expected_key.value()) {
            return ::testing::AssertionFailure() << "Expected key: " << expected_key.value() << ", but got: " << actual_key;
        }
    }

    // Check the value if it exists
    if (expected_value.has_value()) {
        std::string actual_value;
        entry.value(actual_value);
        if (actual_value != expected_value.value()) {
            return ::testing::AssertionFailure() << "Expected value: " << expected_value.value() << ", but got: " << actual_value;
        }
    }

    // Check the write version if it exists
    if (expected_epoch_number.has_value() && expected_minor_version.has_value()) {
        epoch_id_type actual_epoch_number = log_entry::write_version_epoch_number(entry.value_etc());
        std::uint64_t actual_minor_version = log_entry::write_version_minor_write_version(entry.value_etc());

        if (actual_epoch_number != expected_epoch_number.value() || actual_minor_version != expected_minor_version.value()) {
            return ::testing::AssertionFailure() << "Expected write version (epoch_number: " << expected_epoch_number.value()
                                                 << ", minor_write_version: " << expected_minor_version.value() << "), but got (epoch_number: " << actual_epoch_number
                                                 << ", minor_write_version: " << actual_minor_version << ")";
        }
    }

    // Check the blob IDs
    if (entry.type() == log_entry::entry_type::normal_with_blob) {
        std::vector<blob_id_type> actual_blob_ids = entry.get_blob_ids();
        if (actual_blob_ids.size() != expected_blob_ids.size()) {
            return ::testing::AssertionFailure() << "Expected blob IDs size: " << expected_blob_ids.size() << ", but got: " << actual_blob_ids.size();
        }

        for (std::size_t i = 0; i < expected_blob_ids.size(); ++i) {
            if (actual_blob_ids[i] != expected_blob_ids[i]) {
                return ::testing::AssertionFailure() << "Expected blob ID: " << expected_blob_ids[i] << ", but got: " << actual_blob_ids[i];
            }
        }
    }
    // If all checks pass, return success
    return ::testing::AssertionSuccess();
}

}  // namespace limestone::testing
