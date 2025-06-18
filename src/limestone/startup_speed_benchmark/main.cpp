#include <iostream>
#include <vector>
#include <chrono>
#include <limestone/api/configuration.h>
#include <limestone/api/datastore.h>
#include <limestone/api/log_channel.h>
#include <locale>
#include <boost/filesystem.hpp>

static constexpr const char* loc = "log_dir";

int main() {
    // Check if the log directory exists
    boost::filesystem::path dir_path(loc);
    if (!boost::filesystem::exists(dir_path) || !boost::filesystem::is_directory(dir_path)) {
        std::cerr << "Error: directory does not exist: " << boost::filesystem::absolute(dir_path).string() << std::endl;
        return 1;
    }
    std::cerr << "usig log directory: " << boost::filesystem::absolute(dir_path).string() << std::endl;

    // Initialize datasstore
    std::vector<boost::filesystem::path> data_locations{};
    data_locations.emplace_back(loc);
    boost::filesystem::path metadata_location{loc};
    limestone::api::configuration conf(data_locations, metadata_location);

    std::unique_ptr<limestone::api::datastore> datastore_ = std::make_unique<limestone::api::datastore>(conf);
    datastore_->ready();

    // Read the snapshot
    auto snapshot = datastore_->get_snapshot();
    auto cursor = snapshot->get_cursor();

    auto start = std::chrono::steady_clock::now();
    int i = 0;
    while (cursor->next()) {
        i++;
    }
    auto end = std::chrono::steady_clock::now();
    auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

    // Output the result
    struct comma_numpunct : std::numpunct<char> {
        char do_thousands_sep() const override { return ','; }
        std::string do_grouping() const override { return "\3"; }
    };
    std::cerr.imbue(std::locale(std::cerr.getloc(), new comma_numpunct));
    std::cerr << "entry_count = " << i << ", elapsed cursor read time = " << elapsed_ms << "ms" << std::endl;

    datastore_->shutdown();
}