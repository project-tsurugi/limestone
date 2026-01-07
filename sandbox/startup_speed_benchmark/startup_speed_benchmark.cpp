#include <iostream>
#include <vector>
#include <thread>
#include <chrono>
#include <locale>
#include <memory>

#include <boost/filesystem.hpp>
#include <limestone/api/configuration.h>
#include <limestone/api/datastore.h>
#include <limestone/api/log_channel.h>

namespace {

    struct BenchmarkResult {
        std::string mode;
        std::size_t entry_count{};
        std::size_t elapsed_ms{};
    };

    struct comma_numpunct : std::numpunct<char> {
        char do_thousands_sep() const override { return ','; }
        std::string do_grouping() const override { return "\3"; }
    };

    BenchmarkResult measure_standard_cursor(const boost::filesystem::path& loc) {
        limestone::api::configuration conf{};
        conf.set_data_location(loc);

        std::unique_ptr<limestone::api::datastore> ds = std::make_unique<limestone::api::datastore>(conf);
        ds->ready();
        auto snapshot = ds->get_snapshot();
        auto cursor = snapshot->get_cursor();

        auto start = std::chrono::steady_clock::now();
        std::size_t count = 0;
        while (cursor->next()) ++count;
        auto end = std::chrono::steady_clock::now();

        ds->shutdown();

        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
        std::cerr << "standard_cursor entry_count = " << count << ", elapsed = " << elapsed << "ms\n";

        return {"standard_cursor", count, static_cast<std::size_t>(elapsed)};
    }

    BenchmarkResult measure_partitioned_cursor(const boost::filesystem::path& loc, std::size_t partition_count) {
        limestone::api::configuration conf{};
        conf.set_data_location(loc);

        std::unique_ptr<limestone::api::datastore> ds = std::make_unique<limestone::api::datastore>(conf);
        ds->ready();
        auto snapshot = ds->get_snapshot();
        auto cursors = snapshot->get_partitioned_cursors(partition_count);

        auto start = std::chrono::steady_clock::now();
        std::size_t count = 0;
        std::vector<std::thread> threads;
        for (auto& cursor : cursors) {
            threads.emplace_back([&count, &cursor]() {
                std::size_t local = 0;
                while (cursor->next()) ++local;
                __sync_fetch_and_add(&count, local);  // atomic add
            });
        }
        for (auto& t : threads) t.join();
        auto end = std::chrono::steady_clock::now();

        ds->shutdown();

        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
        std::cerr << "partitioned_cursor[" << partition_count << "] entry_count = " << count << ", elapsed = " << elapsed << "ms\n";

        return {"partitioned_cursor[" + std::to_string(partition_count) + "]", count, static_cast<std::size_t>(elapsed)};
    }

    void print_summary(const std::vector<BenchmarkResult>& results) {
        std::cerr << "\n=== Benchmark Summary ===\n";
        for (const auto& r : results) {
            std::cerr << r.mode << ": entry_count = " << r.entry_count
                      << ", elapsed = " << r.elapsed_ms << "ms\n";
        }
    }
}  // namespace

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <log_directory>\n";
        return 1;
    }

    boost::filesystem::path loc(argv[1]);

    if (!boost::filesystem::exists(loc) || !boost::filesystem::is_directory(loc)) {
        std::cerr << "Error: directory does not exist: " << boost::filesystem::absolute(loc).string() << "\n";
        return 1;
    }

    std::cerr.imbue(std::locale(std::cerr.getloc(), new comma_numpunct));
    std::cerr << "using log directory: " << boost::filesystem::absolute(loc).string() << "\n";

    std::vector<BenchmarkResult> results;

    std::cerr << "\n== measuring standard cursor ==\n";
    results.push_back(measure_standard_cursor(loc));

    std::vector<std::size_t> partition_counts = {1, 2, 4, 8, 16};
    for (auto n : partition_counts) {
        std::cerr << "\n== measuring partitioned cursor with " << n << " partitions ==\n";
        results.push_back(measure_partitioned_cursor(loc, n));
    }

    print_summary(results);
    return 0;
}
