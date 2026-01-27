#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <string_view>
#include <vector>

namespace limestone::grpc::backend {

class tp_monitor_backend {
public:
    struct create_result {
        bool ok{};
        std::uint64_t tpm_id{};
        std::string message{};
    };

    struct result {
        bool ok{};
        std::string message{};
    };

    tp_monitor_backend() = default;
    ~tp_monitor_backend() = default;

    tp_monitor_backend(const tp_monitor_backend&) = delete;
    tp_monitor_backend& operator=(const tp_monitor_backend&) = delete;
    tp_monitor_backend(tp_monitor_backend&&) = delete;
    tp_monitor_backend& operator=(tp_monitor_backend&&) = delete;

    create_result create(std::string_view tx_id, std::uint64_t ts_id);
    create_result create_and_join(std::string_view tx_id1,
                                  std::uint64_t ts_id1,
                                  std::string_view tx_id2,
                                  std::uint64_t ts_id2);
    result join(std::uint64_t tpm_id, std::string_view tx_id, std::uint64_t ts_id);
    result barrier_notify(std::uint64_t tpm_id, std::uint64_t ts_id);
    result destroy(std::uint64_t tpm_id);

private:
    struct monitor_state {
        std::uint64_t tpm_id{};
        std::uint32_t participant_count{};
        std::set<std::uint64_t> participants{};
        std::set<std::uint64_t> arrived{};
        std::mutex mtx{};
        std::condition_variable cv{};
        bool destroyed{};
    };

    std::shared_ptr<monitor_state> find_state(std::uint64_t tpm_id);

    std::atomic_uint64_t next_tpm_id_{1};
    std::mutex mtx_{};
    std::map<std::uint64_t, std::shared_ptr<monitor_state>> monitors_{};
};

} // namespace limestone::grpc::backend
