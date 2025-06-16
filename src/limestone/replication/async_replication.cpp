#include "async_replication.h"

#include <glog/logging.h>

#include <cstdlib>
#include <stdexcept>

#include "limestone/logging.h"
#include "logging_helper.h"

namespace limestone::replication {

std::string to_string(async_replication mode) {
    switch (mode) {
        case async_replication::disabled: return "disabled";
        case async_replication::std_async: return "std_async";
        case async_replication::single_thread_async: return "single_thread_async";
        case async_replication::boost_thread_pool_async: return "boost_thread_pool_async";
        case async_replication::tbb_thread_pool_async: return "tbb_thread_pool_async";
        default: return "unknown";
    }
}

async_replication async_replication_from_string(const std::string& str) {
    if (str == "disabled" || str.empty()) return async_replication::disabled;
    if (str == "std_async") return async_replication::std_async;
    if (str == "single_thread_async") return async_replication::single_thread_async;
    if (str == "boost_thread_pool_async") return async_replication::boost_thread_pool_async;
    if (str == "tbb_thread_pool_async") return async_replication::tbb_thread_pool_async;
    throw std::invalid_argument("Invalid async_replication string: " + str);
}

async_replication async_replication_from_env(const char* env_name) {
    const char* env = std::getenv(env_name);
    std::string v = env ? std::string(env) : "";
    try {
        return async_replication_from_string(v);
    } catch (const std::invalid_argument&) {
        LOG_LP(FATAL) << "Invalid value for " << env_name << ": " << v;
        // Defensive: LOG_LP(FATAL) should terminate the process; std::abort() is for redundancy.
        std::abort();
    }
}

} // namespace limestone::replication
