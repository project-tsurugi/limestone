#include <ctime>
#include <cstdint>

namespace limestone::internal {

inline uint64_t now_nsec() {
    struct timespec ts{};
    if (clock_gettime(CLOCK_MONOTONIC_RAW, &ts) != 0) {
        return 0; // Retuurn 0 indicating an error
    }
    return static_cast<uint64_t>(ts.tv_sec) * 1'000'000'000ULL + ts.tv_nsec;
}

}  // namespace limestone::internal