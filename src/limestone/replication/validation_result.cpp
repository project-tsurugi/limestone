#include "replication/validation_result.h"

namespace limestone::replication {

validation_result validation_result::success() noexcept {
    return {true, 0, {}};
}

validation_result validation_result::error(uint16_t code, std::string message) {
    return {false, code, std::move(message)};
}

validation_result::validation_result(bool ok, uint16_t code, std::string message)
    : ok_(ok), error_code_(code), error_message_(std::move(message)) {}

bool validation_result::ok() const noexcept {
    return ok_;
}

uint16_t validation_result::error_code() const noexcept {
    return error_code_;
}

const std::string& validation_result::error_message() const noexcept {
    return error_message_;
}

} // namespace limestone::replication