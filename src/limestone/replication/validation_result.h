#pragma once

#include <string>
#include <cstdint>

namespace limestone::replication {

class validation_result {
public:
    static validation_result success() noexcept;
    static validation_result error(uint16_t code, std::string message);

    [[nodiscard]] bool ok() const noexcept;
    [[nodiscard]] uint16_t error_code() const noexcept;
    [[nodiscard]] const std::string& error_message() const noexcept;

private:
    validation_result(bool ok, uint16_t code, std::string message);

    bool ok_;
    uint16_t error_code_;
    std::string error_message_;
};

// TODO: 現状は、error_codeにダミー値を入れているが、整理して適切な値を入れる必要がある

} // namespace limestone::replication