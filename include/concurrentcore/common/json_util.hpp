#pragma once

#include <nlohmann/json.hpp>

#include <cstdint>
#include <optional>

namespace concurrentcore {

/// Returns the value if `j` is a non-negative integer, accepting both of
/// nlohmann's integer representations (parsers produce unsigned for
/// non-negative literals, but programmatically built JSON is signed).
/// Floats, negatives, strings, etc. yield std::nullopt.
[[nodiscard]] inline std::optional<std::uint64_t> as_non_negative_integer(const nlohmann::json& j) {
    if (j.is_number_unsigned()) {
        return j.get<std::uint64_t>();
    }
    if (j.is_number_integer()) {
        const auto v = j.get<std::int64_t>();
        if (v >= 0) {
            return static_cast<std::uint64_t>(v);
        }
    }
    return std::nullopt;
}

}  // namespace concurrentcore
