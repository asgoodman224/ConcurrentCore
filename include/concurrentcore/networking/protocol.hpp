#pragma once

#include "concurrentcore/common/error.hpp"

#include <nlohmann/json.hpp>

#include <cstddef>
#include <optional>
#include <string>
#include <string_view>

/// JSON message layer shared by the server and the client.
/// See docs/PROTOCOL.md for the full schema with examples.
namespace concurrentcore::protocol {

inline constexpr int kProtocolVersion = 1;

/// Maximum nesting depth accepted in a request. Requests are shallow; the
/// limit rejects pathological "[[[[...]]]]" payloads cheaply.
inline constexpr std::size_t kMaxJsonDepth = 32;

enum class Operation { SubmitJob, GetJobStatus, GetJobResult, CancelJob, GetServerStats, Ping };

[[nodiscard]] std::string_view to_string(Operation op) noexcept;
[[nodiscard]] std::optional<Operation> operation_from_string(std::string_view text) noexcept;

/// Event pushed to a client that submitted a job with "notify": true.
inline constexpr std::string_view kJobFinishedEvent = "JOB_FINISHED";

/// Parses a payload. Throws CoreError(MalformedJson) on syntax errors,
/// excessive nesting, or an empty payload.
[[nodiscard]] nlohmann::json parse(std::string_view payload);

/// Serializes compactly. Invalid UTF-8 in strings is replaced with U+FFFD
/// instead of throwing, so a response can always be produced.
[[nodiscard]] std::string serialize(const nlohmann::json& message);

/// {"kind":"response","type":op,"request_id":id,"ok":true,"result":result}
[[nodiscard]] nlohmann::json make_ok(std::string_view op, const nlohmann::json& request_id,
                                     nlohmann::json result);

/// {"kind":"response","type":op,"request_id":id,"ok":false,
///  "error":{"code":CODE,"message":message}}
[[nodiscard]] nlohmann::json make_error(std::string_view op, const nlohmann::json& request_id,
                                        ErrorCode code, std::string_view message);

/// {"kind":"event","type":event,"request_id":id,"data":data}
[[nodiscard]] nlohmann::json make_event(std::string_view event, const nlohmann::json& request_id,
                                        nlohmann::json data);

}  // namespace concurrentcore::protocol
