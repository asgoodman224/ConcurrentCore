#include "concurrentcore/networking/protocol.hpp"

#include <array>
#include <utility>

namespace concurrentcore::protocol {

std::string_view to_string(Operation op) noexcept {
    switch (op) {
        case Operation::SubmitJob: return "SUBMIT_JOB";
        case Operation::GetJobStatus: return "GET_JOB_STATUS";
        case Operation::GetJobResult: return "GET_JOB_RESULT";
        case Operation::CancelJob: return "CANCEL_JOB";
        case Operation::GetServerStats: return "GET_SERVER_STATS";
        case Operation::Ping: return "PING";
    }
    return "UNKNOWN";
}

std::optional<Operation> operation_from_string(std::string_view text) noexcept {
    static constexpr std::array<Operation, 6> kAll{
        Operation::SubmitJob, Operation::GetJobStatus,   Operation::GetJobResult,
        Operation::CancelJob, Operation::GetServerStats, Operation::Ping};
    for (const Operation op : kAll) {
        if (to_string(op) == text) {
            return op;
        }
    }
    return std::nullopt;
}

nlohmann::json parse(std::string_view payload) {
    if (payload.empty()) {
        throw CoreError(ErrorCode::MalformedJson, "empty message");
    }
    bool too_deep = false;
    // The parser callback sees the nesting depth of every element; we abort
    // (by discarding the value and flagging) once the limit is exceeded.
    const nlohmann::json::parser_callback_t depth_guard =
        [&too_deep](int depth, nlohmann::json::parse_event_t, nlohmann::json&) {
            if (depth > static_cast<int>(kMaxJsonDepth)) {
                too_deep = true;
                return false;
            }
            return true;
        };
    nlohmann::json value =
        nlohmann::json::parse(payload.begin(), payload.end(), depth_guard, /*allow_exceptions=*/false);
    if (too_deep) {
        throw CoreError(ErrorCode::MalformedJson, "JSON nesting exceeds depth limit");
    }
    if (value.is_discarded()) {
        throw CoreError(ErrorCode::MalformedJson, "message is not valid JSON");
    }
    return value;
}

std::string serialize(const nlohmann::json& message) {
    return message.dump(-1, ' ', false, nlohmann::json::error_handler_t::replace);
}

nlohmann::json make_ok(std::string_view op, const nlohmann::json& request_id,
                       nlohmann::json result) {
    return {
        {"kind", "response"},
        {"type", op},
        {"request_id", request_id},
        {"ok", true},
        {"result", std::move(result)},
    };
}

nlohmann::json make_error(std::string_view op, const nlohmann::json& request_id, ErrorCode code,
                          std::string_view message) {
    return {
        {"kind", "response"},
        {"type", op},
        {"request_id", request_id},
        {"ok", false},
        {"error", {{"code", to_string(code)}, {"message", message}}},
    };
}

nlohmann::json make_event(std::string_view event, const nlohmann::json& request_id,
                          nlohmann::json data) {
    return {
        {"kind", "event"},
        {"type", event},
        {"request_id", request_id},
        {"data", std::move(data)},
    };
}

}  // namespace concurrentcore::protocol
