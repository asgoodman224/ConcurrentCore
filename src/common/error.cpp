#include "concurrentcore/common/error.hpp"

#include <array>
#include <utility>

namespace concurrentcore {
namespace {

constexpr std::array<std::pair<ErrorCode, std::string_view>, 19> kErrorNames{{
    {ErrorCode::MalformedJson, "MALFORMED_JSON"},
    {ErrorCode::InvalidRequest, "INVALID_REQUEST"},
    {ErrorCode::UnknownOperation, "UNKNOWN_OPERATION"},
    {ErrorCode::InvalidParams, "INVALID_PARAMS"},
    {ErrorCode::UnknownTaskType, "UNKNOWN_TASK_TYPE"},
    {ErrorCode::MessageTooLarge, "MESSAGE_TOO_LARGE"},
    {ErrorCode::JobNotFound, "JOB_NOT_FOUND"},
    {ErrorCode::JobNotFinished, "JOB_NOT_FINISHED"},
    {ErrorCode::QueueFull, "QUEUE_FULL"},
    {ErrorCode::ServerShuttingDown, "SERVER_SHUTTING_DOWN"},
    {ErrorCode::ServerBusy, "SERVER_BUSY"},
    {ErrorCode::FileNotFound, "FILE_NOT_FOUND"},
    {ErrorCode::PathNotAllowed, "PATH_NOT_ALLOWED"},
    {ErrorCode::OutputExists, "OUTPUT_EXISTS"},
    {ErrorCode::IoError, "IO_ERROR"},
    {ErrorCode::LimitExceeded, "LIMIT_EXCEEDED"},
    {ErrorCode::CorruptData, "CORRUPT_DATA"},
    {ErrorCode::Cancelled, "CANCELLED"},
    {ErrorCode::InternalError, "INTERNAL_ERROR"},
}};

}  // namespace

std::string_view to_string(ErrorCode code) noexcept {
    for (const auto& [value, name] : kErrorNames) {
        if (value == code) {
            return name;
        }
    }
    return "INTERNAL_ERROR";
}

std::optional<ErrorCode> error_code_from_string(std::string_view text) noexcept {
    for (const auto& [value, name] : kErrorNames) {
        if (name == text) {
            return value;
        }
    }
    return std::nullopt;
}

}  // namespace concurrentcore
