#pragma once

#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>

namespace concurrentcore {

/// Error codes shared by the wire protocol and by task execution. The
/// string forms (see to_string) are part of the public protocol.
enum class ErrorCode {
    // Protocol / request level
    MalformedJson,       ///< Frame payload is not valid JSON.
    InvalidRequest,      ///< Valid JSON but not a valid request object.
    UnknownOperation,    ///< "type" is not a supported operation.
    InvalidParams,       ///< Operation or task parameters failed validation.
    UnknownTaskType,     ///< "task_type" is not a supported task.
    MessageTooLarge,     ///< Frame length exceeds the configured maximum.
    JobNotFound,         ///< No job with that id (never existed or evicted).
    JobNotFinished,      ///< GET_JOB_RESULT on a job that is not terminal.
    QueueFull,           ///< Back-pressure: the task queue is at capacity.
    ServerShuttingDown,  ///< Server is draining and accepts no new jobs.
    ServerBusy,          ///< Connection limit reached.
    // Task execution
    FileNotFound,        ///< Input file does not exist in the workspace.
    PathNotAllowed,      ///< Path escapes the workspace or is otherwise unsafe.
    OutputExists,        ///< Output exists and overwrite was not requested.
    IoError,             ///< Read/write failure.
    LimitExceeded,       ///< A configured resource limit was hit.
    CorruptData,         ///< Compressed input is invalid or truncated.
    Cancelled,           ///< The operation observed a cancellation request.
    InternalError,       ///< Unexpected failure; see message.
};

[[nodiscard]] std::string_view to_string(ErrorCode code) noexcept;
[[nodiscard]] std::optional<ErrorCode> error_code_from_string(std::string_view text) noexcept;

/// Exception carrying a protocol-visible error code. Thrown by request
/// validation and task execution; converted to a JSON error at the edge.
class CoreError : public std::runtime_error {
public:
    CoreError(ErrorCode code, const std::string& message)
        : std::runtime_error(message), code_(code) {}

    [[nodiscard]] ErrorCode code() const noexcept { return code_; }

private:
    ErrorCode code_;
};

/// Thrown by a task when it observes a cancellation request.
class OperationCancelled : public CoreError {
public:
    OperationCancelled() : CoreError(ErrorCode::Cancelled, "operation cancelled") {}
};

}  // namespace concurrentcore
