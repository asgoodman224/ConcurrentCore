#pragma once

#include "concurrentcore/common/error.hpp"
#include "concurrentcore/tasks/task_spec.hpp"

#include <nlohmann/json.hpp>

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <stop_token>
#include <string>
#include <string_view>

namespace concurrentcore {

using JobId = std::uint64_t;
using SystemTime = std::chrono::system_clock::time_point;
using SteadyTime = std::chrono::steady_clock::time_point;

enum class JobStatus { Queued, Running, Completed, Failed, Cancelled };

[[nodiscard]] std::string_view to_string(JobStatus status) noexcept;
[[nodiscard]] bool is_terminal(JobStatus status) noexcept;

/// The job state machine. Only these edges are legal:
///   QUEUED  -> RUNNING | CANCELLED
///   RUNNING -> COMPLETED | FAILED | CANCELLED
/// Terminal states have no outgoing edges.
[[nodiscard]] bool can_transition(JobStatus from, JobStatus to) noexcept;

struct JobError {
    ErrorCode code = ErrorCode::InternalError;
    std::string message;
};

enum class CancelOutcome {
    Cancelled,              ///< Job was QUEUED and is now CANCELLED.
    CancellationRequested,  ///< Job is RUNNING; stop was requested cooperatively.
    AlreadyFinished,        ///< Job was already terminal; nothing changed.
    NotFound,               ///< No such job.
};

[[nodiscard]] std::string_view to_string(CancelOutcome outcome) noexcept;

/// Immutable, self-contained copy of a job's state at one instant. Safe to
/// hand to other threads. The (potentially large) result JSON is shared via
/// shared_ptr<const> so snapshots never deep-copy it.
struct JobSnapshot {
    JobId id = 0;
    TaskType type = TaskType::Compute;
    JobStatus status = JobStatus::Queued;
    bool cancel_requested = false;
    SystemTime created_at{};
    std::optional<SystemTime> started_at;
    std::optional<SystemTime> finished_at;
    std::optional<std::chrono::nanoseconds> queue_wait;      ///< started - created
    std::optional<std::chrono::nanoseconds> execution_time;  ///< finished - started
    std::shared_ptr<const nlohmann::json> result;            ///< COMPLETED only
    std::optional<JobError> error;                           ///< FAILED / CANCELLED
};

/// Serializes a snapshot for the wire. `include_result` controls whether
/// the result / error payload is embedded.
[[nodiscard]] nlohmann::json to_json(const JobSnapshot& snapshot, bool include_result);

/// One unit of work and its lifecycle state.
///
/// Thread-safety: `id`, `type` and `spec` are immutable after construction
/// and may be read without locking. All mutable state is guarded by
/// `mutex_`, which is a leaf lock: no other lock is ever acquired while it is
/// held and no callback is invoked under it.
///
/// Exactly-once completion: every terminal transition happens under
/// `mutex_` and succeeds at most once, so exactly one thread "wins" the
/// right to finalize the job (see JobManager).
class Job {
public:
    using CompletionCallback = std::function<void(const JobSnapshot&)>;

    Job(JobId id, TaskSpec spec, CompletionCallback on_complete = {});

    Job(const Job&) = delete;
    Job& operator=(const Job&) = delete;
    Job(Job&&) = delete;
    Job& operator=(Job&&) = delete;
    ~Job() = default;

    [[nodiscard]] JobId id() const noexcept { return id_; }
    [[nodiscard]] TaskType type() const noexcept { return spec_.type; }
    [[nodiscard]] const TaskSpec& spec() const noexcept { return spec_; }

    /// Token observed by the running task for cooperative cancellation.
    [[nodiscard]] std::stop_token stop_token() const noexcept { return stop_source_.get_token(); }

    /// QUEUED -> RUNNING. Returns false if the job was cancelled while queued.
    [[nodiscard]] bool try_start();

    /// RUNNING -> COMPLETED. Returns false if the transition is illegal.
    [[nodiscard]] bool complete(std::shared_ptr<const nlohmann::json> result);

    /// RUNNING -> FAILED.
    [[nodiscard]] bool fail(JobError error);

    /// RUNNING -> CANCELLED, after the task observed the stop request.
    [[nodiscard]] bool finish_cancelled();

    /// QUEUED -> CANCELLED immediately, or for RUNNING jobs requests a
    /// cooperative stop (status stays RUNNING until the task reacts).
    [[nodiscard]] CancelOutcome request_cancel();

    /// Moves the completion callback out. Called once by whichever thread
    /// performed the terminal transition, then invoked without any lock held.
    [[nodiscard]] CompletionCallback take_completion_callback();

    [[nodiscard]] JobStatus status() const;
    [[nodiscard]] JobSnapshot snapshot() const;

private:
    // Applies `to` if legal and stamps the matching timestamps.
    bool transition_locked(JobStatus to);

    const JobId id_;
    const TaskSpec spec_;
    const SystemTime created_at_;
    const SteadyTime created_steady_;
    std::stop_source stop_source_;  // internally thread-safe

    mutable std::mutex mutex_;
    JobStatus status_ = JobStatus::Queued;                // guarded by mutex_
    bool cancel_requested_ = false;                       // guarded by mutex_
    std::optional<SystemTime> started_at_;                // guarded by mutex_
    std::optional<SystemTime> finished_at_;               // guarded by mutex_
    std::optional<SteadyTime> started_steady_;            // guarded by mutex_
    std::optional<SteadyTime> finished_steady_;           // guarded by mutex_
    std::shared_ptr<const nlohmann::json> result_;        // guarded by mutex_
    std::optional<JobError> error_;                       // guarded by mutex_
    CompletionCallback on_complete_;                      // guarded by mutex_
};

}  // namespace concurrentcore
