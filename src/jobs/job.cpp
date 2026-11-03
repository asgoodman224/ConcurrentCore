#include "concurrentcore/jobs/job.hpp"

#include <utility>

namespace concurrentcore {

std::string_view to_string(JobStatus status) noexcept {
    switch (status) {
        case JobStatus::Queued: return "QUEUED";
        case JobStatus::Running: return "RUNNING";
        case JobStatus::Completed: return "COMPLETED";
        case JobStatus::Failed: return "FAILED";
        case JobStatus::Cancelled: return "CANCELLED";
    }
    return "UNKNOWN";
}

bool is_terminal(JobStatus status) noexcept {
    return status == JobStatus::Completed || status == JobStatus::Failed ||
           status == JobStatus::Cancelled;
}

bool can_transition(JobStatus from, JobStatus to) noexcept {
    switch (from) {
        case JobStatus::Queued:
            return to == JobStatus::Running || to == JobStatus::Cancelled;
        case JobStatus::Running:
            return to == JobStatus::Completed || to == JobStatus::Failed ||
                   to == JobStatus::Cancelled;
        case JobStatus::Completed:
        case JobStatus::Failed:
        case JobStatus::Cancelled:
            return false;
    }
    return false;
}

std::string_view to_string(CancelOutcome outcome) noexcept {
    switch (outcome) {
        case CancelOutcome::Cancelled: return "CANCELLED";
        case CancelOutcome::CancellationRequested: return "CANCELLATION_REQUESTED";
        case CancelOutcome::AlreadyFinished: return "ALREADY_FINISHED";
        case CancelOutcome::NotFound: return "NOT_FOUND";
    }
    return "UNKNOWN";
}

namespace {

std::int64_t to_epoch_ms(SystemTime t) {
    return std::chrono::duration_cast<std::chrono::milliseconds>(t.time_since_epoch()).count();
}

double to_ms(std::chrono::nanoseconds d) {
    return std::chrono::duration<double, std::milli>(d).count();
}

}  // namespace

nlohmann::json to_json(const JobSnapshot& s, bool include_result) {
    nlohmann::json j = {
        {"job_id", s.id},
        {"task_type", to_string(s.type)},
        {"status", to_string(s.status)},
        {"cancel_requested", s.cancel_requested},
        {"created_at_ms", to_epoch_ms(s.created_at)},
        {"started_at_ms", s.started_at ? nlohmann::json(to_epoch_ms(*s.started_at)) : nullptr},
        {"finished_at_ms", s.finished_at ? nlohmann::json(to_epoch_ms(*s.finished_at)) : nullptr},
        {"queue_wait_ms", s.queue_wait ? nlohmann::json(to_ms(*s.queue_wait)) : nullptr},
        {"execution_ms", s.execution_time ? nlohmann::json(to_ms(*s.execution_time)) : nullptr},
    };
    if (include_result) {
        j["result"] = s.result ? *s.result : nlohmann::json(nullptr);
        if (s.error) {
            j["error"] = {{"code", to_string(s.error->code)}, {"message", s.error->message}};
        } else {
            j["error"] = nullptr;
        }
    }
    return j;
}

Job::Job(JobId id, TaskSpec spec, CompletionCallback on_complete)
    : id_(id),
      spec_(std::move(spec)),
      created_at_(std::chrono::system_clock::now()),
      created_steady_(std::chrono::steady_clock::now()),
      on_complete_(std::move(on_complete)) {}

bool Job::transition_locked(JobStatus to) {
    if (!can_transition(status_, to)) {
        return false;
    }
    const auto now_sys = std::chrono::system_clock::now();
    const auto now_steady = std::chrono::steady_clock::now();
    if (to == JobStatus::Running) {
        started_at_ = now_sys;
        started_steady_ = now_steady;
    }
    if (is_terminal(to)) {
        finished_at_ = now_sys;
        finished_steady_ = now_steady;
    }
    status_ = to;
    return true;
}

bool Job::try_start() {
    std::lock_guard lock(mutex_);
    return transition_locked(JobStatus::Running);
}

bool Job::complete(std::shared_ptr<const nlohmann::json> result) {
    std::lock_guard lock(mutex_);
    if (status_ != JobStatus::Running) {
        return false;
    }
    result_ = std::move(result);
    return transition_locked(JobStatus::Completed);
}

bool Job::fail(JobError error) {
    std::lock_guard lock(mutex_);
    if (status_ != JobStatus::Running) {
        return false;
    }
    error_ = std::move(error);
    return transition_locked(JobStatus::Failed);
}

bool Job::finish_cancelled() {
    std::lock_guard lock(mutex_);
    if (status_ != JobStatus::Running) {
        return false;
    }
    error_ = JobError{ErrorCode::Cancelled, "job cancelled while running"};
    return transition_locked(JobStatus::Cancelled);
}

CancelOutcome Job::request_cancel() {
    std::lock_guard lock(mutex_);
    switch (status_) {
        case JobStatus::Queued:
            cancel_requested_ = true;
            error_ = JobError{ErrorCode::Cancelled, "job cancelled before it started"};
            transition_locked(JobStatus::Cancelled);
            // Also trip the token, so any code that already captured it sees
            // a consistent view.
            stop_source_.request_stop();
            return CancelOutcome::Cancelled;
        case JobStatus::Running:
            cancel_requested_ = true;
            // request_stop() is lock-free and does not run stop callbacks
            // that could re-enter this object (we register none), so it is
            // safe to call under mutex_.
            stop_source_.request_stop();
            return CancelOutcome::CancellationRequested;
        case JobStatus::Completed:
        case JobStatus::Failed:
        case JobStatus::Cancelled:
            return CancelOutcome::AlreadyFinished;
    }
    return CancelOutcome::AlreadyFinished;
}

Job::CompletionCallback Job::take_completion_callback() {
    std::lock_guard lock(mutex_);
    return std::exchange(on_complete_, CompletionCallback{});
}

JobStatus Job::status() const {
    std::lock_guard lock(mutex_);
    return status_;
}

JobSnapshot Job::snapshot() const {
    std::lock_guard lock(mutex_);
    JobSnapshot s;
    s.id = id_;
    s.type = spec_.type;
    s.status = status_;
    s.cancel_requested = cancel_requested_;
    s.created_at = created_at_;
    s.started_at = started_at_;
    s.finished_at = finished_at_;
    if (started_steady_) {
        s.queue_wait = *started_steady_ - created_steady_;
    }
    if (started_steady_ && finished_steady_) {
        s.execution_time = *finished_steady_ - *started_steady_;
    }
    s.result = result_;  // shared_ptr copy: O(1), no deep copy
    s.error = error_;
    return s;
}

}  // namespace concurrentcore
