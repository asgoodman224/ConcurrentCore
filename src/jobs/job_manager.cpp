#include "concurrentcore/jobs/job_manager.hpp"

#include <spdlog/spdlog.h>

#include <exception>
#include <utility>

namespace concurrentcore {
namespace {

std::uint64_t to_ns(std::chrono::nanoseconds d) {
    return d.count() < 0 ? 0 : static_cast<std::uint64_t>(d.count());
}

double avg_ms(std::uint64_t total_ns, std::uint64_t count) {
    return count == 0 ? 0.0 : static_cast<double>(total_ns) / static_cast<double>(count) / 1e6;
}

}  // namespace

JobManager::JobManager(JobManagerConfig config, std::shared_ptr<const Workspace> workspace,
                       Executor executor)
    : config_(std::move(config)),
      workspace_(std::move(workspace)),
      executor_(executor ? std::move(executor) : Executor(&execute_task)),
      started_at_(std::chrono::steady_clock::now()),
      registry_(config_.max_retained_jobs),
      pool_(config_.worker_count, config_.queue_capacity) {
    if (!workspace_) {
        throw std::invalid_argument("JobManager requires a workspace");
    }
}

JobManager::~JobManager() {
    // Default destruction is an immediate (cancelling) shutdown: an owner
    // that wanted a drain calls shutdown() explicitly with a timeout first.
    try {
        shutdown(std::chrono::milliseconds{0});
    } catch (const std::exception& e) {
        spdlog::critical("JobManager destructor: shutdown failed: {}", e.what());
    }
}

SubmitResult JobManager::submit(TaskSpec spec, Job::CompletionCallback on_complete) {
    {
        std::lock_guard lock(state_mutex_);
        if (!accepting_) {
            rejected_.fetch_add(1, std::memory_order_relaxed);
            return SubmitResult{std::nullopt, ErrorCode::ServerShuttingDown};
        }
        ++outstanding_;  // reserved under the same lock that shutdown flips
    }

    const JobId id = next_id_.fetch_add(1, std::memory_order_relaxed);
    auto job = std::make_shared<Job>(id, std::move(spec), std::move(on_complete));
    registry_.insert(job);

    // The closure holds a shared_ptr, so the Job stays alive while queued
    // even if it is evicted from the registry or cancelled meanwhile.
    UniqueTask task([this, job] { run_job(job); });
    const PushResult pushed = pool_.try_post(std::move(task));
    if (pushed != PushResult::Accepted) {
        // Roll back: the job was never visible as runnable work.
        registry_.erase(id);
        release_outstanding();
        rejected_.fetch_add(1, std::memory_order_relaxed);
        return SubmitResult{std::nullopt, pushed == PushResult::Full ? ErrorCode::QueueFull
                                                                     : ErrorCode::ServerShuttingDown};
    }
    submitted_.fetch_add(1, std::memory_order_relaxed);
    return SubmitResult{id, ErrorCode::InternalError};
}

void JobManager::run_job(const std::shared_ptr<Job>& job) {
    // A job cancelled while queued is already CANCELLED (and finalized by
    // the cancelling thread); the worker simply discards it.
    if (!job->try_start()) {
        return;
    }
    running_.fetch_add(1, std::memory_order_relaxed);

    const TaskContext ctx{*workspace_, config_.limits, job->stop_token()};
    // No lock is held while the task executes.
    try {
        nlohmann::json result = executor_(job->spec(), ctx);
        (void)job->complete(std::make_shared<const nlohmann::json>(std::move(result)));
    } catch (const OperationCancelled&) {
        (void)job->finish_cancelled();
    } catch (const CoreError& e) {
        (void)job->fail(JobError{e.code(), e.what()});
    } catch (const std::exception& e) {
        (void)job->fail(JobError{ErrorCode::InternalError, e.what()});
    } catch (...) {
        (void)job->fail(JobError{ErrorCode::InternalError, "unknown exception"});
    }

    running_.fetch_sub(1, std::memory_order_relaxed);
    finalize(job);
}

void JobManager::finalize(const std::shared_ptr<Job>& job) {
    // Called exactly once per job by the thread that performed the terminal
    // transition (worker for RUNNING jobs, canceller for QUEUED jobs).
    const JobSnapshot snap = job->snapshot();
    switch (snap.status) {
        case JobStatus::Completed: completed_.fetch_add(1, std::memory_order_relaxed); break;
        case JobStatus::Failed: failed_.fetch_add(1, std::memory_order_relaxed); break;
        case JobStatus::Cancelled: cancelled_.fetch_add(1, std::memory_order_relaxed); break;
        case JobStatus::Queued:
        case JobStatus::Running:
            spdlog::error("finalize() called on non-terminal job {}", snap.id);
            return;
    }
    if (snap.execution_time) {
        exec_ns_total_.fetch_add(to_ns(*snap.execution_time), std::memory_order_relaxed);
        exec_count_.fetch_add(1, std::memory_order_relaxed);
    }
    if (snap.queue_wait) {
        wait_ns_total_.fetch_add(to_ns(*snap.queue_wait), std::memory_order_relaxed);
        wait_count_.fetch_add(1, std::memory_order_relaxed);
    }

    registry_.mark_finished(snap.id);

    if (auto callback = job->take_completion_callback()) {
        try {
            callback(snap);
        } catch (const std::exception& e) {
            spdlog::error("completion callback for job {} threw: {}", snap.id, e.what());
        }
    }
    release_outstanding();
}

void JobManager::release_outstanding() {
    bool now_idle = false;
    {
        std::lock_guard lock(state_mutex_);
        --outstanding_;
        now_idle = outstanding_ == 0;
    }
    if (now_idle) {
        idle_cv_.notify_all();
    }
}

std::optional<JobSnapshot> JobManager::snapshot(JobId id) const {
    const auto job = registry_.find(id);  // registry lock released on return
    if (!job) {
        return std::nullopt;
    }
    return job->snapshot();
}

CancelOutcome JobManager::cancel(JobId id) {
    const auto job = registry_.find(id);
    if (!job) {
        return CancelOutcome::NotFound;
    }
    const CancelOutcome outcome = job->request_cancel();
    if (outcome == CancelOutcome::Cancelled) {
        // QUEUED -> CANCELLED happened on this thread, so this thread owns
        // finalization. The queued closure will see try_start() fail.
        finalize(job);
    }
    return outcome;
}

bool JobManager::wait_idle(std::chrono::milliseconds timeout) const {
    std::unique_lock lock(state_mutex_);
    return idle_cv_.wait_for(lock, timeout, [this] { return outstanding_ == 0; });
}

JobManagerStats JobManager::stats() const {
    JobManagerStats s;
    s.submitted = submitted_.load(std::memory_order_relaxed);
    s.rejected = rejected_.load(std::memory_order_relaxed);
    s.completed = completed_.load(std::memory_order_relaxed);
    s.failed = failed_.load(std::memory_order_relaxed);
    s.cancelled = cancelled_.load(std::memory_order_relaxed);
    s.running = running_.load(std::memory_order_relaxed);
    s.retained_jobs = registry_.size();

    const ThreadPoolStats pool = pool_.stats();
    s.queue_depth = pool.queued_tasks;
    s.queue_capacity = pool.queue_capacity;
    s.worker_count = pool.worker_count;
    s.active_workers = pool.active_workers;

    s.avg_execution_ms = avg_ms(exec_ns_total_.load(std::memory_order_relaxed),
                                exec_count_.load(std::memory_order_relaxed));
    s.avg_queue_wait_ms = avg_ms(wait_ns_total_.load(std::memory_order_relaxed),
                                 wait_count_.load(std::memory_order_relaxed));
    s.uptime_seconds =
        std::chrono::duration<double>(std::chrono::steady_clock::now() - started_at_).count();
    const auto finished = s.completed + s.failed + s.cancelled;
    s.jobs_per_second =
        s.uptime_seconds > 0.0 ? static_cast<double>(finished) / s.uptime_seconds : 0.0;
    {
        std::lock_guard lock(state_mutex_);
        s.accepting = accepting_;
    }
    return s;
}

void JobManager::shutdown(std::chrono::milliseconds drain_timeout) {
    std::lock_guard serialize(shutdown_mutex_);
    if (shut_down_) {
        return;
    }

    bool drained = false;
    {
        std::unique_lock lock(state_mutex_);
        accepting_ = false;  // from now on submit() fails fast
        drained = idle_cv_.wait_for(lock, drain_timeout, [this] { return outstanding_ == 0; });
    }

    if (!drained) {
        // Escalate: cancel everything still pending. Snapshot the job list
        // first; cancel() runs with no registry lock held.
        std::size_t cancelled_queued = 0;
        std::size_t stop_requested = 0;
        for (const auto& job : registry_.all()) {
            const CancelOutcome outcome = job->request_cancel();
            if (outcome == CancelOutcome::Cancelled) {
                finalize(job);
                ++cancelled_queued;
            } else if (outcome == CancelOutcome::CancellationRequested) {
                ++stop_requested;
            }
        }
        if (cancelled_queued + stop_requested > 0) {
            spdlog::info("shutdown: cancelled {} queued job(s), requested stop for {} running job(s)",
                         cancelled_queued, stop_requested);
        }
    }

    // Graceful pool shutdown: remaining queue entries are cancelled jobs
    // (discarded instantly by run_job) and running tasks stop at their next
    // cancellation point.
    pool_.shutdown(ShutdownMode::Graceful);
    shut_down_ = true;
}

}  // namespace concurrentcore
