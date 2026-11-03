#pragma once

#include "concurrentcore/concurrency/thread_pool.hpp"
#include "concurrentcore/jobs/job.hpp"
#include "concurrentcore/jobs/job_registry.hpp"
#include "concurrentcore/tasks/tasks.hpp"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>

namespace concurrentcore {

struct JobManagerConfig {
    std::size_t worker_count = ThreadPool::default_worker_count();
    std::size_t queue_capacity = 1024;
    std::size_t max_retained_jobs = 10'000;  ///< Finished jobs kept for queries.
    TaskLimits limits;
};

/// Point-in-time metrics. Individual counters are exact; the snapshot as a
/// whole is not taken atomically (counters are read one by one), which is
/// acceptable for monitoring and avoids a global metrics lock.
struct JobManagerStats {
    std::uint64_t submitted = 0;
    std::uint64_t rejected = 0;  ///< QUEUE_FULL / SERVER_SHUTTING_DOWN
    std::uint64_t completed = 0;
    std::uint64_t failed = 0;
    std::uint64_t cancelled = 0;
    std::uint64_t running = 0;
    std::uint64_t retained_jobs = 0;
    std::size_t queue_depth = 0;
    std::size_t queue_capacity = 0;
    std::size_t worker_count = 0;
    std::size_t active_workers = 0;
    double avg_execution_ms = 0.0;   ///< Over jobs that started and finished.
    double avg_queue_wait_ms = 0.0;  ///< Over jobs that started.
    double uptime_seconds = 0.0;
    double jobs_per_second = 0.0;  ///< Finished jobs / uptime (lifetime average).
    bool accepting = true;
};

struct SubmitResult {
    std::optional<JobId> job_id;         ///< Set on success.
    ErrorCode error = ErrorCode::InternalError;  ///< Meaningful only on failure.
    [[nodiscard]] bool ok() const noexcept { return job_id.has_value(); }
};

/// Owns the job lifecycle: validation-free admission, scheduling onto the
/// thread pool, execution, cancellation, metrics and shutdown.
///
/// Producer-consumer: network threads (producers) call submit(), which
/// inserts the Job into the registry and try-posts a small closure to the
/// bounded pool queue. Worker threads (consumers) pop closures and run them.
/// submit() never blocks on a full queue; it reports QUEUE_FULL so that a
/// network thread is never stalled by back-pressure.
///
/// Locking (see docs/CONCURRENCY.md for the full policy):
///   * state_mutex_    guards accepting_/outstanding_   (leaf)
///   * shutdown_mutex_ serializes shutdown()             (outermost, held
///                     only by the thread calling shutdown)
///   * JobRegistry::mutex_, Job::mutex_, the pool queue's mutex: leaves.
/// No two leaf locks are ever held simultaneously, so lock-order deadlocks
/// are impossible. No lock is held while a task runs or while a completion
/// callback runs.
class JobManager {
public:
    /// Pluggable executor (defaults to execute_task). Tests inject gated
    /// executors to drive jobs through specific states deterministically.
    using Executor = std::function<nlohmann::json(const TaskSpec&, const TaskContext&)>;

    JobManager(JobManagerConfig config, std::shared_ptr<const Workspace> workspace,
               Executor executor = {});
    ~JobManager();

    JobManager(const JobManager&) = delete;
    JobManager& operator=(const JobManager&) = delete;
    JobManager(JobManager&&) = delete;
    JobManager& operator=(JobManager&&) = delete;

    /// Non-blocking admission. On success the job is QUEUED. `on_complete`
    /// (optional) is invoked exactly once, without locks held, on whichever
    /// thread moves the job to a terminal state; it must be thread-safe and
    /// must not block (the server uses it to post a network write).
    [[nodiscard]] SubmitResult submit(TaskSpec spec, Job::CompletionCallback on_complete = {});

    [[nodiscard]] std::optional<JobSnapshot> snapshot(JobId id) const;

    [[nodiscard]] CancelOutcome cancel(JobId id);

    [[nodiscard]] JobManagerStats stats() const;

    /// Blocks until there are no QUEUED/RUNNING jobs or the timeout elapses.
    bool wait_idle(std::chrono::milliseconds timeout) const;

    /// Graceful shutdown with escalation:
    ///   1. stop admitting jobs (submit -> SERVER_SHUTTING_DOWN);
    ///   2. wait up to `drain_timeout` for outstanding jobs to finish;
    ///   3. if any remain, cancel them (queued -> CANCELLED immediately,
    ///      running -> cooperative stop);
    ///   4. shut down the pool and join all workers.
    /// drain_timeout == 0 gives an immediate (cancelling) shutdown.
    /// Idempotent; concurrent callers block until the first one finishes.
    void shutdown(std::chrono::milliseconds drain_timeout);

    [[nodiscard]] const Workspace& workspace() const noexcept { return *workspace_; }
    [[nodiscard]] const TaskLimits& limits() const noexcept { return config_.limits; }

private:
    void run_job(const std::shared_ptr<Job>& job);
    void finalize(const std::shared_ptr<Job>& job);
    void release_outstanding();

    const JobManagerConfig config_;
    const std::shared_ptr<const Workspace> workspace_;
    const Executor executor_;
    const std::chrono::steady_clock::time_point started_at_;

    JobRegistry registry_;
    std::atomic<JobId> next_id_{1};

    // Metrics: independent relaxed atomics. No other data is published
    // through them, so relaxed ordering is sufficient and contention-free
    // apart from cache-line traffic.
    std::atomic<std::uint64_t> submitted_{0};
    std::atomic<std::uint64_t> rejected_{0};
    std::atomic<std::uint64_t> completed_{0};
    std::atomic<std::uint64_t> failed_{0};
    std::atomic<std::uint64_t> cancelled_{0};
    std::atomic<std::uint64_t> running_{0};
    std::atomic<std::uint64_t> exec_ns_total_{0};
    std::atomic<std::uint64_t> exec_count_{0};
    std::atomic<std::uint64_t> wait_ns_total_{0};
    std::atomic<std::uint64_t> wait_count_{0};

    // Admission state. accepting_ and outstanding_ change together under
    // state_mutex_, which closes the "check accepting, then shutdown drains,
    // then increment" race.
    mutable std::mutex state_mutex_;
    mutable std::condition_variable idle_cv_;
    bool accepting_ = true;          // guarded by state_mutex_
    std::uint64_t outstanding_ = 0;  // QUEUED + RUNNING jobs; guarded by state_mutex_

    std::mutex shutdown_mutex_;
    bool shut_down_ = false;  // guarded by shutdown_mutex_

    // Declared last: destroyed first, so workers are joined before any
    // other member they reference goes away.
    ThreadPool pool_;
};

}  // namespace concurrentcore
