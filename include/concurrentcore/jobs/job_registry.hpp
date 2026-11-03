#pragma once

#include "concurrentcore/jobs/job.hpp"

#include <cstddef>
#include <deque>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

namespace concurrentcore {

/// Thread-safe map of JobId -> Job with bounded retention of finished jobs.
///
/// Active (QUEUED / RUNNING) jobs are always retained. Once a job reaches a
/// terminal state it is appended to a FIFO; when more than
/// `max_retained_finished` finished jobs exist, the oldest are evicted and
/// subsequent lookups report JOB_NOT_FOUND. Memory is therefore bounded by
/// (active jobs) + max_retained_finished, and active jobs are bounded by the
/// thread pool's queue capacity plus its worker count.
///
/// Locking: `mutex_` is a leaf lock. The registry never calls into a Job
/// while holding it; it only stores and hands out shared_ptrs. Callers
/// receive a shared_ptr and operate on the Job after the registry lock is
/// released, so a Job evicted concurrently stays alive for as long as any
/// caller still holds it (no use-after-free).
class JobRegistry {
public:
    explicit JobRegistry(std::size_t max_retained_finished);

    /// Inserts a new job. Throws std::logic_error on duplicate id.
    void insert(std::shared_ptr<Job> job);

    /// Removes a job unconditionally (used to roll back a failed submit).
    bool erase(JobId id);

    [[nodiscard]] std::shared_ptr<Job> find(JobId id) const;

    /// Records that `id` reached a terminal state and evicts the oldest
    /// finished jobs beyond the retention limit. Returns the number evicted.
    std::size_t mark_finished(JobId id);

    /// Copies out every retained job (for shutdown-time cancellation).
    [[nodiscard]] std::vector<std::shared_ptr<Job>> all() const;

    [[nodiscard]] std::size_t size() const;
    [[nodiscard]] std::size_t finished_count() const;
    [[nodiscard]] std::size_t max_retained_finished() const noexcept { return max_retained_; }

private:
    const std::size_t max_retained_;
    mutable std::mutex mutex_;
    std::unordered_map<JobId, std::shared_ptr<Job>> jobs_;  // guarded by mutex_
    std::deque<JobId> finished_order_;                      // guarded by mutex_
};

}  // namespace concurrentcore
