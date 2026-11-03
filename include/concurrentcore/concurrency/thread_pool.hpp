#pragma once

#include "concurrentcore/concurrency/bounded_queue.hpp"
#include "concurrentcore/concurrency/unique_task.hpp"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <future>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <type_traits>
#include <vector>

namespace concurrentcore {

enum class ShutdownMode {
    Graceful,   ///< Stop accepting work, finish every task already queued.
    Immediate,  ///< Stop accepting work, discard queued tasks, finish running ones.
};

/// Thrown when work is submitted to a pool that has been shut down.
class ThreadPoolStoppedError : public std::runtime_error {
public:
    ThreadPoolStoppedError() : std::runtime_error("thread pool is shut down") {}
};

struct ThreadPoolStats {
    std::size_t worker_count = 0;
    std::size_t active_workers = 0;  ///< Workers currently executing a task.
    std::size_t queued_tasks = 0;
    std::size_t queue_capacity = 0;
    std::uint64_t completed_tasks = 0;  ///< Tasks that returned normally.
    std::uint64_t failed_tasks = 0;     ///< Fire-and-forget tasks that threw.
};

/// Fixed-size pool of worker threads consuming a bounded FIFO task queue.
///
/// Lifecycle:
///   * All workers are created in the constructor and joined in shutdown();
///     threads are never detached and never created on demand.
///   * Idle workers block inside BoundedQueue::pop() on a condition variable
///     (no busy-waiting) and are woken one-per-task by push().
///   * shutdown() closes the queue first. Because close() and push() are
///     serialized by the queue mutex, a concurrent submission either lands in
///     the queue before the close (and is then run or discarded according to
///     the shutdown mode) or is rejected with ThreadPoolStoppedError. There is
///     no window in which a task is accepted and silently lost.
///   * No lock is held while a task runs.
///
/// Thread-safety: every public member function may be called concurrently.
/// shutdown() must not be called from one of the pool's own workers (it
/// would join itself); doing so throws std::logic_error.
class ThreadPool {
public:
    static constexpr std::size_t kDefaultQueueCapacity = 1024;

    explicit ThreadPool(std::size_t worker_count = default_worker_count(),
                        std::size_t queue_capacity = kDefaultQueueCapacity);
    ~ThreadPool();

    ThreadPool(const ThreadPool&) = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;
    ThreadPool(ThreadPool&&) = delete;
    ThreadPool& operator=(ThreadPool&&) = delete;

    /// Fire-and-forget submission. Blocks while the queue is full.
    /// Throws ThreadPoolStoppedError after shutdown. Exceptions escaping the
    /// task are caught by the worker, logged and counted in `failed_tasks`.
    /// Avoid calling this from a worker thread while the queue may be full;
    /// use try_post() there to avoid self-deadlock.
    void post(UniqueTask task);

    /// Non-blocking submission. `task` is moved from only on Accepted.
    [[nodiscard]] PushResult try_post(UniqueTask&& task);

    /// Submits a callable and returns a future for its result. Exceptions
    /// thrown by the callable are delivered through the future. If the task
    /// is discarded by an immediate shutdown the future reports
    /// std::future_errc::broken_promise.
    template <typename F, typename R = std::invoke_result_t<std::decay_t<F>&>>
    [[nodiscard]] std::future<R> submit(F&& fn) {
        std::packaged_task<R()> task(std::forward<F>(fn));
        std::future<R> result = task.get_future();
        post(UniqueTask(std::move(task)));
        return result;
    }

    /// Stops the pool and joins every worker. Returns the number of queued
    /// tasks that were discarded (always 0 for Graceful). Idempotent and safe
    /// to call concurrently; later calls return 0.
    std::size_t shutdown(ShutdownMode mode = ShutdownMode::Graceful);

    [[nodiscard]] ThreadPoolStats stats() const;
    [[nodiscard]] std::size_t worker_count() const noexcept { return worker_ids_.size(); }
    [[nodiscard]] bool is_shut_down() const;

    [[nodiscard]] static std::size_t default_worker_count() noexcept;

private:
    void worker_loop();
    [[nodiscard]] bool on_worker_thread() const noexcept;

    // Declaration order matters: the queue must outlive the workers.
    BoundedQueue<UniqueTask> queue_;

    std::atomic<std::size_t> active_workers_{0};
    std::atomic<std::uint64_t> completed_tasks_{0};
    std::atomic<std::uint64_t> failed_tasks_{0};

    // Serializes shutdown(); never acquired by worker threads, so holding it
    // while joining workers cannot deadlock.
    mutable std::mutex lifecycle_mutex_;
    bool shut_down_ = false;  // guarded by lifecycle_mutex_

    // Written only by the constructor, read-only afterwards.
    std::vector<std::thread::id> worker_ids_;
    std::vector<std::jthread> workers_;  // joined (only) under lifecycle_mutex_
};

}  // namespace concurrentcore
