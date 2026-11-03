#include "concurrentcore/concurrency/thread_pool.hpp"

#include <spdlog/spdlog.h>

#include <algorithm>
#include <exception>

namespace concurrentcore {

ThreadPool::ThreadPool(std::size_t worker_count, std::size_t queue_capacity)
    : queue_(queue_capacity) {
    if (worker_count == 0) {
        throw std::invalid_argument("ThreadPool requires at least one worker");
    }
    workers_.reserve(worker_count);
    worker_ids_.reserve(worker_count);
    try {
        for (std::size_t i = 0; i < worker_count; ++i) {
            workers_.emplace_back([this] { worker_loop(); });
            worker_ids_.push_back(workers_.back().get_id());
        }
    } catch (...) {
        // Thread creation failed part-way. Workers already started are
        // blocked in pop(); close the queue so they exit, then join them
        // (jthread joins on destruction) before propagating.
        queue_.close();
        workers_.clear();
        throw;
    }
}

ThreadPool::~ThreadPool() {
    try {
        shutdown(ShutdownMode::Graceful);
    } catch (const std::exception& e) {
        // Destructors must not throw. The only realistic failure is the
        // destructor running on a worker thread, which is a usage bug.
        spdlog::critical("ThreadPool destructor: shutdown failed: {}", e.what());
    }
}

std::size_t ThreadPool::default_worker_count() noexcept {
    const unsigned hw = std::thread::hardware_concurrency();
    return hw == 0 ? 4 : static_cast<std::size_t>(hw);
}

void ThreadPool::post(UniqueTask task) {
    if (queue_.push(std::move(task)) != PushResult::Accepted) {
        throw ThreadPoolStoppedError();
    }
}

PushResult ThreadPool::try_post(UniqueTask&& task) { return queue_.try_push(std::move(task)); }

void ThreadPool::worker_loop() {
    // pop() returns nullopt only once the queue is closed AND empty, so a
    // graceful shutdown naturally drains all accepted work first.
    while (std::optional<UniqueTask> task = queue_.pop()) {
        active_workers_.fetch_add(1, std::memory_order_relaxed);
        try {
            (*task)();
            completed_tasks_.fetch_add(1, std::memory_order_relaxed);
        } catch (const std::exception& e) {
            failed_tasks_.fetch_add(1, std::memory_order_relaxed);
            spdlog::error("thread pool task threw an exception: {}", e.what());
        } catch (...) {
            failed_tasks_.fetch_add(1, std::memory_order_relaxed);
            spdlog::error("thread pool task threw a non-standard exception");
        }
        task.reset();  // destroy captured state before reporting idle
        active_workers_.fetch_sub(1, std::memory_order_relaxed);
    }
}

bool ThreadPool::on_worker_thread() const noexcept {
    const auto self = std::this_thread::get_id();
    return std::find(worker_ids_.begin(), worker_ids_.end(), self) != worker_ids_.end();
}

std::size_t ThreadPool::shutdown(ShutdownMode mode) {
    // Checked before taking the lifecycle lock: a worker blocking on that
    // lock while another thread joins it would deadlock.
    if (on_worker_thread()) {
        throw std::logic_error("ThreadPool::shutdown() called from a worker thread");
    }

    std::lock_guard lifecycle(lifecycle_mutex_);
    if (shut_down_) {
        return 0;
    }

    queue_.close();  // from here on every push is rejected

    std::size_t discarded = 0;
    if (mode == ShutdownMode::Immediate) {
        // Destroying the drained tasks breaks the promise of any submit()
        // future, so waiters observe std::future_errc::broken_promise.
        std::vector<UniqueTask> dropped = queue_.drain();
        discarded = dropped.size();
    }

    for (auto& worker : workers_) {
        if (worker.joinable()) {
            worker.join();
        }
    }
    shut_down_ = true;
    return discarded;
}

ThreadPoolStats ThreadPool::stats() const {
    ThreadPoolStats s;
    s.worker_count = worker_ids_.size();
    s.active_workers = active_workers_.load(std::memory_order_relaxed);
    s.queued_tasks = queue_.size();
    s.queue_capacity = queue_.capacity();
    s.completed_tasks = completed_tasks_.load(std::memory_order_relaxed);
    s.failed_tasks = failed_tasks_.load(std::memory_order_relaxed);
    return s;
}

bool ThreadPool::is_shut_down() const {
    std::lock_guard lifecycle(lifecycle_mutex_);
    return shut_down_;
}

}  // namespace concurrentcore
