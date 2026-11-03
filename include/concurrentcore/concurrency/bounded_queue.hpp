#pragma once

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <utility>
#include <vector>

namespace concurrentcore {

/// Result of a push attempt. On anything other than `Accepted` the caller's
/// item has NOT been moved from, so ownership stays with the caller.
enum class PushResult {
    Accepted,  ///< Item is now owned by the queue.
    Full,      ///< Non-blocking / timed push: no capacity was available.
    Closed,    ///< The queue was closed; no further items are accepted.
};

/// A generic bounded, blocking, multi-producer / multi-consumer FIFO queue.
///
/// Synchronization invariants (all guarded by `mutex_`):
///   I1. `items_.size() <= capacity_` at all times.
///   I2. Once `closed_` becomes true it never becomes false again.
///   I3. After `closed_` is set, no item is ever added to `items_`.
///   I4. Items already in the queue when it is closed remain poppable, so a
///       consumer can drain outstanding work ("graceful" close).
///
/// Wake-up protocol (prevents lost wake-ups):
///   * Every state change that could unblock a waiter happens while holding
///     `mutex_`, and every wait uses a predicate re-checked under `mutex_`.
///     A waiter therefore either observes the new state before sleeping or
///     is already registered on the condition variable when notify happens.
///   * push  -> notify_one(not_empty_)   (exactly one consumer can use the item)
///   * pop   -> notify_one(not_full_)    (exactly one slot was freed)
///   * close -> notify_all(both)          (every waiter must re-evaluate)
///   * drain -> notify_all(not_full_)     (many slots may have been freed)
///   Notifications are issued after the lock is released so that a woken
///   thread does not immediately block on the mutex we still hold.
///
/// Items are moved in and out; T only needs to be move-constructible.
template <typename T>
class BoundedQueue {
public:
    explicit BoundedQueue(std::size_t capacity) : capacity_(capacity) {
        if (capacity_ == 0) {
            throw std::invalid_argument("BoundedQueue capacity must be greater than zero");
        }
    }

    BoundedQueue(const BoundedQueue&) = delete;
    BoundedQueue& operator=(const BoundedQueue&) = delete;
    BoundedQueue(BoundedQueue&&) = delete;
    BoundedQueue& operator=(BoundedQueue&&) = delete;
    ~BoundedQueue() = default;

    /// Blocks until there is space or the queue is closed.
    /// `item` is moved from only when the result is `Accepted`.
    [[nodiscard]] PushResult push(T&& item) {
        {
            std::unique_lock lock(mutex_);
            not_full_.wait(lock, [this] { return closed_ || items_.size() < capacity_; });
            if (closed_) {
                return PushResult::Closed;
            }
            items_.push_back(std::move(item));
        }
        not_empty_.notify_one();
        return PushResult::Accepted;
    }

    /// Never blocks on capacity (it may briefly block acquiring the mutex).
    [[nodiscard]] PushResult try_push(T&& item) {
        {
            std::lock_guard lock(mutex_);
            if (closed_) {
                return PushResult::Closed;
            }
            if (items_.size() >= capacity_) {
                return PushResult::Full;
            }
            items_.push_back(std::move(item));
        }
        not_empty_.notify_one();
        return PushResult::Accepted;
    }

    /// Blocks for at most `timeout` waiting for capacity.
    template <typename Rep, typename Period>
    [[nodiscard]] PushResult push_for(T&& item, std::chrono::duration<Rep, Period> timeout) {
        {
            std::unique_lock lock(mutex_);
            const bool ready = not_full_.wait_for(
                lock, timeout, [this] { return closed_ || items_.size() < capacity_; });
            if (closed_) {
                return PushResult::Closed;
            }
            if (!ready) {
                return PushResult::Full;
            }
            items_.push_back(std::move(item));
        }
        not_empty_.notify_one();
        return PushResult::Accepted;
    }

    /// Blocks until an item is available or the queue is closed AND empty.
    /// Returns std::nullopt only in the latter case, which is the consumer's
    /// signal to exit.
    [[nodiscard]] std::optional<T> pop() {
        std::optional<T> result;
        {
            std::unique_lock lock(mutex_);
            not_empty_.wait(lock, [this] { return closed_ || !items_.empty(); });
            if (items_.empty()) {
                return std::nullopt;  // closed and fully drained
            }
            result.emplace(std::move(items_.front()));
            items_.pop_front();
        }
        not_full_.notify_one();
        return result;
    }

    /// Non-blocking pop.
    [[nodiscard]] std::optional<T> try_pop() {
        std::optional<T> result;
        {
            std::lock_guard lock(mutex_);
            if (items_.empty()) {
                return std::nullopt;
            }
            result.emplace(std::move(items_.front()));
            items_.pop_front();
        }
        not_full_.notify_one();
        return result;
    }

    /// Blocks for at most `timeout` waiting for an item.
    template <typename Rep, typename Period>
    [[nodiscard]] std::optional<T> pop_for(std::chrono::duration<Rep, Period> timeout) {
        std::optional<T> result;
        {
            std::unique_lock lock(mutex_);
            not_empty_.wait_for(lock, timeout, [this] { return closed_ || !items_.empty(); });
            if (items_.empty()) {
                return std::nullopt;
            }
            result.emplace(std::move(items_.front()));
            items_.pop_front();
        }
        not_full_.notify_one();
        return result;
    }

    /// Rejects all future pushes and wakes every blocked producer and
    /// consumer. Items already queued stay available to pop(). Idempotent.
    void close() {
        {
            std::lock_guard lock(mutex_);
            closed_ = true;
        }
        not_empty_.notify_all();
        not_full_.notify_all();
    }

    /// Removes and returns every queued item (FIFO order). Used for
    /// immediate shutdown, where pending work is discarded.
    [[nodiscard]] std::vector<T> drain() {
        std::vector<T> drained;
        {
            std::lock_guard lock(mutex_);
            drained.reserve(items_.size());
            for (auto& item : items_) {
                drained.push_back(std::move(item));
            }
            items_.clear();
        }
        not_full_.notify_all();
        return drained;
    }

    [[nodiscard]] std::size_t size() const {
        std::lock_guard lock(mutex_);
        return items_.size();
    }

    [[nodiscard]] bool empty() const {
        std::lock_guard lock(mutex_);
        return items_.empty();
    }

    [[nodiscard]] bool is_closed() const {
        std::lock_guard lock(mutex_);
        return closed_;
    }

    [[nodiscard]] std::size_t capacity() const noexcept { return capacity_; }

private:
    const std::size_t capacity_;
    mutable std::mutex mutex_;
    std::condition_variable not_empty_;
    std::condition_variable not_full_;
    std::deque<T> items_;  // guarded by mutex_
    bool closed_ = false;  // guarded by mutex_
};

}  // namespace concurrentcore
