#include "concurrentcore/concurrency/thread_pool.hpp"

#include "support/test_support.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <future>
#include <latch>
#include <stdexcept>
#include <thread>
#include <vector>

using concurrentcore::PushResult;
using concurrentcore::ShutdownMode;
using concurrentcore::ThreadPool;
using concurrentcore::ThreadPoolStoppedError;
using concurrentcore::UniqueTask;
using namespace std::chrono_literals;

TEST(ThreadPool, RejectsZeroWorkers) { EXPECT_THROW(ThreadPool(0, 8), std::invalid_argument); }

TEST(ThreadPool, DefaultWorkerCountIsPositive) { EXPECT_GT(ThreadPool::default_worker_count(), 0U); }

TEST(ThreadPool, SubmitReturnsResultThroughFuture) {
    ThreadPool pool(2, 16);
    auto f = pool.submit([] { return 6 * 7; });
    EXPECT_EQ(f.get(), 42);
}

TEST(ThreadPool, SubmitAcceptsMoveOnlyCallables) {
    ThreadPool pool(1, 4);
    auto data = std::make_unique<int>(5);
    auto f = pool.submit([d = std::move(data)] { return *d * 2; });
    EXPECT_EQ(f.get(), 10);
}

TEST(ThreadPool, ExecutesTasksConcurrently) {
    constexpr std::size_t kWorkers = 4;
    ThreadPool pool(kWorkers, 16);
    // Every task waits until all kWorkers tasks are running at once. This
    // can only complete if the pool really runs them in parallel.
    std::latch all_running(kWorkers);
    std::vector<std::future<bool>> futures;
    for (std::size_t i = 0; i < kWorkers; ++i) {
        futures.push_back(pool.submit([&all_running] {
            all_running.count_down();
            return cctest::wait_until([&] { return all_running.try_wait(); }, 10s);
        }));
    }
    for (auto& f : futures) {
        EXPECT_TRUE(f.get());
    }
}

TEST(ThreadPool, ExceptionPropagatesThroughFuture) {
    ThreadPool pool(1, 4);
    auto f = pool.submit([]() -> int { throw std::runtime_error("boom"); });
    EXPECT_THROW(
        {
            try {
                (void)f.get();
            } catch (const std::runtime_error& e) {
                EXPECT_STREQ(e.what(), "boom");
                throw;
            }
        },
        std::runtime_error);
}

TEST(ThreadPool, ThrowingPostedTaskDoesNotKillWorker) {
    ThreadPool pool(1, 4);
    pool.post([] { throw std::runtime_error("fire-and-forget failure"); });
    pool.post([] { throw 17; });  // non-std exception
    // The single worker must survive both to run this one.
    EXPECT_EQ(pool.submit([] { return 1; }).get(), 1);
    EXPECT_EQ(pool.stats().failed_tasks, 2U);
}

TEST(ThreadPool, TryPostReportsFullWithoutConsumingTask) {
    // Declared before the pool so it outlives the workers that wait on it
    // (the pool joins its workers when destroyed).
    cctest::Gate gate;
    ThreadPool pool(1, 1);
    std::promise<void> running;
    pool.post([&] {
        running.set_value();
        gate.wait();
    });
    running.get_future().wait();  // worker is now busy

    UniqueTask fill([] {});
    ASSERT_EQ(pool.try_post(std::move(fill)), PushResult::Accepted);
    UniqueTask extra([] {});
    EXPECT_EQ(pool.try_post(std::move(extra)), PushResult::Full);
    EXPECT_TRUE(static_cast<bool>(extra));  // not moved from
    gate.open();
}

TEST(ThreadPool, StatsReportActiveWorkers) {
    // Declared before the pool so it outlives the workers that wait on it
    // (the pool joins its workers when destroyed).
    cctest::Gate gate;
    ThreadPool pool(3, 8);
    std::latch started(2);
    for (int i = 0; i < 2; ++i) {
        pool.post([&] {
            started.count_down();
            gate.wait();
        });
    }
    started.wait();
    EXPECT_EQ(pool.stats().active_workers, 2U);
    EXPECT_EQ(pool.stats().worker_count, 3U);
    gate.open();
    pool.shutdown();
    EXPECT_EQ(pool.stats().active_workers, 0U);
    EXPECT_EQ(pool.stats().completed_tasks, 2U);
}

TEST(ThreadPool, GracefulShutdownRunsEveryQueuedTask) {
    // Declared before the pool so it outlives the workers that wait on it
    // (the pool joins its workers when destroyed).
    cctest::Gate gate;
    ThreadPool pool(1, 256);
    std::promise<void> running;
    pool.post([&] {
        running.set_value();
        gate.wait();
    });
    running.get_future().wait();

    std::atomic<int> executed{0};
    for (int i = 0; i < 100; ++i) {
        pool.post([&] { ++executed; });
    }
    std::thread stopper([&] { EXPECT_EQ(pool.shutdown(ShutdownMode::Graceful), 0U); });
    gate.open();
    stopper.join();
    EXPECT_EQ(executed.load(), 100);
    EXPECT_TRUE(pool.is_shut_down());
}

TEST(ThreadPool, ImmediateShutdownDiscardsQueuedTasks) {
    // Declared before the pool so it outlives the workers that wait on it
    // (the pool joins its workers when destroyed).
    cctest::Gate gate;
    ThreadPool pool(1, 64);
    std::promise<void> running;
    pool.post([&] {
        running.set_value();
        gate.wait();
    });
    running.get_future().wait();

    std::atomic<int> executed{0};
    std::vector<std::future<void>> futures;
    for (int i = 0; i < 10; ++i) {
        futures.push_back(pool.submit([&] { ++executed; }));
    }
    std::size_t discarded = 0;
    std::thread stopper([&] { discarded = pool.shutdown(ShutdownMode::Immediate); });
    // The only worker is blocked, so the queue can only empty via drain().
    ASSERT_TRUE(cctest::wait_until([&] { return pool.stats().queued_tasks == 0; }));
    gate.open();
    stopper.join();

    EXPECT_EQ(discarded, 10U);
    EXPECT_EQ(executed.load(), 0);
    for (auto& f : futures) {
        try {
            f.get();
            ADD_FAILURE() << "discarded task's future should report broken_promise";
        } catch (const std::future_error& e) {
            EXPECT_EQ(e.code(), std::make_error_code(std::future_errc::broken_promise));
        }
    }
}

TEST(ThreadPool, SubmissionAfterShutdownIsRejected) {
    ThreadPool pool(2, 8);
    pool.shutdown();
    EXPECT_THROW(pool.post([] {}), ThreadPoolStoppedError);
    EXPECT_THROW((void)pool.submit([] { return 1; }), ThreadPoolStoppedError);
    UniqueTask t([] {});
    EXPECT_EQ(pool.try_post(std::move(t)), PushResult::Closed);
}

TEST(ThreadPool, ShutdownIsIdempotentAndSafeToCallConcurrently) {
    ThreadPool pool(4, 64);
    for (int i = 0; i < 50; ++i) {
        pool.post([] {});
    }
    std::vector<std::thread> stoppers;
    for (int i = 0; i < 4; ++i) {
        stoppers.emplace_back([&] { pool.shutdown(); });
    }
    for (auto& t : stoppers) {
        t.join();
    }
    EXPECT_EQ(pool.shutdown(), 0U);
    EXPECT_EQ(pool.stats().completed_tasks, 50U);
}

TEST(ThreadPool, ShutdownFromWorkerThreadIsRejected) {
    ThreadPool pool(1, 4);
    auto f = pool.submit([&pool] { pool.shutdown(); });
    EXPECT_THROW(f.get(), std::logic_error);
}

TEST(ThreadPool, ConcurrentSubmitAndShutdownLosesNoAcceptedTask) {
    // Producers race against shutdown. Every post() that returns normally
    // must be executed (graceful mode); every rejected one must throw.
    ThreadPool pool(4, 64);
    std::atomic<int> accepted{0};
    std::atomic<int> executed{0};
    std::atomic<bool> stop_producing{false};
    std::latch producers_started(4);
    std::vector<std::thread> producers;
    for (int p = 0; p < 4; ++p) {
        producers.emplace_back([&] {
            producers_started.count_down();
            while (!stop_producing.load()) {
                try {
                    pool.post([&] { ++executed; });
                    ++accepted;
                } catch (const ThreadPoolStoppedError&) {
                    return;
                }
            }
        });
    }
    producers_started.wait();
    ASSERT_TRUE(cctest::wait_until([&] { return accepted.load() > 1000; }));
    pool.shutdown(ShutdownMode::Graceful);
    stop_producing = true;
    for (auto& t : producers) {
        t.join();
    }
    EXPECT_EQ(executed.load(), accepted.load());
}
