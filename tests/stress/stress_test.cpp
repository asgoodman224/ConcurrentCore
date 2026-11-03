// Stress tests: high contention, large task counts, cancellation under load
// and repeated lifecycle churn. Intended to be run under sanitizers too.

#include "concurrentcore/client/client.hpp"
#include "concurrentcore/concurrency/bounded_queue.hpp"
#include "concurrentcore/concurrency/thread_pool.hpp"
#include "concurrentcore/jobs/job_manager.hpp"
#include "concurrentcore/server/tcp_server.hpp"

#include "support/test_support.hpp"

#include <gtest/gtest.h>
#include <spdlog/spdlog.h>

#include <atomic>
#include <random>
#include <thread>
#include <vector>

using namespace concurrentcore;
using namespace std::chrono_literals;

namespace {

TaskSpec compute(std::uint64_t limit) { return TaskSpec{TaskType::Compute, ComputeParams{limit}}; }

class StressTest : public ::testing::Test {
protected:
    void SetUp() override { spdlog::set_level(spdlog::level::warn); }
    cctest::TempDir dir_;
};

}  // namespace

TEST_F(StressTest, QueueUnderHeavyContention) {
    BoundedQueue<std::uint64_t> q(8);  // tiny capacity: producers block constantly
    constexpr int kProducers = 12;
    constexpr int kConsumers = 12;
    constexpr std::uint64_t kPerProducer = 20'000;
    std::atomic<std::uint64_t> sum{0};
    std::atomic<std::uint64_t> count{0};
    std::vector<std::thread> threads;
    for (int c = 0; c < kConsumers; ++c) {
        threads.emplace_back([&] {
            while (auto v = q.pop()) {
                sum += *v;
                ++count;
            }
        });
    }
    std::vector<std::thread> producers;
    for (int p = 0; p < kProducers; ++p) {
        producers.emplace_back([&q] {
            for (std::uint64_t i = 1; i <= kPerProducer; ++i) {
                std::uint64_t v = i;
                ASSERT_EQ(q.push(std::move(v)), PushResult::Accepted);
            }
        });
    }
    for (auto& t : producers) {
        t.join();
    }
    q.close();
    for (auto& t : threads) {
        t.join();
    }
    EXPECT_EQ(count.load(), kProducers * kPerProducer);
    EXPECT_EQ(sum.load(), kProducers * (kPerProducer * (kPerProducer + 1) / 2));
}

TEST_F(StressTest, ThreadPoolRunsHundredsOfThousandsOfTinyTasks) {
    ThreadPool pool(8, 128);
    std::atomic<std::uint64_t> executed{0};
    constexpr int kProducers = 8;
    constexpr int kPerProducer = 25'000;
    std::vector<std::thread> producers;
    for (int p = 0; p < kProducers; ++p) {
        producers.emplace_back([&] {
            for (int i = 0; i < kPerProducer; ++i) {
                pool.post([&executed] { executed.fetch_add(1, std::memory_order_relaxed); });
            }
        });
    }
    for (auto& t : producers) {
        t.join();
    }
    pool.shutdown(ShutdownMode::Graceful);
    EXPECT_EQ(executed.load(), static_cast<std::uint64_t>(kProducers * kPerProducer));
}

TEST_F(StressTest, JobManagerConcurrentProducersWithBackPressure) {
    JobManagerConfig cfg;
    cfg.worker_count = 8;
    cfg.queue_capacity = 64;  // small: forces QUEUE_FULL retries
    cfg.max_retained_jobs = 100'000;
    JobManager mgr(cfg, std::make_shared<const Workspace>(dir_.path()));
    constexpr int kProducers = 8;
    constexpr int kPerProducer = 500;
    std::atomic<std::uint64_t> queue_full_retries{0};
    std::vector<std::thread> producers;
    for (int p = 0; p < kProducers; ++p) {
        producers.emplace_back([&] {
            for (int i = 0; i < kPerProducer; ++i) {
                for (;;) {
                    const auto r = mgr.submit(compute(2000));
                    if (r.ok()) {
                        break;
                    }
                    ASSERT_EQ(r.error, ErrorCode::QueueFull);
                    ++queue_full_retries;
                    std::this_thread::yield();
                }
            }
        });
    }
    for (auto& t : producers) {
        t.join();
    }
    ASSERT_TRUE(mgr.wait_idle(60s));
    const auto s = mgr.stats();
    EXPECT_EQ(s.completed, static_cast<std::uint64_t>(kProducers * kPerProducer));
    EXPECT_EQ(s.submitted, s.completed);
    EXPECT_EQ(s.failed, 0U);
    EXPECT_EQ(s.rejected, queue_full_retries.load());
}

TEST_F(StressTest, CancellationUnderLoad) {
    JobManagerConfig cfg;
    cfg.worker_count = 6;
    cfg.queue_capacity = 5000;
    cfg.max_retained_jobs = 100'000;
    JobManager mgr(cfg, std::make_shared<const Workspace>(dir_.path()));

    constexpr int kJobs = 3000;
    std::atomic<int> callbacks{0};
    std::vector<JobId> ids;
    ids.reserve(kJobs);
    for (int i = 0; i < kJobs; ++i) {
        const auto r = mgr.submit(compute(30'000), [&](const JobSnapshot& s) {
            EXPECT_TRUE(is_terminal(s.status));
            callbacks.fetch_add(1);
        });
        ASSERT_TRUE(r.ok());
        ids.push_back(*r.job_id);
    }
    // Four threads cancel overlapping random subsets while workers run.
    std::vector<std::thread> cancellers;
    for (int t = 0; t < 4; ++t) {
        cancellers.emplace_back([&, t] {
            std::mt19937 rng(static_cast<unsigned>(t + 1));
            std::uniform_int_distribution<std::size_t> pick(0, ids.size() - 1);
            for (int i = 0; i < 1500; ++i) {
                (void)mgr.cancel(ids[pick(rng)]);
            }
        });
    }
    for (auto& t : cancellers) {
        t.join();
    }
    ASSERT_TRUE(mgr.wait_idle(60s));

    const auto s = mgr.stats();
    EXPECT_EQ(s.completed + s.cancelled, static_cast<std::uint64_t>(kJobs));
    EXPECT_GT(s.cancelled, 0U);
    EXPECT_EQ(callbacks.load(), kJobs);  // exactly once per job
    for (const auto id : ids) {
        const auto snap = mgr.snapshot(id);
        ASSERT_TRUE(snap.has_value());
        ASSERT_TRUE(is_terminal(snap->status));
        if (snap->status == JobStatus::Completed) {
            ASSERT_NE(snap->result, nullptr);
        }
    }
}

TEST_F(StressTest, RepeatedThreadPoolStartupAndShutdown) {
    for (int i = 0; i < 200; ++i) {
        ThreadPool pool(4, 16);
        std::atomic<int> n{0};
        for (int j = 0; j < 20; ++j) {
            pool.post([&n] { ++n; });
        }
        if (i % 2 == 0) {
            pool.shutdown(ShutdownMode::Graceful);
            ASSERT_EQ(n.load(), 20);
        } else {
            const auto discarded = pool.shutdown(ShutdownMode::Immediate);
            ASSERT_EQ(static_cast<std::size_t>(n.load()) + discarded, 20U);
        }
    }
}

TEST_F(StressTest, RepeatedServerStartupAndShutdown) {
    for (int i = 0; i < 25; ++i) {
        ServerConfig c;
        c.port = 0;
        c.worker_threads = 2;
        c.io_threads = 2;
        c.workspace = dir_.path();
        Server server(c);
        server.start();
        Client client;
        client.connect("127.0.0.1", server.port(), 5s);
        ASSERT_TRUE(client.ping()["ok"]);
        ASSERT_TRUE(client.submit("compute", {{"limit", 50'000}}, true)["ok"]);
        if (i % 2 == 0) {
            ASSERT_TRUE(client.wait_for_event(10s).has_value());
        }
        server.shutdown(i % 3 == 0 ? 0ms : 5000ms);
    }
}

TEST_F(StressTest, ManyClientsHammerTheServer) {
    ServerConfig c;
    c.port = 0;
    c.worker_threads = 8;
    c.io_threads = 4;
    c.queue_capacity = 4096;
    c.workspace = dir_.path();
    Server server(c);
    server.start();

    constexpr int kClients = 32;
    constexpr int kJobs = 40;
    std::atomic<int> finished{0};
    std::vector<std::thread> threads;
    for (int t = 0; t < kClients; ++t) {
        threads.emplace_back([&, t] {
            Client client;
            client.connect("127.0.0.1", server.port(), 10s);
            for (int j = 0; j < kJobs; ++j) {
                const auto r = client.submit("text-analysis",
                                             {{"text", "client " + std::to_string(t) + " job " + std::to_string(j)}},
                                             true, 30s);
                ASSERT_TRUE(r["ok"]) << r.dump();
                // Interleave queries to exercise concurrent reads.
                (void)client.status(r["result"]["job_id"].get<std::uint64_t>(), 30s);
            }
            for (int j = 0; j < kJobs; ++j) {
                const auto e = client.wait_for_event(60s);
                ASSERT_TRUE(e.has_value());
                ASSERT_EQ((*e)["data"]["status"], "COMPLETED");
                ASSERT_EQ((*e)["data"]["result"]["words"], 4);
                ++finished;
            }
        });
    }
    for (auto& t : threads) {
        t.join();
    }
    EXPECT_EQ(finished.load(), kClients * kJobs);
    server.shutdown(5s);
}
