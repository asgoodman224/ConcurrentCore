#include "concurrentcore/concurrency/bounded_queue.hpp"

#include "support/test_support.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <latch>
#include <memory>
#include <numeric>
#include <thread>
#include <vector>

using concurrentcore::BoundedQueue;
using concurrentcore::PushResult;
using namespace std::chrono_literals;

TEST(BoundedQueue, RejectsZeroCapacity) {
    EXPECT_THROW(BoundedQueue<int>(0), std::invalid_argument);
}

TEST(BoundedQueue, FifoOrderSingleThread) {
    BoundedQueue<int> q(8);
    for (int i = 0; i < 8; ++i) {
        int v = i;
        ASSERT_EQ(q.try_push(std::move(v)), PushResult::Accepted);
    }
    for (int i = 0; i < 8; ++i) {
        auto v = q.try_pop();
        ASSERT_TRUE(v.has_value());
        EXPECT_EQ(*v, i);
    }
    EXPECT_FALSE(q.try_pop().has_value());
}

TEST(BoundedQueue, CapacityIsEnforcedAndRejectedItemIsNotMoved) {
    BoundedQueue<std::unique_ptr<int>> q(2);
    auto a = std::make_unique<int>(1);
    auto b = std::make_unique<int>(2);
    auto c = std::make_unique<int>(3);
    EXPECT_EQ(q.try_push(std::move(a)), PushResult::Accepted);
    EXPECT_EQ(q.try_push(std::move(b)), PushResult::Accepted);
    EXPECT_EQ(q.size(), 2U);
    EXPECT_EQ(q.try_push(std::move(c)), PushResult::Full);
    // Ownership stays with the caller on failure.
    ASSERT_NE(c, nullptr);
    EXPECT_EQ(*c, 3);
}

TEST(BoundedQueue, PushForTimesOutWhenFull) {
    BoundedQueue<int> q(1);
    int a = 1;
    ASSERT_EQ(q.try_push(std::move(a)), PushResult::Accepted);
    int b = 2;
    EXPECT_EQ(q.push_for(std::move(b), 20ms), PushResult::Full);
}

TEST(BoundedQueue, PopForTimesOutWhenEmpty) {
    BoundedQueue<int> q(1);
    EXPECT_FALSE(q.pop_for(20ms).has_value());
}

TEST(BoundedQueue, BlockedProducerWakesWhenSpaceIsFreed) {
    BoundedQueue<int> q(1);
    int first = 1;
    ASSERT_EQ(q.try_push(std::move(first)), PushResult::Accepted);

    std::atomic<bool> pushed{false};
    std::thread producer([&] {
        int second = 2;
        EXPECT_EQ(q.push(std::move(second)), PushResult::Accepted);
        pushed = true;
    });
    // The queue is full, so the producer cannot have completed.
    std::this_thread::sleep_for(20ms);
    EXPECT_FALSE(pushed.load());

    EXPECT_EQ(q.pop(), 1);  // frees a slot -> must wake the producer
    producer.join();
    EXPECT_TRUE(pushed.load());
    EXPECT_EQ(q.pop(), 2);
}

TEST(BoundedQueue, BlockedConsumerWakesOnPush) {
    BoundedQueue<int> q(4);
    std::optional<int> received;
    std::thread consumer([&] { received = q.pop(); });
    int v = 42;
    ASSERT_EQ(q.push(std::move(v)), PushResult::Accepted);
    consumer.join();
    ASSERT_TRUE(received.has_value());
    EXPECT_EQ(*received, 42);
}

TEST(BoundedQueue, CloseWakesAllBlockedConsumers) {
    BoundedQueue<int> q(4);
    constexpr int kConsumers = 8;
    std::latch started(kConsumers);
    std::atomic<int> got_nullopt{0};
    std::vector<std::thread> consumers;
    for (int i = 0; i < kConsumers; ++i) {
        consumers.emplace_back([&] {
            started.count_down();
            if (!q.pop().has_value()) {
                ++got_nullopt;
            }
        });
    }
    started.wait();
    q.close();  // a lost wake-up would hang the join below (ctest timeout)
    for (auto& t : consumers) {
        t.join();
    }
    EXPECT_EQ(got_nullopt.load(), kConsumers);
}

TEST(BoundedQueue, CloseWakesBlockedProducers) {
    BoundedQueue<int> q(1);
    int fill = 0;
    ASSERT_EQ(q.try_push(std::move(fill)), PushResult::Accepted);
    constexpr int kProducers = 4;
    std::latch started(kProducers);
    std::atomic<int> closed_results{0};
    std::vector<std::thread> producers;
    for (int i = 0; i < kProducers; ++i) {
        producers.emplace_back([&] {
            started.count_down();
            int v = 1;
            if (q.push(std::move(v)) == PushResult::Closed) {
                ++closed_results;
            }
        });
    }
    started.wait();
    q.close();
    for (auto& t : producers) {
        t.join();
    }
    EXPECT_EQ(closed_results.load(), kProducers);
}

TEST(BoundedQueue, ClosedQueueStillDrainsExistingItems) {
    BoundedQueue<int> q(4);
    for (int i = 0; i < 3; ++i) {
        int v = i;
        ASSERT_EQ(q.push(std::move(v)), PushResult::Accepted);
    }
    q.close();
    EXPECT_TRUE(q.is_closed());
    int rejected = 99;
    EXPECT_EQ(q.push(std::move(rejected)), PushResult::Closed);
    EXPECT_EQ(q.try_push(std::move(rejected)), PushResult::Closed);
    EXPECT_EQ(q.pop(), 0);
    EXPECT_EQ(q.pop(), 1);
    EXPECT_EQ(q.pop(), 2);
    EXPECT_FALSE(q.pop().has_value());  // closed and empty
}

TEST(BoundedQueue, DrainRemovesEverythingInOrder) {
    BoundedQueue<int> q(8);
    for (int i = 0; i < 5; ++i) {
        int v = i;
        ASSERT_EQ(q.push(std::move(v)), PushResult::Accepted);
    }
    const auto drained = q.drain();
    EXPECT_EQ(drained, (std::vector<int>{0, 1, 2, 3, 4}));
    EXPECT_TRUE(q.empty());
}

TEST(BoundedQueue, SupportsMoveOnlyTypes) {
    BoundedQueue<std::unique_ptr<std::string>> q(2);
    auto p = std::make_unique<std::string>("payload");
    ASSERT_EQ(q.push(std::move(p)), PushResult::Accepted);
    EXPECT_EQ(p, nullptr);  // moved in on success
    auto out = q.pop();
    ASSERT_TRUE(out.has_value());
    EXPECT_EQ(**out, "payload");
}

TEST(BoundedQueue, MultipleProducersSingleConsumerPreservesPerProducerOrder) {
    BoundedQueue<std::pair<int, int>> q(16);
    constexpr int kProducers = 4;
    constexpr int kPerProducer = 5000;
    std::vector<std::thread> producers;
    for (int p = 0; p < kProducers; ++p) {
        producers.emplace_back([&q, p] {
            for (int i = 0; i < kPerProducer; ++i) {
                auto item = std::make_pair(p, i);
                ASSERT_EQ(q.push(std::move(item)), PushResult::Accepted);
            }
        });
    }
    std::vector<int> last(kProducers, -1);
    for (int n = 0; n < kProducers * kPerProducer; ++n) {
        auto item = q.pop();
        ASSERT_TRUE(item.has_value());
        // FIFO: each producer's items arrive in the order it pushed them.
        EXPECT_EQ(item->second, last[static_cast<std::size_t>(item->first)] + 1);
        last[static_cast<std::size_t>(item->first)] = item->second;
    }
    for (auto& t : producers) {
        t.join();
    }
}

TEST(BoundedQueue, MultipleProducersMultipleConsumersDeliverEachItemExactlyOnce) {
    BoundedQueue<int> q(32);
    constexpr int kProducers = 6;
    constexpr int kConsumers = 6;
    constexpr int kPerProducer = 10'000;
    std::vector<std::vector<int>> received(kConsumers);
    std::vector<std::thread> consumers;
    for (int c = 0; c < kConsumers; ++c) {
        consumers.emplace_back([&q, &received, c] {
            while (auto v = q.pop()) {
                received[static_cast<std::size_t>(c)].push_back(*v);
            }
        });
    }
    std::vector<std::thread> producers;
    for (int p = 0; p < kProducers; ++p) {
        producers.emplace_back([&q, p] {
            for (int i = 0; i < kPerProducer; ++i) {
                int v = p * kPerProducer + i;
                ASSERT_EQ(q.push(std::move(v)), PushResult::Accepted);
            }
        });
    }
    for (auto& t : producers) {
        t.join();
    }
    q.close();
    for (auto& t : consumers) {
        t.join();
    }
    std::vector<int> all;
    for (const auto& r : received) {
        all.insert(all.end(), r.begin(), r.end());
    }
    std::sort(all.begin(), all.end());
    std::vector<int> expected(kProducers * kPerProducer);
    std::iota(expected.begin(), expected.end(), 0);
    EXPECT_EQ(all, expected);  // no loss, no duplication
}
