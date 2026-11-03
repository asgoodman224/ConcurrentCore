#include "concurrentcore/jobs/job.hpp"
#include "concurrentcore/jobs/job_registry.hpp"

#include "support/test_support.hpp"

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <latch>
#include <thread>
#include <vector>

using namespace concurrentcore;

namespace {

TaskSpec compute_spec(std::uint64_t limit = 10) {
    return TaskSpec{TaskType::Compute, ComputeParams{limit}};
}

std::shared_ptr<const nlohmann::json> result_of(int v) {
    return std::make_shared<const nlohmann::json>(nlohmann::json{{"v", v}});
}

}  // namespace

TEST(JobStateMachine, TransitionTableIsExactlyTheDocumentedOne) {
    constexpr std::array kAll{JobStatus::Queued, JobStatus::Running, JobStatus::Completed,
                              JobStatus::Failed, JobStatus::Cancelled};
    for (const auto from : kAll) {
        for (const auto to : kAll) {
            const bool expected =
                (from == JobStatus::Queued && (to == JobStatus::Running || to == JobStatus::Cancelled)) ||
                (from == JobStatus::Running &&
                 (to == JobStatus::Completed || to == JobStatus::Failed || to == JobStatus::Cancelled));
            EXPECT_EQ(can_transition(from, to), expected)
                << to_string(from) << " -> " << to_string(to);
        }
    }
}

TEST(Job, HappyPathSetsTimestampsAndResult) {
    Job job(1, compute_spec());
    auto s = job.snapshot();
    EXPECT_EQ(s.status, JobStatus::Queued);
    EXPECT_FALSE(s.started_at.has_value());

    ASSERT_TRUE(job.try_start());
    s = job.snapshot();
    EXPECT_EQ(s.status, JobStatus::Running);
    EXPECT_TRUE(s.started_at.has_value());
    EXPECT_TRUE(s.queue_wait.has_value());
    EXPECT_FALSE(s.finished_at.has_value());

    ASSERT_TRUE(job.complete(result_of(7)));
    s = job.snapshot();
    EXPECT_EQ(s.status, JobStatus::Completed);
    ASSERT_TRUE(s.finished_at.has_value());
    ASSERT_TRUE(s.execution_time.has_value());
    EXPECT_GE(s.execution_time->count(), 0);
    ASSERT_NE(s.result, nullptr);
    EXPECT_EQ((*s.result)["v"], 7);
}

TEST(Job, IllegalTransitionsAreRejected) {
    Job job(1, compute_spec());
    EXPECT_FALSE(job.complete(result_of(1)));  // QUEUED -> COMPLETED
    EXPECT_FALSE(job.fail({ErrorCode::IoError, "x"}));
    EXPECT_FALSE(job.finish_cancelled());
    ASSERT_TRUE(job.try_start());
    EXPECT_FALSE(job.try_start());  // RUNNING -> RUNNING
    ASSERT_TRUE(job.fail({ErrorCode::IoError, "disk"}));
    EXPECT_FALSE(job.complete(result_of(1)));  // terminal is final
    EXPECT_FALSE(job.finish_cancelled());
    const auto s = job.snapshot();
    EXPECT_EQ(s.status, JobStatus::Failed);
    ASSERT_TRUE(s.error.has_value());
    EXPECT_EQ(s.error->code, ErrorCode::IoError);
}

TEST(Job, CancelQueuedJobIsImmediate) {
    Job job(1, compute_spec());
    EXPECT_EQ(job.request_cancel(), CancelOutcome::Cancelled);
    EXPECT_EQ(job.status(), JobStatus::Cancelled);
    EXPECT_FALSE(job.try_start());  // the worker will skip it
    EXPECT_TRUE(job.stop_token().stop_requested());
    EXPECT_EQ(job.request_cancel(), CancelOutcome::AlreadyFinished);
}

TEST(Job, CancelRunningJobIsCooperative) {
    Job job(1, compute_spec());
    ASSERT_TRUE(job.try_start());
    const auto token = job.stop_token();
    EXPECT_FALSE(token.stop_requested());
    EXPECT_EQ(job.request_cancel(), CancelOutcome::CancellationRequested);
    EXPECT_TRUE(token.stop_requested());
    EXPECT_EQ(job.status(), JobStatus::Running);  // until the task reacts
    EXPECT_TRUE(job.snapshot().cancel_requested);
    ASSERT_TRUE(job.finish_cancelled());
    EXPECT_EQ(job.status(), JobStatus::Cancelled);
}

TEST(Job, TaskThatFinishesDespiteCancelRequestIsCompleted) {
    Job job(1, compute_spec());
    ASSERT_TRUE(job.try_start());
    EXPECT_EQ(job.request_cancel(), CancelOutcome::CancellationRequested);
    EXPECT_TRUE(job.complete(result_of(1)));  // finished before observing the stop
    EXPECT_EQ(job.status(), JobStatus::Completed);
}

TEST(Job, CompletionCallbackIsTakenExactlyOnce) {
    int calls = 0;
    Job job(1, compute_spec(), [&](const JobSnapshot&) { ++calls; });
    auto cb = job.take_completion_callback();
    ASSERT_TRUE(static_cast<bool>(cb));
    EXPECT_FALSE(static_cast<bool>(job.take_completion_callback()));
    cb(job.snapshot());
    EXPECT_EQ(calls, 1);
}

TEST(Job, ToJsonContainsDocumentedFields) {
    Job job(9, compute_spec());
    ASSERT_TRUE(job.try_start());
    ASSERT_TRUE(job.complete(result_of(3)));
    const auto j = to_json(job.snapshot(), true);
    EXPECT_EQ(j["job_id"], 9);
    EXPECT_EQ(j["task_type"], "compute");
    EXPECT_EQ(j["status"], "COMPLETED");
    EXPECT_TRUE(j["created_at_ms"].is_number());
    EXPECT_TRUE(j["started_at_ms"].is_number());
    EXPECT_TRUE(j["finished_at_ms"].is_number());
    EXPECT_TRUE(j["execution_ms"].is_number());
    EXPECT_EQ(j["result"]["v"], 3);
    EXPECT_TRUE(j["error"].is_null());
    const auto brief = to_json(job.snapshot(), false);
    EXPECT_FALSE(brief.contains("result"));
}

TEST(Job, SnapshotsAreConsistentUnderConcurrentTransitions) {
    // Readers hammer snapshot() while the job moves through its lifecycle;
    // every snapshot must satisfy the state invariants.
    for (int iteration = 0; iteration < 200; ++iteration) {
        Job job(1, compute_spec());
        std::atomic<bool> done{false};
        std::atomic<int> violations{0};
        std::vector<std::thread> readers;
        for (int r = 0; r < 3; ++r) {
            readers.emplace_back([&] {
                while (!done.load()) {
                    const auto s = job.snapshot();
                    const bool ok =
                        (s.status == JobStatus::Queued && !s.started_at && !s.finished_at) ||
                        (s.status == JobStatus::Running && s.started_at && !s.finished_at) ||
                        (s.status == JobStatus::Completed && s.started_at && s.finished_at &&
                         s.result != nullptr);
                    if (!ok) {
                        ++violations;
                    }
                }
            });
        }
        ASSERT_TRUE(job.try_start());
        ASSERT_TRUE(job.complete(result_of(iteration)));
        done = true;
        for (auto& t : readers) {
            t.join();
        }
        ASSERT_EQ(violations.load(), 0);
    }
}

TEST(Job, CancelRacingWithStartHasExactlyOneWinner) {
    for (int iteration = 0; iteration < 2000; ++iteration) {
        Job job(1, compute_spec());
        std::latch go(2);
        bool started = false;
        CancelOutcome outcome{};
        std::thread starter([&] {
            go.arrive_and_wait();
            started = job.try_start();
        });
        std::thread canceller([&] {
            go.arrive_and_wait();
            outcome = job.request_cancel();
        });
        starter.join();
        canceller.join();
        if (started) {
            // Start won: cancel must have hit a RUNNING job.
            ASSERT_EQ(outcome, CancelOutcome::CancellationRequested);
            ASSERT_EQ(job.status(), JobStatus::Running);
        } else {
            // Cancel won: the job never runs.
            ASSERT_EQ(outcome, CancelOutcome::Cancelled);
            ASSERT_EQ(job.status(), JobStatus::Cancelled);
        }
    }
}

// --------------------------------------------------------------- registry

TEST(JobRegistry, InsertFindErase) {
    JobRegistry reg(10);
    reg.insert(std::make_shared<Job>(1, compute_spec()));
    EXPECT_NE(reg.find(1), nullptr);
    EXPECT_EQ(reg.find(2), nullptr);
    EXPECT_THROW(reg.insert(std::make_shared<Job>(1, compute_spec())), std::logic_error);
    EXPECT_TRUE(reg.erase(1));
    EXPECT_FALSE(reg.erase(1));
    EXPECT_EQ(reg.size(), 0U);
}

TEST(JobRegistry, RetentionEvictsOldestFinishedButNeverActiveJobs) {
    JobRegistry reg(3);
    for (JobId id = 1; id <= 10; ++id) {
        reg.insert(std::make_shared<Job>(id, compute_spec()));
    }
    // Finish jobs 1..6 in order; only the 3 most recent finished survive.
    std::size_t evicted = 0;
    for (JobId id = 1; id <= 6; ++id) {
        evicted += reg.mark_finished(id);
    }
    EXPECT_EQ(evicted, 3U);
    for (JobId id = 1; id <= 3; ++id) {
        EXPECT_EQ(reg.find(id), nullptr) << id;
    }
    for (JobId id = 4; id <= 10; ++id) {
        EXPECT_NE(reg.find(id), nullptr) << id;  // 4-6 retained, 7-10 active
    }
    EXPECT_EQ(reg.finished_count(), 3U);
    EXPECT_EQ(reg.size(), 7U);
}

TEST(JobRegistry, EvictedJobStaysAliveForHolders) {
    JobRegistry reg(1);
    reg.insert(std::make_shared<Job>(1, compute_spec()));
    reg.insert(std::make_shared<Job>(2, compute_spec()));
    const auto held = reg.find(1);
    reg.mark_finished(1);
    reg.mark_finished(2);  // evicts 1
    EXPECT_EQ(reg.find(1), nullptr);
    ASSERT_NE(held, nullptr);
    EXPECT_EQ(held->id(), 1U);  // shared ownership: no dangling pointer
}

TEST(JobRegistry, ConcurrentAccessIsSafeAndBounded) {
    JobRegistry reg(100);
    constexpr int kThreads = 8;
    constexpr int kPerThread = 2000;
    std::vector<std::thread> threads;
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&reg, t] {
            for (int i = 0; i < kPerThread; ++i) {
                const JobId id = static_cast<JobId>(t * kPerThread + i + 1);
                reg.insert(std::make_shared<Job>(id, compute_spec()));
                (void)reg.find(id);
                (void)reg.find(id / 2 + 1);
                reg.mark_finished(id);
            }
        });
    }
    for (auto& t : threads) {
        t.join();
    }
    EXPECT_EQ(reg.size(), 100U);
    EXPECT_EQ(reg.finished_count(), 100U);
}
