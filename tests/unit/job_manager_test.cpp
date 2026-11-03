#include "concurrentcore/jobs/job_manager.hpp"

#include "support/test_support.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <future>
#include <latch>
#include <thread>
#include <vector>

using namespace concurrentcore;
using namespace std::chrono_literals;

namespace {

TaskSpec compute(std::uint64_t limit) { return TaskSpec{TaskType::Compute, ComputeParams{limit}}; }

class JobManagerTest : public ::testing::Test {
protected:
    std::shared_ptr<const Workspace> workspace() {
        return std::make_shared<const Workspace>(dir_.path());
    }

    static JobManagerConfig config(std::size_t workers, std::size_t queue, std::size_t retained = 1000) {
        JobManagerConfig c;
        c.worker_count = workers;
        c.queue_capacity = queue;
        c.max_retained_jobs = retained;
        return c;
    }

    cctest::TempDir dir_;
};

/// Executor whose jobs block until released; reports when each starts and
/// honours cancellation while blocked.
struct GatedExecutor {
    cctest::Gate gate;
    cctest::Counter started;
    std::atomic<int> executions{0};

    JobManager::Executor make() {
        return [this](const TaskSpec&, const TaskContext& ctx) -> nlohmann::json {
            ++executions;
            started.increment();
            while (!gate.wait_for(1ms)) {
                ctx.throw_if_cancelled();
            }
            return {{"done", true}};
        };
    }
};

}  // namespace

TEST_F(JobManagerTest, RunsRealTaskToCompletion) {
    JobManager mgr(config(2, 16), workspace());
    const auto r = mgr.submit(compute(1000));
    ASSERT_TRUE(r.ok());
    ASSERT_TRUE(mgr.wait_idle(10s));
    const auto snap = mgr.snapshot(*r.job_id);
    ASSERT_TRUE(snap.has_value());
    EXPECT_EQ(snap->status, JobStatus::Completed);
    EXPECT_EQ((*snap->result)["prime_count"], 168);
    const auto stats = mgr.stats();
    EXPECT_EQ(stats.submitted, 1U);
    EXPECT_EQ(stats.completed, 1U);
    EXPECT_GE(stats.avg_execution_ms, 0.0);
}

TEST_F(JobManagerTest, FailuresArePropagatedWithErrorCodes) {
    std::atomic<int> n{0};
    JobManager mgr(config(1, 16), workspace(), [&](const TaskSpec&, const TaskContext&) -> nlohmann::json {
        if (n++ == 0) {
            throw CoreError(ErrorCode::IoError, "disk on fire");
        }
        throw std::runtime_error("unexpected");
    });
    const auto a = mgr.submit(compute(1));
    const auto b = mgr.submit(compute(1));
    ASSERT_TRUE(mgr.wait_idle(10s));
    const auto sa = mgr.snapshot(*a.job_id);
    const auto sb = mgr.snapshot(*b.job_id);
    EXPECT_EQ(sa->status, JobStatus::Failed);
    EXPECT_EQ(sa->error->code, ErrorCode::IoError);
    EXPECT_EQ(sa->error->message, "disk on fire");
    EXPECT_EQ(sb->status, JobStatus::Failed);
    EXPECT_EQ(sb->error->code, ErrorCode::InternalError);
    EXPECT_EQ(mgr.stats().failed, 2U);
}

TEST_F(JobManagerTest, FailedTaskOnRealWorkspaceReportsFileNotFound) {
    JobManager mgr(config(1, 4), workspace());
    const auto r = mgr.submit(TaskSpec{TaskType::Sha256, Sha256Params{"missing.bin"}});
    ASSERT_TRUE(mgr.wait_idle(10s));
    const auto s = mgr.snapshot(*r.job_id);
    EXPECT_EQ(s->status, JobStatus::Failed);
    EXPECT_EQ(s->error->code, ErrorCode::FileNotFound);
}

TEST_F(JobManagerTest, CancelQueuedJobNeverExecutes) {
    GatedExecutor exec;
    JobManager mgr(config(1, 16), workspace(), exec.make());
    const auto running = mgr.submit(compute(1));
    ASSERT_TRUE(exec.started.wait_for_at_least(1));
    const auto queued = mgr.submit(compute(1));
    EXPECT_EQ(mgr.snapshot(*queued.job_id)->status, JobStatus::Queued);

    EXPECT_EQ(mgr.cancel(*queued.job_id), CancelOutcome::Cancelled);
    EXPECT_EQ(mgr.snapshot(*queued.job_id)->status, JobStatus::Cancelled);

    exec.gate.open();
    ASSERT_TRUE(mgr.wait_idle(10s));
    EXPECT_EQ(exec.executions.load(), 1);  // the cancelled job was skipped
    EXPECT_EQ(mgr.snapshot(*running.job_id)->status, JobStatus::Completed);
    EXPECT_EQ(mgr.stats().cancelled, 1U);
}

TEST_F(JobManagerTest, CancelRunningJobIsCooperative) {
    GatedExecutor exec;
    JobManager mgr(config(1, 4), workspace(), exec.make());
    const auto r = mgr.submit(compute(1));
    ASSERT_TRUE(exec.started.wait_for_at_least(1));
    EXPECT_EQ(mgr.cancel(*r.job_id), CancelOutcome::CancellationRequested);
    ASSERT_TRUE(mgr.wait_idle(10s));
    const auto s = mgr.snapshot(*r.job_id);
    EXPECT_EQ(s->status, JobStatus::Cancelled);
    EXPECT_EQ(s->error->code, ErrorCode::Cancelled);
    EXPECT_TRUE(s->cancel_requested);
}

TEST_F(JobManagerTest, CancelRealComputeTaskWhileRunning) {
    JobManager mgr(config(1, 4), workspace());
    const auto r = mgr.submit(compute(150'000'000));  // many seconds of work
    ASSERT_TRUE(cctest::wait_until([&] { return mgr.snapshot(*r.job_id)->status == JobStatus::Running; }));
    EXPECT_EQ(mgr.cancel(*r.job_id), CancelOutcome::CancellationRequested);
    ASSERT_TRUE(mgr.wait_idle(10s));
    EXPECT_EQ(mgr.snapshot(*r.job_id)->status, JobStatus::Cancelled);
}

TEST_F(JobManagerTest, CancelUnknownOrFinishedJob) {
    JobManager mgr(config(1, 4), workspace());
    EXPECT_EQ(mgr.cancel(12345), CancelOutcome::NotFound);
    const auto r = mgr.submit(compute(10));
    ASSERT_TRUE(mgr.wait_idle(10s));
    EXPECT_EQ(mgr.cancel(*r.job_id), CancelOutcome::AlreadyFinished);
}

TEST_F(JobManagerTest, FullQueueRejectsWithoutBlocking) {
    GatedExecutor exec;
    JobManager mgr(config(1, 2), workspace(), exec.make());
    ASSERT_TRUE(mgr.submit(compute(1)).ok());
    ASSERT_TRUE(exec.started.wait_for_at_least(1));  // worker busy, queue empty
    ASSERT_TRUE(mgr.submit(compute(1)).ok());
    ASSERT_TRUE(mgr.submit(compute(1)).ok());
    const auto rejected = mgr.submit(compute(1));
    EXPECT_FALSE(rejected.ok());
    EXPECT_EQ(rejected.error, ErrorCode::QueueFull);
    EXPECT_EQ(mgr.stats().rejected, 1U);
    EXPECT_EQ(mgr.stats().queue_depth, 2U);
    exec.gate.open();
    ASSERT_TRUE(mgr.wait_idle(10s));
    EXPECT_EQ(mgr.stats().completed, 3U);
}

TEST_F(JobManagerTest, CompletionCallbackRunsExactlyOncePerJob) {
    GatedExecutor exec;
    JobManager mgr(config(2, 64), workspace(), exec.make());
    std::atomic<int> callbacks{0};
    std::vector<JobId> ids;
    for (int i = 0; i < 20; ++i) {
        ids.push_back(*mgr.submit(compute(1), [&](const JobSnapshot& s) {
                           EXPECT_TRUE(is_terminal(s.status));
                           ++callbacks;
                       }).job_id);
    }
    // Cancel half while most are still queued.
    for (std::size_t i = 0; i < ids.size(); i += 2) {
        (void)mgr.cancel(ids[i]);
    }
    exec.gate.open();
    ASSERT_TRUE(mgr.wait_idle(10s));
    EXPECT_EQ(callbacks.load(), 20);
}

TEST_F(JobManagerTest, ShutdownRejectsNewSubmissions) {
    JobManager mgr(config(1, 4), workspace());
    mgr.shutdown(1s);
    const auto r = mgr.submit(compute(1));
    EXPECT_FALSE(r.ok());
    EXPECT_EQ(r.error, ErrorCode::ServerShuttingDown);
    EXPECT_FALSE(mgr.stats().accepting);
}

TEST_F(JobManagerTest, GracefulShutdownDrainsQueuedJobs) {
    GatedExecutor exec;
    JobManager mgr(config(1, 64), workspace(), exec.make());
    std::vector<JobId> ids;
    for (int i = 0; i < 10; ++i) {
        ids.push_back(*mgr.submit(compute(1)).job_id);
    }
    ASSERT_TRUE(exec.started.wait_for_at_least(1));
    std::thread stopper([&] { mgr.shutdown(30s); });
    exec.gate.open();
    stopper.join();
    for (const auto id : ids) {
        EXPECT_EQ(mgr.snapshot(id)->status, JobStatus::Completed);
    }
}

TEST_F(JobManagerTest, ShutdownTimeoutCancelsQueuedAndRunningJobs) {
    GatedExecutor exec;  // never opened
    JobManager mgr(config(2, 64), workspace(), exec.make());
    std::vector<JobId> ids;
    for (int i = 0; i < 10; ++i) {
        ids.push_back(*mgr.submit(compute(1)).job_id);
    }
    ASSERT_TRUE(exec.started.wait_for_at_least(2));
    mgr.shutdown(50ms);  // drain times out -> escalate to cancellation
    for (const auto id : ids) {
        EXPECT_EQ(mgr.snapshot(id)->status, JobStatus::Cancelled);
    }
    EXPECT_EQ(exec.executions.load(), 2);  // queued ones never started
    EXPECT_EQ(mgr.stats().cancelled, 10U);
}

TEST_F(JobManagerTest, FinishedJobRetentionIsBounded) {
    JobManager mgr(config(2, 64, /*retained=*/5), workspace());
    std::vector<JobId> ids;
    for (int i = 0; i < 20; ++i) {
        ids.push_back(*mgr.submit(compute(100)).job_id);
        ASSERT_TRUE(mgr.wait_idle(10s));  // finish in submission order
    }
    for (std::size_t i = 0; i < 15; ++i) {
        EXPECT_FALSE(mgr.snapshot(ids[i]).has_value()) << "job " << ids[i] << " should be evicted";
    }
    for (std::size_t i = 15; i < 20; ++i) {
        EXPECT_TRUE(mgr.snapshot(ids[i]).has_value());
    }
    EXPECT_EQ(mgr.stats().retained_jobs, 5U);
}

TEST_F(JobManagerTest, ConcurrentStatusQueriesWhileJobsRun) {
    JobManager mgr(config(4, 256), workspace());
    std::vector<JobId> ids;
    for (int i = 0; i < 100; ++i) {
        ids.push_back(*mgr.submit(compute(20'000)).job_id);
    }
    std::atomic<bool> stop{false};
    std::atomic<int> bad{0};
    std::vector<std::thread> readers;
    for (int r = 0; r < 4; ++r) {
        readers.emplace_back([&] {
            while (!stop.load()) {
                for (const auto id : ids) {
                    const auto s = mgr.snapshot(id);
                    if (!s || (s->status == JobStatus::Completed && !s->result)) {
                        ++bad;
                    }
                }
                (void)mgr.stats();
            }
        });
    }
    ASSERT_TRUE(mgr.wait_idle(30s));
    stop = true;
    for (auto& t : readers) {
        t.join();
    }
    EXPECT_EQ(bad.load(), 0);
    EXPECT_EQ(mgr.stats().completed, 100U);
}
