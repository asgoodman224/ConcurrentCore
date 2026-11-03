# Concurrency design

This document describes every thread, every piece of shared mutable state,
the lock that protects it, and the rules that keep the system free of data
races, deadlocks and lifetime bugs. File references are to `src/` and
`include/concurrentcore/`.

## 1. Threads

| Thread | Count | Created by | Runs | Never does |
|---|---|---|---|---|
| main | 1 | OS | `start()`, waits for SIGINT/SIGTERM, `shutdown()` | — |
| io threads | `io_threads` (default 2) | `Server::start` | `io_context::run()`: accept, read/parse frames, answer requests, write responses | execute tasks; block on the task queue |
| workers | `worker_threads` (default = hardware concurrency) | `ThreadPool` constructor | pop tasks from the bounded queue and execute them | blocking socket I/O; touching a `Session` directly |
| Asio internal | 1 (Windows timer thread) | Asio | timer wake-ups | — |

All threads are created at startup and joined at shutdown. No thread is
detached. No thread is created per request or per job.

## 2. Producer–consumer pipeline

```
 io thread (producer)                         worker thread (consumer)
 ─────────────────────                        ────────────────────────
 parse SUBMIT_JOB
 validate params ──► TaskSpec
 JobManager::submit
   state_mutex_: accepting? ++outstanding
   registry_.insert(job)        (registry lock)
   pool_.try_post(closure) ───► BoundedQueue ───► pop() (blocks on not_empty_)
   └─ QUEUE_FULL → reply now                       job->try_start()   (job lock)
 reply {"job_id", "QUEUED"}                        execute task       (NO lock held)
                                                   job->complete()    (job lock)
                                                   finalize(): metrics, retention,
                                                     completion callback ──► asio::post
                                                                             to session strand
 session strand: enqueue JOB_FINISHED event ◄───────────────────────────────┘
```

The producer never blocks on the queue. `try_post` returns `Full`
immediately and the client gets `QUEUE_FULL`. This is deliberate. If an io
thread blocked on a full queue, it would stall every connection it serves.
Waiting under back-pressure is pushed to the client instead.

## 3. Shared state and its protection

| State | Owner | Protected by | Notes |
|---|---|---|---|
| queue items, `closed_` | `BoundedQueue` | `BoundedQueue::mutex_` + `not_empty_`/`not_full_` | invariants I1–I4 below |
| `shut_down_`, joining workers | `ThreadPool` | `lifecycle_mutex_` | never taken by workers |
| `worker_ids_` | `ThreadPool` | immutable after construction | read without lock |
| job counters, `active_workers_` | `ThreadPool`, `JobManager` | `std::atomic`, relaxed | independent counters; nothing else is published through them |
| job status, timestamps, result, error, callback | `Job` | `Job::mutex_` | leaf lock |
| `id`, `spec` | `Job` | immutable after construction | read without lock |
| stop flag | `Job` | `std::stop_source` | internally thread-safe |
| `jobs_` map, finished FIFO | `JobRegistry` | `JobRegistry::mutex_` | leaf lock |
| `accepting_`, `outstanding_` | `JobManager` | `state_mutex_` + `idle_cv_` | changed together, atomically |
| shutdown sequencing | `JobManager` | `shutdown_mutex_` | held only by the shutting-down thread |
| session map | `Server` | `sessions_mutex_` + `sessions_cv_` | leaf lock |
| server state (`Created/Running/Stopped`) | `Server` | `lifecycle_mutex_` | |
| socket, timer, read buffer, write queue, flags | `Session` | **the session's strand** | no mutex needed |
| acceptor, retry timer | `Server` | the acceptor's strand | `shutdown()` posts the close onto it |
| `Workspace`, `ServerConfig`, `TaskLimits` | shared | immutable | safe to read from any thread |

### BoundedQueue invariants (`concurrency/bounded_queue.hpp`)

* **I1** `items_.size() <= capacity_` at all times.
* **I2** `closed_` goes from false to true exactly once and never back.
* **I3** After close, nothing is ever added.
* **I4** Items present at close remain poppable. A graceful drain is just
  "keep popping until `nullopt`".

**No lost wake-ups.** Every state change that could release a waiter
happens under `mutex_`. Every wait uses the predicate overload
(`cv.wait(lock, pred)`), so the condition is re-checked under the lock after
each wake-up (spurious or real). A consumer either sees the item before it
sleeps, or is already registered on the condition variable when `notify_one`
runs. `close()` and `drain()` use `notify_all`, because every waiter must
re-evaluate. Notifications are sent *after* unlocking, so the woken thread
does not immediately block on a mutex the notifier still holds.

**Ownership on failure.** `push`/`try_push` take `T&&` but move from it only
when the result is `Accepted`. On `Full` or `Closed`, the caller still owns the
item. Tests check this with `std::unique_ptr` payloads.

## 4. Lock ordering and deadlock freedom

The rule is simple and is checked by review: **all data locks are leaves.**
No code acquires a second data lock while holding one, and no lock is held
while calling user code, a callback, or a blocking operation.

| Lock | May be held while acquiring… |
|---|---|
| `Job::mutex_` | nothing |
| `JobRegistry::mutex_` | nothing (it hands out `shared_ptr`s; callers lock the `Job` afterwards) |
| `BoundedQueue::mutex_` | nothing |
| `JobManager::state_mutex_` | nothing |
| `Server::sessions_mutex_` | nothing (copies `weak_ptr`s out, then calls sessions unlocked) |
| `JobManager::shutdown_mutex_` | `state_mutex_` (briefly), then the leaves *sequentially* |
| `ThreadPool::lifecycle_mutex_` | the queue mutex (via `close`/`drain`); it also joins workers |
| `Server::lifecycle_mutex_` | everything above, sequentially; it also joins io threads |

The only nested acquisitions are by *lifecycle* locks (shutdown paths), and
they always nest outer to inner in the order
`Server::lifecycle → JobManager::shutdown → JobManager::state / ThreadPool::lifecycle → leaves`.
No thread ever acquires these in the opposite order:

* Workers never touch any lifecycle lock.
* A lifecycle lock may be held while *joining* threads only if those threads
  can finish without that lock. `ThreadPool::shutdown` joins workers, which
  never take `lifecycle_mutex_`. `Server::shutdown` joins io threads, which
  never take `Server::lifecycle_mutex_`.
* Self-joins are rejected explicitly. `ThreadPool::shutdown()` from a worker
  throws `std::logic_error`; the check uses the immutable `worker_ids_` and runs
  *before* taking the lifecycle lock. `Server::shutdown()` from an io thread
  throws, detected through a `thread_local` marker.

Since there is no cycle in the "held while acquiring" graph, lock-order
deadlocks cannot occur.

## 5. Avoiding expensive work under locks

* Tasks run with **no** lock held. `run_job` takes the job lock only for the
  O(1) state transitions before and after.
* `Job::snapshot()` copies a `shared_ptr<const json>` for the result instead
  of deep-copying it, so status queries are O(1) even for large results.
* Registry eviction moves evicted `shared_ptr`s into a local vector, so `Job`
  destructors run *after* the registry lock is released.
* Completion callbacks are moved out under the job lock and invoked after
  it is released.
* Metrics are relaxed atomics. Collecting stats never blocks job execution.
  The per-call cost is a few atomic loads plus brief queue/registry/session
  locks to read sizes.

## 6. The network side: strands instead of mutexes

Each accepted socket is created on its own `asio::strand`. Every handler for
that socket and its timer runs on that strand. Asio guarantees that handlers on
one strand never run concurrently, even with several io threads. So all
`Session` members (socket, decoder, write queue, flags) are accessed
single-threaded, without a mutex.

Code on other threads reaches a session only through two entry points,
`deliver()` and `close()`. Both do nothing except `asio::post` a lambda onto
the strand. Worker threads therefore never touch sockets, never block on
network I/O, and are unaffected by slow clients.

Back-pressure per connection: above `max_pending_write_bytes / 2` queued
bytes, the session stops reading new requests (`read_paused_`). It resumes
below a quarter. At the hard limit it drops the connection. Workers only
`post`; they are never blocked by a client that does not read.

## 7. Object lifetimes (use-after-free and dangling references)

| Object | Owned by | Kept alive by | Accessed from other threads via |
|---|---|---|---|
| `Job` | `JobRegistry` (shared) | the queued closure's `shared_ptr` | `shared_ptr` copies; eviction cannot free a job that is still queued or being queried |
| `Session` | itself (`enable_shared_from_this`) | the `shared_ptr` captured by every pending async handler | `weak_ptr` only (`make_event_sink`) |
| `JobManager`, `Server`, `Workspace` | `Server` / main | outlive every thread that uses them, because threads are joined before destruction | raw references, safe because of the join ordering |

Key points:

* **Completion events cannot hit a dead connection.** The sink holds a
  `weak_ptr<Session>`. If the client disconnected, `lock()` fails and the event
  is dropped. A finished job never extends a session's lifetime. A `Session`
  never references a `Job`, so there are no reference cycles.
* **Buffers outlive in-flight I/O.** `close_now()` deliberately does *not*
  clear the write queue. An aborted `async_write` may still reference its
  buffer until the handler runs (on Windows IOCP the kernel can touch it until
  the completion is dequeued). The queue is freed with the session, which
  that pending handler keeps alive.
* **Destruction order is shutdown order.** `JobManager` declares `pool_` last,
  so it is destroyed first: workers are joined before the registry, workspace or
  metrics they use. `Server` joins workers (inside `JobManager::shutdown`)
  before closing sessions, and joins io threads before destroying the
  `io_context`.
* **No double deletion.** There is no manual `delete` in the code base.
  Ownership is `unique_ptr` (server → job manager, tasks in the queue via
  `UniqueTask`), `shared_ptr` where sharing is real (jobs, sessions), and
  values elsewhere.

## 8. Cancellation

Cancellation is cooperative and uses C++20 `std::stop_source` / `std::stop_token`:

* **QUEUED job:** `request_cancel()` moves it to `CANCELLED` under the job lock
  and the cancelling thread finalizes it. When a worker later pops the closure,
  `try_start()` fails and the job is skipped. It still occupied a queue slot
  until then.
* **RUNNING job:** `request_cancel()` calls `request_stop()`. The status stays
  `RUNNING` until the task reaches a cancellation point, throws
  `OperationCancelled`, and the worker records `CANCELLED`.
* **Race between start and cancel:** both transitions happen under the job lock,
  so exactly one wins. The stress test `Job.CancelRacingWithStartHasExactlyOneWinner`
  runs this race 2000 times.
* **Exactly-once finalization:** every terminal transition succeeds at most
  once. Only the thread whose transition succeeded calls `finalize()`, so
  metrics, retention and the completion callback run exactly once per job.
  `StressTest.CancellationUnderLoad` checks this with thousands of concurrent
  cancels.

**When cancellation cannot interrupt immediately:**

| Task | Cancellation points | Worst-case delay after a cancel request |
|---|---|---|
| `compute` | every 4096 candidates | one batch of 4096 primality tests (milliseconds even near the maximum limit) |
| `sha256`, `text-analysis` (file) | before every read chunk (`io_chunk_bytes`, 64 KiB) | one chunk read + hash |
| `compress` | before every input chunk | one chunk read + `deflate` of that chunk |
| `decompress` | before every inflate step | one 64 KiB output chunk |
| `text-analysis` (inline text) | none | the whole input; it is bounded by the 1 MiB message limit and takes microseconds |

A blocking `read()` on a slow or networked filesystem cannot be interrupted;
the check happens once it returns. A task that completes its work before
observing the request ends `COMPLETED`, not `CANCELLED`.

## 9. Shutdown

`Server::shutdown(drain_timeout)` (also invoked by SIGINT/SIGTERM and the
destructor) is idempotent and serialized by `Server::lifecycle_mutex_`:

1. **Stop accepting connections.** The acceptor lives on a strand, so the close
   is posted there and the caller waits for it.
2. **`JobManager::shutdown(drain_timeout)`:**
   1. Under `state_mutex_`: `accepting_ = false`, then wait on `idle_cv_` until
      `outstanding_ == 0` or the timeout expires. Because `submit()` checks
      `accepting_` and increments `outstanding_` under the same mutex, no job
      can slip in between the check and the wait.
   2. On timeout, cancel everything: queued jobs become `CANCELLED`
      immediately; running jobs get a stop request.
   3. `pool_.shutdown(Graceful)`: close the queue (concurrent `try_post` now
      returns `Closed` and `submit` rolls back), let workers drain it
      (cancelled entries are skipped instantly), and join them.

   Connections stay open throughout, so clients can still query status and
   receive `JOB_FINISHED` events.
3. **Close sessions.** Each session is asked to flush its queued writes
   (bounded by a 2 s flush timer) and close.
4. **Bounded wait, then stop.** Wait up to 3 s for all sessions to report
   closed, call `io_context::stop()`, and join the io threads.

`ThreadPool` offers two modes on its own:

* **Graceful:** finish every accepted task.
* **Immediate:** drain and destroy queued tasks. `submit()` futures then report
  `broken_promise`. Running tasks finish.

Both are tested, including races between `post()` and `shutdown()`
(`ThreadPool.ConcurrentSubmitAndShutdownLosesNoAcceptedTask`).

## 10. Atomics: where and why

Atomics are used only for independent counters and gauges:
`submitted/completed/failed/cancelled/rejected/running`, latency sums,
`active_workers_`, `next_id_`, session ids and connection counters. All
use `memory_order_relaxed`, because no other memory is published through
them. A reader that sees `completed == 5` is not entitled to assume anything
about job contents. Job state is always read under the job's mutex. Anything
needing cross-variable consistency (`accepting_` + `outstanding_`) uses a
mutex instead.

## 11. Verification

* **ThreadSanitizer** is not available for Windows targets, which is the only
  platform available in the development environment. The CI workflow
  (`.github/workflows/ci.yml`) runs the full suite under TSan on Linux; see
  `docs/BENCHMARKS.md` and the README for what was and was not executed
  locally.
* **AddressSanitizer + UndefinedBehaviorSanitizer** (Clang 23, llvm-mingw)
  run the unit, integration and stress suites.
* **libstdc++ debug mode** (`-D_GLIBCXX_DEBUG -D_GLIBCXX_ASSERTIONS`) runs the
  suites with checked iterators and containers.
* Stress tests: 240 000-item MPMC queue contention, 200 000 tiny tasks,
  4 000 jobs from 8 producers under `QUEUE_FULL` back-pressure, 3 000 jobs with
  6 000 concurrent random cancels, 200 pool and 25 server start/stop cycles,
  and 32 clients × 40 jobs.

See `docs/DESIGN_DECISIONS.md` §"A real ODR bug" for the most interesting
defect these tools found during development.
