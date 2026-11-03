# ConcurrentCore interview guide

This guide explains the project for someone who knows C++ and basic operating
systems concepts but has not built a concurrent server before. Every answer
refers to the actual code, so you can open the file and point at it.

---

## Part 1: The ideas

### What does the server do?

It is a job server. A client connects over TCP and sends a JSON request such
as "compute the SHA-256 of `data/big.bin`". The server replies immediately
with a job id, runs the job in the background on one of several worker
threads, and lets the client ask for the status or result later. The client
can also ask to be notified when the job finishes. There are five job types:
SHA-256 checksum, zlib/gzip compression, decompression, text statistics, and
a CPU-heavy prime-counting computation.

### Why is multithreading necessary?

Two different reasons:

1. **Parallelism.** A computer with 12 hardware threads can run 12 jobs at once.
   With one thread, 400 compute jobs take about 12× longer (see
   `docs/BENCHMARKS.md` for measured numbers).
2. **Responsiveness.** If the thread that reads from the network also ran the
   jobs, a client sending `PING` would wait behind a 3-second compression
   job. ConcurrentCore separates **io threads** (network only) from
   **worker threads** (jobs only), so the server always answers quickly.

### How does the thread pool work? (`src/concurrency/thread_pool.cpp`)

When the pool is constructed it starts N `std::jthread`s. Each runs
`worker_loop()`:

```cpp
while (std::optional<UniqueTask> task = queue_.pop()) {   // sleeps while empty
    (*task)();                                           // run it, no lock held
}
```

`pop()` blocks on a condition variable while the queue is empty, so idle
workers use no CPU. Submitting work pushes a task and wakes one sleeping
worker. Threads are never created per job and never detached. At shutdown
the queue is closed, `pop()` returns `nullopt` once the queue is empty, the
loops end, and `shutdown()` joins every thread.

### What is producer–consumer?

A pattern in which one set of threads *produces* work items and another set
*consumes* them, connected by a shared queue. Here the io threads produce
(they turn `SUBMIT_JOB` requests into tasks) and the workers consume. The
queue decouples them: producers do not wait for jobs to run, and consumers do
not care where jobs came from. The queue is **bounded** (1024 slots by
default) so a flood of requests cannot use unlimited memory. When it is
full, the server rejects new jobs with `QUEUE_FULL`.

### Why are mutexes necessary?

When two threads touch the same data and at least one writes, the result
depends on timing; this is a *data race*, and in C++ it is undefined behaviour.
Example: two workers both do `items_.pop_front()` on a `std::deque` at the
same time. The deque's internal pointers can be corrupted. A `std::mutex`
lets only one thread at a time run the code that touches that data (the
*critical section*). ConcurrentCore uses `std::lock_guard` and
`std::unique_lock` (RAII), so a lock is always released, even if an
exception is thrown.

### What do condition variables do?

A mutex prevents simultaneous access; it does not let a thread *wait for
something to happen* efficiently. A condition variable does. A worker calls
`not_empty_.wait(lock, [&]{ return closed_ || !items_.empty(); })`. This
atomically releases the mutex and sleeps, then re-acquires the mutex when
woken and re-checks the condition. A producer that adds an item calls
`not_empty_.notify_one()`. The re-check matters because wake-ups can be
spurious, or another worker may have taken the item first.

### What race conditions could occur, and how are they prevented?

| Race | Prevention |
|---|---|
| Two workers pop the same task | queue mutex |
| Producer adds while consumer checks "empty", then the consumer sleeps forever (lost wake-up) | predicate wait under the same mutex the producer holds while pushing |
| A job is cancelled at the same moment a worker starts it | both transitions happen under the job's mutex; exactly one wins (tested 2000×) |
| A job finishes while a client is reading its status | snapshots are copied under the job mutex; the result is a `shared_ptr<const json>` |
| Shutdown begins while a client is submitting | `accepting_` and the outstanding-job counter change together under one mutex |
| A worker sends a result to a client that just disconnected | worker holds only a `weak_ptr` to the session; if expired, the event is dropped |
| Two handlers of one connection run at once on two io threads | each connection has its own Asio *strand*, which serializes its handlers |
| Two jobs write the same output file | atomic "rename without replace" (`link()` / `MoveFileExW`) |

### How are deadlocks avoided?

A deadlock needs a cycle: thread A holds lock 1 and waits for lock 2 while B
holds 2 and waits for 1. The rule here is that every data lock is a
**leaf**: no code ever holds two data locks at once. For example, the
registry hands out a `shared_ptr<Job>` and *releases its lock* before the
caller locks the job. With no nesting there can be no cycle. The only
nesting is in shutdown paths, and it always goes outer to inner.
Two more traps are handled explicitly:

* **Self-join:** a worker calling `pool.shutdown()` would wait for itself.
  It throws `std::logic_error` instead.
* **Blocking a producer on a full queue from an io thread** would stall the
  network. `try_post()` never blocks; the client gets `QUEUE_FULL`.

### How is memory managed?

No manual `delete` anywhere. Ownership is explicit:

* `unique_ptr` for exclusive ownership: the server owns the `JobManager`, and
  each queued task is a move-only `UniqueTask` holding a `unique_ptr` to its
  callable.
* `shared_ptr` only where ownership really is shared. A `Job` is referenced
  by the registry *and* by its queued closure, so eviction cannot free a job
  that is still waiting to run. A `Session` is kept alive by its own pending
  async operations.
* `weak_ptr` to break cycles and express "use it if it still exists"
  (completion events to sessions).
* RAII wrappers for C resources: `Deflater`/`Inflater` call
  `deflateEnd`/`inflateEnd`, `InputFile` closes its `FILE*`,
  `AtomicOutputFile` deletes its temp file unless committed.
* Bounded containers everywhere (queue, retained jobs, write queues).

### How does the TCP protocol work?

TCP delivers a stream of bytes, not messages. One `recv` might return half a
message or three messages. So every message is **framed**: 4 bytes of length
(big-endian) followed by that many bytes of JSON. The `FrameDecoder`
accumulates bytes and hands out complete frames. It checks the length as soon
as the 4 header bytes arrive and rejects anything over 1 MiB *before*
buffering it. Requests carry a `request_id` that the response echoes, so a
client can send many requests without waiting and still match the answers.

### How does graceful shutdown work?

Ctrl+C → `Server::shutdown()`:

1. Stop accepting new connections.
2. Stop accepting new jobs (`SERVER_SHUTTING_DOWN`), but keep serving
   existing connections so clients can still check status and receive
   completion events.
3. Wait up to `drain_timeout` (10 s) for queued and running jobs to finish.
4. If time runs out, cancel what is left: queued jobs instantly, running jobs
   cooperatively.
5. Join the worker threads.
6. Flush and close every connection, stop the event loop, join the io threads.

Each step completes before the next starts, so, for example, no worker can
try to send an event after the event loop is gone.

### How could the system scale?

* **Vertically:** more workers (bounded by cores for CPU-bound jobs); more io
  threads if many clients send many small requests.
* **Horizontally:** run several servers behind a load balancer. Jobs are
  independent, but job state is in memory, so a client must query the server
  that ran its job. Fixing that means sharing state (a database or Redis) or
  routing by job id (consistent hashing).
* **Better scheduling:** per-worker queues with work stealing to reduce
  contention on the single queue mutex at very high task rates; priority
  queues; per-client fairness so one client cannot fill the queue.

### What are the limitations?

* No authentication or encryption. It binds to localhost by default for that
  reason.
* Job state lives in memory and is lost on restart.
* One shared FIFO queue: a burst from one client delays everyone, and the
  single queue mutex becomes a bottleneck for very small tasks.
* Cancellation is cooperative. A blocking read on a slow filesystem cannot be
  interrupted.
* Path checks are vulnerable to a symlink race (TOCTOU) by a local user who
  can write into the workspace.
* Clients cannot upload files; tasks operate on files already on the server.

### What improvements could be made?

Work-stealing scheduler, priorities and per-client quotas, TLS and token
auth, a persistent job store, streaming file upload, Prometheus metrics with
latency histograms, `openat`/`O_NOFOLLOW`-based file access to close the
TOCTOU window, and `io_uring`/IOCP-based async file I/O.

---

## Part 2: Interview questions and answers

**1. Walk me through what happens when a client submits a job.**
The client sends a framed JSON `SUBMIT_JOB`. On the session's strand, the io
thread decodes the frame, and `RequestHandler` validates the envelope and the
task parameters (`parse_task_spec`: unknown keys, path safety, ranges).
`JobManager::submit` checks `accepting_` and increments `outstanding_` under
`state_mutex_`. It creates a `Job` in `shared_ptr`, inserts it in the
registry, and calls `pool_.try_post()` with a closure that holds the
`shared_ptr`. If the queue is full, it rolls back and returns `QUEUE_FULL`.
Otherwise the client gets `{"job_id":..., "status":"QUEUED"}`. A worker pops
the closure, transitions the job to RUNNING, executes it with no lock held,
records the result, and runs `finalize()`. That updates metrics, retention,
and the optional completion event, which is posted to the session's strand.

**2. Why a bounded queue? What happens when it is full?**
An unbounded queue turns overload into unbounded memory growth and
unbounded latency. Bounding it makes overload visible. The network path uses
`try_push`, which returns `Full` immediately, and the client gets
`QUEUE_FULL` and can back off. In-process callers can use the blocking
`push()` instead. The stress test `JobManagerConcurrentProducersWithBackPressure`
checks that every retry eventually succeeds and that `rejected` matches the
number of retries.

**3. How do you avoid lost wake-ups in the queue?**
Every push or pop changes state under the mutex, and every wait uses
`wait(lock, predicate)`. A consumer checks the predicate while holding the
mutex. If it goes to sleep, it atomically releases the mutex in `wait()`,
so the producer cannot slip a push in between "check" and "sleep". I
notify after unlocking (an optimisation, not a correctness requirement).
`close()` uses `notify_all`, because every waiter must wake and see
`closed_`. The tests `CloseWakesAllBlockedConsumers`/`Producers` would hang
(and time out in CTest) if a wake-up were lost.

**4. Why does `push` take `T&&` but only sometimes move from it?**
So ownership is never lost on failure. If the queue is full or closed, the
caller still owns the item and can retry it, reject it, or report it. A
`std::unique_ptr` test verifies the pointer is still non-null after `Full`.

**5. Explain graceful vs immediate shutdown of the pool.**
Both close the queue first. Since `close()` and `push()` are serialized by
the queue mutex, any submission either got in before the close or is rejected
with `ThreadPoolStoppedError`; nothing is silently lost. Graceful then lets
workers drain the queue. Immediate calls `drain()` and destroys the queued
tasks; destroying a `packaged_task` breaks its promise, so `submit()` callers
see `std::future_errc::broken_promise`. Both then join all workers.
`ConcurrentSubmitAndShutdownLosesNoAcceptedTask` races producers against
shutdown and checks executed == accepted.

**6. How does cancellation work, and what are its limits?**
Each job has a `std::stop_source`. Cancelling a QUEUED job flips it to
CANCELLED under its mutex; when a worker later pops it, `try_start()` fails
and it is skipped. Cancelling a RUNNING job calls `request_stop()`. Tasks
call `ctx.throw_if_cancelled()` between chunks (every 64 KiB of I/O, every
4096 numbers in compute), and the worker records CANCELLED. Limits: the
latency of a cancel is one chunk of work; a blocking read cannot be
interrupted; a task that finishes before checking reports COMPLETED.

**7. What if a cancel and a worker start the same job at the same instant?**
Both `try_start()` (QUEUED→RUNNING) and `request_cancel()` take the job's
mutex. Whichever gets it first wins. If start wins, cancel sees RUNNING and
requests a stop. If cancel wins, the job is CANCELLED and `try_start()`
returns false. A unit test runs this race 2000 times with a `std::latch` so
both threads start together.

**8. How do you guarantee a completion callback runs exactly once?**
Terminal transitions (`complete`, `fail`, `finish_cancelled`, cancel-while-queued)
succeed at most once because they check the current state under the
mutex. Only the thread whose transition succeeded calls `finalize()`, which
moves the callback out of the job (`take_completion_callback`) and invokes it
*after* releasing the lock. `CancellationUnderLoad` checks that the callback
count equals the job count with thousands of concurrent cancels.

**9. How does a worker thread send a result to the right client without
touching the socket?**
The completion callback captures an `EventSink` holding a
`weak_ptr<Session>`. The worker calls it. It `lock()`s the weak pointer and,
if the session is alive, calls `deliver()`. `deliver()` only does
`asio::post(strand, ...)`. The actual write happens later on the session's
own strand. If the client disconnected, `lock()` fails and the event is
dropped. The worker never blocks and never touches a destroyed object.

**10. What is a strand and why use it instead of a mutex?**
A strand is an Asio executor that guarantees handlers posted to it never run
concurrently, even when several threads call `io_context::run()`. Each
connection gets its own strand. All of its socket operations, its timer, and
its buffers are only touched by handlers on that strand, so the session needs
no locks. A mutex would also work, but you would have to remember to take it
on every path. With a strand, the serialization is structural.

**11. How do you handle a client that sends data but never reads responses?**
Its outgoing queue grows. Above half of `max_pending_write_bytes` (8 MiB) the
session stops reading new requests from it, which is back-pressure. Below a
quarter it resumes. If the hard limit would be exceeded (for example by
completion events), the connection is closed. Other clients and all workers
are unaffected, because workers only `post` and the slow client's writes
happen on its own strand.

**12. How do you handle partial reads and oversized messages?**
`FrameDecoder::feed` appends whatever arrived; `next()` returns a frame only
once header + payload are complete. Tests feed frames one byte at a time
and in random chunk sizes with a fixed seed. Size is checked from the
4-byte header alone, so a client announcing 4 GB is rejected before the
server allocates anything. Because the stream cannot be resynchronized after
that, the server sends `MESSAGE_TOO_LARGE` and closes. Malformed JSON *inside*
a valid frame is just an error response; the connection stays open.

**13. How do you prevent path traversal?**
Two layers. First, lexical validation at submit time: reject absolute paths,
`..`, drive letters, `:` (NTFS alternate data streams), backslashes, NUL,
overlong paths and Windows device names. Second, resolution at execution time:
`std::filesystem::canonical` follows symlinks, and the real path must be
inside the workspace root by a **component-wise** comparison. A string-prefix
check would accept `/srv/ws-evil` for root `/srv/ws`. Remaining gap: a
symlink swapped between check and open (TOCTOU), documented as a limitation.

**14. How do you protect against decompression bombs?**
Inflate in 64 KiB output chunks and check `produced_total + produced >
max_output_bytes` *before* writing each chunk. A tiny file that would expand
to gigabytes is stopped at the limit (256 MiB by default; clients can ask for
less, never more). The partial output lives in a temp file that is deleted
when the job fails. A test compresses 8 MiB of zeros (~1000:1) and checks
that a 1 MiB limit triggers `LIMIT_EXCEEDED` and leaves no files behind.

**15. Which atomics did you use, and why relaxed ordering?**
Only independent counters and gauges: submitted, completed, failed, running,
latency sums, active workers, id generators. `memory_order_relaxed` is
correct because nothing else is published through them. No thread reads a
counter and then assumes something about job data. Job data is always read
under the job mutex. Where two variables must change together (`accepting_`
and `outstanding_`), I used a mutex, because separate atomics would reopen the
"check, then shutdown, then increment" race.

**16. What was the hardest bug?**
An intermittent hang in `~io_context` on Windows with GCC (full story in
`docs/DESIGN_DECISIONS.md` §12). gdb showed Asio joining a garbage thread
handle, and the thread descriptor contained a UTF-16 file path. Clang builds
and AddressSanitizer were clean. Adding a never-executed test made a different
test hang, which pointed at layout or ODR, not logic. The cause: Asio picks
`posix_thread` or `win_thread` depending on whether `_POSIX_THREADS` is
already defined when it is first included. On MinGW, including `<thread>`
first defines it. So translation units disagreed on a class layout. Fix: pin
`ASIO_HAS_PTHREADS` for every TU in CMake. The lesson: configuration macros
must be uniform across TUs, and "unrelated code changes the bug" is a strong
ODR signal.

**17. How do you test concurrency without flaky sleeps?**
With synchronization primitives that make the interleaving explicit.
`std::latch` starts threads together. A `Gate` (mutex + condition variable)
holds a worker inside a task until the test opens it, which makes "the
queue is full" or "the job is RUNNING" deterministic. Promises signal "the
worker has started". The few polls (`wait_until`) wait for externally
observable state with a deadline and fail rather than hang. Correctness
tests that would deadlock on a bug fail through CTest's timeout instead of
passing by luck.

**18. Why not use `std::function` for queued tasks?**
`std::function` requires a copyable callable. A `std::packaged_task` (used by
`submit()`) and lambdas that own `unique_ptr`s are move-only. C++23 has
`std::move_only_function`. For C++20 I wrote `UniqueTask`, a 40-line
type-erased wrapper around `unique_ptr<Concept>`. That avoids the
`shared_ptr<packaged_task>` workaround and its extra allocation and atomic
refcount per task.

**19. What would you change for 100 000 tiny tasks per second?**
Measure first. The likely bottleneck is the single queue mutex plus a
condition-variable wake-up per task. Options: batch submissions, per-worker
queues with work stealing (like `std::async` implementations, TBB, or Go's
scheduler), spin briefly before sleeping, and a lock-free MPMC ring buffer
for the hot path. I would keep the bounded and close semantics, because they
are what make shutdown and back-pressure correct.

**20. How do you know the benchmark numbers are real?**
They come from `concurrentcore-bench`, which generates deterministic inputs
from fixed seeds, runs each configuration several times (median wall time,
pooled latency percentiles), and writes CSV + Markdown automatically. The
report includes the machine (CPU model, cores, OS, compiler). Its notes
explain effects like hybrid P/E-core CPUs and oversubscription above the
core count, rather than claiming linear scaling.
