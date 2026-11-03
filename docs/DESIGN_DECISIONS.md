# Design decisions and trade-offs

## 1. Asio for the network, a custom pool for the work

Asio's `io_context` could run tasks too (`asio::post` to a thread pool). I kept
networking and task execution on **separate thread sets**:

* A slow 2-second compression job can never delay accepting connections or
  answering `PING`, because io threads never run tasks.
* The task queue is *bounded* and its back-pressure is visible to clients
  (`QUEUE_FULL`). Asio's internal queue is unbounded.
* The pool, queue and job state machine are explicit, testable code rather
  than library internals, which is the point of the project.

## 2. Non-blocking admission (`try_post`) instead of blocking producers

An io thread serves many connections. If `SUBMIT_JOB` blocked on a full queue,
every connection on that thread would freeze. Rejecting with `QUEUE_FULL`
keeps the server responsive and pushes retry policy to clients, which know
whether they can wait. The blocking `ThreadPool::post` and `submit` still
exist for in-process users, and the tests exercise them.

## 3. One mutex per job, leaf-only locking

Alternatives were a single registry-wide mutex (simple, but every status query
contends with every state transition) or lock-free job state (atomics plus
hand-written state machines; hard to make timestamps, results and status
consistent). A small `std::mutex` per `Job`, with a strict "never hold two
data locks" rule, gives:

* consistent snapshots (status, timestamps and result always agree);
* contention limited to the threads touching *that* job;
* deadlock freedom by construction (no lock-order graph to maintain).

## 4. Strands instead of mutexes for sessions

Each connection's state is touched only from its strand, so the session code
reads like single-threaded code. Other threads interact by `post`ing. This is
simpler and faster than locking around socket operations. It also makes
"never block a worker on the network" structural rather than a convention.

## 5. Push notification *and* polling

`GET_JOB_STATUS` polling is simple and stateless. `notify: true` shows the
harder case of delivering results from worker threads to the *right* client
safely: through a `weak_ptr` to the session, posted onto its strand. The CLI
uses events for `submit --wait` and polling for `wait --job-id`.

## 6. Cooperative cancellation with `std::stop_token`

Threads cannot be safely killed in C++. Tasks check a `stop_token` between
bounded units of work (one I/O chunk, 4096 loop iterations). Queued jobs are
cancelled instantly; running jobs stop within one chunk. The documented
consequence: a task that finishes before it observes the request is reported
`COMPLETED`.

## 7. Bounded everything

| Resource | Bound |
|---|---|
| queued tasks | `queue_capacity` |
| running tasks | `worker_threads` |
| finished jobs kept in memory | `max_retained_jobs` (FIFO eviction) |
| request size | `max_message_bytes`, checked from the header |
| per-connection outgoing bytes | `max_pending_write_bytes` |
| connections | `max_connections` |
| JSON depth | 32 |
| decompressed output | `max_decompressed_bytes` (per job, clamped by the server) |
| compute work | `max_compute_limit` |

Memory use is therefore bounded by configuration, not by client behaviour.

## 8. Crypto and compression libraries

* **SHA-256: Mbed TLS 3.6 LTS.** A widely deployed, audited library that
  builds from source with CMake on every platform. OpenSSL is the usual
  choice on Linux, but it cannot be built through CMake `FetchContent`, which
  would make the "clean checkout → build" path depend on system packages.
  Only `mbedcrypto` is built, and only its SHA-256 module is used.
* **zlib 1.3.2** for deflate. zlib and gzip containers are supported, with
  auto-detection on decompression.
* All dependencies are pinned to exact releases with SHA-256 archive hashes
  (`cmake/Dependencies.cmake`).

## 9. Atomic file output

Outputs are written to `.<name>.<random>.ccpart` next to the destination,
created with exclusive-create (`"wbx"`), and published by rename only after
success. Without `overwrite`, publishing uses a *no-replace* primitive
(`link()` on POSIX, `MoveFileExW` without `REPLACE_EXISTING` on Windows).
"Check if exists, then rename" is not used, because it races with a
concurrent job writing the same output.

## 10. Custom benchmark harness instead of Google Benchmark

Google Benchmark measures the throughput of one function in a loop. The
questions here are about a *system*: per-job latency distributions
(p50/p95/p99) under queueing, scaling across worker counts, process CPU
utilization, and end-to-end versus in-process throughput. A purpose-built
harness answers these directly and writes CSV plus Markdown.

## 11. Things deliberately left out

* **Authentication and TLS.** The server binds to loopback by default and
  warns when bound elsewhere. It is not meant to be exposed to untrusted
  networks.
* **File upload over the protocol.** Tasks operate on files already in the
  workspace. The protocol does not pretend otherwise.
* **Persistence.** Job state is in memory and lost on restart.
* **Priority scheduling and work stealing.** One FIFO queue keeps fairness and
  reasoning simple. See README "Future improvements".

## 12. A real ODR bug found during development

While building the integration suite with GCC 16 on Windows (MinGW-w64,
winpthreads), one test hung during server destruction. The hang was
intermittent and depended on the build: adding an *unrelated, never-executed*
test to the same file made a different test hang on its sixth
repetition. Clang/libc++ builds, and AddressSanitizer runs, were clean.

What the investigation found:

1. gdb showed the main thread stuck in Asio's `win_thread::join()` inside
   `~io_context`, waiting on handle `0x29`. The thread descriptor it read held
   UTF-16 text (`\and…`, part of a temp-directory path). PowerShell's thread
   list showed that Asio's timer thread had **never been started**.
2. Asio only starts that thread `if (!timer_thread_.joinable())`. So
   `timer_thread_` already held garbage right after construction: the
   constructor and the member functions disagreed about what the field was.
3. The cause is in Asio's auto-configuration. `asio::detail::thread` becomes
   `posix_thread` if `_POSIX_THREADS` is defined **when Asio's config header is
   first included**, and `win_thread` otherwise. On MinGW, winpthreads
   defines `_POSIX_THREADS`, and libstdc++'s `<thread>`/`<mutex>` include it.
   Translation units that included `<thread>` before `<asio.hpp>` (e.g. the
   session, via `thread_pool.hpp`) compiled one layout; the test file, which
   included `<asio.hpp>` first via `client.hpp`, compiled the other. That is an
   ODR violation. The linker keeps one copy of each inline function, so
   whether the program broke depended on which TU's copies it happened to
   keep. That explains why unrelated code changed the outcome.
4. A 10-line program printing `ASIO_HAS_PTHREADS` with and without a
   preceding `#include <thread>` confirmed it (0 vs 1).

**Fix:** pin the configuration for every TU in CMake (`ASIO_HAS_PTHREADS=1` on
MinGW), so include order no longer matters. The previously hanging test then
passed 100 out of 100 repetitions, and the full suite passed. The same
investigation, run under AddressSanitizer, also found a test-only bug: a
stack `Gate` destroyed before the pool whose worker was waiting on it. The
fix was to declare synchronization objects before the pool that uses them.

Lesson: header-only libraries that configure themselves from *other*
headers' macros are a source of include-order ODR bugs. Configuration
macros belong on the command line, applied uniformly to every TU.
