#pragma once

#include "concurrentcore/jobs/job_manager.hpp"
#include "concurrentcore/server/request_handler.hpp"
#include "concurrentcore/server/server_config.hpp"
#include "concurrentcore/server/session.hpp"

#include <asio.hpp>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>

namespace concurrentcore {

/// Asynchronous TCP front end plus the job engine behind it.
///
/// Threads:
///   * `io_threads` threads run io_context::run(). They accept connections,
///     parse frames and answer requests. They never execute tasks.
///   * `worker_threads` threads in the JobManager's pool execute tasks.
///     They never perform blocking socket I/O; completion events are
///     posted back to the owning session's strand.
///
/// Shutdown order (shutdown()):
///   1. close the acceptor (no new connections);
///   2. JobManager::shutdown(drain_timeout): reject new jobs, drain or
///      cancel outstanding jobs, join workers. Existing connections keep
///      working meanwhile, so clients can poll status and receive events;
///   3. ask every session to flush and close;
///   4. wait (bounded) for sessions to close, stop the io_context, join
///      the io threads.
/// Each step completes before the next begins, so no worker can post to a
/// session after the event loop is gone.
class Server {
public:
    explicit Server(ServerConfig config, JobManager::Executor executor = {});
    ~Server();

    Server(const Server&) = delete;
    Server& operator=(const Server&) = delete;
    Server(Server&&) = delete;
    Server& operator=(Server&&) = delete;

    /// Binds, listens and starts the io threads. Throws on bind failure.
    void start();

    /// Graceful shutdown using config.shutdown_drain_timeout.
    void shutdown();
    /// Shutdown with an explicit drain timeout (0 = cancel immediately).
    /// Idempotent and thread-safe. Must not be called from an io thread.
    void shutdown(std::chrono::milliseconds drain_timeout);

    /// Actual bound port (useful when config.port == 0).
    [[nodiscard]] std::uint16_t port() const noexcept { return port_.load(); }
    [[nodiscard]] const ServerConfig& config() const noexcept { return config_; }
    [[nodiscard]] JobManager& jobs() noexcept { return *jobs_; }
    [[nodiscard]] std::size_t active_connections() const;
    [[nodiscard]] nlohmann::json connection_stats() const;

private:
    void do_accept();
    void on_accept(asio::ip::tcp::socket socket);
    void on_session_closed(std::uint64_t id);
    void run_io_thread();

    enum class State { Created, Running, Stopped };

    const ServerConfig config_;
    std::shared_ptr<const Workspace> workspace_;
    std::unique_ptr<JobManager> jobs_;

    asio::io_context io_;
    asio::ip::tcp::acceptor acceptor_;  // bound to a strand: accept handlers serialized
    asio::steady_timer accept_retry_timer_;
    RequestHandler handler_;

    mutable std::mutex sessions_mutex_;  // leaf lock
    std::condition_variable sessions_cv_;
    std::unordered_map<std::uint64_t, std::weak_ptr<Session>> sessions_;  // guarded

    std::atomic<std::uint64_t> next_session_id_{1};
    std::atomic<std::uint64_t> accepted_total_{0};
    std::atomic<std::uint64_t> rejected_total_{0};
    std::atomic<std::uint16_t> port_{0};

    std::mutex lifecycle_mutex_;  // serializes start/shutdown
    State state_ = State::Created;  // guarded by lifecycle_mutex_
    std::vector<std::thread> io_threads_;  // joined in shutdown
};

}  // namespace concurrentcore
