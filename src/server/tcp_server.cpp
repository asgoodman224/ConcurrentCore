#include "concurrentcore/server/tcp_server.hpp"

#include <spdlog/spdlog.h>

#include <filesystem>
#include <future>
#include <stdexcept>

namespace concurrentcore {
namespace {

std::shared_ptr<const Workspace> make_workspace(const std::filesystem::path& dir) {
    std::error_code ec;
    std::filesystem::create_directories(dir, ec);
    if (ec) {
        throw std::invalid_argument("cannot create workspace directory '" + dir.string() +
                                    "': " + ec.message());
    }
    return std::make_shared<const Workspace>(dir);
}

// Marks io threads so shutdown() can detect (and refuse) a self-join.
thread_local const Server* t_io_thread_owner = nullptr;

ServerConfig validated(ServerConfig c) {
    validate(c);
    return c;
}

JobManagerConfig job_config(const ServerConfig& c) {
    JobManagerConfig jc;
    jc.worker_count = c.worker_threads;
    jc.queue_capacity = c.queue_capacity;
    jc.max_retained_jobs = c.max_retained_jobs;
    jc.limits = c.task_limits;
    return jc;
}

}  // namespace

Server::Server(ServerConfig config, JobManager::Executor executor)
    : config_(validated(std::move(config))),
      workspace_(make_workspace(config_.workspace)),
      jobs_(std::make_unique<JobManager>(job_config(config_), workspace_, std::move(executor))),
      acceptor_(asio::make_strand(io_)),
      accept_retry_timer_(acceptor_.get_executor()),
      handler_(*jobs_, [this] { return connection_stats(); }) {}

Server::~Server() {
    try {
        shutdown();
    } catch (const std::exception& e) {
        spdlog::critical("Server destructor: shutdown failed: {}", e.what());
    }
}

void Server::start() {
    std::lock_guard lifecycle(lifecycle_mutex_);
    if (state_ != State::Created) {
        throw std::logic_error("Server::start() may only be called once");
    }

    asio::ip::tcp::resolver resolver(io_);
    const auto endpoints = resolver.resolve(config_.host, std::to_string(config_.port),
                                            asio::ip::resolver_base::passive);
    if (endpoints.empty()) {
        throw std::runtime_error("cannot resolve listen address " + config_.host);
    }
    const asio::ip::tcp::endpoint endpoint = endpoints.begin()->endpoint();

    acceptor_.open(endpoint.protocol());
#ifndef _WIN32
    // On POSIX this only allows rebinding a port in TIME_WAIT. (On Windows
    // SO_REUSEADDR would let another process steal the port, so skip it.)
    acceptor_.set_option(asio::ip::tcp::acceptor::reuse_address(true));
#endif
    acceptor_.bind(endpoint);
    acceptor_.listen(asio::socket_base::max_listen_connections);
    port_.store(acceptor_.local_endpoint().port());

    if (!endpoint.address().is_loopback()) {
        spdlog::warn("listening on non-loopback address {}: the server has no authentication",
                     endpoint.address().to_string());
    }

    // Queue the first accept BEFORE starting the io threads so run() has
    // work and does not return immediately.
    do_accept();
    io_threads_.reserve(config_.io_threads);
    for (std::size_t i = 0; i < config_.io_threads; ++i) {
        io_threads_.emplace_back([this] { run_io_thread(); });
    }
    state_ = State::Running;
    spdlog::info("ConcurrentCore listening on {}:{} (io_threads={}, workers={}, queue={}, workspace={})",
                 endpoint.address().to_string(), port(), config_.io_threads,
                 config_.worker_threads, config_.queue_capacity, workspace_->root().string());
}

void Server::run_io_thread() {
    t_io_thread_owner = this;
    // Handlers are written not to throw; if one does anyway, log it and
    // keep the event loop alive rather than terminating the process.
    for (;;) {
        try {
            io_.run();
            return;
        } catch (const std::exception& e) {
            spdlog::error("unhandled exception in io thread: {}", e.what());
        }
    }
}

void Server::do_accept() {
    // Each accepted socket gets its own strand; that strand becomes the
    // session's serialization domain.
    acceptor_.async_accept(asio::make_strand(io_), [this](std::error_code ec,
                                                          asio::ip::tcp::socket socket) {
        if (!acceptor_.is_open()) {
            return;  // shutting down
        }
        if (ec) {
            if (ec == asio::error::operation_aborted) {
                return;
            }
            // Typically resource exhaustion (EMFILE). Back off briefly
            // instead of spinning on the error.
            spdlog::warn("accept failed: {}; retrying in 100 ms", ec.message());
            accept_retry_timer_.expires_after(std::chrono::milliseconds(100));
            accept_retry_timer_.async_wait([this](std::error_code timer_ec) {
                if (!timer_ec && acceptor_.is_open()) {
                    do_accept();
                }
            });
            return;
        }
        on_accept(std::move(socket));
        do_accept();
    });
}

void Server::on_accept(asio::ip::tcp::socket socket) {
    const std::uint64_t id = next_session_id_.fetch_add(1, std::memory_order_relaxed);
    Session::Options options;
    options.max_message_bytes = config_.max_message_bytes;
    options.max_pending_write_bytes = config_.max_pending_write_bytes;
    options.idle_timeout = config_.idle_timeout;

    auto session = std::make_shared<Session>(std::move(socket), id, options, handler_,
                                             [this](std::uint64_t sid) { on_session_closed(sid); });
    bool admitted = false;
    {
        std::lock_guard lock(sessions_mutex_);
        if (sessions_.size() < config_.max_connections) {
            sessions_.emplace(id, session);
            admitted = true;
        }
    }
    // Session calls happen after the lock is released (they only post).
    if (admitted) {
        accepted_total_.fetch_add(1, std::memory_order_relaxed);
        spdlog::debug("session {} accepted", id);
        session->start();
    } else {
        rejected_total_.fetch_add(1, std::memory_order_relaxed);
        spdlog::warn("connection limit ({}) reached; rejecting connection", config_.max_connections);
        session->reject(ErrorCode::ServerBusy, "connection limit reached");
    }
}

void Server::on_session_closed(std::uint64_t id) {
    bool now_empty = false;
    {
        std::lock_guard lock(sessions_mutex_);
        sessions_.erase(id);
        now_empty = sessions_.empty();
    }
    if (now_empty) {
        sessions_cv_.notify_all();
    }
}

std::size_t Server::active_connections() const {
    std::lock_guard lock(sessions_mutex_);
    return sessions_.size();
}

nlohmann::json Server::connection_stats() const {
    return {
        {"connections",
         {{"active", active_connections()},
          {"accepted_total", accepted_total_.load(std::memory_order_relaxed)},
          {"rejected_total", rejected_total_.load(std::memory_order_relaxed)}}},
        {"io_threads", config_.io_threads},
    };
}

void Server::shutdown() { shutdown(config_.shutdown_drain_timeout); }

void Server::shutdown(std::chrono::milliseconds drain_timeout) {
    // Joining the io threads from one of them would self-deadlock.
    if (t_io_thread_owner == this) {
        throw std::logic_error("Server::shutdown() called from an io thread");
    }
    std::lock_guard lifecycle(lifecycle_mutex_);
    if (state_ == State::Stopped) {
        return;
    }
    const bool was_running = state_ == State::Running;
    state_ = State::Stopped;

    if (!was_running) {
        jobs_->shutdown(drain_timeout);
        return;
    }
    spdlog::info("shutting down (drain timeout {} ms)", drain_timeout.count());

    // 1. Stop accepting. The acceptor lives on a strand, so close it there
    //    and wait until that has happened.
    std::promise<void> acceptor_closed;
    asio::post(acceptor_.get_executor(), [this, &acceptor_closed] {
        std::error_code ignored;
        acceptor_.close(ignored);
        accept_retry_timer_.cancel();
        acceptor_closed.set_value();
    });
    acceptor_closed.get_future().wait();

    // 2. Drain (or cancel) jobs and join the workers. Sessions stay up so
    //    clients can still query status and receive completion events.
    jobs_->shutdown(drain_timeout);

    // 3. Ask every session to flush its queued responses and close.
    std::vector<std::shared_ptr<Session>> live;
    {
        std::lock_guard lock(sessions_mutex_);
        for (const auto& [id, weak] : sessions_) {
            if (auto s = weak.lock()) {
                live.push_back(std::move(s));
            }
        }
    }
    for (const auto& s : live) {
        s->close();
    }
    live.clear();

    // 4. Bounded wait for sessions to finish flushing, then stop the loop.
    {
        std::unique_lock lock(sessions_mutex_);
        sessions_cv_.wait_for(lock, std::chrono::seconds(3), [this] { return sessions_.empty(); });
    }
    io_.stop();
    for (auto& t : io_threads_) {
        t.join();
    }
    io_threads_.clear();
    spdlog::info("shutdown complete");
}

}  // namespace concurrentcore
