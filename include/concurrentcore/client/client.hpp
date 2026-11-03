#pragma once

#include "concurrentcore/networking/frame.hpp"

#include <asio.hpp>
#include <nlohmann/json.hpp>

#include <chrono>
#include <cstdint>
#include <deque>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>

namespace concurrentcore {

/// Connection, timeout or protocol failure on the client side.
class ClientError : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

/// The operation did not complete before its deadline.
class ClientTimeout : public ClientError {
public:
    ClientTimeout() : ClientError("operation timed out") {}
};

/// Synchronous client for the ConcurrentCore protocol.
///
/// Every operation takes a timeout; internally the client issues an
/// asynchronous operation and drives its private io_context with run_for(),
/// cancelling the socket if the deadline passes.
///
/// Responses are matched to requests by request_id. Events (JOB_FINISHED)
/// that arrive while waiting for a response are buffered and returned by
/// wait_for_event().
///
/// Not thread-safe: use one Client per thread.
class Client {
public:
    using Millis = std::chrono::milliseconds;
    static constexpr Millis kDefaultTimeout{10'000};

    explicit Client(std::size_t max_message_bytes = net::kDefaultMaxMessageBytes);
    ~Client();
    Client(const Client&) = delete;
    Client& operator=(const Client&) = delete;
    Client(Client&&) = delete;
    Client& operator=(Client&&) = delete;

    void connect(const std::string& host, std::uint16_t port, Millis timeout = kDefaultTimeout);
    void close() noexcept;
    [[nodiscard]] bool is_connected() const noexcept { return socket_.is_open(); }

    /// Sends `request` (assigning a request_id if absent) and returns the
    /// matching response object.
    [[nodiscard]] nlohmann::json request(nlohmann::json request, Millis timeout = kDefaultTimeout);

    /// Returns the next event, waiting up to `timeout`. nullopt on timeout.
    [[nodiscard]] std::optional<nlohmann::json> wait_for_event(Millis timeout);

    // Convenience wrappers around request().
    [[nodiscard]] nlohmann::json ping(Millis timeout = kDefaultTimeout);
    [[nodiscard]] nlohmann::json submit(std::string_view task_type, nlohmann::json params,
                                        bool notify = false, Millis timeout = kDefaultTimeout);
    [[nodiscard]] nlohmann::json status(std::uint64_t job_id, Millis timeout = kDefaultTimeout);
    [[nodiscard]] nlohmann::json result(std::uint64_t job_id, Millis timeout = kDefaultTimeout);
    [[nodiscard]] nlohmann::json cancel(std::uint64_t job_id, Millis timeout = kDefaultTimeout);
    [[nodiscard]] nlohmann::json stats(Millis timeout = kDefaultTimeout);

    /// Low-level: send one already-serialized payload as a frame.
    void send_payload(std::string_view payload, Millis timeout = kDefaultTimeout);
    /// Low-level: read the next message (response or event).
    [[nodiscard]] nlohmann::json read_message(Millis timeout = kDefaultTimeout);

private:
    // Runs the io_context until `done` is set or the deadline passes.
    // Runs the io_context until `done` is set or the deadline passes. On
    // timeout the pending operation is aborted (socket cancelled, or closed
    // when `close_on_timeout`) and ClientTimeout is thrown.
    void run_until(const bool& done, std::chrono::steady_clock::time_point deadline,
                   bool close_on_timeout);

    asio::io_context io_;
    asio::ip::tcp::socket socket_;
    net::FrameDecoder decoder_;
    std::deque<nlohmann::json> pending_events_;
    std::int64_t next_request_id_ = 1;
};

}  // namespace concurrentcore
