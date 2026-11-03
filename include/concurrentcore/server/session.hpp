#pragma once

#include "concurrentcore/common/error.hpp"
#include "concurrentcore/networking/frame.hpp"
#include "concurrentcore/server/request_handler.hpp"

#include <asio.hpp>

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <string>
#include <string_view>

namespace concurrentcore {

/// One client connection.
///
/// Concurrency model: the socket is bound to its own strand, and every
/// member below that is not const is touched ONLY from handlers running on
/// that strand. The strand serializes them, so the session needs no mutex.
/// The two entry points callable from other threads, deliver() and
/// close(), do nothing but asio::post() a lambda onto the strand.
///
/// Lifetime: every pending async operation holds a shared_ptr to the
/// session, so it lives exactly as long as there is I/O in flight. Worker
/// threads reach a session only through a weak_ptr (see make_event_sink),
/// so a finished job never keeps a dead connection alive and never touches
/// a destroyed one.
///
/// Back-pressure: responses are queued per connection. When the queued
/// bytes exceed half of max_pending_write_bytes the session stops reading
/// new requests until the client drains below a quarter; if queued bytes
/// would exceed the hard limit (a client that never reads), the
/// connection is dropped. Worker threads are never blocked by a slow client.
class Session : public std::enable_shared_from_this<Session> {
public:
    struct Options {
        std::size_t max_message_bytes = net::kDefaultMaxMessageBytes;
        std::size_t max_pending_write_bytes = 8 * 1024 * 1024;
        std::chrono::seconds idle_timeout{300};  ///< 0 disables
        std::chrono::milliseconds close_flush_timeout{2000};
    };

    using ClosedCallback = std::function<void(std::uint64_t session_id)>;

    Session(asio::ip::tcp::socket socket, std::uint64_t id, Options options,
            const RequestHandler& handler, ClosedCallback on_closed);

    Session(const Session&) = delete;
    Session& operator=(const Session&) = delete;
    Session(Session&&) = delete;
    Session& operator=(Session&&) = delete;
    ~Session() = default;

    /// Begins reading requests. Thread-safe.
    void start();

    /// Sends one error response and closes (used when over the connection
    /// limit). Thread-safe.
    void reject(ErrorCode code, std::string message);

    /// Queues a serialized message for sending. Thread-safe, non-blocking.
    void deliver(std::string payload);

    /// Flushes queued writes (bounded by close_flush_timeout) then closes.
    /// Thread-safe, idempotent.
    void close();

    [[nodiscard]] std::uint64_t id() const noexcept { return id_; }

private:
    // --- strand-only below ---
    void do_read();
    void on_read(std::error_code ec, std::size_t bytes);
    void process_frames();
    void enqueue(std::string payload);
    void do_write();
    void on_write(std::error_code ec);
    void arm_timer(std::chrono::steady_clock::duration after);
    void on_timer();
    void begin_graceful_close();
    void close_now(std::string_view reason);
    [[nodiscard]] RequestHandler::EventSink make_event_sink();

    asio::ip::tcp::socket socket_;  // its executor is this session's strand
    asio::steady_timer timer_;      // idle timeout / close-flush deadline
    const std::uint64_t id_;
    const Options options_;
    const RequestHandler& handler_;
    const ClosedCallback on_closed_;

    std::array<char, 16 * 1024> read_buffer_{};
    net::FrameDecoder decoder_;
    RequestHandler::EventSink event_sink_;
    std::deque<std::string> write_queue_;  // front() is being written
    std::size_t pending_write_bytes_ = 0;
    bool read_in_flight_ = false;
    bool read_paused_ = false;
    bool closing_ = false;
    bool closed_ = false;
};

}  // namespace concurrentcore
