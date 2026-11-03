#include "concurrentcore/server/session.hpp"

#include "concurrentcore/networking/protocol.hpp"

#include <spdlog/spdlog.h>

#include <utility>

namespace concurrentcore {

Session::Session(asio::ip::tcp::socket socket, std::uint64_t id, Options options,
                 const RequestHandler& handler, ClosedCallback on_closed)
    : socket_(std::move(socket)),
      timer_(socket_.get_executor()),
      id_(id),
      options_(options),
      handler_(handler),
      on_closed_(std::move(on_closed)),
      decoder_(options.max_message_bytes) {}

void Session::start() {
    asio::post(socket_.get_executor(), [self = shared_from_this()] {
        std::error_code ignored;
        // Request/response protocol: disable Nagle so small responses are
        // not delayed waiting for more data.
        self->socket_.set_option(asio::ip::tcp::no_delay(true), ignored);
        self->event_sink_ = self->make_event_sink();
        if (self->options_.idle_timeout.count() > 0) {
            self->arm_timer(self->options_.idle_timeout);
        }
        self->do_read();
    });
}

void Session::reject(ErrorCode code, std::string message) {
    asio::post(socket_.get_executor(), [self = shared_from_this(), code, msg = std::move(message)] {
        self->enqueue(protocol::serialize(protocol::make_error("ERROR", nullptr, code, msg)));
        self->begin_graceful_close();
    });
}

void Session::deliver(std::string payload) {
    asio::post(socket_.get_executor(), [self = shared_from_this(), p = std::move(payload)]() mutable {
        if (self->closed_ || self->closing_) {
            return;  // connection is going away; drop the event
        }
        self->enqueue(std::move(p));
    });
}

void Session::close() {
    asio::post(socket_.get_executor(), [self = shared_from_this()] { self->begin_graceful_close(); });
}

RequestHandler::EventSink Session::make_event_sink() {
    // Weak reference: if the client has disconnected by the time a job
    // finishes, lock() fails and the event is dropped safely.
    return [weak = weak_from_this()](std::string payload) {
        if (const auto self = weak.lock()) {
            self->deliver(std::move(payload));
        }
    };
}

void Session::do_read() {
    if (closed_ || closing_ || read_in_flight_ || read_paused_) {
        return;
    }
    read_in_flight_ = true;
    socket_.async_read_some(asio::buffer(read_buffer_),
                            [self = shared_from_this()](std::error_code ec, std::size_t n) {
                                self->on_read(ec, n);
                            });
}

void Session::on_read(std::error_code ec, std::size_t bytes) {
    read_in_flight_ = false;
    if (closed_) {
        return;
    }
    if (ec) {
        if (ec == asio::error::eof) {
            // Peer half-closed: flush any responses still queued, then close.
            begin_graceful_close();
        } else if (ec != asio::error::operation_aborted) {
            close_now(ec.message());
        }
        return;
    }
    if (closing_) {
        return;  // input after close() is ignored
    }
    if (options_.idle_timeout.count() > 0) {
        arm_timer(options_.idle_timeout);
    }

    // A read may contain a partial frame, exactly one frame, or many; the
    // decoder reassembles them independent of read boundaries.
    decoder_.feed(std::string_view(read_buffer_.data(), bytes));
    process_frames();
    if (closed_ || closing_) {
        return;
    }
    if (pending_write_bytes_ > options_.max_pending_write_bytes / 2) {
        read_paused_ = true;  // resumed by on_write once the client catches up
        return;
    }
    do_read();
}

void Session::process_frames() {
    for (;;) {
        net::FrameDecoder::Result frame = decoder_.next();
        switch (frame.status) {
            case net::FrameDecoder::Status::NeedMore:
                return;
            case net::FrameDecoder::Status::TooLarge:
                // The stream cannot be resynchronized: report and close.
                spdlog::warn("session {}: frame of {} bytes exceeds limit of {}", id_,
                             frame.declared_length, options_.max_message_bytes);
                enqueue(protocol::serialize(protocol::make_error(
                    "ERROR", nullptr, ErrorCode::MessageTooLarge,
                    "message of " + std::to_string(frame.declared_length) +
                        " bytes exceeds limit of " + std::to_string(options_.max_message_bytes))));
                begin_graceful_close();
                return;
            case net::FrameDecoder::Status::Frame:
                enqueue(handler_.handle(frame.payload, event_sink_));
                if (closed_) {
                    return;  // enqueue may close a slow consumer
                }
                break;
        }
    }
}

void Session::enqueue(std::string payload) {
    if (closed_) {
        return;
    }
    std::string frame = net::encode_frame(payload);
    if (pending_write_bytes_ + frame.size() > options_.max_pending_write_bytes) {
        close_now("pending write limit exceeded (client is not reading)");
        return;
    }
    pending_write_bytes_ += frame.size();
    const bool write_idle = write_queue_.empty();
    write_queue_.push_back(std::move(frame));
    if (write_idle) {
        do_write();
    }
}

void Session::do_write() {
    // std::deque::push_back never invalidates references to existing
    // elements, so the buffer for front() stays valid while more frames are
    // appended during the write.
    asio::async_write(socket_, asio::buffer(write_queue_.front()),
                      [self = shared_from_this()](std::error_code ec, std::size_t) {
                          self->on_write(ec);
                      });
}

void Session::on_write(std::error_code ec) {
    if (closed_) {
        return;
    }
    if (ec) {
        close_now(ec.message());
        return;
    }
    pending_write_bytes_ -= write_queue_.front().size();
    write_queue_.pop_front();

    if (!write_queue_.empty()) {
        do_write();
    } else if (closing_) {
        close_now("closed after flushing responses");
        return;
    }
    if (read_paused_ && pending_write_bytes_ <= options_.max_pending_write_bytes / 4) {
        read_paused_ = false;
        do_read();
    }
}

void Session::arm_timer(std::chrono::steady_clock::duration after) {
    // expires_after() cancels any pending wait (its handler sees
    // operation_aborted), then a fresh wait is started.
    timer_.expires_after(after);
    timer_.async_wait([self = shared_from_this()](std::error_code ec) {
        if (ec == asio::error::operation_aborted) {
            return;
        }
        // A wait that had already expired cannot be cancelled and may run
        // after the timer was re-armed; ignore it if the deadline moved.
        if (self->timer_.expiry() > std::chrono::steady_clock::now()) {
            return;
        }
        self->on_timer();
    });
}

void Session::on_timer() {
    if (closed_) {
        return;
    }
    close_now(closing_ ? "flush timeout during close" : "idle timeout");
}

void Session::begin_graceful_close() {
    if (closed_ || closing_) {
        return;
    }
    closing_ = true;
    if (write_queue_.empty()) {
        close_now("closed");
        return;
    }
    arm_timer(options_.close_flush_timeout);
}

void Session::close_now(std::string_view reason) {
    if (closed_) {
        return;
    }
    closed_ = true;
    std::error_code ignored;
    socket_.shutdown(asio::ip::tcp::socket::shutdown_both, ignored);
    socket_.close(ignored);  // aborts the pending read/write
    timer_.cancel();
    // write_queue_ is deliberately NOT cleared: an aborted async_write may
    // still reference front() until its handler runs. The queue is freed
    // with the session, which that handler keeps alive.
    spdlog::debug("session {} closed: {}", id_, reason);
    if (on_closed_) {
        on_closed_(id_);
    }
}

}  // namespace concurrentcore
