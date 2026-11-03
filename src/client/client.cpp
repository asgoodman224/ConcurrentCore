#include "concurrentcore/client/client.hpp"

#include "concurrentcore/networking/protocol.hpp"

#include <array>
#include <utility>

namespace concurrentcore {

using nlohmann::json;
using SteadyClock = std::chrono::steady_clock;

Client::Client(std::size_t max_message_bytes) : socket_(io_), decoder_(max_message_bytes) {}

Client::~Client() { close(); }

void Client::close() noexcept {
    std::error_code ignored;
    if (socket_.is_open()) {
        socket_.shutdown(asio::ip::tcp::socket::shutdown_both, ignored);
        socket_.close(ignored);
    }
}

void Client::run_until(const bool& done, SteadyClock::time_point deadline,
                       bool close_on_timeout) {
    io_.restart();
    while (!done) {
        const auto now = SteadyClock::now();
        if (now >= deadline) {
            break;
        }
        io_.run_one_for(deadline - now);
    }
    if (!done) {
        // Timed out. Abort the pending operation and run the loop until its
        // handler has executed, so it never touches destroyed stack state.
        // A cancelled read loses no data (unread bytes stay in the kernel
        // buffer); an interrupted write or connect leaves the stream in an
        // unknown state, so those close the connection.
        if (close_on_timeout) {
            close();
        } else {
            std::error_code ignored;
            socket_.cancel(ignored);
        }
        io_.restart();
        while (!done) {
            io_.run_one();
        }
        throw ClientTimeout();
    }
}

void Client::connect(const std::string& host, std::uint16_t port, Millis timeout) {
    close();
    const auto deadline = SteadyClock::now() + timeout;
    asio::ip::tcp::resolver resolver(io_);
    std::error_code ec;
    const auto endpoints = resolver.resolve(host, std::to_string(port), ec);
    if (ec) {
        throw ClientError("cannot resolve " + host + ": " + ec.message());
    }
    bool done = false;
    std::error_code result;
    asio::async_connect(socket_, endpoints,
                        [&](std::error_code e, const asio::ip::tcp::endpoint&) {
                            result = e;
                            done = true;
                        });
    run_until(done, deadline, /*close_on_timeout=*/true);
    if (result) {
        close();
        throw ClientError("cannot connect to " + host + ":" + std::to_string(port) + ": " +
                          result.message());
    }
    socket_.set_option(asio::ip::tcp::no_delay(true), ec);
    decoder_ = net::FrameDecoder(decoder_.max_payload_bytes());
    pending_events_.clear();
}

void Client::send_payload(std::string_view payload, Millis timeout) {
    if (!socket_.is_open()) {
        throw ClientError("not connected");
    }
    const std::string frame = net::encode_frame(payload);
    bool done = false;
    std::error_code result;
    asio::async_write(socket_, asio::buffer(frame), [&](std::error_code e, std::size_t) {
        result = e;
        done = true;
    });
    run_until(done, SteadyClock::now() + timeout, /*close_on_timeout=*/true);
    if (result) {
        close();
        throw ClientError("send failed: " + result.message());
    }
}

json Client::read_message(Millis timeout) {
    const auto deadline = SteadyClock::now() + timeout;
    std::array<char, 16 * 1024> buffer{};
    for (;;) {
        net::FrameDecoder::Result frame = decoder_.next();
        if (frame.status == net::FrameDecoder::Status::Frame) {
            try {
                return protocol::parse(frame.payload);
            } catch (const CoreError& e) {
                throw ClientError(std::string("invalid message from server: ") + e.what());
            }
        }
        if (frame.status == net::FrameDecoder::Status::TooLarge) {
            close();
            throw ClientError("server message exceeds size limit");
        }
        if (!socket_.is_open()) {
            throw ClientError("not connected");
        }
        bool done = false;
        std::error_code result;
        std::size_t received = 0;
        socket_.async_read_some(asio::buffer(buffer), [&](std::error_code e, std::size_t n) {
            result = e;
            received = n;
            done = true;
        });
        run_until(done, deadline, /*close_on_timeout=*/false);
        if (result) {
            close();
            throw ClientError(result == asio::error::eof ? "connection closed by server"
                                                         : "receive failed: " + result.message());
        }
        decoder_.feed(std::string_view(buffer.data(), received));
    }
}

json Client::request(json request, Millis timeout) {
    const auto deadline = SteadyClock::now() + timeout;
    if (!request.contains("request_id")) {
        request["request_id"] = next_request_id_++;
    }
    const json id = request["request_id"];
    send_payload(protocol::serialize(request), timeout);

    for (;;) {
        const auto remaining = std::chrono::duration_cast<Millis>(deadline - SteadyClock::now());
        if (remaining.count() <= 0) {
            throw ClientTimeout();
        }
        json message = read_message(remaining);
        if (message.value("kind", "") == "event") {
            pending_events_.push_back(std::move(message));
            continue;
        }
        // Responses to unrelated (e.g. earlier timed-out) requests are skipped.
        // A connection-level error (request_id null) is returned as-is.
        if (message.value("request_id", json()) == id || message.value("request_id", json()).is_null()) {
            return message;
        }
    }
}

std::optional<json> Client::wait_for_event(Millis timeout) {
    const auto deadline = SteadyClock::now() + timeout;
    while (pending_events_.empty()) {
        const auto remaining = std::chrono::duration_cast<Millis>(deadline - SteadyClock::now());
        if (remaining.count() <= 0) {
            return std::nullopt;
        }
        json message;
        try {
            message = read_message(remaining);
        } catch (const ClientTimeout&) {
            return std::nullopt;
        }
        if (message.value("kind", "") == "event") {
            pending_events_.push_back(std::move(message));
        }
    }
    json event = std::move(pending_events_.front());
    pending_events_.pop_front();
    return event;
}

json Client::ping(Millis timeout) { return request({{"type", "PING"}}, timeout); }

json Client::submit(std::string_view task_type, json params, bool notify, Millis timeout) {
    return request({{"type", "SUBMIT_JOB"},
                    {"task_type", task_type},
                    {"params", std::move(params)},
                    {"notify", notify}},
                   timeout);
}

json Client::status(std::uint64_t job_id, Millis timeout) {
    return request({{"type", "GET_JOB_STATUS"}, {"job_id", job_id}}, timeout);
}

json Client::result(std::uint64_t job_id, Millis timeout) {
    return request({{"type", "GET_JOB_RESULT"}, {"job_id", job_id}}, timeout);
}

json Client::cancel(std::uint64_t job_id, Millis timeout) {
    return request({{"type", "CANCEL_JOB"}, {"job_id", job_id}}, timeout);
}

json Client::stats(Millis timeout) { return request({{"type", "GET_SERVER_STATS"}}, timeout); }

}  // namespace concurrentcore
