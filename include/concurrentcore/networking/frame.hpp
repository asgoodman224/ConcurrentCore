#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <string_view>

namespace concurrentcore::net {

/// Wire framing: every message is a 4-byte unsigned big-endian (network
/// order) payload length followed by exactly that many bytes of UTF-8 JSON.
///
///   +--------+--------+--------+--------+------------------------+
///   |          length (uint32, BE)      |  payload (length bytes) |
///   +--------+--------+--------+--------+------------------------+
inline constexpr std::size_t kFrameHeaderBytes = 4;
inline constexpr std::size_t kDefaultMaxMessageBytes = 1024 * 1024;

/// Returns header + payload as one contiguous buffer.
/// Throws std::length_error if the payload does not fit in 32 bits.
[[nodiscard]] std::string encode_frame(std::string_view payload);

/// Incremental frame parser. TCP is a byte stream: one read may return part
/// of a header, several frames, or a frame split at any byte. feed() appends
/// whatever arrived; next() extracts complete frames one at a time.
///
/// The declared length is validated as soon as the 4 header bytes are
/// present, before any payload is buffered, so an attacker cannot make the
/// server allocate memory by announcing a huge frame. Buffered memory is
/// therefore bounded by max_payload + header + one read chunk.
///
/// After a TooLarge result the stream cannot be resynchronized (the payload
/// boundary is unknown); the decoder stays in the failed state and the
/// connection should be closed.
///
/// Not thread-safe: each connection owns its own decoder.
class FrameDecoder {
public:
    enum class Status {
        Frame,     ///< `payload` holds one complete message.
        NeedMore,  ///< Not enough bytes yet.
        TooLarge,  ///< Declared length exceeds the limit (terminal).
    };

    struct Result {
        Status status = Status::NeedMore;
        std::string payload;
        std::uint32_t declared_length = 0;  ///< Set for TooLarge.
    };

    explicit FrameDecoder(std::size_t max_payload_bytes = kDefaultMaxMessageBytes);

    void feed(std::string_view data);

    [[nodiscard]] Result next();

    [[nodiscard]] std::size_t buffered_bytes() const noexcept { return buffer_.size() - offset_; }
    [[nodiscard]] bool failed() const noexcept { return failed_; }
    [[nodiscard]] std::size_t max_payload_bytes() const noexcept { return max_payload_; }

private:
    void compact();

    std::size_t max_payload_;
    std::string buffer_;
    std::size_t offset_ = 0;  // start of unconsumed data in buffer_
    bool failed_ = false;
    std::uint32_t failed_length_ = 0;
};

}  // namespace concurrentcore::net
