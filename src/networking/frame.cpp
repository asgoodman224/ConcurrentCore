#include "concurrentcore/networking/frame.hpp"

#include <limits>
#include <stdexcept>

namespace concurrentcore::net {

std::string encode_frame(std::string_view payload) {
    if (payload.size() > std::numeric_limits<std::uint32_t>::max()) {
        throw std::length_error("frame payload exceeds 4 GiB");
    }
    const auto n = static_cast<std::uint32_t>(payload.size());
    std::string out;
    out.reserve(kFrameHeaderBytes + payload.size());
    out.push_back(static_cast<char>((n >> 24) & 0xFFU));
    out.push_back(static_cast<char>((n >> 16) & 0xFFU));
    out.push_back(static_cast<char>((n >> 8) & 0xFFU));
    out.push_back(static_cast<char>(n & 0xFFU));
    out.append(payload);
    return out;
}

FrameDecoder::FrameDecoder(std::size_t max_payload_bytes) : max_payload_(max_payload_bytes) {}

void FrameDecoder::feed(std::string_view data) {
    if (failed_ || data.empty()) {
        return;  // a failed stream is never parsed again; drop input
    }
    compact();
    buffer_.append(data.data(), data.size());
}

void FrameDecoder::compact() {
    // Reclaim consumed prefix once it dominates the buffer, keeping
    // amortized O(1) cost per byte without shifting on every frame.
    if (offset_ > 0 && (offset_ == buffer_.size() || offset_ >= buffer_.size() / 2)) {
        buffer_.erase(0, offset_);
        offset_ = 0;
    }
}

FrameDecoder::Result FrameDecoder::next() {
    if (failed_) {
        return Result{Status::TooLarge, {}, failed_length_};
    }
    if (buffered_bytes() < kFrameHeaderBytes) {
        return Result{};
    }
    const auto* h = reinterpret_cast<const unsigned char*>(buffer_.data() + offset_);
    const std::uint32_t length = (static_cast<std::uint32_t>(h[0]) << 24) |
                                 (static_cast<std::uint32_t>(h[1]) << 16) |
                                 (static_cast<std::uint32_t>(h[2]) << 8) |
                                 static_cast<std::uint32_t>(h[3]);
    if (length > max_payload_) {
        failed_ = true;
        failed_length_ = length;
        buffer_.clear();
        buffer_.shrink_to_fit();
        offset_ = 0;
        return Result{Status::TooLarge, {}, length};
    }
    if (buffered_bytes() < kFrameHeaderBytes + length) {
        return Result{};
    }
    Result r;
    r.status = Status::Frame;
    r.payload.assign(buffer_, offset_ + kFrameHeaderBytes, length);
    offset_ += kFrameHeaderBytes + length;
    if (offset_ == buffer_.size()) {
        buffer_.clear();
        offset_ = 0;
    }
    return r;
}

}  // namespace concurrentcore::net
