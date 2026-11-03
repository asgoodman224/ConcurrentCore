#include "concurrentcore/networking/frame.hpp"

#include <gtest/gtest.h>

#include <random>
#include <string>
#include <vector>

using concurrentcore::net::encode_frame;
using concurrentcore::net::FrameDecoder;
using Status = FrameDecoder::Status;

TEST(Frame, HeaderIsBigEndianLength) {
    const std::string f = encode_frame(std::string(0x010203, 'x'));
    ASSERT_GE(f.size(), 4U);
    EXPECT_EQ(static_cast<unsigned char>(f[0]), 0x00);
    EXPECT_EQ(static_cast<unsigned char>(f[1]), 0x01);
    EXPECT_EQ(static_cast<unsigned char>(f[2]), 0x02);
    EXPECT_EQ(static_cast<unsigned char>(f[3]), 0x03);
    EXPECT_EQ(f.size(), 4U + 0x010203);
}

TEST(Frame, DecodesSingleFrame) {
    FrameDecoder d(1024);
    d.feed(encode_frame("hello"));
    auto r = d.next();
    ASSERT_EQ(r.status, Status::Frame);
    EXPECT_EQ(r.payload, "hello");
    EXPECT_EQ(d.next().status, Status::NeedMore);
    EXPECT_EQ(d.buffered_bytes(), 0U);
}

TEST(Frame, DecodesSeveralFramesFromOneFeed) {
    FrameDecoder d(1024);
    d.feed(encode_frame("a") + encode_frame("") + encode_frame("ccc"));
    EXPECT_EQ(d.next().payload, "a");
    auto empty = d.next();
    EXPECT_EQ(empty.status, Status::Frame);
    EXPECT_TRUE(empty.payload.empty());
    EXPECT_EQ(d.next().payload, "ccc");
    EXPECT_EQ(d.next().status, Status::NeedMore);
}

TEST(Frame, ReassemblesByteByByte) {
    FrameDecoder d(1024);
    const std::string wire = encode_frame("{\"type\":\"PING\"}");
    for (std::size_t i = 0; i + 1 < wire.size(); ++i) {
        d.feed(std::string_view(&wire[i], 1));
        ASSERT_EQ(d.next().status, Status::NeedMore) << "byte " << i;
    }
    d.feed(std::string_view(&wire.back(), 1));
    auto r = d.next();
    ASSERT_EQ(r.status, Status::Frame);
    EXPECT_EQ(r.payload, "{\"type\":\"PING\"}");
}

TEST(Frame, AcceptsExactlyTheLimit) {
    FrameDecoder d(16);
    d.feed(encode_frame(std::string(16, 'x')));
    EXPECT_EQ(d.next().status, Status::Frame);
}

TEST(Frame, RejectsOversizedFrameFromHeaderAloneAndStaysFailed) {
    FrameDecoder d(16);
    // Only the header: a 4 GiB claim must be rejected without waiting for
    // (or buffering) any payload.
    const std::string header("\xFF\xFF\xFF\xFF", 4);
    d.feed(header);
    auto r = d.next();
    EXPECT_EQ(r.status, Status::TooLarge);
    EXPECT_EQ(r.declared_length, 0xFFFFFFFFU);
    EXPECT_TRUE(d.failed());
    d.feed(encode_frame("ok"));
    EXPECT_EQ(d.next().status, Status::TooLarge);  // cannot resynchronize
    EXPECT_EQ(d.buffered_bytes(), 0U);
}

TEST(Frame, RandomSplitsReproduceTheOriginalMessages) {
    std::mt19937 rng(12345);  // fixed seed: reproducible
    std::vector<std::string> messages;
    std::string wire;
    for (int i = 0; i < 300; ++i) {
        std::string m(std::uniform_int_distribution<std::size_t>(0, 3000)(rng), '\0');
        for (auto& c : m) {
            c = static_cast<char>(std::uniform_int_distribution<int>(0, 255)(rng));
        }
        wire += encode_frame(m);
        messages.push_back(std::move(m));
    }
    FrameDecoder d(4096);
    std::vector<std::string> decoded;
    std::size_t pos = 0;
    while (pos < wire.size()) {
        const std::size_t n =
            std::min(wire.size() - pos, std::uniform_int_distribution<std::size_t>(1, 5000)(rng));
        d.feed(std::string_view(wire).substr(pos, n));
        pos += n;
        for (auto r = d.next(); r.status == Status::Frame; r = d.next()) {
            decoded.push_back(std::move(r.payload));
        }
    }
    EXPECT_EQ(decoded, messages);
}
