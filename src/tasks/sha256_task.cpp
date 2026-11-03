#include "concurrentcore/tasks/tasks.hpp"

#include "file_io.hpp"

#include <mbedtls/sha256.h>

#include <vector>

namespace concurrentcore {

struct Sha256Hasher::Impl {
    mbedtls_sha256_context ctx{};
    bool finished = false;
};

Sha256Hasher::Sha256Hasher() : impl_(std::make_unique<Impl>()) {
    mbedtls_sha256_init(&impl_->ctx);
    if (mbedtls_sha256_starts(&impl_->ctx, /*is224=*/0) != 0) {
        mbedtls_sha256_free(&impl_->ctx);
        throw CoreError(ErrorCode::InternalError, "mbedtls_sha256_starts failed");
    }
}

Sha256Hasher::~Sha256Hasher() { mbedtls_sha256_free(&impl_->ctx); }

void Sha256Hasher::update(std::span<const std::byte> data) {
    if (impl_->finished) {
        throw CoreError(ErrorCode::InternalError, "Sha256Hasher::update after finish");
    }
    if (data.empty()) {
        return;
    }
    if (mbedtls_sha256_update(&impl_->ctx, reinterpret_cast<const unsigned char*>(data.data()),
                              data.size()) != 0) {
        throw CoreError(ErrorCode::InternalError, "mbedtls_sha256_update failed");
    }
}

void Sha256Hasher::update(std::string_view data) { update(std::as_bytes(std::span(data))); }

std::string Sha256Hasher::finish_hex() {
    if (impl_->finished) {
        throw CoreError(ErrorCode::InternalError, "Sha256Hasher::finish_hex called twice");
    }
    std::array<unsigned char, 32> digest{};
    if (mbedtls_sha256_finish(&impl_->ctx, digest.data()) != 0) {
        throw CoreError(ErrorCode::InternalError, "mbedtls_sha256_finish failed");
    }
    impl_->finished = true;

    static constexpr char kHex[] = "0123456789abcdef";
    std::string hex;
    hex.reserve(digest.size() * 2);
    for (const unsigned char b : digest) {
        hex.push_back(kHex[b >> 4]);
        hex.push_back(kHex[b & 0x0F]);
    }
    return hex;
}

std::string sha256_hex(std::string_view data) {
    Sha256Hasher hasher;
    hasher.update(data);
    return hasher.finish_hex();
}

nlohmann::json run_sha256(const Sha256Params& p, const TaskContext& ctx) {
    const auto path = ctx.workspace.resolve_input(p.file);
    detail::InputFile in(path);
    Sha256Hasher hasher;

    // Fixed-size buffer: memory use is independent of file size.
    std::vector<char> buffer(ctx.limits.io_chunk_bytes);
    std::uint64_t total = 0;
    for (;;) {
        ctx.throw_if_cancelled();
        const std::size_t n = in.read(buffer);
        if (n == 0) {
            break;
        }
        hasher.update(std::string_view(buffer.data(), n));
        total += n;
    }
    return {
        {"file", p.file},
        {"algorithm", "SHA-256"},
        {"digest", hasher.finish_hex()},
        {"bytes", total},
    };
}

}  // namespace concurrentcore
