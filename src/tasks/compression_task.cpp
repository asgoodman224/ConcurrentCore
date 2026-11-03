#include "concurrentcore/tasks/tasks.hpp"

#include "file_io.hpp"

#include <zlib.h>

#include <filesystem>
#include <vector>

namespace concurrentcore {
namespace {

// RAII owners for zlib streams so every exit path releases zlib's memory.
class Deflater {
public:
    Deflater(int level, CompressionFormat format) {
        // windowBits 15 = zlib wrapper; +16 = gzip wrapper.
        const int window_bits = format == CompressionFormat::Gzip ? 15 + 16 : 15;
        if (deflateInit2(&stream_, level, Z_DEFLATED, window_bits, 8, Z_DEFAULT_STRATEGY) != Z_OK) {
            throw CoreError(ErrorCode::InternalError, "deflateInit2 failed");
        }
    }
    ~Deflater() { deflateEnd(&stream_); }
    Deflater(const Deflater&) = delete;
    Deflater& operator=(const Deflater&) = delete;
    Deflater(Deflater&&) = delete;
    Deflater& operator=(Deflater&&) = delete;
    z_stream& get() noexcept { return stream_; }

private:
    z_stream stream_{};
};

class Inflater {
public:
    Inflater() {
        // windowBits 15 + 32: automatically accept zlib or gzip headers.
        if (inflateInit2(&stream_, 15 + 32) != Z_OK) {
            throw CoreError(ErrorCode::InternalError, "inflateInit2 failed");
        }
    }
    ~Inflater() { inflateEnd(&stream_); }
    Inflater(const Inflater&) = delete;
    Inflater& operator=(const Inflater&) = delete;
    Inflater(Inflater&&) = delete;
    Inflater& operator=(Inflater&&) = delete;
    z_stream& get() noexcept { return stream_; }

private:
    z_stream stream_{};
};

Bytef* as_bytef(char* p) noexcept { return reinterpret_cast<Bytef*>(p); }

void reject_same_file(const std::filesystem::path& in, const std::filesystem::path& out) {
    std::error_code ec;
    if (std::filesystem::equivalent(in, out, ec)) {
        throw CoreError(ErrorCode::InvalidParams, "input and output refer to the same file");
    }
}

double ratio(std::uint64_t numerator, std::uint64_t denominator) {
    return denominator == 0 ? 0.0
                            : static_cast<double>(numerator) / static_cast<double>(denominator);
}

}  // namespace

nlohmann::json run_compress(const CompressParams& p, const TaskContext& ctx) {
    const auto in_path = ctx.workspace.resolve_input(p.input);
    const auto out_path = ctx.workspace.resolve_output(p.output);
    reject_same_file(in_path, out_path);

    detail::InputFile in(in_path);
    detail::AtomicOutputFile out(out_path, p.overwrite);
    Deflater deflater(p.level, p.format);
    z_stream& zs = deflater.get();

    const std::size_t chunk = ctx.limits.io_chunk_bytes;
    std::vector<char> in_buf(chunk);
    std::vector<char> out_buf(chunk);
    std::uint64_t original = 0;
    std::uint64_t compressed = 0;

    int flush = Z_NO_FLUSH;
    do {
        ctx.throw_if_cancelled();
        const std::size_t n = in.read(in_buf);
        original += n;
        flush = n == 0 ? Z_FINISH : Z_NO_FLUSH;
        zs.next_in = as_bytef(in_buf.data());
        zs.avail_in = static_cast<uInt>(n);
        // Drain all output this input produces (and, at Z_FINISH, the trailer).
        do {
            zs.next_out = as_bytef(out_buf.data());
            zs.avail_out = static_cast<uInt>(out_buf.size());
            const int rc = deflate(&zs, flush);
            if (rc == Z_STREAM_ERROR) {
                throw CoreError(ErrorCode::InternalError, "deflate failed");
            }
            const std::size_t produced = out_buf.size() - zs.avail_out;
            out.write(std::span<const char>(out_buf.data(), produced));
            compressed += produced;
        } while (zs.avail_out == 0);
    } while (flush != Z_FINISH);

    out.commit();
    return {
        {"input", p.input},
        {"output", p.output},
        {"format", p.format == CompressionFormat::Gzip ? "gzip" : "zlib"},
        {"level", p.level},
        {"original_bytes", original},
        {"compressed_bytes", compressed},
        {"compression_ratio", ratio(compressed, original)},
    };
}

nlohmann::json run_decompress(const DecompressParams& p, const TaskContext& ctx) {
    const auto in_path = ctx.workspace.resolve_input(p.input);
    const auto out_path = ctx.workspace.resolve_output(p.output);
    reject_same_file(in_path, out_path);

    detail::InputFile in(in_path);
    detail::AtomicOutputFile out(out_path, p.overwrite);
    Inflater inflater;
    z_stream& zs = inflater.get();

    const std::size_t chunk = ctx.limits.io_chunk_bytes;
    std::vector<char> in_buf(chunk);
    std::vector<char> out_buf(chunk);
    std::uint64_t consumed = 0;
    std::uint64_t produced_total = 0;
    bool stream_end = false;

    while (!stream_end) {
        ctx.throw_if_cancelled();
        const std::size_t n = in.read(in_buf);
        if (n == 0) {
            throw CoreError(ErrorCode::CorruptData, "compressed stream is truncated");
        }
        zs.next_in = as_bytef(in_buf.data());
        zs.avail_in = static_cast<uInt>(n);

        // Inflate until this input chunk is consumed. A full output buffer
        // (avail_out == 0) means zlib may have more output pending.
        do {
            ctx.throw_if_cancelled();  // a single input chunk can expand a lot
            zs.next_out = as_bytef(out_buf.data());
            zs.avail_out = static_cast<uInt>(out_buf.size());
            const int rc = inflate(&zs, Z_NO_FLUSH);
            switch (rc) {
                case Z_OK:
                case Z_BUF_ERROR:  // no progress possible: input exhausted
                    break;
                case Z_STREAM_END:
                    stream_end = true;
                    break;
                case Z_NEED_DICT:
                case Z_DATA_ERROR:
                    throw CoreError(ErrorCode::CorruptData,
                                    std::string("invalid compressed data: ") +
                                        (zs.msg != nullptr ? zs.msg : "unknown error"));
                case Z_MEM_ERROR:
                    throw CoreError(ErrorCode::InternalError, "zlib out of memory");
                default:
                    throw CoreError(ErrorCode::InternalError,
                                    "inflate failed with code " + std::to_string(rc));
            }
            const std::size_t produced = out_buf.size() - zs.avail_out;
            // Decompression-bomb guard: enforced BEFORE the bytes are
            // written, so at most `max_output_bytes` ever reach the disk.
            if (produced_total + produced > p.max_output_bytes) {
                throw CoreError(ErrorCode::LimitExceeded,
                                "decompressed size exceeds limit of " +
                                    std::to_string(p.max_output_bytes) + " bytes");
            }
            out.write(std::span<const char>(out_buf.data(), produced));
            produced_total += produced;
        } while (zs.avail_out == 0 && !stream_end);
        consumed += n - zs.avail_in;
        if (stream_end && zs.avail_in > 0) {
            throw CoreError(ErrorCode::CorruptData, "unexpected trailing data after stream end");
        }
    }

    // Anything after the end of the compressed stream is rejected rather
    // than silently ignored.
    std::array<char, 1> probe{};
    if (in.read(probe) != 0) {
        throw CoreError(ErrorCode::CorruptData, "unexpected trailing data after stream end");
    }

    out.commit();
    return {
        {"input", p.input},
        {"output", p.output},
        {"compressed_bytes", consumed},
        {"decompressed_bytes", produced_total},
        {"max_output_bytes", p.max_output_bytes},
    };
}

}  // namespace concurrentcore
