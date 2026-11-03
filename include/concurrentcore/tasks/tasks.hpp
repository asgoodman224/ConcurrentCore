#pragma once

#include "concurrentcore/common/error.hpp"
#include "concurrentcore/tasks/task_spec.hpp"
#include "concurrentcore/tasks/workspace.hpp"

#include <nlohmann/json.hpp>

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <stop_token>
#include <string>
#include <string_view>

namespace concurrentcore {

/// Everything a running task may use. Passed by const reference; the
/// referenced objects outlive the task (owned by JobManager).
struct TaskContext {
    const Workspace& workspace;
    const TaskLimits& limits;
    std::stop_token stop;

    /// Cancellation point. Tasks call this between bounded units of work
    /// (one I/O chunk, a few thousand loop iterations), so a cancellation
    /// request is honoured within roughly one chunk's worth of time.
    void throw_if_cancelled() const {
        if (stop.stop_requested()) {
            throw OperationCancelled();
        }
    }
};

/// Dispatches to the concrete task implementation. Returns the task's JSON
/// result; throws CoreError (including OperationCancelled) on failure.
[[nodiscard]] nlohmann::json execute_task(const TaskSpec& spec, const TaskContext& ctx);

// ---- Individual tasks ------------------------------------------------------

[[nodiscard]] nlohmann::json run_sha256(const Sha256Params& p, const TaskContext& ctx);
[[nodiscard]] nlohmann::json run_compress(const CompressParams& p, const TaskContext& ctx);
[[nodiscard]] nlohmann::json run_decompress(const DecompressParams& p, const TaskContext& ctx);
[[nodiscard]] nlohmann::json run_text_analysis(const TextAnalysisParams& p, const TaskContext& ctx);
[[nodiscard]] nlohmann::json run_compute(const ComputeParams& p, const TaskContext& ctx);

// ---- Reusable building blocks (also unit-tested directly) ------------------

/// Incremental SHA-256 (Mbed TLS backend).
class Sha256Hasher {
public:
    Sha256Hasher();
    ~Sha256Hasher();
    Sha256Hasher(const Sha256Hasher&) = delete;
    Sha256Hasher& operator=(const Sha256Hasher&) = delete;
    Sha256Hasher(Sha256Hasher&&) = delete;
    Sha256Hasher& operator=(Sha256Hasher&&) = delete;

    void update(std::span<const std::byte> data);
    void update(std::string_view data);
    /// Finalizes and returns the lowercase hex digest. Call at most once.
    [[nodiscard]] std::string finish_hex();

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

[[nodiscard]] std::string sha256_hex(std::string_view data);

/// Streaming text statistics. Feed arbitrary chunks; results are identical
/// regardless of how the input is split.
class TextAnalyzer {
public:
    void feed(std::string_view chunk) noexcept;

    [[nodiscard]] std::uint64_t bytes() const noexcept { return bytes_; }
    /// UTF-8 code points (bytes that are not continuation bytes).
    [[nodiscard]] std::uint64_t characters() const noexcept { return characters_; }
    /// Maximal runs of non-whitespace bytes (ASCII whitespace separates).
    [[nodiscard]] std::uint64_t words() const noexcept { return words_; }
    /// Newline count, plus one if the input is non-empty and does not end
    /// with '\n' (an unterminated final line still counts as a line).
    [[nodiscard]] std::uint64_t lines() const noexcept;

    [[nodiscard]] nlohmann::json to_json() const;

private:
    std::uint64_t bytes_ = 0;
    std::uint64_t characters_ = 0;
    std::uint64_t words_ = 0;
    std::uint64_t newlines_ = 0;
    bool in_word_ = false;
    char last_byte_ = '\0';
};

struct PrimeCountResult {
    std::uint64_t count = 0;
    std::uint64_t largest = 0;  ///< 0 if no prime below the limit.
};

/// Deterministic CPU-bound workload: counts primes p < limit by trial
/// division (6k +/- 1 wheel). Intentionally not a sieve: trial division is
/// compute-bound rather than memory-bound, which makes it a cleaner probe of
/// multi-core scaling. Checks `stop` every 4096 candidates.
[[nodiscard]] PrimeCountResult count_primes_below(std::uint64_t limit, std::stop_token stop = {});

}  // namespace concurrentcore
