#pragma once

#include <nlohmann/json_fwd.hpp>

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <variant>

namespace concurrentcore {

enum class TaskType {
    Sha256,        ///< "sha256"
    Compress,      ///< "compress"
    Decompress,    ///< "decompress"
    TextAnalysis,  ///< "text-analysis"
    Compute,       ///< "compute"
};

[[nodiscard]] std::string_view to_string(TaskType type) noexcept;
[[nodiscard]] std::optional<TaskType> task_type_from_string(std::string_view text) noexcept;

enum class CompressionFormat { Zlib, Gzip };

/// Server-side resource limits applied to every task. Clients may ask for
/// tighter limits, never looser ones.
struct TaskLimits {
    std::uint64_t max_decompressed_bytes = 256ULL * 1024 * 1024;  ///< Decompression-bomb guard.
    std::uint64_t max_compute_limit = 200'000'000;                ///< Upper bound for "compute".
    std::size_t io_chunk_bytes = 64 * 1024;                       ///< Streaming buffer size.
};

struct Sha256Params {
    std::string file;
};

struct CompressParams {
    std::string input;
    std::string output;
    int level = 6;  ///< zlib level 0..9
    CompressionFormat format = CompressionFormat::Zlib;
    bool overwrite = false;
};

struct DecompressParams {
    std::string input;
    std::string output;
    bool overwrite = false;
    std::uint64_t max_output_bytes = 0;  ///< Effective limit (<= server limit).
};

struct TextAnalysisParams {
    std::optional<std::string> file;  ///< Exactly one of file / text is set.
    std::optional<std::string> text;
};

struct ComputeParams {
    std::uint64_t limit = 0;  ///< Count primes p < limit.
};

using TaskParams =
    std::variant<Sha256Params, CompressParams, DecompressParams, TextAnalysisParams, ComputeParams>;

/// A validated, immutable description of the work a job performs.
struct TaskSpec {
    TaskType type = TaskType::Compute;
    TaskParams params;
};

/// Validates client-supplied parameters and builds a TaskSpec. Performs only
/// lexical path checks (absolute paths, "..", etc.); filesystem resolution
/// happens at execution time inside the workspace sandbox.
/// Throws CoreError(UnknownTaskType | InvalidParams | PathNotAllowed).
[[nodiscard]] TaskSpec parse_task_spec(std::string_view task_type, const nlohmann::json& params,
                                       const TaskLimits& limits);

}  // namespace concurrentcore
