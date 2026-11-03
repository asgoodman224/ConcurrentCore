#include "concurrentcore/tasks/task_spec.hpp"

#include "concurrentcore/common/error.hpp"
#include "concurrentcore/common/json_util.hpp"
#include "concurrentcore/tasks/workspace.hpp"

#include <nlohmann/json.hpp>

#include <array>
#include <initializer_list>
#include <string>
#include <utility>

namespace concurrentcore {

std::string_view to_string(TaskType type) noexcept {
    switch (type) {
        case TaskType::Sha256: return "sha256";
        case TaskType::Compress: return "compress";
        case TaskType::Decompress: return "decompress";
        case TaskType::TextAnalysis: return "text-analysis";
        case TaskType::Compute: return "compute";
    }
    return "unknown";
}

std::optional<TaskType> task_type_from_string(std::string_view text) noexcept {
    static constexpr std::array<TaskType, 5> kAll{TaskType::Sha256, TaskType::Compress,
                                                  TaskType::Decompress, TaskType::TextAnalysis,
                                                  TaskType::Compute};
    for (const TaskType t : kAll) {
        if (to_string(t) == text) {
            return t;
        }
    }
    return std::nullopt;
}

namespace {

using nlohmann::json;

[[noreturn]] void invalid(const std::string& message) {
    throw CoreError(ErrorCode::InvalidParams, message);
}

// Strict schema: unknown keys are rejected so client typos fail loudly
// instead of silently falling back to defaults.
void require_only(const json& params, std::initializer_list<std::string_view> allowed) {
    for (const auto& [key, value] : params.items()) {
        bool known = false;
        for (const auto a : allowed) {
            known = known || key == a;
        }
        if (!known) {
            invalid("unknown parameter '" + key + "'");
        }
    }
}

std::string required_path(const json& params, const char* key) {
    const auto it = params.find(key);
    if (it == params.end() || !it->is_string()) {
        invalid(std::string("parameter '") + key + "' (string) is required");
    }
    std::string value = it->get<std::string>();
    Workspace::validate_relative(value);
    return value;
}

std::optional<std::string> optional_string(const json& params, const char* key) {
    const auto it = params.find(key);
    if (it == params.end() || it->is_null()) {
        return std::nullopt;
    }
    if (!it->is_string()) {
        invalid(std::string("parameter '") + key + "' must be a string");
    }
    return it->get<std::string>();
}

bool optional_bool(const json& params, const char* key, bool fallback) {
    const auto it = params.find(key);
    if (it == params.end() || it->is_null()) {
        return fallback;
    }
    if (!it->is_boolean()) {
        invalid(std::string("parameter '") + key + "' must be a boolean");
    }
    return it->get<bool>();
}

std::optional<std::uint64_t> optional_uint(const json& params, const char* key) {
    const auto it = params.find(key);
    if (it == params.end() || it->is_null()) {
        return std::nullopt;
    }
    const auto value = as_non_negative_integer(*it);
    if (!value) {
        invalid(std::string("parameter '") + key + "' must be a non-negative integer");
    }
    return value;
}

Sha256Params parse_sha256(const json& params) {
    require_only(params, {"file"});
    return Sha256Params{required_path(params, "file")};
}

CompressParams parse_compress(const json& params) {
    require_only(params, {"input", "output", "level", "format", "overwrite"});
    CompressParams p;
    p.input = required_path(params, "input");

    if (const auto fmt = optional_string(params, "format")) {
        if (*fmt == "zlib") {
            p.format = CompressionFormat::Zlib;
        } else if (*fmt == "gzip") {
            p.format = CompressionFormat::Gzip;
        } else {
            invalid("parameter 'format' must be \"zlib\" or \"gzip\"");
        }
    }
    if (const auto level = optional_uint(params, "level")) {
        if (*level > 9) {
            invalid("parameter 'level' must be between 0 and 9");
        }
        p.level = static_cast<int>(*level);
    }
    if (const auto out = optional_string(params, "output")) {
        Workspace::validate_relative(*out);
        p.output = *out;
    } else {
        p.output = p.input + (p.format == CompressionFormat::Gzip ? ".gz" : ".zz");
        Workspace::validate_relative(p.output);
    }
    p.overwrite = optional_bool(params, "overwrite", false);
    if (p.output == p.input) {
        invalid("'output' must differ from 'input'");
    }
    return p;
}

DecompressParams parse_decompress(const json& params, const TaskLimits& limits) {
    require_only(params, {"input", "output", "overwrite", "max_output_bytes"});
    DecompressParams p;
    p.input = required_path(params, "input");
    p.output = required_path(params, "output");
    p.overwrite = optional_bool(params, "overwrite", false);
    p.max_output_bytes = limits.max_decompressed_bytes;
    if (const auto requested = optional_uint(params, "max_output_bytes")) {
        if (*requested == 0 || *requested > limits.max_decompressed_bytes) {
            invalid("parameter 'max_output_bytes' must be between 1 and " +
                    std::to_string(limits.max_decompressed_bytes));
        }
        p.max_output_bytes = *requested;
    }
    if (p.output == p.input) {
        invalid("'output' must differ from 'input'");
    }
    return p;
}

TextAnalysisParams parse_text_analysis(const json& params) {
    require_only(params, {"file", "text"});
    TextAnalysisParams p;
    p.text = optional_string(params, "text");
    if (params.contains("file")) {
        p.file = required_path(params, "file");
    }
    if (p.file.has_value() == p.text.has_value()) {
        invalid("exactly one of 'file' or 'text' is required");
    }
    return p;
}

ComputeParams parse_compute(const json& params, const TaskLimits& limits) {
    require_only(params, {"limit"});
    const auto limit = optional_uint(params, "limit");
    if (!limit) {
        invalid("parameter 'limit' (non-negative integer) is required");
    }
    if (*limit > limits.max_compute_limit) {
        invalid("parameter 'limit' must be <= " + std::to_string(limits.max_compute_limit));
    }
    return ComputeParams{*limit};
}

}  // namespace

TaskSpec parse_task_spec(std::string_view task_type, const json& params, const TaskLimits& limits) {
    const auto type = task_type_from_string(task_type);
    if (!type) {
        throw CoreError(ErrorCode::UnknownTaskType,
                        "unknown task type '" + std::string(task_type) +
                            "' (expected sha256, compress, decompress, text-analysis, compute)");
    }
    const json empty = json::object();
    const json& p = params.is_null() ? empty : params;
    if (!p.is_object()) {
        invalid("'params' must be a JSON object");
    }

    TaskSpec spec;
    spec.type = *type;
    switch (*type) {
        case TaskType::Sha256: spec.params = parse_sha256(p); break;
        case TaskType::Compress: spec.params = parse_compress(p); break;
        case TaskType::Decompress: spec.params = parse_decompress(p, limits); break;
        case TaskType::TextAnalysis: spec.params = parse_text_analysis(p); break;
        case TaskType::Compute: spec.params = parse_compute(p, limits); break;
    }
    return spec;
}

}  // namespace concurrentcore
