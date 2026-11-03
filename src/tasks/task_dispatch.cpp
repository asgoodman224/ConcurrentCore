#include "concurrentcore/tasks/tasks.hpp"

#include <type_traits>
#include <variant>

namespace concurrentcore {

nlohmann::json execute_task(const TaskSpec& spec, const TaskContext& ctx) {
    return std::visit(
        [&ctx](const auto& params) -> nlohmann::json {
            using T = std::decay_t<decltype(params)>;
            if constexpr (std::is_same_v<T, Sha256Params>) {
                return run_sha256(params, ctx);
            } else if constexpr (std::is_same_v<T, CompressParams>) {
                return run_compress(params, ctx);
            } else if constexpr (std::is_same_v<T, DecompressParams>) {
                return run_decompress(params, ctx);
            } else if constexpr (std::is_same_v<T, TextAnalysisParams>) {
                return run_text_analysis(params, ctx);
            } else {
                static_assert(std::is_same_v<T, ComputeParams>);
                return run_compute(params, ctx);
            }
        },
        spec.params);
}

}  // namespace concurrentcore
