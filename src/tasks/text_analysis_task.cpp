#include "concurrentcore/tasks/tasks.hpp"

#include "file_io.hpp"

#include <vector>

namespace concurrentcore {
namespace {

constexpr bool is_ascii_space(unsigned char c) noexcept {
    return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\v' || c == '\f';
}

}  // namespace

void TextAnalyzer::feed(std::string_view chunk) noexcept {
    // A small state machine (in_word_, last_byte_) carries context across
    // chunk boundaries, so words and lines split between reads are counted
    // exactly once.
    for (const char ch : chunk) {
        const auto c = static_cast<unsigned char>(ch);
        if ((c & 0xC0U) != 0x80U) {
            ++characters_;
        }
        if (c == '\n') {
            ++newlines_;
        }
        if (is_ascii_space(c)) {
            in_word_ = false;
        } else if (!in_word_) {
            in_word_ = true;
            ++words_;
        }
    }
    if (!chunk.empty()) {
        last_byte_ = chunk.back();
    }
    bytes_ += chunk.size();
}

std::uint64_t TextAnalyzer::lines() const noexcept {
    return newlines_ + ((bytes_ > 0 && last_byte_ != '\n') ? 1U : 0U);
}

nlohmann::json TextAnalyzer::to_json() const {
    return {
        {"bytes", bytes_},
        {"characters", characters_},
        {"words", words_},
        {"lines", lines()},
    };
}

nlohmann::json run_text_analysis(const TextAnalysisParams& p, const TaskContext& ctx) {
    TextAnalyzer analyzer;
    nlohmann::json result;
    if (p.text) {
        // Inline text is bounded by the server's maximum message size.
        analyzer.feed(*p.text);
        result = analyzer.to_json();
        result["source"] = "inline";
        return result;
    }

    const auto path = ctx.workspace.resolve_input(*p.file);
    detail::InputFile in(path);
    std::vector<char> buffer(ctx.limits.io_chunk_bytes);
    for (;;) {
        ctx.throw_if_cancelled();
        const std::size_t n = in.read(buffer);
        if (n == 0) {
            break;
        }
        analyzer.feed(std::string_view(buffer.data(), n));
    }
    result = analyzer.to_json();
    result["source"] = "file";
    result["file"] = *p.file;
    return result;
}

}  // namespace concurrentcore
