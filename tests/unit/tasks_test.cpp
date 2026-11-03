#include "concurrentcore/tasks/tasks.hpp"

#include "support/test_support.hpp"

#include <gtest/gtest.h>

#include <stop_token>

using namespace concurrentcore;
using cctest::read_file;
using cctest::write_file;

namespace {

class TaskTest : public ::testing::Test {
protected:
    TaskTest() : ws_(dir_.path()) {}

    TaskContext ctx() { return TaskContext{ws_, limits_, stop_.get_token()}; }

    std::size_t leftover_temp_files() const {
        std::size_t n = 0;
        for (const auto& e : std::filesystem::directory_iterator(dir_.path())) {
            if (e.path().extension() == ".ccpart") {
                ++n;
            }
        }
        return n;
    }

    cctest::TempDir dir_;
    Workspace ws_;
    TaskLimits limits_;
    std::stop_source stop_;
};

}  // namespace

// ------------------------------------------------------------------ SHA-256

TEST(Sha256, NistTestVectors) {
    EXPECT_EQ(sha256_hex(""), "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
    EXPECT_EQ(sha256_hex("abc"), "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
    EXPECT_EQ(sha256_hex("abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq"),
              "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1");
    EXPECT_EQ(sha256_hex(std::string(1'000'000, 'a')),
              "cdc76e5c9914fb9281a1c7e284d73e67f1809a48a497200e046d39ccc7112cd0");
}

TEST(Sha256, IncrementalEqualsOneShot) {
    const std::string data = cctest::deterministic_bytes(100'000);
    Sha256Hasher h;
    for (std::size_t i = 0; i < data.size(); i += 777) {
        h.update(std::string_view(data).substr(i, 777));
    }
    EXPECT_EQ(h.finish_hex(), sha256_hex(data));
    EXPECT_THROW((void)h.finish_hex(), CoreError);
}

TEST_F(TaskTest, Sha256FileIsStreamedInChunks) {
    limits_.io_chunk_bytes = 512;  // force many chunks
    const std::string data = cctest::deterministic_bytes(70'000);
    write_file(dir_.path() / "data.bin", data);
    const auto r = run_sha256({"data.bin"}, ctx());
    EXPECT_EQ(r["digest"], sha256_hex(data));
    EXPECT_EQ(r["bytes"], data.size());
}

// ------------------------------------------------------------ text analysis

TEST(TextAnalyzer, CountsBytesCharactersWordsLines) {
    TextAnalyzer a;
    a.feed("hello world\nsecond  line\n");
    EXPECT_EQ(a.bytes(), 25U);
    EXPECT_EQ(a.characters(), 25U);
    EXPECT_EQ(a.words(), 4U);
    EXPECT_EQ(a.lines(), 2U);
}

TEST(TextAnalyzer, EdgeCases) {
    TextAnalyzer empty;
    EXPECT_EQ(empty.lines(), 0U);
    EXPECT_EQ(empty.words(), 0U);

    TextAnalyzer unterminated;
    unterminated.feed("one\ntwo");
    EXPECT_EQ(unterminated.lines(), 2U);

    TextAnalyzer blank;
    blank.feed("\n\n\n");
    EXPECT_EQ(blank.lines(), 3U);
    EXPECT_EQ(blank.words(), 0U);

    TextAnalyzer utf8;
    utf8.feed("h\xC3\xA9llo w\xC3\xB6rld \xE2\x82\xAC");  // héllo wörld €
    EXPECT_EQ(utf8.bytes(), 17U);
    EXPECT_EQ(utf8.characters(), 13U);
    EXPECT_EQ(utf8.words(), 3U);
}

TEST(TextAnalyzer, ResultIsIndependentOfChunking) {
    const std::string text = "The  quick\tbrown\r\nfox\n\njumps over  the lazy dog\n tail";
    TextAnalyzer whole;
    whole.feed(text);
    TextAnalyzer bytewise;
    for (const char c : text) {
        bytewise.feed(std::string_view(&c, 1));
    }
    EXPECT_EQ(whole.to_json(), bytewise.to_json());
    EXPECT_EQ(whole.words(), 10U);
}

TEST_F(TaskTest, TextAnalysisOnFileAndInline) {
    write_file(dir_.path() / "t.txt", "a b c\nd e\n");
    const auto f = run_text_analysis({std::string("t.txt"), std::nullopt}, ctx());
    EXPECT_EQ(f["words"], 5);
    EXPECT_EQ(f["lines"], 2);
    EXPECT_EQ(f["source"], "file");
    const auto i = run_text_analysis({std::nullopt, std::string("x y")}, ctx());
    EXPECT_EQ(i["words"], 2);
    EXPECT_EQ(i["source"], "inline");
}

// ------------------------------------------------------------------ compute

TEST(Compute, KnownPrimeCounts) {
    EXPECT_EQ(count_primes_below(0).count, 0U);
    EXPECT_EQ(count_primes_below(2).count, 0U);
    EXPECT_EQ(count_primes_below(3).count, 1U);
    EXPECT_EQ(count_primes_below(10).count, 4U);
    EXPECT_EQ(count_primes_below(100).count, 25U);
    EXPECT_EQ(count_primes_below(100).largest, 97U);
    EXPECT_EQ(count_primes_below(100'000).count, 9592U);
}

TEST(Compute, HonoursCancellation) {
    std::stop_source s;
    s.request_stop();
    EXPECT_THROW((void)count_primes_below(10'000'000, s.get_token()), OperationCancelled);
}

// -------------------------------------------------------- compress / inflate

TEST_F(TaskTest, CompressDecompressRoundTripBothFormats) {
    limits_.io_chunk_bytes = 4096;
    const std::string data = cctest::deterministic_bytes(300'000);
    write_file(dir_.path() / "in.bin", data);
    for (const auto format : {CompressionFormat::Zlib, CompressionFormat::Gzip}) {
        for (const int level : {0, 1, 9}) {
            CompressParams cp{"in.bin", "packed", level, format, /*overwrite=*/true};
            const auto c = run_compress(cp, ctx());
            EXPECT_EQ(c["original_bytes"], data.size());
            EXPECT_EQ(c["compressed_bytes"], std::filesystem::file_size(dir_.path() / "packed"));

            DecompressParams dp{"packed", "out.bin", true, limits_.max_decompressed_bytes};
            const auto d = run_decompress(dp, ctx());
            EXPECT_EQ(d["decompressed_bytes"], data.size());
            EXPECT_EQ(read_file(dir_.path() / "out.bin"), data) << "level " << level;
        }
    }
    EXPECT_EQ(leftover_temp_files(), 0U);
}

TEST_F(TaskTest, DecompressionBombIsStoppedAtTheLimit) {
    write_file(dir_.path() / "zeros.bin", std::string(8 * 1024 * 1024, '\0'));
    (void)run_compress({"zeros.bin", "zeros.zz", 9, CompressionFormat::Zlib, false}, ctx());
    EXPECT_LT(std::filesystem::file_size(dir_.path() / "zeros.zz"), 64U * 1024);  // ~1000:1

    DecompressParams dp{"zeros.zz", "boom.bin", false, 1024 * 1024};
    try {
        (void)run_decompress(dp, ctx());
        FAIL() << "expected LimitExceeded";
    } catch (const CoreError& e) {
        EXPECT_EQ(e.code(), ErrorCode::LimitExceeded);
    }
    EXPECT_FALSE(std::filesystem::exists(dir_.path() / "boom.bin"));
    EXPECT_EQ(leftover_temp_files(), 0U);  // partial output removed
}

TEST_F(TaskTest, CorruptTruncatedAndTrailingDataAreRejected) {
    const std::string data = cctest::deterministic_bytes(50'000);
    write_file(dir_.path() / "in.bin", data);
    (void)run_compress({"in.bin", "good.zz", 6, CompressionFormat::Zlib, false}, ctx());
    const std::string good = read_file(dir_.path() / "good.zz");

    auto expect_corrupt = [&](const std::string& name, const std::string& bytes) {
        write_file(dir_.path() / name, bytes);
        try {
            (void)run_decompress({name, name + ".out", false, limits_.max_decompressed_bytes}, ctx());
            ADD_FAILURE() << name << ": expected CorruptData";
        } catch (const CoreError& e) {
            EXPECT_EQ(e.code(), ErrorCode::CorruptData) << name << ": " << e.what();
        }
        EXPECT_FALSE(std::filesystem::exists(dir_.path() / (name + ".out")));
    };
    expect_corrupt("garbage.zz", "this is not compressed data at all");
    expect_corrupt("truncated.zz", good.substr(0, good.size() / 2));
    expect_corrupt("trailing.zz", good + "EXTRA");
    std::string flipped = good;
    flipped[flipped.size() / 2] = static_cast<char>(flipped[flipped.size() / 2] ^ 0x55);
    expect_corrupt("flipped.zz", flipped);
    expect_corrupt("empty.zz", "");
    EXPECT_EQ(leftover_temp_files(), 0U);
}

TEST_F(TaskTest, ExistingOutputIsNotOverwrittenUnlessRequested) {
    write_file(dir_.path() / "in.txt", "hello");
    write_file(dir_.path() / "out.zz", "precious");
    try {
        (void)run_compress({"in.txt", "out.zz", 6, CompressionFormat::Zlib, false}, ctx());
        FAIL() << "expected OutputExists";
    } catch (const CoreError& e) {
        EXPECT_EQ(e.code(), ErrorCode::OutputExists);
    }
    EXPECT_EQ(read_file(dir_.path() / "out.zz"), "precious");
    (void)run_compress({"in.txt", "out.zz", 6, CompressionFormat::Zlib, true}, ctx());
    EXPECT_NE(read_file(dir_.path() / "out.zz"), "precious");
}

TEST_F(TaskTest, CancelledCompressionLeavesNoPartialOutput) {
    write_file(dir_.path() / "in.bin", cctest::deterministic_bytes(200'000));
    stop_.request_stop();
    EXPECT_THROW((void)run_compress({"in.bin", "out.zz", 6, CompressionFormat::Zlib, false}, ctx()),
                 OperationCancelled);
    EXPECT_FALSE(std::filesystem::exists(dir_.path() / "out.zz"));
    EXPECT_EQ(leftover_temp_files(), 0U);
}

TEST_F(TaskTest, ExecuteTaskDispatchesByType) {
    const auto r = execute_task(TaskSpec{TaskType::Compute, ComputeParams{30}}, ctx());
    EXPECT_EQ(r["prime_count"], 10);
}

// ---------------------------------------------------------- parameter parsing

TEST(TaskSpecParsing, ValidSpecs) {
    const TaskLimits limits;
    auto s = parse_task_spec("sha256", {{"file", "a/b.bin"}}, limits);
    EXPECT_EQ(s.type, TaskType::Sha256);
    EXPECT_EQ(std::get<Sha256Params>(s.params).file, "a/b.bin");

    s = parse_task_spec("compress", {{"input", "x.txt"}, {"format", "gzip"}, {"level", 9}}, limits);
    const auto& cp = std::get<CompressParams>(s.params);
    EXPECT_EQ(cp.output, "x.txt.gz");  // default output name
    EXPECT_EQ(cp.level, 9);

    s = parse_task_spec("decompress", {{"input", "x.gz"}, {"output", "x"}, {"max_output_bytes", 10}}, limits);
    EXPECT_EQ(std::get<DecompressParams>(s.params).max_output_bytes, 10U);

    s = parse_task_spec("text-analysis", {{"text", "hi"}}, limits);
    EXPECT_EQ(*std::get<TextAnalysisParams>(s.params).text, "hi");

    s = parse_task_spec("compute", {{"limit", 1000}}, limits);
    EXPECT_EQ(std::get<ComputeParams>(s.params).limit, 1000U);
}

TEST(TaskSpecParsing, InvalidSpecsAreRejectedWithSpecificCodes) {
    const TaskLimits limits;
    auto code_of = [&](std::string_view type, const nlohmann::json& params) {
        try {
            (void)parse_task_spec(type, params, limits);
        } catch (const CoreError& e) {
            return e.code();
        }
        return ErrorCode::InternalError;  // "no error" sentinel
    };
    EXPECT_EQ(code_of("rot13", {}), ErrorCode::UnknownTaskType);
    EXPECT_EQ(code_of("sha256", {}), ErrorCode::InvalidParams);
    EXPECT_EQ(code_of("sha256", {{"file", 5}}), ErrorCode::InvalidParams);
    EXPECT_EQ(code_of("sha256", {{"file", "a"}, {"extra", 1}}), ErrorCode::InvalidParams);
    EXPECT_EQ(code_of("sha256", {{"file", "../etc/passwd"}}), ErrorCode::PathNotAllowed);
    EXPECT_EQ(code_of("sha256", {{"file", "/etc/passwd"}}), ErrorCode::PathNotAllowed);
    EXPECT_EQ(code_of("compress", {{"input", "a"}, {"level", 10}}), ErrorCode::InvalidParams);
    EXPECT_EQ(code_of("compress", {{"input", "a"}, {"level", -1}}), ErrorCode::InvalidParams);
    EXPECT_EQ(code_of("compress", {{"input", "a"}, {"format", "bzip2"}}), ErrorCode::InvalidParams);
    EXPECT_EQ(code_of("compress", {{"input", "a"}, {"output", "a"}}), ErrorCode::InvalidParams);
    EXPECT_EQ(code_of("decompress", {{"input", "a"}}), ErrorCode::InvalidParams);
    EXPECT_EQ(code_of("decompress", {{"input", "a"}, {"output", "b"}, {"max_output_bytes", 0}}),
              ErrorCode::InvalidParams);
    EXPECT_EQ(code_of("decompress", {{"input", "a"}, {"output", "b"},
                                     {"max_output_bytes", limits.max_decompressed_bytes + 1}}),
              ErrorCode::InvalidParams);
    EXPECT_EQ(code_of("text-analysis", {}), ErrorCode::InvalidParams);
    EXPECT_EQ(code_of("text-analysis", {{"file", "a"}, {"text", "b"}}), ErrorCode::InvalidParams);
    EXPECT_EQ(code_of("compute", {{"limit", limits.max_compute_limit + 1}}), ErrorCode::InvalidParams);
    EXPECT_EQ(code_of("compute", {{"limit", 1.5}}), ErrorCode::InvalidParams);
    EXPECT_EQ(code_of("compute", nlohmann::json::array()), ErrorCode::InvalidParams);
}
