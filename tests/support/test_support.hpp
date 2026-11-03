#pragma once

// Shared helpers for the ConcurrentCore test suites.

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <functional>
#include <mutex>
#include <random>
#include <sstream>
#include <string>
#include <thread>

namespace cctest {

namespace fs = std::filesystem;
using namespace std::chrono_literals;

/// Unique temporary directory, removed on destruction.
class TempDir {
public:
    TempDir() {
        std::random_device rd;
        const auto base = fs::temp_directory_path();
        for (int attempt = 0; attempt < 16; ++attempt) {
            path_ = base / ("cc-test-" + std::to_string(rd()) + std::to_string(rd()));
            std::error_code ec;
            if (fs::create_directory(path_, ec)) {
                path_ = fs::canonical(path_);
                return;
            }
        }
        throw std::runtime_error("cannot create temporary directory");
    }
    ~TempDir() {
        std::error_code ec;
        fs::remove_all(path_, ec);
    }
    TempDir(const TempDir&) = delete;
    TempDir& operator=(const TempDir&) = delete;

    [[nodiscard]] const fs::path& path() const noexcept { return path_; }

private:
    fs::path path_;
};

inline void write_file(const fs::path& path, const std::string& content) {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    out.write(content.data(), static_cast<std::streamsize>(content.size()));
    if (!out) {
        throw std::runtime_error("write_file failed: " + path.string());
    }
}

inline std::string read_file(const fs::path& path) {
    std::ifstream in(path, std::ios::binary);
    std::ostringstream ss;
    ss << in.rdbuf();
    return ss.str();
}

/// Deterministic pseudo-random bytes (xorshift64*), compressible-ish:
/// mixes random bytes with repeated text so compression ratios are realistic.
inline std::string deterministic_bytes(std::size_t size, std::uint64_t seed = 0x9E3779B97F4A7C15ULL) {
    std::string out;
    out.reserve(size);
    std::uint64_t x = seed == 0 ? 1 : seed;
    static constexpr std::string_view kText = "the quick brown fox jumps over the lazy dog ";
    while (out.size() < size) {
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        const std::uint64_t r = x * 2685821657736338717ULL;
        if ((r & 3U) == 0) {
            out.append(kText.substr(0, std::min(kText.size(), size - out.size())));
        } else {
            out.push_back(static_cast<char>(r >> 56));
        }
    }
    return out;
}

/// Polls `pred` until true or `timeout` elapses. Used only for externally
/// observable state that has no notification hook; returns false on
/// timeout so the test fails instead of hanging.
inline bool wait_until(const std::function<bool()>& pred,
                       std::chrono::milliseconds timeout = std::chrono::milliseconds(10'000)) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (!pred()) {
        if (std::chrono::steady_clock::now() >= deadline) {
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return true;
}

/// A one-shot gate: threads block in wait() until open() is called.
/// Deterministic replacement for "sleep and hope" synchronization.
class Gate {
public:
    void open() {
        {
            std::lock_guard lock(mutex_);
            open_ = true;
        }
        cv_.notify_all();
    }
    void wait() {
        std::unique_lock lock(mutex_);
        cv_.wait(lock, [this] { return open_; });
    }
    template <typename Rep, typename Period>
    bool wait_for(std::chrono::duration<Rep, Period> timeout) {
        std::unique_lock lock(mutex_);
        return cv_.wait_for(lock, timeout, [this] { return open_; });
    }

private:
    std::mutex mutex_;
    std::condition_variable cv_;
    bool open_ = false;
};

/// Counts arrivals and lets a test wait for a target count.
class Counter {
public:
    void increment() {
        {
            std::lock_guard lock(mutex_);
            ++value_;
        }
        cv_.notify_all();
    }
    bool wait_for_at_least(std::uint64_t target, std::chrono::milliseconds timeout = 10'000ms) {
        std::unique_lock lock(mutex_);
        return cv_.wait_for(lock, timeout, [&] { return value_ >= target; });
    }
    std::uint64_t value() {
        std::lock_guard lock(mutex_);
        return value_;
    }

private:
    std::mutex mutex_;
    std::condition_variable cv_;
    std::uint64_t value_ = 0;
};

}  // namespace cctest
