#pragma once

#include "concurrentcore/concurrency/thread_pool.hpp"
#include "concurrentcore/tasks/task_spec.hpp"

#include <nlohmann/json_fwd.hpp>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <string>

namespace concurrentcore {

struct ServerConfig {
    std::string host = "127.0.0.1";  ///< Loopback by default for safety.
    std::uint16_t port = 7878;       ///< 0 = pick an ephemeral port.
    std::size_t io_threads = 2;      ///< Threads running the Asio event loop.
    std::size_t worker_threads = ThreadPool::default_worker_count();
    std::size_t queue_capacity = 1024;
    std::size_t max_message_bytes = 1024 * 1024;
    std::size_t max_connections = 256;
    std::size_t max_pending_write_bytes = 8 * 1024 * 1024;  ///< Per connection.
    std::chrono::seconds idle_timeout{300};                 ///< 0 disables.
    std::size_t max_retained_jobs = 10'000;
    std::filesystem::path workspace = "workspace";
    std::chrono::milliseconds shutdown_drain_timeout{10'000};
    std::string log_level = "info";
    TaskLimits task_limits;
};

/// Applies keys from a JSON object onto `config`. Unknown keys and values
/// of the wrong type throw std::invalid_argument.
void apply_json(ServerConfig& config, const nlohmann::json& j);

/// Reads a JSON config file and applies it. Throws std::invalid_argument.
void load_config_file(ServerConfig& config, const std::filesystem::path& file);

/// Throws std::invalid_argument describing the first invalid setting.
void validate(const ServerConfig& config);

}  // namespace concurrentcore
