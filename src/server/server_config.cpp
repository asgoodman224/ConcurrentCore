#include "concurrentcore/server/server_config.hpp"

#include "concurrentcore/common/json_util.hpp"

#include <nlohmann/json.hpp>

#include <fstream>
#include <limits>
#include <stdexcept>

namespace concurrentcore {
namespace {

using nlohmann::json;

std::uint64_t get_uint(const json& value, const std::string& key) {
    const auto v = as_non_negative_integer(value);
    if (!v) {
        throw std::invalid_argument("config '" + key + "' must be a non-negative integer");
    }
    return *v;
}

std::size_t get_size(const json& value, const std::string& key) {
    const auto v = get_uint(value, key);
    if (v > std::numeric_limits<std::size_t>::max()) {
        throw std::invalid_argument("config '" + key + "' is too large");
    }
    return static_cast<std::size_t>(v);
}

std::string get_string(const json& value, const std::string& key) {
    if (!value.is_string()) {
        throw std::invalid_argument("config '" + key + "' must be a string");
    }
    return value.get<std::string>();
}

}  // namespace

void apply_json(ServerConfig& c, const json& j) {
    if (!j.is_object()) {
        throw std::invalid_argument("config must be a JSON object");
    }
    for (const auto& [key, value] : j.items()) {
        if (key == "host") {
            c.host = get_string(value, key);
        } else if (key == "port") {
            const auto port = get_uint(value, key);
            if (port > 65535) {
                throw std::invalid_argument("config 'port' must be <= 65535");
            }
            c.port = static_cast<std::uint16_t>(port);
        } else if (key == "io_threads") {
            c.io_threads = get_size(value, key);
        } else if (key == "worker_threads") {
            c.worker_threads = get_size(value, key);
        } else if (key == "queue_capacity") {
            c.queue_capacity = get_size(value, key);
        } else if (key == "max_message_bytes") {
            c.max_message_bytes = get_size(value, key);
        } else if (key == "max_connections") {
            c.max_connections = get_size(value, key);
        } else if (key == "max_pending_write_bytes") {
            c.max_pending_write_bytes = get_size(value, key);
        } else if (key == "idle_timeout_seconds") {
            c.idle_timeout = std::chrono::seconds(get_uint(value, key));
        } else if (key == "max_retained_jobs") {
            c.max_retained_jobs = get_size(value, key);
        } else if (key == "workspace") {
            const std::string text = get_string(value, key);
            c.workspace = std::filesystem::path(std::u8string(text.begin(), text.end()));
        } else if (key == "shutdown_drain_timeout_ms") {
            c.shutdown_drain_timeout = std::chrono::milliseconds(get_uint(value, key));
        } else if (key == "log_level") {
            c.log_level = get_string(value, key);
        } else if (key == "max_decompressed_bytes") {
            c.task_limits.max_decompressed_bytes = get_uint(value, key);
        } else if (key == "max_compute_limit") {
            c.task_limits.max_compute_limit = get_uint(value, key);
        } else if (key == "io_chunk_bytes") {
            c.task_limits.io_chunk_bytes = get_size(value, key);
        } else {
            throw std::invalid_argument("unknown config key '" + key + "'");
        }
    }
}

void load_config_file(ServerConfig& config, const std::filesystem::path& file) {
    std::ifstream in(file);
    if (!in) {
        throw std::invalid_argument("cannot open config file: " + file.string());
    }
    json j = json::parse(in, nullptr, /*allow_exceptions=*/false, /*ignore_comments=*/true);
    if (j.is_discarded()) {
        throw std::invalid_argument("config file is not valid JSON: " + file.string());
    }
    apply_json(config, j);
}

void validate(const ServerConfig& c) {
    auto require = [](bool ok, const char* message) {
        if (!ok) {
            throw std::invalid_argument(message);
        }
    };
    require(!c.host.empty(), "host must not be empty");
    require(c.io_threads >= 1 && c.io_threads <= 64, "io_threads must be in [1, 64]");
    require(c.worker_threads >= 1 && c.worker_threads <= 1024, "worker_threads must be in [1, 1024]");
    require(c.queue_capacity >= 1, "queue_capacity must be >= 1");
    require(c.max_message_bytes >= 64 && c.max_message_bytes <= 64ULL * 1024 * 1024,
            "max_message_bytes must be in [64, 64 MiB]");
    require(c.max_connections >= 1, "max_connections must be >= 1");
    require(c.max_pending_write_bytes >= c.max_message_bytes,
            "max_pending_write_bytes must be >= max_message_bytes");
    require(c.max_retained_jobs >= 1, "max_retained_jobs must be >= 1");
    require(c.task_limits.io_chunk_bytes >= 512 && c.task_limits.io_chunk_bytes <= 16 * 1024 * 1024,
            "io_chunk_bytes must be in [512, 16 MiB]");
    require(c.task_limits.max_decompressed_bytes >= 1, "max_decompressed_bytes must be >= 1");
}

}  // namespace concurrentcore
