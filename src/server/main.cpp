// concurrentcore-server: multithreaded task processing server.

#include "concurrentcore/server/tcp_server.hpp"
#include "concurrentcore/version.hpp"

#include <asio.hpp>
#include <spdlog/spdlog.h>

#include <charconv>
#include <csignal>
#include <iostream>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace {

using concurrentcore::ServerConfig;

constexpr std::string_view kUsage = R"(Usage: concurrentcore-server [options]

Options:
  --config FILE              Load settings from a JSON file (CLI flags override it)
  --host HOST                Listen address (default 127.0.0.1)
  --port N                   Listen port, 0 = ephemeral (default 7878)
  --workers N                Worker threads (default: hardware concurrency)
  --io-threads N             Network event-loop threads (default 2)
  --queue-capacity N         Bounded task queue capacity (default 1024)
  --workspace DIR            Directory that file tasks are confined to (default ./workspace)
  --max-message-bytes N      Maximum request size (default 1048576)
  --max-connections N        Maximum concurrent clients (default 256)
  --idle-timeout S           Close idle connections after S seconds, 0 = never (default 300)
  --retention N              Finished jobs kept for queries (default 10000)
  --drain-timeout-ms N       Graceful shutdown drain timeout (default 10000)
  --log-level LEVEL          trace|debug|info|warn|error (default info)
  --version                  Print version and exit
  -h, --help                 Show this help

Stop the server with Ctrl+C (SIGINT) or SIGTERM: it stops accepting work,
lets queued jobs finish (up to the drain timeout), cancels the rest, and exits.
)";

std::uint64_t parse_uint(std::string_view flag, std::string_view text) {
    std::uint64_t value = 0;
    const auto [ptr, ec] = std::from_chars(text.data(), text.data() + text.size(), value);
    if (ec != std::errc() || ptr != text.data() + text.size()) {
        throw std::invalid_argument(std::string(flag) + " expects a non-negative integer, got '" +
                                    std::string(text) + "'");
    }
    return value;
}

ServerConfig parse_args(const std::vector<std::string_view>& args) {
    ServerConfig config;
    // First pass: --config, so explicit flags can override file values.
    for (std::size_t i = 0; i + 1 < args.size(); ++i) {
        if (args[i] == "--config") {
            concurrentcore::load_config_file(config, std::string(args[i + 1]));
        }
    }
    for (std::size_t i = 0; i < args.size(); ++i) {
        const std::string_view flag = args[i];
        auto value = [&]() -> std::string_view {
            if (i + 1 >= args.size()) {
                throw std::invalid_argument(std::string(flag) + " requires a value");
            }
            return args[++i];
        };
        if (flag == "--config") {
            (void)value();
        } else if (flag == "--host") {
            config.host = std::string(value());
        } else if (flag == "--port") {
            const auto port = parse_uint(flag, value());
            if (port > 65535) {
                throw std::invalid_argument("--port must be <= 65535");
            }
            config.port = static_cast<std::uint16_t>(port);
        } else if (flag == "--workers") {
            config.worker_threads = parse_uint(flag, value());
        } else if (flag == "--io-threads") {
            config.io_threads = parse_uint(flag, value());
        } else if (flag == "--queue-capacity") {
            config.queue_capacity = parse_uint(flag, value());
        } else if (flag == "--workspace") {
            const std::string_view v = value();
            config.workspace = std::filesystem::path(std::u8string(v.begin(), v.end()));
        } else if (flag == "--max-message-bytes") {
            config.max_message_bytes = parse_uint(flag, value());
        } else if (flag == "--max-connections") {
            config.max_connections = parse_uint(flag, value());
        } else if (flag == "--idle-timeout") {
            config.idle_timeout = std::chrono::seconds(parse_uint(flag, value()));
        } else if (flag == "--retention") {
            config.max_retained_jobs = parse_uint(flag, value());
        } else if (flag == "--drain-timeout-ms") {
            config.shutdown_drain_timeout = std::chrono::milliseconds(parse_uint(flag, value()));
        } else if (flag == "--log-level") {
            config.log_level = std::string(value());
        } else {
            throw std::invalid_argument("unknown option '" + std::string(flag) + "' (see --help)");
        }
    }
    return config;
}

}  // namespace

int main(int argc, char** argv) {
    const std::vector<std::string_view> args(argv + 1, argv + argc);
    for (const auto a : args) {
        if (a == "-h" || a == "--help") {
            std::cout << kUsage;
            return 0;
        }
        if (a == "--version") {
            std::cout << "concurrentcore-server " << concurrentcore::kVersion << '\n';
            return 0;
        }
    }

    ServerConfig config;
    try {
        config = parse_args(args);
        concurrentcore::validate(config);
    } catch (const std::exception& e) {
        std::cerr << "error: " << e.what() << '\n';
        return 2;
    }

    const auto level = spdlog::level::from_str(config.log_level);
    spdlog::set_level(level);
    spdlog::set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%^%l%$] [tid %t] %v");

    try {
        concurrentcore::Server server(config);

        // Install signal handling before start() so a signal arriving during
        // startup is not lost.
        asio::io_context signal_io;
        asio::signal_set signals(signal_io, SIGINT, SIGTERM);
        std::optional<int> received;
        signals.async_wait([&received](std::error_code ec, int signo) {
            if (!ec) {
                received = signo;
            }
        });

        server.start();
        signal_io.run();  // blocks until SIGINT / SIGTERM
        spdlog::info("received signal {}, shutting down", received.value_or(0));
        server.shutdown();
    } catch (const std::exception& e) {
        spdlog::critical("fatal: {}", e.what());
        return 1;
    }
    return 0;
}
