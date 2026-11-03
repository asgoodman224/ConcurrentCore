// concurrentcore-client: command-line client for the ConcurrentCore server.

#include "concurrentcore/client/client.hpp"
#include "concurrentcore/version.hpp"

#include <algorithm>
#include <charconv>
#include <chrono>
#include <cstdio>
#include <iomanip>
#include <iostream>
#include <map>
#include <optional>
#include <set>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

namespace {

using nlohmann::json;
using concurrentcore::Client;
using concurrentcore::ClientError;

constexpr int kExitOk = 0;
constexpr int kExitRequestFailed = 1;
constexpr int kExitUsage = 2;
constexpr int kExitConnection = 3;

constexpr std::string_view kHelp = R"(Usage: concurrentcore-client [global options] <command> [command options]

Commands:
  ping                         Check that the server is reachable
  submit --type TYPE [...]     Submit a job; prints the job id
  status --job-id ID           Show a job's status and timings
  result --job-id ID           Show a finished job's result or error
  wait   --job-id ID           Poll until the job finishes, then show the result
  cancel --job-id ID           Cancel a queued job / request stop of a running job
  stats                        Show server statistics
  help                         Show this help

Global options:
  --host HOST                  Server address (default 127.0.0.1)
  --port N                     Server port (default 7878)
  --timeout-ms N               Per-request timeout (default 10000)
  --json                       Print raw JSON responses

Submit options (paths are relative to the server's workspace directory):
  --type TYPE                  sha256 | compress | decompress | text-analysis | compute
  --file PATH                  Input file (sha256, text-analysis; alias of --input)
  --input PATH / --output PATH Input / output files (compress, decompress)
  --level N                    Compression level 0-9 (compress, default 6)
  --format zlib|gzip           Container format (compress, default zlib)
  --overwrite                  Allow replacing an existing output file
  --max-output-bytes N         Decompression size limit (decompress)
  --text TEXT                  Inline text (text-analysis)
  --limit N                    Count primes below N (compute)
  --params JSON                Raw params object, merged over the flags above
  --wait                       Wait for completion and print the result
  --wait-timeout-ms N          How long --wait / wait may block (default 300000)

Examples:
  concurrentcore-client ping
  concurrentcore-client submit --type sha256 --file data/sample.bin --wait
  concurrentcore-client submit --type text-analysis --file sample.txt
  concurrentcore-client submit --type compress --input sample.txt --format gzip --level 9
  concurrentcore-client submit --type decompress --input sample.txt.gz --output copy.txt
  concurrentcore-client submit --type compute --limit 5000000
  concurrentcore-client status --job-id 1
  concurrentcore-client result --job-id 1
  concurrentcore-client cancel --job-id 1
  concurrentcore-client stats

Exit codes: 0 success, 1 request rejected by server, 2 usage error, 3 connection error.
)";

struct UsageError : std::invalid_argument {
    using std::invalid_argument::invalid_argument;
};

struct Args {
    std::string command;
    std::map<std::string, std::string, std::less<>> options;
    std::set<std::string, std::less<>> flags;
};

const std::set<std::string_view> kBooleanFlags{"--json", "--overwrite", "--wait", "--help"};

Args parse(const std::vector<std::string_view>& argv) {
    Args a;
    for (std::size_t i = 0; i < argv.size(); ++i) {
        const std::string_view tok = argv[i];
        if (tok.starts_with("--")) {
            if (kBooleanFlags.contains(tok)) {
                a.flags.emplace(tok);
                continue;
            }
            if (i + 1 >= argv.size()) {
                throw UsageError(std::string(tok) + " requires a value");
            }
            a.options[std::string(tok)] = std::string(argv[++i]);
        } else if (tok == "-h") {
            a.command = "help";
        } else if (a.command.empty()) {
            a.command = std::string(tok);
        } else {
            throw UsageError("unexpected argument '" + std::string(tok) + "'");
        }
    }
    return a;
}

std::optional<std::string> opt(const Args& a, std::string_view key) {
    const auto it = a.options.find(key);
    return it == a.options.end() ? std::nullopt : std::optional<std::string>(it->second);
}

std::uint64_t to_uint(std::string_view key, const std::string& text) {
    std::uint64_t v = 0;
    const auto [p, ec] = std::from_chars(text.data(), text.data() + text.size(), v);
    if (ec != std::errc() || p != text.data() + text.size()) {
        throw UsageError(std::string(key) + " expects a non-negative integer");
    }
    return v;
}

std::uint64_t uint_opt(const Args& a, std::string_view key, std::uint64_t fallback) {
    const auto v = opt(a, key);
    return v ? to_uint(key, *v) : fallback;
}

std::uint64_t require_job_id(const Args& a) {
    const auto v = opt(a, "--job-id");
    if (!v) {
        throw UsageError("--job-id is required");
    }
    return to_uint("--job-id", *v);
}

json build_params(const Args& a, const std::string& type) {
    json p = json::object();
    const auto file = opt(a, "--file");
    const auto input = opt(a, "--input");
    if (type == "sha256" || type == "text-analysis") {
        if (file || input) {
            p["file"] = file ? *file : *input;
        }
        if (const auto text = opt(a, "--text")) {
            p["text"] = *text;
        }
    } else if (type == "compress" || type == "decompress") {
        if (input || file) {
            p["input"] = input ? *input : *file;
        }
        if (const auto out = opt(a, "--output")) {
            p["output"] = *out;
        }
        if (a.flags.contains("--overwrite")) {
            p["overwrite"] = true;
        }
        if (type == "compress") {
            if (const auto level = opt(a, "--level")) {
                p["level"] = to_uint("--level", *level);
            }
            if (const auto fmt = opt(a, "--format")) {
                p["format"] = *fmt;
            }
        } else if (const auto max = opt(a, "--max-output-bytes")) {
            p["max_output_bytes"] = to_uint("--max-output-bytes", *max);
        }
    } else if (type == "compute") {
        if (const auto limit = opt(a, "--limit")) {
            p["limit"] = to_uint("--limit", *limit);
        }
    }
    if (const auto raw = opt(a, "--params")) {
        const json extra = json::parse(*raw, nullptr, false);
        if (!extra.is_object()) {
            throw UsageError("--params must be a JSON object");
        }
        p.update(extra);
    }
    return p;
}

std::string fmt_ms(const json& v) {
    if (!v.is_number()) {
        return "-";
    }
    std::ostringstream os;
    os << std::fixed << std::setprecision(2) << v.get<double>() << " ms";
    return os.str();
}

// Returns true if the response is ok; otherwise prints the error.
bool check(const json& response) {
    if (response.value("ok", false)) {
        return true;
    }
    const json& err = response.contains("error") ? response["error"] : json::object();
    std::cerr << "error: " << err.value("code", "UNKNOWN") << ": " << err.value("message", "")
              << '\n';
    return false;
}

void print_job(const json& job, bool with_result) {
    std::cout << "Job " << job.value("job_id", 0) << '\n'
              << "  type:        " << job.value("task_type", "?") << '\n'
              << "  status:      " << job.value("status", "?") << '\n'
              << "  queue wait:  " << fmt_ms(job.value("queue_wait_ms", json())) << '\n'
              << "  execution:   " << fmt_ms(job.value("execution_ms", json())) << '\n';
    if (job.value("cancel_requested", false)) {
        std::cout << "  cancel:      requested\n";
    }
    if (!with_result) {
        return;
    }
    if (job.contains("error") && !job["error"].is_null()) {
        std::cout << "  error:       " << job["error"].value("code", "?") << ": "
                  << job["error"].value("message", "") << '\n';
    }
    if (job.contains("result") && !job["result"].is_null()) {
        std::cout << "  result:\n";
        std::istringstream lines(job["result"].dump(2));
        for (std::string line; std::getline(lines, line);) {
            std::cout << "    " << line << '\n';
        }
    }
}

void print_stats(const json& s) {
    const json& jobs = s["jobs"];
    std::cout << std::fixed << std::setprecision(2)
              << "Uptime:            " << s.value("uptime_seconds", 0.0) << " s\n"
              << "Accepting jobs:    " << (s.value("accepting_jobs", false) ? "yes" : "no") << '\n'
              << "Jobs submitted:    " << jobs.value("submitted", 0) << '\n'
              << "Jobs completed:    " << jobs.value("completed", 0) << '\n'
              << "Jobs failed:       " << jobs.value("failed", 0) << '\n'
              << "Jobs cancelled:    " << jobs.value("cancelled", 0) << '\n'
              << "Jobs rejected:     " << jobs.value("rejected", 0) << '\n'
              << "Jobs running:      " << jobs.value("running", 0) << '\n'
              << "Queue depth:       " << s["queue"].value("depth", 0) << " / "
              << s["queue"].value("capacity", 0) << '\n'
              << "Workers active:    " << s["workers"].value("active", 0) << " / "
              << s["workers"].value("total", 0) << '\n'
              << "Avg execution:     " << s["latency_ms"].value("avg_execution", 0.0) << " ms\n"
              << "Avg queue wait:    " << s["latency_ms"].value("avg_queue_wait", 0.0) << " ms\n"
              << "Throughput:        " << s["throughput"].value("jobs_per_second", 0.0)
              << " jobs/s (lifetime average)\n";
    if (s.contains("connections")) {
        std::cout << "Connections:       " << s["connections"].value("active", 0) << " active, "
                  << s["connections"].value("accepted_total", 0) << " accepted total\n";
    }
}

bool is_terminal(const std::string& status) {
    return status == "COMPLETED" || status == "FAILED" || status == "CANCELLED";
}

int run(const Args& a) {
    const bool raw = a.flags.contains("--json");
    const std::string host = opt(a, "--host").value_or("127.0.0.1");
    const auto port_value = uint_opt(a, "--port", 7878);
    if (port_value == 0 || port_value > 65535) {
        throw UsageError("--port must be in [1, 65535]");
    }
    const auto timeout = std::chrono::milliseconds(uint_opt(a, "--timeout-ms", 10'000));
    const auto wait_timeout = std::chrono::milliseconds(uint_opt(a, "--wait-timeout-ms", 300'000));

    const std::string& cmd = a.command;
    // Validate the command before connecting so usage errors are reported
    // even when no server is running.
    static const std::set<std::string_view> kCommands{"ping",   "submit", "status", "result",
                                                      "wait",   "cancel", "stats"};
    if (!kCommands.contains(cmd)) {
        throw UsageError("unknown command '" + cmd + "' (see 'help')");
    }
    std::optional<std::uint64_t> job_id;
    if (cmd == "status" || cmd == "result" || cmd == "wait" || cmd == "cancel") {
        job_id = require_job_id(a);
    }
    std::string type;
    json params;
    if (cmd == "submit") {
        type = opt(a, "--type").value_or("");
        if (type.empty()) {
            throw UsageError("submit requires --type");
        }
        params = build_params(a, type);
    }

    Client client;
    client.connect(host, static_cast<std::uint16_t>(port_value), timeout);

    auto emit = [raw](const json& response) {
        if (raw) {
            std::cout << response.dump(2) << '\n';
        }
        return check(response);
    };

    auto wait_for_result = [&](std::uint64_t id) -> int {
        // Poll with gentle backoff; bounded by --wait-timeout-ms.
        const auto deadline = std::chrono::steady_clock::now() + wait_timeout;
        auto delay = std::chrono::milliseconds(20);
        for (;;) {
            const json st = client.status(id, timeout);
            if (!check(st)) {
                return kExitRequestFailed;
            }
            if (is_terminal(st["result"].value("status", ""))) {
                break;
            }
            if (std::chrono::steady_clock::now() + delay > deadline) {
                std::cerr << "error: timed out waiting for job " << id << '\n';
                return kExitRequestFailed;
            }
            // Short client-side sleep between polls; the server never sleeps.
            std::this_thread::sleep_for(delay);
            delay = std::min(delay * 2, std::chrono::milliseconds(500));
        }
        const json res = client.result(id, timeout);
        if (!emit(res)) {
            return kExitRequestFailed;
        }
        if (!raw) {
            print_job(res["result"], true);
        }
        return res["result"].value("status", "") == "COMPLETED" ? kExitOk : kExitRequestFailed;
    };

    if (cmd == "ping") {
        const auto t0 = std::chrono::steady_clock::now();
        const json r = client.ping(timeout);
        const auto rtt = std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - t0);
        if (!emit(r)) {
            return kExitRequestFailed;
        }
        if (!raw) {
            std::cout << "PONG from " << host << ':' << port_value << " (protocol v"
                      << r["result"].value("protocol_version", 0) << ", round trip " << std::fixed
                      << std::setprecision(2) << rtt.count() << " ms)\n";
        }
        return kExitOk;
    }
    if (cmd == "submit") {
        const bool wait = a.flags.contains("--wait");
        const json r = client.submit(type, params, /*notify=*/wait, timeout);
        if (!emit(r)) {
            return kExitRequestFailed;
        }
        const auto id = r["result"].value("job_id", std::uint64_t{0});
        if (!raw) {
            std::cout << "Job " << id << " submitted (QUEUED)\n";
        }
        if (!wait) {
            return kExitOk;
        }
        // The server pushes JOB_FINISHED to this connection when the job is
        // done; no polling needed.
        const auto event = client.wait_for_event(wait_timeout);
        if (!event) {
            std::cerr << "error: timed out waiting for job " << id << '\n';
            return kExitRequestFailed;
        }
        if (raw) {
            std::cout << event->dump(2) << '\n';
        } else {
            print_job((*event)["data"], true);
        }
        return (*event)["data"].value("status", "") == "COMPLETED" ? kExitOk : kExitRequestFailed;
    }
    if (cmd == "status" || cmd == "result") {
        const bool with_result = cmd == "result";
        const json r = with_result ? client.result(*job_id, timeout) : client.status(*job_id, timeout);
        if (!emit(r)) {
            return kExitRequestFailed;
        }
        if (!raw) {
            print_job(r["result"], with_result);
        }
        return kExitOk;
    }
    if (cmd == "wait") {
        return wait_for_result(*job_id);
    }
    if (cmd == "cancel") {
        const json r = client.cancel(*job_id, timeout);
        if (!emit(r)) {
            return kExitRequestFailed;
        }
        if (!raw) {
            std::cout << "Job " << *job_id << ": " << r["result"].value("outcome", "?") << '\n';
        }
        return kExitOk;
    }
    // stats
    const json r = client.stats(timeout);
    if (!emit(r)) {
        return kExitRequestFailed;
    }
    if (!raw) {
        print_stats(r["result"]);
    }
    return kExitOk;
}

}  // namespace

int main(int argc, char** argv) {
    const std::vector<std::string_view> argv_view(argv + 1, argv + argc);
    try {
        const Args args = parse(argv_view);
        if (args.command.empty() || args.command == "help" || args.flags.contains("--help")) {
            std::cout << kHelp;
            return args.command.empty() ? kExitUsage : kExitOk;
        }
        if (args.command == "version") {
            std::cout << "concurrentcore-client " << concurrentcore::kVersion << '\n';
            return kExitOk;
        }
        return run(args);
    } catch (const UsageError& e) {
        std::cerr << "usage error: " << e.what() << "\nRun 'concurrentcore-client help' for usage.\n";
        return kExitUsage;
    } catch (const ClientError& e) {
        std::cerr << "connection error: " << e.what() << '\n';
        return kExitConnection;
    } catch (const std::exception& e) {
        std::cerr << "error: " << e.what() << '\n';
        return kExitRequestFailed;
    }
}
