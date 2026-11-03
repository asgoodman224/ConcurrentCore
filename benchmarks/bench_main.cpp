// concurrentcore-bench: reproducible benchmark harness.
//
// Measures how throughput and latency scale with the number of worker
// threads, for three workloads and two measurement scopes:
//
//   scope "task"    : in-process. Jobs go straight into a JobManager; latency
//                     is submit -> completion callback. Isolates the
//                     scheduler + task execution from networking.
//   scope "network" : end-to-end. A real Server on localhost, several client
//                     connections submitting with notify=true; latency is
//                     measured client-side from send to JOB_FINISHED event.
//
//   workload "compute" : CPU-bound prime counting (deterministic).
//   workload "sha256"  : read + hash files from the workspace (I/O + CPU).
//   workload "compress": read + deflate + write files (I/O + CPU).
//   workload "tiny"    : near-empty jobs; measures per-job overhead.
//
// All inputs are generated deterministically from fixed seeds. Results are
// written to CSV and to a Markdown report; nothing is estimated or invented.

#include "concurrentcore/client/client.hpp"
#include "concurrentcore/jobs/job_manager.hpp"
#include "concurrentcore/server/tcp_server.hpp"
#include "concurrentcore/version.hpp"

#include <spdlog/spdlog.h>

#include <algorithm>
#include <atomic>
#include <charconv>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <mutex>
#include <numeric>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#ifdef _WIN32
#include <windows.h>
#else
#include <sys/resource.h>
#endif

#if defined(__x86_64__) || defined(__i386__)
#include <cpuid.h>
#elif defined(_M_X64) || defined(_M_IX86)
#include <intrin.h>
#endif

namespace fs = std::filesystem;
using namespace concurrentcore;
using Clock = std::chrono::steady_clock;

namespace {

// ----------------------------------------------------------- environment

std::string cpu_brand() {
#if defined(__x86_64__) || defined(__i386__)
    unsigned regs[12] = {};
    if (__get_cpuid_max(0x80000000, nullptr) >= 0x80000004) {
        for (unsigned i = 0; i < 3; ++i) {
            __get_cpuid(0x80000002 + i, &regs[i * 4], &regs[i * 4 + 1], &regs[i * 4 + 2], &regs[i * 4 + 3]);
        }
        std::string s(reinterpret_cast<const char*>(regs), sizeof(regs));
        s.erase(s.find_last_not_of(std::string(" \0", 2)) + 1);
        s.erase(0, s.find_first_not_of(' '));
        return s;
    }
#elif defined(_M_X64) || defined(_M_IX86)
    int regs[12] = {};
    for (int i = 0; i < 3; ++i) {
        __cpuid(&regs[i * 4], static_cast<int>(0x80000002 + i));
    }
    std::string s(reinterpret_cast<const char*>(regs), sizeof(regs));
    s.erase(s.find_last_not_of(std::string(" \0", 2)) + 1);
    return s;
#endif
    return "unknown";
}

std::string os_description() {
#ifdef _WIN32
    using RtlGetVersionFn = LONG(WINAPI*)(PRTL_OSVERSIONINFOW);
    if (HMODULE ntdll = GetModuleHandleW(L"ntdll.dll")) {
        // Cast through FARPROC's generic function pointer type.
        auto fn = reinterpret_cast<RtlGetVersionFn>(reinterpret_cast<void*>(GetProcAddress(ntdll, "RtlGetVersion")));
        RTL_OSVERSIONINFOW info{};
        info.dwOSVersionInfoSize = sizeof(info);
        if (fn != nullptr && fn(&info) == 0) {
            return "Windows " + std::to_string(info.dwMajorVersion) + "." +
                   std::to_string(info.dwMinorVersion) + " build " + std::to_string(info.dwBuildNumber);
        }
    }
    return "Windows";
#else
    std::ifstream in("/etc/os-release");
    for (std::string line; std::getline(in, line);) {
        if (line.rfind("PRETTY_NAME=", 0) == 0) {
            return line.substr(13, line.size() - 14);
        }
    }
    return CC_BENCH_SYSTEM;
#endif
}

std::string compiler_description() {
#if defined(__clang__)
    return std::string("Clang ") + __clang_version__;
#elif defined(__GNUC__)
    return std::string("GCC ") + __VERSION__;
#elif defined(_MSC_VER)
    return "MSVC " + std::to_string(_MSC_VER);
#else
    return "unknown";
#endif
}

/// Process CPU time (user + kernel) consumed so far.
std::chrono::nanoseconds process_cpu_time() {
#ifdef _WIN32
    FILETIME created, exited, kernel, user;
    GetProcessTimes(GetCurrentProcess(), &created, &exited, &kernel, &user);
    auto to_ns = [](FILETIME ft) {
        const std::uint64_t t = (std::uint64_t{ft.dwHighDateTime} << 32) | ft.dwLowDateTime;
        return std::chrono::nanoseconds(static_cast<std::int64_t>(t * 100));
    };
    return to_ns(kernel) + to_ns(user);
#else
    rusage ru{};
    getrusage(RUSAGE_SELF, &ru);
    auto tv = [](timeval v) {
        return std::chrono::seconds(v.tv_sec) + std::chrono::microseconds(v.tv_usec);
    };
    return std::chrono::duration_cast<std::chrono::nanoseconds>(tv(ru.ru_utime) + tv(ru.ru_stime));
#endif
}

// ------------------------------------------------------------- statistics

double percentile(std::vector<double> sorted, double p) {
    if (sorted.empty()) {
        return 0.0;
    }
    // Nearest-rank method on an already sorted vector.
    const auto rank = static_cast<std::size_t>(std::ceil(p / 100.0 * static_cast<double>(sorted.size())));
    return sorted[std::clamp<std::size_t>(rank, 1, sorted.size()) - 1];
}

struct RunResult {
    double wall_ms = 0;
    double cpu_ms = 0;
    std::vector<double> latencies_ms;
    std::uint64_t failures = 0;
};

struct Row {
    std::string scope;
    std::string workload;
    std::size_t workers = 0;
    std::size_t jobs = 0;
    std::size_t repetitions = 0;
    double wall_median = 0, wall_min = 0, wall_max = 0;
    double throughput = 0;
    double lat_avg = 0, p50 = 0, p95 = 0, p99 = 0;
    double cpu_util_pct = 0;  // 100 = one fully busy core
    double speedup = 0;
    std::uint64_t failures = 0;
};

Row summarize(const std::string& scope, const std::string& workload, std::size_t workers,
              std::size_t jobs, const std::vector<RunResult>& runs) {
    Row r;
    r.scope = scope;
    r.workload = workload;
    r.workers = workers;
    r.jobs = jobs;
    r.repetitions = runs.size();
    std::vector<double> walls;
    std::vector<double> lat;
    double cpu_total = 0;
    double wall_total = 0;
    for (const auto& run : runs) {
        walls.push_back(run.wall_ms);
        lat.insert(lat.end(), run.latencies_ms.begin(), run.latencies_ms.end());
        cpu_total += run.cpu_ms;
        wall_total += run.wall_ms;
        r.failures += run.failures;
    }
    std::sort(walls.begin(), walls.end());
    std::sort(lat.begin(), lat.end());
    r.wall_median = walls[walls.size() / 2];
    r.wall_min = walls.front();
    r.wall_max = walls.back();
    r.throughput = static_cast<double>(jobs) / (r.wall_median / 1000.0);
    r.lat_avg = lat.empty() ? 0 : std::accumulate(lat.begin(), lat.end(), 0.0) / static_cast<double>(lat.size());
    r.p50 = percentile(lat, 50);
    r.p95 = percentile(lat, 95);
    r.p99 = percentile(lat, 99);
    r.cpu_util_pct = wall_total > 0 ? 100.0 * cpu_total / wall_total : 0;
    return r;
}

// --------------------------------------------------------------- workloads

struct Options {
    std::vector<std::size_t> workers{1, 2, 4, 8, 16};
    std::size_t repetitions = 3;
    std::size_t compute_jobs = 400;
    std::size_t tiny_jobs = 20000;
    std::uint64_t compute_limit = 200'000;
    std::size_t io_jobs = 160;
    std::size_t io_files = 16;
    std::size_t io_file_bytes = 4 * 1024 * 1024;
    std::size_t clients = 8;
    std::vector<std::string> scopes{"task", "network"};
    std::vector<std::string> workloads{"compute", "sha256", "compress", "tiny"};
    std::string csv = "benchmark_results.csv";
    std::string markdown = "benchmark_report.md";
};

void generate_files(const fs::path& dir, const Options& o) {
    std::mt19937_64 rng(20261103);  // fixed seed: identical inputs every run
    std::string buf(o.io_file_bytes, '\0');
    static constexpr std::string_view kWords[] = {"alpha ", "beta ", "gamma ", "delta ", "epsilon\n"};
    for (std::size_t f = 0; f < o.io_files; ++f) {
        // Half random bytes, half text: realistic, moderately compressible.
        for (std::size_t i = 0; i < buf.size();) {
            if ((rng() & 1U) == 0) {
                buf[i++] = static_cast<char>(rng());
            } else {
                for (const char c : kWords[rng() % 5]) {
                    if (i < buf.size()) {
                        buf[i++] = c;
                    }
                }
            }
        }
        std::ofstream(dir / ("input_" + std::to_string(f) + ".bin"), std::ios::binary)
            .write(buf.data(), static_cast<std::streamsize>(buf.size()));
    }
}

/// Builds the request for job number `i` of a workload.
std::pair<std::string, nlohmann::json> job_request(const std::string& workload, std::size_t i,
                                                   const Options& o) {
    const std::string file = "input_" + std::to_string(i % o.io_files) + ".bin";
    if (workload == "tiny") {
        // Near-zero work per job: isolates scheduling / protocol overhead.
        return {"text-analysis", {{"text", "tiny job " + std::to_string(i)}}};
    }
    if (workload == "compute") {
        return {"compute", {{"limit", o.compute_limit}}};
    }
    if (workload == "sha256") {
        return {"sha256", {{"file", file}}};
    }
    return {"compress", {{"input", file}, {"output", "out_" + std::to_string(i) + ".zz"}, {"level", 1}, {"overwrite", true}}};
}

std::size_t job_count(const std::string& workload, const Options& o) {
    if (workload == "tiny") {
        return o.tiny_jobs;
    }
    return workload == "compute" ? o.compute_jobs : o.io_jobs;
}

RunResult run_task_scope(const std::string& workload, std::size_t workers, const fs::path& ws,
                         const Options& o) {
    const std::size_t n = job_count(workload, o);
    JobManagerConfig cfg;
    cfg.worker_count = workers;
    cfg.queue_capacity = n + 16;
    cfg.max_retained_jobs = n + 16;
    JobManager mgr(cfg, std::make_shared<const Workspace>(ws));

    std::vector<Clock::time_point> submitted(n);
    std::vector<double> latency(n, 0.0);
    std::atomic<std::uint64_t> failures{0};
    std::mutex m;
    std::condition_variable cv;
    std::size_t done = 0;

    const auto cpu0 = process_cpu_time();
    const auto t0 = Clock::now();
    for (std::size_t i = 0; i < n; ++i) {
        auto [type, params] = job_request(workload, i, o);
        TaskSpec spec = parse_task_spec(type, params, mgr.limits());
        submitted[i] = Clock::now();
        const auto r = mgr.submit(std::move(spec), [&, i](const JobSnapshot& s) {
            const auto now = Clock::now();
            latency[i] = std::chrono::duration<double, std::milli>(now - submitted[i]).count();
            if (s.status != JobStatus::Completed) {
                failures.fetch_add(1);
            }
            std::lock_guard lock(m);
            ++done;
            cv.notify_one();
        });
        if (!r.ok()) {
            throw std::runtime_error("benchmark submission rejected");
        }
    }
    {
        std::unique_lock lock(m);
        cv.wait(lock, [&] { return done == n; });
    }
    const auto t1 = Clock::now();
    const auto cpu1 = process_cpu_time();
    mgr.shutdown(std::chrono::milliseconds(0));

    RunResult r;
    r.wall_ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
    r.cpu_ms = std::chrono::duration<double, std::milli>(cpu1 - cpu0).count();
    r.latencies_ms = std::move(latency);
    r.failures = failures.load();
    return r;
}

RunResult run_network_scope(const std::string& workload, std::size_t workers, const fs::path& ws,
                            const Options& o) {
    const std::size_t n = job_count(workload, o);
    ServerConfig cfg;
    cfg.port = 0;
    cfg.io_threads = 2;
    cfg.worker_threads = workers;
    cfg.queue_capacity = n + 16;
    cfg.max_retained_jobs = n + 16;
    cfg.workspace = ws;
    Server server(cfg);
    server.start();

    // Connect every client before starting the clock.
    std::vector<std::unique_ptr<Client>> clients;
    for (std::size_t c = 0; c < o.clients; ++c) {
        clients.push_back(std::make_unique<Client>());
        clients.back()->connect("127.0.0.1", server.port());
    }

    std::vector<std::vector<double>> lat(o.clients);
    std::atomic<std::uint64_t> failures{0};
    const auto cpu0 = process_cpu_time();
    const auto t0 = Clock::now();
    std::vector<std::thread> threads;
    for (std::size_t c = 0; c < o.clients; ++c) {
        threads.emplace_back([&, c] {
            Client& client = *clients[c];
            std::map<std::uint64_t, Clock::time_point> sent;  // job id -> send time
            for (std::size_t i = c; i < n; i += o.clients) {
                auto [type, params] = job_request(workload, i, o);
                const auto ts = Clock::now();
                const auto resp = client.submit(type, params, /*notify=*/true, std::chrono::seconds(60));
                if (!resp.value("ok", false)) {
                    failures.fetch_add(1);
                    continue;
                }
                sent[resp["result"]["job_id"].get<std::uint64_t>()] = ts;
            }
            for (std::size_t k = 0; k < sent.size(); ++k) {
                const auto e = client.wait_for_event(std::chrono::seconds(300));
                const auto now = Clock::now();
                if (!e) {
                    failures.fetch_add(1);
                    break;
                }
                const auto& job = (*e)["data"];
                if (job.value("status", "") != "COMPLETED") {
                    failures.fetch_add(1);
                }
                const auto it = sent.find(job["job_id"].get<std::uint64_t>());
                if (it != sent.end()) {
                    lat[c].push_back(std::chrono::duration<double, std::milli>(now - it->second).count());
                }
            }
        });
    }
    for (auto& t : threads) {
        t.join();
    }
    const auto t1 = Clock::now();
    const auto cpu1 = process_cpu_time();
    clients.clear();
    server.shutdown(std::chrono::milliseconds(0));

    RunResult r;
    r.wall_ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
    r.cpu_ms = std::chrono::duration<double, std::milli>(cpu1 - cpu0).count();
    for (auto& v : lat) {
        r.latencies_ms.insert(r.latencies_ms.end(), v.begin(), v.end());
    }
    r.failures = failures.load();
    return r;
}

// ------------------------------------------------------------------ output

std::string fixed(double v, int precision = 2) {
    std::ostringstream os;
    os << std::fixed << std::setprecision(precision) << v;
    return os.str();
}

void write_csv(const std::string& path, const std::vector<Row>& rows) {
    std::ofstream out(path);
    out << "scope,workload,workers,jobs,repetitions,wall_ms_median,wall_ms_min,wall_ms_max,"
           "throughput_jobs_per_s,latency_avg_ms,latency_p50_ms,latency_p95_ms,latency_p99_ms,"
           "cpu_util_pct,speedup_vs_1_worker,failures\n";
    for (const auto& r : rows) {
        out << r.scope << ',' << r.workload << ',' << r.workers << ',' << r.jobs << ','
            << r.repetitions << ',' << fixed(r.wall_median) << ',' << fixed(r.wall_min) << ','
            << fixed(r.wall_max) << ',' << fixed(r.throughput) << ',' << fixed(r.lat_avg, 3) << ','
            << fixed(r.p50, 3) << ',' << fixed(r.p95, 3) << ',' << fixed(r.p99, 3) << ','
            << fixed(r.cpu_util_pct, 1) << ',' << fixed(r.speedup, 2) << ',' << r.failures << '\n';
    }
}

void write_markdown(const std::string& path, const std::vector<Row>& rows, const Options& o,
                    double total_seconds) {
    std::ofstream out(path);
    const auto now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    std::tm tm{};
#ifdef _WIN32
    localtime_s(&tm, &now);
#else
    localtime_r(&now, &tm);
#endif
    const unsigned cores = std::thread::hardware_concurrency();
    out << "# ConcurrentCore benchmark report\n\n"
        << "Generated by `concurrentcore-bench` " << kVersion << " on "
        << std::put_time(&tm, "%Y-%m-%d %H:%M") << " (total run time " << fixed(total_seconds, 1)
        << " s).\n\n"
        << "## Environment\n\n"
        << "| Item | Value |\n|---|---|\n"
        << "| OS | " << os_description() << " |\n"
        << "| CPU | " << cpu_brand() << " |\n"
        << "| Logical cores | " << cores << " |\n"
        << "| Compiler | " << compiler_description() << " |\n"
        << "| Build type | " << CC_BENCH_BUILD_TYPE << " |\n\n"
        << "## Parameters\n\n"
        << "- Repetitions per configuration: " << o.repetitions
        << " (wall time = median; latency percentiles pooled over all repetitions)\n"
        << "- compute: " << o.compute_jobs << " jobs, each counting primes below "
        << o.compute_limit << " (trial division)\n"
        << "- sha256 / compress: " << o.io_jobs << " jobs over " << o.io_files
        << " deterministic files of " << o.io_file_bytes / (1024 * 1024)
        << " MiB (compress uses level 1 and writes its output)\n"
        << "- tiny: " << o.tiny_jobs
        << " inline text-analysis jobs of a few bytes each (measures per-job overhead, not work)\n"
        << "- network scope: " << o.clients
        << " concurrent client connections, 2 io threads, submit with notify=true\n"
        << "- CPU utilization = process CPU time / wall time (100% = one fully busy core; max "
        << cores * 100 << "%)\n\n";

    std::vector<std::pair<std::string, std::string>> groups;
    for (const auto& r : rows) {
        if (std::find(groups.begin(), groups.end(), std::make_pair(r.scope, r.workload)) == groups.end()) {
            groups.emplace_back(r.scope, r.workload);
        }
    }
    out << "## Results\n";
    for (const auto& [scope, workload] : groups) {
        out << "\n### " << scope << " / " << workload << "\n\n"
            << "| Workers | Wall (ms, median) | Throughput (jobs/s) | Speedup | Avg lat (ms) | p50 (ms) | p95 (ms) | p99 (ms) | CPU util | Failures |\n"
            << "|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n";
        for (const auto& r : rows) {
            if (r.scope == scope && r.workload == workload) {
                out << "| " << r.workers << " | " << fixed(r.wall_median, 1) << " | " << fixed(r.throughput, 1)
                    << " | " << fixed(r.speedup) << "x | " << fixed(r.lat_avg) << " | " << fixed(r.p50) << " | "
                    << fixed(r.p95) << " | " << fixed(r.p99) << " | " << fixed(r.cpu_util_pct, 0) << "% | "
                    << r.failures << " |\n";
            }
        }
    }
    out << "\n## Notes\n\n"
        << "- Latency in the task scope is measured from submission to the completion callback, so it\n"
        << "  includes queueing delay: all jobs are submitted at once, so later jobs wait for earlier ones.\n"
        << "- Network-scope latency is measured by the client from sending SUBMIT_JOB to receiving\n"
        << "  the JOB_FINISHED event, and also includes the queueing delay.\n"
        << "- Worker counts above the number of logical cores (" << cores
        << ") oversubscribe the CPU; no further speedup is expected for CPU-bound work there.\n"
        << "- File workloads read from the OS page cache after the warm-up run, so they measure\n"
        << "  hashing/compression throughput more than raw disk bandwidth.\n";
}

// ------------------------------------------------------------------- CLI

std::vector<std::string> split(const std::string& s) {
    std::vector<std::string> out;
    std::stringstream ss(s);
    for (std::string item; std::getline(ss, item, ',');) {
        if (!item.empty()) {
            out.push_back(item);
        }
    }
    return out;
}

std::uint64_t to_uint(const std::string& s) {
    std::uint64_t v = 0;
    const auto [p, ec] = std::from_chars(s.data(), s.data() + s.size(), v);
    if (ec != std::errc() || p != s.data() + s.size()) {
        throw std::invalid_argument("expected a non-negative integer: " + s);
    }
    return v;
}

constexpr std::string_view kUsage = R"(Usage: concurrentcore-bench [options]
  --workers LIST        Worker counts (default 1,2,4,8,16)
  --repetitions N       Runs per configuration (default 3)
  --scopes LIST         task,network (default both)
  --workloads LIST      compute,sha256,compress,tiny (default all)
  --compute-jobs N      Jobs per compute run (default 400)
  --compute-limit N     Prime-count limit per compute job (default 200000)
  --io-jobs N           Jobs per sha256/compress run (default 160)
  --tiny-jobs N         Jobs per "tiny" (overhead) run (default 20000)
  --io-files N          Number of input files (default 16)
  --io-file-mb N        Size of each input file in MiB (default 4)
  --clients N           Client connections in network scope (default 8)
  --csv PATH            CSV output (default benchmark_results.csv)
  --markdown PATH       Markdown report (default benchmark_report.md)
  --quick               Small configuration for smoke testing
)";

Options parse(int argc, char** argv) {
    Options o;
    for (int i = 1; i < argc; ++i) {
        const std::string a = argv[i];
        auto next = [&]() -> std::string {
            if (i + 1 >= argc) {
                throw std::invalid_argument(a + " requires a value");
            }
            return argv[++i];
        };
        if (a == "--workers") {
            o.workers.clear();
            for (const auto& w : split(next())) {
                o.workers.push_back(to_uint(w));
            }
        } else if (a == "--repetitions") {
            o.repetitions = to_uint(next());
        } else if (a == "--scopes") {
            o.scopes = split(next());
        } else if (a == "--workloads") {
            o.workloads = split(next());
        } else if (a == "--compute-jobs") {
            o.compute_jobs = to_uint(next());
        } else if (a == "--compute-limit") {
            o.compute_limit = to_uint(next());
        } else if (a == "--tiny-jobs") {
            o.tiny_jobs = to_uint(next());
        } else if (a == "--io-jobs") {
            o.io_jobs = to_uint(next());
        } else if (a == "--io-files") {
            o.io_files = to_uint(next());
        } else if (a == "--io-file-mb") {
            o.io_file_bytes = to_uint(next()) * 1024 * 1024;
        } else if (a == "--clients") {
            o.clients = to_uint(next());
        } else if (a == "--csv") {
            o.csv = next();
        } else if (a == "--markdown") {
            o.markdown = next();
        } else if (a == "--quick") {
            o.workers = {1, 2, 4};
            o.repetitions = 1;
            o.compute_jobs = 40;
            o.io_jobs = 16;
            o.tiny_jobs = 2000;
            o.io_files = 4;
            o.io_file_bytes = 1024 * 1024;
            o.clients = 4;
        } else if (a == "-h" || a == "--help") {
            std::cout << kUsage;
            std::exit(0);
        } else {
            throw std::invalid_argument("unknown option " + a);
        }
    }
    if (o.workers.empty() || o.repetitions == 0 || o.io_files == 0 || o.clients == 0) {
        throw std::invalid_argument("worker list, repetitions, io-files and clients must be non-empty/positive");
    }
    return o;
}

}  // namespace

int main(int argc, char** argv) {
    Options opts;
    try {
        opts = parse(argc, argv);
    } catch (const std::exception& e) {
        std::cerr << "error: " << e.what() << '\n' << kUsage;
        return 2;
    }
    spdlog::set_level(spdlog::level::warn);

    const fs::path ws = fs::temp_directory_path() / ("cc-bench-" + std::to_string(std::random_device{}()));
    fs::create_directories(ws);
    std::cout << "Generating " << opts.io_files << " input files of " << opts.io_file_bytes << " bytes in "
              << ws.string() << "\n";
    generate_files(ws, opts);

    const auto bench_start = Clock::now();
    std::vector<Row> rows;
    try {
        for (const auto& scope : opts.scopes) {
            for (const auto& workload : opts.workloads) {
                auto run_once = [&](std::size_t workers) {
                    return scope == "task" ? run_task_scope(workload, workers, ws, opts)
                                           : run_network_scope(workload, workers, ws, opts);
                };
                (void)run_once(opts.workers.front());  // warm-up (page cache, allocator), not recorded
                double baseline = 0;
                for (const auto workers : opts.workers) {
                    std::vector<RunResult> runs;
                    for (std::size_t rep = 0; rep < opts.repetitions; ++rep) {
                        runs.push_back(run_once(workers));
                    }
                    Row row = summarize(scope, workload, workers, job_count(workload, opts), runs);
                    if (baseline == 0) {
                        baseline = row.wall_median;
                    }
                    row.speedup = baseline / row.wall_median;
                    std::cout << std::left << std::setw(8) << scope << std::setw(10) << workload
                              << " workers=" << std::setw(3) << workers << " wall=" << std::setw(9)
                              << fixed(row.wall_median, 1) << "ms  thr=" << std::setw(9) << fixed(row.throughput, 1)
                              << "jobs/s  p50=" << fixed(row.p50) << "ms  p99=" << fixed(row.p99)
                              << "ms  cpu=" << fixed(row.cpu_util_pct, 0) << "%  speedup=" << fixed(row.speedup)
                              << "x  failures=" << row.failures << std::endl;
                    rows.push_back(row);
                }
            }
        }
    } catch (const std::exception& e) {
        std::cerr << "benchmark failed: " << e.what() << '\n';
        std::error_code ec;
        fs::remove_all(ws, ec);
        return 1;
    }
    const double total = std::chrono::duration<double>(Clock::now() - bench_start).count();
    write_csv(opts.csv, rows);
    write_markdown(opts.markdown, rows, opts, total);
    std::error_code ec;
    fs::remove_all(ws, ec);
    std::cout << "Wrote " << opts.csv << " and " << opts.markdown << "\n";
    return 0;
}
