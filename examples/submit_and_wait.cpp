// Minimal use of the ConcurrentCore client library: connect, submit a job
// with notify=true, wait for the JOB_FINISHED event and print the result.
//
//   concurrentcore-example-client [port] [limit]

#include "concurrentcore/client/client.hpp"

#include <charconv>
#include <iostream>
#include <string_view>

namespace {
std::uint64_t arg_or(int argc, char** argv, int index, std::uint64_t fallback) {
    if (argc <= index) {
        return fallback;
    }
    std::uint64_t v = fallback;
    const std::string_view s(argv[index]);
    std::from_chars(s.data(), s.data() + s.size(), v);
    return v;
}
}  // namespace

int main(int argc, char** argv) {
    const auto port = static_cast<std::uint16_t>(arg_or(argc, argv, 1, 7878));
    const auto limit = arg_or(argc, argv, 2, 1'000'000);
    try {
        concurrentcore::Client client;
        client.connect("127.0.0.1", port);

        const auto submitted = client.submit("compute", {{"limit", limit}}, /*notify=*/true);
        if (!submitted.value("ok", false)) {
            std::cerr << "rejected: " << submitted["error"].dump() << '\n';
            return 1;
        }
        std::cout << "submitted job " << submitted["result"]["job_id"] << '\n';

        const auto event = client.wait_for_event(std::chrono::seconds(60));
        if (!event) {
            std::cerr << "timed out\n";
            return 1;
        }
        const auto& job = (*event)["data"];
        std::cout << "status: " << job["status"] << ", primes below " << limit << ": "
                  << job["result"]["prime_count"] << " (" << job["execution_ms"] << " ms)\n";
        return 0;
    } catch (const concurrentcore::ClientError& e) {
        std::cerr << "client error: " << e.what() << '\n';
        return 2;
    }
}
