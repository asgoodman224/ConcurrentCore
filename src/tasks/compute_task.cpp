#include "concurrentcore/tasks/tasks.hpp"

namespace concurrentcore {
namespace {

bool is_prime(std::uint64_t n) noexcept {
    if (n < 2) {
        return false;
    }
    if (n < 4) {
        return true;  // 2, 3
    }
    if (n % 2 == 0 || n % 3 == 0) {
        return false;
    }
    // Every prime > 3 has the form 6k +/- 1.
    for (std::uint64_t d = 5; d * d <= n; d += 6) {
        if (n % d == 0 || n % (d + 2) == 0) {
            return false;
        }
    }
    return true;
}

}  // namespace

PrimeCountResult count_primes_below(std::uint64_t limit, std::stop_token stop) {
    PrimeCountResult r;
    constexpr std::uint64_t kCheckInterval = 4096;
    for (std::uint64_t n = 2; n < limit; ++n) {
        if (n % kCheckInterval == 0 && stop.stop_requested()) {
            throw OperationCancelled();
        }
        if (is_prime(n)) {
            ++r.count;
            r.largest = n;
        }
    }
    return r;
}

nlohmann::json run_compute(const ComputeParams& p, const TaskContext& ctx) {
    ctx.throw_if_cancelled();
    const PrimeCountResult r = count_primes_below(p.limit, ctx.stop);
    return {
        {"algorithm", "prime-count/trial-division"},
        {"limit", p.limit},
        {"prime_count", r.count},
        {"largest_prime", r.largest == 0 ? nlohmann::json(nullptr) : nlohmann::json(r.largest)},
    };
}

}  // namespace concurrentcore
