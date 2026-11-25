// EventLoop Timer Rolling Benchmark Suite
// The other half of a timer queue's life: timers that do fire. Each
// iteration arms one timer with a fixed horizon, advances the clock by one
// millisecond, and runs everything that came due, so the live count
// settles at about the horizon in milliseconds and every timer is
// eventually popped. Measures insert + expiry together.
//
// Part of the wheel-vs-heap decision (rule in timerChurn.cpp).
//
// Covers:
// - Insert + advance + expire at ~1K, ~10K and ~100K live timers

#include <support/framework.h>

// clang-format off
#include <random>   // std::mt19937_64 — jitter on deadlines
// clang-format on

using namespace EventLoop::detail;

namespace {

using ms = std::chrono::milliseconds;

/// Arms one timer per simulated millisecond and runs whatever is due.
template <typename Queue> class Rolling {
  public:
    Rolling(Queue queue, TimerClock::time_point start, std::int64_t horizonMs)
        : queue_(std::move(queue)), start_(start), horizonMs_(horizonMs), random_(7) {
        // Reach steady state before anything is measured.
        for (std::int64_t i = 0; i < 2 * horizonMs_; ++i) {
            step();
        }
    }

    void step() {
        const std::int64_t jitter =
            static_cast<std::int64_t>(random_() % static_cast<std::uint64_t>(horizonMs_ / 5 + 1));
        const auto now = start_ + ms(nowMs_);
        (void)queue_.insert(now + ms(horizonMs_ + jitter), [this] { ++fired_; }, now);

        ++nowMs_;
        const auto later = start_ + ms(nowMs_);
        const std::uint64_t limit = queue_.sequence();
        while (auto callback = queue_.popExpired(later, limit)) {
            (*callback)();
        }
    }

    [[nodiscard]] std::uint64_t fired() const noexcept {
        return fired_;
    }

  private:
    Queue queue_;
    TimerClock::time_point start_;
    std::int64_t horizonMs_;
    std::int64_t nowMs_ = 0;
    std::uint64_t fired_ = 0;
    std::mt19937_64 random_;
};

} // namespace

// Measures insert + expiry at a given horizon, wheel vs heap.
static void bench_rolling(const char* label, std::int64_t horizonMs) {
    const auto base = TimerClock::now();
    Rolling<TimerWheel> wheel(TimerWheel(base), base, horizonMs);
    Rolling<TimerHeap> heap(TimerHeap(), base, horizonMs);

    auto wheelExpr = [&] { wheel.step(); };
    auto heapExpr = [&] { heap.step(); };

    BENCH(label, wheelExpr, heapExpr);
}

// Executes all rolling benchmark cases.
static void run_benchmarks() {
    bench_rolling("rolling 1K live", 1'000);
    bench_rolling("rolling 10K live", 10'000);
    bench_rolling("rolling 100K live", 100'000);
}

REGISTER_BENCH_SUITE();
