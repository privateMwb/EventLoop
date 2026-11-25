// EventLoop Timer Churn Benchmark Suite
// Compares the timing wheel (TimerWheel) against the binary heap
// (TimerHeap) on the workload a server actually generates: timeouts that
// are armed and then almost always cancelled before they fire (a read
// timeout that is cancelled when the data arrives).
//
// DECISION RULE (fixed before the first run, so the result cannot be
// argued into the answer afterwards): the wheel replaces the heap only if
//   (a) churn at 100K live timers is at least 2.0x faster than the heap, AND
//   (b) no churn or rolling case at 1K live timers is more than 1.2x slower.
// Otherwise the heap stays: it is simpler, and complexity has to be paid
// for by a clear win. The outcome is recorded in docs/Timers.md.
//
// Each iteration arms one new timer (30 s horizon, random) and cancels one
// random live timer, holding the live count steady. Time is faked, so
// nothing fires and the cost measured is insert + cancel only.
//
// Covers:
// - Insert + cancel at 1K, 10K and 100K live timers

#include <support/framework.h>

// clang-format off
#include <random>   // std::mt19937_64 — deterministic deadlines and victims
#include <vector>   // std::vector — live handles
// clang-format on

using namespace EventLoop::detail;

namespace {

using ms = std::chrono::milliseconds;

/// Holds `live` pending timers and swaps one per step.
template <typename Queue> class Churn {
  public:
    Churn(Queue queue, TimerClock::time_point now, std::size_t live)
        : queue_(std::move(queue)), now_(now), random_(42) {
        ids_.reserve(live);
        for (std::size_t i = 0; i < live; ++i) {
            ids_.push_back(arm());
        }
    }

    /// Arms one timer and cancels one random live one.
    void step() {
        const std::size_t victim = random_() % ids_.size();
        (void)queue_.cancel(ids_[victim]);
        ids_[victim] = arm();
    }

  private:
    TimerId arm() {
        const auto deadline = now_ + ms(1000 + static_cast<std::int64_t>(random_() % 29000));
        return queue_.insert(deadline, [] {}, now_);
    }

    Queue queue_;
    TimerClock::time_point now_;
    std::mt19937_64 random_;
    std::vector<TimerId> ids_;
};

} // namespace

// Measures insert + cancel at a given live-timer count, wheel vs heap.
static void bench_churn(const char* label, std::size_t live) {
    const auto base = TimerClock::now();
    Churn<TimerWheel> wheel(TimerWheel(base), base, live);
    Churn<TimerHeap> heap(TimerHeap(), base, live);

    auto wheelExpr = [&] { wheel.step(); };
    auto heapExpr = [&] { heap.step(); };

    BENCH(label, wheelExpr, heapExpr);
}

// Executes all churn benchmark cases.
static void run_benchmarks() {
    bench_churn("churn 1K live", 1'000);
    bench_churn("churn 10K live", 10'000);
    bench_churn("churn 100K live", 100'000);
}

REGISTER_BENCH_SUITE();
