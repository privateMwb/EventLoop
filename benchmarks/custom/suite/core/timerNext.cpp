// EventLoop Timer Next-Deadline Benchmark Suite
// The reactor asks the timer queue for its next deadline before every wait,
// so this call is on the hot path of every loop iteration. The heap answers
// from its root; the wheel scans occupancy bitmasks. Checks the wheel's
// answer stays cheap enough not to matter.
//
// Part of the wheel-vs-heap decision (rule in timerChurn.cpp).
//
// Covers:
// - nextDeadline() with 10K and 100K timers pending

#include <support/framework.h>

// clang-format off
#include <cstdint>  // std::uint64_t — sink for results
#include <random>   // std::mt19937_64 — deadlines
// clang-format on

using namespace EventLoop::detail;

namespace {

using ms = std::chrono::milliseconds;

volatile std::int64_t gSink = 0; ///< Keeps results alive so the call is not optimised away.

/// Fills a queue with `count` timers up to 30 s out.
template <typename Queue> void fill(Queue& queue, TimerClock::time_point now, std::size_t count) {
    std::mt19937_64 random(3);
    for (std::size_t i = 0; i < count; ++i) {
        (void)queue.insert(
            now + ms(1000 + static_cast<std::int64_t>(random() % 29000)), [] {}, now);
    }
}

} // namespace

// Measures nextDeadline() with a given number of pending timers.
static void bench_next(const char* label, std::size_t count) {
    const auto base = TimerClock::now();
    TimerWheel wheel(base);
    TimerHeap heap;
    fill(wheel, base, count);
    fill(heap, base, count);

    auto wheelExpr = [&] { gSink = wheel.nextDeadline()->time_since_epoch().count(); };
    auto heapExpr = [&] { gSink = heap.nextDeadline()->time_since_epoch().count(); };

    BENCH(label, wheelExpr, heapExpr);
}

// Executes all next-deadline benchmark cases.
static void run_benchmarks() {
    bench_next("next 10K pending", 10'000);
    bench_next("next 100K pending", 100'000);
}

REGISTER_BENCH_SUITE();
