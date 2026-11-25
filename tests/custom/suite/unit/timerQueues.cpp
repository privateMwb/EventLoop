// Timer queue test suite: TimerHeap and TimerWheel.
//
// Both queues implement one interface and must behave the same way, so every
// scenario runs against both. A randomized differential test checks them
// against a simple model over a wide span of time (every wheel level,
// cascades, and a deadline beyond the wheel's reach).
//
// Coverage:
// - Ordering, "never early", and one-millisecond resolution
// - Cancellation, stale handles, slot reuse
// - Callbacks that cancel or insert timers while a pass is running
// - Far-future deadlines and large time jumps
// - Randomized agreement with a model

#include <support/framework.h>

// clang-format off
#include <EventLoop/detail/TimerHeap.h>  // TimerHeap — heap-backed queue
#include <candidates/TimerWheel.h>          // TimerWheel — the benchmarked candidate, kept tested

#include <algorithm>                     // std::sort, std::is_sorted — checking results
#include <chrono>                        // std::chrono::milliseconds — time arithmetic
#include <cstdint>                       // std::int64_t, std::uint64_t — model keys
#include <map>                           // std::map — the model
#include <random>                        // std::mt19937_64 — seeded randomness
#include <type_traits>                   // std::is_same_v — choosing a constructor
#include <utility>                       // std::pair — model keys
#include <vector>                        // std::vector — fired tags
// clang-format on

using namespace EventLoop::detail;
using ms = std::chrono::milliseconds;

namespace {

TimerClock::time_point baseTime() {
    static const TimerClock::time_point base = TimerClock::now();
    return base;
}

template <typename Queue> Queue makeQueue() {
    if constexpr (std::is_same_v<Queue, TimerWheel>) {
        return Queue(baseTime());
    } else {
        return Queue();
    }
}

} // namespace

/// Fixture holding the tags fired by timer callbacks.
struct Fired {
    std::vector<int> tags;
    TimerCallback tag(int value) {
        return [this, value] { tags.push_back(value); };
    }
};

/// Runs one pass: every timer due at `now` that existed when it started.
template <typename Queue> void runPass(Queue& queue, TimerClock::time_point now) {
    const std::uint64_t limit = queue.sequence();
    while (auto callback = queue.popExpired(now, limit)) {
        (*callback)();
    }
}

template <typename Queue> void firesInOrder() {
    Queue queue = makeQueue<Queue>();
    Fired fired;
    const auto base = baseTime();
    for (int value : {30, 10, 50, 20, 40}) {
        (void)queue.insert(base + ms(value), fired.tag(value), base);
    }

    runPass(queue, base + ms(100));

    CHK((fired.tags == std::vector<int>{10, 20, 30, 40, 50}));
    CHK(queue.size() == 0);
}

template <typename Queue> void notDueStaysPending() {
    Queue queue = makeQueue<Queue>();
    Fired fired;
    const auto base = baseTime();
    (void)queue.insert(base + ms(50), fired.tag(1), base);

    runPass(queue, base + ms(49));
    CHK(fired.tags.empty());
    CHK(queue.size() == 1);

    runPass(queue, base + ms(50));
    CHK(fired.tags.size() == 1);
}

template <typename Queue> void neverFiresEarly() {
    Queue queue = makeQueue<Queue>();
    Fired fired;
    const auto base = baseTime();
    const auto deadline = base + std::chrono::microseconds(10500); // 10.5 ms

    (void)queue.insert(deadline, fired.tag(1), base);

    runPass(queue, base + ms(10)); // Before the deadline.
    CHK(fired.tags.empty());

    runPass(queue, base + ms(11)); // After it: fires (at most one tick late).
    CHK(fired.tags.size() == 1);
}

template <typename Queue> void pastDeadlineIsDue() {
    Queue queue = makeQueue<Queue>();
    Fired fired;
    const auto base = baseTime();

    (void)queue.insert(base - ms(500), fired.tag(1), base + ms(10));

    runPass(queue, base + ms(10));
    CHK(fired.tags.size() == 1);
}

template <typename Queue> void cancelPreventsFiring() {
    Queue queue = makeQueue<Queue>();
    Fired fired;
    const auto base = baseTime();
    const TimerId keep = queue.insert(base + ms(10), fired.tag(1), base);
    const TimerId drop = queue.insert(base + ms(10), fired.tag(2), base);
    (void)keep;

    CHK(queue.cancel(drop));
    CHK(!queue.cancel(drop)); // Already cancelled.
    CHK(queue.size() == 1);

    runPass(queue, base + ms(20));
    CHK((fired.tags == std::vector<int>{1}));
}

template <typename Queue> void staleHandleIsRejected() {
    Queue queue = makeQueue<Queue>();
    Fired fired;
    const auto base = baseTime();
    const TimerId first = queue.insert(base + ms(5), fired.tag(1), base);

    runPass(queue, base + ms(10)); // Fires; its slot is released.
    CHK(!queue.cancel(first));

    // The slot is reused by a new timer; the old handle must not touch it.
    const TimerId second = queue.insert(base + ms(500), fired.tag(2), base + ms(10));
    CHK(!queue.cancel(first));
    CHK(queue.size() == 1);
    CHK(queue.cancel(second));
    CHK(!queue.cancel(TimerId{}));
}

template <typename Queue> void cancelDuringPass() {
    Queue queue = makeQueue<Queue>();
    Fired fired;
    const auto base = baseTime();
    TimerId later;
    (void)queue.insert(
        base + ms(10),
        [&] {
            queue.cancel(later);
            fired.tags.push_back(1);
        },
        base);
    later = queue.insert(base + ms(20), fired.tag(2), base);

    runPass(queue, base + ms(100)); // Both are due; the first cancels the second.

    CHK((fired.tags == std::vector<int>{1}));
    CHK(queue.size() == 0);
}

template <typename Queue> void insertDuringPassWaits() {
    Queue queue = makeQueue<Queue>();
    Fired fired;
    const auto base = baseTime();
    const auto now = base + ms(100);
    (void)queue.insert(
        base + ms(10),
        [&] {
            fired.tags.push_back(1);
            (void)queue.insert(base + ms(20), fired.tag(2), now); // Already due.
        },
        base);

    runPass(queue, now);
    CHK((fired.tags == std::vector<int>{1})); // Not run in the same pass.

    runPass(queue, now);
    CHK((fired.tags == std::vector<int>{1, 2}));
}

template <typename Queue> void farFutureDeadline() {
    Queue queue = makeQueue<Queue>();
    Fired fired;
    const auto base = baseTime();
    constexpr std::int64_t year = std::int64_t{365} * 24 * 3600 * 1000;
    (void)queue.insert(base + ms(3 * year), fired.tag(1), base); // Past the wheel's reach.

    runPass(queue, base + ms(2 * year));
    CHK(fired.tags.empty());

    runPass(queue, base + ms(3 * year - 1));
    CHK(fired.tags.empty());

    runPass(queue, base + ms(3 * year));
    CHK(fired.tags.size() == 1);
}

template <typename Queue> void largeJumpFiresAll() {
    Queue queue = makeQueue<Queue>();
    Fired fired;
    const auto base = baseTime();
    std::mt19937_64 random(7);
    for (int i = 0; i < 1000; ++i) {
        (void)queue.insert(base + ms(1 + static_cast<std::int64_t>(random() % 600000)),
                           fired.tag(i), base);
    }

    runPass(queue, base + ms(3600 * 1000)); // One hour in a single step.

    CHK(fired.tags.size() == 1000);
    CHK(queue.size() == 0);
}

template <typename Queue> void nextDeadlineIsBound() {
    Queue queue = makeQueue<Queue>();
    Fired fired;
    const auto base = baseTime();
    CHK(!queue.nextDeadline().has_value());

    (void)queue.insert(base + ms(5000), fired.tag(1), base);
    (void)queue.insert(base + ms(900), fired.tag(2), base);

    const auto next = queue.nextDeadline();
    CHK(next.has_value());
    CHK(*next <= base + ms(900)); // Never later than the real earliest deadline...
    CHK(*next > base);            // ...and not in the past while nothing is due.
}

/// Seeded random operations checked against a model: the set of timers that
/// fire at each step must equal the set the model says are due.
template <typename Queue> void matchesModel() {
    for (std::uint64_t seed = 1; seed <= 6; ++seed) {
        std::mt19937_64 random(seed);
        Queue queue = makeQueue<Queue>();
        const auto base = baseTime();

        std::map<std::pair<std::int64_t, int>, TimerId> pending; // (deadline ms, tag) -> id
        std::map<int, std::int64_t> deadlineOf;                  // tag -> deadline ms
        std::int64_t nowMs = 0;
        int nextTag = 0;
        bool ok = true;

        for (int step = 0; step < 6000 && ok; ++step) {
            const auto op = random() % 10;
            if (op < 5) { // Insert.
                const int scale = static_cast<int>(random() % 5);
                std::int64_t span = 0;
                switch (scale) {
                case 0:
                    span = 64;
                    break; // Level 0.
                case 1:
                    span = 4096;
                    break; // Level 1.
                case 2:
                    span = 262144;
                    break; // Level 2.
                case 3:
                    span = 16777216;
                    break; // Level 3.
                default:
                    span = std::int64_t{1} << 38;
                    break; // Beyond the top level.
                }
                const std::int64_t deadline =
                    nowMs + static_cast<std::int64_t>(random() % static_cast<std::uint64_t>(span));
                const int tag = nextTag++;
                const TimerId id = queue.insert(
                    base + ms(deadline),
                    [&, tag] {
                        pending.erase({deadlineOf[tag], tag});
                        deadlineOf.erase(tag);
                    },
                    base + ms(nowMs));
                pending[{deadline, tag}] = id;
                deadlineOf[tag] = deadline;
            } else if (op < 7 && !pending.empty()) { // Cancel a random pending timer.
                auto it = pending.begin();
                std::advance(it, static_cast<std::ptrdiff_t>(random() % pending.size()));
                const int tag = it->first.second;
                if (!queue.cancel(it->second)) {
                    ok = false;
                }
                deadlineOf.erase(tag);
                pending.erase(it);
            } else { // Advance time and run a pass.
                const std::int64_t jumps[] = {1, 3, 70, 5000, 300000, 20000000};
                nowMs += static_cast<std::int64_t>(
                             random() % static_cast<std::uint64_t>(jumps[random() % 6])) +
                         1;

                // Everything the model says is due, and nothing else, must fire.
                std::vector<int> expected;
                for (const auto& [key, id] : pending) {
                    if (key.first <= nowMs) {
                        expected.push_back(key.second);
                    }
                }
                std::size_t before = pending.size();
                runPass(queue, base + ms(nowMs));
                std::size_t fired = before - pending.size();
                if (fired != expected.size()) {
                    ok = false;
                }
                for (int tag : expected) {
                    if (deadlineOf.count(tag) != 0) {
                        ok = false; // It was due but did not fire.
                    }
                }
            }
            if (queue.size() != pending.size()) {
                ok = false;
            }
        }
        CHK(ok);
    }
}

#define QUEUE_SCENARIO(name)                                                                       \
    static void heap_##name() {                                                                    \
        name<TimerHeap>();                                                                         \
    }                                                                                              \
    static void wheel_##name() {                                                                   \
        name<TimerWheel>();                                                                        \
    }

QUEUE_SCENARIO(firesInOrder)
QUEUE_SCENARIO(notDueStaysPending)
QUEUE_SCENARIO(neverFiresEarly)
QUEUE_SCENARIO(pastDeadlineIsDue)
QUEUE_SCENARIO(cancelPreventsFiring)
QUEUE_SCENARIO(staleHandleIsRejected)
QUEUE_SCENARIO(cancelDuringPass)
QUEUE_SCENARIO(insertDuringPassWaits)
QUEUE_SCENARIO(farFutureDeadline)
QUEUE_SCENARIO(largeJumpFiresAll)
QUEUE_SCENARIO(nextDeadlineIsBound)
QUEUE_SCENARIO(matchesModel)

static void run_tests() {
    RUN(heap_firesInOrder);
    RUN(wheel_firesInOrder);
    RUN(heap_notDueStaysPending);
    RUN(wheel_notDueStaysPending);
    RUN(heap_neverFiresEarly);
    RUN(wheel_neverFiresEarly);
    RUN(heap_pastDeadlineIsDue);
    RUN(wheel_pastDeadlineIsDue);
    RUN(heap_cancelPreventsFiring);
    RUN(wheel_cancelPreventsFiring);
    RUN(heap_staleHandleIsRejected);
    RUN(wheel_staleHandleIsRejected);
    RUN(heap_cancelDuringPass);
    RUN(wheel_cancelDuringPass);
    RUN(heap_insertDuringPassWaits);
    RUN(wheel_insertDuringPassWaits);
    RUN(heap_farFutureDeadline);
    RUN(wheel_farFutureDeadline);
    RUN(heap_largeJumpFiresAll);
    RUN(wheel_largeJumpFiresAll);
    RUN(heap_nextDeadlineIsBound);
    RUN(wheel_nextDeadlineIsBound);
    RUN(heap_matchesModel);
    RUN(wheel_matchesModel);
}

REGISTER_TEST_SUITE();
