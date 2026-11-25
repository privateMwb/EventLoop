// EventLoop::sleepFor / sleepUntil test suite.
//
// Coverage:
// - A sleeper resumes at its deadline and not before
// - A due deadline does not suspend
// - Destroying a sleeping Task cancels its timer; nothing resumes later
// - Many sleepers wake in deadline order

#include <support/framework.h>

// clang-format off
#include <string> // std::string — wake-order log
// clang-format on

using namespace EventLoop;
using ms = std::chrono::milliseconds;

static Task<void> sleepThenMark(Reactor& reactor, ms duration, int& stage) {
    stage = 1;
    co_await sleepFor(reactor, duration);
    stage = 2;
}

static Task<void> sleepThenLog(Reactor& reactor, ms duration, char tag, std::string& log) {
    co_await sleepFor(reactor, duration);
    log += tag;
}

static Task<int> tickThrice(Reactor& reactor, int& ticks) {
    for (int i = 0; i < 3; ++i) {
        co_await sleepFor(reactor, ms(10));
        ++ticks;
    }
    co_return ticks;
}

// Verifies a sleeper wakes at its deadline and not a moment before.
static void sleeper_wakes_at_deadline() {
    ManualClock clock;
    Reactor reactor(clock.source());
    int stage = 0;
    Task<void> task = sleepThenMark(reactor, ms(50), stage);
    task.start();
    CHK(stage == 1);
    CHK(reactor.timerCount() == 1);

    clock.advance(ms(49));
    reactor.runOnce(0);
    CHK(stage == 1);

    clock.advance(ms(1));
    reactor.runOnce(0);
    CHK(stage == 2);
    CHK(task.done());
    CHK(reactor.timerCount() == 0);
}

// Verifies a zero or negative sleep does not suspend.
static void due_sleep_does_not_suspend() {
    ManualClock clock;
    Reactor reactor(clock.source());
    int stage = 0;

    Task<void> zero = sleepThenMark(reactor, ms(0), stage);
    zero.start();
    CHK(stage == 2);
    CHK(reactor.timerCount() == 0);

    stage = 0;
    Task<void> negative = sleepThenMark(reactor, ms(-30), stage);
    negative.start();
    CHK(stage == 2);
}

// Verifies sleepUntil with a past deadline completes at once.
static void sleep_until_past_is_ready() {
    ManualClock clock;
    Reactor reactor(clock.source());
    bool done = false;
    auto body = [](Reactor& r, Reactor::Clock::time_point when, bool& flag) -> Task<void> {
        co_await sleepUntil(r, when);
        flag = true;
    };
    Task<void> task = body(reactor, clock.now() - ms(1), done);
    task.start();

    CHK(done);
}

// Verifies a destroyed sleeper cancels its timer and is never resumed.
static void destroyed_sleeper_never_wakes() {
    ManualClock clock;
    Reactor reactor(clock.source());
    int stage = 0;
    {
        Task<void> task = sleepThenMark(reactor, ms(50), stage);
        task.start();
        CHK(reactor.timerCount() == 1);
    } // Destroyed while asleep.

    CHK(reactor.timerCount() == 0);
    clock.advance(ms(500));
    CHK(reactor.runOnce(0) == 0);
    CHK(stage == 1); // Never resumed.
}

// Verifies sleepers wake in deadline order.
static void sleepers_wake_in_order() {
    ManualClock clock;
    Reactor reactor(clock.source());
    std::string log;
    Task<void> a = sleepThenLog(reactor, ms(30), 'c', log);
    Task<void> b = sleepThenLog(reactor, ms(10), 'a', log);
    Task<void> c = sleepThenLog(reactor, ms(20), 'b', log);
    a.start();
    b.start();
    c.start();

    clock.advance(ms(100));
    reactor.runOnce(0);

    CHK(log == "abc");
}

// Verifies a loop of sleeps ticks once per interval.
static void sleep_loop_ticks_steadily() {
    ManualClock clock;
    Reactor reactor(clock.source());
    int ticks = 0;
    Task<int> task = tickThrice(reactor, ticks);
    task.start();

    for (int i = 0; i < 3; ++i) {
        CHK(ticks == i);
        clock.advance(ms(10));
        reactor.runOnce(0);
    }

    CHK(ticks == 3);
    CHK(task.done());
    CHK(task.result() == 3);
}

static void run_tests() {
    RUN(sleeper_wakes_at_deadline);
    RUN(due_sleep_does_not_suspend);
    RUN(sleep_until_past_is_ready);
    RUN(destroyed_sleeper_never_wakes);
    RUN(sleepers_wake_in_order);
    RUN(sleep_loop_ticks_steadily);
}

REGISTER_TEST_SUITE();
