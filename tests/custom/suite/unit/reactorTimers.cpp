// EventLoop::Reactor timer test suite.
//
// Coverage:
// - at/after/cancelTimer with a controllable clock
// - Timers run after I/O in the same iteration
// - Ordering, never early, handle safety
// - Waits are capped by the next timer
// - A timer that arms a timer, a throwing timer, and the re-entrancy guard

#include <support/framework.h>

// clang-format off
#include <chrono>    // std::chrono::steady_clock, milliseconds — real-time checks
#include <stdexcept> // std::invalid_argument, std::logic_error, std::runtime_error — expected failures
#include <string>    // std::string — ordering log
// clang-format on

using namespace EventLoop;
using ms = std::chrono::milliseconds;

// Verifies a timer fires once its time has come, and not before.
static void timer_fires_when_due() {
    ManualClock clock;
    Reactor reactor(clock.source());
    int fired = 0;
    reactor.after(ms(10), [&] { ++fired; });

    CHK(reactor.runOnce(0) == 0);
    clock.advance(ms(9));
    CHK(reactor.runOnce(0) == 0);
    CHK(fired == 0);

    clock.advance(ms(1));
    CHK(reactor.runOnce(0) == 1);
    CHK(fired == 1);
    CHK(reactor.runOnce(0) == 0); // One-shot.
}

// Verifies timers fire in deadline order, whatever the insertion order.
static void timers_fire_in_order() {
    ManualClock clock;
    Reactor reactor(clock.source());
    std::string order;
    reactor.after(ms(30), [&] { order += 'c'; });
    reactor.after(ms(10), [&] { order += 'a'; });
    reactor.after(ms(20), [&] { order += 'b'; });

    clock.advance(ms(100));
    reactor.runOnce(0);

    CHK(order == "abc");
}

// Verifies timers run after the I/O events of the same iteration.
static void timers_run_after_events() {
    ManualClock clock;
    Reactor reactor(clock.source());
    Pipe pipe = makePipe();
    std::string order;
    reactor.add(pipe.readEnd.get(), Event::Readable, [&](int fd, Event) {
        order += 'E';
        drain(fd);
    });
    reactor.after(ms(5), [&] { order += 'T'; });
    clock.advance(ms(5));
    writeByte(pipe.writeEnd.get());

    CHK(reactor.runOnce(1000) == 2);
    CHK(order == "ET");
}

// Verifies a cancelled timer never fires and its handle is then inert.
static void cancel_timer_prevents_fire() {
    ManualClock clock;
    Reactor reactor(clock.source());
    int fired = 0;
    const auto keep = reactor.after(ms(10), [&] { fired += 1; });
    const auto drop = reactor.after(ms(10), [&] { fired += 100; });
    (void)keep;

    CHK(reactor.timerCount() == 2);
    CHK(reactor.cancelTimer(drop));
    CHK(!reactor.cancelTimer(drop));
    CHK(reactor.timerCount() == 1);

    clock.advance(ms(10));
    reactor.runOnce(0);

    CHK(fired == 1);
    CHK(reactor.timerCount() == 0);
}

// Verifies a handle for a timer that already fired is harmless to cancel.
static void stale_handle_is_harmless() {
    ManualClock clock;
    Reactor reactor(clock.source());
    const auto first = reactor.after(ms(1), [] {});
    clock.advance(ms(1));
    reactor.runOnce(0);

    const auto second = reactor.after(ms(50), [] {}); // May reuse the slot.

    CHK(!reactor.cancelTimer(first));
    CHK(reactor.timerCount() == 1);
    CHK(reactor.cancelTimer(second));
}

// Verifies a zero delay runs on the next iteration.
static void zero_delay_runs_next() {
    ManualClock clock;
    Reactor reactor(clock.source());
    int fired = 0;
    reactor.after(ms(0), [&] { ++fired; });
    reactor.after(ms(-5), [&] { ++fired; });

    CHK(reactor.runOnce(0) == 2);
    CHK(fired == 2);
}

// Verifies a timer armed by a timer waits for the next iteration.
static void timer_arming_timer_waits() {
    ManualClock clock;
    Reactor reactor(clock.source());
    int first = 0;
    int second = 0;
    reactor.after(ms(0), [&] {
        ++first;
        reactor.after(ms(0), [&] { ++second; });
    });

    CHK(reactor.runOnce(0) == 1);
    CHK(first == 1 && second == 0);

    CHK(reactor.runOnce(0) == 1);
    CHK(second == 1);
}

// Verifies a timer can cancel another that is due in the same iteration.
static void timer_cancels_due_timer() {
    ManualClock clock;
    Reactor reactor(clock.source());
    int fired = 0;
    Reactor::TimerId victim;
    reactor.after(ms(1), [&] {
        ++fired;
        reactor.cancelTimer(victim);
    });
    victim = reactor.after(ms(2), [&] { fired += 100; });

    clock.advance(ms(10));
    reactor.runOnce(0);

    CHK(fired == 1);
}

// Verifies a throwing timer leaves the remaining due timers queued.
static void throwing_timer_keeps_rest() {
    ManualClock clock;
    Reactor reactor(clock.source());
    int ran = 0;
    reactor.after(ms(1), [] { throw std::runtime_error("boom"); });
    reactor.after(ms(2), [&] { ++ran; });
    clock.advance(ms(10));

    CHK_THROWS(reactor.runOnce(0), std::runtime_error);
    CHK(ran == 0);

    CHK(reactor.runOnce(0) == 1);
    CHK(ran == 1);
}

// Verifies an empty callback is rejected.
static void empty_timer_is_rejected() {
    Reactor reactor;
    CHK_THROWS(reactor.after(ms(1), MoveOnlyFunction<void()>{}), std::invalid_argument);
}

// Verifies calling runOnce from inside a callback is refused, and harmless.
static void reentrant_run_is_refused() {
    ManualClock clock;
    Reactor reactor(clock.source());
    bool refused = false;
    reactor.after(ms(0), [&] {
        try {
            reactor.runOnce(0);
        } catch (const std::logic_error&) {
            refused = true;
        }
    });

    reactor.runOnce(0);

    CHK(refused);
    CHK(reactor.runOnce(0) == 0); // The reactor is still usable.
}

// Verifies an unbounded wait ends when the next timer is due.
static void wait_is_capped_by_timer() {
    Reactor reactor;
    int fired = 0;
    const auto start = std::chrono::steady_clock::now();
    reactor.after(ms(30), [&] { ++fired; });

    reactor.runOnce(-1); // Would block forever without the timer.

    const auto elapsed = std::chrono::steady_clock::now() - start;
    CHK(fired == 1);
    CHK(elapsed >= ms(30));
    CHK(elapsed < ms(2000));
}

// Verifies a real-clock timer never fires before its deadline.
static void real_timer_never_early() {
    Reactor reactor;
    const auto deadline = reactor.now() + ms(15);
    Reactor::Clock::time_point firedAt{};
    reactor.at(deadline, [&] { firedAt = Reactor::Clock::now(); });

    while (reactor.timerCount() > 0) {
        reactor.runOnce(1000);
    }

    CHK(firedAt >= deadline);
}

// Verifies run() ends when a timer calls stop().
static void timer_stops_run_loop() {
    Reactor reactor;
    int ticks = 0;
    reactor.after(ms(10), [&] {
        ++ticks;
        reactor.stop();
    });

    reactor.run();

    CHK(ticks == 1);
}

static void run_tests() {
    RUN(timer_fires_when_due);
    RUN(timers_fire_in_order);
    RUN(timers_run_after_events);
    RUN(cancel_timer_prevents_fire);
    RUN(stale_handle_is_harmless);
    RUN(zero_delay_runs_next);
    RUN(timer_arming_timer_waits);
    RUN(timer_cancels_due_timer);
    RUN(throwing_timer_keeps_rest);
    RUN(empty_timer_is_rejected);
    RUN(reentrant_run_is_refused);
    RUN(wait_is_capped_by_timer);
    RUN(real_timer_never_early);
    RUN(timer_stops_run_loop);
}

REGISTER_TEST_SUITE();
