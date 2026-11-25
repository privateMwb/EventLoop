// EventLoop::withTimeout test suite.
//
// Coverage:
// - Success, timeout, and exceptions
// - A timed-out task is cancelled: locals destroyed, never resumed, its I/O
//   deregistered
// - Tie-breaking and the zero limit
// - Destroying the waiting parent cancels the timer and the child
// - Nesting

#include <support/framework.h>

// clang-format off
#include <common/echoHarness.h> // echo::pumpUntil — test driving helper

#include <array>                // std::array — read buffer
#include <cstddef>              // std::byte — buffer element
#include <memory>               // std::unique_ptr — move-only results
#include <span>                 // std::span — buffer views
#include <stdexcept>            // std::logic_error, std::runtime_error — expected failures
#include <string_view>          // std::string_view — short literals
#include <sys/socket.h>         // socketpair — connected local pair
#include <system_error>         // std::errc — checking the error code
#include <utility>              // std::exchange — Tracker move
// clang-format on

using namespace EventLoop;
using ms = std::chrono::milliseconds;

/// Counts live Trackers that were destroyed.
struct Tracker {
    explicit Tracker(int& counter) noexcept : counter_(&counter) {}
    Tracker(Tracker&& other) noexcept : counter_(std::exchange(other.counter_, nullptr)) {}
    Tracker(const Tracker&) = delete;
    Tracker& operator=(const Tracker&) = delete;
    ~Tracker() {
        if (counter_ != nullptr) {
            ++*counter_;
        }
    }

  private:
    int* counter_;
};

static Task<int> sleepThenReturn(Reactor& reactor, ms duration, int value) {
    co_await sleepFor(reactor, duration);
    co_return value;
}

static Task<int> sleepWithLocal(Reactor& reactor, ms duration, int& destroyed, bool& resumed) {
    Tracker local(destroyed);
    co_await sleepFor(reactor, duration);
    resumed = true;
    co_return 1;
}

static Task<void> sleepThenVoid(Reactor& reactor, ms duration) {
    co_await sleepFor(reactor, duration);
}

static Task<int> sleepThenThrow(Reactor& reactor, ms duration) {
    co_await sleepFor(reactor, duration);
    throw std::runtime_error("child failed");
    co_return 0;
}

static Task<int> immediate(int value) {
    co_return value;
}

static Task<std::unique_ptr<int>> moveOnlyAfter(Reactor& reactor, ms duration) {
    co_await sleepFor(reactor, duration);
    co_return std::make_unique<int>(5);
}

/// Outcome of a timed wait, as plain data.
enum class Outcome { Value, TimedOut, Failed };

static Task<Outcome> timedValue(Reactor& reactor, Task<int> inner, ms limit, int& value) {
    try {
        value = co_await withTimeout(reactor, std::move(inner), limit);
        co_return Outcome::Value;
    } catch (const TimeoutError&) {
        co_return Outcome::TimedOut;
    } catch (...) {
        co_return Outcome::Failed;
    }
}

static Task<std::size_t> readOnce(Socket& socket, std::span<std::byte> buffer) {
    co_return co_await socket.read(buffer);
}

static Task<Outcome> timedRead(Reactor& reactor, Socket& socket, std::span<std::byte> buffer,
                               ms limit) {
    try {
        (void)co_await withTimeout(reactor, readOnce(socket, buffer), limit);
        co_return Outcome::Value;
    } catch (const TimeoutError&) {
        co_return Outcome::TimedOut;
    }
}

// Verifies a task that finishes in time returns its value and leaves no timer.
static void finishes_before_limit() {
    ManualClock clock;
    Reactor reactor(clock.source());
    int value = 0;
    Task<Outcome> task = timedValue(reactor, sleepThenReturn(reactor, ms(10), 7), ms(50), value);
    task.start();
    CHK(reactor.timerCount() == 2); // The child's sleep and the limit.

    clock.advance(ms(10));
    reactor.runOnce(0);

    CHK(task.done());
    CHK(task.result() == Outcome::Value);
    CHK(value == 7);
    CHK(reactor.timerCount() == 0); // The limit was cancelled.
}

// Verifies the limit passing first cancels the task and throws TimeoutError.
static void limit_cancels_task() {
    ManualClock clock;
    Reactor reactor(clock.source());
    int destroyed = 0;
    bool resumed = false;
    int value = 0;
    Task<Outcome> task =
        timedValue(reactor, sleepWithLocal(reactor, ms(100), destroyed, resumed), ms(50), value);
    task.start();
    CHK(destroyed == 0);

    clock.advance(ms(50));
    reactor.runOnce(0);

    CHK(task.done());
    CHK(task.result() == Outcome::TimedOut);
    CHK(destroyed == 1);            // The child's locals were destroyed.
    CHK(reactor.timerCount() == 0); // Its sleep timer went with it.

    clock.advance(ms(500));
    reactor.runOnce(0);
    CHK(!resumed); // The cancelled child never ran again.
}

// Verifies TimeoutError carries the timed_out error code.
static void timeout_error_has_code() {
    const TimeoutError error;
    CHK(error.code() == std::errc::timed_out);
}

// Verifies the child's exception reaches the caller and the timer is cancelled.
static void child_exception_propagates() {
    ManualClock clock;
    Reactor reactor(clock.source());
    int value = 0;
    Task<Outcome> task = timedValue(reactor, sleepThenThrow(reactor, ms(10)), ms(50), value);
    task.start();

    clock.advance(ms(10));
    reactor.runOnce(0);

    CHK(task.done());
    CHK(task.result() == Outcome::Failed);
    CHK(reactor.timerCount() == 0);
}

// Verifies a task that never needs to wait succeeds even with no time allowed.
static void immediate_child_beats_zero_limit() {
    ManualClock clock;
    Reactor reactor(clock.source());
    int value = 0;
    Task<Outcome> task = timedValue(reactor, immediate(9), ms(0), value);
    task.start();

    CHK(task.done());
    CHK(task.result() == Outcome::Value);
    CHK(value == 9);
    CHK(reactor.timerCount() == 0);
}

// Verifies a task that has to wait times out under a zero limit.
static void waiting_child_misses_zero_limit() {
    ManualClock clock;
    Reactor reactor(clock.source());
    int value = 0;
    Task<Outcome> task = timedValue(reactor, sleepThenReturn(reactor, ms(10), 1), ms(0), value);
    task.start();

    reactor.runOnce(0); // The limit is already due.

    CHK(task.done());
    CHK(task.result() == Outcome::TimedOut);
}

// Verifies that on an exact tie the task wins.
static void tie_goes_to_task() {
    ManualClock clock;
    Reactor reactor(clock.source());
    int value = 0;
    Task<Outcome> task = timedValue(reactor, sleepThenReturn(reactor, ms(50), 3), ms(50), value);
    task.start();

    clock.advance(ms(50)); // Both timers are due in the same pass.
    reactor.runOnce(0);

    CHK(task.result() == Outcome::Value);
    CHK(value == 3);
}

// Verifies a void task and a move-only result both work.
static void void_and_move_only_results() {
    ManualClock clock;
    Reactor reactor(clock.source());
    auto voidBody = [](Reactor& r) -> Task<void> {
        co_await withTimeout(r, sleepThenVoid(r, ms(5)), ms(50));
    };
    auto ownedBody = [](Reactor& r, int& out) -> Task<void> {
        auto result = co_await withTimeout(r, moveOnlyAfter(r, ms(5)), ms(50));
        out = *result;
    };
    int out = 0;
    Task<void> a = voidBody(reactor);
    Task<void> b = ownedBody(reactor, out);
    a.start();
    b.start();

    clock.advance(ms(5));
    reactor.runOnce(0);

    CHK(a.done() && b.done());
    CHK(out == 5);
}

// Verifies destroying the waiting parent cancels the timer and the child.
static void destroyed_parent_cancels_all() {
    ManualClock clock;
    Reactor reactor(clock.source());
    int destroyed = 0;
    bool resumed = false;
    int value = 0;
    {
        Task<Outcome> task = timedValue(
            reactor, sleepWithLocal(reactor, ms(100), destroyed, resumed), ms(50), value);
        task.start();
        CHK(reactor.timerCount() == 2);
    } // Parent destroyed while both are pending.

    CHK(destroyed == 1);
    CHK(reactor.timerCount() == 0);
    clock.advance(ms(500));
    CHK(reactor.runOnce(0) == 0);
    CHK(!resumed);
}

// Verifies an empty Task is rejected.
static void empty_task_is_rejected() {
    ManualClock clock;
    Reactor reactor(clock.source());
    int value = 0;
    Task<Outcome> task = timedValue(reactor, Task<int>{}, ms(10), value);
    task.start();

    CHK(task.done());
    CHK(task.result() == Outcome::Failed); // logic_error, caught as "failed".
}

static Task<Outcome> innerTimesOut(Reactor& reactor, int& value) {
    // The inner limit (30 ms) passes before the sleep (50 ms) ends.
    co_return co_await timedValue(reactor, sleepThenReturn(reactor, ms(50), 1), ms(30), value);
}

// Verifies nested timeouts: the inner one fires, the outer still succeeds.
static void nested_timeouts() {
    ManualClock clock;
    Reactor reactor(clock.source());
    auto outer = [](Reactor& r, int& value) -> Task<Outcome> {
        co_return co_await withTimeout(r, innerTimesOut(r, value), ms(100));
    };
    int value = 0;
    Task<Outcome> task = outer(reactor, value);
    task.start();
    CHK(reactor.timerCount() == 3); // The sleep, the inner limit and the outer limit.

    clock.advance(ms(30));
    reactor.runOnce(0);

    CHK(task.done());
    CHK(task.result() == Outcome::TimedOut); // The inner outcome, returned by the outer.
    CHK(reactor.timerCount() == 0);
}

// Verifies timing out a socket read deregisters it and leaves the socket usable.
static void timed_out_read_deregisters() {
    ManualClock clock;
    Reactor reactor(clock.source());
    int fds[2];
    CHK(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, fds) == 0);
    Socket reader = Socket::adopt(reactor, UniqueFd(fds[0]));
    Socket writer = Socket::adopt(reactor, UniqueFd(fds[1]));
    std::array<std::byte, 16> buffer;

    Task<Outcome> task = timedRead(reactor, reader, buffer, ms(40));
    task.start();
    CHK(!task.done());

    clock.advance(ms(40));
    reactor.runOnce(0);

    CHK(task.done());
    CHK(task.result() == Outcome::TimedOut);
    CHK(reactor.timerCount() == 0);

    // The read's slot is free again: the next read works.
    const std::string_view text = "late";
    syncWait([](Socket& w, std::string_view t) -> Task<void> {
        co_await w.writeAll(std::as_bytes(std::span(t)));
    }(writer, text));
    CHK(syncWait(readOnce(reader, buffer)) == 4);
}

static void run_tests() {
    RUN(finishes_before_limit);
    RUN(limit_cancels_task);
    RUN(timeout_error_has_code);
    RUN(child_exception_propagates);
    RUN(immediate_child_beats_zero_limit);
    RUN(waiting_child_misses_zero_limit);
    RUN(tie_goes_to_task);
    RUN(void_and_move_only_results);
    RUN(destroyed_parent_cancels_all);
    RUN(empty_task_is_rejected);
    RUN(nested_timeouts);
    RUN(timed_out_read_deregisters);
}

REGISTER_TEST_SUITE();
