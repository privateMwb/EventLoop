// EventLoop::Reactor::defer test suite.
//
// Coverage:
// - Deferred callbacks run on the next wait without blocking
// - They run after the events of that wait
// - A callback deferred from a deferred callback waits for the next wait
// - A throwing callback leaves the rest queued

#include <support/framework.h>

// clang-format off
#include <stdexcept> // std::invalid_argument, std::runtime_error — expected failures
#include <string>    // std::string — ordering log
// clang-format on

using namespace EventLoop;

// Verifies a deferred callback runs on the next wait, even an unbounded one.
static void deferred_runs_without_blocking() {
    Reactor reactor;
    int calls = 0;
    reactor.defer([&] { ++calls; });

    CHK(reactor.runOnce(-1) == 1); // Would block forever if it ignored the queue.
    CHK(calls == 1);
    CHK(reactor.runOnce(0) == 0);
}

// Verifies deferred callbacks run after the events of the same wait.
static void deferred_runs_after_events() {
    Reactor reactor;
    Pipe pipe = makePipe();
    std::string order;
    reactor.add(pipe.readEnd.get(), Event::Readable, [&](int fd, Event) {
        order += 'E';
        drain(fd);
    });
    reactor.defer([&] { order += 'D'; });
    writeByte(pipe.writeEnd.get());

    CHK(reactor.runOnce(1000) == 2);
    CHK(order == "ED");
}

// Verifies work deferred by a deferred callback waits for the next wait.
static void redeferred_waits_for_next_wait() {
    Reactor reactor;
    int first = 0;
    int second = 0;
    reactor.defer([&] {
        ++first;
        reactor.defer([&] { ++second; });
    });

    CHK(reactor.runOnce(0) == 1);
    CHK(first == 1);
    CHK(second == 0);

    CHK(reactor.runOnce(0) == 1);
    CHK(second == 1);
}

// Verifies an empty callback is rejected.
static void empty_deferred_is_rejected() {
    Reactor reactor;
    CHK_THROWS(reactor.defer(MoveOnlyFunction<void()>{}), std::invalid_argument);
}

// Verifies a throwing deferred callback leaves the remaining ones queued.
static void throwing_deferred_keeps_rest() {
    Reactor reactor;
    int ran = 0;
    reactor.defer([] { throw std::runtime_error("boom"); });
    reactor.defer([&] { ++ran; });

    CHK_THROWS(reactor.runOnce(0), std::runtime_error);
    CHK(ran == 0);

    CHK(reactor.runOnce(0) == 1);
    CHK(ran == 1);
}

static void run_tests() {
    RUN(deferred_runs_without_blocking);
    RUN(deferred_runs_after_events);
    RUN(redeferred_waits_for_next_wait);
    RUN(empty_deferred_is_rejected);
    RUN(throwing_deferred_keeps_rest);
}

REGISTER_TEST_SUITE();
