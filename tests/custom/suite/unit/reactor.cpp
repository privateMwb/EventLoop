// EventLoop::Reactor test suite.
//
// Coverage:
// - Registration, modification and removal of descriptors
// - Dispatch of readiness events with the reported flags
// - Level- vs edge-triggered delivery
// - Callbacks that mutate the reactor while it dispatches
// - Error handling for invalid registrations
// - stop()/run() control flow

// clang-format off
#include <support/framework.h>

#include <system_error> // std::system_error — expected from bad registrations
#include <stdexcept>    // std::invalid_argument — expected from an empty callback
// clang-format on

using namespace EventLoop;

// Verifies a readable descriptor dispatches with the Readable flag.
static void add_dispatches_readable_event() {
    Reactor reactor;
    Pipe pipe = makePipe();
    int seenFd = -1;
    Event seen = Event::None;
    reactor.add(pipe.readEnd.get(), Event::Readable, [&](int fd, Event ready) {
        seenFd = fd;
        seen = ready;
    });

    writeByte(pipe.writeEnd.get());

    CHK(reactor.runOnce(1000) == 1);
    CHK(seenFd == pipe.readEnd.get());
    CHK(has(seen, Event::Readable));
}

// Verifies polling an idle reactor dispatches nothing.
static void idle_poll_dispatches_nothing() {
    Reactor reactor;
    Pipe pipe = makePipe();
    int calls = 0;
    reactor.add(pipe.readEnd.get(), Event::Readable, [&](int, Event) { ++calls; });

    CHK(reactor.runOnce(0) == 0);
    CHK(calls == 0);
}

// Verifies level-triggered delivery repeats while data stays unread.
static void level_redelivers_unread_data() {
    Reactor reactor;
    Pipe pipe = makePipe();
    int calls = 0;
    reactor.add(pipe.readEnd.get(), Event::Readable, [&](int, Event) { ++calls; });

    writeByte(pipe.writeEnd.get());

    CHK(reactor.runOnce(1000) == 1);
    CHK(reactor.runOnce(0) == 1);
    CHK(calls == 2);
}

// Verifies edge-triggered delivery fires once per readiness change.
static void edge_delivers_once_only() {
    Reactor reactor;
    Pipe pipe = makePipe();
    int calls = 0;
    reactor.add(pipe.readEnd.get(), Event::Readable | Event::Edge, [&](int, Event) { ++calls; });

    writeByte(pipe.writeEnd.get());
    CHK(reactor.runOnce(1000) == 1);
    CHK(reactor.runOnce(0) == 0); // Unread data does not re-trigger an edge.

    drain(pipe.readEnd.get());
    writeByte(pipe.writeEnd.get());
    CHK(reactor.runOnce(1000) == 1); // A fresh arrival is a fresh edge.
    CHK(calls == 2);
}

// Verifies modify() switches the interest of a registered descriptor.
static void modify_changes_interest_set() {
    Reactor reactor;
    Pipe pipe = makePipe();
    int calls = 0;
    reactor.add(pipe.writeEnd.get(), Event::Writable, [&](int, Event) { ++calls; });

    CHK(reactor.runOnce(1000) == 1); // A fresh pipe is writable.

    reactor.modify(pipe.writeEnd.get(), Event::Readable); // A write end is never readable.
    CHK(reactor.runOnce(0) == 0);
    CHK(calls == 1);
}

// Verifies a removed descriptor receives no further events.
static void remove_stops_event_delivery() {
    Reactor reactor;
    Pipe pipe = makePipe();
    int calls = 0;
    reactor.add(pipe.readEnd.get(), Event::Readable, [&](int, Event) { ++calls; });
    writeByte(pipe.writeEnd.get());

    reactor.remove(pipe.readEnd.get());

    CHK(!reactor.contains(pipe.readEnd.get()));
    CHK(reactor.size() == 0);
    CHK(reactor.runOnce(0) == 0);
    CHK(calls == 0);
}

// Verifies removing an unknown descriptor is a harmless no-op.
static void remove_unknown_fd_ignored() {
    Reactor reactor;
    reactor.remove(12345);
    CHK(reactor.size() == 0);
}

// Verifies a descriptor can be removed after it was already closed.
static void remove_after_close_works() {
    Reactor reactor;
    Pipe pipe = makePipe();
    const int fd = pipe.readEnd.get();
    reactor.add(fd, Event::Readable, [](int, Event) {});

    pipe.readEnd.reset(); // Closed first; the kernel drops it from epoll.
    reactor.remove(fd);

    CHK(!reactor.contains(fd));
}

// Verifies invalid registrations are rejected with the right errors.
static void add_rejects_bad_input() {
    Reactor reactor;
    Pipe pipe = makePipe();
    const int fd = pipe.readEnd.get();
    reactor.add(fd, Event::Readable, [](int, Event) {});

    CHK_THROWS(reactor.add(fd, Event::Readable, [](int, Event) {}), std::system_error);
    CHK_THROWS(reactor.add(-1, Event::Readable, [](int, Event) {}), std::system_error);
    CHK_THROWS(reactor.add(pipe.writeEnd.get(), Event::Readable, Reactor::Callback{}),
               std::invalid_argument);
    CHK_THROWS(reactor.modify(9999, Event::Readable), std::system_error);
    CHK(reactor.size() == 1);
}

// Verifies a callback can remove its own registration safely.
static void callback_removes_itself() {
    Reactor reactor;
    Pipe pipe = makePipe();
    int calls = 0;
    reactor.add(pipe.readEnd.get(), Event::Readable, [&](int fd, Event) {
        ++calls;
        reactor.remove(fd);
    });
    writeByte(pipe.writeEnd.get());

    CHK(reactor.runOnce(1000) == 1);
    CHK(!reactor.contains(pipe.readEnd.get()));
    CHK(reactor.runOnce(0) == 0);
    CHK(calls == 1);
}

// Verifies a callback that removes another fd suppresses its event.
static void callback_removes_other_fd() {
    Reactor reactor;
    Pipe a = makePipe();
    Pipe b = makePipe();
    const int fdA = a.readEnd.get();
    const int fdB = b.readEnd.get();
    int calls = 0;
    // Whichever fires first removes the other, so exactly one call happens
    // regardless of the order the kernel reports the two events in.
    auto handler = [&](int fd, Event) {
        ++calls;
        reactor.remove(fd == fdA ? fdB : fdA);
    };
    reactor.add(fdA, Event::Readable, handler);
    reactor.add(fdB, Event::Readable, handler);
    writeByte(a.writeEnd.get());
    writeByte(b.writeEnd.get());

    CHK(reactor.runOnce(1000) == 1);
    CHK(calls == 1);
}

// Verifies a callback can register a new descriptor while dispatching.
static void callback_adds_new_fd() {
    Reactor reactor;
    Pipe first = makePipe();
    Pipe second = makePipe();
    int secondCalls = 0;
    reactor.add(first.readEnd.get(), Event::Readable, [&](int fd, Event) {
        reactor.remove(fd);
        reactor.add(second.readEnd.get(), Event::Readable, [&](int, Event) { ++secondCalls; });
    });
    writeByte(first.writeEnd.get());
    writeByte(second.writeEnd.get());

    CHK(reactor.runOnce(1000) == 1);
    CHK(reactor.runOnce(1000) == 1);
    CHK(secondCalls == 1);
}

// Verifies stop() from a callback ends run().
static void stop_ends_run_loop() {
    Reactor reactor;
    Pipe pipe = makePipe();
    int calls = 0;
    reactor.add(pipe.readEnd.get(), Event::Readable, [&](int fd, Event) {
        ++calls;
        drain(fd);
        reactor.stop();
    });
    writeByte(pipe.writeEnd.get());

    reactor.run(1000);

    CHK(calls == 1);
}

// Verifies an exception from a callback leaves the reactor usable.
static void throwing_callback_keeps_reactor() {
    Reactor reactor;
    Pipe pipe = makePipe();
    int calls = 0;
    reactor.add(pipe.readEnd.get(), Event::Readable, [&](int fd, Event) {
        ++calls;
        drain(fd);
        if (calls == 1) {
            throw std::runtime_error("boom");
        }
    });

    writeByte(pipe.writeEnd.get());
    CHK_THROWS(reactor.runOnce(1000), std::runtime_error);

    writeByte(pipe.writeEnd.get());
    CHK(reactor.runOnce(1000) == 1);
    CHK(calls == 2);
}

static void run_tests() {
    RUN(add_dispatches_readable_event);
    RUN(idle_poll_dispatches_nothing);
    RUN(level_redelivers_unread_data);
    RUN(edge_delivers_once_only);
    RUN(modify_changes_interest_set);
    RUN(remove_stops_event_delivery);
    RUN(remove_unknown_fd_ignored);
    RUN(remove_after_close_works);
    RUN(add_rejects_bad_input);
    RUN(callback_removes_itself);
    RUN(callback_removes_other_fd);
    RUN(callback_adds_new_fd);
    RUN(stop_ends_run_loop);
    RUN(throwing_callback_keeps_reactor);
}

REGISTER_TEST_SUITE();
