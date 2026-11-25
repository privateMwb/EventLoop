// Reactor stale-event regression suite.
//
// Coverage:
// - A descriptor number closed and reused inside one dispatch batch must
//   not receive the event that was queued for its predecessor.

// clang-format off
#include <support/framework.h>

#include <unistd.h> // dup2, close — forcing descriptor-number reuse
// clang-format on

using namespace EventLoop;

// Verifies an event queued for a closed fd is not delivered to its successor.
static void reused_fd_gets_no_stale_event() {
    Reactor reactor;
    Pipe a = makePipe();
    Pipe b = makePipe();
    const int fdA = a.readEnd.get();
    const int fdB = b.readEnd.get();

    bool swapped = false;
    bool staleDelivered = false;
    int reusedFd = -1;
    Pipe replacement; // Keeps the successor's write end alive.

    // Both read ends are ready in one batch. Whichever runs first closes the
    // other and re-registers a *different, not-ready* pipe under the very same
    // descriptor number. The victim's event is still queued in this batch.
    auto handler = [&](int fd, Event) {
        if (swapped) {
            return;
        }
        swapped = true;

        const int victim = (fd == fdA) ? fdB : fdA;
        Pipe& victimPipe = (fd == fdA) ? b : a;

        reactor.remove(victim);
        victimPipe.readEnd.reset(); // Closes the victim's descriptor number.

        // The lowest free number is usually `victim` itself, but dup2 makes the
        // reuse certain instead of depending on that.
        Pipe fresh = makePipe();
        const int source = fresh.readEnd.release();
        if (source != victim) {
            ::dup2(source, victim);
            ::close(source);
        }
        victimPipe.readEnd.reset(victim); // Re-own it; closes on scope exit.
        replacement = std::move(fresh);
        reusedFd = victim;

        reactor.add(victim, Event::Readable, [&](int, Event) { staleDelivered = true; });
    };

    reactor.add(fdA, Event::Readable, handler);
    reactor.add(fdB, Event::Readable, handler);
    writeByte(a.writeEnd.get());
    writeByte(b.writeEnd.get());

    CHK(reactor.runOnce(1000) == 1);
    CHK(swapped);
    CHK(reusedFd >= 0);
    CHK(!staleDelivered);
}

static void run_tests() {
    RUN(reused_fd_gets_no_stale_event);
}

REGISTER_TEST_SUITE();
