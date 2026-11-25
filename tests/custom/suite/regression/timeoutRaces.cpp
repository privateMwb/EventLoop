// Timeout/I-O/cancellation race regression suite.
//
// Many reads race their time limit, the arrival of their data, and an
// optional cancellation, all landing on the same or neighbouring
// milliseconds. A model predicts the exact outcome of every one, from the
// ordering rules:
//   within one step, a cancellation is applied first, then I/O events are
//   handled, then timers run; so data arriving in the step a limit expires
//   wins, and a cancellation in that step beats both.
// After the run no timer may remain and every socket's read slot must be
// free again (a leaked registration would make the next read fail).
//
// Coverage:
// - `.within()` reads and `withTimeout()`-wrapped reads, mixed
// - Data, timeout, and cancellation outcomes at identical and adjacent steps
// - No leaked timers or registrations afterwards

#include <support/framework.h>

// clang-format off
#include <array>          // std::array — per-case read buffers
#include <cstddef>        // std::byte — buffer element
#include <cstdint>        // std::int64_t — step arithmetic
#include <memory>         // std::unique_ptr — stable per-case storage
#include <random>         // std::mt19937_64 — seeded scenarios
#include <span>           // std::span — buffer views
#include <sys/socket.h>   // socketpair — connected local pair
#include <system_error>   // std::errc, std::system_error — telling timeouts apart
#include <unistd.h>       // ::write — delivering data to a case
#include <vector>         // std::vector — the cases
// clang-format on

using namespace EventLoop;
using ms = std::chrono::milliseconds;

namespace {

enum class Outcome { Pending, Data, Timeout, Cancelled };

constexpr std::int64_t kNever = 1'000'000; ///< "No such event".
constexpr std::int64_t kSteps = 140;       ///< Simulated milliseconds.

Task<Outcome> withinReader(Socket& socket, std::span<std::byte> buffer, ms limit) {
    try {
        (void)co_await socket.read(buffer).within(limit);
        co_return Outcome::Data;
    } catch (const std::system_error& error) {
        if (error.code() == std::errc::timed_out) {
            co_return Outcome::Timeout;
        }
        throw;
    }
}

Task<std::size_t> readOnce(Socket& socket, std::span<std::byte> buffer) {
    co_return co_await socket.read(buffer);
}

Task<Outcome> wrappedReader(Reactor& reactor, Socket& socket, std::span<std::byte> buffer,
                            ms limit) {
    try {
        (void)co_await withTimeout(reactor, readOnce(socket, buffer), limit);
        co_return Outcome::Data;
    } catch (const TimeoutError&) {
        co_return Outcome::Timeout;
    }
}

/// One racing read: its socket pair, timing, and the outcome the model expects.
struct Case {
    Socket writer;
    Socket reader;
    std::array<std::byte, 8> buffer{};
    std::int64_t limit = 0;    ///< Time limit, in steps.
    std::int64_t writeAt = 0;  ///< Step the data is written, or kNever.
    std::int64_t cancelAt = 0; ///< Step the Task is cancelled, or kNever.
    bool wrapped = false;      ///< Uses withTimeout() instead of .within().
    Task<Outcome> task;
    Outcome expected = Outcome::Pending;
    bool cancelled = false;
};

/// The outcome the ordering rules dictate.
Outcome modelOutcome(const Case& c) {
    // Cancellation in a step happens before that step's events and timers.
    if (c.cancelAt != kNever && c.cancelAt <= std::min(c.writeAt, c.limit)) {
        return Outcome::Cancelled;
    }
    // Events are handled before timers, so data in the step the limit
    // expires still wins.
    if (c.writeAt != kNever && c.writeAt <= c.limit) {
        return Outcome::Data;
    }
    return Outcome::Timeout;
}

} // namespace

template <typename Unused = void> static void runRaces(std::uint64_t seed) {
    ManualClock clock;
    Reactor reactor(clock.source());
    std::mt19937_64 random(seed);

    std::vector<std::unique_ptr<Case>> cases;
    for (int i = 0; i < 60; ++i) {
        auto c = std::make_unique<Case>();
        int fds[2];
        CHK(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, fds) == 0);
        c->writer = Socket::adopt(reactor, UniqueFd(fds[0]));
        c->reader = Socket::adopt(reactor, UniqueFd(fds[1]));
        c->limit = 1 + static_cast<std::int64_t>(random() % 100);
        c->writeAt = (random() % 10 < 3) ? kNever : static_cast<std::int64_t>(random() % 121);
        c->cancelAt = (random() % 4 != 0) ? kNever : static_cast<std::int64_t>(random() % 111);
        c->wrapped = (random() % 2) != 0;
        // Pull some events onto the same step, where the ordering rules matter.
        if (c->writeAt != kNever && random() % 3 == 0) {
            c->writeAt = c->limit;
        }
        if (c->cancelAt != kNever && random() % 3 == 0) {
            c->cancelAt = (random() % 2) ? c->limit : c->writeAt;
        }
        c->expected = modelOutcome(*c);
        cases.push_back(std::move(c));
    }

    for (auto& c : cases) {
        c->task = c->wrapped ? wrappedReader(reactor, c->reader, c->buffer, ms(c->limit))
                             : withinReader(c->reader, c->buffer, ms(c->limit));
        c->task.start();
    }

    for (std::int64_t step = 0; step <= kSteps; ++step) {
        for (auto& c : cases) {
            if (c->writeAt == step) {
                const char byte = 'x';
                (void)!::write(c->writer.fd(), &byte, 1);
            }
            if (c->cancelAt == step && !c->task.done()) {
                c->task.cancel();
                c->cancelled = true;
            }
        }
        if (step > 0) {
            clock.advance(ms(1));
        }
        reactor.runOnce(0);
    }

    int mismatches = 0;
    for (auto& c : cases) {
        Outcome actual = Outcome::Pending;
        if (c->cancelled) {
            actual = Outcome::Cancelled;
        } else if (c->task.done()) {
            actual = c->task.result();
        }
        if (actual != c->expected) {
            ++mismatches;
        }
    }
    CHK(mismatches == 0);
    CHK(reactor.timerCount() == 0); // Nothing armed survives.

    // Every read slot must be free: a leaked registration would make a fresh
    // read fail with logic_error instead of just waiting.
    int stuck = 0;
    for (auto& c : cases) {
        Task<std::size_t> probe = readOnce(c->reader, c->buffer);
        probe.start();
        if (probe.done()) {
            // Finished at once: acceptable only if data was left over (the
            // unread byte of an already-completed or cancelled case).
            try {
                (void)probe.result();
            } catch (const std::logic_error&) {
                ++stuck;
            } catch (...) {
            }
        }
    }
    CHK(stuck == 0);
}

static void races_seed_1() {
    runRaces(1);
}
static void races_seed_2() {
    runRaces(2);
}
static void races_seed_3() {
    runRaces(3);
}
static void races_seed_4() {
    runRaces(4);
}
static void races_seed_5() {
    runRaces(5);
}
static void races_seed_6() {
    runRaces(6);
}
static void races_seed_7() {
    runRaces(7);
}
static void races_seed_8() {
    runRaces(8);
}

static void run_tests() {
    RUN(races_seed_1);
    RUN(races_seed_2);
    RUN(races_seed_3);
    RUN(races_seed_4);
    RUN(races_seed_5);
    RUN(races_seed_6);
    RUN(races_seed_7);
    RUN(races_seed_8);
}

REGISTER_TEST_SUITE();
