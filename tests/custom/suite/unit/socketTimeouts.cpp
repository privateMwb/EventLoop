// EventLoop::Socket time limit, cancellation and back-off test suite.
//
// Coverage:
// - .within() on read, writeAll and accept
// - Data that is ready beats a timeout that is also due
// - Closing under a timed operation, and destroying its Task, leave no timer
// - accept() backs off when out of descriptors and recovers

#include <support/framework.h>

// clang-format off
#include <common/echoHarness.h> // echo::connectClient, echo::makePayload — test driving helpers

#include <array>                // std::array — buffers
#include <cstddef>              // std::byte — buffer element
#include <fcntl.h>              // fcntl, F_GETFD — probing which descriptors are open
#include <span>                 // std::span — buffer views
#include <string>               // std::string — payloads
#include <string_view>          // std::string_view — short literals
#include <sys/resource.h>       // getrlimit, setrlimit, RLIMIT_NOFILE — forcing EMFILE
#include <sys/socket.h>         // socketpair — connected local pair
#include <system_error>         // std::errc, std::system_error — checking failures
#include <unistd.h>             // ::read — draining a peer
// clang-format on

using namespace EventLoop;
using ms = std::chrono::milliseconds;

struct SocketPair {
    Socket a;
    Socket b;
};

static SocketPair makePair(Reactor& reactor) {
    int fds[2];
    CHK(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, fds) == 0);
    return SocketPair{Socket::adopt(reactor, UniqueFd(fds[0])),
                      Socket::adopt(reactor, UniqueFd(fds[1]))};
}

template <typename T> static std::error_code failureOf(Task<T>& task) {
    try {
        (void)task.result();
    } catch (const std::system_error& error) {
        return error.code();
    }
    return {};
}

static Task<std::size_t> readWithin(Socket& socket, std::span<std::byte> buffer, ms limit) {
    co_return co_await socket.read(buffer).within(limit);
}

static Task<std::size_t> readPlain(Socket& socket, std::span<std::byte> buffer) {
    co_return co_await socket.read(buffer);
}

static Task<void> writeWithin(Socket& socket, std::span<const std::byte> data, ms limit) {
    co_await socket.writeAll(data).within(limit);
}

static Task<void> writeText(Socket& socket, std::string_view text) {
    co_await socket.writeAll(std::as_bytes(std::span(text)));
}

static Task<Socket> acceptWithin(Socket& listener, ms limit) {
    co_return co_await listener.accept().within(limit);
}

static Task<Socket> acceptPlain(Socket& listener) {
    co_return co_await listener.accept();
}

// Verifies a read with no data fails with ETIMEDOUT at its limit, and the socket stays usable.
static void read_within_times_out() {
    ManualClock clock;
    Reactor reactor(clock.source());
    SocketPair pair = makePair(reactor);
    std::array<std::byte, 16> buffer;
    Task<std::size_t> reader = readWithin(pair.b, buffer, ms(50));
    reader.start();
    CHK(reactor.timerCount() == 1);

    clock.advance(ms(49));
    reactor.runOnce(0);
    CHK(!reader.done());

    clock.advance(ms(1));
    reactor.runOnce(0);

    CHK(reader.done());
    CHK(failureOf(reader) == std::errc::timed_out);
    CHK(reactor.timerCount() == 0);

    syncWait(writeText(pair.a, "after")); // The socket still works.
    CHK(syncWait(readPlain(pair.b, buffer)) == 5);
}

// Verifies data arriving in time completes the read and cancels its timer.
static void read_within_completes_in_time() {
    ManualClock clock;
    Reactor reactor(clock.source());
    SocketPair pair = makePair(reactor);
    std::array<std::byte, 16> buffer;
    Task<std::size_t> reader = readWithin(pair.b, buffer, ms(50));
    reader.start();

    clock.advance(ms(20));
    syncWait(writeText(pair.a, "ok"));
    reactor.runOnce(0);

    CHK(reader.done());
    CHK(reader.result() == 2);
    CHK(reactor.timerCount() == 0);
}

// Verifies data and an expired limit in the same pass: the data wins.
static void data_beats_expired_limit() {
    ManualClock clock;
    Reactor reactor(clock.source());
    SocketPair pair = makePair(reactor);
    std::array<std::byte, 16> buffer;
    Task<std::size_t> reader = readWithin(pair.b, buffer, ms(50));
    reader.start();

    syncWait(writeText(pair.a, "x"));
    clock.advance(ms(500)); // Far past the limit, but the data is already there.
    reactor.runOnce(0);

    CHK(reader.done());
    CHK(reader.result() == 1); // Not ETIMEDOUT.
}

static Task<std::size_t> drainThenRead(Socket& socket, int firstReads) {
    std::array<std::byte, 1> buffer;
    for (int i = 0; i < firstReads; ++i) {
        (void)co_await socket.read(buffer); // Completes immediately.
    }
    // The fairness budget is used up, so this read yields. Its zero limit is
    // already due when the loop gets to it, and the timer runs before the
    // deferred retry; the data in the socket must still win.
    co_return co_await socket.read(buffer).within(ms(0));
}

// Verifies a due timeout does not discard data a retry would have delivered.
static void due_timeout_delivers_ready_data() {
    ManualClock clock;
    Reactor reactor(clock.source());
    SocketPair pair = makePair(reactor);
    syncWait(writeText(pair.a, std::string(64, 'x')));

    Task<std::size_t> reader = drainThenRead(pair.b, 16);
    reader.start();
    CHK(!reader.done()); // Yielded after its fair share.

    reactor.runOnce(0); // Timer first, then the deferred retry.

    CHK(reader.done());
    CHK(reader.result() == 1); // Data, not ETIMEDOUT.
    CHK(reactor.timerCount() == 0);
}

// Verifies a stalled writeAll times out and the socket recovers once drained.
static void write_all_within_times_out() {
    ManualClock clock;
    Reactor reactor(clock.source());
    SocketPair pair = makePair(reactor);
    const std::string payload = echo::makePayload(8 * 1024 * 1024); // More than the buffers hold.
    Task<void> writer = writeWithin(pair.a, std::as_bytes(std::span(payload)), ms(30));
    writer.start();
    CHK(!writer.done());

    clock.advance(ms(30));
    reactor.runOnce(0);

    CHK(writer.done());
    CHK(failureOf(writer) == std::errc::timed_out);
    CHK(reactor.timerCount() == 0);

    char chunk[65536]; // Drain the peer; the write side must be usable again.
    while (::read(pair.b.fd(), chunk, sizeof(chunk)) > 0) {
    }
    syncWait(writeText(pair.a, "recovered"));
}

// Verifies accept() with a limit times out, and a later accept still works.
static void accept_within_times_out() {
    ManualClock clock;
    Reactor reactor(clock.source());
    Socket listener = Socket::listen(reactor, Endpoint::loopback(0));
    Task<Socket> acceptor = acceptWithin(listener, ms(30));
    acceptor.start();

    clock.advance(ms(30));
    reactor.runOnce(0);

    CHK(acceptor.done());
    CHK(failureOf(acceptor) == std::errc::timed_out);
    CHK(reactor.timerCount() == 0);

    Task<Socket> next = acceptPlain(listener);
    next.start();
    UniqueFd client = echo::connectClient(listener.localEndpoint().port());
    CHK(echo::pumpUntil(reactor, [&] { return next.done(); }));
    CHK(next.result().valid());
}

// Verifies closing a socket under a timed read wakes it with ECANCELED and cancels the timer.
static void close_cancels_timed_read() {
    ManualClock clock;
    Reactor reactor(clock.source());
    SocketPair pair = makePair(reactor);
    std::array<std::byte, 16> buffer;
    Task<std::size_t> reader = readWithin(pair.b, buffer, ms(100));
    reader.start();
    CHK(reactor.timerCount() == 1);

    pair.b.close();
    reactor.runOnce(0);

    CHK(reader.done());
    CHK(failureOf(reader) == std::errc::operation_canceled);
    CHK(reactor.timerCount() == 0);
}

// Verifies destroying a Task with a timed read pending leaves no timer behind.
static void destroyed_task_cancels_timer() {
    ManualClock clock;
    Reactor reactor(clock.source());
    SocketPair pair = makePair(reactor);
    std::array<std::byte, 16> buffer;
    {
        Task<std::size_t> reader = readWithin(pair.b, buffer, ms(100));
        reader.start();
        CHK(reactor.timerCount() == 1);
    }

    CHK(reactor.timerCount() == 0);
    clock.advance(ms(1000));
    reactor.runOnce(0); // Past the old limit: there is no timer left to fire into the dead frame.
    CHK(reactor.timerCount() == 0);
}

/// Lowers the descriptor limit for its lifetime and restores it afterwards.
class FdLimit {
  public:
    FdLimit() {
        ::getrlimit(RLIMIT_NOFILE, &saved_);
    }
    ~FdLimit() {
        restore();
    }
    FdLimit(const FdLimit&) = delete;
    FdLimit& operator=(const FdLimit&) = delete;

    /// Caps descriptors so the lowest free number is already out of range.
    void exhaust() {
        int lowestFree = 0;
        while (::fcntl(lowestFree, F_GETFD) != -1) {
            ++lowestFree;
        }
        rlimit lowered = saved_;
        lowered.rlim_cur = static_cast<rlim_t>(lowestFree);
        ::setrlimit(RLIMIT_NOFILE, &lowered);
    }

    void restore() {
        ::setrlimit(RLIMIT_NOFILE, &saved_);
    }

  private:
    rlimit saved_{};
};

// Verifies accept() backs off on EMFILE, retries on a timer, and recovers.
static void accept_backs_off_when_exhausted() {
    ManualClock clock;
    Reactor reactor(clock.source());
    Socket listener = Socket::listen(reactor, Endpoint::loopback(0));
    UniqueFd client = echo::connectClient(listener.localEndpoint().port());

    FdLimit limit;
    limit.exhaust(); // accept4 now fails with EMFILE.
    Task<Socket> acceptor = acceptPlain(listener);
    acceptor.start();

    CHK(!acceptor.done());          // It did not fail...
    CHK(reactor.timerCount() == 1); // ...it is waiting on a back-off timer.

    clock.advance(ms(100));
    reactor.runOnce(0); // Retries; still out of descriptors.
    CHK(!acceptor.done());
    CHK(reactor.timerCount() == 1); // Backed off again.

    limit.restore();
    clock.advance(ms(100));
    reactor.runOnce(0);

    CHK(acceptor.done());
    CHK(acceptor.result().valid());
    CHK(reactor.timerCount() == 0);
}

static void run_tests() {
    RUN(read_within_times_out);
    RUN(read_within_completes_in_time);
    RUN(data_beats_expired_limit);
    RUN(due_timeout_delivers_ready_data);
    RUN(write_all_within_times_out);
    RUN(accept_within_times_out);
    RUN(close_cancels_timed_read);
    RUN(destroyed_task_cancels_timer);
    RUN(accept_backs_off_when_exhausted);
}

REGISTER_TEST_SUITE();
