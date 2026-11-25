// EventLoop::Socket lifetime test suite.
//
// Coverage:
// - Destroying a Task suspended on a socket deregisters its operation
// - A Task that owns its Socket closes it when destroyed mid-read
// - Closing or destroying a Socket wakes pending operations with ECANCELED,
//   from the loop and never inline
// - Moving a Socket does not disturb an operation already waiting on it

#include <support/framework.h>

// clang-format off
#include <common/echoHarness.h> // echo::pumpUntil — test driving helper

#include <system_error>         // std::error_code, std::errc — checking how operations ended

#include <array>                // std::array — read buffers
#include <cstddef>              // std::byte — buffer element type
#include <span>                 // std::span — buffer views
#include <string_view>          // std::string_view — short literals
#include <sys/socket.h>         // socketpair — connected local pair
#include <unistd.h>             // ::read — direct peer inspection
// clang-format on

using namespace EventLoop;

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

/// How a finished Task failed, or an empty code if it succeeded.
template <typename T> static std::error_code failureOf(Task<T>& task) {
    try {
        (void)task.result();
    } catch (const std::system_error& error) {
        return error.code();
    }
    return {};
}

static Task<std::size_t> readInto(Socket& socket, std::span<std::byte> buffer) {
    co_return co_await socket.read(buffer);
}

static Task<void> writeText(Socket& socket, std::string_view text) {
    co_await socket.writeAll(std::as_bytes(std::span(text)));
}

/// Owns its socket by value, as a connection handler would.
static Task<void> readOwned(Socket socket, bool& finished) {
    std::array<std::byte, 16> buffer;
    (void)co_await socket.read(buffer);
    finished = true;
}

// Verifies a destroyed reader leaves the socket usable for the next one.
static void destroyed_reader_deregisters() {
    Reactor reactor;
    SocketPair pair = makePair(reactor);
    std::array<std::byte, 16> buffer;
    {
        Task<std::size_t> reader = readInto(pair.b, buffer);
        reader.start();
        CHK(!reader.done());
    } // Destroyed while suspended on b.

    syncWait(writeText(pair.a, "x"));
    reactor.runOnce(0); // The edge arrives; there is nobody to resume.

    // The slot is free again: a fresh reader gets the byte immediately.
    CHK(syncWait(readInto(pair.b, buffer)) == 1);
}

// Verifies destroying a Task that owns its socket closes the connection.
static void owning_task_closes_socket() {
    Reactor reactor;
    SocketPair pair = makePair(reactor);
    bool finished = false;
    {
        Task<void> handler = readOwned(std::move(pair.b), finished);
        handler.start();
        CHK(!handler.done());
        CHK(reactor.size() == 2);
    } // The frame, its pending read and its Socket are destroyed.

    CHK(!finished);
    CHK(reactor.size() == 1); // Only a remains registered.
    std::array<std::byte, 4> buffer;
    CHK(syncWait(readInto(pair.a, buffer)) == 0); // The peer saw the close.
}

// Verifies closing a socket wakes a pending read with ECANCELED, from the loop.
static void close_cancels_pending_read() {
    Reactor reactor;
    SocketPair pair = makePair(reactor);
    std::array<std::byte, 16> buffer;
    Task<std::size_t> reader = readInto(pair.b, buffer);
    reader.start();

    pair.b.close();
    CHK(!reader.done()); // Not resumed inline from close().
    CHK(reactor.size() == 1);

    reactor.runOnce(0); // The wake, plus the peer's hang-up event.

    CHK(reader.done());
    CHK(failureOf(reader) == std::errc::operation_canceled);
}

// Verifies a pending read is woken, not leaked, when its Socket is destroyed.
static void pending_read_wakes_on_destroy() {
    Reactor reactor;
    std::array<std::byte, 16> buffer;
    Task<std::size_t> reader;
    {
        SocketPair pair = makePair(reactor);
        reader = readInto(pair.b, buffer);
        reader.start();
    } // Both sockets destroyed with the read still pending.
    CHK(!reader.done());
    CHK(reactor.size() == 0);

    reactor.runOnce(0);

    CHK(reader.done());
    CHK(failureOf(reader) == std::errc::operation_canceled);
}

// Verifies destroying the waiting Task before the wake is delivered is safe.
static void destroyed_waiter_skips_wake() {
    Reactor reactor;
    SocketPair pair = makePair(reactor);
    std::array<std::byte, 16> buffer;
    {
        Task<std::size_t> reader = readInto(pair.b, buffer);
        reader.start();
        pair.b.close(); // Wake queued, but the Task dies first.
    }

    reactor.runOnce(0); // The queued wake finds nobody waiting.
    CHK(reactor.size() == 1);
}

// Verifies a pending accept is woken when the listener goes away.
static void pending_accept_wakes_on_destroy() {
    Reactor reactor;
    Task<Socket> acceptor;
    {
        Socket listener = Socket::listen(reactor, Endpoint::loopback(0));
        acceptor = [](Socket& l) -> Task<Socket> { co_return co_await l.accept(); }(listener);
        acceptor.start();
        CHK(!acceptor.done());
    }

    reactor.runOnce(0);

    CHK(acceptor.done());
    CHK(failureOf(acceptor) == std::errc::operation_canceled);
    CHK(reactor.size() == 0);
}

// Verifies moving a Socket does not disturb an operation already waiting.
static void move_keeps_pending_read() {
    Reactor reactor;
    SocketPair pair = makePair(reactor);
    std::array<std::byte, 16> buffer;
    Task<std::size_t> reader = readInto(pair.b, buffer);
    reader.start();

    Socket moved = std::move(pair.b); // The reader still waits on the same state.
    syncWait(writeText(pair.a, "ok"));

    CHK(echo::pumpUntil(reactor, [&] { return reader.done(); }));
    CHK(reader.result() == 2);
    CHK(moved.valid());
}

static void run_tests() {
    RUN(destroyed_reader_deregisters);
    RUN(owning_task_closes_socket);
    RUN(close_cancels_pending_read);
    RUN(pending_read_wakes_on_destroy);
    RUN(destroyed_waiter_skips_wake);
    RUN(pending_accept_wakes_on_destroy);
    RUN(move_keeps_pending_read);
}

REGISTER_TEST_SUITE();
