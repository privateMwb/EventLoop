// EventLoop::Socket test suite.
//
// Coverage:
// - read/write/writeAll: immediate completion, suspension and resumption
// - EOF, errors, empty sockets and bad arguments
// - One waiter per direction, reader and writer running together
// - accept() and connect()
// - Fairness: a socket cannot monopolise the loop

#include <support/framework.h>

// clang-format off
#include <common/echoHarness.h> // echo::pumpUntil, echo::makePayload — test driving helpers

#include <array>                // std::array — read buffers
#include <cstddef>              // std::byte — buffer element type
#include <span>                 // std::span — buffer views
#include <stdexcept>            // std::invalid_argument, std::logic_error — expected failures
#include <string>               // std::string — payloads
#include <string_view>          // std::string_view — short literals
#include <system_error>         // std::system_error — expected failures
#include <sys/socket.h>         // socketpair — connected local pair
#include <unistd.h>             // ::read — draining a peer directly
// clang-format on

using namespace EventLoop;

/// Two connected, registered stream sockets.
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

static Task<std::size_t> readInto(Socket& socket, std::span<std::byte> buffer) {
    co_return co_await socket.read(buffer);
}

static Task<void> writeText(Socket& socket, std::string_view text) {
    co_await socket.writeAll(std::as_bytes(std::span(text)));
}

static Task<std::size_t> writeSome(Socket& socket, std::string_view text) {
    co_return co_await socket.write(std::as_bytes(std::span(text)));
}

static std::string_view asText(std::span<const std::byte> bytes) {
    return {reinterpret_cast<const char*>(bytes.data()), bytes.size()};
}

// Verifies data written earlier is read back without suspending.
static void read_completes_immediately_with_data() {
    Reactor reactor;
    SocketPair pair = makePair(reactor);
    std::array<std::byte, 64> buffer;

    syncWait(writeText(pair.a, "hello"));
    const std::size_t count = syncWait(readInto(pair.b, buffer));

    CHK(count == 5);
    CHK(asText(std::span(buffer).first(count)) == "hello");
}

// Verifies a read with no data suspends and is resumed when data arrives.
static void read_suspends_until_data_arrives() {
    Reactor reactor;
    SocketPair pair = makePair(reactor);
    std::array<std::byte, 64> buffer;

    Task<std::size_t> reader = readInto(pair.b, buffer);
    reader.start();
    CHK(!reader.done());

    syncWait(writeText(pair.a, "later"));

    CHK(echo::pumpUntil(reactor, [&] { return reader.done(); }));
    const std::size_t count = reader.result();
    CHK(asText(std::span(buffer).first(count)) == "later");
}

// Verifies a read on a closed peer reports end of stream.
static void read_returns_zero_at_eof() {
    Reactor reactor;
    SocketPair pair = makePair(reactor);
    std::array<std::byte, 16> buffer;

    pair.a.close();

    CHK(syncWait(readInto(pair.b, buffer)) == 0);
}

// Verifies a suspended read is resumed with EOF when the peer closes.
static void suspended_read_sees_eof() {
    Reactor reactor;
    SocketPair pair = makePair(reactor);
    std::array<std::byte, 16> buffer;
    Task<std::size_t> reader = readInto(pair.b, buffer);
    reader.start();
    CHK(!reader.done());

    pair.a.close();

    CHK(echo::pumpUntil(reactor, [&] { return reader.done(); }));
    CHK(reader.result() == 0);
}

// Verifies shutdownWrite() delivers EOF while the other direction stays open.
static void shutdown_write_signals_eof() {
    Reactor reactor;
    SocketPair pair = makePair(reactor);
    std::array<std::byte, 16> buffer;

    pair.a.shutdownWrite();

    CHK(syncWait(readInto(pair.b, buffer)) == 0);
    syncWait(writeText(pair.b, "still open")); // b can still send to a.
    CHK(syncWait(readInto(pair.a, buffer)) == 10);
}

// Verifies bad arguments and empty sockets fail loudly.
static void bad_uses_are_reported() {
    Reactor reactor;
    SocketPair pair = makePair(reactor);
    std::array<std::byte, 16> buffer;

    CHK_THROWS(pair.a.read({}), std::invalid_argument);

    Socket empty;
    CHK(!empty.valid());
    CHK_THROWS(syncWait(readInto(empty, buffer)), std::system_error);
    CHK_THROWS(empty.shutdown(SHUT_WR), std::system_error);
    CHK_THROWS(empty.localEndpoint(), std::system_error);
}

// Verifies write() reports how much the socket took.
static void write_reports_bytes_taken() {
    Reactor reactor;
    SocketPair pair = makePair(reactor);

    CHK(syncWait(writeSome(pair.a, "abcdef")) == 6);
}

// Verifies a second waiter in the same direction is rejected.
static void second_reader_is_rejected() {
    Reactor reactor;
    SocketPair pair = makePair(reactor);
    std::array<std::byte, 16> first;
    std::array<std::byte, 16> second;
    Task<std::size_t> waiting = readInto(pair.b, first);
    waiting.start();

    CHK_THROWS(syncWait(readInto(pair.b, second)), std::logic_error);
    CHK(!waiting.done()); // The first reader is unaffected.
}

// Verifies one coroutine reading and another writing on a socket both proceed.
static void reader_and_writer_coexist() {
    Reactor reactor;
    SocketPair pair = makePair(reactor);
    std::array<std::byte, 16> buffer;
    Task<std::size_t> reader = readInto(pair.a, buffer); // Waits to read on a.
    reader.start();

    syncWait(writeText(pair.a, "out")); // Writing on a meanwhile is fine.

    CHK(!reader.done());
    syncWait(writeText(pair.b, "in"));
    CHK(echo::pumpUntil(reactor, [&] { return reader.done(); }));
    CHK(reader.result() == 2);
}

// Verifies writeAll suspends on a full socket, keeps its progress, and finishes.
static void write_all_survives_full_buffer() {
    Reactor reactor;
    SocketPair pair = makePair(reactor);
    const std::string payload = echo::makePayload(8 * 1024 * 1024);

    Task<void> writer = writeText(pair.a, payload);
    writer.start();
    CHK(!writer.done()); // Far more than the kernel buffers hold.

    std::string received;
    char chunk[65536];
    for (int i = 0; i < 20000 && !(writer.done() && received.size() == payload.size()); ++i) {
        reactor.runOnce(1);
        for (;;) {
            const ssize_t n = ::read(pair.b.fd(), chunk, sizeof(chunk));
            if (n <= 0) {
                break;
            }
            received.append(chunk, static_cast<std::size_t>(n));
        }
    }

    CHK(writer.done());
    CHK(received.size() == payload.size());
    CHK(received == payload);
}

static Task<Socket> acceptOne(Socket& listener) {
    co_return co_await listener.accept();
}

// Verifies accept() waits for a connection and returns a usable socket.
static void accept_waits_for_connection() {
    Reactor reactor;
    Socket listener = Socket::listen(reactor, Endpoint::loopback(0));
    CHK(listener.localEndpoint().port() != 0);

    Task<Socket> acceptor = acceptOne(listener);
    acceptor.start();
    CHK(!acceptor.done());

    UniqueFd client = echo::connectClient(listener.localEndpoint().port());

    CHK(echo::pumpUntil(reactor, [&] { return acceptor.done(); }));
    Socket accepted = acceptor.result();
    CHK(accepted.valid());
    CHK(accepted.localEndpoint().family() == AF_INET);
}

// Verifies connect() reaches a listener and both ends can talk.
static void connect_reaches_listener() {
    Reactor reactor;
    Socket listener = Socket::listen(reactor, Endpoint::loopback(0));

    Task<Socket> connecting = Socket::connect(reactor, listener.localEndpoint());
    connecting.start();
    Task<Socket> accepting = acceptOne(listener);
    accepting.start();

    CHK(echo::pumpUntil(reactor, [&] { return connecting.done() && accepting.done(); }));
    Socket client = connecting.result();
    Socket server = accepting.result();

    std::array<std::byte, 16> buffer;
    syncWait(writeText(client, "ping"));
    CHK(echo::pumpUntil(reactor, [&] { return true; })); // No-op: the data is already there.
    CHK(syncWait(readInto(server, buffer)) == 4);
}

// Verifies connecting to a closed port fails with a system_error.
static void connect_refused_is_reported() {
    Reactor reactor;
    Endpoint closedPort;
    {
        Socket listener = Socket::listen(reactor, Endpoint::loopback(0));
        closedPort = listener.localEndpoint();
    } // Closed: nothing listens on this port any more.

    Task<Socket> connecting = Socket::connect(reactor, closedPort);
    connecting.start();

    CHK(echo::pumpUntil(reactor, [&] { return connecting.done(); }));
    CHK_THROWS(connecting.result(), std::system_error);
}

static Task<void> readManyTimes(Socket& socket, int count, int& reads) {
    std::array<std::byte, 1> buffer;
    for (int i = 0; i < count; ++i) {
        (void)co_await socket.read(buffer);
        ++reads;
    }
}

// Verifies a socket that is always ready yields to the loop instead of hogging it.
static void always_ready_socket_yields() {
    Reactor reactor;
    SocketPair hog = makePair(reactor);
    SocketPair other = makePair(reactor);
    syncWait(writeText(hog.a, std::string(2000, 'x'))); // 2000 bytes: always readable.

    std::array<std::byte, 8> buffer;
    Task<std::size_t> bystander = readInto(other.b, buffer);
    bystander.start();                  // Waits for data on a different socket.
    syncWait(writeText(other.a, "hi")); // Its data is now pending in the reactor.

    int reads = 0;
    Task<void> reader = readManyTimes(hog.b, 1000, reads);
    reader.start();

    // The hog gave up the loop after its fair share instead of finishing all 1000.
    CHK(reads > 0 && reads < 100);
    CHK(!reader.done());

    reactor.runOnce(0); // One loop turn: the bystander gets served, the hog advances a bit.
    CHK(bystander.done());
    CHK(reads < 1000);

    CHK(echo::pumpUntil(reactor, [&] { return reader.done(); }));
    CHK(reads == 1000);
}

static void run_tests() {
    RUN(read_completes_immediately_with_data);
    RUN(read_suspends_until_data_arrives);
    RUN(read_returns_zero_at_eof);
    RUN(suspended_read_sees_eof);
    RUN(shutdown_write_signals_eof);
    RUN(bad_uses_are_reported);
    RUN(write_reports_bytes_taken);
    RUN(second_reader_is_rejected);
    RUN(reader_and_writer_coexist);
    RUN(write_all_survives_full_buffer);
    RUN(accept_waits_for_connection);
    RUN(connect_reaches_listener);
    RUN(connect_refused_is_reported);
    RUN(always_ready_socket_yields);
}

REGISTER_TEST_SUITE();
