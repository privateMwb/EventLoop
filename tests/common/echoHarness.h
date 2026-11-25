#pragma once

// Behavioural scenarios shared by every echo server implementation. The
// callback server (Phase 1) and the coroutine server (Phase 3) must pass
// exactly the same scenarios: that is what "behaves identically" means.
//
// Include after <support/framework.h>. A `Server` type needs a constructor
// taking `Reactor&`, `port()` and `connectionCount()`.

// clang-format off
#include <EventLoop/EventLoop.h> // Reactor, UniqueFd, setNonBlocking — the runtime under test

#include <arpa/inet.h>           // inet_pton, htons — client address
#include <cerrno>                // errno — client connect failure
#include <netinet/in.h>          // sockaddr_in — client address
#include <string>                // std::string — payloads
#include <sys/socket.h>          // socket, connect, send, shutdown — client side
#include <system_error>          // std::system_error — client connect failure
#include <unistd.h>              // read — client side
#include <vector>                // std::vector — many clients
// clang-format on

namespace echo {

/// Result of one client exchange.
struct Exchange {
    std::string received; ///< Everything echoed back.
    bool eof = false;     ///< The server closed its side.
};

/// Connects a non-blocking client to the server on loopback.
inline EventLoop::UniqueFd connectClient(std::uint16_t port) {
    EventLoop::UniqueFd client(::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0));
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(port);
    ::inet_pton(AF_INET, "127.0.0.1", &address.sin_addr);
    // A blocking connect completes against the listen backlog without the
    // server needing to run; only afterwards does the client go non-blocking.
    if (::connect(client.get(), reinterpret_cast<sockaddr*>(&address), sizeof(address)) < 0) {
        throw std::system_error(errno, std::generic_category(), "connect");
    }
    EventLoop::setNonBlocking(client.get());
    return client;
}

/// Builds a recognisable payload of `size` bytes.
inline std::string makePayload(std::size_t size) {
    std::string payload(size, '\0');
    for (std::size_t i = 0; i < size; ++i) {
        payload[i] = static_cast<char>('a' + (i * 7) % 26);
    }
    return payload;
}

/// Drives client and server on one thread: send `payload`, step the reactor,
/// collect what comes back. With `halfClose`, shuts down the client's write
/// side after sending and runs until the server closes.
inline Exchange exchange(EventLoop::Reactor& reactor, int client, const std::string& payload,
                         bool halfClose = false) {
    Exchange result;
    std::size_t sent = 0;
    bool shutDown = false;

    for (int iteration = 0; iteration < 20000; ++iteration) {
        if (sent < payload.size()) {
            const ssize_t n =
                ::send(client, payload.data() + sent, payload.size() - sent, MSG_NOSIGNAL);
            if (n > 0) {
                sent += static_cast<std::size_t>(n);
            }
        } else if (halfClose && !shutDown) {
            ::shutdown(client, SHUT_WR);
            shutDown = true;
        }

        reactor.runOnce(1);

        char buffer[16384];
        for (;;) {
            const ssize_t n = ::read(client, buffer, sizeof(buffer));
            if (n > 0) {
                result.received.append(buffer, static_cast<std::size_t>(n));
            } else {
                result.eof = (n == 0);
                break;
            }
        }

        const bool complete = result.received.size() >= payload.size();
        if (halfClose ? result.eof : complete) {
            break;
        }
    }
    return result;
}

/// Steps the reactor until `done()` or the iteration budget runs out.
template <typename Predicate> bool pumpUntil(EventLoop::Reactor& reactor, Predicate done) {
    for (int i = 0; i < 2000 && !done(); ++i) {
        reactor.runOnce(1);
    }
    return done();
}

/// A short message is echoed back unchanged.
template <typename Server> void roundTripsMessage() {
    EventLoop::Reactor reactor;
    Server server(reactor);
    EventLoop::UniqueFd client = connectClient(server.port());

    const std::string message = "hello, reactor";
    const Exchange result = exchange(reactor, client.get(), message);

    CHK(result.received == message);
}

/// A payload far larger than the socket buffers survives backpressure.
template <typename Server> void handlesLargePayload() {
    EventLoop::Reactor reactor;
    Server server(reactor);
    EventLoop::UniqueFd client = connectClient(server.port());

    const std::string payload = makePayload(4 * 1024 * 1024);
    const Exchange result = exchange(reactor, client.get(), payload);

    CHK(result.received.size() == payload.size());
    CHK(result.received == payload);
}

/// Many simultaneous clients are each echoed correctly.
template <typename Server> void servesManyClients() {
    EventLoop::Reactor reactor;
    Server server(reactor);

    std::vector<EventLoop::UniqueFd> clients;
    for (int i = 0; i < 32; ++i) {
        clients.push_back(connectClient(server.port()));
    }
    CHK(pumpUntil(reactor, [&] { return server.connectionCount() == 32; }));

    int correct = 0;
    for (int i = 0; i < 32; ++i) {
        const std::string message = "client-" + std::to_string(i);
        if (exchange(reactor, clients[static_cast<std::size_t>(i)].get(), message).received ==
            message) {
            ++correct;
        }
    }
    CHK(correct == 32);
}

/// Queued echo data is flushed before a half-closed client is dropped.
template <typename Server> void flushesBeforeClose() {
    EventLoop::Reactor reactor;
    Server server(reactor);
    EventLoop::UniqueFd client = connectClient(server.port());

    const std::string payload = makePayload(1024 * 1024);
    const Exchange result = exchange(reactor, client.get(), payload, true);

    CHK(result.received == payload);
    CHK(result.eof);
    CHK(pumpUntil(reactor, [&] { return server.connectionCount() == 0; }));
}

/// A disconnecting client is removed from the server.
template <typename Server> void dropsClosedClient() {
    EventLoop::Reactor reactor;
    Server server(reactor);
    EventLoop::UniqueFd client = connectClient(server.port());
    CHK(pumpUntil(reactor, [&] { return server.connectionCount() == 1; }));

    client.reset();

    CHK(pumpUntil(reactor, [&] { return server.connectionCount() == 0; }));
    CHK(reactor.size() == 1); // Only the listener remains registered.
}

/// Destroying the server closes its clients' connections instead of leaking them.
template <typename Server> void closesClientsOnDestroy() {
    EventLoop::Reactor reactor;
    std::vector<EventLoop::UniqueFd> clients;
    {
        Server server(reactor);
        for (int i = 0; i < 3; ++i) {
            clients.push_back(connectClient(server.port()));
        }
        CHK(pumpUntil(reactor, [&] { return server.connectionCount() == 3; }));
    } // The server, its connections and their sockets are destroyed here.

    CHK(reactor.size() == 0);
    int sawEof = 0;
    for (EventLoop::UniqueFd& client : clients) {
        char byte;
        if (::read(client.get(), &byte, 1) == 0) {
            ++sawEof;
        }
    }
    CHK(sawEof == 3);
}

} // namespace echo
