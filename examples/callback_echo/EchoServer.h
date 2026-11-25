/**
 * @file            EchoServer.h
 *
 * @date            2026-11-23
 *
 * @version         0.1.0
 *
 * @copyright       Copyright (c) 2026 privateMwb
 *                  All rights reserved.
 *                  https://github.com/privateMwb/EventLoop
 *
 * @attention       This source is released under the MIT license
 *                  SPDX-License-Identifier: MIT
 *                  <http://opensource.org/licenses/MIT>
 */

#pragma once

// clang-format off
#include <EventLoop/EventLoop.h> // Reactor, Event, UniqueFd, setNonBlocking — the reactor this server runs on

#include <arpa/inet.h>           // inet_pton, htons, ntohs — loopback address and port conversion
#include <cerrno>                // errno, EINTR, EAGAIN, ECONNABORTED — syscall result handling
#include <cstdint>               // std::uint16_t — port type
#include <netinet/in.h>          // sockaddr_in, INADDR_LOOPBACK — IPv4 socket address
#include <string>                // std::string — per-connection write backlog
#include <sys/socket.h>          // socket, bind, listen, accept4, recv, send — the socket calls
#include <system_error>          // std::system_error — setup failures
#include <unordered_map>         // std::unordered_map — live connections by fd
#include <utility>               // std::move — handing a descriptor to its connection
// clang-format on

namespace EchoExample {

using namespace EventLoop;

/**
 * @brief Plain echo server built only on Reactor callbacks. No coroutines.
 *
 * @details This is the Phase 1 proof that the core loop (register, wait,
 * dispatch, remove) works, and the behavioural reference the coroutine
 * rewrite in Phase 3 must match.
 *
 * Behaviour: bytes read from a client are written straight back. If the
 * socket cannot take them all, the rest is queued and Writable interest is
 * switched on; while the queue is large the server stops reading from that
 * client (backpressure) so a slow reader cannot make memory grow without
 * bound. When a client half-closes, the queue is flushed before the
 * connection is closed.
 *
 * The server must be destroyed before the Reactor it was given.
 */
class EchoServer {
  public:
    /// @brief Listens on 127.0.0.1:`port`. Port 0 picks a free port; see port().
    /// @throws std::system_error if the socket cannot be created, bound or registered.
    explicit EchoServer(Reactor& reactor, std::uint16_t port = 0) : reactor_(reactor) {
        UniqueFd listener(::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
        if (!listener.valid()) {
            throw std::system_error(errno, std::generic_category(), "socket");
        }

        const int enable = 1;
        ::setsockopt(listener.get(), SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(enable));

        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_port = htons(port);
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        if (::bind(listener.get(), reinterpret_cast<sockaddr*>(&address), sizeof(address)) < 0 ||
            ::listen(listener.get(), 128) < 0) {
            throw std::system_error(errno, std::generic_category(), "bind/listen");
        }

        socklen_t length = sizeof(address);
        ::getsockname(listener.get(), reinterpret_cast<sockaddr*>(&address), &length);
        port_ = ntohs(address.sin_port);

        listener_ = std::move(listener);
        reactor_.add(listener_.get(), Event::Readable, [this](int, Event) { onAccept(); });
    }

    ~EchoServer() {
        for (const auto& [fd, connection] : connections_) {
            reactor_.remove(fd);
        }
        reactor_.remove(listener_.get());
    }

    EchoServer(const EchoServer&) = delete;
    EchoServer& operator=(const EchoServer&) = delete;

    /// @brief The port actually bound.
    [[nodiscard]] std::uint16_t port() const noexcept {
        return port_;
    }

    /// @brief Number of open client connections.
    [[nodiscard]] std::size_t connectionCount() const noexcept {
        return connections_.size();
    }

  private:
    /// Stop reading from a client once this many bytes are queued for it.
    static constexpr std::size_t kBackpressureLimit = 1 << 20;
    /// Read calls per readiness event, so one fast client cannot starve the rest.
    static constexpr int kReadsPerEvent = 16;

    struct Connection {
        UniqueFd fd;                      ///< The client socket.
        std::string pending;              ///< Bytes read but not yet written back.
        bool peerClosed = false;          ///< The client sent EOF.
        Event interest = Event::Readable; ///< Interest currently registered.
    };

    /// Accepts every pending connection. Level-triggered, so anything left
    /// over simply triggers another call.
    void onAccept() {
        for (;;) {
            const int clientFd =
                ::accept4(listener_.get(), nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
            if (clientFd < 0) {
                if (errno == EINTR || errno == ECONNABORTED) {
                    continue;
                }
                // EAGAIN: drained. Anything else (e.g. EMFILE): give up for
                // now. Level-triggered delivery will retry on the next wait.
                return;
            }

            UniqueFd owned(clientFd);
            reactor_.add(clientFd, Event::Readable,
                         [this](int fd, Event ready) { onConnection(fd, ready); });
            connections_.emplace(clientFd, Connection{std::move(owned)});
        }
    }

    void onConnection(int fd, Event ready) {
        const auto it = connections_.find(fd);
        if (it == connections_.end()) {
            return;
        }
        Connection& connection = it->second;

        if (has(ready, Event::Error)) {
            closeConnection(fd);
            return;
        }
        if (has(ready, Event::Writable) && !flush(connection)) {
            closeConnection(fd);
            return;
        }
        if ((has(ready, Event::Readable) || has(ready, Event::HangUp)) && !connection.peerClosed &&
            connection.pending.size() < kBackpressureLimit && !readAndEcho(connection)) {
            closeConnection(fd);
            return;
        }
        if (connection.peerClosed && connection.pending.empty()) {
            closeConnection(fd);
            return;
        }
        updateInterest(fd, connection);
    }

    /// Writes as much of the backlog as the socket accepts.
    /// @return false on a fatal socket error.
    bool flush(Connection& connection) {
        while (!connection.pending.empty()) {
            const ssize_t sent = ::send(connection.fd.get(), connection.pending.data(),
                                        connection.pending.size(), MSG_NOSIGNAL);
            if (sent > 0) {
                connection.pending.erase(0, static_cast<std::size_t>(sent));
            } else if (sent < 0 && errno == EINTR) {
                continue;
            } else if (sent < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                return true;
            } else {
                return false;
            }
        }
        return true;
    }

    /// Reads a bounded number of chunks and echoes them.
    /// @return false on a fatal socket error.
    bool readAndEcho(Connection& connection) {
        char buffer[16384];
        for (int i = 0; i < kReadsPerEvent; ++i) {
            const ssize_t received = ::recv(connection.fd.get(), buffer, sizeof(buffer), 0);
            if (received > 0) {
                connection.pending.append(buffer, static_cast<std::size_t>(received));
                if (!flush(connection)) {
                    return false;
                }
                if (connection.pending.size() >= kBackpressureLimit) {
                    return true;
                }
            } else if (received == 0) {
                connection.peerClosed = true;
                return true;
            } else if (errno == EINTR) {
                continue;
            } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
                return true;
            } else {
                return false;
            }
        }
        return true;
    }

    /// Re-registers interest to match the connection's current state.
    void updateInterest(int fd, Connection& connection) {
        Event desired = Event::None;
        if (!connection.peerClosed && connection.pending.size() < kBackpressureLimit) {
            desired |= Event::Readable;
        }
        if (!connection.pending.empty()) {
            desired |= Event::Writable;
        }
        if (desired != connection.interest) {
            reactor_.modify(fd, desired);
            connection.interest = desired;
        }
    }

    void closeConnection(int fd) {
        reactor_.remove(fd);
        connections_.erase(fd);
    }

    Reactor& reactor_;                                ///< The reactor this server runs on.
    UniqueFd listener_;                               ///< The listening socket.
    std::uint16_t port_ = 0;                          ///< The port actually bound.
    std::unordered_map<int, Connection> connections_; ///< Open clients by fd.
};

} // namespace EchoExample
