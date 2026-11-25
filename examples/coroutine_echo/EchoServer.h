/**
 * @file            EchoServer.h
 *
 * @date            2026-11-24
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
#include <EventLoop/EventLoop.h> // Reactor, Socket, Task, Endpoint — the runtime this server is written on

#include <array>                 // std::array — per-connection buffer
#include <cstddef>               // std::byte, std::size_t — buffer and counts
#include <cstdint>               // std::uint16_t, std::uint64_t — port and connection ids
#include <span>                  // std::span — views of the buffer
#include <system_error>          // std::system_error — connection failures
#include <unordered_map>         // std::unordered_map — owned connection tasks
#include <utility>               // std::move — handing sockets to their tasks
#include <vector>                // std::vector — finished connection ids
// clang-format on

namespace CoroEchoExample {

using namespace EventLoop;

/**
 * @brief The Phase 1 echo server rewritten with coroutines. Same observable
 * behaviour, but the handler reads like blocking code.
 *
 * @details Each connection is one `Task` that loops `read`, `writeAll`.
 * Backpressure needs no code of its own: while a slow client has not taken
 * the echo, `writeAll` is suspended, so the loop is not reading, so at most
 * one buffer per connection is ever held. A half-closed client gets its echo
 * flushed before the connection is closed, because every read is echoed in
 * full before the next one.
 *
 * Ownership: the server owns every connection Task (`start()`ed, not
 * `spawn`ed), so destroying the server destroys suspended connections
 * cleanly. A Task cannot destroy itself while running, so a finished
 * connection is queued and its frame is reaped by the accept loop on its
 * next pass (and by the destructor).
 *
 * The server must be destroyed before the Reactor it was given.
 */
class EchoServer {
  public:
    /// @brief Listens on 127.0.0.1:`port`. Port 0 picks a free port; see port().
    /// @throws std::system_error if the socket cannot be created, bound or registered.
    explicit EchoServer(Reactor& reactor, std::uint16_t port = 0)
        : listener_(Socket::listen(reactor, Endpoint::loopback(port))) {
        acceptTask_ = acceptLoop();
        acceptTask_.start();
    }

    ~EchoServer() {
        // Destroy the acceptor first so nothing starts new connections while
        // the rest are torn down. Each suspended Task releases its Socket.
        acceptTask_ = Task<void>{};
        connections_.clear();
    }

    EchoServer(const EchoServer&) = delete;
    EchoServer& operator=(const EchoServer&) = delete;

    /// @brief The port actually bound.
    [[nodiscard]] std::uint16_t port() const {
        return listener_.localEndpoint().port();
    }

    /// @brief Number of open client connections.
    [[nodiscard]] std::size_t connectionCount() const noexcept {
        return active_;
    }

  private:
    /// Accepts connections forever, giving each its own Task.
    Task<void> acceptLoop() {
        for (;;) {
            Socket client = co_await listener_.accept();
            reapFinished();

            const std::uint64_t id = nextId_++;
            ++active_;
            Task<void>& task = connections_.emplace(id, serve(std::move(client), id)).first->second;
            task.start();
        }
    }

    /// Echoes one connection until it ends. `client` and `id` are taken by
    /// value because the frame must own them (docs/Lifetimes.md, rule 3).
    Task<void> serve(Socket client, std::uint64_t id) {
        std::array<std::byte, 8192> buffer;
        try {
            for (;;) {
                const std::size_t received = co_await client.read(buffer);
                if (received == 0) {
                    break; // The peer finished sending; everything it sent is already echoed.
                }
                co_await client.writeAll(std::span<const std::byte>(buffer).first(received));
            }
        } catch (const std::system_error&) {
            // Reset by the peer or similar: just close.
        }

        // A finished frame keeps its parameters alive until it is reaped, so
        // close explicitly instead of waiting for `client` to be destroyed.
        client.close();
        --active_;
        finished_.push_back(id);
    }

    /// Destroys the frames of connections that have finished. Only called
    /// from the accept loop, never from inside a connection's own frame.
    void reapFinished() {
        for (const std::uint64_t id : finished_) {
            connections_.erase(id);
        }
        finished_.clear();
    }

    Socket listener_;                                           ///< The listening socket.
    Task<void> acceptTask_;                                     ///< The accept loop.
    std::unordered_map<std::uint64_t, Task<void>> connections_; ///< Connection tasks by id.
    std::vector<std::uint64_t> finished_; ///< Ids of finished connections awaiting reaping.
    std::uint64_t nextId_ = 1;            ///< Next connection id.
    std::size_t active_ = 0;              ///< Connections currently open.
};

} // namespace CoroEchoExample
