/**
 * @file            Socket.h
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
#include <EventLoop/Endpoint.h> // Endpoint — where to listen or connect
#include <EventLoop/Fd.h>       // UniqueFd — owns the descriptor
#include <EventLoop/Reactor.h>  // Reactor — readiness source and timers
#include <EventLoop/Task.h>     // Task — return type of connect()

#include <chrono>               // std::chrono::duration, ceil — operation timeouts
#include <coroutine>            // std::coroutine_handle — suspended operation
#include <cstddef>              // std::byte, std::size_t — buffers and counts
#include <memory>               // std::shared_ptr — shared per-socket state
#include <optional>             // std::optional — the optional time limit
#include <span>                 // std::span — non-owning buffer views
#include <sys/socket.h>         // SHUT_WR — shutdown direction
// clang-format on

namespace EventLoop {

namespace detail {

struct SocketState;

/**
 * @brief Base of every socket awaitable.
 *
 * @details The awaitable tries its system call immediately (`await_ready`)
 * and suspends only if the call would block. A suspended operation is
 * retried by the socket's reactor callback whenever the descriptor signals
 * readiness; the coroutine is resumed only once the call has produced a
 * definitive outcome (data, EOF, or an error), so spurious or stale
 * readiness events never reach user code.
 *
 * An operation can also end because its time limit passed (ETIMEDOUT) or
 * because its socket was closed under it (ECANCELED). In both cases it
 * deregisters first and is resumed from the reactor loop, never inline from
 * the code that triggered it.
 *
 * Operations are not copyable or movable: once suspended, the reactor holds
 * a pointer to the object living in the coroutine frame. The destructor
 * deregisters it and cancels its timers, which is what makes destroying a
 * suspended Task safe.
 */
class SocketOp {
  public:
    SocketOp(const SocketOp&) = delete;
    SocketOp& operator=(const SocketOp&) = delete;

    /// Tries the operation. True if it finished (successfully or with an error).
    [[nodiscard]] bool await_ready() noexcept;

    /// Registers the operation to be retried on readiness, and arms its timers.
    /// @throws std::logic_error if another coroutine already waits in the same direction.
    void await_suspend(std::coroutine_handle<> handle);

  protected:
    enum class Direction { Read, Write };

    SocketOp(std::shared_ptr<SocketState> state, Direction direction) noexcept;
    ~SocketOp();

    /// The descriptor, or -1 once the socket has been closed.
    [[nodiscard]] int descriptor() const noexcept;

    /// The reactor the socket is registered with.
    [[nodiscard]] Reactor* reactor() const noexcept;

    /// Sets a limit on how long the operation may stay suspended.
    void setTimeout(Reactor::Clock::duration limit) noexcept {
        timeout_ = limit;
    }

    /// Asks for a retry after `delay` if the attempt that is running does not
    /// complete (used to back off when out of descriptors).
    void retryLater(Reactor::Clock::duration delay) noexcept {
        retryAfter_ = delay;
    }

    int error_ = 0; ///< errno of a failed operation, or 0.

  private:
    friend struct SocketState;

    /// Performs the system call. Returns true when finished (including on
    /// error, recorded in `error_`) and false when it would block.
    [[nodiscard]] virtual bool attempt() noexcept = 0;

    /// attempt(), except that a closed socket finishes the operation with ECANCELED.
    [[nodiscard]] bool poll() noexcept;

    /// Ends a waiting operation: deregisters it, cancels its timers, resumes the coroutine.
    void finish(SocketOp*& slot);

    /// The time limit passed while the operation was waiting.
    void onTimeout();

    /// Arms the retry timer if the last attempt asked for one and none is pending.
    void armRetry();

    /// Arms the timers an operation needs while suspended.
    void armTimers();

    /// Cancels every timer this operation armed.
    void cancelTimers() noexcept;

    /// The socket's waiter slot for this operation's direction.
    [[nodiscard]] SocketOp*& slot() noexcept;

    std::shared_ptr<SocketState> state_; ///< Keeps the shared state alive until destruction.
    std::coroutine_handle<> handle_{};   ///< The suspended coroutine.
    std::optional<Reactor::Clock::duration> timeout_{}; ///< Limit on the wait, if any.
    Reactor::Clock::duration retryAfter_{}; ///< Back-off requested by the last attempt, if any.
    Reactor::TimerId timer_{};              ///< The armed time limit, if any.
    Reactor::TimerId retryTimer_{};         ///< The armed back-off retry, if any.
    Direction direction_;                   ///< Which readiness this operation waits for.
    bool registered_ = false;               ///< Currently the waiter of its direction.
    bool yielded_ = false; ///< Suspended to give other work a turn, not because it would block.
};

/// Adds `within()` to an operation, returning the concrete type so the
/// result can be awaited directly.
template <typename Derived> class TimedOp : public SocketOp {
  public:
    /**
     * @brief Fails the operation with `ETIMEDOUT` if it stays suspended longer
     * than `timeout`.
     *
     * @details Usable only on the temporary returned by `Socket::read()` and
     * friends: `co_await socket.read(buffer).within(200ms)`. The limit
     * counts from the moment the operation suspends. If the operation could
     * complete at the moment the limit passes (data arrived but its event has
     * not been handled yet) it completes normally instead of timing out. A
     * timed-out operation leaves the socket usable.
     */
    template <typename Rep, typename Period>
    [[nodiscard]] Derived&& within(std::chrono::duration<Rep, Period> timeout) && noexcept {
        setTimeout(std::chrono::ceil<Reactor::Clock::duration>(timeout));
        return static_cast<Derived&&>(*this);
    }

  protected:
    using SocketOp::SocketOp;
};

} // namespace detail

/**
 * @brief Non-blocking stream socket with awaitable operations.
 *
 * @details
 * ```cpp
 * Socket client = co_await Socket::connect(reactor, Endpoint::loopback(7000));
 * std::size_t n = co_await client.read(buffer).within(200ms);   // 0 means the peer closed
 * co_await client.writeAll(std::as_bytes(std::span(message)));
 * ```
 *
 * **Operations.** `read`, `write`, `writeAll` and `accept` each try the
 * system call first and suspend only on EAGAIN. Failures are thrown as
 * `std::system_error` at the `co_await`. At most one coroutine may wait for
 * reading and one for writing on a socket at the same time (a reader and a
 * writer can run concurrently); a second waiter in the same direction gets
 * `std::logic_error`.
 *
 * **Time limits.** `.within(duration)` on an operation fails it with
 * `ETIMEDOUT` (`std::errc::timed_out`) if it stays suspended too long. For a
 * limit on a whole task use `withTimeout()` (Timeout.h).
 *
 * **Closing under a pending operation.** `close()` or destroying the socket
 * wakes every operation still waiting on it with `ECANCELED`
 * (`std::errc::operation_canceled`). The wake comes from the reactor loop,
 * never inline from the destructor, so it is safe to close from anywhere.
 *
 * **Readiness.** The socket registers with its Reactor once, edge-triggered
 * for both directions. This is safe because an operation always tries the
 * system call before waiting, so data that arrived earlier is never missed.
 *
 * **Fairness.** A socket whose operations keep completing immediately would
 * never yield to the rest of the loop. After 16 consecutive immediate
 * completions the next operation suspends and is retried through
 * `Reactor::defer()`, letting other ready work run first.
 *
 * **Lifetimes.** See docs/Sockets.md. The Reactor must outlive every Socket
 * and every operation on one. Destroying a suspended Task is safe because
 * every operation deregisters itself and cancels its timers.
 */
class Socket {
  public:
    class ReadOp;
    class WriteOp;
    class WriteAllOp;
    class AcceptOp;

    /// @brief Constructs an empty socket.
    Socket() noexcept = default;

    /// @brief Closes the socket (see close()).
    ~Socket();

    Socket(const Socket&) = delete;
    Socket& operator=(const Socket&) = delete;

    Socket(Socket&& other) noexcept = default;
    Socket& operator=(Socket&& other) noexcept;

    /**
     * @brief Creates a listening TCP socket bound to `endpoint`.
     * @param backlog Pending-connection queue length.
     * @throws std::system_error if the socket cannot be created, bound or registered.
     */
    [[nodiscard]] static Socket listen(Reactor& reactor, const Endpoint& endpoint,
                                       int backlog = 128);

    /**
     * @brief Takes ownership of an existing stream descriptor, switching it
     * to non-blocking mode and registering it with `reactor`.
     * @throws std::system_error if the descriptor is invalid or cannot be registered.
     */
    [[nodiscard]] static Socket adopt(Reactor& reactor, UniqueFd fd);

    /**
     * @brief Connects a new TCP socket to `endpoint`.
     * @return A task yielding the connected socket. Failure (refused,
     * unreachable, ...) is thrown as `std::system_error` when awaited. To
     * limit how long it may take, wrap it in `withTimeout()`; cancelling it
     * closes the half-open socket.
     * @note `reactor` must outlive the returned task and the socket.
     */
    [[nodiscard]] static Task<Socket> connect(Reactor& reactor, Endpoint endpoint);

    /// @brief True if this object owns a socket.
    [[nodiscard]] bool valid() const noexcept {
        return static_cast<bool>(state_);
    }

    /// @brief The descriptor, or -1 if empty.
    [[nodiscard]] int fd() const noexcept {
        return fd_.get();
    }

    /// @brief The local address the socket is bound to.
    /// @throws std::system_error on failure or if the socket is empty.
    [[nodiscard]] Endpoint localEndpoint() const;

    /**
     * @brief Reads up to `buffer.size()` bytes.
     * @return Awaitable yielding the byte count; 0 means the peer closed its sending side.
     * @throws std::invalid_argument if `buffer` is empty (0 would be indistinguishable from EOF).
     * @note `buffer` must stay valid until the operation completes.
     */
    [[nodiscard]] ReadOp read(std::span<std::byte> buffer);

    /**
     * @brief Writes some of `data`.
     * @return Awaitable yielding how many bytes were accepted (possibly fewer than `data.size()`).
     * @note `data` must stay valid until the operation completes.
     */
    [[nodiscard]] WriteOp write(std::span<const std::byte> data);

    /**
     * @brief Writes all of `data`, suspending as often as needed.
     * @note `data` must stay valid until the operation completes. With
     * `.within()`, the limit covers the whole write, not each pause.
     */
    [[nodiscard]] WriteAllOp writeAll(std::span<const std::byte> data);

    /**
     * @brief Accepts one connection from a listening socket.
     * @return Awaitable yielding the new, registered Socket.
     * @note If the process is out of descriptors the operation does not
     * fail: it retries every 100 ms until one is free (or its time limit
     * passes).
     */
    [[nodiscard]] AcceptOp accept();

    /// @brief Shuts down one or both directions (`SHUT_RD`, `SHUT_WR`, `SHUT_RDWR`).
    /// @throws std::system_error on failure (a not-connected socket is not an error).
    void shutdown(int how);

    /// @brief Signals end-of-stream to the peer while still allowing reads.
    void shutdownWrite() {
        shutdown(SHUT_WR);
    }

    /// @brief Deregisters and closes the socket. Operations still pending on
    /// it complete with ECANCELED, resumed from the reactor loop. Does
    /// nothing if empty.
    void close() noexcept;

  private:
    /// Registers an already non-blocking descriptor with `reactor`.
    Socket(Reactor& reactor, UniqueFd fd);

    UniqueFd fd_; ///< The descriptor.
    std::shared_ptr<detail::SocketState>
        state_; ///< State shared with the reactor callback and operations.
};

/// Awaitable returned by Socket::read().
class Socket::ReadOp final : public detail::TimedOp<Socket::ReadOp> {
  public:
    /// @throws std::system_error if the read failed or timed out.
    [[nodiscard]] std::size_t await_resume() const;

  private:
    friend class Socket;
    ReadOp(std::shared_ptr<detail::SocketState> state, std::span<std::byte> buffer) noexcept;
    [[nodiscard]] bool attempt() noexcept override;

    std::span<std::byte> buffer_; ///< Destination.
    std::size_t transferred_ = 0; ///< Bytes read.
};

/// Awaitable returned by Socket::write().
class Socket::WriteOp final : public detail::TimedOp<Socket::WriteOp> {
  public:
    /// @throws std::system_error if the write failed or timed out.
    [[nodiscard]] std::size_t await_resume() const;

  private:
    friend class Socket;
    WriteOp(std::shared_ptr<detail::SocketState> state, std::span<const std::byte> data) noexcept;
    [[nodiscard]] bool attempt() noexcept override;

    std::span<const std::byte> data_; ///< Source.
    std::size_t transferred_ = 0;     ///< Bytes written.
};

/// Awaitable returned by Socket::writeAll().
class Socket::WriteAllOp final : public detail::TimedOp<Socket::WriteAllOp> {
  public:
    /// @throws std::system_error if the write failed or timed out.
    void await_resume() const;

  private:
    friend class Socket;
    WriteAllOp(std::shared_ptr<detail::SocketState> state,
               std::span<const std::byte> data) noexcept;
    [[nodiscard]] bool attempt() noexcept override;

    std::span<const std::byte> data_; ///< Source.
    std::size_t written_ = 0;         ///< Progress; kept across retries.
};

/// Awaitable returned by Socket::accept().
class Socket::AcceptOp final : public detail::TimedOp<Socket::AcceptOp> {
  public:
    /// @throws std::system_error if accept failed or timed out.
    [[nodiscard]] Socket await_resume();

  private:
    friend class Socket;
    explicit AcceptOp(std::shared_ptr<detail::SocketState> state) noexcept;
    [[nodiscard]] bool attempt() noexcept override;

    UniqueFd accepted_; ///< The accepted descriptor, until handed over.
};

} // namespace EventLoop
