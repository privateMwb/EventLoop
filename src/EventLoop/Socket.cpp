/**
 * @file Socket.cpp
 * @brief Socket, its awaitable operations, and the shared per-socket state.
 *
 * Contains the reactor-facing dispatcher (SocketState), the operation base
 * class, each concrete operation, and the Socket factories.
 */

// ============================================================
// Implementation for EventLoop::Socket and detail::SocketOp.
// ============================================================
//
//  Sections:
//   1. Shared State & Dispatch
//   2. Operation Base
//   3. Operations
//   4. Socket
//
// ============================================================

// clang-format off
#include <EventLoop/Socket.h> // Socket, SocketOp — what this file implements

#include <cerrno>             // errno, EAGAIN, EINTR, ECANCELED, ETIMEDOUT, ... — system call outcomes
#include <chrono>             // std::chrono::milliseconds — accept back-off
#include <stdexcept>          // std::logic_error, std::invalid_argument — misuse reports
#include <sys/socket.h>       // socket, bind, listen, accept4, send, recv, getsockopt, ... — the socket calls
#include <system_error>       // std::system_error, std::generic_category — failure reporting
#include <utility>            // std::move — handing over ownership
// clang-format on

namespace EventLoop {

namespace detail {

namespace {

/// Consecutive immediate completions a socket may produce before the next
/// operation is forced to yield to the rest of the loop.
constexpr unsigned kImmediateBudget = 16;

} // namespace

// ============================================================
// Section 1 — Shared State & Dispatch
// ============================================================

/// State shared by a Socket, its reactor callback, and every operation on it.
/// Outlives the Socket if operations still hold it.
struct SocketState {
    Reactor* reactor = nullptr;  ///< The reactor the socket is registered with.
    int fd = -1;                 ///< The descriptor, or -1 once closed.
    SocketOp* readOp = nullptr;  ///< The coroutine waiting to read, if any.
    SocketOp* writeOp = nullptr; ///< The coroutine waiting to write, if any.
    unsigned immediate = 0;      ///< Consecutive operations that completed without waiting.

    /// Reactor callback: retries whichever operations the event concerns.
    void onEvent(Event ready) {
        immediate = 0;
        const bool failed = has(ready, Event::Error) || has(ready, Event::HangUp);
        if (failed || has(ready, Event::Readable)) {
            completeIfReady(readOp);
        }
        // The read side may have resumed code that closed the socket or
        // destroyed the writer, so the slot is read afresh here.
        if (failed || has(ready, Event::Writable)) {
            completeIfReady(writeOp);
        }
    }

    /// Deferred retry used to yield: run both waiting operations once.
    void retry() {
        immediate = 0;
        completeIfReady(readOp);
        completeIfReady(writeOp);
    }

    /// Retries the operation in `slot`; resumes its coroutine if it finished.
    void completeIfReady(SocketOp*& slot);

    /// Marks the socket closed. Waiting operations stay registered; the next
    /// retry finishes them with ECANCELED (see Socket::close()).
    void markClosed() noexcept {
        fd = -1;
    }
};

} // namespace detail

namespace detail {

// ============================================================
// Section 2 — Operation Base
// ============================================================

SocketOp::SocketOp(std::shared_ptr<SocketState> state, Direction direction) noexcept
    : state_(std::move(state)), direction_(direction) {}

SocketOp::~SocketOp() {
    cancelTimers();
    if (registered_ && state_) {
        SocketOp*& waiter = slot();
        if (waiter == this) {
            waiter = nullptr;
        }
    }
}

int SocketOp::descriptor() const noexcept {
    return state_ ? state_->fd : -1;
}

Reactor* SocketOp::reactor() const noexcept {
    return state_ ? state_->reactor : nullptr;
}

SocketOp*& SocketOp::slot() noexcept {
    return direction_ == Direction::Read ? state_->readOp : state_->writeOp;
}

bool SocketOp::poll() noexcept {
    if (state_->fd < 0) {
        error_ = ECANCELED; // The socket was closed while this was waiting.
        return true;
    }
    retryAfter_ = {};
    return attempt();
}

bool SocketOp::await_ready() noexcept {
    if (!state_ || state_->fd < 0) {
        error_ = EBADF;
        return true;
    }
    if (state_->immediate >= kImmediateBudget) {
        yielded_ = true; // Give other ready work a turn first.
        return false;
    }
    retryAfter_ = {};
    if (attempt()) {
        ++state_->immediate;
        return true;
    }
    return false;
}

void SocketOp::await_suspend(std::coroutine_handle<> handle) {
    SocketOp*& waiter = slot();
    if (waiter != nullptr) {
        throw std::logic_error("Socket: another coroutine is already waiting in this direction");
    }
    handle_ = handle;
    armTimers(); // May throw; it leaves nothing armed if it does.
    waiter = this;
    registered_ = true;
}

void SocketOp::armTimers() {
    Reactor& loop = *state_->reactor;
    try {
        if (yielded_) {
            loop.defer([state = state_] { state->retry(); });
        }
        if (timeout_) {
            timer_ = loop.after(*timeout_, [this] { onTimeout(); });
        }
        armRetry();
    } catch (...) {
        cancelTimers();
        throw;
    }
}

void SocketOp::armRetry() {
    if (retryAfter_ > Reactor::Clock::duration::zero() && !retryTimer_) {
        retryTimer_ = state_->reactor->after(retryAfter_, [this] {
            retryTimer_ = {}; // Fired: a new one may be armed if the retry still fails.
            state_->retry();
        });
    }
}

void SocketOp::cancelTimers() noexcept {
    if (state_ == nullptr || state_->reactor == nullptr) {
        return;
    }
    if (timer_) {
        state_->reactor->cancelTimer(timer_);
        timer_ = {};
    }
    if (retryTimer_) {
        state_->reactor->cancelTimer(retryTimer_);
        retryTimer_ = {};
    }
}

void SocketOp::finish(SocketOp*& waiter) {
    waiter = nullptr;
    registered_ = false;
    cancelTimers();
    const std::coroutine_handle<> handle = handle_;
    handle.resume(); // May destroy this operation, the socket, or both; nothing is touched
                     // afterwards.
}

void SocketOp::onTimeout() {
    timer_ = {}; // This timer has fired.
    SocketOp*& waiter = slot();
    if (waiter != this) {
        return; // Already finished some other way.
    }
    if (!poll()) {
        // Nothing could complete it even now. (If something could, such as
        // data that arrived but whose event is still queued, it wins.)
        error_ = ETIMEDOUT;
    }
    finish(waiter);
}

void SocketState::completeIfReady(SocketOp*& waiter) {
    SocketOp* const op = waiter;
    if (op == nullptr) {
        return;
    }
    if (!op->poll()) {
        op->armRetry(); // Out of resources: come back later instead of waiting for an edge.
        return;
    }
    op->finish(waiter);
}

} // namespace detail

// ============================================================
// Section 3 — Operations
// ============================================================

namespace {

/// How long accept() waits before retrying after running out of descriptors.
constexpr std::chrono::milliseconds kAcceptBackoff{100};

/// True for the errors that mean "try again later" rather than "failed".
bool wouldBlock(int error) noexcept {
    return error == EAGAIN || error == EWOULDBLOCK;
}

/// Throws the system_error for `error` if it is non-zero.
void throwIfFailed(int error, const char* what) {
    if (error != 0) {
        throw std::system_error(error, std::generic_category(), what);
    }
}

/// Waits for a non-blocking connect to finish. Completion is checked, not
/// assumed from the event, so spurious wakeups are harmless.
class ConnectOp final : public detail::SocketOp {
  public:
    explicit ConnectOp(std::shared_ptr<detail::SocketState> state) noexcept
        : SocketOp(std::move(state), Direction::Write) {}

    void await_resume() const {
        throwIfFailed(error_, "connect");
    }

  private:
    [[nodiscard]] bool attempt() noexcept override {
        int pending = 0;
        socklen_t length = sizeof(pending);
        if (::getsockopt(descriptor(), SOL_SOCKET, SO_ERROR, &pending, &length) < 0) {
            error_ = errno;
            return true;
        }
        if (pending != 0) {
            error_ = pending;
            return true;
        }
        sockaddr_storage peer{};
        socklen_t peerLength = sizeof(peer);
        if (::getpeername(descriptor(), reinterpret_cast<sockaddr*>(&peer), &peerLength) == 0) {
            return true; // Connected.
        }
        if (errno == ENOTCONN) {
            return false; // Still in progress.
        }
        error_ = errno;
        return true;
    }
};

} // namespace

Socket::ReadOp::ReadOp(std::shared_ptr<detail::SocketState> state,
                       std::span<std::byte> buffer) noexcept
    : TimedOp(std::move(state), Direction::Read), buffer_(buffer) {}

bool Socket::ReadOp::attempt() noexcept {
    for (;;) {
        const ssize_t received = ::recv(descriptor(), buffer_.data(), buffer_.size(), 0);
        if (received >= 0) {
            transferred_ = static_cast<std::size_t>(received);
            return true;
        }
        if (errno == EINTR) {
            continue;
        }
        if (wouldBlock(errno)) {
            return false;
        }
        error_ = errno;
        return true;
    }
}

std::size_t Socket::ReadOp::await_resume() const {
    throwIfFailed(error_, "read");
    return transferred_;
}

Socket::WriteOp::WriteOp(std::shared_ptr<detail::SocketState> state,
                         std::span<const std::byte> data) noexcept
    : TimedOp(std::move(state), Direction::Write), data_(data) {}

bool Socket::WriteOp::attempt() noexcept {
    for (;;) {
        const ssize_t sent = ::send(descriptor(), data_.data(), data_.size(), MSG_NOSIGNAL);
        if (sent >= 0) {
            transferred_ = static_cast<std::size_t>(sent);
            return true;
        }
        if (errno == EINTR) {
            continue;
        }
        if (wouldBlock(errno)) {
            return false;
        }
        error_ = errno;
        return true;
    }
}

std::size_t Socket::WriteOp::await_resume() const {
    throwIfFailed(error_, "write");
    return transferred_;
}

Socket::WriteAllOp::WriteAllOp(std::shared_ptr<detail::SocketState> state,
                               std::span<const std::byte> data) noexcept
    : TimedOp(std::move(state), Direction::Write), data_(data) {}

bool Socket::WriteAllOp::attempt() noexcept {
    while (written_ < data_.size()) {
        const ssize_t sent =
            ::send(descriptor(), data_.data() + written_, data_.size() - written_, MSG_NOSIGNAL);
        if (sent >= 0) {
            written_ += static_cast<std::size_t>(sent);
        } else if (errno == EINTR) {
            continue;
        } else if (wouldBlock(errno)) {
            return false; // Progress so far is kept for the retry.
        } else {
            error_ = errno;
            return true;
        }
    }
    return true;
}

void Socket::WriteAllOp::await_resume() const {
    throwIfFailed(error_, "write");
}

Socket::AcceptOp::AcceptOp(std::shared_ptr<detail::SocketState> state) noexcept
    : TimedOp(std::move(state), Direction::Read) {}

bool Socket::AcceptOp::attempt() noexcept {
    for (;;) {
        const int accepted =
            ::accept4(descriptor(), nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
        if (accepted >= 0) {
            accepted_ = UniqueFd(accepted);
            return true;
        }
        if (errno == EINTR || errno == ECONNABORTED) {
            continue; // The connection vanished before we got it: take the next.
        }
        if (wouldBlock(errno)) {
            return false; // Nothing to accept: wait for the next connection.
        }
        if (errno == EMFILE || errno == ENFILE || errno == ENOBUFS || errno == ENOMEM) {
            // Out of descriptors or memory. Failing would drop the listener and
            // spinning would starve the loop, so back off and try again; the
            // pending connection is still queued in the kernel.
            retryLater(kAcceptBackoff);
            return false;
        }
        error_ = errno;
        return true;
    }
}

Socket Socket::AcceptOp::await_resume() {
    throwIfFailed(error_, "accept");
    return Socket(*reactor(), std::move(accepted_));
}

// ============================================================
// Section 4 — Socket
// ============================================================

Socket::Socket(Reactor& reactor, UniqueFd fd) : fd_(std::move(fd)) {
    auto state = std::make_shared<detail::SocketState>();
    state->reactor = &reactor;
    state->fd = fd_.get();

    // One edge-triggered registration for both directions. Operations always
    // try their system call first, so an edge that arrives while nobody waits
    // loses nothing.
    reactor.add(fd_.get(), Event::Readable | Event::Writable | Event::Edge,
                [state](int, Event ready) { state->onEvent(ready); });
    state_ = std::move(state);
}

Socket::~Socket() {
    close();
}

Socket& Socket::operator=(Socket&& other) noexcept {
    if (this != &other) {
        close();
        fd_ = std::move(other.fd_);
        state_ = std::move(other.state_);
    }
    return *this;
}

void Socket::close() noexcept {
    if (!state_) {
        return;
    }
    // Stop the reactor delivering events for this descriptor, then close it.
    state_->markClosed();
    try {
        state_->reactor->remove(fd_.get());
    } catch (...) {
        // The descriptor is closed next regardless; nothing useful to do.
    }
    fd_.reset();

    // Operations still waiting finish with ECANCELED. They are resumed from the
    // loop rather than here: this may be a destructor, and the coroutine that
    // gets woken may be the very code that owns this object.
    if (state_->readOp != nullptr || state_->writeOp != nullptr) {
        try {
            state_->reactor->defer([state = state_] { state->retry(); });
        } catch (...) {
            // Out of memory: the waiters stay suspended until their Task is destroyed.
        }
    }
    state_.reset();
}

Socket Socket::listen(Reactor& reactor, const Endpoint& endpoint, int backlog) {
    UniqueFd fd(::socket(endpoint.family(), SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
    if (!fd.valid()) {
        throw std::system_error(errno, std::generic_category(), "socket");
    }

    const int enable = 1;
    ::setsockopt(fd.get(), SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(enable));

    if (::bind(fd.get(), endpoint.data(), endpoint.size()) < 0) {
        throw std::system_error(errno, std::generic_category(), "bind");
    }
    if (::listen(fd.get(), backlog) < 0) {
        throw std::system_error(errno, std::generic_category(), "listen");
    }
    return Socket(reactor, std::move(fd));
}

Socket Socket::adopt(Reactor& reactor, UniqueFd fd) {
    setNonBlocking(fd.get());
    return Socket(reactor, std::move(fd));
}

Task<Socket> Socket::connect(Reactor& reactor, Endpoint endpoint) {
    UniqueFd fd(::socket(endpoint.family(), SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
    if (!fd.valid()) {
        throw std::system_error(errno, std::generic_category(), "socket");
    }
    Socket socket(reactor, std::move(fd));

    if (::connect(socket.fd(), endpoint.data(), endpoint.size()) < 0 && errno != EINPROGRESS) {
        throw std::system_error(errno, std::generic_category(), "connect");
    }
    co_await ConnectOp(socket.state_);
    co_return std::move(socket);
}

Endpoint Socket::localEndpoint() const {
    if (!valid()) {
        throw std::system_error(EBADF, std::generic_category(), "localEndpoint");
    }
    sockaddr_storage address{};
    socklen_t length = sizeof(address);
    if (::getsockname(fd_.get(), reinterpret_cast<sockaddr*>(&address), &length) < 0) {
        throw std::system_error(errno, std::generic_category(), "getsockname");
    }
    return Endpoint::fromSockaddr(reinterpret_cast<const sockaddr*>(&address), length);
}

Socket::ReadOp Socket::read(std::span<std::byte> buffer) {
    if (buffer.empty()) {
        throw std::invalid_argument("Socket::read: empty buffer");
    }
    return ReadOp(state_, buffer);
}

Socket::WriteOp Socket::write(std::span<const std::byte> data) {
    return WriteOp(state_, data);
}

Socket::WriteAllOp Socket::writeAll(std::span<const std::byte> data) {
    return WriteAllOp(state_, data);
}

Socket::AcceptOp Socket::accept() {
    return AcceptOp(state_);
}

void Socket::shutdown(int how) {
    if (!valid()) {
        throw std::system_error(EBADF, std::generic_category(), "shutdown");
    }
    if (::shutdown(fd_.get(), how) < 0 && errno != ENOTCONN) {
        throw std::system_error(errno, std::generic_category(), "shutdown");
    }
}

} // namespace EventLoop
