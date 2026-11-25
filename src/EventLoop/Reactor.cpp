/**
 * @file Reactor.cpp
 * @brief Reactor implementation.
 *
 * Contains the epoll wrapper: registration, modification and removal of
 * descriptors, and the wait-and-dispatch loop.
 */

// ============================================================
// Implementation for EventLoop::Reactor.
// ============================================================
//
//  Sections:
//   1. Helpers
//   2. Constructors & Destructor
//   3. Registration
//   4. Dispatch
//
// ============================================================

// clang-format off
#include <EventLoop/Reactor.h> // Reactor — the class this file implements

#include <algorithm>           // std::min — capping the wait at the next timer
#include <array>               // std::array — the per-wait event buffer
#include <chrono>              // std::chrono::ceil, milliseconds — timer wait conversion
#include <cerrno>              // errno, EINTR, EEXIST, ENOENT, EBADF — epoll failure handling
#include <cstdint>             // std::uint64_t — packing fd and generation into epoll_event::data
#include <limits>              // std::numeric_limits — clamping the wait to int range
#include <stdexcept>           // std::invalid_argument, std::logic_error — misuse reports
#include <sys/epoll.h>         // epoll_create1, epoll_ctl, epoll_wait, EPOLL* — the kernel interface
#include <system_error>        // std::system_error, std::generic_category — failure reporting
// clang-format on

namespace EventLoop {

namespace {

/// Upper bound on events fetched per epoll_wait. Bounds stack use; anything
/// beyond it is simply delivered on the next call (level-triggered) or the
/// next wait (edge-triggered keeps it queued in the kernel).
constexpr int kMaxEvents = 64;

// ============================================================
// Section 1 — Helpers
// ============================================================

/// Builds the exception thrown for a failed system call.
std::system_error makeError(int code, const char* what) {
    return std::system_error(code, std::generic_category(), what);
}

/// Translates interest flags to the epoll bit mask.
std::uint32_t toEpoll(Event interest) noexcept {
    std::uint32_t mask = 0;
    if (has(interest, Event::Readable)) {
        mask |= EPOLLIN;
    }
    if (has(interest, Event::Writable)) {
        mask |= EPOLLOUT;
    }
    if (has(interest, Event::Edge)) {
        mask |= EPOLLET;
    }
    return mask;
}

/// Translates an epoll readiness mask to report flags.
Event fromEpoll(std::uint32_t mask) noexcept {
    Event ready = Event::None;
    if ((mask & EPOLLIN) != 0) {
        ready |= Event::Readable;
    }
    if ((mask & EPOLLOUT) != 0) {
        ready |= Event::Writable;
    }
    if ((mask & EPOLLERR) != 0) {
        ready |= Event::Error;
    }
    if ((mask & EPOLLHUP) != 0) {
        ready |= Event::HangUp;
    }
    return ready;
}

/// Packs a descriptor and its generation into epoll_event::data.
std::uint64_t packId(int fd, std::uint32_t generation) noexcept {
    return (static_cast<std::uint64_t>(generation) << 32) | static_cast<std::uint32_t>(fd);
}

} // namespace

// ============================================================
// Section 2 — Constructors & Destructor
// ============================================================

Reactor::Reactor(ClockSource clock)
    : epollFd_(::epoll_create1(EPOLL_CLOEXEC)), clock_(std::move(clock)) {
    if (!epollFd_.valid()) {
        throw makeError(errno, "epoll_create1");
    }
}

Reactor::~Reactor() = default;

// ============================================================
// Section 3 — Registration
// ============================================================

void Reactor::add(int fd, Event interest, Callback callback) {
    if (!callback) {
        throw std::invalid_argument("Reactor::add: empty callback");
    }
    if (fd < 0) {
        throw makeError(EBADF, "Reactor::add");
    }
    if (entries_.contains(fd)) {
        throw makeError(EEXIST, "Reactor::add");
    }

    const std::uint32_t generation = nextGeneration_++;

    epoll_event event{};
    event.events = toEpoll(interest);
    event.data.u64 = packId(fd, generation);
    if (::epoll_ctl(epollFd_.get(), EPOLL_CTL_ADD, fd, &event) < 0) {
        throw makeError(errno, "Reactor::add");
    }

    auto entry = std::make_shared<Entry>();
    entry->callback = std::move(callback);
    entry->generation = generation;
    entries_.emplace(fd, std::move(entry));
}

void Reactor::modify(int fd, Event interest) {
    const auto it = entries_.find(fd);
    if (it == entries_.end()) {
        throw makeError(ENOENT, "Reactor::modify");
    }

    epoll_event event{};
    event.events = toEpoll(interest);
    event.data.u64 = packId(fd, it->second->generation);
    if (::epoll_ctl(epollFd_.get(), EPOLL_CTL_MOD, fd, &event) < 0) {
        throw makeError(errno, "Reactor::modify");
    }
}

void Reactor::remove(int fd) {
    const auto it = entries_.find(fd);
    if (it == entries_.end()) {
        return;
    }

    // ENOENT/EBADF mean the descriptor was already closed, in which case the
    // kernel has dropped it from the set itself. Only the bookkeeping is left.
    if (::epoll_ctl(epollFd_.get(), EPOLL_CTL_DEL, fd, nullptr) < 0 && errno != ENOENT &&
        errno != EBADF) {
        throw makeError(errno, "Reactor::remove");
    }
    entries_.erase(it);
}

bool Reactor::contains(int fd) const noexcept {
    return entries_.contains(fd);
}

Reactor::Clock::time_point Reactor::now() {
    return clock_ ? clock_() : Clock::now();
}

Reactor::TimerId Reactor::at(Clock::time_point deadline, MoveOnlyFunction<void()> callback) {
    if (!callback) {
        throw std::invalid_argument("Reactor::at: empty callback");
    }
    return timers_.insert(deadline, std::move(callback));
}

Reactor::TimerId Reactor::after(Clock::duration delay, MoveOnlyFunction<void()> callback) {
    return at(now() + delay, std::move(callback));
}

bool Reactor::cancelTimer(TimerId id) noexcept {
    return timers_.cancel(id);
}

void Reactor::defer(MoveOnlyFunction<void()> task) {
    if (!task) {
        throw std::invalid_argument("Reactor::defer: empty callback");
    }
    deferred_.push_back(std::move(task));
}

// ============================================================
// Section 4 — Dispatch
// ============================================================

std::size_t Reactor::runOnce(int timeoutMs) {
    if (running_) {
        throw std::logic_error("Reactor::runOnce is not re-entrant");
    }
    struct RunningGuard {
        explicit RunningGuard(bool& flag) noexcept : flag_(flag) {
            flag_ = true;
        }
        ~RunningGuard() {
            flag_ = false;
        }
        RunningGuard(const RunningGuard&) = delete;
        RunningGuard& operator=(const RunningGuard&) = delete;
        bool& flag_;
    } guard(running_);

    std::array<epoll_event, kMaxEvents> events;

    // Deferred work is waiting: collect whatever is already ready, but do not
    // block. Otherwise never sleep past the earliest timer.
    int effectiveTimeout = timeoutMs;
    if (!deferred_.empty()) {
        effectiveTimeout = 0;
    } else if (const auto next = timers_.nextDeadline()) {
        const auto remaining = *next - now();
        long long waitMs = 0;
        if (remaining > Clock::duration::zero()) {
            // Round up: waking a hair early would find the timer not yet due.
            waitMs = std::chrono::ceil<std::chrono::milliseconds>(remaining).count();
        }
        waitMs = std::min<long long>(waitMs, std::numeric_limits<int>::max());
        effectiveTimeout = timeoutMs < 0 ? static_cast<int>(waitMs)
                                         : std::min(timeoutMs, static_cast<int>(waitMs));
    }
    const int count = ::epoll_wait(epollFd_.get(), events.data(), kMaxEvents, effectiveTimeout);
    if (count < 0) {
        if (errno == EINTR) {
            return 0;
        }
        throw makeError(errno, "epoll_wait");
    }

    std::size_t dispatched = 0;
    for (int i = 0; i < count; ++i) {
        const std::uint64_t id = events[static_cast<std::size_t>(i)].data.u64;
        const int fd = static_cast<int>(id & 0xFFFFFFFFu);
        const auto generation = static_cast<std::uint32_t>(id >> 32);

        // An earlier callback in this batch may have removed this descriptor,
        // or removed it and registered a new one under the same number. In
        // both cases the event is stale and must not reach anyone.
        const auto it = entries_.find(fd);
        if (it == entries_.end() || it->second->generation != generation) {
            continue;
        }

        // Hold the entry for the duration of the call: the callback may
        // remove its own registration, which would otherwise destroy the
        // very function object that is executing.
        const std::shared_ptr<Entry> entry = it->second;
        ++dispatched;
        entry->callback(fd, fromEpoll(events[static_cast<std::size_t>(i)].events));
    }

    // Timers come after I/O, so a completion that lands in the same iteration
    // as its timeout is handled first and can cancel the timer. Timers armed
    // while this pass runs wait for the next one.
    const Clock::time_point current = now();
    const std::uint64_t timerLimit = timers_.sequence();
    while (auto timer = timers_.popExpired(current, timerLimit)) {
        ++dispatched;
        (*timer)();
    }

    // Run what was queued before this point. Callbacks deferred from here on
    // wait for the next call, so a task that re-defers itself cannot starve
    // I/O. Popping one at a time leaves the rest queued if one throws.
    for (std::size_t pending = deferred_.size(); pending > 0; --pending) {
        MoveOnlyFunction<void()> task = std::move(deferred_.front());
        deferred_.pop_front();
        ++dispatched;
        task();
    }
    return dispatched;
}

void Reactor::run(int timeoutMs) {
    while (!stopRequested_) {
        runOnce(timeoutMs);
    }
    stopRequested_ = false;
}

} // namespace EventLoop
