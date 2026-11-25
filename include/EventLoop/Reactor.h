/**
 * @file            Reactor.h
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
#include <EventLoop/Event.h>              // Event — interest and readiness flags
#include <EventLoop/Fd.h>                 // UniqueFd — owns the epoll descriptor
#include <EventLoop/detail/TimerHeap.h>   // TimerHeap, TimerId, TimerClock — the timer queue

#include <FunctionPro/MoveOnlyFunction.h> // MoveOnlyFunction — type-erased, move-only callback

#include <chrono>                         // std::chrono::steady_clock — timer time base
#include <cstddef>                        // std::size_t — event counts
#include <deque>                          // std::deque — queue of deferred callbacks
#include <memory>                         // std::shared_ptr — keeps a running callback alive
#include <unordered_map>                  // std::unordered_map — fd to registration
// clang-format on

namespace EventLoop {

using namespace FunctionPro;

/**
 * @brief Single-threaded `epoll` reactor: register a descriptor, wait for
 * readiness, dispatch a callback, remove it.
 *
 * @details
 * **Threading.** A Reactor belongs to one thread. Every member, including
 * `stop()`, must be called from the thread that runs the loop (typically
 * from inside a callback). Cross-thread wakeup arrives with the
 * multi-threaded mode.
 *
 * **Level- vs edge-triggered.** Registrations are level-triggered by
 * default: while the descriptor stays ready the callback keeps firing, so a
 * handler that reads only part of the data is called again rather than
 * stalling. Add `Event::Edge` to the interest to opt in to edge-triggered
 * delivery, where the callback fires once per readiness change and must
 * drain the descriptor to `EAGAIN` itself. Edge mode costs fewer wakeups
 * but makes an unread descriptor silent forever, which is why it is opt-in.
 *
 * **Callbacks may mutate the reactor.** A callback can `remove()` its own
 * descriptor or any other, `add()` new ones, and `modify()` interest. A
 * removed descriptor never receives a late event, even from the batch
 * currently being dispatched, and a descriptor number that is closed and
 * reused inside one batch is not confused with its predecessor.
 *
 * **Timers.** `at()`/`after()` schedule a callback for a point in time and
 * return a handle for `cancelTimer()`. They run on the loop thread after the
 * events of a wait have been dispatched, so when an I/O completion and a
 * timeout fall in the same iteration the completion is processed first and
 * can cancel the timer. Timers use a binary heap, chosen over a timing
 * wheel by benchmark (docs/Timers.md). Resolution is the 1 ms of the epoll
 * timeout: a timer never fires early and may fire about a millisecond late.
 * The clock can be replaced (see the constructor) so tests control time.
 *
 * **Not re-entrant.** `runOnce()` must not be called from inside a callback,
 * timer or coroutine run by the same reactor. Cancellation relies on this:
 * a timer callback that destroys a suspended coroutine is only safe because
 * no coroutine can be executing at that moment.
 *
 * **Deferred work.** `defer()` queues a callback to run later on the loop
 * thread, after the events of the current wait have been dispatched. It is
 * how code yields to the rest of the loop without waiting for I/O, for
 * example a coroutine that has used up its fair share of immediate results.
 *
 * **Exceptions.** An exception thrown by a callback propagates out of
 * `runOnce()`/`run()`; the reactor stays consistent and can be run again.
 * Deferred callbacks not yet run stay queued.
 *
 * **Lifetime.** The reactor does not own the descriptors it watches. Call
 * `remove()` before closing one. (If a descriptor is closed first the
 * kernel drops it from the epoll set on its own, and `remove()` still
 * cleans up the bookkeeping.)
 */
class Reactor {
  public:
    /// @brief Callback invoked as `callback(fd, ready)` when `fd` is ready.
    using Callback = MoveOnlyFunction<void(int, Event)>;

    /// @brief Clock timers are measured on.
    using Clock = detail::TimerClock;

    /// @brief Handle to a scheduled timer; see cancelTimer().
    using TimerId = detail::TimerId;

    /// @brief Replacement time source, for tests that need to control time.
    using ClockSource = MoveOnlyFunction<Clock::time_point()>;

    /// @brief Creates the epoll instance.
    /// @param clock Time source for timers; empty means `Clock::now()`. A
    /// replacement clock that does not advance with real time only makes
    /// sense together with `runOnce(0)`, because waits are still real.
    /// @throws std::system_error if `epoll_create1` fails.
    explicit Reactor(ClockSource clock = {});

    ~Reactor();

    Reactor(const Reactor&) = delete;
    Reactor& operator=(const Reactor&) = delete;

    // Callbacks commonly capture `this`-style references to the reactor, so
    // it must keep a stable address.
    Reactor(Reactor&&) = delete;
    Reactor& operator=(Reactor&&) = delete;

    /**
     * @brief Starts watching `fd` for `interest`.
     * @param fd Descriptor to watch. Should already be non-blocking.
     * @param interest `Readable`/`Writable`, optionally with `Edge`.
     * @param callback Called with the descriptor and the readiness reported.
     * @throws std::invalid_argument if `callback` is empty.
     * @throws std::system_error if `fd` is invalid or already registered,
     * or if `epoll_ctl` fails.
     */
    void add(int fd, Event interest, Callback callback);

    /**
     * @brief Changes the interest of an already-registered descriptor.
     * @throws std::system_error if `fd` is not registered or `epoll_ctl` fails.
     */
    void modify(int fd, Event interest);

    /// @brief Stops watching `fd`. Does nothing if it is not registered.
    void remove(int fd);

    /// @brief True if `fd` is currently registered.
    [[nodiscard]] bool contains(int fd) const noexcept;

    /// @brief Queues `task` to run on the loop thread after the current wait's
    /// events have been dispatched. A callback deferred while deferred
    /// callbacks are running waits for the next `runOnce()`, so a task that
    /// keeps re-deferring itself cannot starve I/O. While anything is
    /// queued, waits do not block.
    /// @throws std::invalid_argument if `task` is empty.
    void defer(MoveOnlyFunction<void()> task);

    /// @brief The reactor's current time (its clock source, or `Clock::now()`).
    [[nodiscard]] Clock::time_point now();

    /**
     * @brief Runs `callback` once on the loop thread at or after `deadline`.
     * @return A handle for cancelTimer(). It stays safe to use after the timer
     * fires or is cancelled; it is then simply no longer pending.
     * @throws std::invalid_argument if `callback` is empty.
     */
    TimerId at(Clock::time_point deadline, MoveOnlyFunction<void()> callback);

    /// @brief Runs `callback` once, `delay` from now. A zero or negative delay
    /// runs it on the next `runOnce()`.
    TimerId after(Clock::duration delay, MoveOnlyFunction<void()> callback);

    /// @brief Cancels a timer. Returns false if it already fired or was cancelled.
    bool cancelTimer(TimerId id) noexcept;

    /// @brief Number of timers waiting to fire.
    [[nodiscard]] std::size_t timerCount() const noexcept {
        return timers_.size();
    }

    /// @brief Number of registered descriptors.
    [[nodiscard]] std::size_t size() const noexcept {
        return entries_.size();
    }

    /**
     * @brief Waits once and dispatches whatever is ready.
     * @param timeoutMs Milliseconds to wait; 0 polls, -1 waits indefinitely.
     * The wait never outlasts the earliest timer, so a due timer is not
     * delayed by a long or unbounded `timeoutMs`.
     * @return Number of callbacks invoked, timers and deferred ones included.
     * A wait interrupted by a signal returns 0.
     * @throws std::logic_error if called from inside this reactor's own dispatch.
     * @throws std::system_error if `epoll_wait` fails.
     */
    std::size_t runOnce(int timeoutMs = -1);

    /**
     * @brief Runs `runOnce(timeoutMs)` repeatedly until `stop()` is called.
     * @param timeoutMs Per-iteration wait. Use a finite value if the loop
     * must notice a stop request that arrives from outside a callback.
     */
    void run(int timeoutMs = -1);

    /// @brief Makes `run()` return after the current iteration finishes.
    void stop() noexcept {
        stopRequested_ = true;
    }

  private:
    /// One registration. Held by shared_ptr so a callback that removes its
    /// own descriptor keeps running on a live object.
    struct Entry {
        Callback callback;            ///< What to invoke on readiness.
        std::uint32_t generation = 0; ///< Distinguishes reuses of the same fd number.
    };

    UniqueFd epollFd_;                                        ///< The epoll instance.
    std::unordered_map<int, std::shared_ptr<Entry>> entries_; ///< Registrations by fd.
    detail::TimerHeap timers_;                                ///< Pending timers.
    ClockSource clock_;                             ///< Replacement time source, or empty.
    bool running_ = false;                          ///< Inside runOnce(); guards against re-entry.
    std::deque<MoveOnlyFunction<void()>> deferred_; ///< Callbacks waiting for the next dispatch.
    std::uint32_t nextGeneration_ = 1;              ///< Next generation to hand out.
    bool stopRequested_ = false;                    ///< Set by stop(), cleared when run() returns.
};

} // namespace EventLoop
