/**
 * @file            Timeout.h
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
#include <EventLoop/Reactor.h> // Reactor — timers and the clock
#include <EventLoop/Task.h>    // Task — what is timed and what is returned

#include <chrono>              // std::chrono::ceil — duration conversion
#include <coroutine>           // std::coroutine_handle — the awaiting coroutine
#include <system_error>        // std::system_error, std::errc — the timeout error
#include <type_traits>         // std::is_void_v — void vs value results
// clang-format on

namespace EventLoop {

/// Thrown by withTimeout() when the time limit passes first. It is a
/// `std::system_error` carrying `std::errc::timed_out`.
class TimeoutError : public std::system_error {
  public:
    TimeoutError()
        : std::system_error(std::make_error_code(std::errc::timed_out), "operation timed out") {}
};

namespace detail {

/**
 * @brief The awaiter inside withTimeout(): runs a child Task against a timer.
 *
 * @details Lives in withTimeout's coroutine frame, next to the child Task it
 * refers to. Two things can end the wait, and exactly one of them does:
 *  - the child finishes, which resumes the parent through the child's
 *    continuation; await_resume then cancels the timer; or
 *  - the timer fires: the child is cancelled (its frame destroyed wherever it
 *    is suspended) and the parent is resumed with the timeout recorded.
 *
 * Both run on the reactor's thread and never at the same moment, because a
 * timer callback only runs while no coroutine executes. If the parent is
 * destroyed while waiting, the destructor cancels the timer and the child is
 * destroyed with the frame, so nothing is left to resume.
 *
 * On an exact tie the child wins: it armed its own wait before this timer was
 * armed, so its timer sorts first.
 */
template <typename T> class TimeoutAwaiter {
  public:
    TimeoutAwaiter(Reactor& reactor, Task<T>& child, Reactor::Clock::time_point deadline) noexcept
        : reactor_(reactor), child_(child), deadline_(deadline) {}

    TimeoutAwaiter(const TimeoutAwaiter&) = delete;
    TimeoutAwaiter& operator=(const TimeoutAwaiter&) = delete;

    ~TimeoutAwaiter() {
        cancelTimer();
    }

    /// Starts the child. If it finishes without ever suspending, no wait is needed.
    [[nodiscard]] bool await_ready() {
        if (!child_.valid()) {
            throw std::logic_error("withTimeout: empty Task");
        }
        child_.start();
        return child_.done();
    }

    void await_suspend(std::coroutine_handle<> parent) {
        parent_ = parent;
        // Arm the timer first: if that throws, the child's continuation has
        // not been touched yet.
        timer_ = reactor_.at(deadline_, [this] { onTimeout(); });
        child_.handle_.promise().continuation = parent;
    }

    /// Yields the child's result, rethrows its exception, or throws TimeoutError.
    T await_resume() {
        cancelTimer();
        if (timedOut_) {
            throw TimeoutError();
        }
        return child_.result();
    }

  private:
    void cancelTimer() noexcept {
        if (timer_) {
            reactor_.cancelTimer(timer_);
            timer_ = {};
        }
    }

    /// Timer callback: the limit passed with the child still suspended.
    void onTimeout() {
        timer_ = {}; // This timer has fired; there is nothing left to cancel.
        timedOut_ = true;
        child_.cancel();
        parent_.resume();
    }

    Reactor& reactor_;                    ///< Where the timer lives.
    Task<T>& child_;                      ///< The timed Task, owned by the parent's frame.
    Reactor::Clock::time_point deadline_; ///< When the limit passes.
    std::coroutine_handle<> parent_{};    ///< The coroutine waiting on this.
    Reactor::TimerId timer_{};            ///< The armed timeout, if suspended.
    bool timedOut_ = false;               ///< The timer won.
};

} // namespace detail

/**
 * @brief Runs `task` with a time limit.
 *
 * @details The task starts when the returned Task is first awaited, and the
 * limit counts from then. If the task finishes first, its result (or
 * exception) is returned. If the limit passes first, the task is **cancelled**
 * (destroyed where it is suspended, so its locals are destroyed and any I/O
 * it was waiting on is deregistered) and `TimeoutError` is thrown.
 *
 * It cannot interrupt a task that never suspends: the limit is checked when
 * control returns to the reactor. A task that finishes without ever
 * suspending succeeds even with a zero limit.
 *
 * On a tie between the task's own wait and the limit, the task wins; and if
 * an I/O completion lands in the same loop iteration as the timeout, the
 * completion is handled first and cancels the timeout.
 *
 * @param reactor The reactor providing the timer; must outlive the result.
 * @param task The work to run. Owned by the returned Task.
 * @param timeout The limit. Zero or negative means "only if it needs no waiting".
 * @throws TimeoutError when the limit passes first.
 */
template <typename T, typename Rep, typename Period>
Task<T> withTimeout(Reactor& reactor, Task<T> task, std::chrono::duration<Rep, Period> timeout) {
    const Reactor::Clock::time_point deadline =
        reactor.now() + std::chrono::ceil<Reactor::Clock::duration>(timeout);
    detail::TimeoutAwaiter<T> awaiter(reactor, task, deadline);
    if constexpr (std::is_void_v<T>) {
        co_await awaiter;
    } else {
        co_return co_await awaiter;
    }
}

} // namespace EventLoop
