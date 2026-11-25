/**
 * @file            Sleep.h
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

#include <chrono>             // std::chrono::ceil — duration conversion
#include <coroutine>          // std::coroutine_handle — the sleeping coroutine
// clang-format on

namespace EventLoop {

/**
 * @brief Awaitable that suspends the coroutine until a point in time.
 *
 * @details Created by sleepFor() / sleepUntil(). The timer is armed only if
 * the deadline has not already passed; a due deadline does not suspend at all.
 * The destructor cancels the timer, which is what makes destroying a sleeping
 * Task safe: after it, nothing can resume the dead frame.
 *
 * Not copyable or movable: while suspended, the reactor holds a pointer to
 * this object, which lives in the coroutine frame.
 */
class SleepOp {
  public:
    SleepOp(Reactor& reactor, Reactor::Clock::time_point deadline) noexcept
        : reactor_(reactor), deadline_(deadline) {}

    SleepOp(const SleepOp&) = delete;
    SleepOp& operator=(const SleepOp&) = delete;

    ~SleepOp() {
        if (timer_) {
            reactor_.cancelTimer(timer_); // False (and harmless) if it already fired.
        }
    }

    [[nodiscard]] bool await_ready() {
        return reactor_.now() >= deadline_;
    }

    void await_suspend(std::coroutine_handle<> handle) {
        timer_ = reactor_.at(deadline_, [handle] { handle.resume(); });
    }

    void await_resume() noexcept {
        timer_ = {};
    }

  private:
    Reactor& reactor_;                    ///< Where the timer lives.
    Reactor::Clock::time_point deadline_; ///< When to wake.
    Reactor::TimerId timer_{};            ///< The armed timer, if suspended.
};

/**
 * @brief Suspends until `duration` has passed. A zero or negative duration
 * does not suspend.
 *
 * Not a way to yield to other work: a due sleep completes immediately. Use a
 * small positive duration if that is the intent.
 */
template <typename Rep, typename Period>
[[nodiscard]] SleepOp sleepFor(Reactor& reactor, std::chrono::duration<Rep, Period> duration) {
    return SleepOp(reactor, reactor.now() + std::chrono::ceil<Reactor::Clock::duration>(duration));
}

/// @brief Suspends until `deadline`. A deadline already in the past does not suspend.
[[nodiscard]] inline SleepOp sleepUntil(Reactor& reactor, Reactor::Clock::time_point deadline) {
    return SleepOp(reactor, deadline);
}

} // namespace EventLoop
