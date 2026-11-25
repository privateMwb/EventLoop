#pragma once

// A clock the test advances by hand. Pass `source()` to the Reactor
// constructor so timers, sleeps and timeouts follow this clock instead of
// real time. Combine with `runOnce(0)`: waits are still real, only the
// timers' notion of "now" is fake.
//
// Must outlive the Reactor that uses it.

// clang-format off
#include <EventLoop/Reactor.h> // Reactor::Clock, Reactor::ClockSource — the clock types

#include <chrono>              // std::chrono::milliseconds — time arithmetic
// clang-format on

class ManualClock {
  public:
    /// The current fake time.
    [[nodiscard]] EventLoop::Reactor::Clock::time_point now() const noexcept {
        return now_;
    }

    /// Moves fake time forward.
    void advance(std::chrono::milliseconds amount) noexcept {
        now_ += amount;
    }

    /// A time source for the Reactor constructor.
    [[nodiscard]] EventLoop::Reactor::ClockSource source() {
        return [this] { return now_; };
    }

  private:
    // Far from the epoch so subtracting a few seconds never underflows.
    EventLoop::Reactor::Clock::time_point now_{std::chrono::hours(1000)};
};
