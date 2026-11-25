/**
 * @file            Event.h
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
#include <cstdint> // std::uint32_t — underlying type of Event
// clang-format on

namespace EventLoop {

/**
 * @brief Readiness flags, used both as the interest a caller registers and
 * as the readiness the reactor reports back.
 *
 * `Readable` and `Writable` are meaningful in both directions. `Error` and
 * `HangUp` are report-only: the kernel delivers them whether or not they
 * were asked for, and passing them as interest has no effect. `Edge` is
 * interest-only and switches that registration to edge-triggered delivery.
 */
enum class Event : std::uint32_t {
    None = 0,
    Readable = 1u << 0, ///< Data can be read (or a connection can be accepted).
    Writable = 1u << 1, ///< Data can be written without blocking.
    Error = 1u << 2,    ///< The descriptor has a pending error. Report-only.
    HangUp = 1u << 3,   ///< The peer hung up. Report-only.
    Edge = 1u << 4,     ///< Edge-triggered delivery. Interest-only.
};

/// @brief Combines two flag sets.
[[nodiscard]] constexpr Event operator|(Event lhs, Event rhs) noexcept {
    return static_cast<Event>(static_cast<std::uint32_t>(lhs) | static_cast<std::uint32_t>(rhs));
}

/// @brief Intersects two flag sets.
[[nodiscard]] constexpr Event operator&(Event lhs, Event rhs) noexcept {
    return static_cast<Event>(static_cast<std::uint32_t>(lhs) & static_cast<std::uint32_t>(rhs));
}

/// @brief Adds flags to a set in place.
constexpr Event& operator|=(Event& lhs, Event rhs) noexcept {
    lhs = lhs | rhs;
    return lhs;
}

/// @brief True if `set` contains every bit of `flag`. `Event::None` is never contained.
[[nodiscard]] constexpr bool has(Event set, Event flag) noexcept {
    return flag != Event::None && (set & flag) == flag;
}

} // namespace EventLoop
