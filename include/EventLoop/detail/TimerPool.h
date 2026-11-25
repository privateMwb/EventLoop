/**
 * @file            TimerPool.h
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
#include <FunctionPro/MoveOnlyFunction.h> // MoveOnlyFunction — type-erased, move-only timer callback

#include <chrono>                         // std::chrono::steady_clock — timer time base
#include <cstddef>                        // std::size_t — pool size
#include <cstdint>                        // std::uint32_t, std::uint64_t, std::int64_t — indices, sequence, ticks
#include <utility>                        // std::move — slot handover
#include <vector>                         // std::vector — node storage
// clang-format on

namespace EventLoop::detail {

/// Clock all timers are measured on.
using TimerClock = std::chrono::steady_clock;

/// What a timer runs when it fires.
using TimerCallback = FunctionPro::MoveOnlyFunction<void()>;

/// Handle to a pending timer. Safe to keep after the timer fires or is
/// cancelled: the generation no longer matches, so a stale handle is
/// recognised instead of hitting whatever reused the slot.
struct TimerId {
    std::uint32_t index = 0;      ///< Slot in the node pool.
    std::uint32_t generation = 0; ///< 0 means "no timer".

    /// True if this handle was ever issued (not whether it is still pending).
    [[nodiscard]] explicit operator bool() const noexcept {
        return generation != 0;
    }

    friend bool operator==(TimerId, TimerId) = default;
};

inline constexpr std::uint32_t kNoNode = 0xFFFFFFFFu; ///< "No node" link value.

/// One timer. Fields beyond `callback` and `deadline` are used by whichever
/// queue owns the node (heap position, wheel links).
struct TimerNode {
    TimerCallback callback;            ///< What to run.
    TimerClock::time_point deadline{}; ///< When to run it.
    std::int64_t expiryTick = 0;       ///< Wheel only: deadline in whole ticks.
    std::uint64_t sequence = 0;        ///< Insertion order, for ties and fairness.
    std::uint32_t generation = 1;      ///< Bumped each time the slot is released.
    std::uint32_t next = kNoNode;      ///< Wheel list link; free-list link when released.
    std::uint32_t prev = kNoNode;      ///< Wheel list link.
    std::uint32_t position = 0;        ///< Heap index, or wheel slot code.
    bool pending = false;              ///< Currently owned by a queue.
};

/// Slab of timer nodes with a free list. Indices stay valid while the vector
/// grows; references do not, so queues hold indices only.
class TimerPool {
  public:
    /// Takes a node (reusing a released one if possible) and returns its index.
    [[nodiscard]] std::uint32_t allocate() {
        if (free_ != kNoNode) {
            const std::uint32_t index = free_;
            free_ = nodes_[index].next;
            nodes_[index].next = kNoNode;
            return index;
        }
        nodes_.emplace_back();
        return static_cast<std::uint32_t>(nodes_.size() - 1);
    }

    /// Returns a node to the pool and invalidates handles to it.
    void release(std::uint32_t index) noexcept {
        TimerNode& node = nodes_[index];
        node.callback.reset();
        node.pending = false;
        if (++node.generation == 0) {
            node.generation = 1; // 0 is reserved for "no timer".
        }
        node.next = free_;
        node.prev = kNoNode;
        free_ = index;
    }

    [[nodiscard]] TimerNode& operator[](std::uint32_t index) noexcept {
        return nodes_[index];
    }
    [[nodiscard]] const TimerNode& operator[](std::uint32_t index) const noexcept {
        return nodes_[index];
    }

    /// True if `id` names a timer that is still pending.
    [[nodiscard]] bool isPending(TimerId id) const noexcept {
        return id.generation != 0 && id.index < nodes_.size() &&
               nodes_[id.index].generation == id.generation && nodes_[id.index].pending;
    }

  private:
    std::vector<TimerNode> nodes_; ///< All nodes, live and free.
    std::uint32_t free_ = kNoNode; ///< Head of the free list.
};

} // namespace EventLoop::detail
