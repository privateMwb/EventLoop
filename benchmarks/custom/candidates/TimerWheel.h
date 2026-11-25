/**
 * @file            TimerWheel.h
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
#include <EventLoop/detail/TimerPool.h> // TimerPool, TimerNode, TimerId — shared node storage

#include <algorithm>                    // std::min — clamping ticks
#include <array>                        // std::array — slot heads and masks
#include <bit>                          // std::rotr, std::countr_zero — finding the next occupied slot
#include <chrono>                       // std::chrono::ceil/floor/milliseconds — tick conversion
#include <cstddef>                      // std::size_t — sizes
#include <cstdint>                      // std::uint32_t, std::uint64_t, std::int64_t — indices, masks, ticks
#include <optional>                     // std::optional — "maybe a timer"
#include <utility>                      // std::move — handing out callbacks
// clang-format on

namespace EventLoop::detail {

/**
 * @brief Timer queue backed by a hierarchical timing wheel with 1 ms ticks.
 *
 * @details Six levels of 64 slots each; level `l` slots are `64^l` ticks
 * wide, which reaches roughly two years. A timer sits in the lowest level
 * whose window contains it. Insert and cancel are O(1) (intrusive lists).
 * Advancing time cascades timers from a coarse slot into finer ones as their
 * block comes due. A bitmask per level finds the next occupied slot, so the
 * wheel skips idle time instead of stepping through it tick by tick.
 *
 * Resolution is one millisecond: a deadline is rounded *up* to a whole tick,
 * so a timer never fires early and may fire up to one tick late.
 *
 * Same interface as TimerHeap. **Not used by the library**: it lost the
 * benchmark in benchmarks/custom/suite/scaling/timerChurn.cpp against the
 * rule recorded there (see docs/Timers.md). It stays tested and benchmarked
 * so the decision can be revisited with data.
 */
class TimerWheel {
  public:
    using Tick = std::int64_t;

    /// @param base The instant tick 0 refers to. Defaults to now.
    explicit TimerWheel(TimerClock::time_point base = TimerClock::now()) : base_(base) {
        heads_.fill(kNoNode);
    }

    /// Schedules `callback` for `deadline`. `now` lets the wheel catch up to
    /// real time first, so a near deadline is not filed under a stale, far window.
    [[nodiscard]] TimerId insert(TimerClock::time_point deadline, TimerCallback callback,
                                 TimerClock::time_point now) {
        advanceTo(floorTick(now));
        const std::uint32_t index = pool_.allocate();
        TimerNode& node = pool_[index];
        node.callback = std::move(callback);
        node.deadline = deadline;
        node.expiryTick = ceilTick(deadline);
        node.sequence = nextSequence_++;
        node.pending = true;
        place(index);
        ++size_;
        return TimerId{index, node.generation};
    }

    /// Cancels a pending timer. Returns false if it already fired or was cancelled.
    bool cancel(TimerId id) noexcept {
        if (!pool_.isPending(id)) {
            return false;
        }
        unlink(id.index);
        pool_.release(id.index);
        --size_;
        return true;
    }

    /// Removes and returns the next timer that is due at `now` and was
    /// inserted before `beforeSequence`, or nothing.
    [[nodiscard]] std::optional<TimerCallback> popExpired(TimerClock::time_point now,
                                                          std::uint64_t beforeSequence) {
        advanceTo(floorTick(now));
        if (readyHead_ == kNoNode) {
            return std::nullopt;
        }
        const std::uint32_t index = readyHead_;
        TimerNode& node = pool_[index];
        if (node.sequence >= beforeSequence) {
            return std::nullopt;
        }
        TimerCallback callback = std::move(node.callback);
        unlink(index);
        pool_.release(index);
        --size_;
        return callback;
    }

    /// A time no later than the earliest pending deadline, if any timer is
    /// pending. It is a lower bound: waking then and finding nothing due is
    /// fine, the wheel just cascades and the caller asks again.
    [[nodiscard]] std::optional<TimerClock::time_point> nextDeadline() const noexcept {
        if (size_ == 0) {
            return std::nullopt;
        }
        if (readyHead_ != kNoNode) {
            return TimerClock::time_point{}; // Already due.
        }
        return base_ + std::chrono::milliseconds(nextEventTick(kNever));
    }

    /// Number of pending timers.
    [[nodiscard]] std::size_t size() const noexcept {
        return size_;
    }

    /// Sequence number the next inserted timer will get.
    [[nodiscard]] std::uint64_t sequence() const noexcept {
        return nextSequence_;
    }

  private:
    static constexpr unsigned kBits = 6;                      ///< log2 of slots per level.
    static constexpr unsigned kSlots = 1u << kBits;           ///< Slots per level.
    static constexpr unsigned kLevels = 6;                    ///< Levels (reach: 64^6 ticks).
    static constexpr std::uint32_t kReady = kLevels * kSlots; ///< Position code of the ready list.
    static constexpr Tick kNever = std::int64_t{1} << 62;     ///< "No event" sentinel tick.

    [[nodiscard]] Tick floorTick(TimerClock::time_point t) const noexcept {
        return std::chrono::floor<std::chrono::milliseconds>(t - base_).count();
    }

    [[nodiscard]] Tick ceilTick(TimerClock::time_point t) const noexcept {
        return std::chrono::ceil<std::chrono::milliseconds>(t - base_).count();
    }

    /// Files a node under the right level and slot for the current tick.
    void place(std::uint32_t index) noexcept {
        const Tick expiry = pool_[index].expiryTick;
        if (expiry <= current_) {
            linkReady(index);
            return;
        }
        // Lowest level where the node is at most 63 blocks ahead. (The
        // current block's slot is always empty, so 64 would also work; the
        // stricter bound is kept for clarity.) Its cascade is then always
        // in the future.
        unsigned level = 0;
        Tick ahead = expiry - current_;
        while (ahead >= kSlots && level + 1 < kLevels) {
            ++level;
            ahead = (expiry >> (kBits * level)) - (current_ >> (kBits * level));
        }
        unsigned slot = 0;
        if (ahead >= kSlots) {
            // Beyond the top level's reach: park it in its farthest slot. It
            // is re-filed when that block comes due.
            slot = static_cast<unsigned>(((current_ >> (kBits * level)) + (kSlots - 1)) &
                                         (kSlots - 1));
        } else {
            slot = static_cast<unsigned>((expiry >> (kBits * level)) & (kSlots - 1));
        }
        linkSlot(index, level, slot);
    }

    void linkSlot(std::uint32_t index, unsigned level, unsigned slot) noexcept {
        const unsigned code = level * kSlots + slot;
        TimerNode& node = pool_[index];
        node.position = code;
        node.prev = kNoNode;
        node.next = heads_[code];
        if (heads_[code] != kNoNode) {
            pool_[heads_[code]].prev = index;
        }
        heads_[code] = index;
        masks_[level] |= std::uint64_t{1} << slot;
    }

    void linkReady(std::uint32_t index) noexcept {
        TimerNode& node = pool_[index];
        node.position = kReady;
        node.next = kNoNode;
        node.prev = readyTail_;
        if (readyTail_ != kNoNode) {
            pool_[readyTail_].next = index;
        } else {
            readyHead_ = index;
        }
        readyTail_ = index;
    }

    /// Removes a node from whichever list holds it.
    void unlink(std::uint32_t index) noexcept {
        TimerNode& node = pool_[index];
        if (node.position == kReady) {
            if (node.prev != kNoNode) {
                pool_[node.prev].next = node.next;
            } else {
                readyHead_ = node.next;
            }
            if (node.next != kNoNode) {
                pool_[node.next].prev = node.prev;
            } else {
                readyTail_ = node.prev;
            }
            return;
        }
        const unsigned code = node.position;
        if (node.prev != kNoNode) {
            pool_[node.prev].next = node.next;
        } else {
            heads_[code] = node.next;
            if (node.next == kNoNode) {
                masks_[code / kSlots] &= ~(std::uint64_t{1} << (code % kSlots));
            }
        }
        if (node.next != kNoNode) {
            pool_[node.next].prev = node.prev;
        }
    }

    /// Detaches a whole slot list and returns its head.
    std::uint32_t takeSlot(unsigned level, unsigned slot) noexcept {
        const unsigned code = level * kSlots + slot;
        const std::uint32_t head = heads_[code];
        heads_[code] = kNoNode;
        masks_[level] &= ~(std::uint64_t{1} << slot);
        return head;
    }

    /// Moves a level's slot down: every node is filed again for the new tick.
    void cascade(unsigned level, unsigned slot) noexcept {
        std::uint32_t index = takeSlot(level, slot);
        while (index != kNoNode) {
            const std::uint32_t next = pool_[index].next;
            place(index);
            index = next;
        }
    }

    /// Moves everything in a level-0 slot to the ready list, oldest first.
    void drainToReady(unsigned slot) noexcept {
        std::uint32_t index = takeSlot(0, slot);
        // Slot lists are newest-first; reverse them so ties fire in insertion order.
        std::uint32_t reversed = kNoNode;
        while (index != kNoNode) {
            const std::uint32_t next = pool_[index].next;
            pool_[index].next = reversed;
            reversed = index;
            index = next;
        }
        while (reversed != kNoNode) {
            const std::uint32_t next = pool_[reversed].next;
            linkReady(reversed);
            reversed = next;
        }
    }

    /// Processes the tick `current_` just reached: cascades every level whose
    /// block boundary it is, highest first, then readies the level-0 slot.
    void step() noexcept {
        unsigned top = 0;
        while (top + 1 < kLevels && (current_ & ((Tick{1} << (kBits * (top + 1))) - 1)) == 0) {
            ++top;
        }
        for (unsigned level = top; level >= 1; --level) {
            cascade(level, static_cast<unsigned>((current_ >> (kBits * level)) & (kSlots - 1)));
        }
        drainToReady(static_cast<unsigned>(current_ & (kSlots - 1)));
    }

    /// The earliest tick after `current_` at which something happens (a level-0
    /// timer fires, or a coarser block comes due and must cascade), or
    /// `limit` if nothing is pending sooner.
    [[nodiscard]] Tick nextEventTick(Tick limit) const noexcept {
        Tick best = limit;
        if (masks_[0] != 0) {
            const unsigned start = static_cast<unsigned>((current_ + 1) & (kSlots - 1));
            const int distance = std::countr_zero(std::rotr(masks_[0], static_cast<int>(start)));
            best = std::min(best, current_ + 1 + distance);
        }
        for (unsigned level = 1; level < kLevels; ++level) {
            if (masks_[level] == 0) {
                continue;
            }
            const Tick block = current_ >> (kBits * level);
            const unsigned start = static_cast<unsigned>((block + 1) & (kSlots - 1));
            const int distance =
                std::countr_zero(std::rotr(masks_[level], static_cast<int>(start)));
            best = std::min(best, (block + 1 + distance) << (kBits * level));
        }
        return best;
    }

    /// Advances to `target`, jumping straight over stretches with nothing to do.
    void advanceTo(Tick target) noexcept {
        while (current_ < target) {
            current_ = std::min(target, nextEventTick(target));
            step();
        }
    }

    TimerClock::time_point base_;                       ///< The instant tick 0 refers to.
    TimerPool pool_;                                    ///< Node storage.
    std::array<std::uint32_t, kLevels * kSlots> heads_; ///< Head node of each slot list.
    std::array<std::uint64_t, kLevels> masks_{};        ///< Occupied-slot bitmask per level.
    std::uint32_t readyHead_ = kNoNode;                 ///< Due timers, oldest first.
    std::uint32_t readyTail_ = kNoNode;                 ///< Tail of the ready list.
    Tick current_ = 0;                                  ///< The last tick processed.
    std::size_t size_ = 0;                              ///< Pending timers.
    std::uint64_t nextSequence_ = 0;                    ///< Insertion counter.
};

} // namespace EventLoop::detail
