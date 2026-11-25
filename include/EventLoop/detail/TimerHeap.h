/**
 * @file            TimerHeap.h
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

#include <cstddef>                      // std::size_t — sizes and positions
#include <cstdint>                      // std::uint32_t, std::uint64_t — indices and sequence
#include <optional>                     // std::optional — "maybe a timer"
#include <utility>                      // std::move — handing out callbacks
#include <vector>                       // std::vector — the heap array
// clang-format on

namespace EventLoop::detail {

/**
 * @brief Timer queue backed by an indexed binary min-heap.
 *
 * Insert and cancel are O(log n); the earliest deadline is O(1). Every node
 * remembers its heap position, so cancelling removes the entry immediately
 * rather than leaving garbage until it would have fired.
 *
 * The queue the Reactor uses. Chosen over a timing wheel by benchmark; see
 * docs/Timers.md.
 */
class TimerHeap {
  public:
    /// Schedules `callback` for `deadline`. `now` is unused: it exists so
    /// this queue and the candidate TimerWheel (which needs it) share one
    /// call shape.
    [[nodiscard]] TimerId insert(TimerClock::time_point deadline, TimerCallback callback,
                                 TimerClock::time_point now = {}) {
        (void)now;
        const std::uint32_t index = pool_.allocate();
        TimerNode& node = pool_[index];
        node.callback = std::move(callback);
        node.deadline = deadline;
        node.sequence = nextSequence_++;
        node.pending = true;
        node.position = static_cast<std::uint32_t>(heap_.size());
        heap_.push_back(index);
        siftUp(node.position);
        return TimerId{index, node.generation};
    }

    /// Cancels a pending timer. Returns false if it already fired or was cancelled.
    bool cancel(TimerId id) noexcept {
        if (!pool_.isPending(id)) {
            return false;
        }
        removeAt(pool_[id.index].position);
        pool_.release(id.index);
        return true;
    }

    /// Removes and returns the earliest timer that is due at `now` and was
    /// inserted before `beforeSequence`, or nothing.
    [[nodiscard]] std::optional<TimerCallback> popExpired(TimerClock::time_point now,
                                                          std::uint64_t beforeSequence) {
        if (heap_.empty()) {
            return std::nullopt;
        }
        const std::uint32_t index = heap_.front();
        TimerNode& node = pool_[index];
        if (node.deadline > now || node.sequence >= beforeSequence) {
            return std::nullopt;
        }
        TimerCallback callback = std::move(node.callback);
        removeAt(0);
        pool_.release(index);
        return callback;
    }

    /// The earliest pending deadline, if any.
    [[nodiscard]] std::optional<TimerClock::time_point> nextDeadline() const noexcept {
        if (heap_.empty()) {
            return std::nullopt;
        }
        return pool_[heap_.front()].deadline;
    }

    /// Number of pending timers.
    [[nodiscard]] std::size_t size() const noexcept {
        return heap_.size();
    }

    /// Sequence number the next inserted timer will get.
    [[nodiscard]] std::uint64_t sequence() const noexcept {
        return nextSequence_;
    }

  private:
    [[nodiscard]] bool less(std::uint32_t a, std::uint32_t b) const noexcept {
        const TimerNode& left = pool_[a];
        const TimerNode& right = pool_[b];
        return left.deadline != right.deadline ? left.deadline < right.deadline
                                               : left.sequence < right.sequence;
    }

    void place(std::size_t position, std::uint32_t index) noexcept {
        heap_[position] = index;
        pool_[index].position = static_cast<std::uint32_t>(position);
    }

    void siftUp(std::size_t position) noexcept {
        const std::uint32_t index = heap_[position];
        while (position > 0) {
            const std::size_t parent = (position - 1) / 2;
            if (!less(index, heap_[parent])) {
                break;
            }
            place(position, heap_[parent]);
            position = parent;
        }
        place(position, index);
    }

    void siftDown(std::size_t position) noexcept {
        const std::uint32_t index = heap_[position];
        const std::size_t count = heap_.size();
        for (;;) {
            std::size_t child = 2 * position + 1;
            if (child >= count) {
                break;
            }
            if (child + 1 < count && less(heap_[child + 1], heap_[child])) {
                ++child;
            }
            if (!less(heap_[child], index)) {
                break;
            }
            place(position, heap_[child]);
            position = child;
        }
        place(position, index);
    }

    /// Removes the entry at `position`, restoring the heap property.
    void removeAt(std::size_t position) noexcept {
        const std::uint32_t last = heap_.back();
        heap_.pop_back();
        if (position < heap_.size()) {
            place(position, last);
            siftDown(position);
            siftUp(pool_[last].position);
        }
    }

    TimerPool pool_;                  ///< Node storage.
    std::vector<std::uint32_t> heap_; ///< Node indices in heap order.
    std::uint64_t nextSequence_ = 0;  ///< Insertion counter.
};

} // namespace EventLoop::detail
