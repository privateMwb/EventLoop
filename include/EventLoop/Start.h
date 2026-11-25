/**
 * @file            Start.h
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
#include <EventLoop/Task.h>   // Task — what gets started

#include <condition_variable> // std::condition_variable — syncWait's wakeup
#include <coroutine>          // std::coroutine_handle, std::suspend_always, std::suspend_never — root coroutines
#include <exception>          // std::terminate, std::exception_ptr — unhandled failures, result transport
#include <mutex>              // std::mutex, std::lock_guard, std::unique_lock — syncWait's wakeup
#include <optional>           // std::optional — syncWait result storage
#include <stdexcept>          // std::logic_error — rejecting empty or started tasks
#include <type_traits>        // std::is_void_v — void vs value results
#include <utility>            // std::move — handing tasks to their root coroutines
// clang-format on

namespace EventLoop {

namespace detail {

/// One-shot cross-thread signal used by syncWait.
class Latch {
  public:
    /// Releases the waiter. Notifies while holding the lock so the waiter
    /// cannot finish and destroy the Latch before this call has returned.
    void signal() {
        const std::lock_guard<std::mutex> lock(mutex_);
        signalled_ = true;
        condition_.notify_all();
    }

    /// Blocks until signal() has been called.
    void wait() {
        std::unique_lock<std::mutex> lock(mutex_);
        condition_.wait(lock, [this] { return signalled_; });
    }

  private:
    std::mutex mutex_;
    std::condition_variable condition_;
    bool signalled_ = false;
};

/// Self-owning, eagerly started coroutine used by spawn(). Its frame is
/// destroyed automatically when the body finishes.
struct DetachedTask {
    struct promise_type {
        [[nodiscard]] DetachedTask get_return_object() const noexcept {
            return {};
        }
        [[nodiscard]] std::suspend_never initial_suspend() const noexcept {
            return {};
        }
        [[nodiscard]] std::suspend_never final_suspend() const noexcept {
            return {};
        }
        void return_void() const noexcept {}
        // Nobody is left to receive the exception.
        void unhandled_exception() const noexcept {
            std::terminate();
        }
    };
};

/// Runs `task` to completion inside a self-destroying frame.
inline DetachedTask runDetached(Task<void> task) {
    co_await std::move(task);
}

/// Where syncWait collects a result.
template <typename T> struct SyncResult {
    std::optional<T> value;
    std::exception_ptr error;
};

template <> struct SyncResult<void> {
    std::exception_ptr error;
};

/// Caller-owned root coroutine used by syncWait. Starts suspended and
/// signals a Latch as its very last action.
class SyncRoot {
  public:
    struct promise_type {
        /// Signals the Latch without touching the frame afterwards: the
        /// waiting thread may destroy the frame the moment it is released.
        struct FinalAwaiter {
            [[nodiscard]] bool await_ready() const noexcept {
                return false;
            }

            void await_suspend(std::coroutine_handle<promise_type> self) const noexcept {
                Latch* latch = self.promise().latch;
                latch->signal();
            }

            void await_resume() const noexcept {}
        };

        [[nodiscard]] SyncRoot get_return_object() noexcept {
            return SyncRoot{std::coroutine_handle<promise_type>::from_promise(*this)};
        }
        [[nodiscard]] std::suspend_always initial_suspend() const noexcept {
            return {};
        }
        [[nodiscard]] FinalAwaiter final_suspend() const noexcept {
            return {};
        }
        void return_void() const noexcept {}
        void unhandled_exception() const noexcept {
            std::terminate();
        }

        Latch* latch = nullptr; ///< Signalled when the body finishes.
    };

    SyncRoot(const SyncRoot&) = delete;
    SyncRoot& operator=(const SyncRoot&) = delete;
    SyncRoot(SyncRoot&& other) noexcept : handle_(std::exchange(other.handle_, {})) {}
    SyncRoot& operator=(SyncRoot&&) = delete;

    ~SyncRoot() {
        if (handle_) {
            handle_.destroy();
        }
    }

    /// Starts the body on the calling thread, signalling `latch` at the end.
    void run(Latch& latch) {
        handle_.promise().latch = &latch;
        handle_.resume();
    }

  private:
    explicit SyncRoot(std::coroutine_handle<promise_type> handle) noexcept : handle_(handle) {}

    std::coroutine_handle<promise_type> handle_{};
};

/// Awaits `task` and records the outcome in `out`. `out` is a deliberate
/// reference: it lives on syncWait's stack for the whole run.
template <typename T> SyncRoot runSync(Task<T> task, SyncResult<T>& out) {
    try {
        if constexpr (std::is_void_v<T>) {
            co_await std::move(task);
        } else {
            out.value.emplace(co_await std::move(task));
        }
    } catch (...) {
        out.error = std::current_exception();
    }
}

} // namespace detail

/**
 * @brief Runs `task` detached: starts it immediately on the calling thread,
 * and lets it destroy itself when it finishes.
 *
 * @details The body runs eagerly, up to its first suspension, before spawn()
 * returns. After that it is resumed by whatever it is waiting on. There is
 * no handle: a spawned task cannot be cancelled or awaited (cancellation
 * arrives with timers), and if the thing it waits on never fires, its frame
 * lives until the process exits. An exception escaping a spawned task calls
 * std::terminate, because there is nobody left to receive it.
 *
 * @throws std::logic_error if `task` is empty or was already started.
 */
inline void spawn(Task<void> task) {
    if (!task.valid()) {
        throw std::logic_error("spawn: empty Task");
    }
    if (task.started()) {
        throw std::logic_error("spawn: Task was already started");
    }
    detail::runDetached(std::move(task));
}

/**
 * @brief Runs `task` and blocks the calling thread until it finishes.
 *
 * @details The body starts on the calling thread. If it suspends, some other
 * thread (or this one, from a reactor it drives elsewhere) must resume it; it
 * then finishes on that thread and wakes this one. Do not call syncWait from
 * a thread that is itself the only thing able to resume the task, such as a
 * reactor's own loop thread: it would wait forever.
 *
 * @return The task's result.
 * @throws whatever exception escaped the task, or std::logic_error if it was empty.
 */
template <typename T> T syncWait(Task<T> task) {
    detail::SyncResult<T> result;
    detail::Latch latch;
    detail::SyncRoot root = detail::runSync(std::move(task), result);
    root.run(latch);
    latch.wait();

    if (result.error) {
        std::rethrow_exception(result.error);
    }
    if constexpr (!std::is_void_v<T>) {
        return std::move(*result.value);
    }
}

} // namespace EventLoop
