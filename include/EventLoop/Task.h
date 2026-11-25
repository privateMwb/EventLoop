/**
 * @file            Task.h
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
#include <concepts>    // std::constructible_from — constrains return_value
#include <coroutine>   // std::coroutine_handle, std::suspend_always, std::noop_coroutine — coroutine machinery
#include <exception>   // std::exception_ptr, std::current_exception, std::rethrow_exception — error transport
#include <optional>    // std::optional — result storage without requiring default construction
#include <stdexcept>   // std::logic_error — reports of Task misuse
#include <string>      // std::string — building misuse messages
#include <type_traits> // std::is_reference_v, std::is_nothrow_constructible_v — compile-time checks
#include <utility>     // std::exchange, std::move, std::forward — ownership transfer
// clang-format on

namespace EventLoop {

template <typename T = void> class Task;

namespace detail {

template <typename T> class TimeoutAwaiter; // Timeout.h — needs access to a Task's promise.

/// State shared by every Task promise: who to resume when the body finishes,
/// whether it has been started, and a captured exception.
struct TaskPromiseBase {
    /// Resumes the awaiting coroutine when the body finishes, using symmetric
    /// transfer so a chain of awaits never grows the stack.
    struct FinalAwaiter {
        [[nodiscard]] bool await_ready() const noexcept {
            return false;
        }

        template <typename Promise>
        [[nodiscard]] std::coroutine_handle<>
        await_suspend(std::coroutine_handle<Promise> self) const noexcept {
            const std::coroutine_handle<> next = self.promise().continuation;
            return next ? next : std::noop_coroutine();
        }

        void await_resume() const noexcept {}
    };

    // Lazy: the body does not run until the Task is awaited, spawned or started.
    [[nodiscard]] std::suspend_always initial_suspend() const noexcept {
        return {};
    }
    [[nodiscard]] FinalAwaiter final_suspend() const noexcept {
        return {};
    }

    std::coroutine_handle<> continuation{}; ///< The awaiting coroutine, if any.
    std::exception_ptr error;               ///< Exception that escaped the body.
    bool started = false;                   ///< Set once the body has been resumed.
};

/// Promise for a Task that produces a value.
template <typename T> struct TaskPromise : TaskPromiseBase {
    [[nodiscard]] Task<T> get_return_object() noexcept;

    template <typename U = T>
        requires std::constructible_from<T, U&&>
    void return_value(U&& value) noexcept(std::is_nothrow_constructible_v<T, U&&>) {
        result.emplace(std::forward<U>(value));
    }

    void unhandled_exception() noexcept {
        error = std::current_exception();
    }

    /// Hands the outcome to the awaiter: rethrows a captured exception, or
    /// moves the value out. The outcome can be taken once.
    [[nodiscard]] T takeResult() {
        if (error) {
            std::rethrow_exception(std::exchange(error, nullptr));
        }
        if (!result) {
            throw std::logic_error("Task result was already consumed");
        }
        T value = std::move(*result);
        result.reset();
        return value;
    }

    std::optional<T> result; ///< The produced value until it is taken.
};

/// Promise for a Task that produces no value.
template <> struct TaskPromise<void> : TaskPromiseBase {
    [[nodiscard]] Task<void> get_return_object() noexcept;

    void return_void() noexcept {}

    void unhandled_exception() noexcept {
        error = std::current_exception();
    }

    /// Rethrows a captured exception. The outcome can be taken once.
    void takeResult() {
        if (error) {
            std::rethrow_exception(std::exchange(error, nullptr));
        }
        if (consumed) {
            throw std::logic_error("Task result was already consumed");
        }
        consumed = true;
    }

    bool consumed = false; ///< The outcome has been taken.
};

} // namespace detail

/**
 * @brief Lazy, move-only coroutine task that owns its coroutine frame.
 *
 * @details
 * A Task does nothing until something drives it:
 *  - `co_await std::move(task)` from another coroutine (the normal way);
 *  - `spawn(std::move(task))` to run it detached (see Start.h);
 *  - `syncWait(std::move(task))` to block a thread until it finishes;
 *  - `task.start()` to run it to its first suspension while keeping ownership.
 *
 * The Task object owns the frame: destroying the Task destroys the frame,
 * wherever the body is suspended. See docs/Lifetimes.md for the complete
 * ownership and misuse rules.
 *
 * Only an rvalue Task can be awaited, because awaiting consumes the result.
 *
 * @tparam T Result type. References are not supported.
 */
template <typename T> class [[nodiscard]] Task {
    static_assert(!std::is_reference_v<T>,
                  "Task<T&> is not supported; return a value or a pointer");

  public:
    using promise_type = detail::TaskPromise<T>;
    using Handle = std::coroutine_handle<promise_type>;

    /// @brief Constructs an empty Task that owns no coroutine.
    Task() noexcept = default;

    Task(const Task&) = delete;
    Task& operator=(const Task&) = delete;

    Task(Task&& other) noexcept : handle_(std::exchange(other.handle_, {})) {}

    Task& operator=(Task&& other) noexcept {
        if (this != &other) {
            destroy();
            handle_ = std::exchange(other.handle_, {});
        }
        return *this;
    }

    /// @brief Destroys the frame, wherever the body is suspended.
    ~Task() {
        destroy();
    }

    /// @brief True if this Task owns a coroutine.
    [[nodiscard]] bool valid() const noexcept {
        return static_cast<bool>(handle_);
    }

    /// @brief True if the body has finished (returned or thrown).
    [[nodiscard]] bool done() const noexcept {
        return handle_ && handle_.done();
    }

    /// @brief True if the body has been resumed at least once.
    [[nodiscard]] bool started() const noexcept {
        return handle_ && handle_.promise().started;
    }

    /**
     * @brief Runs the body to its first suspension point (or to completion),
     * keeping ownership of the frame.
     * @throws std::logic_error if the Task is empty or was already started.
     */
    void start() {
        requireValid("start");
        if (handle_.promise().started) {
            throw std::logic_error("Task::start: Task was already started");
        }
        handle_.promise().started = true;
        handle_.resume();
    }

    /**
     * @brief Takes the result of a finished Task started with start().
     * @throws std::logic_error if the Task is empty, unfinished, or its result was taken.
     * @throws whatever exception escaped the body.
     */
    [[nodiscard]] T result() {
        requireValid("result");
        if (!handle_.done()) {
            throw std::logic_error("Task::result: Task has not finished");
        }
        return handle_.promise().takeResult();
    }

    /**
     * @brief Cancels the Task: destroys its frame wherever the body is
     * suspended, and leaves this object empty.
     *
     * @details The body never runs again. Locals are destroyed, and any awaiter
     * in flight deregisters itself (docs/Lifetimes.md, rule 5), so nothing
     * can resume the dead frame later. Does nothing on an empty or finished
     * Task other than releasing it.
     *
     * Must not be called while the body is executing (from inside itself).
     * Called from a timer or I/O callback of the reactor that drives the
     * Task, it is safe, because no coroutine is running then.
     */
    void cancel() noexcept {
        destroy();
    }

    /// Awaiting an lvalue would silently consume it; write `co_await std::move(task)`.
    void operator co_await() & = delete;

    /// @brief Awaits the Task: starts the body, suspends the caller until it
    /// finishes, and yields its result or rethrows its exception.
    [[nodiscard]] auto operator co_await() && noexcept {
        return Awaiter{handle_};
    }

  private:
    friend struct detail::TaskPromise<T>;
    template <typename U> friend class detail::TimeoutAwaiter;

    /// Awaiter returned by operator co_await. Does not own the frame.
    struct Awaiter {
        Handle handle;

        [[nodiscard]] bool await_ready() const {
            if (!handle) {
                throw std::logic_error("co_await on an empty Task");
            }
            if (handle.done()) {
                return true; // Already finished: just take the result.
            }
            if (handle.promise().started) {
                // Resuming a body that is suspended on something else would
                // run it twice.
                throw std::logic_error("co_await on a Task that was already started");
            }
            return false;
        }

        [[nodiscard]] std::coroutine_handle<>
        await_suspend(std::coroutine_handle<> awaiting) const noexcept {
            handle.promise().started = true;
            handle.promise().continuation = awaiting;
            return handle; // Symmetric transfer into the child.
        }

        [[nodiscard]] T await_resume() const {
            return handle.promise().takeResult();
        }
    };

    explicit Task(Handle handle) noexcept : handle_(handle) {}

    void requireValid(const char* operation) const {
        if (!handle_) {
            throw std::logic_error(std::string("Task::") + operation + ": empty Task");
        }
    }

    void destroy() noexcept {
        if (handle_) {
            handle_.destroy();
            handle_ = {};
        }
    }

    Handle handle_{}; ///< The owned coroutine frame, or empty.
};

namespace detail {

template <typename T> Task<T> TaskPromise<T>::get_return_object() noexcept {
    return Task<T>{std::coroutine_handle<TaskPromise<T>>::from_promise(*this)};
}

inline Task<void> TaskPromise<void>::get_return_object() noexcept {
    return Task<void>{std::coroutine_handle<TaskPromise<void>>::from_promise(*this)};
}

} // namespace detail

} // namespace EventLoop
