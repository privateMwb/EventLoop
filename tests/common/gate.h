#pragma once

// Hand-driven awaitables for tests and misuse programs. They stand in for
// "something external resumes the coroutine later" without needing a reactor.

// clang-format off
#include <coroutine> // std::coroutine_handle — the suspended coroutine
// clang-format on

/**
 * One-shot gate with a single waiter. A coroutine does `co_await gate`; the
 * test later calls open() to resume it.
 *
 * Well-behaved: the awaiter deregisters itself in its destructor, so
 * destroying a suspended coroutine leaves nothing dangling. This is the
 * contract every real awaitable must follow (docs/Lifetimes.md, rule 5).
 */
class Gate {
  public:
    class Awaiter {
      public:
        explicit Awaiter(Gate& gate) noexcept : gate_(gate) {}
        Awaiter(const Awaiter&) = delete;
        Awaiter& operator=(const Awaiter&) = delete;

        ~Awaiter() {
            if (gate_.waiter_ == this) {
                gate_.waiter_ = nullptr;
            }
        }

        [[nodiscard]] bool await_ready() const noexcept {
            return gate_.open_;
        }

        void await_suspend(std::coroutine_handle<> handle) noexcept {
            handle_ = handle;
            gate_.waiter_ = this;
        }

        void await_resume() const noexcept {}

      private:
        friend class Gate;
        Gate& gate_;
        std::coroutine_handle<> handle_{};
    };

    [[nodiscard]] Awaiter operator co_await() noexcept {
        return Awaiter{*this};
    }

    /// Opens the gate and resumes the waiter, if any, on the calling thread.
    void open() {
        open_ = true;
        if (waiter_ != nullptr) {
            const std::coroutine_handle<> handle = waiter_->handle_;
            waiter_ = nullptr;
            handle.resume();
        }
    }

    /// True if a coroutine is currently suspended on this gate.
    [[nodiscard]] bool hasWaiter() const noexcept {
        return waiter_ != nullptr;
    }

  private:
    Awaiter* waiter_ = nullptr;
    bool open_ = false;
};

/**
 * Deliberately broken gate: it remembers the coroutine handle and never
 * forgets it. Destroying the suspended coroutine leaves the handle dangling,
 * and open() then resumes freed memory. Used only by the misuse programs.
 */
class BadGate {
  public:
    [[nodiscard]] bool await_ready() const noexcept {
        return false;
    }
    void await_suspend(std::coroutine_handle<> handle) noexcept {
        handle_ = handle;
    }
    void await_resume() const noexcept {}

    void open() {
        handle_.resume();
    }

  private:
    std::coroutine_handle<> handle_{};
};
