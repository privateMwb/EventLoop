// Cross-thread Task test suite.
//
// Coverage:
// - syncWait blocks until a task that another thread resumes finishes
// - syncWait is safe to call from many threads at once

#include <support/framework.h>

// clang-format off
#include <atomic>    // std::atomic — start flag and counters
#include <coroutine> // std::coroutine_handle — the cross-thread awaiter
#include <mutex>     // std::mutex, std::lock_guard — guards the handle handoff
#include <thread>    // std::thread, std::this_thread — helper threads
#include <vector>    // std::vector — the thread set
// clang-format on

using namespace EventLoop;

/// Thread-safe one-shot gate: a coroutine suspends on it, another thread
/// resumes the coroutine on its own stack.
class AsyncGate {
  public:
    class Awaiter {
      public:
        explicit Awaiter(AsyncGate& gate) noexcept : gate_(gate) {}

        [[nodiscard]] bool await_ready() const noexcept {
            return false;
        }

        [[nodiscard]] bool await_suspend(std::coroutine_handle<> handle) {
            const std::lock_guard<std::mutex> lock(gate_.mutex_);
            if (gate_.open_) {
                return false; // Opened before we got here: keep running.
            }
            gate_.handle_ = handle;
            return true;
        }

        void await_resume() const noexcept {}

      private:
        AsyncGate& gate_;
    };

    [[nodiscard]] Awaiter operator co_await() noexcept {
        return Awaiter{*this};
    }

    /// Blocks the calling thread until a coroutine is suspended on the gate.
    void waitForWaiter() {
        for (;;) {
            {
                const std::lock_guard<std::mutex> lock(mutex_);
                if (handle_) {
                    return;
                }
            }
            std::this_thread::yield();
        }
    }

    /// Opens the gate and resumes the waiter on the calling thread.
    void open() {
        std::coroutine_handle<> handle;
        {
            const std::lock_guard<std::mutex> lock(mutex_);
            open_ = true;
            handle = std::exchange(handle_, {});
        }
        if (handle) {
            handle.resume();
        }
    }

  private:
    std::mutex mutex_;
    std::coroutine_handle<> handle_{};
    bool open_ = false;
};

static Task<int> waitForGate(AsyncGate& gate) {
    co_await gate;
    co_return 42;
}

static Task<int> plusOne(int value) {
    co_return value + 1;
}

// Verifies syncWait returns a result produced after another thread resumes the task.
static void sync_wait_across_threads() {
    AsyncGate gate;
    std::thread opener([&gate] {
        gate.waitForWaiter();
        gate.open();
    });

    const int value = syncWait(waitForGate(gate));
    opener.join();

    CHK(value == 42);
}

// Verifies concurrent syncWait calls on separate threads do not interfere.
static void sync_wait_many_threads() {
    constexpr int threadCount = 8;
    constexpr int iterations = 500;
    std::atomic<int> wrong{0};

    std::vector<std::thread> threads;
    for (int t = 0; t < threadCount; ++t) {
        threads.emplace_back([&wrong] {
            for (int i = 0; i < iterations; ++i) {
                if (syncWait(plusOne(i)) != i + 1) {
                    ++wrong;
                }
            }
        });
    }
    for (std::thread& thread : threads) {
        thread.join();
    }

    CHK(wrong.load() == 0);
}

static void run_tests() {
    RUN(sync_wait_across_threads);
    RUN(sync_wait_many_threads);
}

REGISTER_TEST_SUITE();
