/**
 * @file staleResume.cpp
 * @brief Misuse: resuming a coroutine whose Task was already destroyed.
 *
 * The Task is destroyed while suspended, which frees the frame. The awaitable
 * never deregistered, so opening it resumes freed memory. AddressSanitizer
 * must report heap-use-after-free.
 */

// clang-format off
#include <EventLoop/EventLoop.h> // Task — the coroutine type being misused
#include <common/gate.h>         // BadGate — an awaitable that never deregisters
// clang-format on

using namespace EventLoop;

static Task<void> waiter(BadGate& gate) {
    co_await gate;
}

int main() {
    BadGate gate;
    {
        Task<void> task = waiter(gate);
        task.start(); // Suspends on the gate.
    } // Task destroyed mid-suspend: the frame is freed.

    gate.open(); // Resumes the freed frame.
    return 0;
}
