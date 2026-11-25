/**
 * @file destroyedClosure.cpp
 * @brief Misuse: a lambda coroutine outliving its closure.
 *
 * A lambda's captures live in the closure object, not in the coroutine frame;
 * the frame only remembers where the closure is. Destroying the closure while
 * the coroutine is suspended leaves it reading dead captures. AddressSanitizer
 * must report heap-use-after-free.
 */

// clang-format off
#include <EventLoop/EventLoop.h> // Task, spawn — the coroutine type being misused
#include <common/gate.h>         // Gate — resumes the coroutine on demand

#include <string>                // std::string — a capture with heap storage
// clang-format on

using namespace EventLoop;

static Task<void> store(Task<int> task, int& out) {
    out = co_await std::move(task);
}

int main() {
    Gate gate;
    int result = -1;

    auto* closure = new auto([text = std::string(200, 'x'), &gate]() -> Task<int> {
        co_await gate;
        co_return text[0]; // Reads a capture of the closure.
    });

    Task<int> task = (*closure)();
    delete closure; // The closure dies while its coroutine is still pending.

    spawn(store(std::move(task), result)); // Runs until the gate.
    gate.open();                           // Resumes and reads the dead capture.
    return result;
}
