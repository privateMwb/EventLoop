/**
 * @file danglingView.cpp
 * @brief Misuse: a reference-like parameter that dies before the coroutine reads it.
 *
 * A coroutine copies its parameters into the frame, but a string_view copies
 * only the pointer. The temporary string it points into is freed at the end
 * of the call expression, long before the coroutine resumes. AddressSanitizer
 * must report heap-use-after-free.
 */

// clang-format off
#include <EventLoop/EventLoop.h> // Task, spawn — the coroutine type being misused
#include <common/gate.h>         // Gate — resumes the coroutine on demand

#include <string>                // std::string — the temporary that dies
#include <string_view>           // std::string_view — the dangling parameter
// clang-format on

using namespace EventLoop;

static Task<int> firstChar(std::string_view text, Gate& gate) {
    co_await gate;
    co_return text[0]; // Reads the freed string.
}

static Task<void> store(Task<int> task, int& out) {
    out = co_await std::move(task);
}

int main() {
    Gate gate;
    int result = -1;

    // The temporary std::string is destroyed at the end of this statement.
    Task<int> task = firstChar(std::string(200, 'x'), gate);

    spawn(store(std::move(task), result)); // Runs until the gate.
    gate.open();                           // Resumes and reads the dangling view.
    return result;
}
