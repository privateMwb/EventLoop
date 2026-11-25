// EventLoop::spawn and Task::start test suite.
//
// Coverage:
// - spawn() runs eagerly up to the first suspension
// - Detached tasks finish when whatever they wait on fires
// - Bad arguments are rejected
// - Task::start() keeps ownership and exposes the result

#include <support/framework.h>

// clang-format off
#include <memory>    // std::unique_ptr, std::make_unique — one gate per task
#include <stdexcept> // std::logic_error — expected failures
#include <vector>    // std::vector — many gates
// clang-format on

using namespace EventLoop;

static Task<void> markAfterGate(Gate& gate, int& stage) {
    stage = 1;
    co_await gate;
    stage = 2;
}

static Task<int> valueAfterGate(Gate& gate) {
    co_await gate;
    co_return 11;
}

// Verifies spawn runs the body up to its first suspension immediately.
static void spawn_runs_eagerly() {
    Gate gate;
    int stage = 0;

    spawn(markAfterGate(gate, stage));

    CHK(stage == 1);
    CHK(gate.hasWaiter());

    gate.open();

    CHK(stage == 2);
    CHK(!gate.hasWaiter());
}

// Verifies a spawned task that never suspends finishes during spawn.
static void spawn_finishes_without_suspend() {
    Gate gate;
    gate.open();
    int stage = 0;

    spawn(markAfterGate(gate, stage));

    CHK(stage == 2);
}

// Verifies many detached tasks each complete when released.
static void spawn_many_tasks_complete() {
    constexpr int count = 1000;
    std::vector<std::unique_ptr<Gate>> gates;
    std::vector<int> stages(count, 0);

    for (int i = 0; i < count; ++i) {
        gates.push_back(std::make_unique<Gate>());
        spawn(markAfterGate(*gates.back(), stages[static_cast<std::size_t>(i)]));
    }
    int suspended = 0;
    for (int stage : stages) {
        suspended += (stage == 1) ? 1 : 0;
    }
    CHK(suspended == count);

    for (auto& gate : gates) {
        gate->open();
    }
    int finished = 0;
    for (int stage : stages) {
        finished += (stage == 2) ? 1 : 0;
    }
    CHK(finished == count);
}

// Verifies spawn rejects empty and already-started tasks.
static void spawn_rejects_bad_tasks() {
    CHK_THROWS(spawn(Task<void>{}), std::logic_error);

    Gate gate;
    int stage = 0;
    Task<void> started = markAfterGate(gate, stage);
    started.start();
    CHK_THROWS(spawn(std::move(started)), std::logic_error);
}

// Verifies start() runs to the first suspension and keeps ownership.
static void start_keeps_ownership() {
    Gate gate;
    Task<int> task = valueAfterGate(gate);

    task.start();
    CHK(task.started());
    CHK(!task.done());
    CHK_THROWS(task.result(), std::logic_error); // Not finished yet.

    gate.open();

    CHK(task.done());
    CHK(task.result() == 11);
}

// Verifies start() cannot be repeated and awaiting a started task is refused.
static void start_rejects_second_use() {
    Gate gate;
    Task<int> task = valueAfterGate(gate);
    task.start();

    CHK_THROWS(task.start(), std::logic_error);
    CHK_THROWS(syncWait(std::move(task)), std::logic_error);
}

static void run_tests() {
    RUN(spawn_runs_eagerly);
    RUN(spawn_finishes_without_suspend);
    RUN(spawn_many_tasks_complete);
    RUN(spawn_rejects_bad_tasks);
    RUN(start_keeps_ownership);
    RUN(start_rejects_second_use);
}

REGISTER_TEST_SUITE();
