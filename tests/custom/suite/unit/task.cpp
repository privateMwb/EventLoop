// EventLoop::Task test suite.
//
// Coverage:
// - Lazy start, results, void tasks and move-only results
// - Exception propagation through awaits
// - Misuse that must fail loudly rather than corrupt state
// - Symmetric transfer on a very deep await chain

#include <support/framework.h>

// clang-format off
#include <memory>    // std::unique_ptr — move-only result type
#include <stdexcept> // std::logic_error, std::runtime_error — expected failures
// clang-format on

using namespace EventLoop;

static int gBodyRuns = 0;

static Task<int> constantTask(int value) {
    ++gBodyRuns;
    co_return value;
}

static Task<void> voidTask(int& out) {
    out = 7;
    co_return;
}

static Task<int> addTask(int a, int b) {
    const int left = co_await constantTask(a);
    const int right = co_await constantTask(b);
    co_return left + right;
}

static Task<int> throwingTask() {
    throw std::runtime_error("boom");
    co_return 0;
}

static Task<int> forwardingTask() {
    co_return co_await throwingTask();
}

static Task<std::unique_ptr<int>> moveOnlyTask() {
    co_return std::make_unique<int>(99);
}

static Task<int> countDown(int depth) {
    if (depth == 0) {
        co_return 0;
    }
    co_return 1 + co_await countDown(depth - 1);
}

static Task<void> awaitTwice(Task<int> task) {
    (void)co_await std::move(task);
    (void)co_await std::move(task);
}

// Verifies the body does not run until the Task is driven.
static void task_is_lazy() {
    gBodyRuns = 0;
    {
        Task<int> task = constantTask(5);
        CHK(gBodyRuns == 0);
        CHK(task.valid());
        CHK(!task.started());
    }
    CHK(gBodyRuns == 0); // Destroyed without ever running.

    CHK(syncWait(constantTask(5)) == 5);
    CHK(gBodyRuns == 1);
}

// Verifies a value result is returned through syncWait.
static void sync_wait_returns_value() {
    CHK(syncWait(constantTask(42)) == 42);
}

// Verifies a void task runs its body.
static void void_task_runs_body() {
    int out = 0;
    syncWait(voidTask(out));
    CHK(out == 7);
}

// Verifies a task can await child tasks and combine results.
static void nested_awaits_combine_results() {
    CHK(syncWait(addTask(20, 22)) == 42);
}

// Verifies move-only results are transferred, not copied.
static void move_only_result_works() {
    const std::unique_ptr<int> value = syncWait(moveOnlyTask());
    CHK(value != nullptr);
    CHK(*value == 99);
}

// Verifies moving a Task transfers ownership of the frame.
static void move_transfers_ownership() {
    Task<int> original = constantTask(1);
    Task<int> moved = std::move(original);

    CHK(!original.valid());
    CHK(moved.valid());
    CHK(syncWait(std::move(moved)) == 1);
}

// Verifies an exception in the body surfaces at the await.
static void exception_reaches_awaiter() {
    CHK_THROWS(syncWait(throwingTask()), std::runtime_error);
}

// Verifies an exception travels through an intermediate await.
static void exception_crosses_await_chain() {
    CHK_THROWS(syncWait(forwardingTask()), std::runtime_error);
}

// Verifies awaiting an empty Task throws instead of crashing.
static void empty_task_await_throws() {
    CHK_THROWS(syncWait(Task<int>{}), std::logic_error);
}

// Verifies the result cannot be consumed twice.
static void result_consumed_once() {
    CHK_THROWS(syncWait(awaitTwice(constantTask(3))), std::logic_error);
}

// Verifies a moderately deep await chain completes, in any build type.
static void deep_chain_completes() {
    CHK(syncWait(countDown(10000)) == 10000);
}

#ifdef NDEBUG
// Verifies a very deep await chain does not grow the stack. Only meaningful
// when the compiler honours symmetric transfer, which GCC does only when
// optimising (-O2 and up), so this runs in release-style builds only.
static void deep_chain_stays_flat() {
    CHK(syncWait(countDown(200000)) == 200000);
}
#endif

static void run_tests() {
    RUN(task_is_lazy);
    RUN(sync_wait_returns_value);
    RUN(void_task_runs_body);
    RUN(nested_awaits_combine_results);
    RUN(move_only_result_works);
    RUN(move_transfers_ownership);
    RUN(exception_reaches_awaiter);
    RUN(exception_crosses_await_chain);
    RUN(empty_task_await_throws);
    RUN(result_consumed_once);
    RUN(deep_chain_completes);
#ifdef NDEBUG
    RUN(deep_chain_stays_flat);
#endif
}

REGISTER_TEST_SUITE();
