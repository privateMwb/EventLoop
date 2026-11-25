// EventLoop build smoke test suite.
//
// Coverage:
// - Library version string is reported
// - ThreadPoolPro, FunctionPro and PoolPro (PoolAllocator) headers
//   compile and link into the EventLoop target

#include <support/framework.h>

using namespace EventLoop;

// Verifies the library reports its version.
static void version_is_reported() {
    CHK(version() == "0.1.0");
}

// Verifies a ThreadPoolPro task runs and returns its result.
static void thread_pool_runs_task() {
    ThreadPoolPro::ThreadPool pool(2);
    auto result = pool.enqueue([] { return 42; });
    CHK(result.get() == 42);
}

// Verifies a FunctionPro callable can be bound and invoked.
static void function_pro_invokes_callable() {
    FunctionPro::Function<int()> f = [] { return 7; };
    CHK(f() == 7);
}

// Verifies a PoolPro block can be allocated and returned.
static void pool_allocates_block() {
    PoolPro::Pool<> pool(64, 4);
    void* block = pool.allocate();
    CHK(block != nullptr);
    pool.deallocate(block);
}

static void run_tests() {
    RUN(version_is_reported);
    RUN(thread_pool_runs_task);
    RUN(function_pro_invokes_callable);
    RUN(pool_allocates_block);
}

REGISTER_TEST_SUITE();
