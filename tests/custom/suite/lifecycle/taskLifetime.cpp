// EventLoop::Task lifetime test suite.
//
// Coverage:
// - The Task owns its frame: destroying it mid-suspend runs destructors of
//   everything alive in the body, children included
// - Parameters are owned by the frame
// - Awaiters that deregister make destruction safe against late resumption
// - The same guarantee against a real Reactor registration

#include <support/framework.h>

// clang-format off
#include <coroutine> // std::coroutine_handle — the awaiter below
#include <utility>   // std::exchange — Tracker move
// clang-format on

using namespace EventLoop;

/// Counts how many live (not moved-from) Trackers have been destroyed.
struct Tracker {
    explicit Tracker(int& counter) noexcept : counter_(&counter) {}
    Tracker(Tracker&& other) noexcept : counter_(std::exchange(other.counter_, nullptr)) {}
    Tracker(const Tracker&) = delete;
    Tracker& operator=(const Tracker&) = delete;
    ~Tracker() {
        if (counter_ != nullptr) {
            ++*counter_;
        }
    }

  private:
    int* counter_;
};

static Task<void> localThenWait(Gate& gate, int& destroyed) {
    Tracker local(destroyed);
    co_await gate;
}

static Task<void> childWaits(Gate& gate, int& destroyed) {
    Tracker childLocal(destroyed);
    co_await gate;
}

static Task<void> parentOfChild(Gate& gate, int& destroyed) {
    Tracker parentLocal(destroyed);
    co_await childWaits(gate, destroyed);
}

static Task<void> ownsParameter(Tracker tracker) {
    co_return;
}

static Task<void> flagAfterGate(Gate& gate, bool& flag) {
    co_await gate;
    flag = true;
}

/// Test-only awaiter: suspends until `fd` is readable on `reactor`, and
/// removes its registration if destroyed first (rule 5).
class ReadableAwaiter {
  public:
    ReadableAwaiter(Reactor& reactor, int fd) noexcept : reactor_(reactor), fd_(fd) {}
    ReadableAwaiter(const ReadableAwaiter&) = delete;
    ReadableAwaiter& operator=(const ReadableAwaiter&) = delete;

    ~ReadableAwaiter() {
        if (registered_) {
            reactor_.remove(fd_);
        }
    }

    [[nodiscard]] bool await_ready() const noexcept {
        return false;
    }

    void await_suspend(std::coroutine_handle<> handle) {
        reactor_.add(fd_, Event::Readable, [handle](int, Event) { handle.resume(); });
        registered_ = true;
    }

    void await_resume() {
        reactor_.remove(fd_);
        registered_ = false;
    }

  private:
    Reactor& reactor_;
    int fd_;
    bool registered_ = false;
};

static Task<void> waitReadable(Reactor& reactor, int fd, bool& flag) {
    co_await ReadableAwaiter(reactor, fd);
    flag = true;
}

// Verifies destroying a suspended Task destroys the body's locals.
static void destroy_suspended_runs_destructors() {
    Gate gate;
    int destroyed = 0;
    {
        Task<void> task = localThenWait(gate, destroyed);
        task.start();
        CHK(destroyed == 0);
        CHK(gate.hasWaiter());
    }
    CHK(destroyed == 1);
}

// Verifies the suspended awaiter deregisters, so a late open() is harmless.
static void late_open_after_destroy() {
    Gate gate;
    int destroyed = 0;
    {
        Task<void> task = localThenWait(gate, destroyed);
        task.start();
    }
    CHK(!gate.hasWaiter());

    gate.open(); // Nothing is registered, so nothing is resumed.

    CHK(destroyed == 1);
}

// Verifies destroying a parent destroys the child it was awaiting.
static void destroy_parent_destroys_child() {
    Gate gate;
    int destroyed = 0;
    {
        Task<void> parent = parentOfChild(gate, destroyed);
        parent.start();
        CHK(destroyed == 0);
    }
    CHK(destroyed == 2); // Child local and parent local.
}

// Verifies a Task that never ran still destroys its by-value parameters.
static void never_started_frees_parameters() {
    int destroyed = 0;
    {
        Task<void> task = ownsParameter(Tracker(destroyed));
        CHK(destroyed == 0);
    }
    CHK(destroyed == 1);
}

// Verifies a finished Task destroys cleanly and its locals ran once.
static void finished_task_destroys_cleanly() {
    Gate gate;
    gate.open();
    int destroyed = 0;
    {
        Task<void> task = localThenWait(gate, destroyed);
        task.start();
        CHK(task.done());
        CHK(destroyed == 1); // Local left scope when the body finished.
    }
    CHK(destroyed == 1);
}

// Verifies a resumed Task completes normally when it is not destroyed.
static void reactor_resumes_waiting_task() {
    Reactor reactor;
    Pipe pipe = makePipe();
    bool flag = false;
    Task<void> task = waitReadable(reactor, pipe.readEnd.get(), flag);
    task.start();
    CHK(reactor.contains(pipe.readEnd.get()));

    writeByte(pipe.writeEnd.get());

    CHK(reactor.runOnce(1000) == 1);
    CHK(flag);
    CHK(task.done());
    CHK(!reactor.contains(pipe.readEnd.get()));
}

// Verifies destroying a Task suspended on the reactor leaves no stale resume.
static void destroy_task_unregisters_fd() {
    Reactor reactor;
    Pipe pipe = makePipe();
    bool flag = false;
    {
        Task<void> task = waitReadable(reactor, pipe.readEnd.get(), flag);
        task.start();
        CHK(reactor.contains(pipe.readEnd.get()));
    }
    CHK(!reactor.contains(pipe.readEnd.get())); // Awaiter removed itself.

    writeByte(pipe.writeEnd.get());

    CHK(reactor.runOnce(0) == 0); // Nothing resumes the destroyed frame.
    CHK(!flag);
}

static void run_tests() {
    RUN(destroy_suspended_runs_destructors);
    RUN(late_open_after_destroy);
    RUN(destroy_parent_destroys_child);
    RUN(never_started_frees_parameters);
    RUN(finished_task_destroys_cleanly);
    RUN(reactor_resumes_waiting_task);
    RUN(destroy_task_unregisters_fd);
}

REGISTER_TEST_SUITE();
