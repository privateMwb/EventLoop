# Task lifetimes

The rules for `Task<T>`, written down before anything depends on them.
Each rule that can be broken has a deliberate-misuse program in
`tests/misuse/` that AddressSanitizer must catch (enable with
`-DBUILD_MISUSE_TESTS=ON`).

## 1. The Task owns the frame

A `Task` is a move-only owner of one coroutine frame. Destroying the `Task`
destroys the frame, wherever the body is suspended. A moved-from or default
`Task` owns nothing (`valid()` is false).

Tasks are **lazy**: the body does not run until the Task is awaited,
`spawn`ed, `syncWait`ed or `start()`ed. A Task destroyed without ever running
runs no body code, but still destroys its parameters.

A *finished* Task's frame, parameters included, lives until the Task object is
destroyed. A coroutine that must release a resource on completion (close a
socket, say) has to do it explicitly rather than rely on a by-value parameter
being destroyed.

## 2. Ways to run a Task

| How | Owner afterwards | Notes |
|---|---|---|
| `co_await std::move(task)` | the awaiting coroutine's frame | The normal way. Result or exception is delivered at the `co_await`. |
| `spawn(std::move(task))` | nobody (self-destroying) | Runs eagerly to the first suspension. No handle, no cancellation yet. An escaping exception calls `std::terminate`. |
| `syncWait(std::move(task))` | the call | Blocks the calling thread. Never call it from the only thread that can resume the task (a reactor's own loop thread). |
| `task.start()` | you keep the `Task` | Runs to the first suspension. Later `done()` / `result()`. This is the mode that lets you destroy a suspended task. |

Only an rvalue `Task` can be awaited. `co_await task` on an lvalue is a
compile error, because awaiting consumes the result.

Misuse that is detected and reported as `std::logic_error` (not undefined
behaviour): awaiting or starting an empty Task, starting a Task twice,
awaiting a Task that was already `start()`ed but has not finished, and taking
a result twice.

## 3. Parameters are copied into the frame; references are not owned

A coroutine copies its parameters into the frame, but a reference, pointer,
`std::string_view` or `std::span` parameter copies only the *address*. The
referent must outlive the entire coroutine, not just the call.

```cpp
Task<int> firstChar(std::string_view text, Gate& gate) { co_await gate; co_return text[0]; }
auto t = firstChar(std::string(200, 'x'), gate);   // temporary dies here
// ... later, after resume: text[0] reads freed memory
```

**Rule:** take owning types by value in a coroutine (`std::string`, not
`std::string_view` or `const std::string&`). Demonstrated by
`tests/misuse/danglingView.cpp`.

## 3b. Lambda coroutines keep their captures in the closure, not the frame

The frame stores only a pointer to the lambda object. If the closure is
destroyed while its coroutine is suspended, the coroutine reads dead
captures.

**Rule:** never `co_await` inside a lambda coroutine whose closure may die
first (a temporary, a moved-from object). Pass state as parameters instead.
Demonstrated by `tests/misuse/destroyedClosure.cpp`.

## 4. Results and exceptions

- The result is moved out at the `co_await`, exactly once.
- An exception escaping the body is stored and rethrown at the `co_await`.
- A Task whose exception is never observed (never awaited, destroyed after
  failing) drops it silently.

## 5. Destroying a Task mid-suspend

Destroying a suspended `Task` is allowed and safe, provided every awaitable
follows this contract:

> **An awaitable that hands the coroutine handle to something external (a
> reactor registration, a timer, a queue) must remove that hand-off in its
> destructor.**

Awaiter objects live in the coroutine frame, so their destructors run when
the frame is destroyed. On destruction, in order:

1. locals alive at the suspension point are destroyed (RAII holds);
2. the awaiter in flight is destroyed, deregistering itself;
3. a child `Task` being awaited is destroyed the same way, recursively.

An awaitable that skips step 2 leaves a dangling handle; resuming it later
resumes freed memory. Demonstrated by `tests/misuse/staleResume.cpp`. The
well-behaved version (`Gate`) and a real `Reactor` awaiter are exercised in
`tests/custom/suite/lifecycle/taskLifetime.cpp`.

`Task::cancel()` is this destruction under a clearer name (see rule 10).

Never destroy a Task whose body is **currently executing** (from inside the
coroutine itself, or while another thread is running it). That is undefined
behaviour. Destroy only suspended or finished tasks.

## 6. Detached tasks

A `spawn`ed task owns itself. If whatever it waits on never fires, its frame
is never freed (it lives until the process exits), and there is still no
handle to cancel it with. To get cancellation, keep the Task instead: `start()`
it and hold it (as the coroutine echo server does), or wrap the work in
`withTimeout()`, which cancels the task it limits.

## 7. Threads

A `Task` is not thread-safe: resume it from one thread at a time. A task may
be resumed on a different thread from the one that started it (that is how
`syncWait` across threads works); the resuming thread is responsible for the
happens-before edge with the thread that registered the handle, for example a
mutex or the reactor's own queue.

## 8. Stack depth and symmetric transfer

Awaiting a child and returning to the parent use symmetric transfer, so a
chain of awaits does not grow the stack. **GCC only guarantees the tail call
when optimising (-O2 and up).** In a GCC Debug or -O1 build, a chain of
roughly 100,000 nested awaits overflows an 8 MiB stack; the test suite keeps
its always-on depth at 10,000 and checks the flat-stack property with 200,000
only in `NDEBUG` builds. Clang is expected to honour it at any level, but
that is not yet verified here.

## 9. Sockets

Operations on a `Socket` follow rule 5. See `Sockets.md` for what closing or
destroying a socket under a pending operation means, and for the other
socket-specific rules.

## 10. Cancellation and timeouts

Cancelling a Task means destroying its frame (`Task::cancel()`, or just
dropping the Task). Everything in rule 5 applies, which is why a cancelled
task is never resumed afterwards.

Two situations make that safe to do from a callback:

- A reactor timer callback (as `withTimeout()` uses) or I/O callback may
  cancel a Task, because it only runs from the reactor loop while no coroutine
  executes. `Reactor::runOnce()` throws `std::logic_error` if called from inside
  its own dispatch, so a nested loop cannot break this.
- A destructor may not resume coroutines, so closing a `Socket` queues its
  pending operations' wake-up with `Reactor::defer()` instead of resuming
  them inline.

`withTimeout()` owns the task it limits. Its limit passing cancels the child
and throws `TimeoutError`; the child finishing first cancels the timer. Both
the timer and the child are destroyed with the waiting parent. Details and the
ordering rules for simultaneous events are in `Timers.md`.
