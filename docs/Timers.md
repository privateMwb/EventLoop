# Timers, timeouts and cancellation

## The timer queue: heap, not wheel

The reactor keeps its timers in an **indexed binary heap**
(`include/EventLoop/detail/TimerHeap.h`). A hierarchical timing wheel
(`benchmarks/custom/candidates/TimerWheel.h`) was built and benchmarked
against it first, as the roadmap asked.

### The rule, written before the first run

It is at the top of `benchmarks/custom/suite/scaling/timerChurn.cpp`:

> The wheel replaces the heap only if (a) churn at 100K live timers is at
> least **2.0x** faster than the heap, **and** (b) no churn or rolling case at
> 1K live timers is more than 1.2x slower. Otherwise the heap stays: it is
> simpler, and complexity has to be paid for by a clear win.

### Result

Five runs of `./benchmarks`, median of *heap time / wheel time* (above 1 means
the wheel is faster). The framework runs each case at three iteration counts
on the same live state; the first two include cold-cache warm-up, the 1M column
is the steady state.

| Case | 10K iters | 100K iters | 1M iters | 1M range |
|---|---|---|---|---|
| churn, 1K live | 2.01 | 2.33 | 2.37 | 2.27–2.65 |
| churn, 10K live | 1.15 | 2.48 | 2.45 | 2.09–2.54 |
| **churn, 100K live** | 1.19 | 1.63 | **1.81** | 1.30–2.15 |
| rolling, 1K live | 2.19 | 2.16 | 2.19 | 1.85–2.27 |
| rolling, 10K live | 1.82 | 2.52 | 2.46 | 2.21–2.51 |
| rolling, 100K live | 1.32 | 1.72 | 1.81 | 1.73–2.15 |
| next-deadline, 10K | 1.51 | 2.07 | 2.03 | 1.36–2.08 |
| next-deadline, 100K | 2.09 | 1.99 | 2.06 | 1.31–2.10 |

*churn* arms a timer and cancels another at a steady live count (timeouts that
never fire); *rolling* arms one per simulated millisecond and expires them
(timeouts that do fire); *next-deadline* is the call made before every wait.

**The wheel was faster in every measurement, but the rule's bar was 2.0x on
churn at 100K live and the median is 1.81x (range 1.30–2.15), so the heap
stays.** The rule did not say which iteration count to read; the steady-state
column is the fairest, and the wheel misses the bar in all three.

Why the margin is not worth taking: at 100K live timers the difference is
roughly 85 ns per arm-and-cancel pair (about 150 ns for the heap against about
70 ns for the wheel in the first run). Even 100,000 timer operations a second is
under 1% of one core. At 100K live both structures are limited by cache misses
on random nodes, which is why the wheel's relative lead shrinks there.

### Caveats

- One shared single-core sandbox, g++ 13, `-O3`. Not Termux, not ARM. The
  ratios are the finding; the absolute times are not portable. Re-run
  `-DBUILD_BENCHMARKS=ON` on the target before trusting either.
- Fake time: nothing is actually slept. Insert, cancel and expiry costs only.
- **Revisit** if Phase 6's benchmark shows timer cost in a profile. The wheel
  stays under test (`tests/custom/suite/unit/timerQueues.cpp` runs every
  scenario on both, plus a randomized comparison against a model), so
  switching is a one-line change in `Reactor.h`.

### Correctness of both

36,000 randomized operations per queue are compared with a model across every
wheel level, the cascades, and a deadline beyond the wheel's reach. Mutation
checks on the wheel (skipped cascade level, off-by-one event skip, wrong
due-now boundary) each turn the suite red. Two other mutations survived and
turned out to be equivalent mutants: the wheel's current slot is always empty,
so a timer exactly 64 blocks ahead may share its index, and the order in which
levels cascade within one tick does not matter.

## Semantics

- **Clock.** `Reactor::now()` is `steady_clock` unless a replacement was given to
  the constructor. Tests pass a `ManualClock` and use `runOnce(0)`.
- **Resolution.** The epoll timeout is milliseconds, rounded **up**. A timer
  never fires early and may fire about a millisecond late, plus whatever the
  loop is busy with.
- **Order inside one `runOnce()`:** I/O events, then timers, then deferred
  callbacks. Timers and callbacks armed while their own pass is running wait
  for the next pass, so a timer that re-arms itself cannot starve I/O.
- **Handles** (`TimerId`) are generation-checked. Cancelling a handle after
  its timer fired, or after the slot was reused, returns false and touches
  nothing.
- **`runOnce()` is not re-entrant.** Calling it from a callback, timer or
  coroutine run by the same reactor throws `std::logic_error`. Cancellation
  depends on this (below).

## Sleeping

```cpp
co_await sleepFor(reactor, 50ms);
co_await sleepUntil(reactor, deadline);
```

A deadline that has already passed does not suspend, so `sleepFor(0ms)` is not
a way to yield. Destroying a sleeping Task cancels its timer.

## Time limits

Two tools, for two shapes of work.

**`.within(duration)` on a socket operation** fails that one operation with
`ETIMEDOUT` (`std::errc::timed_out`). No extra coroutine frame, no
cancellation of anything else:

```cpp
std::size_t n = co_await client.read(buffer).within(200ms);
```

If the operation could complete at the moment its limit passes, it completes
instead of timing out: the timer handler tries the system call once first.

**`withTimeout(reactor, task, duration)`** limits a whole `Task`. If the limit
passes first, the task is **cancelled** and `TimeoutError` (a
`std::system_error` with `std::errc::timed_out`) is thrown:

```cpp
auto reply = co_await withTimeout(reactor, fetch(host), 2s);
```

It cannot interrupt a task that never suspends: the limit is only checked when
control returns to the reactor.

## Cancellation

Cancellation is **destruction**: `Task::cancel()` (or letting the Task go)
destroys the frame wherever it is suspended. Locals are destroyed in order,
nested child Tasks are destroyed with it, and every awaiter in flight
deregisters in its destructor (`Lifetimes.md`, rule 5). That is why nothing
can resume a cancelled task later: nothing is left registered anywhere.

`withTimeout` uses exactly this. When its timer fires it cancels the child and
resumes the waiting parent, from a timer callback. That is safe because timer
callbacks only run from the reactor loop, never while a coroutine executes
(the re-entrancy guard enforces it).

### Ordering rules (all tested)

These decide who wins when several things become true in the same
millisecond. They are checked by a randomized test that predicts the exact
outcome of 480 racing reads (`regression/timeoutRaces.cpp`), and that test
fails when the reactor runs timers before I/O.

1. A cancellation applied before a loop iteration beats everything in it.
2. Within an iteration, I/O completions are handled before timers. Data that
   arrives in the millisecond a limit expires wins.
3. A `.within()` operation that could complete when its limit passes completes.
4. On an exact tie between a task's own timer and its `withTimeout` limit,
   the task wins (it armed its timer first).
5. A task that finishes without ever suspending succeeds even with a zero
   limit; one that has to wait does not.

### Sockets

- **Closing a socket** wakes every operation pending on it with `ECANCELED`
  (`std::errc::operation_canceled`). The wake is queued with
  `Reactor::defer()` and delivered from the loop: it is never inline, because
  `close()` may be running inside a destructor. This replaces Phase 3's
  "abandoned, never resumed".
- **`accept()` out of descriptors** no longer waits for the next connection:
  it retries every 100 ms on a timer, and recovers when descriptors are free.
  Tested by forcing `EMFILE` with `RLIMIT_NOFILE`.
