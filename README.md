# EventLoop

> Placeholder name — see ROADMAP.md, open questions. Candidates: `EventLoop`, `ReactorPro`, `CoroLoop`.

Async I/O runtime for C++: an `epoll` reactor, coroutine `Task<T>` and awaitables, timers, and one loop per thread. Linux only.

**Status:** Phase 4 — `epoll` reactor, `Task<T>`, awaitable sockets, and timers: `sleepFor`, `withTimeout`, per-operation `.within()` limits, and task cancellation. Two echo servers, callback and coroutine, pass identical tests. Single-threaded.

## Build

```sh
git submodule update --init   # or scripts/add_submodules.sh on a fresh repo
cmake -S . -B build
cmake --build build --parallel
./build/tests
./build/echo_coroutine 7000   # then: nc 127.0.0.1 7000 (echo_callback is the Phase 1 version)
```

Requires GCC >= 12 or Clang >= 16, CMake >= 3.20, C++23.

See `ROADMAP.md` for the phase plan and `docs/Lifetimes.md` for the `Task` ownership and misuse rules, `docs/Sockets.md` for socket semantics, and `docs/Timers.md` for timers, timeouts, cancellation and the heap-vs-wheel decision.
