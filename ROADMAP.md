# EventLoop — Library Roadmap

*(Placeholder title — no repo name picked yet, same as "DnsCheckup"
before it became "DnsLint". Candidates: `EventLoop`, `ReactorPro`,
`CoroLoop`.)*

## Goal

An async I/O runtime for C++20: an `epoll` reactor, coroutine-based
`Task<T>` and awaitables, timers, and a multi-threaded mode with one
loop per thread. Handler code reads like blocking code
(`auto n = co_await socket.read(buf);`) but a handful of threads can
hold 10k+ connections open.

This is a library, not an app. Its proof is a before/after benchmark
against `FalconHTTP`'s thread-per-connection backend, and a follow-up
app (live chat server) that could not exist without it.

## Dependencies

```
external/
├── threadpoolpro/   (reused as-is — blocking work is offloaded here)
├── falconhttp/      (extended — second backend behind a flag)
├── functionpro/     (reused as-is — type-erased callbacks, no std::function)
└── poolallocator/   (reused as-is — optional, for coroutine frame allocation)
```

Depends on nothing landing first, but Phase 6 (the `FalconHTTP`
integration) needs `falconhttp-eventloop-backend-roadmap.md` written
before it starts. This library is where the runtime gets built, not
where `FalconHTTP` gets changed.

## Phases

### Phase 0 — Repo scaffold

Header-and-source layout, `CMakeLists.txt` confirming a real build
against all submodules, CI with sanitizers (ASan, TSan, UBSan) and
CodeQL from day one. Same "read the real headers before writing a line
of new code" discipline used throughout DnsLint.

### Phase 1 — Single-threaded reactor, no coroutines

Raw `epoll` wrapper: register a file descriptor, wait for readiness,
dispatch a callback. Built into a plain echo server using only
callbacks. Proves the core loop (register, wait, dispatch, remove)
works before any coroutine machinery exists. Cheapest place to catch a
bug, and it settles edge-triggered vs level-triggered behavior early.

### Phase 2 — `Task<T>` and the coroutine layer

`Task<T>` with a promise type, a way to start one, and a way to await
one from another. Lifetime rules get written down here, not discovered
later: who owns the frame, what happens if a `Task` is destroyed
mid-suspend, and what a dangling reference across `co_await` looks
like. Tested with deliberate misuse under ASan.

### Phase 3 — Socket awaitables

`co_await socket.read(buf)`, `write`, `accept`, `connect`. Each one
tries the syscall first, and only suspends on `EAGAIN`, registering
with the reactor and resuming when the descriptor is ready. The Phase 1
echo server is rewritten with coroutines and must behave identically.

### Phase 4 — Timers and cancellation

A timer heap (or wheel; see open questions) driving
`co_await sleep(50ms)` and `withTimeout(task, 200ms)`. Cancellation is
the hard part: a timed-out read must unregister cleanly, and a
cancelled `Task` must not resume. Tested with races between timer
expiry and I/O completion.

### Phase 5 — Multi-threaded mode

One loop per thread, with `SO_REUSEPORT` so the kernel spreads incoming
connections across them. A `ThreadPoolPro` bridge so a handler can
`co_await` blocking work (a `MiniDatabase` save, a hash, a slow parse)
without freezing every connection on its loop. This is the part that
needs the most careful design: a blocking call in a handler is the
easiest way to break the whole model.

### Phase 6 — `FalconHTTP` integration and benchmark

Add the EventLoop backend to `FalconHTTP` behind a
`--backend=threads|eventloop` flag. Keep the threaded backend; do not
delete it. Benchmark both on identical code at 100, 1k and 10k
concurrent connections: requests/sec, p50/p99 latency, memory, thread
count. Methodology and hardware documented next to the numbers.
`Shrtn`, `DnsLint` and `FalconEye` inherit the new backend with no
changes of their own.

### Phase 7 — Release polish

Stretch scope, order TBD once Phases 0–6 are solid:
- `io_uring` backend behind the same interface, benchmarked against
  `epoll`.
- Fuzzing for the socket and timer layers.
- A written "pitfalls" page: coroutine lifetime bugs, cancellation,
  edge- vs level-triggered `epoll`, and why blocking inside a handler
  stalls a loop.
- A small `docs/` diagram of the loop, the awaitable lifecycle and the
  thread layout.

## Follow-up app (separate roadmap)

Live chat server on top of this library: rooms, presence, message
fan-out over WebSockets, a `FalconEye`-style stats panel, and a load
test that connects 10k fake users. Needs WebSocket support in
`FalconHTTP` first. Gets its own roadmap once Phase 6 lands.

## Open questions (flag, don't silently decide)

- **Naming** — still open.
- **Platform scope** — `epoll` is Linux-only. Supporting `kqueue`
  (macOS/BSD) or IOCP (Windows) would widen reach but triples the
  reactor surface. Worth deciding after Phase 3, not now.
- **Timer structure** — a binary heap is simpler; a hierarchical timer
  wheel gives O(1) insert at scale. Decide once Phase 4 is being built,
  with a benchmark rather than a guess.
- **Coroutine frame allocation** — default heap allocation is simplest;
  `PoolAllocator`-backed frames cut allocator pressure but add
  complexity. Measure first.
- **Compiler baseline** — C++20 coroutines have uneven quirks across
  GCC, Clang and MSVC versions. Pick the supported minimums at Phase 0
  and enforce them in CI.
- **Deployment story** — same "not yet deployed, mechanics only"
  honesty DnsLint's own release notes used. The benchmark needs a Linux
  host, and Vercel can't run it; not solving this prematurely.
