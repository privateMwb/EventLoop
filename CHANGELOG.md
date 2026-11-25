# Changelog

## Unreleased

- Phase 4: reactor timers (`Reactor::at/after/cancelTimer`, replaceable clock) on an indexed binary heap; `sleepFor`/`sleepUntil`; `withTimeout` and `TimeoutError`; `Task::cancel()`; per-operation `.within()` limits on `Socket` reads, writes and accepts. `Reactor::runOnce()` now rejects re-entry. Behaviour changes: closing a `Socket` now wakes pending operations with `ECANCELED` (was: abandoned), and `accept()` backs off with a timer on descriptor exhaustion (was: waited for the next connection). Heap chosen over a timing wheel by benchmark against a rule fixed in advance; the wheel is kept tested as a candidate. Benchmarks added (`-DBUILD_BENCHMARKS=ON`). See `docs/Timers.md`.

- Phase 3: `Socket` with awaitable `read`, `write`, `writeAll`, `accept` and `Socket::connect`, plus `Endpoint` (numeric IPv4/IPv6). One edge-triggered registration per socket; each operation tries its syscall first and the dispatcher retries it on readiness, so stale events are harmless. A per-socket fairness budget yields through the new `Reactor::defer()`. Coroutine echo server (`examples/coroutine_echo`) passes the same scenarios as the callback server via `tests/common/echoHarness.h`. See `docs/Sockets.md`.

- Phase 2: `Task<T>` (lazy, move-only, owns its frame, symmetric transfer), `spawn`, `syncWait`, `Task::start/result`; lifetime rules in `docs/Lifetimes.md`; ASan misuse programs (`-DBUILD_MISUSE_TESTS=ON`) for a dangling view, a destroyed lambda closure, and a stale resume. Known limit: GCC only guarantees symmetric transfer at -O2 and up.

- Phase 1: `Reactor` (epoll wrapper: add/modify/remove, `runOnce`/`run`/`stop`), `Event` flags, `UniqueFd` and `setNonBlocking`, and a callback-only `EchoServer` example (`examples/callback_echo`). Level-triggered by default, `Event::Edge` opt-in. Dispatch is safe against callbacks that remove their own or other descriptors, and against descriptor-number reuse within a batch.

- Phase 0: repo scaffold, submodule wiring, link smoke tests, CI (build matrix, ASan+UBSan, TSan, CodeQL).
