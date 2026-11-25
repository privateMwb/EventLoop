# Sockets

How `Socket` turns readiness into `co_await`, and the rules that follow.
Lifetime rules for `Task` itself are in `Lifetimes.md`; this file only adds
what sockets and the reactor change.

## Model

```cpp
Socket client = co_await Socket::connect(reactor, Endpoint::loopback(7000));
std::size_t n = co_await client.read(buffer);          // 0 means the peer closed
co_await client.writeAll(std::as_bytes(std::span(message)));
```

- `read`, `write`, `writeAll` and `accept` **try the system call first**
  (`await_ready`) and suspend only on `EAGAIN`.
- Failures are thrown as `std::system_error` at the `co_await`.
- `connect` is a `Task<Socket>`: it starts the non-blocking connect, waits for
  completion, and checks `SO_ERROR` / `getpeername` rather than trusting the
  event.
- Addresses are numeric IPv4/IPv6 (`Endpoint`). There is no name resolution.

## Readiness: one edge-triggered registration per socket

A socket registers with its reactor **once**, for read and write, with
`Event::Edge`. No `epoll_ctl` happens per operation.

Edge-triggered delivery is only dangerous if an event can be missed. It
cannot be here, because an operation always runs its system call before
waiting: data that arrived earlier is found by that first call, and data that
arrives after `EAGAIN` produces a fresh edge.

## Who retries: the dispatcher, not the coroutine

When a suspended operation's edge arrives, the reactor callback **runs the
system call itself** and resumes the coroutine only if the call produced a
definitive outcome (data, EOF, or an error). If it still would block, the
operation simply keeps waiting.

This makes spurious and stale events harmless. A stale event can exist: data
arrives (edge queued), the coroutine reads it directly and suspends again,
then the old edge is delivered. A design that resumed on the edge and
retried inside `await_resume` could not handle that, because `await_resume`
cannot suspend again.

## Rules

1. **One waiter per direction.** At most one coroutine waiting to read and one
   waiting to write. A second waiter in the same direction gets
   `std::logic_error`. A reader and a writer together are fine.
2. **Buffers must outlive the operation.** `read`/`write` take spans, which
   are non-owning (`Lifetimes.md`, rule 3). Awaiting immediately in the same
   expression, with the buffer in the same frame, is the safe pattern.
3. **The Reactor must outlive every Socket** (and every task using one).
   `~Socket` calls `Reactor::remove`.
4. **Closing or destroying a Socket wakes pending operations with
   `ECANCELED`.** The wake is queued with `Reactor::defer()` and delivered from
   the loop, never inline, because `close()` may be running inside a
   destructor and the coroutine being woken may own the object. A coroutine
   that holds a `Socket&` to a destroyed socket must not touch it after being
   woken: the wake itself never does.
5. **Destroying a suspended Task is safe.** Each operation deregisters in its
   destructor and cancels its timers; the shared per-socket state outlives the
   `Socket` object for exactly this reason.
6. **Moving a Socket is safe** even with an operation waiting: the reactor
   callback and operations hold the shared state, not the `Socket` object.
7. **A finished Task keeps its parameters alive until it is destroyed.** A
   handler taking `Socket` by value must `close()` it explicitly before
   finishing, or the descriptor stays open until the frame is reaped.

## Time limits

`co_await socket.read(buffer).within(200ms)` fails that operation with
`ETIMEDOUT` if it stays suspended longer, and leaves the socket usable. It
works on `read`, `write`, `writeAll` and `accept`. The limit counts from the
moment the operation suspends, and for `writeAll` it covers the whole write.
For a limit on a whole task, including `connect`, use `withTimeout()`. See
`Timers.md` for the ordering rules when a limit and I/O coincide.

## Fairness

A socket whose operations keep completing immediately (a fast sender, an
always-writable peer) would never return to the loop, starving every other
connection. After **16 consecutive immediate completions** the next
operation suspends and is retried through `Reactor::defer()`, which runs
after the current batch of events. The budget is per socket and resets
whenever the reactor or a deferred retry drives it.

The Phase 1 callback server had the same protection (16 reads per event);
this keeps the coroutine version from silently losing it.

## Known limits

- **Descriptor exhaustion.** `accept()` does not fail on `EMFILE`/`ENFILE`/
  `ENOBUFS`/`ENOMEM`: it retries every 100 ms on a timer until a descriptor is
  free or its own time limit passes. The back-off is a fixed constant, not
  configurable.
- **Cost.** Each socket allocates a `shared_ptr` state, and each operation
  copies the `shared_ptr` (an atomic increment). Cheap, but a measured
  candidate for Phase 7 if it shows up in the benchmark.
- **Frame size.** The coroutine echo server keeps an 8 KiB buffer in each
  connection's frame.
- **Echo server reaping.** Finished connection frames are reaped on the accept
  loop's next pass, so a burst of closes with no new connection leaves their
  (small) frames allocated until one arrives or the server is destroyed.
