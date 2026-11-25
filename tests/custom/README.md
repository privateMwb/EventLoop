# Test Suite

Custom framework: `CHK()` assertions, `RUN()` per case, `REGISTER_TEST_SUITE()` at file end. Suites live under `suite/<category>/`.

| Category | Focus |
|---|---|
| Unit | Individual functions or components in isolation (reactor, sockets, timers, timeouts, timer queues) |
| Integration | Echo servers on real sockets; one shared scenario set (`common/echoHarness.h`) holds the callback and coroutine servers to identical behaviour |
| Lifecycle | Ownership and destruction of Tasks and Sockets, including mid-suspend |
| Concurrency | Tasks resumed across threads |
| Regression | Bugs that must stay fixed, and the randomized timeout/I-O/cancellation race test that checks every outcome against a model |

`unit/build.cpp` proves the library and its submodules compile and link.

Deliberate-misuse programs live in `tests/misuse/` and are built with `-DBUILD_MISUSE_TESTS=ON`; each must be caught by AddressSanitizer (see `docs/Lifetimes.md`).
