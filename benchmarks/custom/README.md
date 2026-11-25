# Benchmarks

Comparison benchmarks using the ecosystem's custom framework: each `BENCH`
runs an expression for the library's candidate (the "custom" column) against a
reference at 10K, 100K and 1M iterations.

Here the two sides are the timing wheel (candidate) and the binary heap (what
the reactor uses). The decision rule and results are in `docs/Timers.md`.

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DBUILD_BENCHMARKS=ON
cmake --build build --target benchmarks
./build/benchmarks
```

Suites live under `suite/<category>/`:

| Category | Suite | What it measures |
|---|---|---|
| scaling | `timerChurn` | arm + cancel at 1K / 10K / 100K live timers |
| scaling | `timerRolling` | arm + expire at about 1K / 10K / 100K live timers |
| core | `timerNext` | `nextDeadline()`, called before every reactor wait |

Results depend heavily on the machine; compare ratios, and re-run on the
target hardware before drawing conclusions.
