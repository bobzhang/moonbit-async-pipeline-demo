# moonbit-async-pipeline-demo

A small demo of [`moonbitlang/async`](https://github.com/moonbitlang/async): a producer-worker-collector pipeline that converts numbers to FizzBuzz concurrently across 4 worker tasks.

```
producer ──► input queue ──► worker × 4 ──► output queue ──► collector
```

## Run

Requires MoonBit toolchain on Linux/macOS/Windows native target.

```bash
moon run src
```

## What it shows

- `async fn main` as the entry point
- `@async.with_task_group` for **structured concurrency** — when the group returns, every spawned task is guaranteed to have terminated
- `@async.with_timeout` as a hard cap on the whole pipeline
- `@aqueue.Queue` for inter-task message passing, with **`.close()` as the EOF signal** that lets workers exit cleanly via `try ... catch`
- Visible parallel speedup: 15 jobs × ~90 ms avg "think time" ≈ 1.35 s sequentially, finishes in ~430 ms with 4 workers

## Sample output

```
[t+0060ms]  worker 2 -> #02: 2
[t+0116ms]  worker 1 -> #01: 1
[t+0138ms]  worker 3 -> #03: Fizz
[t+0152ms]  worker 4 -> #04: 4
[t+0152ms]  worker 1 -> #06: Fizz
[t+0197ms]  worker 3 -> #07: 7
[t+0211ms]  worker 2 -> #05: Buzz
[t+0252ms]  worker 3 -> #10: Buzz   ← #10 finishes before #8 (out-of-order completion)
[t+0256ms]  worker 4 -> #08: 8
[t+0287ms]  worker 3 -> #12: Fizz
[t+0303ms]  worker 1 -> #09: Fizz
[t+0354ms]  worker 2 -> #11: 11
[t+0371ms]  worker 1 -> #15: FizzBuzz
[t+0389ms]  worker 4 -> #13: 13
[t+0428ms]  worker 3 -> #14: 14

done in 428ms with 4 workers
```

## License

Apache-2.0
