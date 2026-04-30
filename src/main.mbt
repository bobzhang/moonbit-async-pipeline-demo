// Fancy async-IO demo: a producer-worker-collector pipeline.
//
//   producer ──► input queue ──► worker × 4 ──► output queue ──► collector
//
// Each worker consumes a number, "thinks" for a randomized 30-150ms,
// converts it to FizzBuzz, and pushes the result downstream. The collector
// prints results in completion order with elapsed wall-clock time, so you
// can see the workers racing in parallel. The whole pipeline is wrapped in
// a 3s timeout as a safety net.

///|
struct Job {
  id : Int
  n : Int
}

///|
struct Done {
  job_id : Int
  worker : Int
  text : String
}

///|
fn fizzbuzz(n : Int) -> String {
  match (n % 3, n % 5) {
    (0, 0) => "FizzBuzz"
    (0, _) => "Fizz"
    (_, 0) => "Buzz"
    _ => n.to_string()
  }
}

///|
let total_jobs : Int = 15

///|
let n_workers : Int = 4

///|
async fn producer(input : @aqueue.Queue[Job]) -> Unit {
  for i in 1..=total_jobs {
    input.put({ id: i, n: i })
    @async.sleep(20) // simulate jobs arriving
  }
  input.close() // signal: no more work
}

///|
async fn worker(
  id : Int,
  rng : @random.Rand,
  input : @aqueue.Queue[Job],
  output : @aqueue.Queue[Done],
) -> Unit {
  // Loop until input.get() raises QueueAlreadyClosed.
  while true {
    let job = try input.get() catch {
      _ => break
    }
    let think_ms = 30 + rng.int(limit=120)
    @async.sleep(think_ms)
    output.put({ job_id: job.id, worker: id, text: fizzbuzz(job.n) })
  }
}

///|
async fn collector(output : @aqueue.Queue[Done], start : Int64) -> Unit {
  for _ in 0..<total_jobs {
    let r = output.get()
    let elapsed = @async.now() - start // ms since pipeline start
    let line = "[t+\{pad4(elapsed.to_int())}ms]  worker \{r.worker} -> #\{pad2(r.job_id)}: \{r.text}\n"
    @stdio.stdout.write(line)
  }
}

///|
fn pad2(n : Int) -> String {
  if n < 10 {
    "0\{n}"
  } else {
    n.to_string()
  }
}

///|
fn pad4(n : Int) -> String {
  let s = n.to_string()
  match s.length() {
    1 => "000\{s}"
    2 => "00\{s}"
    3 => "0\{s}"
    _ => s
  }
}

///|
async fn pipeline() -> Unit {
  let input : @aqueue.Queue[Job] = @aqueue.Queue::new(kind=Unbounded)
  let output : @aqueue.Queue[Done] = @aqueue.Queue::new(kind=Unbounded)
  let rng = @random.Rand::new()
  let start = @async.now()
  @async.with_task_group(group => {
    group.spawn_bg(() => producer(input))
    for w in 1..=n_workers {
      group.spawn_bg(() => worker(w, rng, input, output))
    }
    // Run collector on this task; group returns when collector finishes
    // (all 15 results consumed) and the workers naturally drain.
    collector(output, start)
    output.close()
  })
  let total = @async.now() - start
  @stdio.stdout.write("\ndone in \{total}ms with \{n_workers} workers\n")
}

///|
async fn main {
  // Hard cap on the whole pipeline; if anything wedges, fail loudly.
  @async.with_timeout(3000, pipeline)
}
