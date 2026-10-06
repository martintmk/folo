# vicinal

Hardware-aware worker pool for processor-local and fixed processor-set execution.

By default, each task is executed on the same processor that spawned it, ensuring optimal cache
locality and minimizing cross-processor data movement. A fixed processor set can instead share one
task queue across multiple processors.

## Example

```rust
use vicinal::Pool;

#[tokio::main]
async fn main() {
    let pool = Pool::new();
    let scheduler = pool.scheduler();

    let task1 = scheduler.spawn(|| 42);
    let _task2 = scheduler.spawn(|| println!("doing some stuff"));

    assert_eq!(task1.await.unwrap(), 42);

    let scheduler2 = scheduler.clone();
    let task3 = scheduler.spawn(move || scheduler2.spawn(|| 55));

    assert_eq!(task3.await.unwrap().await.unwrap(), 55);
}
```

## Configuration

```rust
use std::time::Duration;

use many_cpus::SystemHardware;
use new_zealand::nz;
use vicinal::Pool;

fn multi_processor_configuration() {
    let pool = Pool::builder()
        .processors(SystemHardware::current().processors())
        .max_workers(nz!(4_usize))
        .on_thread_start(initialize_worker)
        .build();

    // Returns immediately. New tasks are rejected from this point onward.
    pool.start_shutdown(Duration::from_secs(5), Duration::from_secs(30));
}

fn initialize_worker() {}
```

`JoinHandle` resolves to `Result<T, JoinError>`. The opaque error reports whether the task
panicked or was abandoned during shutdown.

## Tradeoffs

- **Single task latency on an idle pool** is prioritized. The expectation is that tasks are
  short-lived so that the pool is often idle.
- A fixed processor-set pool can be used as a general-purpose blocking pool, but it does not grow
  dynamically. Every blocked task occupies one of the configured workers.

## Platform support

The package is tested on the following operating systems:

* Windows 11 x64
* Windows Server 2022 x64
* Ubuntu 24.04 x64

On non-Windows non-Linux platforms (e.g. mac OS), the package will not uphold the processor
locality guarantees, but will otherwise function correctly as a worker pool.

## See also

More details in the [package documentation](https://docs.rs/vicinal/).

This is part of the [Folo project](https://github.com/folo-rs/folo) that provides mechanisms for
high-performance hardware-aware programming in Rust.
