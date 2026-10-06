#![cfg_attr(coverage_nightly, feature(coverage_attribute))]
#![cfg_attr(docsrs, feature(doc_cfg))]

//! Hardware-aware worker pool for processor-local and fixed processor-set execution.
//!
//! By default, each task is executed on the same processor that spawned it, ensuring optimal cache
//! locality and minimizing cross-processor data movement. A pool can instead be configured with a
//! fixed [`many_cpus::ProcessorSet`] whose workers share tasks across selected processors.
//!
//! # Quick start
//!
//! ```rust
//! use vicinal::Pool;
//! # use futures::executor::block_on;
//!
//! # block_on(async {
//! let pool = Pool::new();
//! let scheduler = pool.scheduler();
//!
//! let task1 = scheduler.spawn(|| 42);
//! let _task2 = scheduler.spawn(|| println!("doing some stuff"));
//!
//! assert_eq!(task1.await.unwrap(), 42);
//!
//! let scheduler2 = scheduler.clone();
//! let task3 = scheduler.spawn(move || scheduler2.spawn(|| 55));
//!
//! assert_eq!(task3.await.unwrap().await.unwrap(), 55);
//! # });
//! ```
//!
//! # Tradeoffs
//!
//! - **Single task latency on an idle pool** is prioritized. The expectation is that tasks are
//!   short-lived so that the pool is often idle.
//! - A fixed processor-set pool can be used as a general-purpose blocking pool, but it does not
//!   grow dynamically. Every blocked task occupies one of the configured workers.
//!
//! # Shutdown behavior
//!
//! When the [`Pool`] is dropped, it signals all worker threads to shut down and waits for any
//! currently-executing tasks to complete. Queued tasks that have not started execution are
//! abandoned.
//!
//! If you need to ensure all spawned tasks complete before shutdown, await their [`JoinHandle`]s
//! before dropping the pool.
//!
//! [`Pool::start_shutdown`] starts a non-blocking timed shutdown. New tasks are rejected
//! immediately. Urgent tasks drain first for their configured timeout, followed by normal tasks.
//!
//! # Panics
//!
//! If a task panics, the panic is captured and returned as a [`JoinError`].
//!
//! # Platform support
//!
//! The package is tested on the following operating systems:
//!
//! * Windows 11 x64
//! * Windows Server 2022 x64
//! * Ubuntu 24.04 x64
//!
//! On non-Windows non-Linux platforms (e.g. mac OS), the package will not uphold the processor
//! locality guarantees, but will otherwise function correctly as a worker pool.

mod constants;
mod join_handle;
mod metrics;
mod pool;
mod processor_registry;
mod processor_state;
mod scheduler;
mod task;
mod worker;

pub(crate) use constants::NEVER_POISONED;
pub use join_handle::*;
pub(crate) use pool::PoolInner;
pub use pool::*;
pub(crate) use processor_registry::ProcessorRegistry;
pub(crate) use processor_state::{ProcessorState, ShutdownPhase};
pub use scheduler::*;
pub(crate) use task::*;
pub(crate) use worker::*;

#[cfg(test)]
::testing::set_allocator!();
