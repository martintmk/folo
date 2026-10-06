//! Integration tests for the vicinal worker pool.
//!
//! These tests verify full pool functionality with real threads.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, mpsc};
use std::thread;
use std::time::Duration;

use events_once::EventPool;
use futures::executor::block_on;
use many_cpus::SystemHardware;
use many_cpus::fake::HardwareBuilder;
use new_zealand::nz;
use testing::with_watchdog;
use vicinal::Pool;

#[test]
fn spawn_and_await_value() {
    with_watchdog(|| {
        let pool = Pool::new();
        let scheduler = pool.scheduler();

        let handle = scheduler.spawn(|| 42);
        let result = block_on(handle);

        assert_eq!(result.unwrap(), 42);
    });
}

#[test]
fn spawn_and_await_unit() {
    with_watchdog(|| {
        let pool = Pool::new();
        let scheduler = pool.scheduler();

        let handle = scheduler.spawn(|| {});
        block_on(handle).unwrap();
    });
}

#[test]
fn spawn_and_await_panic() {
    with_watchdog(|| {
        let pool = Pool::new();
        let scheduler = pool.scheduler();

        let handle = scheduler.spawn(|| panic!("intentional panic"));
        assert!(block_on(handle).unwrap_err().is_panic());
    });
}

#[test]
fn clone_scheduler_spawn_from_both() {
    with_watchdog(|| {
        let pool = Pool::new();
        let scheduler1 = pool.scheduler();
        let scheduler2 = scheduler1.clone();

        let handle1 = scheduler1.spawn(|| 1);
        let handle2 = scheduler2.spawn(|| 2);

        assert_eq!(block_on(handle1).unwrap(), 1);
        assert_eq!(block_on(handle2).unwrap(), 2);
    });
}

#[test]
fn nested_spawn_via_captured_scheduler() {
    with_watchdog(|| {
        let pool = Pool::new();
        let scheduler = pool.scheduler();

        let scheduler_clone = scheduler.clone();
        let outer_handle = scheduler.spawn(move || scheduler_clone.spawn(|| 99));

        let inner_handle = block_on(outer_handle).unwrap();
        let result = block_on(inner_handle).unwrap();

        assert_eq!(result, 99);
    });
}

#[test]
fn drop_pool_with_no_tasks() {
    with_watchdog(|| {
        let pool = Pool::new();
        drop(pool);
    });
}

#[test]
fn drop_pool_after_awaiting_task() {
    with_watchdog(|| {
        let pool = Pool::new();
        let scheduler = pool.scheduler();

        let handle = scheduler.spawn(|| 42);
        assert_eq!(block_on(handle).unwrap(), 42);

        drop(pool);
    });
}

#[test]
fn spawn_many_tasks_await_all() {
    // Repeated submissions exercise task/result slot reuse; native runs retain the larger batch.
    const TASK_COUNT: usize = if cfg!(miri) { 4 } else { 100 };

    with_watchdog(|| {
        let pool = Pool::new();
        let scheduler = pool.scheduler();

        for (i, handle) in (0..TASK_COUNT)
            .map(|i| scheduler.spawn(move || i * 2))
            .enumerate()
        {
            assert_eq!(block_on(handle).unwrap(), i * 2);
        }
    });
}

#[test]
fn scheduler_sent_to_another_thread() {
    with_watchdog(|| {
        let pool = Pool::new();
        let scheduler = pool.scheduler();

        let handle = thread::spawn(move || {
            let task_handle = scheduler.spawn(|| 123);
            block_on(task_handle).unwrap()
        });

        let result = handle.join().unwrap();
        assert_eq!(result, 123);
    });
}

#[test]
fn concurrent_spawns_from_multiple_threads() {
    // Keep multiple submissions from every producer under Miri, with native stress coverage.
    const TASKS_PER_PRODUCER: usize = if cfg!(miri) { 2 } else { 25 };
    // Retain competing producers even when interpreting only a small batch from each.
    const PRODUCER_COUNT: usize = 4;

    with_watchdog(|| {
        let pool = Pool::new();
        let scheduler = pool.scheduler();
        let completed = Arc::new(AtomicUsize::new(0));
        let start = Arc::new(Barrier::new(PRODUCER_COUNT));

        let thread_handles: Vec<_> = std::iter::repeat_with(|| {
            let scheduler = scheduler.clone();
            let completed = Arc::clone(&completed);
            let start = Arc::clone(&start);

            thread::spawn(move || {
                start.wait();
                #[expect(
                    clippy::needless_collect,
                    reason = "submit the whole batch before awaiting any task to exercise queue contention"
                )]
                let handles: Vec<_> = (0..TASKS_PER_PRODUCER)
                    .map(|i| {
                        let completed = Arc::clone(&completed);
                        scheduler.spawn(move || {
                            completed.fetch_add(1, Ordering::Relaxed);
                            i
                        })
                    })
                    .collect();

                for (i, handle) in handles.into_iter().enumerate() {
                    assert_eq!(block_on(handle).unwrap(), i);
                }
            })
        })
        .take(PRODUCER_COUNT)
        .collect();

        for handle in thread_handles {
            handle.join().unwrap();
        }

        assert_eq!(
            completed.load(Ordering::Relaxed),
            PRODUCER_COUNT * TASKS_PER_PRODUCER
        );
    });
}

#[test]
fn spawn_and_forget_completes() {
    with_watchdog(|| {
        let pool = Pool::new();
        let scheduler = pool.scheduler();
        let event_pool = EventPool::<()>::new();

        let (tx, rx) = event_pool.rent();
        scheduler.spawn_and_forget(move || {
            // Signal completion via the event.
            tx.send(());
        });

        // Wait for the task to complete.
        block_on(rx).unwrap();
    });
}

#[test]
fn spawn_urgent_and_forget_completes() {
    with_watchdog(|| {
        let pool = Pool::new();
        let scheduler = pool.scheduler();
        let event_pool = EventPool::<()>::new();

        let (tx, rx) = event_pool.rent();
        scheduler.spawn_urgent_and_forget(move || {
            // Signal completion via the event.
            tx.send(());
        });

        // Wait for the task to complete.
        block_on(rx).unwrap();
    });
}

#[test]
fn spawn_and_forget_multiple_tasks() {
    // A small batch still checks independent completion signals; native runs retain the burst.
    const TASK_COUNT: usize = if cfg!(miri) { 4 } else { 50 };

    with_watchdog(|| {
        let pool = Pool::new();
        let scheduler = pool.scheduler();
        let event_pool = EventPool::<()>::new();
        let completed = Arc::new(AtomicUsize::new(0));

        let mut receivers = Vec::new();

        for _ in 0..TASK_COUNT {
            let (tx, rx) = event_pool.rent();
            let completed = Arc::clone(&completed);
            receivers.push(rx);

            scheduler.spawn_and_forget(move || {
                completed.fetch_add(1, Ordering::Relaxed);
                tx.send(());
            });
        }

        // Wait for all tasks to complete.
        for rx in receivers {
            block_on(rx).unwrap();
        }

        assert_eq!(completed.load(Ordering::Relaxed), TASK_COUNT);
    });
}

#[test]
fn spawn_and_await_with_fake_hardware() {
    with_watchdog(|| {
        let hardware =
            SystemHardware::fake(HardwareBuilder::from_counts(nz!(2_usize), nz!(1_usize)));
        let pool = Pool::builder().hardware(hardware).build();
        let scheduler = pool.scheduler();

        let handle = scheduler.spawn(|| 42);
        assert_eq!(block_on(handle).unwrap(), 42);
    });
}

#[test]
fn spawn_many_tasks_with_single_processor_hardware() {
    // Repeated submissions reuse a single processor's state without a large interpreted batch.
    const TASK_COUNT: usize = if cfg!(miri) { 4 } else { 50 };

    with_watchdog(|| {
        let hardware =
            SystemHardware::fake(HardwareBuilder::from_counts(nz!(1_usize), nz!(1_usize)));
        let pool = Pool::builder().hardware(hardware).build();
        let scheduler = pool.scheduler();

        for (i, handle) in (0..TASK_COUNT)
            .map(|i| scheduler.spawn(move || i))
            .enumerate()
        {
            assert_eq!(block_on(handle).unwrap(), i);
        }
    });
}

#[test]
fn fire_and_forget_with_fake_hardware() {
    with_watchdog(|| {
        let hardware =
            SystemHardware::fake(HardwareBuilder::from_counts(nz!(2_usize), nz!(1_usize)));
        let pool = Pool::builder().hardware(hardware).build();
        let scheduler = pool.scheduler();
        let event_pool = EventPool::<()>::new();

        let (tx, rx) = event_pool.rent();
        scheduler.spawn_and_forget(move || {
            tx.send(());
        });

        block_on(rx).unwrap();
    });
}

#[test]
fn concurrent_spawns_with_fake_hardware() {
    // Keep multiple submissions from every producer under Miri, with native stress coverage.
    const TASKS_PER_PRODUCER: usize = if cfg!(miri) { 2 } else { 10 };
    // Exercise the same competing producers and hardware topology at either batch size.
    const PRODUCER_COUNT: usize = 4;

    with_watchdog(|| {
        let hardware =
            SystemHardware::fake(HardwareBuilder::from_counts(nz!(4_usize), nz!(2_usize)));
        let pool = Pool::builder().hardware(hardware).build();
        let scheduler = pool.scheduler();
        let completed = Arc::new(AtomicUsize::new(0));
        let start = Arc::new(Barrier::new(PRODUCER_COUNT));

        let thread_handles: Vec<_> = std::iter::repeat_with(|| {
            let scheduler = scheduler.clone();
            let completed = Arc::clone(&completed);
            let start = Arc::clone(&start);

            thread::spawn(move || {
                start.wait();
                #[expect(
                    clippy::needless_collect,
                    reason = "submit the whole batch before awaiting any task to exercise queue contention"
                )]
                let handles: Vec<_> = (0..TASKS_PER_PRODUCER)
                    .map(|i| {
                        let completed = Arc::clone(&completed);
                        scheduler.spawn(move || {
                            completed.fetch_add(1, Ordering::Relaxed);
                            i
                        })
                    })
                    .collect();

                for (i, handle) in handles.into_iter().enumerate() {
                    assert_eq!(block_on(handle).unwrap(), i);
                }
            })
        })
        .take(PRODUCER_COUNT)
        .collect();

        for handle in thread_handles {
            handle.join().unwrap();
        }

        assert_eq!(
            completed.load(Ordering::Relaxed),
            PRODUCER_COUNT * TASKS_PER_PRODUCER
        );
    });
}

#[test]
fn spawn_and_await_panic_with_fake_hardware() {
    with_watchdog(|| {
        let hardware =
            SystemHardware::fake(HardwareBuilder::from_counts(nz!(2_usize), nz!(1_usize)));
        let pool = Pool::builder().hardware(hardware).build();
        let scheduler = pool.scheduler();

        let handle = scheduler.spawn(|| panic!("intentional panic"));
        assert!(block_on(handle).unwrap_err().is_panic());
    });
}

#[test]
fn spawn_urgent_with_fake_hardware() {
    with_watchdog(|| {
        let hardware =
            SystemHardware::fake(HardwareBuilder::from_counts(nz!(3_usize), nz!(1_usize)));
        let pool = Pool::builder().hardware(hardware).build();
        let scheduler = pool.scheduler();

        let handle = scheduler.spawn_urgent(|| "urgent result");
        assert_eq!(block_on(handle).unwrap(), "urgent result");
    });
}

#[test]
fn nested_spawn_with_fake_hardware() {
    with_watchdog(|| {
        let hardware =
            SystemHardware::fake(HardwareBuilder::from_counts(nz!(2_usize), nz!(1_usize)));
        let pool = Pool::builder().hardware(hardware).build();
        let scheduler = pool.scheduler();

        let scheduler_clone = scheduler.clone();
        let outer_handle = scheduler.spawn(move || scheduler_clone.spawn(|| 99));

        let inner_handle = block_on(outer_handle).unwrap();
        assert_eq!(block_on(inner_handle).unwrap(), 99);
    });
}

#[test]
fn shutdown_rejects_new_tasks_and_can_be_waited_on() {
    with_watchdog(|| {
        let hardware =
            SystemHardware::fake(HardwareBuilder::from_counts(nz!(1_usize), nz!(1_usize)));
        let pool = Pool::builder()
            .hardware(hardware)
            .max_workers(nz!(1_usize))
            .build();
        let scheduler = pool.scheduler();
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();

        let running = scheduler.spawn(move || {
            started_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            42
        });
        started_rx.recv().unwrap();
        let accepted_before_shutdown = scheduler.spawn(|| 7);

        pool.start_shutdown(Duration::from_secs(1), Duration::from_secs(1));
        pool.start_shutdown(Duration::ZERO, Duration::ZERO);

        let rejected = scheduler.spawn(|| 99);
        assert!(block_on(rejected).unwrap_err().is_shutdown());

        release_tx.send(()).unwrap();
        assert_eq!(block_on(running).unwrap(), 42);
        assert_eq!(block_on(accepted_before_shutdown).unwrap(), 7);
    });
}

#[test]
fn shutdown_gives_urgent_tasks_a_separate_drain_window() {
    with_watchdog(|| {
        let hardware =
            SystemHardware::fake(HardwareBuilder::from_counts(nz!(1_usize), nz!(1_usize)));
        let pool = Pool::builder()
            .hardware(hardware)
            .max_workers(nz!(1_usize))
            .build();
        let scheduler = pool.scheduler();
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();

        let running = scheduler.spawn(move || {
            started_tx.send(()).unwrap();
            release_rx.recv().unwrap();
        });
        started_rx.recv().unwrap();

        let abandoned_normal = scheduler.spawn(|| 1);
        let urgent = scheduler.spawn_urgent(|| 2);

        pool.start_shutdown(Duration::from_secs(1), Duration::ZERO);
        release_tx.send(()).unwrap();

        block_on(running).unwrap();
        assert!(block_on(abandoned_normal).unwrap_err().is_shutdown());
        assert_eq!(block_on(urgent).unwrap(), 2);
    });
}

#[test]
fn shutdown_abandons_urgent_tasks_after_their_timeout() {
    with_watchdog(|| {
        let hardware =
            SystemHardware::fake(HardwareBuilder::from_counts(nz!(1_usize), nz!(1_usize)));
        let pool = Pool::builder()
            .hardware(hardware)
            .max_workers(nz!(1_usize))
            .build();
        let scheduler = pool.scheduler();
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();

        let running = scheduler.spawn(move || {
            started_tx.send(()).unwrap();
            release_rx.recv().unwrap();
        });
        started_rx.recv().unwrap();
        let urgent = scheduler.spawn_urgent(|| 2);

        pool.start_shutdown(Duration::ZERO, Duration::ZERO);
        assert!(block_on(urgent).unwrap_err().is_shutdown());

        release_tx.send(()).unwrap();
        block_on(running).unwrap();
    });
}

::testing::set_allocator!();
