//! Example from the README.

use std::time::Duration;

use many_cpus::SystemHardware;
use new_zealand::nz;
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

    multi_processor_configuration();
}

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

::testing::set_allocator!();
