//! Per-processor state for worker threads.

use std::collections::VecDeque;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU8, AtomicU64, AtomicUsize, Ordering};

use event_listener::Event;
use events_once::EventLake;
use plurality::MultiPool;

use crate::ErasedTaskHandle;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub(crate) enum ShutdownPhase {
    Running,
    DrainingUrgent,
    DrainingNormal,
    Stopping,
}

impl ShutdownPhase {
    fn from_u8(value: u8) -> Self {
        match value {
            0 => Self::Running,
            1 => Self::DrainingUrgent,
            2 => Self::DrainingNormal,
            3 => Self::Stopping,
            _ => unreachable!("invalid shutdown phase"),
        }
    }
}

/// Everything a processor's worker threads share: the work they draw from, the storage that
/// work lives in, and the signals that tell them to wake up or stop.
///
/// Processor-local pools create one instance per processor that has received work. Fixed
/// processor-set pools use one shared instance for all selected processors.
pub(crate) struct ProcessorState {
    /// Queue for high-priority tasks. These are executed before regular tasks.
    pub(crate) urgent_queue: Mutex<VecDeque<ErasedTaskHandle>>,

    /// Queue for normal-priority tasks.
    pub(crate) regular_queue: Mutex<VecDeque<ErasedTaskHandle>>,

    /// Event used to wake up sleeping workers when new tasks are added.
    pub(crate) wake_event: Event,

    /// Current shutdown phase for the processor and its workers.
    shutdown_phase: AtomicU8,

    /// Number of live or starting workers for this scheduling domain.
    active_workers: AtomicUsize,

    /// Number of workers currently waiting for work.
    idle_workers: AtomicUsize,

    /// Monotonic index assigned to newly created workers.
    next_worker_index: AtomicUsize,

    /// Backing storage for the task objects queued on this processor.
    ///
    /// `MultiPool` is not `Sync`, so the mutex serializes allocation by scheduler threads.
    /// Slot initialization, queueing, execution and release happen after this lock is released.
    pub(crate) task_pool: Mutex<MultiPool>,

    /// Pool for storing oneshot channels used to return task results.
    pub(crate) result_channel_pool: EventLake,

    /// Counter of total tasks spawned on this processor.
    pub(crate) tasks_spawned: AtomicU64,

    /// Number of accepted urgent tasks that have not completed or been abandoned.
    pending_urgent_tasks: AtomicUsize,

    /// Number of accepted normal tasks that have not completed or been abandoned.
    pending_normal_tasks: AtomicUsize,
}

impl ProcessorState {
    pub(crate) fn new() -> Self {
        Self {
            urgent_queue: Mutex::new(VecDeque::new()),
            regular_queue: Mutex::new(VecDeque::new()),
            wake_event: Event::new(),
            shutdown_phase: AtomicU8::new(ShutdownPhase::Running as u8),
            active_workers: AtomicUsize::new(0),
            idle_workers: AtomicUsize::new(0),
            next_worker_index: AtomicUsize::new(0),
            task_pool: Mutex::new(MultiPool::new()),
            result_channel_pool: EventLake::new(),
            tasks_spawned: AtomicU64::new(0),
            pending_urgent_tasks: AtomicUsize::new(0),
            pending_normal_tasks: AtomicUsize::new(0),
        }
    }

    #[cfg_attr(test, mutants::skip)] // Removing this causes timeouts (workers never stop)
    pub(crate) fn set_shutdown_phase(&self, phase: ShutdownPhase) {
        // Release ordering ensures all prior task queue operations are visible to workers
        // before they observe the shutdown phase.
        self.shutdown_phase.store(phase as u8, Ordering::Release);
        self.wake_event.notify(usize::MAX);
    }

    pub(crate) fn shutdown_phase(&self) -> ShutdownPhase {
        ShutdownPhase::from_u8(self.shutdown_phase.load(Ordering::Acquire))
    }

    pub(crate) fn try_reserve_idle_worker(&self) -> bool {
        self.idle_workers
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |idle_workers| {
                idle_workers.checked_sub(1)
            })
            .is_ok()
    }

    pub(crate) fn try_add_worker(&self, max_workers: usize) -> Option<usize> {
        self.active_workers
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |active_workers| {
                if active_workers >= max_workers {
                    None
                } else {
                    active_workers.checked_add(1)
                }
            })
            .ok()?;

        match self.next_worker_index.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |worker_index| worker_index.checked_add(1),
        ) {
            Ok(worker_index) => Some(worker_index),
            Err(_) => {
                self.worker_exited();
                panic!("worker index overflowed usize");
            }
        }
    }

    pub(crate) fn worker_exited(&self) {
        self.active_workers
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |active_workers| {
                active_workers.checked_sub(1)
            })
            .expect("active worker count must be positive");
    }

    pub(crate) fn enter_idle(&self) {
        self.idle_workers
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |idle_workers| {
                idle_workers.checked_add(1)
            })
            .expect("idle worker count overflowed usize");
    }

    pub(crate) fn leave_idle(&self) {
        _ = self
            .idle_workers
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |idle_workers| {
                idle_workers.checked_sub(1)
            });
    }

    pub(crate) fn discard_regular_tasks(&self) -> usize {
        let mut queue = self.regular_queue.lock().expect(crate::NEVER_POISONED);
        let discarded = queue.len();
        queue.clear();
        Self::abandon_tasks(&self.pending_normal_tasks, discarded);
        discarded
    }

    pub(crate) fn discard_urgent_tasks(&self) -> usize {
        let mut queue = self.urgent_queue.lock().expect(crate::NEVER_POISONED);
        let discarded = queue.len();
        queue.clear();
        Self::abandon_tasks(&self.pending_urgent_tasks, discarded);
        discarded
    }

    pub(crate) fn record_task_spawned(&self, urgent: bool) {
        // Relaxed ordering is sufficient for a monotonic counter with no synchronization needs.
        self.tasks_spawned.fetch_add(1, Ordering::Relaxed);

        let pending_tasks = if urgent {
            &self.pending_urgent_tasks
        } else {
            &self.pending_normal_tasks
        };
        pending_tasks
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |pending_tasks| {
                pending_tasks.checked_add(1)
            })
            .expect("pending task count overflowed usize");
    }

    pub(crate) fn record_task_completed(&self, urgent: bool) {
        let pending_tasks = if urgent {
            &self.pending_urgent_tasks
        } else {
            &self.pending_normal_tasks
        };
        pending_tasks
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |pending_tasks| {
                pending_tasks.checked_sub(1)
            })
            .expect("pending task count must be positive");
    }

    pub(crate) fn pending_urgent_tasks(&self) -> usize {
        self.pending_urgent_tasks.load(Ordering::Acquire)
    }

    pub(crate) fn pending_normal_tasks(&self) -> usize {
        self.pending_normal_tasks.load(Ordering::Acquire)
    }

    fn abandon_tasks(pending_tasks: &AtomicUsize, abandoned: usize) {
        pending_tasks
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |pending_tasks| {
                pending_tasks.checked_sub(abandoned)
            })
            .expect("abandoned task count must not exceed pending task count");
    }

    #[cfg(test)]
    pub(crate) fn get_tasks_spawned(&self) -> u64 {
        // Relaxed ordering is sufficient - no synchronization with other operations needed.
        self.tasks_spawned.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use std::pin::Pin;
    use std::sync::{Arc, Barrier};

    use super::*;
    use crate::{VicinalTask, init_task};

    /// Spans multiple allocation/release operations without making Miri runs expensive.
    const CONCURRENT_TASK_COUNT: usize = 32;

    /// Minimal task used to exercise cross-thread slot release.
    struct NoopTask;

    impl VicinalTask for NoopTask {
        fn call(self: Pin<&mut Self>) {}
    }

    fn alloc_noop_task(state: &ProcessorState) -> ErasedTaskHandle {
        let slot = state.task_pool.lock().unwrap().alloc_uninit_box();
        init_task(slot, NoopTask)
    }

    #[test]
    fn new_creates_empty_queues() {
        let state = ProcessorState::new();

        assert!(state.urgent_queue.lock().unwrap().pop_front().is_none());
        assert!(state.regular_queue.lock().unwrap().pop_front().is_none());
    }

    #[test]
    fn shutdown_phase_initially_running() {
        let state = ProcessorState::new();

        assert_eq!(state.shutdown_phase(), ShutdownPhase::Running);
    }

    #[test]
    fn set_shutdown_phase_updates_phase() {
        let state = ProcessorState::new();

        state.set_shutdown_phase(ShutdownPhase::DrainingUrgent);

        assert_eq!(state.shutdown_phase(), ShutdownPhase::DrainingUrgent);
    }

    #[test]
    fn tasks_spawned_counter_initially_zero() {
        let state = ProcessorState::new();

        assert_eq!(state.get_tasks_spawned(), 0);
    }

    #[test]
    fn record_task_spawned_increments_counter() {
        let state = ProcessorState::new();

        state.record_task_spawned(false);
        state.record_task_spawned(false);
        state.record_task_spawned(false);

        assert_eq!(state.get_tasks_spawned(), 3);
    }

    #[test]
    fn task_slots_can_be_released_while_allocating() {
        let state = Arc::new(ProcessorState::new());
        let tasks = std::iter::repeat_with(|| alloc_noop_task(&state))
            .take(CONCURRENT_TASK_COUNT)
            .collect::<Vec<_>>();
        let barrier = Arc::new(Barrier::new(2));

        std::thread::scope(|scope| {
            scope.spawn({
                let barrier = Arc::clone(&barrier);
                move || {
                    barrier.wait();
                    drop(tasks);
                }
            });

            barrier.wait();
            for _ in 0..CONCURRENT_TASK_COUNT {
                drop(alloc_noop_task(&state));
            }
        });

        assert!(state.task_pool.lock().unwrap().is_empty());
    }
}
