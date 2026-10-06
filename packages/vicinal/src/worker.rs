//! Worker thread logic for executing tasks from queues.

use std::collections::VecDeque;
use std::sync::Mutex;

use crate::{ErasedTaskHandle, NEVER_POISONED, ProcessorState, ShutdownPhase};

/// What one pass of a worker's main loop accomplished, and thus what the loop does next.
///
/// The two `Executed` outcomes mean work was found and the loop should immediately look for
/// more; they are kept apart so tests can assert queue priority. `WaitingForWork` sends the
/// worker to sleep on the wake event and `Shutdown` ends the thread.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum IterationResult {
    ExecutedUrgent,
    ExecutedRegular,
    Shutdown,
    WaitingForWork,
}

/// The body of a worker thread's main loop, isolated from thread management.
///
/// It borrows only the parts of a processor's state that executing work needs, which lets a
/// single iteration be driven directly by tests without spawning a thread or constructing a
/// pool. Priority between the two queues lives here and nowhere else.
pub(crate) struct WorkerCore<'a> {
    urgent_queue: &'a Mutex<VecDeque<ErasedTaskHandle>>,
    regular_queue: &'a Mutex<VecDeque<ErasedTaskHandle>>,
    processor_state: &'a ProcessorState,
}

impl<'a> WorkerCore<'a> {
    pub(crate) fn new(processor_state: &'a ProcessorState) -> Self {
        Self {
            urgent_queue: &processor_state.urgent_queue,
            regular_queue: &processor_state.regular_queue,
            processor_state,
        }
    }

    pub(crate) fn run_one_iteration(&self) -> IterationResult {
        let shutdown_phase = self.processor_state.shutdown_phase();
        if shutdown_phase == ShutdownPhase::Stopping {
            return IterationResult::Shutdown;
        }

        if matches!(
            shutdown_phase,
            ShutdownPhase::Running | ShutdownPhase::DrainingUrgent
        ) {
            let task = self.urgent_queue.lock().expect(NEVER_POISONED).pop_front();
            if let Some(mut task) = task {
                task.as_pin_mut().call();
                self.processor_state.record_task_completed(true);
                return IterationResult::ExecutedUrgent;
            }
        }

        if matches!(
            shutdown_phase,
            ShutdownPhase::Running | ShutdownPhase::DrainingNormal
        ) {
            let task = self.regular_queue.lock().expect(NEVER_POISONED).pop_front();
            if let Some(mut task) = task {
                task.as_pin_mut().call();
                self.processor_state.record_task_completed(false);
                return IterationResult::ExecutedRegular;
            }
        }

        IterationResult::WaitingForWork
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use std::pin::Pin;
    use std::sync::atomic::{AtomicU32, Ordering};

    use plurality::MultiPool;

    use super::*;
    use crate::{VicinalTask, init_task};

    fn alloc_task(pool: &MultiPool, task: impl VicinalTask) -> ErasedTaskHandle {
        init_task(pool.alloc_uninit_box(), task)
    }

    /// A simple task that increments a counter when called.
    struct CountingTask {
        counter: &'static AtomicU32,
        called: bool,
    }

    impl CountingTask {
        fn new(counter: &'static AtomicU32) -> Self {
            Self {
                counter,
                called: false,
            }
        }
    }

    impl VicinalTask for CountingTask {
        fn call(self: Pin<&mut Self>) {
            let this = self.get_mut();
            if !this.called {
                this.called = true;
                this.counter.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    #[test]
    fn empty_queues_no_shutdown_returns_waiting() {
        let state = ProcessorState::new();
        let core = WorkerCore::new(&state);

        assert_eq!(core.run_one_iteration(), IterationResult::WaitingForWork);
    }

    #[test]
    fn empty_queues_while_draining_waits_for_next_phase() {
        let state = ProcessorState::new();
        state.set_shutdown_phase(ShutdownPhase::DrainingUrgent);
        let core = WorkerCore::new(&state);

        assert_eq!(core.run_one_iteration(), IterationResult::WaitingForWork);
    }

    #[test]
    fn urgent_task_executes_and_returns_executed_urgent() {
        static COUNTER: AtomicU32 = AtomicU32::new(0);
        COUNTER.store(0, Ordering::Relaxed);

        let pool = MultiPool::new();
        let state = ProcessorState::new();

        let task = alloc_task(&pool, CountingTask::new(&COUNTER));
        state.urgent_queue.lock().unwrap().push_back(task);
        state.record_task_spawned(true);

        let core = WorkerCore::new(&state);

        assert_eq!(core.run_one_iteration(), IterationResult::ExecutedUrgent);
        assert_eq!(COUNTER.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn urgent_takes_priority_over_regular() {
        static URGENT_COUNTER: AtomicU32 = AtomicU32::new(0);
        static REGULAR_COUNTER: AtomicU32 = AtomicU32::new(0);
        URGENT_COUNTER.store(0, Ordering::Relaxed);
        REGULAR_COUNTER.store(0, Ordering::Relaxed);

        let pool = MultiPool::new();
        let state = ProcessorState::new();

        let urgent_task = alloc_task(&pool, CountingTask::new(&URGENT_COUNTER));
        let regular_task = alloc_task(&pool, CountingTask::new(&REGULAR_COUNTER));
        state.urgent_queue.lock().unwrap().push_back(urgent_task);
        state.regular_queue.lock().unwrap().push_back(regular_task);
        state.record_task_spawned(true);
        state.record_task_spawned(false);

        let core = WorkerCore::new(&state);

        // First iteration should execute urgent task only.
        assert_eq!(core.run_one_iteration(), IterationResult::ExecutedUrgent);
        assert_eq!(URGENT_COUNTER.load(Ordering::Relaxed), 1);
        assert_eq!(REGULAR_COUNTER.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn regular_task_executes_when_urgent_empty() {
        static COUNTER: AtomicU32 = AtomicU32::new(0);
        COUNTER.store(0, Ordering::Relaxed);

        let pool = MultiPool::new();
        let state = ProcessorState::new();

        let task = alloc_task(&pool, CountingTask::new(&COUNTER));
        state.regular_queue.lock().unwrap().push_back(task);
        state.record_task_spawned(false);

        let core = WorkerCore::new(&state);

        assert_eq!(core.run_one_iteration(), IterationResult::ExecutedRegular);
        assert_eq!(COUNTER.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn draining_urgent_prevents_regular_task_execution() {
        static COUNTER: AtomicU32 = AtomicU32::new(0);
        COUNTER.store(0, Ordering::Relaxed);

        let pool = MultiPool::new();
        let state = ProcessorState::new();
        state.set_shutdown_phase(ShutdownPhase::DrainingUrgent);

        let task = alloc_task(&pool, CountingTask::new(&COUNTER));
        state.regular_queue.lock().unwrap().push_back(task);
        state.record_task_spawned(false);

        let core = WorkerCore::new(&state);

        assert_eq!(core.run_one_iteration(), IterationResult::WaitingForWork);
        assert_eq!(COUNTER.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn draining_normal_executes_regular_tasks() {
        static COUNTER: AtomicU32 = AtomicU32::new(0);
        COUNTER.store(0, Ordering::Relaxed);

        let pool = MultiPool::new();
        let state = ProcessorState::new();
        state.set_shutdown_phase(ShutdownPhase::DrainingNormal);
        let task = alloc_task(&pool, CountingTask::new(&COUNTER));
        state.regular_queue.lock().unwrap().push_back(task);
        state.record_task_spawned(false);

        let core = WorkerCore::new(&state);

        assert_eq!(core.run_one_iteration(), IterationResult::ExecutedRegular);
        assert_eq!(COUNTER.load(Ordering::Relaxed), 1);
    }
}
