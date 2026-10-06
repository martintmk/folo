//! Pool management and lifecycle.

use std::any::type_name;
use std::num::NonZero;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::thread::{self, JoinHandle as ThreadJoinHandle};
use std::time::{Duration, Instant};
use std::{fmt, mem, panic};

use event_listener::{Event, Listener, listener};
use events_once::EventLake;
use many_cpus::{Processor, ProcessorId, ProcessorSet, SystemHardware};
use new_zealand::nz;
use tracing::{debug, trace};

use crate::{
    IterationResult, NEVER_POISONED, ProcessorRegistry, ProcessorState, Scheduler, ShutdownPhase,
    WorkerCore,
};

/// Experimentally determined as providing good throughput under
/// heavy I/O primitive create/destroy workload.
const DEFAULT_MAX_WORKERS: NonZero<usize> = nz!(2_usize);

static NEXT_POOL_ID: AtomicU64 = AtomicU64::new(0);

type ThreadInitFn = dyn Fn() + Send + Sync + 'static;

#[cfg(test)]
type HookFn = dyn Fn() + Send + Sync;

// Test hooks allow deterministic testing of race-condition branches by injecting a
// synchronization point (typically a pair of barriers) between two operations that
// normally execute without interruption. Each hook is an optional closure stored
// behind a Mutex. Tests that install hooks must hold HOOK_SERIALIZATION_MUTEX to
// prevent interference between concurrent hook-based tests.
#[cfg(test)]
static HOOK_SERIALIZATION_MUTEX: Mutex<()> = Mutex::new(());
#[cfg(test)]
static HOOK_ENSURE_WORKERS_POST_SPAWN: Mutex<Option<Arc<HookFn>>> = Mutex::new(None);

#[cfg(test)]
thread_local! {
    /// Marks the current thread as a participant in a hook-based test. Only threads with
    /// this flag set to `true` will trigger test hooks when they reach a hook callsite.
    static HOOK_PARTICIPANT: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

pub(crate) struct PoolInner {
    pub(crate) pool_id: u64,
    pub(crate) pool_name: String,
    pub(crate) hardware: SystemHardware,
    pub(crate) registry: ProcessorRegistry,
    pub(crate) processors: Option<ProcessorSet>,
    pub(crate) shared_state: OnceLock<ProcessorState>,
    pub(crate) max_workers: NonZero<usize>,
    pub(crate) worker_handles: Mutex<Vec<ThreadJoinHandle<()>>>,
    pub(crate) admission: RwLock<()>,
    pub(crate) shutdown_started: AtomicBool,
    pub(crate) shutdown_progress_event: Event,
    pub(crate) on_thread_start: AssertUnwindSafe<Arc<ThreadInitFn>>,
    pub(crate) rejected_result_channel_pool: EventLake,
}

#[cfg_attr(coverage_nightly, coverage(off))] // No API contract to test.
impl fmt::Debug for PoolInner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let handle_count = self.worker_handles.lock().expect(NEVER_POISONED).len();

        f.debug_struct(type_name::<Self>())
            .field("pool_id", &self.pool_id)
            .field("pool_name", &self.pool_name)
            .field("processors", &self.processors)
            .field("max_workers", &self.max_workers)
            .field("worker_count", &handle_count)
            .finish_non_exhaustive()
    }
}

impl PoolInner {
    #[cfg_attr(test, mutants::skip)] // Removing this causes timeouts (workers never start)
    pub(crate) fn ensure_worker_available(self: &Arc<Self>, processor_id: ProcessorId) {
        // If the pool is shutting down, we should not spawn new workers.
        if self.shutdown_started.load(Ordering::Relaxed) {
            return;
        }

        let state = self.state_for_spawn(processor_id);

        if state.try_reserve_idle_worker() {
            return;
        }

        let Some(worker_index) = state.try_add_worker(self.max_workers.get()) else {
            return;
        };
        let (target, target_processor_id) = self.worker_target(processor_id, worker_index);
        let multiple_processors = self
            .processors
            .as_ref()
            .is_some_and(|processors| processors.len() > 1);
        let inner_clone = Arc::clone(self);
        let on_thread_start = Arc::clone(&self.on_thread_start.0);
        let thread_name = if multiple_processors {
            format!("{}-{worker_index}", self.pool_name)
        } else {
            format!(
                "{}-{}-{}",
                self.pool_name, target_processor_id, worker_index
            )
        };

        let handle = thread::Builder::new()
            .name(thread_name)
            .spawn(move || {
                let state = inner_clone.state_for_spawn(target_processor_id);
                let _exit_guard = WorkerExitGuard::new(state, &inner_clone.shutdown_progress_event);

                target.pin_current_thread_to();
                (on_thread_start)();

                debug!(
                    pool_name = inner_clone.pool_name.as_str(),
                    pool_id = inner_clone.pool_id,
                    processor_id = target_processor_id,
                    worker_index,
                    "worker thread started"
                );
                worker_loop(&inner_clone, state, target_processor_id, worker_index);
                debug!(
                    pool_name = inner_clone.pool_name.as_str(),
                    pool_id = inner_clone.pool_id,
                    processor_id = target_processor_id,
                    worker_index,
                    "worker thread exiting"
                );
            })
            .unwrap_or_else(|error| {
                state.worker_exited();
                panic!("failed to spawn worker thread: {error}");
            });

        // Test hook: pause here so a concurrent test thread can trigger shutdown
        // before we acquire the lock below, exercising the re-check branch.
        #[cfg(test)]
        if HOOK_PARTICIPANT.get()
            && let Some(hook) = HOOK_ENSURE_WORKERS_POST_SPAWN.lock().unwrap().clone()
        {
            hook();
        }

        let mut worker_handles = self.worker_handles.lock().expect(NEVER_POISONED);

        // Re-check shutdown under the lock to avoid adding handles after a shutdown path has
        // already taken the existing ones.
        //
        // If shutdown is true here, the handles must not be added to the list because the
        // shutdown coordinator may already have taken it. Join them immediately instead.
        if self.shutdown_started.load(Ordering::Acquire) {
            if let Err(payload) = handle.join() {
                panic::resume_unwind(payload);
            }
            return;
        }

        worker_handles.push(handle);
    }

    pub(crate) fn state_for_spawn(&self, processor_id: ProcessorId) -> &ProcessorState {
        if self.processors.is_some() {
            self.shared_state.get_or_init(ProcessorState::new)
        } else {
            self.registry.get_or_init(processor_id)
        }
    }

    fn worker_target(
        &self,
        processor_id: ProcessorId,
        worker_index: usize,
    ) -> (ProcessorSet, ProcessorId) {
        let processors = self.processors.clone().unwrap_or_else(|| {
            self.hardware
                .processors()
                .filter(|processor| processor.id() == processor_id)
                .expect("current processor must be available in the configured hardware")
        });
        let processor_ids = processors
            .processors()
            .iter()
            .map(Processor::id)
            .collect::<Vec<_>>();
        let target_index = worker_index
            .checked_rem(processor_ids.len())
            .expect("processor set must not be empty");
        let target_processor_id = *processor_ids
            .get(target_index)
            .expect("worker target index must reference a selected processor");
        let target = processors
            .filter(|processor| processor.id() == target_processor_id)
            .expect("selected processor must be present in its source set");

        (target, target_processor_id)
    }

    fn set_shutdown_phase_all(&self, phase: ShutdownPhase) {
        self.registry.set_shutdown_phase_all(phase);

        if let Some(state) = self.shared_state.get() {
            state.set_shutdown_phase(phase);
        }
    }

    fn discard_regular_tasks(&self) -> usize {
        let discarded = self
            .registry
            .discard_regular_tasks()
            .checked_add(
                self.shared_state
                    .get()
                    .map_or(0, ProcessorState::discard_regular_tasks),
            )
            .expect("discarded task count must fit in usize");
        self.shutdown_progress_event.notify(usize::MAX);
        discarded
    }

    fn discard_urgent_tasks(&self) -> usize {
        let discarded = self
            .registry
            .discard_urgent_tasks()
            .checked_add(
                self.shared_state
                    .get()
                    .map_or(0, ProcessorState::discard_urgent_tasks),
            )
            .expect("discarded task count must fit in usize");
        self.shutdown_progress_event.notify(usize::MAX);
        discarded
    }

    fn pending_urgent_tasks(&self) -> usize {
        self.registry
            .pending_urgent_tasks()
            .checked_add(
                self.shared_state
                    .get()
                    .map_or(0, ProcessorState::pending_urgent_tasks),
            )
            .expect("pending task count must fit in usize")
    }

    fn pending_normal_tasks(&self) -> usize {
        self.registry
            .pending_normal_tasks()
            .checked_add(
                self.shared_state
                    .get()
                    .map_or(0, ProcessorState::pending_normal_tasks),
            )
            .expect("pending task count must fit in usize")
    }

    fn take_worker_handles(&self) -> Vec<ThreadJoinHandle<()>> {
        mem::take(&mut *self.worker_handles.lock().expect(NEVER_POISONED))
    }

    #[cfg_attr(test, mutants::skip)] // Removing this causes timeouts; requires timing logic to verify.
    fn shutdown_on_drop(&self) {
        let _admission = self.admission.write().expect(NEVER_POISONED);

        if self.shutdown_started.swap(true, Ordering::AcqRel) {
            return;
        }

        self.discard_regular_tasks();
        self.discard_urgent_tasks();
        self.set_shutdown_phase_all(ShutdownPhase::Stopping);

        for handle in self.take_worker_handles() {
            if let Err(payload) = handle.join() {
                panic::resume_unwind(payload);
            }
        }
    }
}

struct WorkerExitGuard<'a> {
    state: &'a ProcessorState,
    exit_event: &'a Event,
}

impl<'a> WorkerExitGuard<'a> {
    fn new(state: &'a ProcessorState, exit_event: &'a Event) -> Self {
        Self { state, exit_event }
    }
}

impl Drop for WorkerExitGuard<'_> {
    fn drop(&mut self) {
        self.state.worker_exited();
        self.exit_event.notify(usize::MAX);
    }
}

#[cfg_attr(test, mutants::skip)] // Removing this causes timeouts; condition logic is race-sensitive
fn worker_loop(
    inner: &PoolInner,
    state: &ProcessorState,
    processor_id: ProcessorId,
    worker_index: usize,
) {
    let core = WorkerCore::new(state);

    loop {
        match core.run_one_iteration() {
            IterationResult::ExecutedUrgent => {
                inner.shutdown_progress_event.notify(usize::MAX);
                trace!(
                    pool_name = inner.pool_name.as_str(),
                    pool_id = inner.pool_id,
                    processor_id,
                    worker_index,
                    "executed urgent task"
                );
            }
            IterationResult::ExecutedRegular => {
                inner.shutdown_progress_event.notify(usize::MAX);
                trace!(
                    pool_name = inner.pool_name.as_str(),
                    pool_id = inner.pool_id,
                    processor_id,
                    worker_index,
                    "executed regular task"
                );
            }
            IterationResult::Shutdown => {
                break;
            }
            IterationResult::WaitingForWork => {
                state.enter_idle();
                listener!(state.wake_event => listener);

                // Re-check after registering listener to avoid lost wakeups.
                let shutdown_phase = state.shutdown_phase();
                let has_work = match shutdown_phase {
                    ShutdownPhase::Running => {
                        !state.urgent_queue.lock().expect(NEVER_POISONED).is_empty()
                            || !state.regular_queue.lock().expect(NEVER_POISONED).is_empty()
                    }
                    ShutdownPhase::DrainingUrgent => {
                        !state.urgent_queue.lock().expect(NEVER_POISONED).is_empty()
                    }
                    ShutdownPhase::DrainingNormal => {
                        !state.regular_queue.lock().expect(NEVER_POISONED).is_empty()
                    }
                    ShutdownPhase::Stopping => true,
                };
                if has_work {
                    state.leave_idle();
                    continue;
                }

                listener.wait();
                state.leave_idle();
            }
        }
    }
}

/// Internal summary used for shutdown diagnostics.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct ShutdownOutcome {
    normal_tasks_abandoned: usize,
    urgent_tasks_abandoned: usize,
    workers_still_running: usize,
    worker_panics: usize,
}

fn run_timed_shutdown(
    inner: &PoolInner,
    urgent_timeout: Duration,
    normal_timeout: Duration,
) -> ShutdownOutcome {
    let mut handles = inner.take_worker_handles();
    let mut outcome = ShutdownOutcome::default();

    if !wait_for_pending_tasks(inner, TaskClass::Urgent, urgent_timeout) {
        outcome.urgent_tasks_abandoned = inner.discard_urgent_tasks();
    }

    inner.set_shutdown_phase_all(ShutdownPhase::DrainingNormal);

    if !wait_for_pending_tasks(inner, TaskClass::Normal, normal_timeout) {
        outcome.normal_tasks_abandoned = inner.discard_regular_tasks();
    }

    inner.set_shutdown_phase_all(ShutdownPhase::Stopping);
    reap_finished_workers(&mut handles, &mut outcome.worker_panics);
    outcome.workers_still_running = handles.len();

    outcome
}

#[derive(Clone, Copy)]
enum TaskClass {
    Urgent,
    Normal,
}

fn wait_for_pending_tasks(inner: &PoolInner, task_class: TaskClass, timeout: Duration) -> bool {
    let deadline = Instant::now().checked_add(timeout);

    loop {
        if pending_tasks(inner, task_class) == 0 {
            return true;
        }

        let Some(deadline) = deadline else {
            listener!(inner.shutdown_progress_event => listener);
            if pending_tasks(inner, task_class) == 0 {
                return true;
            }
            listener.wait();
            continue;
        };

        let now = Instant::now();
        if now >= deadline {
            return false;
        }

        listener!(inner.shutdown_progress_event => listener);
        if pending_tasks(inner, task_class) == 0 {
            return true;
        }

        if listener
            .wait_timeout(deadline.saturating_duration_since(Instant::now()))
            .is_none()
        {
            return pending_tasks(inner, task_class) == 0;
        }
    }
}

fn pending_tasks(inner: &PoolInner, task_class: TaskClass) -> usize {
    match task_class {
        TaskClass::Urgent => inner.pending_urgent_tasks(),
        TaskClass::Normal => inner.pending_normal_tasks(),
    }
}

fn reap_finished_workers(handles: &mut Vec<ThreadJoinHandle<()>>, worker_panics: &mut usize) {
    let mut index = 0;

    while index < handles.len() {
        if handles
            .get(index)
            .is_some_and(ThreadJoinHandle::is_finished)
        {
            let handle = handles.swap_remove(index);
            if handle.join().is_err() {
                *worker_panics = worker_panics
                    .checked_add(1)
                    .expect("worker panic count must fit in usize");
            }
        } else {
            index = index
                .checked_add(1)
                .expect("worker index must fit in usize");
        }
    }
}

/// A hardware-aware worker pool.
///
/// By default, tasks execute on the same processor that spawned them. Use
/// [`PoolBuilder::processors`] to create a shared pool over a fixed processor set.
///
/// # Lifetime
///
/// When the pool is dropped:
/// 1. All worker threads are signaled to shut down.
/// 2. The drop blocks until all currently-executing tasks complete.
/// 3. Any queued tasks that have not started are abandoned.
///
/// To ensure all tasks complete, await their [`JoinHandle`][crate::JoinHandle]s before dropping
/// the pool.
///
/// # Example
///
/// ```rust
/// use vicinal::Pool;
/// # use futures::executor::block_on;
///
/// # block_on(async {
/// let pool = Pool::new();
/// let scheduler = pool.scheduler();
///
/// let handle = scheduler.spawn(|| 42);
/// assert_eq!(handle.await.unwrap(), 42);
/// # });
/// ```
#[derive(Debug)]
pub struct Pool {
    inner: Arc<PoolInner>,
}

impl Pool {
    /// Creates a new pool with default settings.
    ///
    /// Use [`Pool::builder()`] for custom configuration.
    #[must_use]
    pub fn new() -> Self {
        Self::builder().build()
    }

    /// Creates a builder for configuring the pool.
    #[must_use]
    pub fn builder() -> PoolBuilder {
        PoolBuilder::new()
    }

    /// Returns a scheduler that can be used to spawn tasks on this pool.
    ///
    /// The scheduler can be cloned and shared across threads.
    #[must_use]
    pub fn scheduler(&self) -> Scheduler {
        Scheduler::new(Arc::clone(&self.inner))
    }

    /// Starts a timed shutdown and returns immediately.
    ///
    /// Once this method starts, all new tasks are rejected. Accepted urgent tasks drain first for
    /// `urgent_timeout`. Any urgent tasks still queued after that are abandoned. Accepted normal
    /// tasks then drain for `normal_timeout`, after which any still queued are abandoned.
    ///
    /// Tasks already executing cannot be forcibly interrupted. If they are still running after
    /// both timeouts, their worker threads are detached and finish independently.
    ///
    /// If shutdown has already been started, this method does nothing. The first call determines
    /// the drain timeouts.
    pub fn start_shutdown(&self, urgent_timeout: Duration, normal_timeout: Duration) {
        let _admission = self.inner.admission.write().expect(NEVER_POISONED);

        if self.inner.shutdown_started.swap(true, Ordering::AcqRel) {
            return;
        }

        self.inner
            .set_shutdown_phase_all(ShutdownPhase::DrainingUrgent);

        let inner = Arc::clone(&self.inner);
        _ = thread::Builder::new()
            .name(format!("{}-shutdown", self.inner.pool_name))
            .spawn(move || {
                let outcome = run_timed_shutdown(&inner, urgent_timeout, normal_timeout);
                debug!(
                    pool_name = inner.pool_name.as_str(),
                    pool_id = inner.pool_id,
                    normal_tasks_abandoned = outcome.normal_tasks_abandoned,
                    urgent_tasks_abandoned = outcome.urgent_tasks_abandoned,
                    workers_still_running = outcome.workers_still_running,
                    worker_panics = outcome.worker_panics,
                    "pool shutdown completed"
                );
            })
            .expect("failed to spawn shutdown coordinator thread");
    }
}

impl Default for Pool {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for Pool {
    #[cfg_attr(test, mutants::skip)] // Removing this causes timeouts; requires timing logic to verify.
    fn drop(&mut self) {
        self.inner.shutdown_on_drop();
    }
}

/// Builder for configuring a [`Pool`].
pub struct PoolBuilder {
    name: Option<String>,
    max_workers: NonZero<usize>,
    hardware: Option<SystemHardware>,
    processors: Option<ProcessorSet>,
    on_thread_start: AssertUnwindSafe<Arc<ThreadInitFn>>,
}

impl fmt::Debug for PoolBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(type_name::<Self>())
            .field("name", &self.name)
            .field("max_workers", &self.max_workers)
            .field("hardware", &self.hardware)
            .field("processors", &self.processors)
            .finish_non_exhaustive()
    }
}

impl PoolBuilder {
    fn new() -> Self {
        Self {
            name: None,
            max_workers: DEFAULT_MAX_WORKERS,
            hardware: None,
            processors: None,
            on_thread_start: AssertUnwindSafe(Arc::new(|| {})),
        }
    }

    /// Sets the name of the pool, used as a prefix in worker thread names.
    ///
    /// Processor-local and fixed single-processor worker names follow
    /// `{name}-{processor_id}-{worker_index}`. Fixed multi-processor pools use
    /// `{name}-{worker_index}`.
    ///
    /// When no name is specified, the default is `vicinal-{N}` where `N` is an
    /// auto-incrementing counter (e.g. `vicinal-0`, `vicinal-1`).
    #[must_use]
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Sets the maximum number of worker threads in the pool.
    ///
    /// Workers are created on demand when submitted work finds no idle worker, up to this limit.
    /// For a fixed multi-processor pool, new workers are assigned across the selected processors
    /// in round-robin order. Processor-local pools apply this limit to each lazily-created local
    /// worker group.
    #[must_use]
    pub fn max_workers(mut self, count: NonZero<usize>) -> Self {
        self.max_workers = count;
        self
    }

    /// Restricts the pool to a fixed set of processors.
    ///
    /// All tasks share one pair of queues and may be executed by any worker in the set. The pool
    /// grows on demand up to `max_workers` workers across the set.
    ///
    /// Worker names for a multi-processor set follow `{name}-{worker_index}` because no queue is
    /// associated with one processor. A single-processor set keeps the
    /// `{name}-{processor_id}-{worker_index}` naming scheme.
    #[must_use]
    pub fn processors(mut self, processors: ProcessorSet) -> Self {
        self.processors = Some(processors);
        self
    }

    /// Registers a callback to execute once on every worker thread after it is pinned.
    #[must_use]
    pub fn on_thread_start<F>(mut self, callback: F) -> Self
    where
        F: Fn() + Send + Sync + 'static,
    {
        self.on_thread_start = AssertUnwindSafe(Arc::new(callback));
        self
    }

    /// Sets the [`SystemHardware`] instance that the pool uses to determine hardware topology.
    ///
    /// By default the pool uses [`SystemHardware::current()`], which queries the real hardware
    /// of the system. Pass a custom instance (for example one created via
    /// `SystemHardware::fake()`) to override this.
    #[must_use]
    pub fn hardware(mut self, hardware: SystemHardware) -> Self {
        self.hardware = hardware.into();
        self
    }

    /// Builds the pool with the configured settings.
    #[must_use]
    pub fn build(self) -> Pool {
        let hardware = self
            .hardware
            .unwrap_or_else(|| SystemHardware::current().clone());

        let pool_id = NEXT_POOL_ID.fetch_add(1, Ordering::Relaxed);
        let pool_name = self.name.unwrap_or_else(|| format!("vicinal-{pool_id}"));

        let inner = Arc::new(PoolInner {
            pool_id,
            pool_name,
            registry: ProcessorRegistry::new(&hardware),
            hardware,
            processors: self.processors,
            shared_state: OnceLock::new(),
            max_workers: self.max_workers,
            worker_handles: Mutex::new(Vec::new()),
            admission: RwLock::new(()),
            shutdown_started: AtomicBool::new(false),
            shutdown_progress_event: Event::new(),
            on_thread_start: self.on_thread_start,
            rejected_result_channel_pool: EventLake::new(),
        });

        Pool { inner }
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use std::collections::HashSet;
    use std::num::NonZero;
    use std::panic::{AssertUnwindSafe, RefUnwindSafe, UnwindSafe, catch_unwind, resume_unwind};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Barrier};
    use std::thread;

    use many_cpus::SystemHardware;
    use many_cpus::fake::{HardwareBuilder, ProcessorBuilder};
    use new_zealand::nz;
    use static_assertions::assert_impl_all;

    use crate::{Pool, PoolBuilder};

    assert_impl_all!(Pool: UnwindSafe, RefUnwindSafe);
    assert_impl_all!(PoolBuilder: UnwindSafe, RefUnwindSafe);

    fn test_inner(hardware: SystemHardware) -> Arc<super::PoolInner> {
        let pool_id = super::NEXT_POOL_ID.fetch_add(1, Ordering::Relaxed);

        Arc::new(super::PoolInner {
            pool_id,
            pool_name: format!("vicinal-{pool_id}"),
            registry: super::ProcessorRegistry::new(&hardware),
            hardware,
            processors: None,
            shared_state: super::OnceLock::new(),
            max_workers: nz!(1_usize),
            worker_handles: super::Mutex::new(Vec::new()),
            admission: super::RwLock::new(()),
            shutdown_started: AtomicBool::new(false),
            shutdown_progress_event: super::Event::new(),
            on_thread_start: AssertUnwindSafe(Arc::new(|| {})),
            rejected_result_channel_pool: super::EventLake::new(),
        })
    }

    #[test]
    fn pool_new_creates_pool() {
        let pool = Pool::new();
        drop(pool);
    }

    #[test]
    fn pool_builder_allows_configuration() {
        let pool = Pool::builder()
            .max_workers(NonZero::new(4).unwrap())
            .build();
        drop(pool);
    }

    #[test]
    fn pool_builder_accepts_custom_hardware() {
        let hardware =
            SystemHardware::fake(HardwareBuilder::from_counts(nz!(2_usize), nz!(1_usize)));
        let pool = Pool::builder().hardware(hardware).build();
        drop(pool);
    }

    #[test]
    fn scheduler_can_be_obtained() {
        let pool = Pool::new();
        let _scheduler = pool.scheduler();
    }

    #[test]
    fn pool_default_creates_pool() {
        let pool = Pool::default();
        let _scheduler = pool.scheduler();
    }

    #[test]
    fn ensure_worker_available_returns_early_when_shutdown() {
        let hardware =
            SystemHardware::fake(HardwareBuilder::from_counts(nz!(2_usize), nz!(1_usize)));

        let inner = test_inner(hardware);

        // Trigger shutdown so the flag is set.
        inner.shutdown_on_drop();

        // Now call ensure_worker_available - it should return immediately
        // because the shutdown flag is already true.
        inner.ensure_worker_available(0);

        assert!(inner.worker_handles.lock().unwrap().is_empty());
    }

    /// Installs a hook closure and runs the test body while holding the
    /// serialization mutex. The hook is always removed on exit.
    fn with_hook(
        hook: &super::Mutex<Option<Arc<super::HookFn>>>,
        closure: Arc<super::HookFn>,
        body: impl FnOnce(),
    ) {
        let guard = super::HOOK_SERIALIZATION_MUTEX.lock().unwrap();
        *hook.lock().unwrap() = Some(closure);

        // We catch panics from the test body so that we can clean up the hook and
        // drop the serialization guard while not panicking, preventing mutex poisoning.
        let result = catch_unwind(AssertUnwindSafe(body));

        *hook.lock().unwrap() = None;
        drop(guard);

        if let Err(payload) = result {
            resume_unwind(payload);
        }
    }

    struct BarrierHook {
        /// Signaled when the hook fires, indicating that the hooked code
        /// has reached the synchronization point.
        entered: Arc<Barrier>,

        /// Waited on before the hook returns, giving the test thread a
        /// window to perform a racing operation.
        proceed: Arc<Barrier>,

        /// The closure to install as a hook.
        hook: Arc<super::HookFn>,
    }

    /// Creates a two-barrier hook closure that pauses execution at a
    /// synchronization point, giving the test thread a window to perform
    /// a racing operation.
    fn barrier_hook() -> BarrierHook {
        let entered = Arc::new(Barrier::new(2));
        let proceed = Arc::new(Barrier::new(2));
        let e = Arc::clone(&entered);
        let p = Arc::clone(&proceed);
        let hook: Arc<super::HookFn> = Arc::new(move || {
            e.wait();
            p.wait();
        });
        BarrierHook {
            entered,
            proceed,
            hook,
        }
    }

    #[test]
    fn ensure_worker_available_joins_inline_on_concurrent_shutdown() {
        testing::with_watchdog(|| {
            let BarrierHook {
                entered,
                proceed,
                hook,
            } = barrier_hook();

            with_hook(&super::HOOK_ENSURE_WORKERS_POST_SPAWN, hook, || {
                let hardware =
                    SystemHardware::fake(HardwareBuilder::from_counts(nz!(2_usize), nz!(1_usize)));

                let inner = test_inner(hardware);

                let inner_clone = Arc::clone(&inner);

                // Spawn ensure_worker_available on a background thread.
                // It will spawn workers then pause at the hook before
                // acquiring the worker_handles lock.
                let spawner = thread::spawn(move || {
                    super::HOOK_PARTICIPANT.set(true);
                    inner_clone.ensure_worker_available(0);
                });

                // Wait for the hook to fire (workers spawned, lock not
                // yet acquired).
                entered.wait();

                // Trigger shutdown while the spawner is paused.
                inner.shutdown_on_drop();

                // Release the spawner. It will see shutdown=true under
                // the lock and join its handles inline.
                proceed.wait();

                spawner.join().unwrap();

                // All handles should have been joined (none leaked).
                assert!(inner.worker_handles.lock().unwrap().is_empty());
            });
        });
    }

    #[test]
    fn pool_builder_accepts_custom_name() {
        let pool = Pool::builder().name("my_pool").build();
        assert_eq!(pool.inner.pool_name, "my_pool");
    }

    #[test]
    fn pool_default_name_uses_vicinal_prefix() {
        let pool = Pool::new();
        assert!(pool.inner.pool_name.starts_with("vicinal-"));
    }

    #[test]
    fn worker_threads_use_custom_pool_name() {
        testing::with_watchdog(|| {
            let hardware =
                SystemHardware::fake(HardwareBuilder::from_counts(nz!(1_usize), nz!(1_usize)));

            let pool = Pool::builder()
                .name("test_pool")
                .hardware(hardware)
                .max_workers(nz!(1_usize))
                .build();

            let scheduler = pool.scheduler();

            let thread_name = futures::executor::block_on(
                scheduler.spawn(|| thread::current().name().unwrap().to_owned()),
            )
            .unwrap();

            assert!(
                thread_name.starts_with("test_pool-"),
                "expected thread name to start with 'test_pool-', got: {thread_name}"
            );
        });
    }

    #[test]
    fn worker_threads_use_default_name_when_none_specified() {
        testing::with_watchdog(|| {
            let hardware =
                SystemHardware::fake(HardwareBuilder::from_counts(nz!(1_usize), nz!(1_usize)));

            let pool = Pool::builder()
                .hardware(hardware)
                .max_workers(nz!(1_usize))
                .build();

            let scheduler = pool.scheduler();

            let thread_name = futures::executor::block_on(
                scheduler.spawn(|| thread::current().name().unwrap().to_owned()),
            )
            .unwrap();

            assert!(
                thread_name.starts_with("vicinal-"),
                "expected thread name to start with 'vicinal-', got: {thread_name}"
            );
        });
    }

    #[test]
    fn multi_processor_pool_uses_all_processors_and_omits_processor_from_names() {
        testing::with_watchdog(|| {
            let hardware = SystemHardware::fake(
                HardwareBuilder::new()
                    .processor(ProcessorBuilder::new().id(40))
                    .processor(ProcessorBuilder::new().id(41)),
            );
            let processors = hardware.processors();
            let started = Arc::new(AtomicUsize::new(0));
            let worker_details = Arc::new(super::Mutex::new(Vec::new()));

            let pool = Pool::builder()
                .name("shared")
                .processors(processors)
                .max_workers(nz!(2_usize))
                .on_thread_start({
                    let started = Arc::clone(&started);
                    let worker_details = Arc::clone(&worker_details);
                    move || {
                        started.fetch_add(1, Ordering::Relaxed);
                        worker_details.lock().unwrap().push((
                            hardware.current_processor_id(),
                            thread::current().name().unwrap().to_owned(),
                        ));
                    }
                })
                .build();
            let scheduler = pool.scheduler();

            let barrier = Arc::new(Barrier::new(3));
            let handles = (0..3)
                .map(|task_index| {
                    let barrier = Arc::clone(&barrier);
                    scheduler.spawn(move || {
                        if task_index < 2 {
                            barrier.wait();
                        }
                    })
                })
                .collect::<Vec<_>>();
            barrier.wait();
            for handle in handles {
                futures::executor::block_on(handle).unwrap();
            }

            let worker_details = worker_details.lock().unwrap();
            let processor_ids = worker_details
                .iter()
                .map(|(id, _)| *id)
                .collect::<HashSet<_>>();
            let thread_names = worker_details
                .iter()
                .map(|(_, name)| name.as_str())
                .collect::<HashSet<_>>();

            assert_eq!(processor_ids, HashSet::from([40, 41]));
            assert_eq!(thread_names, HashSet::from(["shared-0", "shared-1"]));
            assert_eq!(started.load(Ordering::Relaxed), 2);
        });
    }

    #[test]
    fn pool_starts_with_one_worker_and_grows_only_when_needed() {
        testing::with_watchdog(|| {
            let hardware =
                SystemHardware::fake(HardwareBuilder::from_counts(nz!(1_usize), nz!(1_usize)));
            let started = Arc::new(AtomicUsize::new(0));
            let pool = Pool::builder()
                .hardware(hardware)
                .max_workers(nz!(4_usize))
                .on_thread_start({
                    let started = Arc::clone(&started);
                    move || {
                        started.fetch_add(1, Ordering::Relaxed);
                    }
                })
                .build();

            futures::executor::block_on(pool.scheduler().spawn(|| {})).unwrap();

            assert_eq!(started.load(Ordering::Relaxed), 1);
        });
    }
}
