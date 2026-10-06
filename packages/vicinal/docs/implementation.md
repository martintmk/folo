# vicinal implementation

User-visible behavior is defined in the package [design](design.md).

Processor-local pools maintain a registry of state created on demand for processors that submit
work. Each processor state owns its queues, wake and shutdown signals, task storage and result
channels. Fixed processor-set pools use one shared state for all selected processors.

## Task storage

Each processor owns a heterogeneous object pool that routes task layouts to reusable
slots. Allocation is serialized because the pool facade is not shareable between threads.
The resulting owning handles can be queued, executed and released on worker threads
without holding or revisiting the allocation lock.

Task slot reservation happens while the allocation lock is held. Initialization and type
erasure happen after the lock is released so moving or dropping caller-owned task state
cannot poison shared allocator state. Workers obtain pinned access through the owning
handle, execute the task and then release the individual slot for reuse.

## Queue and worker lifecycle

Normal and urgent queues are independent. A worker removes a task while holding only the
corresponding queue lock, then executes it after releasing the lock. User task code
therefore runs without any processor-state lock held.

Worker threads are created lazily and tracked by the owning pool. A submission reserves an idle
worker when possible; otherwise it creates another worker until the scheduling domain reaches its
maximum. Fixed processor-set workers are assigned across the selected processors in round-robin
order. Worker initialization callbacks run after affinity is applied and before the work loop
starts.

Task admission is serialized against shutdown initiation so every task is either fully accepted
or rejected. Timed shutdown advances workers from urgent-only draining to normal-only draining
and finally stopping. Queue clearing drops result senders, which turns abandoned join handles
into shutdown errors. Workers still executing after both deadlines are detached; their owned
pool state remains alive until they return.

Result channels use a separate typed pool because their layout and lifecycle do not require
heterogeneous task storage.
