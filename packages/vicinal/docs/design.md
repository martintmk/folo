# vicinal design

`vicinal` executes synchronous tasks on explicitly controlled processors. Processor-local
pools keep detached work close to the data and caches used by their caller. Fixed processor-set
pools instead share work across a caller-selected hardware boundary.

## Processor locality

Each task is assigned to the current processor when it is spawned. Workers associated with
that processor execute the task. Platforms without processor-pinning support retain worker
pool behavior without the locality guarantee.

Workers are created lazily because single-task latency on an otherwise idle pool is the
primary optimization target.

A fixed processor-set pool shares its queues across the selected processors. Its maximum worker
count is distributed across those processors as demand grows, and worker names omit a processor
identifier because the pool presents one shared scheduling domain.

This mode can serve as a general-purpose blocking pool. Capacity grows when submissions find no
idle worker, up to the configured ceiling. A blocked task occupies one worker until it returns.

Callers may register worker initialization logic. It executes inside every worker after the
thread is pinned and before the worker accepts tasks.

## Scheduling

Regular and urgent work use separate priority classes. Workers prefer urgent work, but
tasks within a class may execute in any order and urgent work does not preempt a task that
is already running.

Tasks with join handles return their output asynchronously. The handle resolves to either the
task output or an opaque join error that distinguishes panic from shutdown abandonment.
Fire-and-forget tasks discard their output and report panics through tracing.

## Shutdown

Dropping the pool signals its workers and waits for tasks that are already executing.
Queued tasks may be abandoned. Callers that require completion await the corresponding
join handles before dropping the pool.

Timed shutdown starts without blocking and rejects every later submission. Accepted urgent work
drains first until its timeout, after which queued urgent work is abandoned. Normal work then
receives its own drain window. A repeated shutdown request has no effect; the first request
defines the timeouts.
