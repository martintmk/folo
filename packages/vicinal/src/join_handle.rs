//! Join handle for awaiting task completion.

use std::any::Any;
use std::error::Error;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use events_once::PooledReceiver;

use crate::TaskResult;

/// The reason a spawned task did not produce a value.
pub struct JoinError {
    kind: JoinErrorKind,
}

enum JoinErrorKind {
    Shutdown,
    Panic(Box<dyn Any + Send + 'static>),
}

impl JoinError {
    pub(crate) fn shutdown() -> Self {
        Self {
            kind: JoinErrorKind::Shutdown,
        }
    }

    pub(crate) fn panic(payload: Box<dyn Any + Send + 'static>) -> Self {
        Self {
            kind: JoinErrorKind::Panic(payload),
        }
    }

    /// Returns `true` if the task was abandoned during pool shutdown.
    #[must_use]
    pub fn is_shutdown(&self) -> bool {
        matches!(&self.kind, JoinErrorKind::Shutdown)
    }

    /// Returns `true` if the task panicked.
    #[must_use]
    pub fn is_panic(&self) -> bool {
        matches!(&self.kind, JoinErrorKind::Panic(_))
    }

    /// Returns the panic payload, if the task panicked.
    #[must_use]
    pub fn into_panic(self) -> Option<Box<dyn Any + Send + 'static>> {
        match self.kind {
            JoinErrorKind::Shutdown => None,
            JoinErrorKind::Panic(payload) => Some(payload),
        }
    }

    fn panic_message(&self) -> Option<&str> {
        let JoinErrorKind::Panic(payload) = &self.kind else {
            return None;
        };

        payload
            .downcast_ref::<&str>()
            .copied()
            .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
    }
}

impl fmt::Debug for JoinError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.kind {
            JoinErrorKind::Shutdown => f.write_str("JoinError { shutdown: true }"),
            JoinErrorKind::Panic(_) => f
                .debug_struct("JoinError")
                .field("panic", &true)
                .field(
                    "message",
                    &self.panic_message().unwrap_or("unknown panic payload"),
                )
                .finish(),
        }
    }
}

impl fmt::Display for JoinError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.kind {
            JoinErrorKind::Shutdown => {
                f.write_str("task was abandoned because the pool was shut down")
            }
            JoinErrorKind::Panic(_) => write!(
                f,
                "task panicked: {}",
                self.panic_message().unwrap_or("unknown panic payload")
            ),
        }
    }
}

impl Error for JoinError {}

/// A handle to a spawned task that can be awaited to retrieve its result.
///
/// Awaiting the join handle returns the task's return value or a [`JoinError`] describing
/// whether the task panicked or was abandoned during shutdown.
#[derive(Debug)]
pub struct JoinHandle<R>
where
    R: Send + 'static,
{
    receiver: PooledReceiver<TaskResult<R>>,
}

impl<R> JoinHandle<R>
where
    R: Send + 'static,
{
    /// Creates a new join handle from a receiver.
    pub(crate) fn new(receiver: PooledReceiver<TaskResult<R>>) -> Self {
        Self { receiver }
    }
}

impl<R> Future for JoinHandle<R>
where
    R: Send + 'static,
{
    type Output = Result<R, JoinError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let receiver = &mut self.receiver;
        let pinned_receiver = Pin::new(receiver);

        match pinned_receiver.poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(result) => match result {
                Ok(result) => Poll::Ready(result),
                Err(_disconnected) => Poll::Ready(Err(JoinError::shutdown())),
            },
        }
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use std::panic::{self, AssertUnwindSafe, RefUnwindSafe, UnwindSafe};

    use events_once::EventLake;
    use futures::executor::block_on;

    use super::*;

    static_assertions::assert_impl_all!(
        JoinHandle<String>: UnwindSafe, RefUnwindSafe
    );

    #[test]
    fn await_returns_value() {
        let lake = EventLake::new();
        let (sender, receiver) = lake.rent::<TaskResult<i32>>();
        let handle = JoinHandle::new(receiver);

        sender.send(Ok(42));

        let result = block_on(handle);
        assert_eq!(result.unwrap(), 42);
    }

    #[test]
    fn await_returns_unit() {
        let lake = EventLake::new();
        let (sender, receiver) = lake.rent::<TaskResult<()>>();
        let handle = JoinHandle::new(receiver);

        sender.send(Ok(()));

        block_on(handle).unwrap();
    }

    #[test]
    fn await_returns_panic_error() {
        let lake = EventLake::new();
        let (sender, receiver) = lake.rent::<TaskResult<()>>();
        let handle = JoinHandle::new(receiver);

        // Capture a real panic to get a proper payload.
        let panic_result = panic::catch_unwind(AssertUnwindSafe(|| {
            panic!("test panic");
        }));

        sender.send(Err(JoinError::panic(panic_result.unwrap_err())));

        assert!(block_on(handle).unwrap_err().is_panic());
    }

    #[test]
    fn await_returns_shutdown_error_on_disconnect() {
        let lake = EventLake::new();
        let (sender, receiver) = lake.rent::<TaskResult<()>>();
        let handle = JoinHandle::new(receiver);

        // Drop sender without sending, simulating pool shutdown.
        drop(sender);

        assert!(block_on(handle).unwrap_err().is_shutdown());
    }

    #[test]
    fn drop_without_await_does_not_panic() {
        let lake = EventLake::new();
        let (sender, receiver) = lake.rent::<TaskResult<i32>>();
        let handle = JoinHandle::new(receiver);

        sender.send(Ok(42));

        // Just drop the handle without awaiting.
        drop(handle);
    }
}
