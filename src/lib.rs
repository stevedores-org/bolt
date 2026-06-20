//! # bolt — Ray in Rust
//!
//! A minimal, dependency-light **distributed task & actor runtime** in the
//! spirit of [Ray](https://www.ray.io/), built for the Lornu AI agent fleet.
//!
//! bolt gives you three Ray-style primitives, all on safe async Rust:
//!
//! - **Remote tasks** — [`Runtime::submit`] runs a closure on a bounded worker
//!   pool and hands back an [`ObjectRef`] (a future result handle, like Ray's
//!   object refs).
//! - **`get`** — [`Runtime::get`] / [`Runtime::get_all`] block for results,
//!   exactly like `ray.get`.
//! - **Actors** — [`Runtime::actor`] spawns a stateful worker with a serialized
//!   mailbox; [`ActorHandle::call`] runs a closure against its state and returns
//!   an [`ObjectRef`] for the reply.
//!
//! ```
//! use bolt::Runtime;
//!
//! let rt = Runtime::builder().workers(4).build();
//!
//! // Fan out remote tasks, then `get` the results.
//! let refs: Vec<_> = (0..8).map(|i| rt.submit(move || i * i)).collect();
//! let squares = rt.get_all(refs);
//! assert_eq!(squares.iter().sum::<i32>(), 140);
//!
//! // A stateful actor with a serialized mailbox.
//! let counter = rt.actor(0u64);
//! for _ in 0..10 {
//!     counter.call(|n: &mut u64| *n += 1);
//! }
//! assert_eq!(rt.get(counter.call(|n: &mut u64| *n)), 10);
//! ```
//!
//! v0 schedules onto a local multi-threaded pool. The API is deliberately the
//! same shape a multi-node scheduler would expose, so distributing execution
//! (gRPC head/worker, a shared object store) is an additive evolution, not a
//! rewrite.

use tokio::runtime::{Builder as TokioBuilder, Runtime as TokioRuntime};
use tokio::sync::{mpsc, oneshot};

/// A handle to a future task result — bolt's analogue of a Ray object ref.
///
/// Resolve it with [`Runtime::get`] / [`Runtime::get_all`]. Each ref is
/// consumed exactly once on `get`.
#[must_use = "an ObjectRef does nothing until you `get` it"]
pub struct ObjectRef<T> {
    rx: oneshot::Receiver<T>,
}

/// A job posted to an actor's mailbox.
type Job<S> = Box<dyn FnOnce(&mut S) + Send>;

/// A cloneable handle to a stateful actor.
///
/// Calls are processed one at a time in FIFO order against the actor's owned
/// state, so the state never needs locking.
pub struct ActorHandle<S> {
    tx: mpsc::UnboundedSender<Job<S>>,
}

impl<S> Clone for ActorHandle<S> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
        }
    }
}

impl<S: Send + 'static> ActorHandle<S> {
    /// Run `f` against the actor's state and return an [`ObjectRef`] for its
    /// result. Jobs run in the order they were submitted.
    pub fn call<F, R>(&self, f: F) -> ObjectRef<R>
    where
        F: FnOnce(&mut S) -> R + Send + 'static,
        R: Send + 'static,
    {
        let (otx, orx) = oneshot::channel();
        let job: Job<S> = Box::new(move |state: &mut S| {
            // Receiver may be dropped if the caller never `get`s — that's fine.
            let _ = otx.send(f(state));
        });
        // If the actor task is gone, the send fails and `get` will surface it.
        let _ = self.tx.send(job);
        ObjectRef { rx: orx }
    }
}

/// Builder for a [`Runtime`].
pub struct RuntimeBuilder {
    workers: usize,
}

impl Default for RuntimeBuilder {
    fn default() -> Self {
        let workers = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        Self { workers }
    }
}

impl RuntimeBuilder {
    /// Set the number of worker threads (and the cap on concurrent blocking
    /// tasks). Clamped to at least 1.
    pub fn workers(mut self, workers: usize) -> Self {
        self.workers = workers.max(1);
        self
    }

    /// Build the runtime.
    pub fn build(self) -> Runtime {
        let workers = self.workers.max(1);
        let rt = TokioBuilder::new_multi_thread()
            .worker_threads(workers)
            .max_blocking_threads(workers)
            .enable_all()
            .build()
            .expect("bolt: failed to build the worker runtime");
        Runtime { rt, workers }
    }
}

/// The bolt runtime — owns the worker pool. Analogous to `ray.init()`.
pub struct Runtime {
    rt: TokioRuntime,
    workers: usize,
}

impl Default for Runtime {
    fn default() -> Self {
        Self::new()
    }
}

impl Runtime {
    /// Create a runtime sized to the host's available parallelism.
    pub fn new() -> Self {
        RuntimeBuilder::default().build()
    }

    /// Start configuring a runtime.
    pub fn builder() -> RuntimeBuilder {
        RuntimeBuilder::default()
    }

    /// Number of worker threads.
    pub fn workers(&self) -> usize {
        self.workers
    }

    /// Submit a remote task. Returns immediately with an [`ObjectRef`]; the
    /// closure runs on the worker pool.
    pub fn submit<F, T>(&self, f: F) -> ObjectRef<T>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        let (tx, rx) = oneshot::channel();
        self.rt.spawn_blocking(move || {
            // If the result receiver was dropped, no one wanted the value.
            let _ = tx.send(f());
        });
        ObjectRef { rx }
    }

    /// Block until a single task result is ready.
    ///
    /// # Panics
    /// Panics if the worker producing the value panicked or was dropped.
    pub fn get<T>(&self, handle: ObjectRef<T>) -> T
    where
        T: Send + 'static,
    {
        self.rt
            .block_on(handle.rx)
            .expect("bolt: task did not produce a result (worker panicked or was dropped)")
    }

    /// Block until every task result is ready, preserving order.
    pub fn get_all<T>(&self, handles: Vec<ObjectRef<T>>) -> Vec<T>
    where
        T: Send + 'static,
    {
        self.rt.block_on(async move {
            let mut out = Vec::with_capacity(handles.len());
            for handle in handles {
                let value = handle
                    .rx
                    .await
                    .expect("bolt: task did not produce a result (worker panicked or was dropped)");
                out.push(value);
            }
            out
        })
    }

    /// Spawn a stateful actor owning `state`; returns a cloneable
    /// [`ActorHandle`].
    pub fn actor<S>(&self, state: S) -> ActorHandle<S>
    where
        S: Send + 'static,
    {
        let (tx, mut rx) = mpsc::unbounded_channel::<Job<S>>();
        self.rt.spawn(async move {
            let mut state = state;
            while let Some(job) = rx.recv().await {
                job(&mut state);
            }
        });
        ActorHandle { tx }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[test]
    fn submit_and_get() {
        let rt = Runtime::builder().workers(2).build();
        let r = rt.submit(|| 21 * 2);
        assert_eq!(rt.get(r), 42);
    }

    #[test]
    fn get_all_preserves_order_and_runs_in_parallel() {
        let rt = Runtime::builder().workers(8).build();
        let ran = Arc::new(AtomicUsize::new(0));
        let refs: Vec<_> = (0..16)
            .map(|i| {
                let ran = ran.clone();
                rt.submit(move || {
                    ran.fetch_add(1, Ordering::SeqCst);
                    i * i
                })
            })
            .collect();
        let out = rt.get_all(refs);
        assert_eq!(out, (0..16).map(|i| i * i).collect::<Vec<_>>());
        assert_eq!(ran.load(Ordering::SeqCst), 16);
    }

    #[test]
    fn actor_state_persists_across_calls_in_order() {
        let rt = Runtime::builder().workers(4).build();
        let acc = rt.actor(Vec::<u32>::new());
        for i in 0..5 {
            // fire-and-forget record; the snapshot read below is FIFO-after.
            let _ = acc.call(move |v: &mut Vec<u32>| v.push(i));
        }
        let snapshot = rt.get(acc.call(|v: &mut Vec<u32>| v.clone()));
        assert_eq!(snapshot, vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn actor_handle_is_cloneable() {
        let rt = Runtime::new();
        let counter = rt.actor(0u64);
        let c2 = counter.clone();
        let _ = counter.call(|n: &mut u64| *n += 1);
        let _ = c2.call(|n: &mut u64| *n += 41);
        assert_eq!(rt.get(counter.call(|n: &mut u64| *n)), 42);
    }

    #[test]
    fn workers_clamped_to_at_least_one() {
        let rt = Runtime::builder().workers(0).build();
        assert_eq!(rt.workers(), 1);
        assert_eq!(rt.get(rt.submit(|| "ok")), "ok");
    }
}
