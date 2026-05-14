// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use datafusion_common::instant::Instant;
use pin_project::{pin_project, pinned_drop};

use super::Time;

/// Wraps any [`Future`] and accumulates the wall-clock time spent inside
/// each [`Future::poll`] call into `elapsed_compute`. Everything that
/// executes synchronously within a `poll()` scope is measured — including
/// CPU-bound work, memory copies, and any blocking the future performs
/// before returning. Time between polls (when the runtime has suspended the
/// future waiting for I/O, a channel, or a waker) is not measured.
///
/// For futures that mix synchronous CPU work with async I/O this gives a
/// good approximation of CPU time: async I/O causes the future to yield
/// (`Poll::Pending`), so the I/O latency is excluded automatically.
///
/// Note: uses `pin-project` rather than `pin-project-lite` in order to
/// support `PinnedDrop`, which ensures accumulated time is flushed even
/// if the future is cancelled (dropped before completion).
#[pin_project(PinnedDrop)]
pub struct ElapsedComputeFuture<T> {
    #[pin]
    inner: T,
    /// Local accumulator: elapsed time is collected here during each `poll()`
    /// and only flushed to `elapsed_compute` on completion (`Poll::Ready`) or
    /// on drop (`PinnedDrop`). Keeping a separate local `Duration` avoids
    /// performing an atomic operation on every `poll()` call, at the cost of
    /// the reported metric value being unavailable until the future finishes.
    curr: Duration,
    elapsed_compute: Time,
}

#[pinned_drop]
impl<T> PinnedDrop for ElapsedComputeFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if self.curr > Duration::default() {
            let self_projected = self.project();
            self_projected
                .elapsed_compute
                .add_duration(*self_projected.curr);
        }
    }
}

impl<O, F: Future<Output = O>> Future for ElapsedComputeFuture<F> {
    type Output = O;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let self_projected = self.project();
        let start = Instant::now();
        let result = self_projected.inner.poll(cx);
        *self_projected.curr += start.elapsed();
        if result.is_ready() {
            self_projected
                .elapsed_compute
                .add_duration(*self_projected.curr);
            *self_projected.curr = Duration::default();
        }
        result
    }
}

/// Extension trait that wraps any [`Future`] with [`ElapsedComputeFuture`].
pub trait ElapsedComputeFutureExt: Future + Sized {
    /// Wraps this future so that the time spent inside each [`Future::poll`]
    /// call is accumulated into `elapsed_compute`. See [`ElapsedComputeFuture`]
    /// for a full description of what is and is not measured.
    fn with_elapsed_compute(self, elapsed_compute: Time) -> ElapsedComputeFuture<Self>;
}

impl<O, F: Future<Output = O>> ElapsedComputeFutureExt for F {
    fn with_elapsed_compute(self, elapsed_compute: Time) -> ElapsedComputeFuture<Self> {
        ElapsedComputeFuture {
            inner: self,
            curr: Duration::default(),
            elapsed_compute,
        }
    }
}
