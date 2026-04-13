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

//! Structures for Morsel Driven IO.
//!
//! NOTE: As of DataFusion 54.0.0, these are experimental APIs that may change
//! substantially.
//!
//! Morsel Driven IO is a technique for parallelizing the reading of large files
//! by dividing them into smaller "morsels" that are processed independently.
//!
//! It is inspired by the paper [Morsel-Driven Parallelism: A NUMA-Aware Query
//! Evaluation Framework for the Many-Core Age](https://db.in.tum.de/~leis/papers/morsels.pdf).

use crate::PartitionedFile;
use arrow::array::RecordBatch;
use datafusion_common::Result;
use futures::FutureExt;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use std::fmt::Debug;
use std::pin::Pin;
use std::task::{Context, Poll};

/// A Morsel of work ready to resolve to a stream of [`RecordBatch`]es.
///
/// This represents a single morsel of work that is ready to be processed. It
/// has all data necessary (does not need any I/O) and is ready to be turned
/// into a stream of [`RecordBatch`]es for processing by the execution engine.
pub trait Morsel: Send + Debug {
    /// Consume this morsel and produce a stream of [`RecordBatch`]es for processing.
    ///
    /// Note: This may do CPU work to decode already-loaded data, but should not
    /// do any I/O work such as reading from the file.
    fn into_stream(self: Box<Self>) -> BoxStream<'static, Result<RecordBatch>>;
}

/// A Morselizer takes a single [`PartitionedFile`] and creates the initial planner
/// for that file.
///
/// This is the entry point for morsel driven I/O.
pub trait Morselizer: Send + Sync + Debug {
    /// Return the initial [`MorselPlanner`] for this file.
    ///
    /// Morselizing a file may involve CPU work, such as parsing parquet
    /// metadata and evaluating pruning predicates. It should NOT do any I/O
    /// work, such as reading from the file. Any needed I/O should be done using
    /// [`MorselPlan::with_pending_planner`].
    fn plan_file(&self, file: PartitionedFile) -> Result<Box<dyn MorselPlanner>>;
}

/// A Morsel Planner is responsible for creating morsels for a given scan.
///
/// The [`MorselPlanner`] is the unit of I/O. There is only ever a single I/O
/// outstanding for a specific planner. DataFusion may run
/// multiple planners in parallel, which corresponds to multiple parallel
/// I/O requests.
///
/// It is not a Rust `Stream` so that it can explicitly separate CPU bound
/// work from I/O work.
///
/// The design is similar to `ParquetPushDecoder`: when `plan` is called, it
/// should do CPU work to produce the next morsels or discover the next I/O
/// phase.
///
/// Best practice is to spawn I/O in a Tokio task on a separate runtime to
/// ensure that CPU work doesn't block or slow down I/O work, but this is not
/// strictly required by the API.
pub trait MorselPlanner: Send + Debug {
    /// Attempt to plan morsels. This may involve CPU work, such as parsing
    /// parquet metadata and evaluating pruning predicates.
    ///
    /// It should NOT do any I/O work, such as reading from the file. If I/O is
    /// required, the returned [`MorselPlan`] should contain a pending planner
    /// future that the caller polls to drive the I/O work to completion. Once
    /// that future resolves, it yields a planner ready for work.
    ///
    /// Note this function is **not async** to make it explicitly clear that if
    /// I/O is required, it should be done in the returned `io_future`.
    ///
    /// Returns `None` if the planner has no more work to do.
    ///
    /// # Empty Morsel Plans
    ///
    /// It may return `None`, which means no batches will be read from the file
    /// (e.g. due to late-pruning based on statistics).
    ///
    /// # Output Ordering
    ///
    /// See the comments on [`MorselPlan`] for the logical output order.
    fn plan(self: Box<Self>) -> Result<Option<MorselPlan>>;
}

/// Return result of [`MorselPlanner::plan`].
///
/// # Logical Ordering
///
/// For plans where the output order of rows is maintained, the output order of
/// a [`MorselPlanner`] is logically defined as follows:
/// 1. All morsels that are directly produced
/// 2. Recursively, all morsels produced by the returned `planners`
#[derive(Default)]
pub struct MorselPlan {
    /// Morsels ready for CPU work
    morsels: Vec<Box<dyn Morsel>>,
    /// Planners that are ready for CPU work.
    ready_planners: Vec<Box<dyn MorselPlanner>>,
    /// A future with planner I/O that resolves to a CPU ready planner.
    ///
    /// DataFusion will poll this future occasionally to drive the I/O work to
    /// completion. Once it resolves, planning continues with the returned
    /// planner.
    pending_planner: Option<PendingMorselPlanner>,
}

impl MorselPlan {
    /// Create an empty morsel plan.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the ready morsels.
    pub fn with_morsels(mut self, morsels: Vec<Box<dyn Morsel>>) -> Self {
        self.morsels = morsels;
        self
    }

    /// Set the ready child planners.
    pub fn with_planners(mut self, planners: Vec<Box<dyn MorselPlanner>>) -> Self {
        self.ready_planners = planners;
        self
    }

    /// Set the pending planner for an I/O phase.
    pub fn with_pending_planner<F>(mut self, io_future: F) -> Self
    where
        F: Future<Output = Result<Box<dyn MorselPlanner>>> + Send + 'static,
    {
        self.pending_planner = Some(PendingMorselPlanner::new(io_future));
        self
    }

    /// Set the pending planner  for an I/O phase.
    pub fn set_pending_planner<F>(&mut self, io_future: F)
    where
        F: Future<Output = Result<Box<dyn MorselPlanner>>> + Send + 'static,
    {
        self.pending_planner = Some(PendingMorselPlanner::new(io_future));
    }

    /// Take the ready morsels.
    pub fn take_morsels(&mut self) -> Vec<Box<dyn Morsel>> {
        std::mem::take(&mut self.morsels)
    }

    /// Take the ready child planners.
    pub fn take_ready_planners(&mut self) -> Vec<Box<dyn MorselPlanner>> {
        std::mem::take(&mut self.ready_planners)
    }

    /// Take the pending I/O future, if any.
    pub fn take_pending_planner(&mut self) -> Option<PendingMorselPlanner> {
        self.pending_planner.take()
    }

    /// Returns `true` if this plan contains an I/O future.
    pub fn has_io_future(&self) -> bool {
        self.pending_planner.is_some()
    }
}

/// Wrapper for I/O that must complete before planning can continue.
pub struct PendingMorselPlanner {
    future: BoxFuture<'static, Result<Box<dyn MorselPlanner>>>,
}

impl PendingMorselPlanner {
    /// Create a new pending planner future.
    ///
    /// Example
    /// ```
    /// # use datafusion_common::DataFusionError;
    /// # use datafusion_datasource::morsel::{MorselPlanner, PendingMorselPlanner};
    /// let work = async move {
    ///  let planner: Box<dyn MorselPlanner> = {
    ///   // Do I/O work here, then return the next planner to run.
    ///  # unimplemented!();
    ///   };
    ///   Ok(planner) as Result<_, DataFusionError>;
    /// };
    /// let pending_io = PendingMorselPlanner::new(work);
    /// ```
    pub fn new<F>(future: F) -> Self
    where
        F: Future<Output = Result<Box<dyn MorselPlanner>>> + Send + 'static,
    {
        Self {
            future: future.boxed(),
        }
    }

    /// Consume this wrapper and return the underlying future.
    pub fn into_future(self) -> BoxFuture<'static, Result<Box<dyn MorselPlanner>>> {
        self.future
    }
}

/// Forwards polling to the underlying future.
impl Future for PendingMorselPlanner {
    type Output = Result<Box<dyn MorselPlanner>>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // forward request to inner
        self.future.as_mut().poll(cx)
    }
}
