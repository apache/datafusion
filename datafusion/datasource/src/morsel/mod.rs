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

//! Structures for Morsel Driven IO
//!
//! Morsel Driven IO is a technique for parallelizing the reading of large files
//! by dividing them into smaller "morsels" that can be processed independently.
//! It is inspired by the paper [Morsel-Driven Parallelism: A NUMA-Aware Query
//! Evaluation Framework for the Many-Core Age](https://db.in.tum.de/~leis/papers/morsels.pdf)

mod adapters;
#[cfg(test)]
pub(crate) mod test_utils;

use crate::PartitionedFile;
use arrow::array::RecordBatch;
use datafusion_common::error::Result;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use std::fmt::Debug;

pub use adapters::FileOpenerMorselizer;

/// A Morsel of work ready to resolve to a stream of [`RecordBatch`]es
///
/// This represents a single morsel of work that is ready to be processed. It
/// has all data necessary (does not need any I/O) and is ready to be turned
/// into a stream of RecordBatches for processing by the execution engine.
pub trait Morsel: Send + Debug {
    /// Consume this morsel and produce a stream of RecordBatches for processing.
    ///
    /// This should not do any IO work, such as reading from the file.
    fn into_stream(self: Box<Self>) -> BoxStream<'static, Result<RecordBatch>>;

    /// If supported, split this morsel into smaller morsels.
    ///
    /// If not possible or not supported, return an empty Vector.
    ///
    /// This is used for dynamic load balancing of work where there are some
    /// tasks that have nothing else scheduled.
    fn split(&mut self) -> Result<Vec<Box<dyn Morsel>>>;
}

/// A Morselizer takes a single PartitionedFile and breaks it down into smaller chunks
/// that can be planned and read in parallel by the execution engine. This is the entry point for
/// morsel driven IO.
pub trait Morselizer: Send + Sync + Debug {
    /// Return MorselPlanners for this file.
    ///
    /// Each MorselPlanner is responsible for I/O and planning morsels for a
    /// single scan of the file. Returning multiple MorselPlanners allows for
    /// multiple concurrent scans of the same file.
    ///
    /// This may involve CPU work, such as parsing parquet metadata and
    /// evaluating pruning predicates. It should NOT do any IO work, such as
    /// reading from the file. If IO is required, it should return a future that
    /// the caller can poll to drive the IO work to completion, and once the
    /// future is complete, the caller can call `morselize` again to get the
    /// next morsels.
    fn morselize(&self, file: PartitionedFile) -> Result<Vec<Box<dyn MorselPlanner>>>;
}

/// A Morsel Planner is responsible for creating morsels for a given scan.
///
/// The MorselPlanner is the unit of I/O -- there is only ever a single IO
/// outstanding for a specific  MorselPlanner. DataFusion will potentially run
/// multiple MorselPlanners in parallel which corresponds to multiple parallel
/// I/O requests.
///
/// It is not a Rust `Stream` so that it can explicitly separate CPU bound
/// work from IO work.
///
/// The design is similar to `ParquetPushDecoder` -- when `plan` is called, it
/// should do CPU work to produce the next morsels or discover the next I/O
/// phase.
///
/// Best practice is to spawn IO in a tokio Task in a separate IO runtime to
/// ensure that CPU work doesn't block/slowdown IO work, but this is not
/// strictly required by the API.
pub trait MorselPlanner: Send + Debug {
    /// Attempt to plan morsels. This may involve CPU work, such as parsing
    /// parquet metadata and evaluating pruning predicates.
    ///
    /// It should NOT do any IO work, such as reading from the file. If IO is
    /// required, the returned [`MorselPlan`] should contain a future that the
    /// caller polls to drive the IO work to completion. Once the future is
    /// complete, the caller can call `plan` again to get the next morsels.
    ///
    /// Note this function is not async to make it clear explicitly that if IO
    /// is required, it should be done in the returned `io_future`.
    ///
    /// Returns `None` if the MorselPlanner has no more work to do (is done).
    ///
    /// # Empty Morsel Plans
    ///
    /// It may return Some(..) with an empty MorselPlan, which means it is ready
    /// for more CPU work and should be called again.
    ///
    /// # Output Ordering
    ///
    /// See the comments on [`MorselPlan`] for the logical output order
    fn plan(&mut self) -> Result<Option<MorselPlan>>;
}

/// Return result of [`MorselPlanner::plan`]
///
/// # Logical Ordering
/// For plans where the output order of rows is maintained, the output order of
/// a [`MorselPlanner`] is logically defined as follows:
/// 1. All morsels that are directly produced
/// 2. (recursively) All morsels produced by the returned `planners`
#[derive(Default)]
pub struct MorselPlan {
    /// Any Morsels that are ready for processing.
    morsels: Vec<Box<dyn Morsel>>,
    /// Any newly-created planners that are ready for CPU work.
    planners: Vec<Box<dyn MorselPlanner>>,
    /// A future that will drive any IO work to completion
    ///
    /// DataFusion will poll this future occasionally to drive the IO work to
    /// completion. Once the future resolves, DataFusion will call  `plan` again
    /// to get the next morsels. Best practice is to run this in a task on a
    /// separate IO runtime to ensure that CPU work is not blocked by IO work,
    /// but this is not strictly required by the API.
    io_future: Option<BoxFuture<'static, Result<()>>>,
}

impl MorselPlan {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn with_morsels(mut self, morsels: Vec<Box<dyn Morsel>>) -> Self {
        self.morsels = morsels;
        self
    }

    pub fn with_planners(mut self, planners: Vec<Box<dyn MorselPlanner>>) -> Self {
        self.planners = planners;
        self
    }

    pub fn with_io_future(mut self, io_future: BoxFuture<'static, Result<()>>) -> Self {
        self.io_future = Some(io_future);
        self
    }

    pub fn take_io_future(&mut self) -> Option<BoxFuture<'static, Result<()>>> {
        self.io_future.take()
    }

    pub fn set_io_future(&mut self, io_future: BoxFuture<'static, Result<()>>) {
        self.io_future = Some(io_future);
    }

    pub fn take_morsels(&mut self) -> Vec<Box<dyn Morsel>> {
        std::mem::take(&mut self.morsels)
    }

    pub fn take_planners(&mut self) -> Vec<Box<dyn MorselPlanner>> {
        std::mem::take(&mut self.planners)
    }

    pub fn has_io_future(&self) -> bool {
        self.io_future.is_some()
    }
}
