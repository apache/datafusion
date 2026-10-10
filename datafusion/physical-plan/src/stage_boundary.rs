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

//! Provides [`StageBoundary`], which lets a driver wait for input partitions
//! to finish before allowing downstream execution to continue.

use std::sync::Arc;

use datafusion_common::Result;
use datafusion_execution::TaskContext;

use crate::ExecutionPlan;

/// An [`ExecutionPlan`] that materializes its input under driver control.
///
/// [`Self::prime`] drains an input partition independently of downstream
/// polling. Output streams remain pending until [`Self::release`], allowing
/// the driver to inspect the completed stage before downstream execution.
///
/// Implementations must preserve the input schema, partitioning, ordering,
/// batches, and errors, while reporting their own execution behavior in
/// [`ExecutionPlan::properties`].
///
/// Callers retain boundary handles when constructing the plan.
///
/// # Examples
///
/// See [staged_execution.rs] for a buffering implementation and runnable examples.
///
/// [staged_execution.rs]: https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/execution_monitoring/staged_execution.rs
///
/// Drive a group of independent boundaries:
///
/// ```no_run
/// use std::sync::Arc;
///
/// use datafusion_common::Result;
/// use datafusion_execution::TaskContext;
/// use datafusion_physical_plan::StageBoundary;
///
/// async fn drive_stage(
///     boundaries: &[Arc<dyn StageBoundary>],
///     context: Arc<TaskContext>,
/// ) -> Result<()> {
///     for boundary in boundaries {
///         let partition_count = boundary
///             .properties()
///             .output_partitioning()
///             .partition_count();
///         for partition in 0..partition_count {
///             boundary.prime(partition, Arc::clone(&context))?;
///         }
///     }
///
///     while !boundaries.iter().all(|boundary| {
///         let partition_count = boundary
///             .properties()
///             .output_partitioning()
///             .partition_count();
///         (0..partition_count).all(|partition| boundary.is_ready(partition))
///     }) {
///         tokio::task::yield_now().await;
///     }
///
///     // Inspect the completed stage before resuming downstream execution.
///
///     for boundary in boundaries {
///         boundary.release();
///     }
///     Ok(())
/// }
/// ```
pub trait StageBoundary: ExecutionPlan {
    /// Starts draining one input partition in the background.
    ///
    /// Idempotent for each partition. Returns an error for an invalid partition
    /// or a failure to start the drain, including a synchronous error from
    /// [`ExecutionPlan::execute`]. Buffers stream errors until [`Self::release`]
    /// and marks the partition ready.
    fn prime(&self, partition: usize, context: Arc<TaskContext>) -> Result<()>;

    /// Returns whether a partition's drain reached EOF or terminated with an error.
    ///
    /// Once ready, a partition remains ready. Returns `false` for an invalid
    /// partition.
    fn is_ready(&self, partition: usize) -> bool;

    /// Allows all materialized output partitions to flow downstream.
    ///
    /// Idempotent. Drivers call this after all of this boundary's partitions
    /// are ready.
    fn release(&self);
}
