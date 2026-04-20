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

//! Push-based [`Pipeline`] trait. Ported verbatim from PR apache/datafusion#2226.

use std::fmt::Debug;
use std::task::{Context, Poll};

use arrow::record_batch::RecordBatch;
use datafusion_common::Result;

/// A push-based operator. The scheduler drives progress by polling one of the
/// pipeline's output partitions via [`Pipeline::poll_partition`]; when a batch
/// is produced the scheduler routes it to the next pipeline's
/// [`Pipeline::push`]. End-of-input is signalled by [`Pipeline::close`].
///
/// The split between eager (push-time) and lazy (poll-time) work is entirely
/// the pipeline's choice — e.g. a repartition breaker does most work in
/// `push`, while a streaming adapter forwards from a wrapped
/// `SendableRecordBatchStream` inside `poll_partition`.
pub trait Pipeline: Debug + Send + Sync {
    /// Push a batch into `child` of the pipeline for the given `partition`.
    fn push(&self, input: RecordBatch, child: usize, partition: usize) -> Result<()>;

    /// Signal that `child` / `partition` will receive no further batches.
    fn close(&self, child: usize, partition: usize);

    /// Number of output partitions this pipeline produces.
    fn output_partitions(&self) -> usize;

    /// Attempt to drive progress for one output partition.
    ///
    /// * `Poll::Ready(Some(Ok(batch)))` — next batch for the partition.
    /// * `Poll::Ready(Some(Err(e)))` — query failed.
    /// * `Poll::Ready(None)` — partition exhausted.
    /// * `Poll::Pending` — pipeline has stored `cx.waker()` and will wake
    ///   the scheduler when it can make progress.
    fn poll_partition(
        &self,
        cx: &mut Context<'_>,
        partition: usize,
    ) -> Poll<Option<Result<RecordBatch>>>;
}
