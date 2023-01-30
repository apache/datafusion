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

//! Metrics with memory usage tracking capability

use crate::physical_plan::metrics::{
    BaselineMetrics, Count, ExecutionPlanMetricsSet, Time,
};
use std::sync::Arc;
use std::task::Poll;

use crate::error::Result;
use crate::execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use arrow::record_batch::RecordBatch;

/// Wraps a [`BaselineMetrics`] and records memory usage on a [`MemoryReservation`]
#[derive(Debug)]
pub struct MemTrackingMetrics {
    reservation: MemoryReservation,
    metrics: BaselineMetrics,
}

/// Delegates most of the metrics functionalities to the inner BaselineMetrics,
/// intercept memory metrics functionalities and do memory manager bookkeeping.
impl MemTrackingMetrics {
    /// Create memory tracking metrics with reference to memory manager
    pub fn new(
        metrics: &ExecutionPlanMetricsSet,
        pool: &Arc<dyn MemoryPool>,
        partition: usize,
    ) -> Self {
        let reservation = MemoryConsumer::new(format!("MemTrackingMetrics[{partition}]"))
            .register(pool);

        Self {
            reservation,
            metrics: BaselineMetrics::new(metrics, partition),
        }
    }

    /// return the metric for cpu time spend in this operator
    pub fn elapsed_compute(&self) -> &Time {
        self.metrics.elapsed_compute()
    }

    /// return the size for current memory usage
    pub fn mem_used(&self) -> usize {
        self.metrics.mem_used().value()
    }

    /// setup initial memory usage and register it with memory manager
    pub fn init_mem_used(&mut self, size: usize) {
        self.metrics.mem_used().set(size);
        self.reservation.resize(size)
    }

    /// return the metric for the total number of output rows produced
    pub fn output_rows(&self) -> &Count {
        self.metrics.output_rows()
    }

    /// Records the fact that this operator's execution is complete
    /// (recording the `end_time` metric).
    ///
    /// Note care should be taken to call `done()` manually if
    /// `MemTrackingMetrics` is not `drop`ped immediately upon operator
    /// completion, as async streams may not be dropped immediately
    /// depending on the consumer.
    pub fn done(&self) {
        self.metrics.done()
    }

    /// Record that some number of rows have been produced as output
    ///
    /// See the [`super::RecordOutput`] for conveniently recording record
    /// batch output for other thing
    pub fn record_output(&self, num_rows: usize) {
        self.metrics.record_output(num_rows)
    }

    /// Process a poll result of a stream producing output for an
    /// operator, recording the output rows and stream done time and
    /// returning the same poll result
    pub fn record_poll(
        &self,
        poll: Poll<Option<Result<RecordBatch>>>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        self.metrics.record_poll(poll)
    }
}
