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

//! Aggregating partial state that was split into [`FinalBuckets`].
//!
//! See [`BucketedAggregation`].

use std::sync::Arc;

use arrow::compute::BatchCoalescer;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::{DataFusionError, Result};
use datafusion_execution::memory_pool::MemoryReservation;
use datafusion_execution::{TryEmitter, async_try_stream};
use futures::StreamExt;
use futures::future::BoxFuture;

use super::AggregateExec;
use super::aggregate_hash_table::{AggregateHashTable, FinalMarker};
use super::final_buckets::{FinalBuckets, MAX_BUCKET_LEVELS};
use crate::SendableRecordBatchStream;
use crate::metrics;
use crate::metrics::{BaselineMetrics, MetricBuilder, RecordOutput};
use crate::spill::spill_manager::SpillManager;
use crate::stream::RecordBatchStreamAdapter;

/// What a hash aggregation stream needs to split the groups of a hash table
/// that has reached `hash_aggregate_bucket_threshold` into [`FinalBuckets`],
/// and to aggregate those buckets one after another.
///
/// The buckets hold partial state rows, whatever the input of the stream is:
/// a final aggregation routes its input as is, a single stage aggregation
/// routes the state of the table that aggregates its raw input.
pub(super) struct BucketedAggregation {
    /// Number of groups in one hash table that triggers bucketing
    threshold: usize,
    /// Aggregate configuration used to construct the table of each bucket:
    /// its `group_by` refers to the columns of `state_schema`.
    agg: AggregateExec,
    /// Original partition index.
    partition: usize,
    /// Target batch size from configuration.
    batch_size: usize,
    /// Schema of the partial state rows held by the buckets.
    state_schema: SchemaRef,
    /// Schema of the aggregation's output.
    output_schema: SchemaRef,
    /// `None` if spilling is not supported by the configured `DiskManager`.
    spill_manager: Option<SpillManager>,
    /// Number of times a hash table was split into buckets
    bucket_splits: metrics::Count,
    /// Number of times the rows of a bucket were replaced by their aggregated state
    bucket_compactions: metrics::Count,
}

/// What [`BucketedAggregation::output_stream`] works with while it runs.
struct BucketOutput {
    reservation: MemoryReservation,
    baseline_metrics: BaselineMetrics,
    /// Combines the output of small buckets into batches of the target size.
    coalescer: BatchCoalescer,
}

impl BucketedAggregation {
    /// `agg` must aggregate rows of `state_schema` as partial state: see
    /// [`AggregateHashTable::<FinalMarker>::new_over_state`].
    pub(super) fn new(
        threshold: usize,
        agg: AggregateExec,
        partition: usize,
        batch_size: usize,
        state_schema: SchemaRef,
        output_schema: SchemaRef,
        spill_manager: Option<SpillManager>,
    ) -> Self {
        let bucket_splits =
            MetricBuilder::new(&agg.metrics).counter("bucket_splits", partition);
        let bucket_compactions =
            MetricBuilder::new(&agg.metrics).counter("bucket_compactions", partition);
        Self {
            threshold,
            agg,
            partition,
            batch_size,
            state_schema,
            output_schema,
            spill_manager,
            bucket_splits,
            bucket_compactions,
        }
    }

    /// True if a schema of partial state rows can be bucketed.
    ///
    /// Bucketing turns the state of a table into rows and merges those rows
    /// again. That is cheap for fixed-width and string state, but nested
    /// state (the lists kept by `count(distinct)`, `array_agg` or `median`)
    /// is costly to rebuild and takes more memory as rows than inside the
    /// accumulator, so such aggregations keep their single table.
    pub(super) fn supports_state(
        state_schema: &SchemaRef,
        num_group_columns: usize,
    ) -> bool {
        !state_schema
            .fields()
            .iter()
            .skip(num_group_columns)
            .any(|field| field.data_type().is_nested())
    }

    pub(super) fn threshold(&self) -> usize {
        self.threshold
    }

    fn new_table(&self) -> Result<AggregateHashTable<FinalMarker>> {
        AggregateHashTable::<FinalMarker>::new_over_state(
            &self.agg,
            &self.state_schema,
            self.partition,
            Arc::clone(&self.output_schema),
            self.batch_size,
        )
    }

    /// Starts the buckets of `level` with `state`, the state of the table
    /// that is split. `kept` is the share of its input rows which that table
    /// kept as groups.
    pub(super) fn split(
        &self,
        level: u32,
        state: Option<RecordBatch>,
        kept: f64,
    ) -> Result<FinalBuckets> {
        let mut buckets = FinalBuckets::new(
            &self.state_schema,
            self.agg.group_by.num_group_exprs(),
            self.batch_size,
            level,
            self.spill_manager.clone(),
        );
        self.bucket_splits.add(1);
        buckets.expect_kept(kept);
        if let Some(state) = state {
            buckets.route(&state)?;
        }
        Ok(buckets)
    }

    /// Replaces the rows of every bucket that is due for it by their aggregated
    /// state. See the compaction section of [`FinalBuckets`].
    pub(super) fn compact(
        &self,
        buckets: &mut FinalBuckets,
        table: &mut Option<AggregateHashTable<FinalMarker>>,
    ) -> Result<()> {
        while let Some(index) = buckets.bucket_to_compact() {
            let table = match table {
                Some(table) => table,
                None => table.insert(self.new_table()?),
            };
            let mut input_rows = 0;
            for batch in buckets.take_bucket(index)? {
                input_rows += batch.num_rows();
                table.aggregate_batch(&batch)?;
            }
            buckets.put_compacted(
                index,
                table.take_state_batch_keep_capacity()?,
                input_rows,
            );
            self.bucket_compactions.add(1);
        }
        Ok(())
    }

    /// Reserves `other_bytes` plus the memory of `buckets`, spilling buckets
    /// for as long as the reservation does not fit.
    pub(super) fn reserve(
        &self,
        reservation: &MemoryReservation,
        other_bytes: usize,
        buckets: &mut FinalBuckets,
    ) -> Result<()> {
        loop {
            let size = other_bytes.saturating_add(buckets.memory_size());
            match reservation.try_resize(size) {
                Ok(()) => return Ok(()),
                Err(e @ DataFusionError::ResourcesExhausted(_)) => {
                    if buckets.spill_largest()? {
                        continue;
                    }
                    // Every bucket is on disk. What is left is the fixed cost
                    // of routing a batch, which no spill can release, so go on
                    // like the sort based spill path does after it has spilled
                    // its table: with what the pool still grants, which covers
                    // at least the memory held besides these buckets.
                    if buckets.is_fully_spilled()
                        && reservation.try_resize(other_bytes).is_ok()
                    {
                        return Ok(());
                    }
                    return Err(e.context("Hash aggregate has no more buckets to spill"));
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Aggregates the buckets one after another, each with a table of its
    /// own size, and emits the groups of a bucket before reading the next one.
    ///
    /// A bucket whose table reaches the bucketing threshold again, or does
    /// not fit in memory, is split into buckets of the next level.
    pub(super) fn output_stream(
        self: Arc<Self>,
        buckets: FinalBuckets,
        reservation: MemoryReservation,
        baseline_metrics: BaselineMetrics,
    ) -> SendableRecordBatchStream {
        let schema = Arc::clone(&self.output_schema);
        let batch_size = self.batch_size;
        let stream = async_try_stream(move |mut emitter| async move {
            let mut output = BucketOutput {
                reservation,
                baseline_metrics,
                coalescer: BatchCoalescer::new(
                    Arc::clone(&self.output_schema),
                    batch_size,
                )
                .with_biggest_coalesce_batch_size(Some(batch_size / 2)),
            };
            self.produce_output(buckets, &mut output, &mut emitter)
                .await?;
            output.reservation.try_resize(0)?;

            output.coalescer.finish_buffered_batch()?;
            while let Some(batch) = output.coalescer.next_completed_batch() {
                emitter
                    .emit(batch.record_output(&output.baseline_metrics))
                    .await;
            }
            Ok(())
        });
        Box::pin(RecordBatchStreamAdapter::new(schema, stream))
    }

    fn produce_output<'a>(
        &'a self,
        buckets: FinalBuckets,
        output: &'a mut BucketOutput,
        emitter: &'a mut TryEmitter<RecordBatch, DataFusionError>,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let elapsed_compute = output.baseline_metrics.elapsed_compute().clone();
            let next_level = buckets.level() + 1;

            let sources = buckets.into_sources()?;
            // Memory of the buckets that wait for their turn
            let mut waiting_bytes: usize =
                sources.iter().map(|source| source.memory_size()).sum();

            // One table aggregates all the buckets, one after another
            let mut reusable_table = None;
            for source in sources {
                let mut source_bytes = source.memory_size();
                waiting_bytes -= source_bytes;
                let mut input = source.into_stream(&self.state_schema);

                let mut timer = elapsed_compute.timer();
                let mut hash_table = match reusable_table.take() {
                    Some(hash_table) => hash_table,
                    None => self.new_table()?.with_restart(),
                };
                let mut table_rows = 0usize;
                let mut sub_buckets: Option<FinalBuckets> = None;
                let mut compaction_table = None;

                while let Some(batch) = input.next().await.transpose()? {
                    // Batches of an in-memory bucket are released as they are read
                    source_bytes =
                        source_bytes.saturating_sub(batch.get_array_memory_size());
                    let held_bytes = waiting_bytes + source_bytes;

                    if let Some(sub_buckets) = sub_buckets.as_mut() {
                        sub_buckets.route(&batch)?;
                        self.compact(sub_buckets, &mut compaction_table)?;
                        self.reserve(&output.reservation, held_bytes, sub_buckets)?;
                        continue;
                    }

                    hash_table.aggregate_batch(&batch)?;
                    table_rows += batch.num_rows();

                    let can_split = next_level < MAX_BUCKET_LEVELS;
                    let split = match output
                        .reservation
                        .try_resize(held_bytes + hash_table.memory_size())
                    {
                        Ok(()) => {
                            can_split
                                && hash_table.building_group_count() >= self.threshold
                        }
                        Err(DataFusionError::ResourcesExhausted(_)) if can_split => true,
                        Err(e) => return Err(e),
                    };
                    if split {
                        let kept = hash_table.building_group_count() as f64
                            / table_rows.max(1) as f64;
                        let mut new_buckets =
                            self.split(next_level, hash_table.take_state_batch()?, kept)?;
                        self.reserve(
                            &output.reservation,
                            held_bytes + hash_table.memory_size(),
                            &mut new_buckets,
                        )?;
                        sub_buckets = Some(new_buckets);
                    }
                }
                drop(input);

                if let Some(sub_buckets) = sub_buckets {
                    // The table handed its groups over and is empty again
                    reusable_table = Some(hash_table);
                    timer.done();
                    self.produce_output(sub_buckets, output, emitter).await?;
                    continue;
                }

                hash_table.start_output()?;
                while let Some(batch) = hash_table.next_output_batch()? {
                    output
                        .reservation
                        .try_resize(waiting_bytes + hash_table.memory_size())?;
                    output.coalescer.push_batch(batch)?;
                    while let Some(batch) = output.coalescer.next_completed_batch() {
                        timer.done();
                        emitter
                            .emit(batch.record_output(&output.baseline_metrics))
                            .await;
                        timer = elapsed_compute.timer();
                    }
                }
                if hash_table.restart() {
                    reusable_table = Some(hash_table);
                }
                timer.done();
            }

            Ok(())
        })
    }
}
