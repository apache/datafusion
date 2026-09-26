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

//! 2-stage hash aggregation stream implementation.
//!
//! See comments in [`PartialHashAggregateStream`] and [`FinalHashAggregateStream`]
//! for details.

use std::collections::{HashMap, VecDeque};
use std::mem::size_of;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::hash_utils::{RandomState, create_hashes};
use datafusion_common::{
    DataFusionError, Result, assert_ne_or_internal_err, internal_datafusion_err,
};
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion_execution::{TaskContext, TryEmitter, async_try_stream};
use futures::stream::{Stream, StreamExt};

use super::AggregateExec;
use super::aggregate_hash_table::{
    AggregateHashTable, FinalMarker, OrderedAggregateTableMetrics, PartialMarker,
    PartialSkipMarker,
};
use super::bucketed_aggregation::BucketedAggregation;
use super::final_buckets::FinalBuckets;
use super::skip_partial::SkipAggregationProbe;
use super::spill::AggregateSpill;
use crate::metrics::{
    BaselineMetrics, MetricBuilder, MetricCategory, RecordOutput, SpillMetrics,
};
use crate::stream::{EmptyRecordBatchStream, RecordBatchStreamAdapter};
use crate::{InputOrderMode, SendableRecordBatchStream, metrics};

/// Hash aggregation is implemented in two stages: partial and final. This
/// stream implements the partial stage.
///
/// # Example
///
/// SELECT k, AVG(v) FROM t GROUP BY k;
///
/// ## Plan
/// AggregateExec(stage=final)
/// -- RepartitionExec(hash(k))
/// ---- AggregateExec(stage=partial)
///
/// ## Partial Stage Behavior
/// Input: raw rows
/// Output: partial states for all groups (for example, `AVG(x)` emits `SUM(x)`
/// and `COUNT(x)`)
///
/// ## Final Stage Behavior
/// Input: partial states
/// Output: results for all groups (for example, `AVG(x)` calculated from the
/// state)
///
/// # Optimization: DISTINCT LIMIT Soft Limit
///
/// This optimization applies to both [`PartialHashAggregateStream`] and
/// [`FinalHashAggregateStream`].
///
/// Unordered distinct queries such as:
///
/// ```sql
/// SELECT DISTINCT x FROM t LIMIT 10;
/// ```
///
/// are optimized into a two-stage aggregate like:
///
/// ```txt
/// LimitExec, limit=10
/// --AggregateExec(Final), group_by=[x], aggr=[], soft_limit=10
/// ---- RepartitionExec, partitioning=hash(x)
/// ------ AggregateExec(Partial), group_by=[x], aggr=[], soft_limit=10
/// -------- Scan(t)
/// ```
///
/// After each input batch, the stream checks whether the soft limit has been
/// reached. If so, it emits the accumulated groups and stops reading input.
///
/// This operator does not guarantee an exact limit because a single batch can
/// cross the threshold. The downstream limit operator enforces the exact result
/// size.
///
/// # Optimization: Partial Aggregation Skip
///
/// Partial aggregation can be counterproductive for high-cardinality inputs,
/// where most rows create distinct groups. The stream probes the ratio of
/// accumulated groups to input rows while it is still aggregating. If the ratio
/// crosses the configured threshold and all aggregate accumulators can convert
/// raw inputs directly to partial state, the stream emits any already
/// accumulated groups, then switches to a skip state. In that state, each
/// remaining input batch is converted directly to partial aggregate state rows
/// without inserting the rows into the grouped hash table.
///
/// # Feature: Grouping Sets
///
/// `GROUPING SETS`, `CUBE` and `ROLLUP` are expanded in the partial stage: every
/// grouping set of an input batch is evaluated (with the grouping expressions
/// that are not part of the set replaced by `NULL`, plus an internal
/// `__grouping_id` column) and interned into the same hash table. The final
/// stage then merges the expanded keys as a plain group by.
///
/// The partial aggregation skip optimization is disabled for grouping sets.
///
/// # Feature: Memory-limited Execution
///
/// ## Partial Aggregation
///
/// Partial aggregation can emit incomplete results because the final stage merges
/// all intermediate states for the same group. If the memory reservation exceeds
/// its limit after aggregating an input batch, this stream emits all accumulated
/// states and continues aggregating the remaining input with an empty table.
///
/// ## Final Aggregation
///
/// During final aggregation, group keys and states accumulate. If memory usage
/// exceeds the budget, spilling is triggered as follows:
/// 1. After aggregating a new input batch, if the memory reservation exceeds its
///    limit, spill all accumulated groups and states.
///    - Sort all groups by the group keys before spilling.
/// 2. Repeat until the input is exhausted.
/// 3. Perform a sort-preserving merge of all spill files and feed the merged output
///    into an ordered streaming aggregation, which ensures bounded memory usage and
///    evaluates the final result.
///    - [`OrderedFinalAggregateStream`](super::ordered_final_stream::OrderedFinalAggregateStream) is reused for the streaming aggregation.
pub(crate) struct PartialHashAggregateStream {
    /// Output schema: group columns followed by partial aggregate state columns.
    schema: SchemaRef,

    /// Input batches containing raw rows, not partial aggregate state.
    input: SendableRecordBatchStream,

    /// Target output batch size from configuration.
    batch_size: usize,

    /// Memory reservation for group keys and accumulators.
    reservation: MemoryReservation,

    /// Execution metrics shared with the aggregate plan node.
    baseline_metrics: BaselineMetrics,

    /// Tracks partial aggregation row reduction, matching `GroupedHashAggregateStream`.
    reduction_factor: metrics::RatioMetrics,

    /// Number of times accumulated states were emitted due to memory pressure.
    early_emit_count: metrics::Count,

    /// `None` unless `hash_aggregate_bucket_threshold` is set and applies.
    table_flush: Option<PartialTableFlush>,

    /// Tracks whether partial aggregation should switch to direct state conversion.
    skip_aggregation_probe: Option<SkipAggregationProbe>,

    /// Optional soft limit on the number of groups to accumulate before output.
    ///
    /// Invariant: when this is `Some(..)`, the accumulators inside `hash_table` must
    /// be empty. See struct comments for details.
    group_values_soft_limit: Option<usize>,

    /// The hash table owns the lower-level state for emitting output batches.
    hash_table: Option<AggregateHashTable<PartialMarker>>,
}

/// Hash aggregation is implemented in two stages: partial and final. This
/// stream implements the final stage.
///
/// See [`PartialHashAggregateStream`] for details.
pub(crate) struct FinalHashAggregateStream {
    /// Output schema: group columns followed by final aggregate value columns.
    schema: SchemaRef,

    /// Input batches containing partial aggregate state rows.
    input: SendableRecordBatchStream,

    /// Execution metrics shared with the aggregate plan node.
    baseline_metrics: BaselineMetrics,

    /// Memory reservation for group keys, accumulators, and spill sorting.
    reservation: MemoryReservation,

    /// See comments for the same variable in [`PartialHashAggregateStream`].
    group_values_soft_limit: Option<usize>,

    /// The hash table owns the lower-level
    /// state for emitting output batches.
    ///
    /// This will be None when creating the stream
    hash_table: Option<AggregateHashTable<FinalMarker>>,
    /// `None` if spilling is not supported by the configured `DiskManager`.
    spill_context: Option<Box<AggregateSpill>>,
    /// `None` unless `hash_aggregate_bucket_threshold` is set and applies.
    bucketing: Option<Arc<BucketedAggregation>>,
}

#[derive(PartialEq)]
enum HandleInputResult {
    ProcessNext,
    ReachedLimit,
    #[expect(clippy::upper_case_acronyms)]
    OOM,
    /// The table reached the bucket threshold, see [`PartialTableFlush`]
    TableFull,
    SwitchToSkipAggregation,
}

/// A final table whose input is [`BARELY_REDUCING`] moves into buckets once
/// it holds this share of the bucket threshold.
///
/// Moving a table into buckets aggregates its groups a second time. An input
/// of about one row per group never earns that back unless the table is
/// small compared to the input, and such an input gains nothing from a larger
/// first table either.
const EARLY_BUCKETS_DIVISOR: usize = 4;

/// Share of the rows seen so far that started a new group above which the
/// input is taken to hold about one row per group, the same ratio as the
/// default of `skip_partial_aggregation_probe_ratio_threshold`.
const BARELY_REDUCING: f64 = 0.8;

/// Whether a final table of `groups` groups built from `rows` rows moves into
/// buckets.
///
/// An input that repeats its groups keeps its table up to the full threshold:
/// the table reduces that input, which buckets only do by compacting.
fn starts_buckets(groups: usize, rows: usize, threshold: usize) -> bool {
    groups >= threshold
        || (groups >= (threshold / EARLY_BUCKETS_DIVISOR).max(1)
            && groups as f64 >= BARELY_REDUCING * rows as f64)
}

/// Number of flushed groups whose hashes are kept to detect recurring groups.
const FLUSH_SAMPLE_SIZE: usize = 1024;

/// Number of earlier flushes whose samples are kept, so that groups which
/// only come back after many flushes (keys that cycle with a long period) are
/// noticed as well. Costs at most `64 * 1024` remembered hashes.
const FLUSH_SAMPLES_KEPT: usize = 64;

/// Flushing stops once more than this share of the sampled groups of an
/// earlier flush shows up again in a later one.
const MAX_RECURRING_GROUPS: f64 = 0.2;

/// Seed for the hashes of sampled groups, only compared with each other.
const FLUSH_SAMPLE_SEED: RandomState = RandomState::with_seed(8122871950429871369);

/// Keeps the table of a partial hash aggregation small: once it holds
/// `hash_aggregate_bucket_threshold` groups, its state is emitted downstream
/// and the table starts over, the same way it does under memory pressure.
///
/// A table that has outgrown the CPU caches pays a cache miss for every probe
/// and every accumulator update, so capping it keeps the partial aggregation
/// fast; the final aggregation merges whatever is emitted more than once.
///
/// That only pays while flushing is free, that is while a flushed group does
/// not come back: a group that returns is emitted again, and the reduction
/// the partial stage exists for is lost. Each flush therefore remembers a
/// sample of its groups and the following flushes count how many of them they
/// hold again. Sorted, clustered or mostly unique keys never recur and keep being
/// flushed; keys that recur turn flushing off for the rest of the stream,
/// which then grows one table as before.
struct PartialTableFlush {
    threshold: usize,
    /// Number of leading columns of the state batch that are the group keys
    num_group_columns: usize,
    /// Set once flushed groups were seen to recur
    disabled: bool,
    /// Hashes of a sample of the groups emitted by the last flushes, with the
    /// number of the flush that emitted them
    sampled_groups: HashMap<u64, usize>,
    /// `(flush number, sample size)` of the flushes in `sampled_groups`
    sampled_flushes: VecDeque<(usize, usize)>,
    /// Number of flushes so far
    num_flushes: usize,
    /// Groups emitted so far, which the skip aggregation probe must still count
    flushed_groups: usize,
    flush_count: metrics::Count,
    hashes: Vec<u64>,
}

impl PartialTableFlush {
    fn should_flush(&self, num_groups: usize) -> bool {
        !self.disabled && num_groups >= self.threshold
    }

    /// Records the flush of `state`, the emitted groups and their states.
    fn record_flush(&mut self, state: &RecordBatch) -> Result<()> {
        let num_groups = state.num_rows();
        self.flush_count.add(1);
        self.flushed_groups += num_groups;

        self.hashes.clear();
        self.hashes.resize(num_groups, 0);
        create_hashes(
            &state.columns()[..self.num_group_columns],
            &FLUSH_SAMPLE_SEED,
            &mut self.hashes,
        )?;

        // How many sampled groups of each earlier flush are in this one?
        let oldest = self.sampled_flushes.front().map_or(0, |(flush, _)| *flush);
        let mut recurring = vec![0usize; self.sampled_flushes.len()];
        for hash in &self.hashes {
            if let Some(flush) = self.sampled_groups.get(hash) {
                recurring[flush - oldest] += 1;
            }
        }
        let groups_recur = recurring.iter().zip(&self.sampled_flushes).any(
            |(recurring, (_, sample_size))| {
                *recurring as f64 > MAX_RECURRING_GROUPS * *sample_size as f64
            },
        );
        if groups_recur {
            self.disabled = true;
            self.sampled_groups = HashMap::new();
            self.sampled_flushes = VecDeque::new();
            self.hashes = vec![];
            return Ok(());
        }

        if self.sampled_flushes.len() == FLUSH_SAMPLES_KEPT
            && let Some((evicted, _)) = self.sampled_flushes.pop_front()
        {
            self.sampled_groups.retain(|_, flush| *flush != evicted);
        }
        let step = num_groups.div_ceil(FLUSH_SAMPLE_SIZE).max(1);
        let mut sample_size = 0;
        for hash in self.hashes.iter().step_by(step) {
            self.sampled_groups.insert(*hash, self.num_flushes);
            sample_size += 1;
        }
        self.sampled_flushes
            .push_back((self.num_flushes, sample_size));
        self.num_flushes += 1;
        Ok(())
    }
}

impl PartialHashAggregateStream {
    pub fn new(
        agg: &AggregateExec,
        context: &Arc<TaskContext>,
        partition: usize,
    ) -> Result<Self> {
        debug_assert_eq!(agg.mode, super::AggregateMode::Partial);
        debug_assert_eq!(agg.input_order_mode, InputOrderMode::Linear);

        let schema = Arc::clone(&agg.schema);
        let input = agg.input.execute(partition, Arc::clone(context))?;
        let batch_size = context.session_config().batch_size();
        let baseline_metrics = BaselineMetrics::new(&agg.metrics, partition);

        // Preserve the existing aggregate metric surface for this plan node.
        let _spill_metrics = SpillMetrics::new(&agg.metrics, partition);
        let reduction_factor = MetricBuilder::new(&agg.metrics)
            .with_type(metrics::MetricType::Summary)
            .ratio_metrics("reduction_factor", partition);
        let early_emit_count =
            MetricBuilder::new(&agg.metrics).counter("early_emit_count", partition);

        let group_values_soft_limit = agg.limit_options().map(|config| config.limit());
        let bucket_threshold = context
            .session_config()
            .options()
            .execution
            .hash_aggregate_bucket_threshold;
        let num_group_columns = agg.group_by().num_group_exprs();
        // Same conditions as for bucketing in the final aggregation, which
        // receives what is flushed here: see `FinalHashAggregateStream::new`.
        let has_nested_state = schema
            .fields()
            .iter()
            .skip(num_group_columns)
            .any(|field| field.data_type().is_nested());
        let table_flush = (bucket_threshold > 0
            && group_values_soft_limit.is_none()
            && !has_nested_state)
            .then(|| PartialTableFlush {
                threshold: bucket_threshold,
                num_group_columns,
                disabled: false,
                sampled_groups: HashMap::new(),
                sampled_flushes: VecDeque::new(),
                num_flushes: 0,
                flushed_groups: 0,
                flush_count: MetricBuilder::new(&agg.metrics)
                    .counter("table_flush_count", partition),
                hashes: vec![],
            });

        let hash_table = AggregateHashTable::<PartialMarker>::new(
            agg,
            partition,
            Arc::clone(&schema),
            batch_size,
        )?;
        let skip_aggregation_probe = if agg.group_by().is_single() {
            let options = &context.session_config().options().execution;
            let probe_ratio_threshold =
                options.skip_partial_aggregation_probe_ratio_threshold;
            // A threshold >= 1.0 means the ratio (num_groups / input_rows) can
            // never exceed it, so the feature is effectively disabled.
            if probe_ratio_threshold >= 1.0 {
                None
            } else {
                let skipped_aggregation_rows = MetricBuilder::new(&agg.metrics)
                    .with_category(MetricCategory::Rows)
                    .counter("skipped_aggregation_rows", partition);
                Some(SkipAggregationProbe::new(
                    options.skip_partial_aggregation_probe_rows_threshold,
                    probe_ratio_threshold,
                    skipped_aggregation_rows,
                ))
            }
        } else {
            None
        };

        let reservation =
            MemoryConsumer::new(format!("PartialHashAggregateStream[{partition}]"))
                // We interpret 'can spill' as 'can handle memory back pressure'.
                // This value needs to be set to true for the default memory pool implementations
                // to ensure fair application of back pressure amongst the memory consumers.
                .with_can_spill(true)
                .register(context.memory_pool());

        Ok(Self {
            schema,
            input,
            batch_size,
            baseline_metrics,
            reservation,
            reduction_factor,
            early_emit_count,
            table_flush,
            skip_aggregation_probe,
            group_values_soft_limit,
            hash_table: Some(hash_table),
        })
    }

    pub(crate) fn into_stream(self) -> SendableRecordBatchStream {
        let schema = Arc::clone(&self.schema);

        Box::pin(RecordBatchStreamAdapter::new(schema, self.create_stream()))
    }

    /// Entry point for the partial hash aggregate state machine.
    ///
    /// See comments in [`PartialHashAggregateStream`] for high-level ideas.
    fn create_stream(mut self) -> impl Stream<Item = Result<RecordBatch>> {
        async_try_stream(|mut emitter| async move {
            let mut hash_table = self
                .hash_table
                .take()
                .expect("hash_table should not be None");

            debug_assert!(hash_table.is_building());
            let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();

            let mut last_state = HandleInputResult::ProcessNext;
            while let Some(batch) = self.input.next().await.transpose()? {
                let timer = elapsed_compute.timer();
                last_state = self.handle_input_batch(&batch, &mut hash_table)?;

                match last_state {
                    HandleInputResult::ProcessNext => {}
                    HandleInputResult::ReachedLimit
                    | HandleInputResult::SwitchToSkipAggregation => {
                        break;
                    }
                    HandleInputResult::OOM | HandleInputResult::TableFull => {
                        let materialized_group_states = hash_table.take_state_batch()?.ok_or_else(|| {
                            internal_datafusion_err!(
                                "Partial hash aggregate ran out of memory with no aggregated groups"
                            )
                        })?;

                        match (&last_state, self.table_flush.as_mut()) {
                            (HandleInputResult::TableFull, Some(table_flush)) => {
                                table_flush.record_flush(&materialized_group_states)?
                            }
                            _ => self.early_emit_count.add(1),
                        }
                        timer.done();
                        self.emit_on_memory_pressure(
                            materialized_group_states,
                            &mut emitter,
                            hash_table.memory_size(),
                        )
                        .await?;
                    }
                }
            }

            let timer = elapsed_compute.timer();

            let skip_hash_table =
                if last_state == HandleInputResult::SwitchToSkipAggregation {
                    Some(hash_table.partial_skip_table()?)
                } else {
                    self.close_input();

                    None
                };
            hash_table.start_output()?;

            timer.done();

            self.produce_output(hash_table, &mut emitter).await?;

            if let Some(hash_table) = skip_hash_table {
                self.skip_rest_of_aggregation(hash_table, emitter).await?;
            }

            Ok(())
        })
    }

    /// See comments in [`Self::group_values_soft_limit`] for details.
    fn hit_soft_group_limit(
        &self,
        hash_table: &AggregateHashTable<PartialMarker>,
    ) -> bool {
        self.group_values_soft_limit
            .is_some_and(|limit| limit <= hash_table.building_group_count())
    }

    /// Updates skip aggregation probe state.
    fn update_skip_aggregation_probe(&mut self, input_rows: usize, num_groups: usize) {
        if let Some(probe) = self.skip_aggregation_probe.as_mut() {
            probe.update_state(input_rows, num_groups);
        }
    }

    /// Returns true if the aggregation probe indicates that aggregation
    /// should be skipped.
    fn should_skip_aggregation(&self) -> bool {
        self.skip_aggregation_probe
            .as_ref()
            .is_some_and(|probe| probe.should_skip())
    }

    fn close_input(&mut self) {
        let input_schema = self.input.schema();
        self.input = Box::pin(EmptyRecordBatchStream::new(input_schema));
    }

    /// Aggregate input batch into the hash table
    fn handle_input_batch(
        &mut self,
        batch: &RecordBatch,
        hash_table: &mut AggregateHashTable<PartialMarker>,
    ) -> Result<HandleInputResult> {
        // ----------------------------------
        // Step 1: Aggregate the input batch
        // ----------------------------------
        let input_rows = batch.num_rows();
        self.reduction_factor.add_total(input_rows);
        hash_table.aggregate_batch(batch)?;

        // --------------------------------
        // Step 2: Soft limit optimization
        // --------------------------------
        if self.hit_soft_group_limit(hash_table) {
            return Ok(HandleInputResult::ReachedLimit);
        }

        // ----------------------------------------------
        // Step 3: Skip partial aggregation optimization
        // ----------------------------------------------
        let flushed_groups = self
            .table_flush
            .as_ref()
            .map_or(0, |table_flush| table_flush.flushed_groups);
        self.update_skip_aggregation_probe(
            input_rows,
            flushed_groups + hash_table.building_group_count(),
        );

        // True branch: a decision has been made to skip partial aggregation.
        if self.should_skip_aggregation() {
            return Ok(HandleInputResult::SwitchToSkipAggregation);
        }

        // -------------------------------------------------
        // Step 4: Larger-than-memory execution (early emit)
        // -------------------------------------------------
        let resize_result = self.reservation.try_resize(hash_table.memory_size());
        match resize_result {
            Ok(()) => {}
            Err(DataFusionError::ResourcesExhausted(_)) => {
                return Ok(HandleInputResult::OOM);
            }
            Err(e) => return Err(e),
        }

        // -----------------------------------------------------------
        // Step 5: Keep the table small while that is free (see
        // `PartialTableFlush`)
        // -----------------------------------------------------------
        let table_full = self.table_flush.as_ref().is_some_and(|table_flush| {
            table_flush.should_flush(hash_table.building_group_count())
        });
        if table_full {
            return Ok(HandleInputResult::TableFull);
        }
        Ok(HandleInputResult::ProcessNext)
    }

    /// emit a materialized partial-state on memory pressure
    /// batch in `batch_size`(from configuration) slices
    async fn emit_on_memory_pressure(
        &mut self,
        // After each incremental emitting step, the `remaining_groups` will be updated
        // with batch slicing.
        mut remaining_groups: RecordBatch,
        emitter: &mut TryEmitter<RecordBatch, DataFusionError>,
        hash_table_mem_size: usize,
    ) -> Result<()> {
        let remaining_groups_memory = remaining_groups.get_array_memory_size();

        // Emitting clears the aggregate table and releases its
        // accumulated memory. Update the reservation accordingly.
        // We account here for the remaining groups memory to see if we can return batch size states
        // if there is not enough memory, fallback to emit large batch
        match self
            .reservation
            .try_resize(hash_table_mem_size + remaining_groups_memory)
        {
            Ok(_) => {
                // Continue with slicing
            }
            Err(DataFusionError::ResourcesExhausted(_)) => {
                // Fail to reserve memory for the hash table + state batch while slicing so emit a huge batch

                // Try resize without holding the state batch, if it fails there is nothing we can do
                self.reservation.try_resize(hash_table_mem_size)?;

                self.reduction_factor.add_part(remaining_groups.num_rows());
                emitter
                    .emit(remaining_groups.record_output(&self.baseline_metrics))
                    .await;

                return Ok(());
            }
            Err(e) => return Err(e),
        }

        while remaining_groups.num_rows() > self.batch_size {
            // More batch to output, continue in the current state.
            let output = remaining_groups.slice(0, self.batch_size);

            remaining_groups = remaining_groups.slice(
                self.batch_size,
                remaining_groups.num_rows() - self.batch_size,
            );

            self.reduction_factor.add_part(output.num_rows());
            debug_assert!(output.num_rows() > 0);

            emitter
                .emit(output.record_output(&self.baseline_metrics))
                .await;
        }

        self.reduction_factor.add_part(remaining_groups.num_rows());
        debug_assert!(remaining_groups.num_rows() > 0);

        // We are no longer holding on the batch while slicing, so release the memory.
        // The memory will now equal to the hash table size
        self.reservation.try_shrink(remaining_groups_memory)?;

        emitter
            .emit(remaining_groups.record_output(&self.baseline_metrics))
            .await;

        Ok(())
    }

    /// emit partial aggregate state batches.
    async fn produce_output(
        &mut self,
        mut hash_table: AggregateHashTable<PartialMarker>,
        emitter: &mut TryEmitter<RecordBatch, DataFusionError>,
    ) -> Result<()> {
        debug_assert!(!hash_table.is_building());

        let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();
        let mut timer = elapsed_compute.timer();

        loop {
            let Some(batch) = hash_table.next_output_batch()? else {
                // Only reachable when the table held no groups at all: a
                // non-empty table always reports its last batch together with
                // the `Done` state, which the `try_resize` below already zeroes.
                self.reservation.try_resize(0)?;
                return Ok(());
            };

            debug_assert!(batch.num_rows() > 0);

            // The table hands over its groups as they are materialized and
            // reports a size of 0 once it reaches `Done`, so this releases the
            // reservation before the final batch goes downstream.
            let _ = self.reservation.try_resize(hash_table.memory_size());
            self.reduction_factor.add_part(batch.num_rows());

            timer.done();
            emitter
                .emit(batch.record_output(&self.baseline_metrics))
                .await;
            timer = elapsed_compute.timer();
        }
    }

    /// convert raw input directly to partial states.
    async fn skip_rest_of_aggregation(
        &mut self,
        mut hash_table: AggregateHashTable<PartialSkipMarker>,
        mut emitter: TryEmitter<RecordBatch, DataFusionError>,
    ) -> Result<()> {
        let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();

        while let Some(batch) = self.input.next().await.transpose()? {
            if let Some(probe) = self.skip_aggregation_probe.as_mut() {
                probe.record_skipped(&batch);
            }

            let result = {
                let _timer = elapsed_compute.timer();
                hash_table.convert_batch_to_state(&batch)?
            };

            emitter
                .emit(result.record_output(&self.baseline_metrics))
                .await;
        }

        self.close_input();

        Ok(())
    }
}

impl FinalHashAggregateStream {
    pub fn new(
        agg: &AggregateExec,
        context: &Arc<TaskContext>,
        partition: usize,
    ) -> Result<Self> {
        debug_assert!(matches!(
            agg.mode,
            super::AggregateMode::Final | super::AggregateMode::FinalPartitioned
        ));
        debug_assert_eq!(agg.input_order_mode, InputOrderMode::Linear);

        let input = agg.input.execute(partition, Arc::clone(context))?;
        Self::new_with_input(agg, context, partition, input)
    }

    /// Builds the stream over `input` instead of executing the plan's input.
    pub(in crate::aggregates) fn new_with_input(
        agg: &AggregateExec,
        context: &Arc<TaskContext>,
        partition: usize,
        input: SendableRecordBatchStream,
    ) -> Result<Self> {
        let schema = Arc::clone(&agg.schema);
        let input_schema = input.schema();
        let batch_size = context.session_config().batch_size();
        let baseline_metrics = BaselineMetrics::new(&agg.metrics, partition);
        let spill_metrics = SpillMetrics::new(&agg.metrics, partition);

        let hash_table = AggregateHashTable::<FinalMarker>::new(
            agg,
            partition,
            Arc::clone(&schema),
            batch_size,
        )?;

        let can_spill = context.runtime_env().disk_manager.tmp_files_enabled();
        let spill_context = if can_spill {
            Some(Box::new(AggregateSpill::try_new(
                "FinalHashAggregateSpill",
                agg,
                context,
                partition,
                batch_size,
                &InputOrderMode::Linear,
                &input_schema,
                spill_metrics,
            )?))
        } else {
            None
        };

        let reservation =
            MemoryConsumer::new(format!("FinalHashAggregateStream[{partition}]"))
                .with_can_spill(can_spill)
                .register(context.memory_pool());

        let group_values_soft_limit = agg.limit_options().map(|config| config.limit());

        let bucket_threshold = context
            .session_config()
            .options()
            .execution
            .hash_aggregate_bucket_threshold;
        // A soft limit stops reading input early, which bucketing cannot do.
        let bucketing = (bucket_threshold > 0
            && group_values_soft_limit.is_none()
            && BucketedAggregation::supports_state(
                &input_schema,
                agg.group_by().num_group_exprs(),
            ))
        .then(|| {
            Arc::new(BucketedAggregation::new(
                bucket_threshold,
                agg.clone(),
                partition,
                batch_size,
                Arc::clone(&input_schema),
                Arc::clone(&schema),
                spill_context
                    .as_ref()
                    .map(|context| context.spill_manager().clone()),
            ))
        });

        Ok(Self {
            schema,
            input,
            baseline_metrics,
            reservation,
            group_values_soft_limit,
            hash_table: Some(hash_table),
            spill_context,
            bucketing,
        })
    }

    pub(crate) fn into_stream(self) -> SendableRecordBatchStream {
        let schema = Arc::clone(&self.schema);

        Box::pin(RecordBatchStreamAdapter::new(schema, self.create_stream()))
    }

    /// Entry point for the final hash aggregate flow
    ///
    /// See comments in [`FinalHashAggregateStream`] for high-level ideas.
    fn create_stream(mut self) -> impl Stream<Item = Result<RecordBatch>> {
        async_try_stream(|emitter| async move {
            let mut hash_table = self
                .hash_table
                .take()
                .expect("hash_table should not be None");

            let mut spill_context = self.spill_context.take();

            let buckets = self
                .consume_input(&mut hash_table, &mut spill_context)
                .await?;
            self.close_input();

            if let (Some(buckets), Some(bucketing)) = (buckets, self.bucketing.clone()) {
                // The table handed its groups over to the buckets, whose
                // memory this stream's reservation already covers.
                drop(hash_table);
                let empty = self.reservation.new_empty();
                let bucket_reservation = std::mem::replace(&mut self.reservation, empty);
                let mut emitter = emitter;
                let mut output = bucketing.output_stream(
                    buckets,
                    bucket_reservation,
                    self.baseline_metrics.clone(),
                );
                while let Some(batch) = output.next().await.transpose()? {
                    emitter.emit(batch).await;
                }
                return Ok(());
            }

            match spill_context.filter(|s| s.has_spills()) {
                // - If spilled before, perform merging spill runs
                Some(spill_context) => {
                    self.produce_output_from_spills(hash_table, spill_context, emitter)
                        .await?
                }
                // Either all the input fit in memory or hit soft group limit with no spilling
                None => self.produce_output_from_memory(hash_table, emitter).await?,
            }

            Ok(())
        })
    }

    fn close_input(&mut self) {
        let input_schema = self.input.schema();
        self.input = Box::pin(EmptyRecordBatchStream::new(input_schema));
    }

    /// See comments in [`Self::group_values_soft_limit`] for details.
    fn hit_soft_group_limit(&self, hash_table: &AggregateHashTable<FinalMarker>) -> bool {
        self.group_values_soft_limit
            .is_some_and(|limit| limit <= hash_table.building_group_count())
    }

    /// Reserve memory for the current aggregate table.
    fn reservation_size_for_table(
        hash_table: &AggregateHashTable<FinalMarker>,
        spill_context: Option<&AggregateSpill>,
    ) -> usize {
        let table_size = hash_table.memory_size();
        if spill_context.is_some() {
            // Count extra space needed for in-memory sorting and spilling. Only
            // count memory for indices, the payload will be materialize incrementally
            // in smaller chunks.
            table_size.saturating_add(
                hash_table
                    .building_group_count()
                    .saturating_mul(size_of::<u32>()),
            )
        } else {
            table_size
        }
    }

    /// Read input stream, if no memory, then spill and continue reading - aggregate partial state batches into the hash table.
    ///
    /// Spilling: The table cannot reserve enough memory.
    ///           Move all current states into one fully group-key-sorted spill run.
    ///
    /// Bucketing: The table has reached `hash_aggregate_bucket_threshold` groups.
    ///            Move all current states and the rest of the input into hash
    ///            buckets, which are returned for [`Self::produce_output_from_buckets`].
    async fn consume_input(
        &mut self,
        hash_table: &mut AggregateHashTable<FinalMarker>,
        spill_context: &mut Option<Box<AggregateSpill>>,
    ) -> Result<Option<FinalBuckets>> {
        let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();
        let mut buckets: Option<FinalBuckets> = None;
        let mut compaction_table = None;
        // Rows aggregated by `hash_table`
        let mut table_rows = 0usize;

        while let Some(batch) = self.input.next().await.transpose()? {
            let _timer = elapsed_compute.timer();

            if let (Some(buckets), Some(bucketing)) =
                (buckets.as_mut(), self.bucketing.as_ref())
            {
                buckets.route(&batch)?;
                bucketing.compact(buckets, &mut compaction_table)?;
                bucketing.reserve(&self.reservation, 0, buckets)?;
                continue;
            }

            hash_table.aggregate_batch(&batch)?;
            table_rows += batch.num_rows();

            // Soft group limits are usually small and rarely coincide with
            // spilling. Once spilling has occurred, skip this optimization to
            // make the internal logic simpler.
            let spilled = spill_context
                .as_ref()
                .is_some_and(|context| context.has_spills());
            if self.hit_soft_group_limit(hash_table) && !spilled {
                break;
            }

            // Once sorted runs exist the output comes from merging them, so
            // bucketing only starts from a table that has never spilled.
            if let Some(bucketing) = &self.bucketing
                && !spilled
                && starts_buckets(
                    hash_table.building_group_count(),
                    table_rows,
                    bucketing.threshold(),
                )
            {
                let kept =
                    hash_table.building_group_count() as f64 / table_rows.max(1) as f64;
                let mut new_buckets =
                    bucketing.split(0, hash_table.take_state_batch()?, kept)?;
                bucketing.reserve(
                    &self.reservation,
                    hash_table.memory_size(),
                    &mut new_buckets,
                )?;
                buckets = Some(new_buckets);
                continue;
            }

            // Check memory reservation, and potentially spill.
            let resize_result =
                self.reservation
                    .try_resize(Self::reservation_size_for_table(
                        hash_table,
                        spill_context.as_deref(),
                    ));

            match resize_result {
                Ok(()) => {}

                // The table cannot reserve enough memory.
                // Move all current states into one fully group-key-sorted spill run.
                Err(e @ DataFusionError::ResourcesExhausted(_)) => {
                    // OOM and don't support spilling from configuration
                    let spill_context = spill_context.as_mut().ok_or_else(|| e.context(
                        "Final hash aggregate cannot spill because temporary files are not enabled in the DiskManager",
                    ))?;

                    // Sanity check: impossible to OOM when there is no group aggregated.
                    assert_ne_or_internal_err!(
                        hash_table.building_group_count(),
                        0,
                        "Final hash aggregate ran out of memory with no aggregated groups"
                    );

                    // Sorts and spills one complete in-memory state run

                    // Go to the next state to perform spilling the aggregated
                    // groups so far.
                    let result = hash_table
                        .take_state_batch()
                        .and_then(|batch| spill_context.sort_and_spill(batch));

                    // Spilling shrinks the aggregate table and releases its accumulated
                    // memory. Update the reservation accordingly.
                    self.reservation
                        .try_resize(hash_table.memory_size())
                        .map_err(|e| {
                            e.context(
                                "Decreasing allocation after spilling should succeed",
                            )
                        })?;

                    result?;

                    // One sorted run was written; resume reading the original input.
                }
                Err(e) => return Err(e),
            }
        }

        Ok(buckets)
    }

    /// Produce output from spills
    /// 1. Spill in progress in-memory hash table
    /// 2. Switch to ordered final stream
    /// 3. passthrough stream output
    async fn produce_output_from_spills(
        &mut self,
        mut hash_table: AggregateHashTable<FinalMarker>,
        mut spill_context: Box<AggregateSpill>,
        mut emitter: TryEmitter<RecordBatch, DataFusionError>,
    ) -> Result<()> {
        let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();
        let timer = elapsed_compute.timer();

        // Input was exhausted after spilling. Spill the last in-memory run
        hash_table
            .take_state_batch()
            .and_then(|batch| spill_context.sort_and_spill(batch))?;

        // Construct the ordered input used to merge all spill files.
        let mut output_stream =
            self.switch_to_ordered_final_stream(hash_table, spill_context)?;

        timer.done();

        // Forwards output from the fully ordered stream that consumes the merged
        // spill runs.
        //
        // Not wrapping in a timer and not record output batches since this is now `merge_stream` responsibility
        // we just pass through
        while let Some(batch) = output_stream.next().await.transpose()? {
            emitter.emit(batch).await;
        }

        Ok(())
    }

    /// 1. Constructs a globally ordered input stream by applying a sort-preserving
    ///    merge to all spills.
    /// 2. Constructs a replay stream: an ordered final aggregate stream over the
    ///    fully ordered input constructed from the spills.
    ///
    /// Returns the replay stream
    fn switch_to_ordered_final_stream(
        &mut self,
        hash_table: AggregateHashTable<FinalMarker>,
        spill_context: Box<AggregateSpill>,
    ) -> Result<SendableRecordBatchStream> {
        let metrics = OrderedAggregateTableMetrics::from_hash_table(&hash_table);
        drop(hash_table);
        self.reservation.try_resize(0)?;
        spill_context.into_replay_stream(
            &self.baseline_metrics,
            metrics,
            self.reservation.new_empty(),
        )
    }

    /// Emit final aggregate value batches:
    /// Input was exhausted without spilling, or the soft group limit was reached.
    async fn produce_output_from_memory(
        &mut self,
        mut hash_table: AggregateHashTable<FinalMarker>,
        mut emitter: TryEmitter<RecordBatch, DataFusionError>,
    ) -> Result<()> {
        let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();

        let mut timer = elapsed_compute.timer();
        hash_table.start_output()?;

        loop {
            let Some(batch) = hash_table.next_output_batch()? else {
                // Only reachable when the table held no groups at all: a
                // non-empty table always reports its last batch together with
                // the `Done` state, which the `try_resize` below already zeroes.
                self.reservation.try_resize(0)?;
                return Ok(());
            };

            // The table hands over its groups as they are materialized and
            // reports a size of 0 once it reaches `Done`, so this releases the
            // reservation before the final batch goes downstream.
            self.reservation.try_resize(hash_table.memory_size())?;

            timer.done();
            emitter
                .emit(batch.record_output(&self.baseline_metrics))
                .await;
            timer = elapsed_compute.timer();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use super::*;
    use crate::aggregates::{AggregateMode, PhysicalGroupBy};
    use crate::common::collect;
    use crate::execution_plan::ExecutionPlan;
    use crate::test::TestMemoryExec;
    use crate::test::exec::BarrierExec;

    use arrow::array::{AsArray, Int32Array, Int64Array, StringViewArray};
    use arrow::datatypes::{DataType, Field, Int32Type, Schema};
    use datafusion_common::Result;
    use datafusion_execution::config::SessionConfig;
    use datafusion_execution::memory_pool::{GreedyMemoryPool, MemoryPool};
    use datafusion_execution::runtime_env::RuntimeEnvBuilder;
    use datafusion_functions_aggregate::count::count_udaf;
    use datafusion_functions_aggregate::{min_max::min_udaf, sum::sum_udaf};
    use datafusion_physical_expr::aggregate::AggregateExprBuilder;
    use datafusion_physical_expr::expressions::col;
    use futures::channel::mpsc;
    use futures::{FutureExt, StreamExt};
    use std::collections::BTreeMap;

    #[tokio::test]
    async fn test_partial_hash_stream_double_emission_race_condition_bug() -> Result<()> {
        // Fix for https://github.com/apache/datafusion/issues/18701
        // This test specifically proves that we have fixed double emission race condition
        // where emit_early_if_necessary() and switch_to_skip_aggregation()
        // both emit in the same loop iteration, causing data loss

        let schema = Arc::new(Schema::new(vec![
            Field::new("group_col", DataType::Int32, false),
            Field::new("value_col", DataType::Int64, false),
        ]));

        // Create data that will trigger BOTH conditions in the same iteration:
        // 1. More groups than batch_size (triggers early emission when memory pressure hits)
        // 2. High cardinality ratio (triggers skip aggregation)
        let batch_size = 1024; // We'll set this in session config
        let num_groups = batch_size + 100; // Slightly more than batch_size (1124 groups)

        // Create exactly 1 row per group = 100% cardinality ratio
        let group_ids: Vec<i32> = (0..num_groups as i32).collect();
        let values: Vec<i64> = vec![1; num_groups];

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(group_ids)),
                Arc::new(Int64Array::from(values)),
            ],
        )?;
        let input_partitions = vec![vec![batch]];

        // Create constrained memory to trigger early emission but not completely fail
        let runtime = RuntimeEnvBuilder::default()
            .with_memory_limit(1024, 1.0) // small enough to start but will trigger pressure
            .build_arc()?;

        let mut task_ctx = TaskContext::default().with_runtime(runtime);

        // Configure to trigger BOTH conditions:
        // 1. Low probe threshold (triggers skip probe after few rows)
        // 2. Low ratio threshold (triggers skip aggregation immediately)
        // 3. Set batch_size to 1024 so our 1124 groups will trigger early emission
        // This creates the race condition where both emit paths are triggered
        let mut session_config = task_ctx.session_config().clone();
        session_config = session_config.set(
            "datafusion.execution.batch_size",
            &datafusion_common::ScalarValue::UInt64(Some(1024)),
        );
        session_config = session_config.set(
            "datafusion.execution.skip_partial_aggregation_probe_rows_threshold",
            &datafusion_common::ScalarValue::UInt64(Some(50)),
        );
        session_config = session_config.set(
            "datafusion.execution.skip_partial_aggregation_probe_ratio_threshold",
            &datafusion_common::ScalarValue::Float64(Some(0.8)),
        );
        task_ctx = task_ctx.with_session_config(session_config);
        let task_ctx = Arc::new(task_ctx);

        // Create aggregate: COUNT(*) GROUP BY group_col
        let group_expr = vec![(col("group_col", &schema)?, "group_col".to_string())];
        let aggr_expr = vec![Arc::new(
            AggregateExprBuilder::new(count_udaf(), vec![col("value_col", &schema)?])
                .schema(Arc::clone(&schema))
                .alias("count_value")
                .build()?,
        )];

        let exec = TestMemoryExec::try_new(&input_partitions, Arc::clone(&schema), None)?;
        let exec = Arc::new(TestMemoryExec::update_cache(&Arc::new(exec)));

        // Use Partial mode where the race condition occurs
        let aggregate_exec = AggregateExec::try_new(
            AggregateMode::Partial,
            PhysicalGroupBy::new_single(group_expr),
            aggr_expr,
            vec![None],
            exec,
            Arc::clone(&schema),
        )?;

        // Execute and collect results
        let mut stream =
            PartialHashAggregateStream::new(&aggregate_exec, &Arc::clone(&task_ctx), 0)?
                .into_stream();
        let mut results = Vec::new();

        while let Some(result) = stream.next().await {
            let batch = result?;
            results.push(batch);
        }

        // Count total groups emitted
        let mut total_output_groups = 0;
        for batch in &results {
            total_output_groups += batch.num_rows();
        }

        assert_eq!(
            total_output_groups, num_groups,
            "Unexpected number of groups",
        );
        assert_eq!(
            aggregate_exec
                .metrics()
                .unwrap()
                .sum_by_name("early_emit_count")
                .unwrap()
                .as_usize(),
            0
        );

        // Disable skip aggregation so the same input is emitted on memory pressure.
        let runtime = RuntimeEnvBuilder::default()
            .with_memory_limit(1024, 1.0)
            .build_arc()?;
        let session_config = task_ctx.session_config().clone().set(
            "datafusion.execution.skip_partial_aggregation_probe_ratio_threshold",
            &datafusion_common::ScalarValue::Float64(Some(2.0)),
        );
        let no_skip_task_ctx = Arc::new(
            TaskContext::default()
                .with_runtime(runtime)
                .with_session_config(session_config),
        );
        let mut stream =
            PartialHashAggregateStream::new(&aggregate_exec, &no_skip_task_ctx, 0)?
                .into_stream();
        while let Some(result) = stream.next().await {
            result?;
        }

        assert_eq!(
            aggregate_exec
                .metrics()
                .unwrap()
                .sum_by_name("early_emit_count")
                .unwrap()
                .as_usize(),
            1
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_partial_hash_stream_skip_aggregation_probe_not_locked_until_skip()
    -> Result<()> {
        // Test that the probe is not locked until we actually decide to skip.
        // This allows us to continue evaluating the skip condition across multiple batches.
        //
        // Scenario:
        // - Batch 1: Hits rows threshold but NOT ratio threshold (low cardinality) -> don't skip
        // - Batch 2: Now hits ratio threshold (high cardinality) -> skip
        //
        // Without the fix, the probe would be locked after batch 1, preventing the skip
        // decision from being made on batch 2.

        let schema = Arc::new(Schema::new(vec![
            Field::new("group_col", DataType::Int32, false),
            Field::new("value_col", DataType::Int32, false),
        ]));

        // Configure thresholds:
        // - probe_rows_threshold: 100 rows
        // - probe_ratio_threshold: 0.8 (80%)
        let probe_rows_threshold = 100;
        let probe_ratio_threshold = 0.8;

        // Batch 1: 100 rows with only 10 unique groups
        // Ratio: 10/100 = 0.1 (10%) < 0.8 -> should NOT skip
        // This will hit the rows threshold but not the ratio threshold
        let batch1_rows = 100;
        let batch1_groups = 10;
        let mut group_ids_batch1 = Vec::new();
        for i in 0..batch1_rows {
            group_ids_batch1.push((i % batch1_groups) as i32);
        }
        let values_batch1: Vec<i32> = vec![1; batch1_rows];

        let batch1 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(group_ids_batch1)),
                Arc::new(Int32Array::from(values_batch1)),
            ],
        )?;

        // Batch 2: 360 rows with 360 unique NEW groups (starting from group 10)
        // After batch 2, total: 460 rows, 370 groups
        // Ratio: 370/460 is about 0.804 (80.4%) > 0.8 -> SHOULD decide to skip
        let batch2_rows = 360;
        let batch2_groups = 360;
        let group_ids_batch2: Vec<i32> = (batch1_groups..(batch1_groups + batch2_groups))
            .map(|x| x as i32)
            .collect();
        let values_batch2: Vec<i32> = vec![1; batch2_rows];

        let batch2 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(group_ids_batch2)),
                Arc::new(Int32Array::from(values_batch2)),
            ],
        )?;

        // Batch 3: This batch should be skipped since we decided to skip after batch 2
        // 100 rows with 100 unique groups (continuing from where batch 2 left off)
        let batch3_rows = 100;
        let batch3_groups = 100;
        let batch3_start_group = batch1_groups + batch2_groups;
        let group_ids_batch3: Vec<i32> = (batch3_start_group
            ..(batch3_start_group + batch3_groups))
            .map(|x| x as i32)
            .collect();
        let values_batch3: Vec<i32> = vec![1; batch3_rows];

        let batch3 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(group_ids_batch3)),
                Arc::new(Int32Array::from(values_batch3)),
            ],
        )?;

        let input_partitions = vec![vec![batch1, batch2, batch3]];

        let runtime = RuntimeEnvBuilder::default().build_arc()?;
        let mut task_ctx = TaskContext::default().with_runtime(runtime);

        // Configure skip aggregation settings
        let mut session_config = task_ctx.session_config().clone();
        session_config = session_config.set(
            "datafusion.execution.skip_partial_aggregation_probe_rows_threshold",
            &datafusion_common::ScalarValue::UInt64(Some(probe_rows_threshold)),
        );
        session_config = session_config.set(
            "datafusion.execution.skip_partial_aggregation_probe_ratio_threshold",
            &datafusion_common::ScalarValue::Float64(Some(probe_ratio_threshold)),
        );
        task_ctx = task_ctx.with_session_config(session_config);
        let task_ctx = Arc::new(task_ctx);

        // Create aggregate: COUNT(*) GROUP BY group_col
        let group_expr = vec![(col("group_col", &schema)?, "group_col".to_string())];
        let aggr_expr = vec![Arc::new(
            AggregateExprBuilder::new(count_udaf(), vec![col("value_col", &schema)?])
                .schema(Arc::clone(&schema))
                .alias("count_value")
                .build()?,
        )];

        let exec = TestMemoryExec::try_new(&input_partitions, Arc::clone(&schema), None)?;
        let exec = Arc::new(TestMemoryExec::update_cache(&Arc::new(exec)));

        // Use Partial mode
        let aggregate_exec = AggregateExec::try_new(
            AggregateMode::Partial,
            PhysicalGroupBy::new_single(group_expr),
            aggr_expr,
            vec![None],
            exec,
            Arc::clone(&schema),
        )?;

        // Execute and collect results
        let mut stream =
            PartialHashAggregateStream::new(&aggregate_exec, &Arc::clone(&task_ctx), 0)?
                .into_stream();
        let mut results = Vec::new();

        while let Some(result) = stream.next().await {
            let batch = result?;
            results.push(batch);
        }

        // Check that skip aggregation actually happened.
        // The key metric is skipped_aggregation_rows.
        let metrics = aggregate_exec.metrics().unwrap();
        let skipped_rows = metrics
            .sum_by_name("skipped_aggregation_rows")
            .map(|m| m.as_usize())
            .unwrap_or(0);

        // We expect batch 3's rows to be skipped (100 rows)
        assert_eq!(
            skipped_rows, batch3_rows,
            "Expected batch 3's rows ({batch3_rows}) to be skipped",
        );

        Ok(())
    }

    /// Runs the final hash aggregation of `SELECT group_col, COUNT(value_col)
    /// .. GROUP BY group_col` over partial state that holds every group
    /// `num_partitions` times with a count of 1, as if that many partial
    /// aggregations had fed it. The input is not charged to the memory pool,
    /// so a memory limit only constrains the final aggregation.
    ///
    /// Returns the `(group, count)` rows sorted by group, and the final
    /// aggregation's `bucket_splits` and `spill_count` metrics.
    async fn run_final_hash_aggregate(
        num_groups: usize,
        num_partitions: usize,
        bucket_threshold: usize,
        memory_limit: Option<usize>,
    ) -> Result<(Vec<(i32, i64)>, usize, usize)> {
        use datafusion_common::ScalarValue;

        let batch_size = 1024;
        let schema = Arc::new(Schema::new(vec![
            Field::new("group_col", DataType::Int32, false),
            Field::new("value_col", DataType::Int64, false),
        ]));

        let mut runtime = RuntimeEnvBuilder::default();
        if let Some(memory_limit) = memory_limit {
            runtime = runtime.with_memory_limit(memory_limit, 1.0);
        }
        let task_ctx = TaskContext::default().with_runtime(runtime.build_arc()?);
        let session_config = task_ctx
            .session_config()
            .clone()
            .set(
                "datafusion.execution.batch_size",
                &ScalarValue::UInt64(Some(batch_size as u64)),
            )
            .set(
                "datafusion.execution.hash_aggregate_bucket_threshold",
                &ScalarValue::UInt64(Some(bucket_threshold as u64)),
            );
        let task_ctx = Arc::new(task_ctx.with_session_config(session_config));

        let group_by = PhysicalGroupBy::new_single(vec![(
            col("group_col", &schema)?,
            "group_col".to_string(),
        )]);
        let aggr_expr = vec![Arc::new(
            AggregateExprBuilder::new(count_udaf(), vec![col("value_col", &schema)?])
                .schema(Arc::clone(&schema))
                .alias("count_value")
                .build()?,
        )];

        // The partial aggregation is only built for its output schema
        let empty = TestMemoryExec::try_new_exec(&[vec![]], Arc::clone(&schema), None)?;
        let state_schema = AggregateExec::try_new(
            AggregateMode::Partial,
            group_by.clone(),
            aggr_expr.clone(),
            vec![None],
            empty,
            Arc::clone(&schema),
        )?
        .schema();
        let mut state_batches = vec![];
        for _ in 0..num_partitions {
            for start in (0..num_groups).step_by(batch_size) {
                let end = (start + batch_size).min(num_groups);
                let groups: Vec<i32> = (start as i32..end as i32).collect();
                let counts = vec![1i64; groups.len()];
                state_batches.push(RecordBatch::try_new(
                    Arc::clone(&state_schema),
                    vec![
                        Arc::new(Int32Array::from(groups)),
                        Arc::new(Int64Array::from(counts)),
                    ],
                )?);
            }
        }
        let state_input = TestMemoryExec::try_new_exec(
            &[state_batches],
            Arc::clone(&state_schema),
            None,
        )?;
        let final_agg = Arc::new(AggregateExec::try_new(
            AggregateMode::Final,
            group_by.as_final(),
            aggr_expr,
            vec![None],
            state_input,
            Arc::clone(&schema),
        )?);

        let batches =
            crate::collect(Arc::clone(&final_agg) as Arc<dyn ExecutionPlan>, task_ctx)
                .await?;
        let mut rows = vec![];
        for batch in &batches {
            let groups = batch.column(0).as_primitive::<Int32Type>();
            let counts = batch
                .column(1)
                .as_primitive::<arrow::datatypes::Int64Type>();
            rows.extend(
                groups
                    .values()
                    .iter()
                    .copied()
                    .zip(counts.values().iter().copied()),
            );
        }
        rows.sort_unstable();

        let metrics = final_agg.metrics().expect("final aggregate has metrics");
        let bucket_splits = metrics
            .sum_by_name("bucket_splits")
            .map(|value| value.as_usize())
            .unwrap_or(0);
        BUCKET_COMPACTIONS.with(|compactions| {
            compactions.set(
                metrics
                    .sum_by_name("bucket_compactions")
                    .map(|value| value.as_usize())
                    .unwrap_or(0),
            )
        });
        Ok((rows, bucket_splits, metrics.spill_count().unwrap_or(0)))
    }

    /// Runs the partial hash aggregation of `SELECT group_col, COUNT(value_col)
    /// .. GROUP BY group_col` over `keys`, and returns the count of every group
    /// summed over all the state rows emitted for it, the number of state rows,
    /// and the `table_flush_count` metric.
    async fn run_partial_hash_aggregate(
        keys: Vec<i32>,
        bucket_threshold: usize,
    ) -> Result<(BTreeMap<i32, i64>, usize, usize)> {
        use datafusion_common::ScalarValue;

        let batch_size = 1024;
        let schema = Arc::new(Schema::new(vec![
            Field::new("group_col", DataType::Int32, false),
            Field::new("value_col", DataType::Int64, false),
        ]));
        let batches = keys
            .chunks(batch_size)
            .map(|keys| {
                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![
                        Arc::new(Int32Array::from(keys.to_vec())),
                        Arc::new(Int64Array::from(vec![1i64; keys.len()])),
                    ],
                )
            })
            .collect::<Result<Vec<_>, _>>()?;

        let task_ctx = TaskContext::default();
        let session_config = task_ctx
            .session_config()
            .clone()
            .set(
                "datafusion.execution.batch_size",
                &ScalarValue::UInt64(Some(batch_size as u64)),
            )
            .set(
                "datafusion.execution.hash_aggregate_bucket_threshold",
                &ScalarValue::UInt64(Some(bucket_threshold as u64)),
            )
            // keep the skip aggregation probe out of the way
            .set(
                "datafusion.execution.skip_partial_aggregation_probe_ratio_threshold",
                &ScalarValue::Float64(Some(1.0)),
            );
        let task_ctx = Arc::new(task_ctx.with_session_config(session_config));

        let aggr_expr = vec![Arc::new(
            AggregateExprBuilder::new(count_udaf(), vec![col("value_col", &schema)?])
                .schema(Arc::clone(&schema))
                .alias("count_value")
                .build()?,
        )];
        let input = TestMemoryExec::try_new_exec(&[batches], Arc::clone(&schema), None)?;
        let partial = Arc::new(AggregateExec::try_new(
            AggregateMode::Partial,
            PhysicalGroupBy::new_single(vec![(
                col("group_col", &schema)?,
                "group_col".to_string(),
            )]),
            aggr_expr,
            vec![None],
            input,
            Arc::clone(&schema),
        )?);

        let output =
            crate::collect(Arc::clone(&partial) as Arc<dyn ExecutionPlan>, task_ctx)
                .await?;
        let mut counts = BTreeMap::new();
        let mut state_rows = 0;
        for batch in &output {
            state_rows += batch.num_rows();
            let groups = batch.column(0).as_primitive::<Int32Type>();
            let states = batch
                .column(1)
                .as_primitive::<arrow::datatypes::Int64Type>();
            for (group, count) in groups.values().iter().zip(states.values()) {
                *counts.entry(*group).or_insert(0) += count;
            }
        }
        let flushes = partial
            .metrics()
            .expect("partial aggregate has metrics")
            .sum_by_name("table_flush_count")
            .map(|value| value.as_usize())
            .unwrap_or(0);
        Ok((counts, state_rows, flushes))
    }

    #[tokio::test]
    async fn partial_hash_aggregate_flushes_groups_that_do_not_recur() -> Result<()> {
        // Clustered keys: the 3 rows of a group are adjacent
        let keys: Vec<i32> = (0..60_000).map(|row| row / 3).collect();

        let (expected, state_rows, flushes) =
            run_partial_hash_aggregate(keys.clone(), 0).await?;
        assert_eq!((state_rows, flushes), (20_000, 0));
        assert!(expected.values().all(|&count| count == 3));

        let (counts, state_rows, flushes) =
            run_partial_hash_aggregate(keys, 2_000).await?;
        assert_eq!(counts, expected);
        assert!(flushes >= 9, "the table was flushed throughout: {flushes}");
        // A group is only emitted twice when a flush falls between its rows
        assert!(state_rows <= 20_000 + flushes);
        Ok(())
    }

    #[tokio::test]
    async fn partial_hash_aggregate_stops_flushing_groups_that_recur() -> Result<()> {
        // The same 30000 keys come around five times: every flushed group
        // returns, but only 15 flushes later.
        let keys: Vec<i32> = (0..150_000).map(|row| row % 30_000).collect();
        let (expected, _, _) = run_partial_hash_aggregate(keys.clone(), 0).await?;

        let (counts, state_rows, flushes) =
            run_partial_hash_aggregate(keys, 2_000).await?;
        assert_eq!(counts, expected);
        assert!(expected.values().all(|&count| count == 5));
        // The first round is flushed; the flush that sees its groups again is the last
        assert!((15..=17).contains(&flushes), "flushing stopped: {flushes}");
        assert!(
            state_rows <= 64_000,
            "later rounds are reduced: {state_rows}"
        );
        Ok(())
    }

    thread_local! {
        /// `bucket_compactions` metric of the last [`run_final_hash_aggregate`]
        static BUCKET_COMPACTIONS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    }

    #[tokio::test]
    async fn final_hash_aggregate_compacts_repeated_groups() -> Result<()> {
        // Every group arrives 40 times, so buffering the input as is would
        // hold 40 rows per group.
        let (rows, splits, spills) =
            run_final_hash_aggregate(20_000, 40, 1_000, None).await?;
        assert_eq!(rows.len(), 20_000);
        assert!(rows.iter().all(|&(_, count)| count == 40));
        assert!(splits >= 1);
        assert_eq!(spills, 0);
        let compactions = BUCKET_COMPACTIONS.with(|compactions| compactions.get());
        assert!(compactions > 0, "buckets were compacted");
        Ok(())
    }

    #[tokio::test]
    async fn final_hash_aggregate_buckets_match_single_table() -> Result<()> {
        let (expected, splits, _) = run_final_hash_aggregate(50_000, 3, 0, None).await?;
        assert_eq!(splits, 0);
        assert_eq!(expected.len(), 50_000);
        assert!(expected.iter().all(|&(_, count)| count == 3));

        // One split: 64 buckets of ~780 groups stay below the threshold
        let (rows, splits, spills) =
            run_final_hash_aggregate(50_000, 3, 10_000, None).await?;
        assert_eq!(rows, expected);
        assert_eq!(splits, 1);
        assert_eq!(spills, 0);

        // A table that does not reach a quarter of the threshold is left alone
        let (rows, splits, _) =
            run_final_hash_aggregate(50_000, 3, 200_004, None).await?;
        assert_eq!(rows, expected);
        assert_eq!(splits, 0);
        Ok(())
    }

    #[test]
    fn starts_buckets_early_only_when_groups_do_not_repeat() {
        // One row per group: a quarter of the threshold is enough
        assert!(!starts_buckets(249, 249, 1_000));
        assert!(starts_buckets(250, 250, 1_000));
        // Groups repeat: only the full threshold
        assert!(!starts_buckets(999, 10_000, 1_000));
        assert!(starts_buckets(1_000, 10_000, 1_000));
        // A threshold below the divisor
        assert!(starts_buckets(1, 1, 2));
    }

    #[tokio::test]
    async fn final_hash_aggregate_starts_buckets_early_for_unique_groups() -> Result<()> {
        let (expected, _, _) = run_final_hash_aggregate(50_000, 1, 0, None).await?;

        // Every group arrives once, so the table moves into buckets at a
        // quarter of the threshold, which the 50,000 groups never reach.
        // Buckets of ~780 groups are not split again.
        let (rows, splits, _) = run_final_hash_aggregate(50_000, 1, 60_000, None).await?;
        assert_eq!(rows, expected);
        assert_eq!(splits, 1);
        Ok(())
    }

    #[tokio::test]
    async fn final_hash_aggregate_splits_large_buckets_again() -> Result<()> {
        let (expected, _, _) = run_final_hash_aggregate(50_000, 2, 0, None).await?;

        // Buckets of ~780 groups exceed the threshold and are split once more
        let (rows, splits, _) = run_final_hash_aggregate(50_000, 2, 100, None).await?;
        assert_eq!(rows, expected);
        assert!(splits > 1, "buckets were split again, got {splits} splits");
        Ok(())
    }

    #[tokio::test]
    async fn final_hash_aggregate_spills_buckets_under_memory_limit() -> Result<()> {
        let (expected, _, _) = run_final_hash_aggregate(200_000, 3, 0, None).await?;

        let (rows, splits, spills) =
            run_final_hash_aggregate(200_000, 3, 10_000, Some(3 * 1024 * 1024)).await?;
        assert_eq!(rows, expected);
        assert!(splits >= 1);
        assert!(spills > 0, "buckets were spilled");
        Ok(())
    }

    /// Builds a partial hash aggregate stream over a single input batch of
    /// `num_groups` distinct groups, running under `memory_limit` bytes.
    ///
    /// The input does not signal end-of-stream until `wait_finish` is called
    /// on the returned [`BarrierExec`], so any output produced before that can
    /// only come from the memory pressure emission path (normal output waits
    /// for all input). Skip partial aggregation is disabled for the same reason.
    fn partial_stream_under_memory_limit(
        memory_limit: usize,
        batch_size: usize,
        num_groups: usize,
    ) -> Result<(
        SendableRecordBatchStream,
        Arc<BarrierExec>,
        Arc<datafusion_execution::runtime_env::RuntimeEnv>,
    )> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("group_col", DataType::Int32, false),
            Field::new("value_col", DataType::Int64, false),
        ]));

        let group_ids: Vec<i32> = (0..num_groups as i32).collect();
        let values: Vec<i64> = vec![1; num_groups];

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(group_ids)),
                Arc::new(Int64Array::from(values)),
            ],
        )?;
        let input_partitions = vec![vec![batch]];

        let runtime = RuntimeEnvBuilder::default()
            .with_memory_limit(memory_limit, 1.0)
            .build_arc()?;

        let mut task_ctx = TaskContext::default().with_runtime(Arc::clone(&runtime));
        let session_config = task_ctx
            .session_config()
            .clone()
            .set(
                "datafusion.execution.batch_size",
                &datafusion_common::ScalarValue::UInt64(Some(batch_size as u64)),
            )
            .set(
                "datafusion.execution.skip_partial_aggregation_probe_ratio_threshold",
                &datafusion_common::ScalarValue::Float64(Some(1.0)),
            );
        task_ctx = task_ctx.with_session_config(session_config);
        let task_ctx = Arc::new(task_ctx);

        // Create aggregate: COUNT(*) GROUP BY group_col
        let group_expr = vec![(col("group_col", &schema)?, "group_col".to_string())];
        let aggr_expr = vec![Arc::new(
            AggregateExprBuilder::new(count_udaf(), vec![col("value_col", &schema)?])
                .schema(Arc::clone(&schema))
                .alias("count_value")
                .build()?,
        )];

        let input = Arc::new(
            BarrierExec::new(input_partitions, Arc::clone(&schema))
                .without_start_barrier()
                .with_finish_barrier()
                .with_log(false),
        );

        let aggregate_exec = AggregateExec::try_new(
            AggregateMode::Partial,
            PhysicalGroupBy::new_single(group_expr),
            aggr_expr,
            vec![None],
            Arc::clone(&input) as Arc<dyn ExecutionPlan>,
            Arc::clone(&schema),
        )?;

        let stream =
            PartialHashAggregateStream::new(&aggregate_exec, &task_ctx, 0)?.into_stream();

        Ok((stream, input, runtime))
    }

    #[tokio::test]
    async fn test_partial_hash_stream_accounts_held_batch_on_memory_pressure_while_slicing()
    -> Result<()> {
        // When memory pressure triggers early emission, the materialized state
        // batch is held while it is sliced into `batch_size` outputs. The
        // stream must keep that held batch accounted for in its memory
        // reservation until the last slice is emitted; before the fix the
        // reservation was resized down to just the (emptied) hash table size,
        // leaving the held batch unaccounted.

        let batch_size = 1024;
        // One row per group so the state batch is emitted in 4 slices
        let num_groups = 4 * batch_size;

        // Smaller than the building hash table (so pressure triggers) but large
        // enough to hold the materialized state batch (so slicing can proceed)
        let memory_limit = 100 * 1024;
        let (mut stream, input, runtime) =
            partial_stream_under_memory_limit(memory_limit, batch_size, num_groups)?;

        // The first output batch must be a pressure-emitted slice, with the rest
        // of the materialized state batch still held by the stream
        let first = tokio::time::timeout(Duration::from_secs(5), stream.next())
            .await
            .expect(
                "did not get early emit due to OOM, this probably means that the \
                 memory limit is too high to trigger the OOM",
            )
            .expect("stream ended early")?;
        assert_eq!(first.num_rows(), batch_size);

        // The emitted slice shares buffers with the held state batch, so its
        // array memory size reflects the full held allocation
        let held_size = first.get_array_memory_size();
        let reserved = runtime.memory_pool.reserved();
        assert!(
            reserved >= held_size,
            "memory pool has {reserved} bytes reserved but the stream is \
             holding a materialized state batch of {held_size} bytes"
        );

        let second = stream.next().await.expect("stream ended early")?;
        assert_eq!(second.num_rows(), batch_size);

        // Make sure the state batch is really being sliced (and not emitted whole by the fallback path):
        // the second output must share the same underlying buffer as the first
        //
        // If you changed the code and this fail because
        // - you now deep copy `batch_size` from the full state batch, please update this assertion to something else
        // - you only take batch size from the hash table, you can remove the test
        assert_eq!(
            first
                .column(0)
                .as_primitive::<Int32Type>()
                .values()
                .inner()
                .data_ptr(),
            second
                .column(0)
                .as_primitive::<Int32Type>()
                .values()
                .inner()
                .data_ptr(),
            "both batches should be slices of the same materialized state batch"
        );

        // Let the input finish and drain the stream: no groups lost
        input.wait_finish().await;
        let mut total_rows = first.num_rows() + second.num_rows();
        while let Some(batch) = stream.next().await {
            total_rows += batch?.num_rows();
        }
        assert_eq!(total_rows, num_groups);

        Ok(())
    }

    #[tokio::test]
    async fn test_partial_hash_stream_emits_whole_batch_when_held_batch_does_not_fit()
    -> Result<()> {
        // When memory pressure triggers early emission but the materialized
        // state batch itself does not fit in the reservation, the stream must
        // not fail with a resources exhausted error. Instead it gives up on
        // slicing and emits the whole state batch at once.

        let batch_size = 1024;
        let num_groups = 4 * batch_size;

        // Smaller than the materialized state batch (4096 rows of Int32 group
        // keys plus Int64 counts is at least 48 KiB), so the reservation for
        // hash table  held batch fails. The emptied hash table itself is tiny
        // and still fits.
        let memory_limit = 32 * 1024;
        let (mut stream, input, runtime) =
            partial_stream_under_memory_limit(memory_limit, batch_size, num_groups)?;

        let first = tokio::time::timeout(Duration::from_secs(5), stream.next())
            .await
            .expect(
                "did not get early emit due to OOM, this probably means that the \
                 memory limit is too high to trigger the OOM",
            )
            .expect("stream ended early")?;

        // The whole state batch is emitted at once instead of `batch_size` slices
        assert_eq!(first.num_rows(), num_groups);
        assert!(
            first.get_array_memory_size() > memory_limit,
            "test setup is wrong: the state batch fits within the memory limit, \
             so the slicing path would have been taken"
        );

        // Unlike the slicing path, the stream does not hold on to the emitted
        // batch, so it must not be accounted for in the reservation. Only the
        // (emptied) hash table remains reserved
        let emitted_size = first.get_array_memory_size();
        let reserved = runtime.memory_pool.reserved();
        assert!(
            reserved < emitted_size,
            "memory pool has {reserved} bytes reserved but the stream no longer \
             holds the emitted state batch of {emitted_size} bytes"
        );

        input.wait_finish().await;
        let mut total_rows = first.num_rows();
        while let Some(batch) = stream.next().await {
            total_rows += batch?.num_rows();
        }
        assert_eq!(total_rows, num_groups);

        Ok(())
    }

    #[tokio::test]
    async fn test_partial_hash_stream_releases_held_batch_after_last_slice() -> Result<()>
    {
        // While the pressure-emitted state batch is sliced, the stream holds
        // the remaining groups and keeps them reserved. Once the last slice is
        // handed out nothing is held anymore, so the reservation must drop
        // back to just the (emptied) hash table before the input is resumed.

        let batch_size = 1024;
        let num_slices = 4;
        let num_groups = num_slices * batch_size;

        let memory_limit = 100 * 1024;
        let (mut stream, input, runtime) =
            partial_stream_under_memory_limit(memory_limit, batch_size, num_groups)?;

        // The input has not finished, so all of these are pressure-emitted slices
        let mut held_size = 0;
        for slice_idx in 0..num_slices {
            let slice = if slice_idx == 0 {
                tokio::time::timeout(Duration::from_secs(5), stream.next())
                    .await
                    .expect(
                        "did not get early emit due to OOM, this probably means that the \
                         memory limit is too high to trigger the OOM",
                    )
                    .expect("stream ended early")?
            } else {
                stream.next().await.expect("stream ended early")?
            };

            assert_eq!(slice.num_rows(), batch_size);

            // Every slice shares buffers with the held state batch, so this is
            // the size of the full held allocation
            held_size = slice.get_array_memory_size();
            let reserved = runtime.memory_pool.reserved();

            if slice_idx + 1 < num_slices {
                assert!(
                    reserved >= held_size,
                    "after slice {slice_idx} the stream still holds {held_size} \
                     bytes but only {reserved} bytes are reserved"
                );
            } else {
                assert!(
                    reserved < held_size,
                    "after the last slice nothing is held anymore but {reserved} \
                     bytes are still reserved (held batch was {held_size} bytes)"
                );
            }
        }
        assert!(held_size > 0);

        input.wait_finish().await;
        let mut total_rows = num_groups;
        while let Some(batch) = stream.next().await {
            total_rows += batch?.num_rows();
        }
        assert_eq!(total_rows, num_groups);

        Ok(())
    }

    #[derive(Clone, Copy)]
    enum Finish {
        Collect,
        DropDuringReplay,
        InputError,
    }

    #[tokio::test]
    async fn final_hash_spill_replay_with_other_partitions_holding_state() -> Result<()> {
        for spills in [1, 2, 3] {
            run_shared_pool_case(spills, 1024 * 1024, Finish::Collect).await?;
        }
        // An unlimited pool produces the reference results without spilling.
        run_shared_pool_case(0, 10 * 1024 * 1024, Finish::Collect).await
    }

    #[tokio::test]
    async fn final_hash_spill_replay_releases_memory_on_drop() -> Result<()> {
        run_shared_pool_case(1, 1024 * 1024, Finish::DropDuringReplay).await
    }

    #[tokio::test]
    async fn final_hash_spill_releases_memory_on_input_error() -> Result<()> {
        run_shared_pool_case(1, 1024 * 1024, Finish::InputError).await
    }

    /// Partition 0 spills `spills` times and replays while partitions 1..3
    /// keep their aggregate state in the same greedy pool. With `spills` at 0,
    /// partition 0 reads a fixed input that fits in memory.
    ///
    /// Input sizes follow the observed reservations, so the cases do not
    /// depend on the memory accounting of a platform or feature set.
    ///
    /// These cases cover the spill and replay lifecycle in a shared pool:
    /// results, spill metrics, and memory release. Case G in
    /// `aggregate_memory_spill.slt` covers the merge fan-in regression for
    /// issue #25423.
    async fn run_shared_pool_case(
        spills: usize,
        limit: usize,
        finish: Finish,
    ) -> Result<()> {
        const PARTITIONS: usize = 4;
        /// Partitions 1..3 hold at least this much state.
        const HELD_BYTES: usize = 512 * 1024;
        /// A case that never reaches its target fails instead of looping.
        const MAX_BATCHES: i64 = 1000;

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
            Field::new("s", DataType::Utf8View, false),
        ]));
        let groups = PhysicalGroupBy::new_single(vec![
            (col("a", &schema)?, "a".into()),
            (col("b", &schema)?, "b".into()),
        ]);
        let expressions = vec![
            Arc::new(
                AggregateExprBuilder::new(sum_udaf(), vec![col("v", &schema)?])
                    .schema(Arc::clone(&schema))
                    .alias("sum")
                    .build()?,
            ),
            Arc::new(
                AggregateExprBuilder::new(min_udaf(), vec![col("s", &schema)?])
                    .schema(Arc::clone(&schema))
                    .alias("min")
                    .build()?,
            ),
        ];
        let empty = TestMemoryExec::try_new_exec(&[vec![]], Arc::clone(&schema), None)?;
        let partial = AggregateExec::try_new(
            AggregateMode::Partial,
            groups.clone(),
            expressions.clone(),
            vec![None; 2],
            empty,
            Arc::clone(&schema),
        )?;
        let partial_schema = partial.schema();
        let input = TestMemoryExec::try_new_exec(
            &vec![vec![]; PARTITIONS],
            Arc::clone(&partial_schema),
            None,
        )?;
        let aggregate = AggregateExec::try_new(
            AggregateMode::FinalPartitioned,
            groups.as_final(),
            expressions,
            vec![None; 2],
            input,
            schema,
        )?;
        assert_eq!(aggregate.input_order_mode(), &InputOrderMode::Linear);

        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(limit));
        let context = Arc::new(
            TaskContext::default()
                .with_session_config(SessionConfig::new().with_batch_size(128))
                .with_runtime(
                    RuntimeEnvBuilder::new()
                        .with_memory_pool(Arc::clone(&pool))
                        .build_arc()?,
                ),
        );
        let mut streams = vec![];
        let mut senders = vec![];
        for partition in 0..PARTITIONS {
            let (sender, receiver) = mpsc::unbounded();
            let input = Box::pin(RecordBatchStreamAdapter::new(
                Arc::clone(&partial_schema),
                receiver,
            ));
            let stream = FinalHashAggregateStream::new_with_input(
                &aggregate, &context, partition, input,
            )?
            .into_stream();
            senders.push(sender);
            streams.push(stream);
        }
        let mut expected = BTreeMap::new();
        let mut make_batch = |partition: i64, start: i64| {
            for value in start..start + 128 {
                let entry = expected
                    .entry((
                        partition,
                        if partition == 0 && value % 128 == 0 {
                            0
                        } else {
                            value
                        },
                    ))
                    .or_insert_with(|| (0, (value % 2).to_string()));
                entry.0 += value * 2;
            }
            RecordBatch::try_new(
                Arc::clone(&partial_schema),
                vec![
                    Arc::new(Int64Array::from(vec![partition; 128])),
                    Arc::new(Int64Array::from_iter_values((start..start + 128).map(
                        |value| {
                            if partition == 0 && value % 128 == 0 {
                                0
                            } else {
                                value
                            }
                        },
                    ))),
                    Arc::new(Int64Array::from_iter_values(
                        (start..start + 128).map(|v| v * 2),
                    )),
                    Arc::new(StringViewArray::from_iter_values(
                        (start..start + 128).map(|v| if v % 2 == 0 { "0" } else { "1" }),
                    )),
                ],
            )
            .unwrap()
        };
        // Channel inputs return Pending after each supplied batch, so the
        // interleaving below does not depend on task scheduling.
        let mut feed = |partition: usize, batch: i64| {
            senders[partition]
                .unbounded_send(Ok(make_batch(partition as i64, batch * 128)))
                .unwrap();
            assert!(streams[partition].next().now_or_never().is_none());
        };
        // Keep the state of partitions 1..3 live while partition 0 spills and
        // replays.
        let mut held_batches = 0;
        while pool.reserved() < HELD_BYTES {
            assert!(held_batches < MAX_BATCHES, "held state stays small");
            for partition in 1..PARTITIONS {
                feed(partition, held_batches);
            }
            held_batches += 1;
        }
        let held = pool.reserved();
        let spill_count = || aggregate.metrics().unwrap().spill_count().unwrap();
        assert_eq!(spill_count(), 0);
        // Feed partition 0 until it has spilled `spills` times. Key (0, 0)
        // repeats in every batch, so replay must merge its sum across runs.
        let mut batches = 0;
        loop {
            let done = if spills == 0 {
                batches == 70
            } else {
                spill_count() >= spills
            };
            if done {
                break;
            }
            assert!(batches < MAX_BATCHES, "partition 0 did not spill");
            feed(0, batches);
            batches += 1;
        }
        // Add groups after the last spill so replay also merges the final
        // in-memory run.
        for _ in 0..8 {
            feed(0, batches);
            batches += 1;
        }
        assert!(spill_count() >= spills);
        assert_eq!(spill_count() == 0, spills == 0);
        let mut first = streams.remove(0);
        match finish {
            Finish::Collect => {
                senders[0].close_channel();
                let mut output = collect(first).await?;
                assert_eq!(pool.reserved(), held);
                for sender in &senders[1..] {
                    sender.close_channel();
                }
                for stream in streams.drain(..) {
                    output.extend(collect(stream).await?);
                }
                let mut actual = BTreeMap::new();
                for batch in output {
                    let a = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    let b = batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    let sum = batch
                        .column(2)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    let min = batch
                        .column(3)
                        .as_any()
                        .downcast_ref::<StringViewArray>()
                        .unwrap();
                    for row in 0..batch.num_rows() {
                        assert!(
                            actual
                                .insert(
                                    (a.value(row), b.value(row)),
                                    (sum.value(row), min.value(row).to_string())
                                )
                                .is_none()
                        );
                    }
                }
                assert_eq!(actual, expected);
                assert_eq!(spill_count() == 0, spills == 0);
            }
            Finish::DropDuringReplay => {
                senders[0].close_channel();
                first.next().await.unwrap()?;
                assert!(pool.reserved() > held);
                drop(first);
                assert_eq!(pool.reserved(), held);
            }
            Finish::InputError => {
                senders[0]
                    .unbounded_send(datafusion_common::exec_err!(
                        "injected input failure"
                    ))
                    .unwrap();
                let error = first.next().await.unwrap().unwrap_err();
                assert!(error.to_string().contains("injected input failure"));
                assert_eq!(pool.reserved(), held);
                drop(first);
            }
        }
        drop(streams);
        assert_eq!(pool.reserved(), 0);
        Ok(())
    }
}
