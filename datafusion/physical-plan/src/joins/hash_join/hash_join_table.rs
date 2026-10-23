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

//! A regular join's build table, probing, and output materialization.
//!
//! [`HashJoinTable`] accepts probe batches and returns final output batches.
//! Its caller does not manage row indices, lookup cursors, match bitmaps, or
//! output buffering. See the type's example for the input/output contract.

use std::sync::Arc;

use arrow::array::{ArrayRef, UInt32Array, UInt64Array};
use arrow::buffer::{BooleanBuffer, NullBuffer, ScalarBuffer};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::hash_utils::RandomState;
use datafusion_common::{
    JoinSide, JoinType, NullEquality, Result, assert_or_internal_err, internal_err,
};
use datafusion_physical_expr::PhysicalExprRef;
use datafusion_physical_expr_common::utils::evaluate_expressions_to_arrays;

use super::build::JoinBuildState;
use super::stream::lookup_join_hashmap;
use crate::coalesce::{LimitedBatchCoalescer, PushBatchStatus};
use crate::hash_utils::create_hashes;
use crate::joins::Map;
use crate::joins::join_hash_map::ProbeOffset;
use crate::joins::utils::{
    BuildProbeJoinMetrics, ColumnIndex, JoinFilter, JoinKeyComparator,
    adjust_indices_by_join_type, apply_join_filter_to_indices,
    build_batch_empty_build_side, build_batch_from_indices, matchable_join_keys,
};

/// Join semantics and output settings for one probe partition.
pub(super) struct HashJoinTableConfig {
    /// Join type, which determines whether to keep unmatched build or probe rows.
    pub(super) kind: RegularJoinKind,
    /// Output schema after projection.
    pub(super) schema: SchemaRef,
    /// Probe input schema, used for NULL columns in unmatched build output.
    pub(super) probe_schema: SchemaRef,
    /// For each output column, its source side and column index.
    pub(super) column_indices: Vec<ColumnIndex>,
    /// Probe equality-key expressions, paired with the build keys by position.
    pub(super) on_right: Vec<PhysicalExprRef>,
    /// Extra `ON` condition checked after key equality, e.g. `l.x > r.y`.
    /// Its input columns are independent of the output projection.
    pub(super) filter: Option<JoinFilter>,
    /// Hashing state matching the build side, so equal keys hash alike.
    pub(super) random_state: RandomState,
    /// Whether NULL join keys match other NULLs.
    pub(super) null_equality: NullEquality,
    /// Target batch size for internal buffering, from configuration.
    pub(super) batch_size: usize,
    /// Preserve probe order when inserting unmatched rows for a right join.
    pub(super) right_side_ordered: bool,
    /// Maximum output rows from this table; `None` means no limit.
    pub(super) fetch: Option<usize>,
}

/// Hash join's build-side hash index and state for processing the full join logic
/// for one probe-side batch. It vectorizes the computation steps while keeping
/// computation incremental for memory efficiency: only `batch_size` rows of
/// intermediate batches/indices are kept at a time. See the `Incremental Computation`
/// section for details.
///
/// # Architecture
///
/// The key conceptual components inside this struct are:
///
/// - Build-side batch: the full build side, buffered and concatenated.
/// - Probe-side batch: a single batch for the current iteration.
/// - Hash index: a hash map with the scheme `join key -> indices in build-side batch`.
/// - Runtime state (e.g., the current `(build_index, probe_index)` for incremental
///   processing).
/// - Output buffer for incremental processing.
///
/// They work together to perform the full join logic of
/// `build_side join one_probe_batch`.
///
/// # Hash Index
///
/// Conceptually, the hash index maps
/// `join equality key -> row indices in buffered build-side batches`, for example:
///
/// ```text
/// Build keys: [1, 2, 2]
///
/// -- Conceptual hash index
/// Key -> Build row indices
///   1 -> [0]
///   2 -> [1, 2]
/// ```
///
/// # Incremental Computation
///
/// It keeps internal state to support incremental computation and coalesce small
/// outputs. Output is aligned to the configured `batch_size`, like:
///
/// ```text
/// output batch 1: 8192 rows
/// output batch 2: 8192 rows
/// ...
/// last output batch: 1000 rows // Output ends; all remaining rows.
/// ```
///
/// - Incremental Computation: even if each probe row has many matches, it will only
///   materialize up to `batch_size` output rows per iteration, for memory efficiency.
/// - Coalescing: if a probe batch produces fewer than `batch_size` output rows,
///   polling will not produce output (unless at the end). The caller must continue
///   providing probe-side batches until there is an output batch of `batch_size` rows.
///
/// # Key API walkthrough
///
/// Below are the key APIs; see their comments for details. The following example
/// demonstrates how they are used together in the join logic.
/// - [`Self::push_batch`]
/// - [`Self::next_output_batch`]
/// - [`Self::finish_probe`]
///
/// For a full join with one probe partition, build keys `[1, 2, 2]`, and
/// `batch_size = 1`:
///
/// ```text
/// next_output_batch() -> NeedInput
/// push_batch([2])
/// next_output_batch() -> Output([(2, 2)])
/// next_output_batch() -> Output([(2, 2)])
/// next_output_batch() -> NeedInput // This batch is done; the stream may have more.
///
/// push_batch([3])
/// next_output_batch() -> Output([(NULL, 3)])
/// next_output_batch() -> NeedInput
///
/// // The caller reads the probe stream and observes EOF.
/// finish_probe()
/// next_output_batch() -> Output([(1, NULL)])
/// next_output_batch() -> Finished // All output from this table is drained.
/// ```
pub(super) struct HashJoinTable {
    context: HashJoinTableConfig,
    /// Counters and timings for this join partition.
    metrics: BuildProbeJoinMetrics,

    // ===================
    // Static data buffers
    // ===================
    build: Arc<JoinBuildState>,
    /// Prepared input, unchanged while this batch is being joined.
    probe: Option<ProbeBatch>,

    // =============
    // Runtime state
    // =============
    //
    // Track lookup progress, such as the last build/probe row examined,
    // to produce output incrementally.
    /// Current execution phase.
    phase: Phase,
    /// Where to resume candidate lookup; `None` means this batch is exhausted.
    next_offset: Option<ProbeOffset>,
    /// Last probe row passing the full ON condition. Unmatched probe rows
    /// are considered after this row, even when its matches span chunks.
    joined_probe_idx: Option<usize>,
    /// Last equality match, before the ON filter, to count each probe row once
    /// in hit-rate and fanout metrics across lookup chunks.
    matched_probe_idx: Option<u32>,

    // ================
    // Lookup utilities
    // ================
    /// Cached equality checks bound to the build and current probe key arrays.
    key_comparator: Option<JoinKeyComparator>,
    /// Lookup allocations reused across chunks and input batches.
    hashes: Vec<u64>,

    // ==============
    // Output buffers
    // ==============
    //
    // Output construction has two steps:
    // 1. Probe the hash index for matching row-index pairs.
    // 2. Materialize the matched rows, evaluate the residual join filter, and build
    //    the final joined output.
    //
    // For `t1 JOIN t2 ON t1.v1 = t2.v1 AND t1.v2 > t2.v2`:
    // - Step 1: Row-index pairs satisfying `t1.v1 = t2.v1`.
    // - Step 2: Joined rows satisfying `t1.v2 > t2.v2`.
    //
    // Separate buffers are used for these two steps.

    // Output buffers for step 1.
    /// Matched row indices into [`Self::build`]'s data batch.
    build_indices: Vec<u64>,
    /// Matched row indices into [`Self::probe`]'s data batch.
    probe_indices: Vec<u32>,
    /// Retain array-map index buffers through materialization, then reclaim
    /// them for the next lookup without copying.
    array_map_buffers: Option<(ScalarBuffer<u64>, ScalarBuffer<u32>)>,

    // Output buffer for step 2.
    /// Materialized output retained across probe batches and join phases.
    output: LimitedBatchCoalescer,
}

/// Prepared input for one probe batch; all mutable join progress lives in
/// [`HashJoinTable`].
struct ProbeBatch {
    input: RecordBatch,
    keys: Vec<ArrayRef>,
    valid_keys: Option<NullBuffer>,
}

/// The next action for the caller of [`HashJoinTable::next_output_batch`].
pub(super) enum JoinStep {
    /// A joined output batch is ready.
    Output(RecordBatch),
    /// The caller should supply another probe batch or report EOF with
    /// [`HashJoinTable::finish_probe`].
    NeedInput,
    /// All output is drained, possibly before probe EOF. Stop reading input.
    Finished,
}

/// Join work remaining. The output coalescer outlives every phase.
enum Phase {
    Probing,
    /// Only the last probe partition receives this final match snapshot.
    /// Scan it in chunks without holding the shared bitmap lock.
    UnmatchedBuild {
        visited: BooleanBuffer,
        next_row: usize,
        empty_probe: RecordBatch,
    },
    /// No more rows will be produced; flush and drain buffered output.
    DrainingOutput,
}

impl HashJoinTable {
    pub(super) fn new(
        build: Arc<JoinBuildState>,
        context: HashJoinTableConfig,
        metrics: BuildProbeJoinMetrics,
    ) -> Self {
        // An empty map may still have build rows with NULL keys. Left/full
        // joins must retain those rows for final null extension.
        let can_skip_probe = (build.data.batch.num_rows() == 0
            && !context.kind.preserves_probe())
            || (build.data.map.is_empty()
                && matches!(context.kind, RegularJoinKind::Inner));
        let phase = if can_skip_probe {
            Phase::DrainingOutput
        } else {
            Phase::Probing
        };
        let lookup_capacity = if can_skip_probe {
            0
        } else {
            context.batch_size
        };
        Self {
            build,
            probe: None,
            next_offset: None,
            joined_probe_idx: None,
            matched_probe_idx: None,
            key_comparator: None,
            hashes: Vec::new(),
            build_indices: Vec::with_capacity(lookup_capacity),
            probe_indices: Vec::with_capacity(lookup_capacity),
            array_map_buffers: None,
            output: LimitedBatchCoalescer::new(
                Arc::clone(&context.schema),
                context.batch_size,
                context.fetch,
            ),
            context,
            metrics,
            phase,
        }
    }

    /// Accept one probe batch.
    ///
    /// Actual computation happens when calling [`Self::next_output_batch`].
    /// One input batch might produce a large final output (due to one-to-many
    /// join matches), so this module handles computation incrementally.
    pub(super) fn push_batch(&mut self, batch: RecordBatch) -> Result<()> {
        if !matches!(self.phase, Phase::Probing) {
            return internal_err!(
                "HashJoinTable: cannot push input after probing finished"
            );
        }
        assert_or_internal_err!(
            self.probe.is_none(),
            "HashJoinTable: drain the current input before pushing another batch"
        );
        let keys = evaluate_expressions_to_arrays(&self.context.on_right, &batch)?;
        let valid_keys = if let Map::HashMap(_) = self.build.data.map.as_ref() {
            self.hashes.clear();
            self.hashes.resize(batch.num_rows(), 0);
            create_hashes(&keys, &self.context.random_state, &mut self.hashes)?;
            matchable_join_keys(&keys, self.context.null_equality)
        } else {
            None
        };

        self.metrics.input_batches.add(1);
        self.metrics.input_rows.add(batch.num_rows());
        self.metrics.probe_hit_rate.add_total(batch.num_rows());

        // Every cursor and cached comparator belongs to these input arrays.
        // Keep the scratch allocations, but reset all progress together.
        self.next_offset = Some(ProbeOffset::AtRow { row: 0 });
        self.joined_probe_idx = None;
        self.matched_probe_idx = None;
        self.key_comparator = None;
        self.probe = Some(ProbeBatch {
            input: batch,
            keys,
            valid_keys,
        });
        Ok(())
    }

    /// Materialize the next joined batch.
    ///
    /// Handles incremental computation and output coalescing to `batch_size`
    /// internally. See the comments on [`HashJoinTable`] for details.
    pub(super) fn next_output_batch(&mut self) -> Result<JoinStep> {
        let join_time = self.metrics.join_time.clone();
        loop {
            if let Some(batch) = self.output.next_completed_batch() {
                return Ok(JoinStep::Output(batch));
            }
            if self.output.is_finished() {
                return Ok(JoinStep::Finished);
            }

            let _timer = join_time.timer();

            // Evaluate one final joined batch and put it into the output coalescer
            // to combine potentially small batches.
            let batch = match &mut self.phase {
                Phase::Probing => {
                    let Some(batch) = self.probe_chunk()? else {
                        return Ok(JoinStep::NeedInput);
                    };
                    batch
                }
                Phase::UnmatchedBuild {
                    visited,
                    next_row,
                    empty_probe,
                } => {
                    let mut indices = Vec::with_capacity(
                        self.context.batch_size.min(visited.len() - *next_row),
                    );
                    while *next_row < visited.len()
                        && indices.len() < self.context.batch_size
                    {
                        if !visited.value(*next_row) {
                            indices.push(*next_row as u64);
                        }
                        *next_row += 1;
                    }
                    self.metrics.input_rows.add(indices.len());
                    if indices.is_empty() {
                        self.phase = Phase::DrainingOutput;
                        continue;
                    }
                    let probe_indices = UInt32Array::new_null(indices.len());
                    build_batch_from_indices(
                        &self.context.schema,
                        &self.build.data.batch,
                        empty_probe,
                        &UInt64Array::from(indices),
                        &probe_indices,
                        &self.context.column_indices,
                        JoinSide::Left,
                        self.context.kind.join_type(),
                        None,
                    )?
                }
                Phase::DrainingOutput => {
                    self.output.finish()?;
                    continue;
                }
            };
            if self.output.push_batch(batch)? == PushBatchStatus::LimitReached {
                self.release_probe();
                self.phase = Phase::DrainingOutput;
            }
        }
    }

    /// Close this partition's probe input after all its batches have been
    /// drained and the caller has exhausted all probe-side input batches.
    pub(super) fn finish_probe(&mut self) -> Result<()> {
        assert_or_internal_err!(
            matches!(self.phase, Phase::Probing) && self.probe.is_none(),
            "HashJoinTable: finish probing only after draining the current input"
        );
        let join_time = self.metrics.join_time.clone();
        let _timer = join_time.timer();
        self.release_probe();
        self.phase = if let Some(visited) = self.build.finish_probe() {
            self.metrics.input_batches.add(1);
            Phase::UnmatchedBuild {
                visited,
                next_row: 0,
                empty_probe: RecordBatch::new_empty(Arc::clone(
                    &self.context.probe_schema,
                )),
            }
        } else {
            Phase::DrainingOutput
        };
        Ok(())
    }

    /// Join another portion of the current probe batch against the build batch and
    /// produce one final joined batch (with <= `batch_size` rows).
    ///
    /// 1. Resume lookup, collecting up to `batch_size` candidate row-index pairs
    ///    in `build_indices` and `probe_indices`.
    /// 2. Check key equality and the join filter, record successful matches,
    ///    and add probe rows now known to be unmatched for right/full joins.
    /// 3. Materialize those pairs into a batch. [`Self::next_output_batch`]
    ///    appends it to the final output buffer.
    ///
    /// One call may cover part of one probe row's matches or several probe rows.
    /// An empty batch means this chunk produced no output; `None` means the
    /// current probe batch is drained.
    fn probe_chunk(&mut self) -> Result<Option<RecordBatch>> {
        if let Some((build, probe)) = self.array_map_buffers.take() {
            self.build_indices = build.into();
            self.probe_indices = probe.into();
        }
        let Some(probe) = &self.probe else {
            return Ok(None);
        };
        let Some(offset) = self.next_offset.take() else {
            self.probe = None;
            self.key_comparator = None;
            self.joined_probe_idx = None;
            self.matched_probe_idx = None;
            return Ok(None);
        };
        let context = &self.context;
        let build = &self.build;

        // NULL build keys can leave the map empty even when build rows exist.
        // Such rows cannot match but are still emitted at build finalization
        // for left/full joins. Right/full joins preserve this entire probe batch.
        if build.data.map.is_empty() {
            return build_batch_empty_build_side(
                &context.schema,
                &build.data.batch,
                &probe.input,
                &context.column_indices,
                context.kind.join_type(),
            )
            .map(Some);
        }

        let (build_indices, probe_indices, next_offset) = match build.data.map.as_ref() {
            Map::HashMap(map) => lookup_join_hashmap(
                map.as_ref(),
                &build.data.values,
                &probe.keys,
                context.null_equality,
                &self.hashes,
                probe.valid_keys.as_ref(),
                context.batch_size,
                offset,
                &mut self.probe_indices,
                &mut self.build_indices,
                &mut self.key_comparator,
            )?,
            Map::ArrayMap(map) => {
                let next_offset = map.get_matched_indices_with_limit_offset(
                    &probe.keys,
                    context.batch_size,
                    offset,
                    &mut self.probe_indices,
                    &mut self.build_indices,
                )?;
                let build_indices: UInt64Array =
                    std::mem::take(&mut self.build_indices).into();
                let probe_indices: UInt32Array =
                    std::mem::take(&mut self.probe_indices).into();
                self.array_map_buffers = Some((
                    build_indices.values().clone(),
                    probe_indices.values().clone(),
                ));
                (build_indices, probe_indices, next_offset)
            }
        };

        // Both maps return probe indices in ascending order. Remember the last
        // equality match across chunks to avoid counting a high-fanout probe
        // row once per chunk in either ratio metric.
        let values = probe_indices.values();
        let matched_probe_rows = match (values.first(), values.last()) {
            (Some(&first), Some(&last)) => {
                let mut count = 1 + values.windows(2).filter(|w| w[0] != w[1]).count();
                if self.matched_probe_idx == Some(first) {
                    count -= 1;
                }
                self.matched_probe_idx = Some(last);
                count
            }
            _ => 0,
        };
        self.metrics.probe_hit_rate.add_part(matched_probe_rows);
        self.metrics.avg_fanout.add_part(build_indices.len());
        self.metrics.avg_fanout.add_total(matched_probe_rows);

        let (build_indices, probe_indices) = if let Some(filter) = &context.filter {
            apply_join_filter_to_indices(
                &build.data.batch,
                &probe.input,
                build_indices,
                probe_indices,
                filter,
                JoinSide::Left,
                None,
                context.kind.join_type(),
            )?
        } else {
            (build_indices, probe_indices)
        };

        // Only successful ON matches suppress unmatched build output. Hash
        // collisions and rows rejected by the join filter must not mark it.
        build.record_matches(&build_indices);

        let last_joined_probe_idx =
            probe_indices.values().last().map(|&idx| idx as usize);
        let (build_indices, probe_indices) = if context.kind.preserves_probe() {
            let start = self.joined_probe_idx.map_or(0, |idx| idx + 1);
            let end = if next_offset.is_none() {
                probe.input.num_rows()
            } else {
                last_joined_probe_idx.map_or(0, |idx| idx + 1)
            };
            adjust_indices_by_join_type(
                build_indices,
                probe_indices,
                start..end,
                context.kind.join_type(),
                context.right_side_ordered,
            )?
        } else {
            (build_indices, probe_indices)
        };

        self.next_offset = next_offset;
        if last_joined_probe_idx.is_some() {
            self.joined_probe_idx = last_joined_probe_idx;
        }
        build_batch_from_indices(
            &context.schema,
            &build.data.batch,
            &probe.input,
            &build_indices,
            &probe_indices,
            &context.column_indices,
            JoinSide::Left,
            context.kind.join_type(),
            None,
        )
        .map(Some)
    }

    /// Release input and lookup storage on probe EOF or a fetch cutoff.
    /// Output already materialized in the coalescer remains available.
    fn release_probe(&mut self) {
        self.probe = None;
        self.next_offset = None;
        self.joined_probe_idx = None;
        self.matched_probe_idx = None;
        self.key_comparator = None;
        self.hashes = Vec::new();
        self.build_indices = Vec::new();
        self.probe_indices = Vec::new();
        self.array_map_buffers = None;
    }
}

/// The supported semantics of this execution path. Existence and mark joins
/// retain their own path rather than adding branches to regular probing.
#[derive(Clone, Copy)]
pub(super) enum RegularJoinKind {
    Inner,
    Left,
    Right,
    Full,
}

impl RegularJoinKind {
    pub(super) fn from_join_type(join_type: JoinType) -> Option<Self> {
        match join_type {
            JoinType::Inner => Some(Self::Inner),
            JoinType::Left => Some(Self::Left),
            JoinType::Right => Some(Self::Right),
            JoinType::Full => Some(Self::Full),
            _ => None,
        }
    }

    fn join_type(self) -> JoinType {
        match self {
            Self::Inner => JoinType::Inner,
            Self::Left => JoinType::Left,
            Self::Right => JoinType::Right,
            Self::Full => JoinType::Full,
        }
    }

    fn preserves_probe(self) -> bool {
        matches!(self, Self::Right | Self::Full)
    }
}
