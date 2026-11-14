// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Bounded key lookup, residual filtering, and output for regular joins.
//!
//! [`BatchedJoinProcessor`] accepts probe batches and returns final output batches.
//! Its caller does not manage row indices, lookup cursors, match bitmaps, or
//! output buffering. See the type's example for the input/output contract.

use std::sync::Arc;

use arrow::array::{ArrayRef, UInt32Array, UInt64Array};
use arrow::buffer::{BooleanBuffer, NullBuffer};
use arrow::compute::BatchCoalescer;
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
use crate::hash_utils::create_hashes;
use crate::joins::Map;
use crate::joins::join_hash_map::ProbeOffset;
use crate::joins::utils::{
    BuildProbeJoinMetrics, ColumnIndex, JoinFilter, JoinKeyComparator,
    adjust_indices_by_join_type, apply_join_filter_to_indices,
    build_batch_empty_build_side, build_batch_from_indices, matchable_join_keys,
};

/// Join semantics and output settings for one probe partition.
pub(super) struct BatchedJoinProcessorConfig {
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
    /// Hash state shared with build-side insertion.
    pub(super) random_state: RandomState,
    /// Whether NULL equality keys can match.
    pub(super) null_equality: NullEquality,
    /// Extra `ON` condition checked after key equality, e.g. `l.x > r.y`.
    /// Its input columns are independent of the output projection.
    pub(super) filter: Option<JoinFilter>,
    /// Target batch size for internal buffering, from configuration.
    pub(super) batch_size: usize,
    /// Preserve probe order when inserting unmatched rows for a right join.
    pub(super) right_side_ordered: bool,
    /// Maximum output rows from this processor; `None` means no limit.
    pub(super) fetch: Option<usize>,
}

/// Joins probe batches directly against the build map.
///
/// Each lookup chunk contains at most `batch_size` candidate pairs, even if one
/// key has millions of matches. The lookup offset resumes that key in the next
/// chunk. Hash collisions are checked before applying the residual predicate.
/// Residual predicates run before recording matches or emitting unmatched rows,
/// and surviving rows accumulate in a [`BatchCoalescer`] across chunks and inputs.
/// Output batches contain `batch_size` rows, except the final batch at EOF or a
/// fetch cutoff.
/// See the [hash join overview](super) for the build/index/probe boundary.
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
/// next_output_batch() -> Finished // All output from this processor is drained.
/// ```
pub(super) struct BatchedJoinProcessor {
    context: BatchedJoinProcessorConfig,
    metrics: BuildProbeJoinMetrics,
    build: Arc<JoinBuildState>,
    /// Input and residual-match state for the current probe batch.
    probe: Option<ProbeBatch>,
    phase: Phase,
    /// Lookup scratch and pair buffers are reused across input batches.
    hashes_buffer: Vec<u64>,
    probe_indices_buffer: Vec<u32>,
    build_indices_buffer: Vec<u64>,
    /// Materialized output retained across probe batches and join phases.
    output: BatchCoalescer,
    /// Tracker for a total limit pushed into hash join
    remaining_fetch: Option<usize>,
}

/// Input and lookup progress are separate from successful ON matches: rejecting
/// a chunk advances its offset, but does not make those probe rows joined.
struct ProbeBatch {
    input: RecordBatch,
    keys: Vec<ArrayRef>,
    valid_keys: Option<NullBuffer>,
    offset: ProbeOffset,
    key_comparator: Option<JoinKeyComparator>,
    /// Count each probe row once, even when its equality matches span chunks.
    last_probe_row_with_key_match: Option<u32>,
    /// RIGHT/FULL joins retain this across chunks to avoid treating a previously
    /// joined probe row as unmatched after later pairs fail the residual filter.
    last_probe_row_with_on_match: Option<u32>,
}

/// The next action for the caller of [`BatchedJoinProcessor::next_output_batch`].
pub(super) enum JoinStep {
    /// A joined output batch is ready.
    Output(RecordBatch),
    /// The caller should supply another probe batch or report EOF with
    /// [`BatchedJoinProcessor::finish_probe`].
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
    Finished,
}

impl BatchedJoinProcessor {
    pub(super) fn new(
        build: Arc<JoinBuildState>,
        context: BatchedJoinProcessorConfig,
        metrics: BuildProbeJoinMetrics,
    ) -> Self {
        // An empty map may still have build rows with NULL keys. Left/full
        // joins must retain those rows for final null extension.
        let join_type = context.kind.join_type();
        let can_skip_probe = (build.data.batch.num_rows() == 0
            && join_type.empty_build_side_produces_empty_result())
            || (build.data.map.is_empty() && join_type.empty_map_produces_empty_result());
        let phase = if can_skip_probe || context.fetch == Some(0) {
            Phase::DrainingOutput
        } else {
            Phase::Probing
        };
        Self {
            build,
            probe: None,
            hashes_buffer: vec![],
            probe_indices_buffer: vec![],
            build_indices_buffer: vec![],
            output: BatchCoalescer::new(Arc::clone(&context.schema), context.batch_size),
            remaining_fetch: context.fetch,
            context,
            metrics,
            phase,
        }
    }

    /// Accept one probe batch after draining the previous input.
    ///
    /// Evaluate keys and hashes once; candidate lookup and output materialization
    /// happen incrementally in [`Self::next_output_batch`].
    pub(super) fn push_batch(&mut self, batch: RecordBatch) -> Result<()> {
        if !matches!(self.phase, Phase::Probing) {
            return internal_err!(
                "BatchedJoinProcessor: cannot push input after probing finished"
            );
        }
        assert_or_internal_err!(
            self.probe.is_none(),
            "BatchedJoinProcessor: drain the current input before pushing another batch"
        );
        let keys = evaluate_expressions_to_arrays(&self.context.on_right, &batch)?;
        let valid_keys = if matches!(&**self.build.data.map, Map::HashMap(_)) {
            let _timer = self.metrics.join_time.timer();
            self.hashes_buffer.clear();
            self.hashes_buffer.resize(batch.num_rows(), 0);
            create_hashes(&keys, &self.context.random_state, &mut self.hashes_buffer)?;
            matchable_join_keys(&keys, self.context.null_equality)
        } else {
            None
        };
        self.probe = Some(ProbeBatch {
            input: batch,
            keys,
            valid_keys,
            offset: ProbeOffset::AtRow { row: 0 },
            key_comparator: None,
            last_probe_row_with_key_match: None,
            last_probe_row_with_on_match: None,
        });
        Ok(())
    }

    /// Materialize the next joined batch.
    ///
    /// Handles incremental computation and output coalescing to `batch_size`
    /// internally. See the comments on [`BatchedJoinProcessor`] for details.
    pub(super) fn next_output_batch(&mut self) -> Result<JoinStep> {
        let join_time = self.metrics.join_time.clone();
        loop {
            if let Some(batch) = self.output.next_completed_batch() {
                return Ok(JoinStep::Output(batch));
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
                    self.output.finish_buffered_batch()?;
                    self.phase = Phase::Finished;
                    continue;
                }
                Phase::Finished => return Ok(JoinStep::Finished),
            };
            let batch = if let Some(remaining) = &mut self.remaining_fetch {
                let rows = batch.num_rows().min(*remaining);
                *remaining -= rows;
                batch.slice(0, rows)
            } else {
                batch
            };
            self.output.push_batch(batch)?;
            if self.remaining_fetch == Some(0) {
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
            "BatchedJoinProcessor: finish probing only after draining the current input"
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

    /// Join another portion of the current probe batch against the build batch.
    ///
    /// 1. Collect up to `batch_size` key-equal row-index pairs
    ///    in `build_indices` and `probe_indices`.
    /// 2. Apply the join filter, record successful matches,
    ///    and add probe rows now known to be unmatched for right/full joins.
    /// 3. Materialize those pairs into a batch. [`Self::next_output_batch`]
    ///    coalesces surviving pairs and unmatched rows to the output batch size.
    ///
    /// One call may cover part of one probe row's matches or several probe rows.
    /// An empty batch means this chunk produced no output; `None` means the
    /// current probe batch is drained.
    fn probe_chunk(&mut self) -> Result<Option<RecordBatch>> {
        let Some(probe) = &mut self.probe else {
            return Ok(None);
        };
        let context = &self.context;
        let build = &self.build;
        let input = &probe.input;

        // NULL build keys can leave the map empty even when build rows exist.
        // Such rows cannot match but are still emitted at build finalization
        // for left/full joins. Right/full joins preserve this entire probe batch.
        if build.data.map.is_empty() {
            let batch = build_batch_empty_build_side(
                &context.schema,
                &build.data.batch,
                input,
                &context.column_indices,
                context.kind.join_type(),
            )?;
            self.probe = None;
            return Ok(Some(batch));
        }

        // Hash lookup reclaims its scratch internally. ArrayMap returns the
        // scratch as Arrow arrays, so retain their buffers until materialization.
        let mut array_map_buffers = None;
        let (build_indices, probe_indices, next_offset) = match &**build.data.map {
            Map::HashMap(map) => lookup_join_hashmap(
                map.as_ref(),
                &build.data.values,
                &probe.keys,
                context.null_equality,
                &self.hashes_buffer,
                probe.valid_keys.as_ref(),
                context.batch_size,
                probe.offset,
                &mut self.probe_indices_buffer,
                &mut self.build_indices_buffer,
                &mut probe.key_comparator,
            )?,
            Map::ArrayMap(map) => {
                let next_offset = map.get_matched_indices_with_limit_offset(
                    &probe.keys,
                    context.batch_size,
                    probe.offset,
                    &mut self.probe_indices_buffer,
                    &mut self.build_indices_buffer,
                )?;
                let build_indices: UInt64Array =
                    std::mem::take(&mut self.build_indices_buffer).into();
                let probe_indices: UInt32Array =
                    std::mem::take(&mut self.probe_indices_buffer).into();
                array_map_buffers = Some((
                    build_indices.values().clone(),
                    probe_indices.values().clone(),
                ));
                (build_indices, probe_indices, next_offset)
            }
        };

        // Equality pairs are ordered by probe row. A row continuing from the
        // previous chunk contributes fanout, but not another probe hit.
        let mut matched_probe_rows = 0;
        for &row in probe_indices.values().iter() {
            if probe.last_probe_row_with_key_match != Some(row) {
                matched_probe_rows += 1;
                probe.last_probe_row_with_key_match = Some(row);
            }
        }
        self.metrics.probe_hit_rate.add_part(matched_probe_rows);
        self.metrics.avg_fanout.add_part(build_indices.len());
        self.metrics.avg_fanout.add_total(matched_probe_rows);

        let (build_indices, probe_indices) = if let Some(filter) = &context.filter {
            apply_join_filter_to_indices(
                &build.data.batch,
                input,
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

        // Only successful ON matches suppress unmatched build output. Rows
        // rejected by the residual filter remain eligible for final output.
        build.record_matches(&build_indices);

        let (build_indices, probe_indices) = if context.kind.preserves_probe() {
            let last_joined_probe_row = probe_indices.values().last().copied();
            let start = probe
                .last_probe_row_with_on_match
                .map_or(0, |row| row as usize + 1);
            let end = if next_offset.is_none() {
                input.num_rows()
            } else {
                last_joined_probe_row.map_or(0, |row| row as usize + 1)
            };
            if last_joined_probe_row.is_some() {
                probe.last_probe_row_with_on_match = last_joined_probe_row;
            }
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

        let output = build_batch_from_indices(
            &context.schema,
            &build.data.batch,
            input,
            &build_indices,
            &probe_indices,
            &context.column_indices,
            JoinSide::Left,
            context.kind.join_type(),
            None,
        )?;
        drop((build_indices, probe_indices));
        if let Some((build_buffer, probe_buffer)) = array_map_buffers {
            self.build_indices_buffer = build_buffer.into();
            self.probe_indices_buffer = probe_buffer.into();
        }
        if let Some(offset) = next_offset {
            probe.offset = offset;
        } else {
            self.probe = None;
        }
        Ok(Some(output))
    }

    /// Release input and lookup storage on probe EOF or a fetch cutoff.
    /// Output already materialized in the coalescer remains available.
    fn release_probe(&mut self) {
        self.probe = None;
        self.hashes_buffer = Vec::new();
        self.probe_indices_buffer = Vec::new();
        self.build_indices_buffer = Vec::new();
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
