// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Bounded key lookup, residual filtering, and output for regular joins.
//!
//! [`BatchedJoinProcessor`] accepts probe batches and returns final output batches.
//! Its caller does not manage row indices, lookup cursors, match bitmaps, or
//! output buffering. See the example on the type for the input/output contract.

use std::sync::Arc;

use arrow::array::{UInt32Array, UInt64Array};
use arrow::buffer::BooleanBuffer;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::{
    JoinSide, JoinType, Result, assert_or_internal_err, internal_err,
};
use datafusion_physical_expr::PhysicalExprRef;
use datafusion_physical_expr_common::utils::evaluate_expressions_to_arrays;

use super::build::RegularJoinLeftData;
use super::probe_cursor::{GroupCursor, MatchChunk, PairBuffer};
use crate::coalesce::{LimitedBatchCoalescer, PushBatchStatus};
use crate::joins::join_index::{LookupResult, LookupScratch, UniqueMatches};
use crate::joins::utils::{
    BuildProbeJoinMetrics, ColumnIndex, JoinFilter, adjust_indices_by_join_type,
    apply_join_filter_to_indices, build_batch_empty_build_side, build_batch_from_indices,
};

/// Join semantics and output settings for one probe partition.
pub(super) struct BatchedJoinProcessorConfig {
    /// Join type, which determines whether to keep unmatched build or probe rows.
    pub(super) kind: RegularJoinKind,
    /// Output schema after projection.
    pub(super) schema: SchemaRef,
    /// Probe input schema, used to construct NULL columns for unmatched build rows.
    pub(super) probe_schema: SchemaRef,
    /// For each output column, its source side and column index.
    pub(super) column_indices: Vec<ColumnIndex>,
    /// Probe equality-key expressions, paired with the build keys by position.
    pub(super) on_right: Vec<PhysicalExprRef>,
    /// Additional `ON` condition checked after key equality, e.g. `l.x > r.y`.
    /// Its input columns are independent of the output projection.
    pub(super) filter: Option<JoinFilter>,
    /// Configured target batch size for internal buffering.
    pub(super) batch_size: usize,
    /// Preserve probe order when inserting unmatched rows for a right join.
    pub(super) right_side_ordered: bool,
    /// Maximum number of output rows from this processor; `None` means no limit.
    pub(super) fetch: Option<usize>,
}

/// Joins probe batches against the build index.
///
/// See the [hash join overview](super) for the overall architecture. This middle
/// layer:
///
/// 1. Looks up all rows in the probe batch through
///    [`JoinIndexMap`](crate::joins::join_index::JoinIndexMap), which returns
///    constant-size match handles without materializing duplicates. It then
///    expands duplicate groups in chunks of at most `batch_size` key-equal pairs.
/// 2. Applies residual filters and handles unmatched rows for outer joins.
/// 3. Materializes output and coalesces it toward `batch_size` rows.
///
/// # Design Example
///
/// For an inner join with `batch_size = 100`, suppose the residual filter keeps
/// 20 pairs from each chunk:
///
/// ```text
/// Probe row    Match handle    Build matches
/// 0            Group(7)     ->  1,000 rows
/// 1            Group(42)    ->  ...
/// ...          ...
/// 99           Group(3)     ->  ...
///
/// Expand Group(7) one chunk at a time:
///   100 key-equal pairs -> residual filter -> 20 final output rows
/// ```
///
/// After five chunks, the coalescer returns a 100-row batch. End of input or
/// reaching the row limit can flush a smaller final batch.
///
/// # API Example
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
    /// Build-side rows, and raw hash index
    build: Arc<RegularJoinLeftData>,
    /// The current probe batch, and its progress tracker.
    probe: Option<ProbeBatch>,
    phase: Phase,
    /// Lookup scratch, reused across input batches.
    lookup_scratch: LookupScratch,
    /// Pair buffers scratch, reused across input batches.
    pair_buffers: PairBuffer,
    /// Coalesces materialized output and enforces any fetch/limit pushed into
    /// the hash join. For example, with `HashJoinExec(fetch=10)`, the processor
    /// can stop early after materializing 10 output rows.
    output: LimitedBatchCoalescer,
}

/// Current probe batch and its progress.
///
/// Input batches follow the DataFusion [`ExecutionPlan`](crate::ExecutionPlan)
/// convention of coalescing to the configured `batch_size`, so the `Unique`
/// variant can materialize all matches at once without tracking progress.
enum ProbeBatch {
    Unique {
        input: RecordBatch,
        pairs: UniqueMatches,
    },
    Groups {
        input: RecordBatch,
        /// Match handles from the raw hash index and a progress tracker for
        /// incremental materialization.
        cursor: GroupCursor,
        /// Only right and full joins need this across chunks, to avoid emitting a
        /// previously matched probe row as unmatched when later pairs fail the filter.
        last_probe_row_with_on_match: Option<u32>,
    },
}

/// The next action for the caller of [`BatchedJoinProcessor::next_output_batch`].
pub(super) enum JoinStep {
    /// A joined output batch is ready.
    Output(RecordBatch),
    /// The caller should supply another probe batch or report EOF with
    /// [`BatchedJoinProcessor::finish_probe`].
    NeedInput,
    /// All output has been drained, possibly before probe EOF. Stop reading input.
    Finished,
}

/// Remaining join work. The output coalescer persists across all phases.
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
        build: Arc<RegularJoinLeftData>,
        context: BatchedJoinProcessorConfig,
        metrics: BuildProbeJoinMetrics,
    ) -> Self {
        // The map may be empty even when build rows with NULL keys exist. Left
        // and full joins must retain those rows for final null extension.
        let join_type = context.kind.join_type();
        let can_skip_probe = (build.data.batch.num_rows() == 0
            && join_type.empty_build_side_produces_empty_result())
            || (build.data.map.is_empty() && join_type.empty_map_produces_empty_result());
        let phase = if can_skip_probe || context.fetch == Some(0) {
            Phase::DrainingOutput
        } else {
            Phase::Probing
        };
        let lookup_scratch = LookupScratch::new(&build.data.reservation);
        let pair_buffers = PairBuffer::new(&build.data.reservation);
        Self {
            build,
            probe: None,
            lookup_scratch,
            pair_buffers,
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

    /// Accept a batch while probing, after draining the previous input.
    ///
    /// See [`BatchedJoinProcessor`] for the lookup and materialization flow.
    pub(super) fn push_batch(&mut self, batch: RecordBatch) -> Result<()> {
        if !matches!(self.phase, Phase::Probing) {
            return internal_err!(
                "BatchedJoinProcessor: cannot push input after probing has finished"
            );
        }
        assert_or_internal_err!(
            self.probe.is_none(),
            "BatchedJoinProcessor: drain the current input before pushing another batch"
        );
        let keys = evaluate_expressions_to_arrays(&self.context.on_right, &batch)?;

        // Unique results already fit in one chunk. Group handles defer duplicate
        // expansion to avoid materializing every pair in a large group here.
        let matches = {
            let _timer = self.metrics.join_time.timer();
            self.build.data.map.lookup_batch(
                &self.build.data.values,
                &keys,
                &mut self.lookup_scratch,
            )?
        };
        self.probe = Some(match matches {
            LookupResult::Unique(pairs) => ProbeBatch::Unique {
                input: batch,
                pairs,
            },
            LookupResult::Groups(matches) => ProbeBatch::Groups {
                input: batch,
                cursor: GroupCursor::new(matches),
                last_probe_row_with_on_match: None,
            },
        });
        Ok(())
    }

    /// Return the next output batch or signal that input is needed or processing
    /// has finished. See [`BatchedJoinProcessor`] for the processing flow.
    pub(super) fn next_output_batch(&mut self) -> Result<JoinStep> {
        let join_time = self.metrics.join_time.clone();
        loop {
            if let Some(batch) = self.output.next_completed_batch() {
                return Ok(JoinStep::Output(batch));
            }
            let _timer = join_time.timer();

            // Produce one final joined batch and pass it to the output coalescer,
            // which combines small batches.
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
                    self.phase = Phase::Finished;
                    continue;
                }
                Phase::Finished => return Ok(JoinStep::Finished),
            };
            // Operator-level limit reached; stop probing.
            // See the comments on `output` for details.
            if self.output.push_batch(batch)? == PushBatchStatus::LimitReached {
                self.release_probe();
                self.phase = Phase::DrainingOutput;
            }
        }
    }

    /// Report probe EOF after draining the current input, then continue calling
    /// [`Self::next_output_batch`] to drain final output. See [`BatchedJoinProcessor`].
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

    /// Filter and materialize one chunk; see [`BatchedJoinProcessor`] for the flow.
    ///
    /// Records matched build rows and emits unmatched probe rows as required by
    /// the join type. Returns a possibly empty batch, or `None` when the current
    /// probe batch is drained.
    fn probe_chunk(&mut self) -> Result<Option<RecordBatch>> {
        let Some(probe) = &mut self.probe else {
            return Ok(None);
        };
        let context = &self.context;
        let build = &self.build;

        // NULL build keys can leave the map empty even when build rows exist.
        // Such rows cannot match but are still emitted at build finalization
        // for left and full joins. Right and full joins preserve the entire probe batch.
        if build.data.map.is_empty() {
            let input = match probe {
                ProbeBatch::Unique { input, .. } | ProbeBatch::Groups { input, .. } => {
                    input
                }
            };
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

        // Keep the lookup result (and its reservation) alive until output has
        // been materialized. Retain it only while the probe batch has more chunks.
        let (input, chunk, exhausted, last_on_match) = match probe {
            ProbeBatch::Unique { input, pairs } => (
                input,
                MatchChunk {
                    build_rows: UInt64Array::new(pairs.build_rows.clone(), None),
                    probe_rows: UInt32Array::new(pairs.probe_rows.clone(), None),
                    newly_matched_probe_rows: pairs.probe_rows.len(),
                },
                true,
                None,
            ),
            ProbeBatch::Groups {
                input,
                cursor,
                last_probe_row_with_on_match,
            } => {
                let chunk = cursor.next_batch(
                    &build.data.map,
                    context.batch_size,
                    &mut self.pair_buffers,
                )?;
                (
                    input,
                    chunk,
                    cursor.is_finished(),
                    Some(last_probe_row_with_on_match),
                )
            }
        };
        let matched_probe_rows = chunk.newly_matched_probe_rows;
        let (build_indices, probe_indices) = (chunk.build_rows, chunk.probe_rows);
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

        // Only rows that satisfy the full ON condition suppress unmatched build
        // output. Rows rejected by the residual filter remain eligible for final output.
        build.record_matches(&build_indices);

        let (build_indices, probe_indices) = if context.kind.preserves_probe() {
            let last_joined_probe_row = probe_indices.values().last().copied();
            let start = last_on_match
                .as_ref()
                .and_then(|row| **row)
                .map_or(0, |row| row as usize + 1);
            let end = if exhausted {
                input.num_rows()
            } else {
                last_joined_probe_row.map_or(0, |row| row as usize + 1)
            };
            if let Some(last_on_match) = last_on_match
                && last_joined_probe_row.is_some()
            {
                *last_on_match = last_joined_probe_row;
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
        if exhausted {
            self.probe = None;
        }
        Ok(Some(output))
    }

    /// Release probe storage at EOF or the fetch limit, preserving buffered output.
    /// See [`BatchedJoinProcessor`] for the processing lifecycle.
    fn release_probe(&mut self) {
        self.probe = None;
        self.lookup_scratch = LookupScratch::new(&self.build.data.reservation);
        self.pair_buffers = PairBuffer::new(&self.build.data.reservation);
    }
}

/// Join semantics supported by this execution path. Existence and mark joins
/// use a separate path to avoid adding branches to regular probing.
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
