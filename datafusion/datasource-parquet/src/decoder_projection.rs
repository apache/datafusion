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

//! Decoder-projection construction for the parquet scan.
//!
//! [`DecoderProjection`] owns the two halves of "project a decoded parquet
//! batch onto the scan's output schema":
//!
//! * the [`ProjectionMask`] installed on the parquet decoder (and on any
//!   rebuild performed via `into_builder` at a row-group boundary), and
//! * the per-batch transform ([`DecoderProjection::map`]) that applies the
//!   projector and, when needed, rebuilds the batch with the user's
//!   `output_schema` to recover metadata / nullability the file schema does
//!   not carry.
//!
//! The opener constructs one [`DecoderProjection`] per file via
//! [`DecoderProjection::try_new`] and hands it to the push-decoder stream,
//! which calls [`map`](DecoderProjection::map) on every decoded batch.

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Array, BooleanArray, RecordBatch, RecordBatchOptions};
use arrow::compute::kernels::boolean::and;
use arrow::compute::kernels::filter::{filter_record_batch, prep_null_mask_filter};
use arrow::datatypes::SchemaRef;

use datafusion_common::cast::as_boolean_array;
use datafusion_common::instant::Instant;
use datafusion_common::{Result, internal_err};
use datafusion_physical_expr::filter_stats::{FilterCost, duration_nanos};
use datafusion_physical_expr::optional_filter_gate::GateDecision;
use datafusion_physical_expr::projection::{ProjectionExprs, Projector};
use datafusion_physical_expr::split_conjunction;
use datafusion_physical_expr::utils::{collect_columns, reassign_expr_columns};
use datafusion_physical_expr_adapter::replace_columns_with_literals;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use datafusion_physical_plan::metrics::{Count, Time};

use parquet::arrow::ProjectionMask;
use parquet::schema::types::{SchemaDescPtr, SchemaDescriptor};

use crate::ParquetFileMetrics;
use crate::filter_placement::{ConjunctStats, StageSelection};
use crate::opener::{VirtualColumnsState, append_fields};
use crate::projection_read_plan::build_projection_read_plan;
use crate::row_filter::SharedOptionalFilterGate;
use crate::row_filter_cost::evaluation_order;

/// Stream-schema column indices referenced by `projection`, in ascending
/// order, or `None` when the projection already reads every column of
/// `stream_schema` (so narrowing to them would be an identity).
///
/// `projection` must already be rebased onto `stream_schema`.
fn projector_input_indices(
    projection: &ProjectionExprs,
    stream_schema: &SchemaRef,
) -> Option<Vec<usize>> {
    let mut indices: Vec<usize> = projection
        .expr_iter()
        .flat_map(|expr| collect_columns(&expr))
        .map(|col| col.index())
        .collect();
    indices.sort_unstable();
    indices.dedup();

    // Nothing to drop: keep the batch (and the projector) as-is. This is the
    // no-post-scan-filter case, where the decoder mask is already exactly the
    // projection.
    (indices.len() < stream_schema.fields().len()).then_some(indices)
}

/// Compact the working batch to the surviving rows once a conjunct leaves at
/// most this fraction of them alive. Above the threshold the copy would not be
/// repaid by the (smaller) saving on the conjuncts that follow, so the masks are
/// just combined with a cheap bitwise `AND` instead.
///
/// The threshold is higher than the equivalent constant in `FilterExec`'s
/// adaptive evaluator because the post-scan filter's conjunct mix is skewed:
/// the conjuncts that land here at `pushdown_filters = false` are typically a
/// cheap static range predicate followed by a *much* more expensive dynamic
/// filter (a `CASE` over per-partition hash-table probes), so even a modest
/// reduction in the row count reaching the later conjunct pays for the copy.
const COMPACTION_SELECTIVITY_THRESHOLD: f64 = 0.8;

/// Outcome of running the post-scan predicate over one decoded batch.
///
/// The surviving rows are described rather than fully materialized so the
/// caller can drop filter-only columns (see [`DecoderProjection::narrow`])
/// before paying for the final filter kernel.
pub(crate) enum PostScanSelection {
    /// No row survived; nothing to hand downstream.
    Empty,
    /// Some rows survived. `batch` is the working batch — the input batch, or a
    /// compacted copy of it when the loop compacted at least once — and `mask`
    /// is the residual selection over `batch`'s rows (`None` when every row of
    /// `batch` survived).
    Rows {
        batch: RecordBatch,
        mask: Option<BooleanArray>,
    },
}

/// Predicate applied to decoded record batches inside the parquet scan.
///
/// Semantically identical to a `FilterExec` over the scan: rows where the
/// predicate is not `true` are dropped. `NULL` predicate results drop the row
/// (SQL `WHERE` semantics — `filter_record_batch` treats null mask entries as
/// false, same as `FilterExec`'s `batch_filter`).
///
/// # Compact-once evaluation
///
/// The predicate is kept split into its `AND` conjuncts and evaluated
/// sequentially, compacting the working batch to the survivors whenever a
/// conjunct proves selective enough. A fused `BinaryExpr` `AND` does *not*
/// compact between conjuncts, so every conjunct is evaluated on ~every decoded
/// row; with `pushdown_filters = false` the whole predicate lands here, which
/// means an expensive dynamic filter would run on all decoded rows even though
/// a cheap static conjunct ahead of it already rejected most of them. Compacting
/// is what the parquet `RowFilter` path gets for free from arrow-rs (each
/// conjunct is its own `ArrowPredicate`, applied against an accumulating
/// `RowSelection`); this loop is the post-scan equivalent.
///
/// Holds metric handles so per-batch rows-pruned / matched / time accumulate
/// into [`ParquetFileMetrics`] for `EXPLAIN ANALYZE`.
pub(crate) struct PostScanFilter {
    /// The `AND` conjuncts of the predicate, rebased onto the decoder's stream
    /// schema, each with the measurements of the adaptive filter placement
    /// (if the placement manages it). Never empty. They are evaluated in the
    /// order of [`Self::evaluation_order`].
    conjuncts: Vec<PostScanConjunct>,
    rows_pruned: Count,
    rows_matched: Count,
    eval_time: Time,
}

impl PostScanFilter {
    /// Evaluate the conjuncts against `batch` and describe the surviving rows.
    ///
    /// Takes the batch by value because a compaction replaces it; the caller
    /// gets the working batch back inside [`PostScanSelection::Rows`].
    ///
    /// Note the batch handed back is still on the *full* stream schema (the
    /// decoder mask widened for the predicate's columns): every conjunct may
    /// need those columns, so narrowing to the projector's inputs must not
    /// happen until the loop is done.
    pub(crate) fn evaluate(&self, batch: RecordBatch) -> Result<PostScanSelection> {
        // Scoped timer: stops on drop, so the early-return paths still record.
        let _timer = self.eval_time.timer();

        let input_rows = batch.num_rows();
        if input_rows == 0 {
            return Ok(PostScanSelection::Rows { batch, mask: None });
        }

        // `working` is what conjuncts are evaluated against; `acc` is the
        // accumulated (null-free) selection over `working`'s rows since the last
        // compaction, `None` meaning "all of them are still live".
        let mut working = batch;
        let mut acc: Option<BooleanArray> = None;
        // Maps the working rows to the rows of the input batch, for the
        // conjuncts that the adaptive filter placement measures.
        let mut stages = self
            .conjuncts
            .iter()
            .any(|conjunct| conjunct.stats.is_some())
            .then(|| StageSelection::new(input_rows));

        let last = self.conjuncts.len() - 1;
        for (position, index) in self.evaluation_order().into_iter().enumerate() {
            let PostScanConjunct {
                expr: conjunct,
                stats,
                gate,
            } = &self.conjuncts[index];
            let rows_in = working.num_rows();
            // An optional conjunct that its gate skips lets all rows pass.
            let mut gate = gate.as_ref().map(|gate| gate.lock());
            if let Some(gate) = gate.as_mut()
                && gate.begin_batch(rows_in) == GateDecision::Skip
            {
                continue;
            }
            let start = gate.as_ref().map(|gate| gate.now_nanos());
            let measure_start = stats.as_ref().map(|_| Instant::now());
            let array = conjunct.evaluate(&working)?.into_array(rows_in)?;
            let Ok(mask) = as_boolean_array(array.as_ref()) else {
                return internal_err!(
                    "post-scan filter predicate did not evaluate to a BooleanArray"
                );
            };
            // `filter_record_batch` treats a null mask entry as false, so a
            // surviving row is one that is `true` and non-null.
            let mask = match mask.nulls() {
                Some(_) => prep_null_mask_filter(mask),
                None => mask.clone(),
            };
            if let (Some(stages), Some(stats), Some(measure_start)) =
                (stages.as_ref(), stats, measure_start)
            {
                stages.record(stats, &mask, duration_nanos(measure_start.elapsed()));
            }
            if let (Some(gate), Some(start)) = (gate.as_mut(), start) {
                let elapsed = gate.now_nanos().saturating_sub(start);
                gate.record(rows_in, mask.true_count(), Duration::from_nanos(elapsed));
            }
            drop(gate);
            // An all-true conjunct leaves the accumulated selection untouched.
            if mask.true_count() == rows_in {
                continue;
            }

            let folded = match &acc {
                None => mask,
                Some(previous) => and(previous, &mask)?,
            };
            let alive = folded.true_count();
            if alive == 0 {
                self.record(input_rows, 0);
                return Ok(PostScanSelection::Empty);
            }

            // Compaction only benefits the conjuncts that come *after* this
            // one, so never compact on the last: its residual mask goes to the
            // caller, which narrows away the filter-only columns before
            // applying it.
            if position < last
                && (alive as f64) <= COMPACTION_SELECTIVITY_THRESHOLD * rows_in as f64
            {
                if let Some(stages) = stages.as_mut() {
                    stages.compact(&folded);
                }
                // The placement model measures the copy: a row filter does
                // not make it.
                let copy_start = stats.as_ref().map(|_| Instant::now());
                working = filter_record_batch(&working, &folded)?;
                if let (Some(stats), Some(copy_start)) = (stats, copy_start) {
                    stats.record_copy(duration_nanos(copy_start.elapsed()));
                }
                acc = None;
            } else {
                acc = Some(folded);
            }
        }

        let survivors = match &acc {
            Some(mask) => mask.true_count(),
            None => working.num_rows(),
        };
        self.record(input_rows, survivors);
        Ok(PostScanSelection::Rows {
            batch: working,
            mask: acc,
        })
    }

    /// The order to evaluate the conjuncts for the next batch, as indexes
    /// into `conjuncts`: by the pooled measurements of the conjuncts (rows
    /// removed for each nanosecond, see
    /// [`evaluation_order`]), the
    /// conjuncts without enough measurements first in their order. The
    /// post-scan filter can change its order at each batch for free, thus a
    /// file with few row groups does not keep the written order until its
    /// end (TPC-DS Q24: an expensive join filter ran before a cheap one that
    /// removes all rows).
    fn evaluation_order(&self) -> Vec<usize> {
        let costs: Vec<FilterCost> = self
            .conjuncts
            .iter()
            .map(|conjunct| {
                conjunct
                    .stats
                    .as_ref()
                    .map(|stats| stats.observation().cost())
                    .unwrap_or_default()
            })
            .collect();
        evaluation_order(&costs)
    }

    /// Record one batch's contribution to the rows-matched / rows-pruned
    /// metrics. `rows_pruned` stays "total rows in, minus final survivors"
    /// regardless of how many intermediate compactions the loop performed.
    fn record(&self, input_rows: usize, survivors: usize) {
        self.rows_matched.add(survivors);
        self.rows_pruned.add(input_rows - survivors);
    }
}

/// A conjunct for the post-scan filter, see [`DecoderProjection::try_new`].
#[derive(Debug, Clone)]
pub(crate) struct PostScanConjunct {
    /// The conjunct, in terms of the physical file schema.
    pub(crate) expr: Arc<dyn PhysicalExpr>,
    /// The measurements of the adaptive filter placement, if it manages the
    /// conjunct. The post-scan filter records each evaluation.
    pub(crate) stats: Option<Arc<ConjunctStats>>,
    /// The gate of an optional conjunct: the post-scan filter evaluates the
    /// conjunct only on the batches that the gate does not skip. `None` for
    /// a required conjunct.
    pub(crate) gate: Option<SharedOptionalFilterGate>,
}

impl From<Arc<dyn PhysicalExpr>> for PostScanConjunct {
    fn from(expr: Arc<dyn PhysicalExpr>) -> Self {
        Self {
            expr,
            stats: None,
            gate: None,
        }
    }
}

/// The inputs of [`DecoderProjection::try_new`] that do not change for a
/// file. The opener builds the first projection of a file with it, and the
/// stream builds a new projection with it when the adaptive filter
/// placement changes the post-scan conjuncts at a row group boundary.
#[derive(Clone)]
pub(crate) struct DecoderProjectionBuilder {
    pub(crate) projection: ProjectionExprs,
    /// Conjuncts that are always in the post-scan filter of the file: the
    /// required conjuncts that the `RowFilter` rejects, or all required
    /// conjuncts when `pushdown_filters` is false.
    pub(crate) fixed_post_scan: Vec<PostScanConjunct>,
    pub(crate) physical_file_schema: SchemaRef,
    pub(crate) parquet_schema: SchemaDescPtr,
    pub(crate) output_schema: SchemaRef,
    pub(crate) virtual_state: Option<Arc<VirtualColumnsState>>,
    pub(crate) file_metrics: ParquetFileMetrics,
}

impl DecoderProjectionBuilder {
    /// The decoder projection with `placed` (the conjuncts that the adaptive
    /// filter placement puts in the post-scan filter) and
    /// [`Self::fixed_post_scan`] in the post-scan filter.
    pub(crate) fn build(&self, placed: &[PostScanConjunct]) -> Result<DecoderProjection> {
        let post_scan_conjuncts: Vec<PostScanConjunct> = placed
            .iter()
            .chain(&self.fixed_post_scan)
            .cloned()
            .collect();
        DecoderProjection::try_new(
            &self.projection,
            &post_scan_conjuncts,
            &self.physical_file_schema,
            &self.parquet_schema,
            &self.output_schema,
            self.virtual_state.as_deref(),
            &self.file_metrics,
        )
    }
}

/// Per-file decoder projection: the [`ProjectionMask`] installed on the
/// parquet decoder, plus the per-batch transform that maps the decoder's
/// output onto the scan's `output_schema`.
///
/// Built once per file by the opener via [`Self::try_new`]; the
/// push-decoder stream installs [`Self::projection_mask`] on the decoder
/// (and on any rebuild performed via `into_builder` at a row-group
/// boundary) and calls [`Self::map`] on every decoded batch.
pub(crate) struct DecoderProjection {
    projection_mask: ProjectionMask,
    projector: Projector,
    output_schema: SchemaRef,
    /// `true` when the projector's output schema differs from `output_schema`
    /// in metadata / nullability and [`map`](Self::map) must rebuild the batch
    /// with `output_schema`.
    replace_schema: bool,
    /// Predicate to apply on each decoded batch, after any row-level
    /// `RowFilter` and before the projector. Carries conjuncts the `RowFilter`
    /// machinery could not evaluate, plus the whole predicate when
    /// `pushdown_filters = false`. `None` when no conjunct needs post-scan
    /// evaluation, in which case the decoder mask covers exactly the user
    /// projection and there is no extra per-batch work.
    post_scan_filter: Option<PostScanFilter>,
    /// Stream-schema column indices the [`Projector`] reads, used by
    /// [`narrow`](Self::narrow) to drop filter-only columns before the filter
    /// kernel runs. `None` when the projector already reads every stream
    /// column — always the case without a post-scan filter, where the decoder
    /// mask covers exactly the projection and narrowing would be an identity.
    narrow_indices: Option<Vec<usize>>,
    /// Schema of a narrowed, filtered batch: what [`map`](Self::map) consumes,
    /// and the schema any coalescer buffering these batches must be built with.
    /// Equals the full stream schema when [`Self::narrow_indices`] is `None`.
    filtered_schema: SchemaRef,
}

impl DecoderProjection {
    /// Build the decoder projection for a file.
    ///
    /// `projection` references columns in `physical_file_schema` (i.e. already
    /// adapted by the per-file expr adapter); `parquet_schema` is the
    /// corresponding parquet [`SchemaDescriptor`]. `output_schema` is what
    /// consumers of the scan stream expect.
    ///
    /// `virtual_state`, when present, describes virtual columns the reader
    /// will append to each decoded batch (e.g. parquet `row_number`). Virtual
    /// columns are stripped from the projection fed into
    /// `build_projection_read_plan` (which only understands file columns) and
    /// appended to the stream schema so the projector can resolve them.
    ///
    /// `post_scan_conjuncts` are predicate conjuncts that must be evaluated on
    /// decoded batches inside the scan (conjuncts the parquet `RowFilter`
    /// machinery could not place, plus the whole predicate when
    /// `pushdown_filters = false`). They must reference columns in
    /// `physical_file_schema` (virtual-column predicates are never pushed into
    /// the scan). When non-empty the decoder mask is widened to include their
    /// columns, the conjuncts are rebased onto the (widened) stream schema, and
    /// they become [`Self::post_scan_filter`] — kept split so it can compact
    /// between them. When empty this is exactly the prior projection-only
    /// behaviour.
    pub(crate) fn try_new(
        projection: &ProjectionExprs,
        post_scan_conjuncts: &[PostScanConjunct],
        physical_file_schema: &SchemaRef,
        parquet_schema: &SchemaDescriptor,
        output_schema: &SchemaRef,
        virtual_state: Option<&VirtualColumnsState>,
        file_metrics: &ParquetFileMetrics,
    ) -> Result<Self> {
        // Virtual columns are produced by the reader separately from the
        // projection mask, so strip them from the expressions we feed into
        // `build_projection_read_plan`. We substitute each virtual column
        // reference with a null literal; that leaves the remaining Column
        // refs (into `physical_file_schema`) intact for
        // `ProjectionMask::roots`, which only understands file columns.
        let projection_for_read_plan = match virtual_state {
            None => projection.clone(),
            Some(state) => projection.clone().try_map_exprs(|expr| {
                replace_columns_with_literals(expr, state.null_replacements())
            })?,
        };
        // Decoder reads (user projection ∪ post-scan filter columns). Row-level
        // filter columns live inside the parquet RowFilter's per-predicate
        // masks, so they don't need to be in this read plan.
        //
        // A post-scan conjunct may reference a virtual column (e.g. parquet
        // `row_number`): the reader produces those separately, so — like the
        // projection — strip them to null literals before feeding the read
        // plan, which only understands file columns. The *original* conjuncts
        // (with the virtual references intact) are still used below to build
        // the post-scan predicate, which is rebased onto the stream schema
        // where the reader has appended the virtual columns.
        let post_scan_for_read_plan: Vec<Arc<dyn PhysicalExpr>> = match virtual_state {
            None => post_scan_conjuncts
                .iter()
                .map(|conjunct| Arc::clone(&conjunct.expr))
                .collect(),
            Some(state) => post_scan_conjuncts
                .iter()
                .map(|conjunct| {
                    replace_columns_with_literals(
                        Arc::clone(&conjunct.expr),
                        state.null_replacements(),
                    )
                })
                .collect::<Result<Vec<_>>>()?,
        };
        let read_plan = build_projection_read_plan(
            projection_for_read_plan
                .expr_iter()
                .chain(post_scan_for_read_plan.iter().map(Arc::clone)),
            physical_file_schema,
            parquet_schema,
        );

        // The reader produces projected file columns followed by any virtual
        // columns (`ArrowReaderOptions::with_virtual_columns` appends them to
        // each decoded batch).
        let stream_schema = match virtual_state {
            Some(state) => {
                append_fields(&read_plan.projected_schema, state.virtual_columns())
            }
            None => Arc::clone(&read_plan.projected_schema),
        };

        // Rebase the projection onto the decoder's stream schema (column
        // indices change because the decoder yields only the masked columns).
        let rebased_projection = projection
            .clone()
            .try_map_exprs(|expr| reassign_expr_columns(expr, &stream_schema))?;

        // When the mask was widened for post-scan filter columns, the stream
        // batch carries columns the projector never reads. Filtering those
        // through the (expensive) filter kernel only to drop them afterwards
        // is pure waste, so narrow the batch to the projector's inputs first
        // and rebase the projector onto that narrower schema — the same
        // project-then-filter order `FilterExec::filter_and_project` uses.
        let narrow_indices = projector_input_indices(&rebased_projection, &stream_schema);
        let (projector, narrow_indices, filtered_schema) = match narrow_indices {
            Some(indices) => {
                let narrowed = Arc::new(stream_schema.project(&indices)?);
                let renarrowed = rebased_projection
                    .clone()
                    .try_map_exprs(|expr| reassign_expr_columns(expr, &narrowed))?;
                let projector = renarrowed.make_projector(&narrowed)?;
                (projector, Some(indices), narrowed)
            }
            None => (
                rebased_projection.make_projector(&stream_schema)?,
                None,
                Arc::clone(&stream_schema),
            ),
        };

        // Compare against the projector's *output* schema rather than the
        // (possibly widened) stream schema, so widening the mask for post-scan
        // filter columns does not flip this flag.
        let replace_schema = projector.output_schema() != output_schema;

        // Rebase the post-scan conjuncts onto the same (widened) stream schema
        // and conjoin them into a single predicate for per-batch evaluation.
        let post_scan_filter = if post_scan_conjuncts.is_empty() {
            None
        } else {
            // Split each conjunct again after rebasing: the caller's list is
            // already conjunct-wise, but a single entry may still be a nested
            // `AND` (e.g. a `RowFilter`-rejected conjunct). The compact-once
            // loop can only compact between the pieces it can see.
            let mut rebased = vec![];
            for conjunct in post_scan_conjuncts {
                let expr =
                    reassign_expr_columns(Arc::clone(&conjunct.expr), &stream_schema)?;
                // The gate of an optional conjunct decides for the whole
                // conjunct, thus it is not split.
                if conjunct.gate.is_some() {
                    rebased.push(PostScanConjunct {
                        expr,
                        ..conjunct.clone()
                    });
                    continue;
                }
                let pieces = split_conjunction(&expr);
                // The measurements belong to the whole conjunct. The adaptive
                // filter placement only manages conjuncts of the root `AND`
                // chain, which are never split again.
                let stats = if pieces.len() == 1 {
                    conjunct.stats.clone()
                } else {
                    None
                };
                rebased.extend(pieces.into_iter().map(|piece| PostScanConjunct {
                    expr: Arc::clone(piece),
                    stats: stats.clone(),
                    gate: None,
                }));
            }
            Some(PostScanFilter {
                conjuncts: rebased,
                rows_pruned: file_metrics.post_scan_rows_pruned.clone(),
                rows_matched: file_metrics.post_scan_rows_matched.clone(),
                eval_time: file_metrics.post_scan_filter_eval_time.clone(),
            })
        };

        Ok(Self {
            projection_mask: read_plan.projection_mask,
            projector,
            output_schema: Arc::clone(output_schema),
            replace_schema,
            post_scan_filter,
            narrow_indices,
            filtered_schema,
        })
    }

    /// The projection mask to install on every parquet decoder in the scan.
    pub(crate) fn projection_mask(&self) -> &ProjectionMask {
        &self.projection_mask
    }

    /// The post-scan filter for this file, if any conjunct needs per-batch
    /// evaluation. Applied by the push-decoder stream to each decoded batch
    /// (after any row-level `RowFilter`, before the projector).
    pub(crate) fn post_scan_filter(&self) -> Option<&PostScanFilter> {
        self.post_scan_filter.as_ref()
    }

    /// Drop the columns only the post-scan filter needed, leaving just what
    /// the [`Projector`] reads.
    ///
    /// Call this *after* [`PostScanFilter::evaluate`] (every conjunct may need
    /// the filter-only columns) but *before* applying the residual mask it
    /// returned: `RecordBatch::project` is a cheap `Arc` reslice, so narrowing
    /// first keeps the final filter kernel off columns that would be discarded
    /// immediately afterwards. Returns the batch unchanged when the projector
    /// already reads every stream column.
    pub(crate) fn narrow(&self, batch: RecordBatch) -> Result<RecordBatch> {
        match &self.narrow_indices {
            Some(indices) => Ok(batch.project(indices)?),
            None => Ok(batch),
        }
    }

    /// Schema of the batches [`map`](Self::map) consumes, i.e. what
    /// [`narrow`](Self::narrow) produces. Used to build the coalescer that
    /// reassembles post-filter batches back to the target batch size.
    pub(crate) fn filtered_schema(&self) -> &SchemaRef {
        &self.filtered_schema
    }

    /// Whether this file has a post-scan filter. Used by the opener to decide
    /// whether a decoder-local LIMIT is safe (it is not, because the filter
    /// can reject rows after the decoder counts them).
    pub(crate) fn has_post_scan_filter(&self) -> bool {
        self.post_scan_filter.is_some()
    }

    /// Map a decoded batch onto the scan's output schema.
    ///
    /// Applies the [`Projector`] and, when the projector's output schema
    /// differs from `output_schema` in metadata or nullability, rebuilds the
    /// batch with `output_schema` (some writers emit OPTIONAL fields even when
    /// the data has no nulls; some logical schemas carry field-level metadata
    /// the file schema does not).
    pub(crate) fn map(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        let projected = self.projector.project_batch(batch)?;
        if !self.replace_schema {
            return Ok(projected);
        }
        let (_stream_schema, arrays, num_rows) = projected.into_parts();
        let options = RecordBatchOptions::new().with_row_count(Some(num_rows));
        Ok(RecordBatch::try_new_with_options(
            Arc::clone(&self.output_schema),
            arrays,
            &options,
        )?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::ScalarValue;
    use datafusion_expr::Operator;
    use datafusion_physical_expr::expressions::{BinaryExpr, Column, Literal};

    fn batch(values: Vec<Option<i32>>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
        ]));
        let a = Int32Array::from(values.clone());
        // `b` mirrors `a` so both conjuncts can be expressed over real data
        // while still exercising two separate columns.
        let b = Int32Array::from(values);
        RecordBatch::try_new(schema, vec![Arc::new(a), Arc::new(b)]).unwrap()
    }

    fn gt(column: &str, index: usize, value: i32) -> Arc<dyn PhysicalExpr> {
        Arc::new(BinaryExpr::new(
            Arc::new(Column::new(column, index)),
            Operator::Gt,
            Arc::new(Literal::new(ScalarValue::Int32(Some(value)))),
        ))
    }

    fn filter(conjuncts: Vec<Arc<dyn PhysicalExpr>>) -> PostScanFilter {
        PostScanFilter {
            conjuncts: conjuncts.into_iter().map(PostScanConjunct::from).collect(),
            rows_pruned: Count::new(),
            rows_matched: Count::new(),
            eval_time: Time::new(),
        }
    }

    /// Apply the selection the way the push-decoder does, so the test asserts
    /// on the rows that actually reach the coalescer.
    fn survivors(filter: &PostScanFilter, input: RecordBatch) -> Vec<Option<i32>> {
        match filter.evaluate(input).unwrap() {
            PostScanSelection::Empty => Vec::new(),
            PostScanSelection::Rows { batch, mask } => {
                let batch = match mask {
                    Some(mask) => filter_record_batch(&batch, &mask).unwrap(),
                    None => batch,
                };
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .iter()
                    .collect()
            }
        }
    }

    /// A conjunct after a compaction is measured in the positions of the
    /// input batch, not of the compacted batch. The rows that the compaction
    /// removed count as passing.
    #[test]
    fn measures_skippable_rows_on_input_positions() {
        // (a % 4) = 0: keeps every fourth row, no empty window. The loop then
        // compacts the batch to 64 rows.
        let every_fourth: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(BinaryExpr::new(
                Arc::new(Column::new("a", 0)),
                Operator::Modulo,
                Arc::new(Literal::new(ScalarValue::Int32(Some(4)))),
            )),
            Operator::Eq,
            Arc::new(Literal::new(ScalarValue::Int32(Some(0)))),
        ));
        // b > 127: removes the first 32 rows of the compacted batch, that is
        // the live rows of the input rows 0..128. In the compacted batch this
        // is half of one window of 64 rows.
        let upper_half = gt("b", 1, 127);
        let first = Arc::new(ConjunctStats::default());
        let second = Arc::new(ConjunctStats::default());
        let filter = PostScanFilter {
            conjuncts: vec![
                PostScanConjunct {
                    expr: every_fourth,
                    stats: Some(Arc::clone(&first)),
                    gate: None,
                },
                PostScanConjunct {
                    expr: upper_half,
                    stats: Some(Arc::clone(&second)),
                    gate: None,
                },
            ],
            rows_pruned: Count::new(),
            rows_matched: Count::new(),
            eval_time: Time::new(),
        };
        let rows = survivors(&filter, batch((0..256).map(Some).collect()));
        assert_eq!(rows, (128..256).step_by(4).map(Some).collect::<Vec<_>>());

        let first = first.observation();
        assert_eq!(
            (first.rows_in, first.rows_out, first.skippable_rows),
            (256, 64, 0)
        );
        // The first conjunct keeps 25% of the rows: the loop copies the
        // working batch after it, and measures the copy for it.
        assert_eq!(first.post_scan_rows, 256);
        assert!(first.copy_nanos > 0);
        // The rows that the first conjunct removed pass: no empty window.
        let second = second.observation();
        assert_eq!(
            (second.rows_in, second.rows_out, second.skippable_rows),
            (256, 256 - 32, 0)
        );
    }

    /// A gated optional conjunct sees only the rows that the conjuncts
    /// before it let pass, and a paused gate skips it.
    #[test]
    fn gated_conjunct_sees_rows_after_earlier_conjuncts() {
        use crate::metrics::OptionalFilterMetrics;
        use crate::row_filter::OptionalFilterGateState;
        use datafusion_physical_expr::filter_stats::ManualClock;
        use datafusion_physical_expr::optional_filter_gate::{
            OptionalFilterGate, OptionalFilterGateConfig,
        };
        use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;

        // The first conjunct keeps the rows with `a > 99`. The optional
        // conjunct `b > 99` removes the same rows: after the first conjunct
        // it removes nothing, thus its gate pauses it after its first window
        // of 2 batches.
        let optional = gt("b", 1, 99);
        let gate = OptionalFilterGateState::shared(
            OptionalFilterGate::new(
                Arc::clone(&optional),
                OptionalFilterGateConfig::default(),
            )
            .with_clock(Arc::new(ManualClock::new())),
            OptionalFilterMetrics::new(&ExecutionPlanMetricsSet::new(), 0, "file"),
        );
        let filter = PostScanFilter {
            conjuncts: vec![
                PostScanConjunct::from(gt("a", 0, 99)),
                PostScanConjunct {
                    expr: optional,
                    stats: None,
                    gate: Some(Arc::clone(&gate)),
                },
            ],
            rows_pruned: Count::new(),
            rows_matched: Count::new(),
            eval_time: Time::new(),
        };
        // Batches where half of the rows reach the optional conjunct: a
        // window of 2 batches has more than `MIN_OBSERVED_ROWS` rows.
        let rows = 82 * 200;
        let input = || batch((0..rows).map(|i| Some(i % 200)).collect());
        for _ in 0..2 {
            assert_eq!(survivors(&filter, input()).len(), rows as usize / 2);
        }
        assert!(gate.lock().is_paused());
        // The paused gate skips the optional conjunct: same result.
        assert_eq!(survivors(&filter, input()).len(), rows as usize / 2);
    }

    /// The conjuncts run in the order of their pooled measurements: the
    /// conjunct that removes more rows for each nanosecond first. The
    /// result does not change.
    #[test]
    fn conjuncts_run_in_measured_order() {
        let rows = 16 * 8192;
        let expensive = Arc::new(ConjunctStats::default());
        let cheap = Arc::new(ConjunctStats::default());
        let filter = PostScanFilter {
            conjuncts: vec![
                PostScanConjunct {
                    expr: gt("a", 0, 49),
                    stats: Some(Arc::clone(&expensive)),
                    gate: None,
                },
                PostScanConjunct {
                    expr: gt("b", 1, 149),
                    stats: Some(Arc::clone(&cheap)),
                    gate: None,
                },
            ],
            rows_pruned: Count::new(),
            rows_matched: Count::new(),
            eval_time: Time::new(),
        };
        // Unmeasured: the written order.
        assert_eq!(filter.evaluation_order(), vec![0, 1]);
        // The second conjunct removes more rows for each nanosecond.
        expensive.record(rows, rows / 2, 0, 20 * rows as u64);
        cheap.record(rows, rows / 4, 0, rows as u64);
        assert_eq!(filter.evaluation_order(), vec![1, 0]);
        let input = batch((0..200).map(Some).collect());
        assert_eq!(
            survivors(&filter, input),
            (150..200).map(Some).collect::<Vec<_>>()
        );
    }

    #[test]
    fn compacts_between_conjuncts_without_losing_rows() {
        // 100 rows; `a > 79` keeps 20 (20% — below the compaction threshold, so
        // the loop compacts), then `b > 89` keeps 10 of those.
        let input = batch((0..100).map(Some).collect());
        let f = filter(vec![gt("a", 0, 79), gt("b", 1, 89)]);
        assert_eq!(
            survivors(&f, input),
            (90..100).map(Some).collect::<Vec<_>>()
        );
        assert_eq!(f.rows_matched.value(), 10);
        assert_eq!(f.rows_pruned.value(), 90);
    }

    #[test]
    fn conjunct_order_does_not_change_the_result() {
        let a = filter(vec![gt("a", 0, 79), gt("b", 1, 89)]);
        let b = filter(vec![gt("b", 1, 89), gt("a", 0, 79)]);
        let expected = (90..100).map(Some).collect::<Vec<_>>();
        assert_eq!(survivors(&a, batch((0..100).map(Some).collect())), expected);
        assert_eq!(survivors(&b, batch((0..100).map(Some).collect())), expected);
    }

    /// A `NULL` predicate result drops the row, matching `filter_record_batch`
    /// and `FilterExec`. The null must be dropped even when it is the *first*
    /// conjunct that produced it and a later conjunct would have said `true`.
    #[test]
    fn null_predicate_results_drop_the_row() {
        let values: Vec<Option<i32>> = (0..100)
            .map(|i| if i == 95 { None } else { Some(i) })
            .collect();
        let f = filter(vec![gt("a", 0, 79), gt("b", 1, 89)]);
        let expected: Vec<Option<i32>> =
            (90..100).filter(|i| *i != 95).map(Some).collect();
        assert_eq!(survivors(&f, batch(values)), expected);
        assert_eq!(f.rows_matched.value(), 9);
        assert_eq!(f.rows_pruned.value(), 91);
    }

    #[test]
    fn all_rows_rejected_reports_empty() {
        let input = batch((0..100).map(Some).collect());
        let f = filter(vec![gt("a", 0, 500), gt("b", 1, 0)]);
        assert!(matches!(
            f.evaluate(input).unwrap(),
            PostScanSelection::Empty
        ));
        assert_eq!(f.rows_matched.value(), 0);
        assert_eq!(f.rows_pruned.value(), 100);
    }

    #[test]
    fn all_rows_kept_needs_no_mask() {
        let input = batch((0..100).map(Some).collect());
        let f = filter(vec![gt("a", 0, -1), gt("b", 1, -1)]);
        match f.evaluate(input).unwrap() {
            PostScanSelection::Rows { batch, mask } => {
                assert!(mask.is_none(), "an all-true predicate needs no mask");
                assert_eq!(batch.num_rows(), 100);
            }
            PostScanSelection::Empty => panic!("expected every row to survive"),
        }
        assert_eq!(f.rows_matched.value(), 100);
        assert_eq!(f.rows_pruned.value(), 0);
    }

    #[test]
    fn empty_batch_is_passed_through() {
        let input = batch(Vec::new());
        let f = filter(vec![gt("a", 0, 79)]);
        assert_eq!(survivors(&f, input), Vec::<Option<i32>>::new());
        assert_eq!(f.rows_matched.value(), 0);
        assert_eq!(f.rows_pruned.value(), 0);
    }
}
