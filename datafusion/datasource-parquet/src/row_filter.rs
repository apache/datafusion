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

//! Utilities to push down of DataFusion filter predicates (any DataFusion
//! `PhysicalExpr` that evaluates to a [`BooleanArray`]) to the parquet decoder
//! level in `arrow-rs`.
//!
//! DataFusion will use a `ParquetRecordBatchStream` to read data from parquet
//! into [`RecordBatch`]es.
//!
//! The `ParquetRecordBatchStream` takes an optional `RowFilter` which is itself
//! a Vec of `Box<dyn ArrowPredicate>`. During decoding, the predicates are
//! evaluated in order, to generate a mask which is used to avoid decoding rows
//! in projected columns which do not pass the filter which can significantly
//! reduce the amount of compute required for decoding and thus improve query
//! performance.
//!
//! Since the predicates are applied serially in the order defined in the
//! `RowFilter`, the optimal ordering depends on the exact filters. The best
//! filters to execute first have two properties:
//!
//! 1. They are relatively inexpensive to evaluate (e.g. they read
//!    column chunks which are relatively small)
//!
//! 2. They filter many (contiguous) rows, reducing the amount of decoding
//!    required for subsequent filters and projected columns
//!
//! If requested, this code will reorder the filters based on heuristics try and
//! reduce the evaluation cost.
//!
//! The basic algorithm for constructing the `RowFilter` is as follows
//!
//! 1. Break conjunctions into separate predicates. An expression
//!    like `a = 1 AND (b = 2 AND c = 3)` would be
//!    separated into the expressions `a = 1`, `b = 2`, and `c = 3`.
//! 2. Determine whether each predicate can be evaluated as an `ArrowPredicate`.
//! 3. Determine, for each predicate, the total compressed size of all
//!    columns required to evaluate the predicate.
//! 4. Re-order predicates by total size (from step 3).
//! 5. "Compile" each predicate `Expr` to a `DatafusionArrowPredicate`.
//! 6. Build the `RowFilter` from the ordered predicates.
//!
//! List-aware predicates (for example, `array_has`, `array_has_all`, and
//! `array_has_any`) can be evaluated directly during Parquet decoding.
//! Struct field access via `get_field` is also supported when the accessed
//! leaf is a primitive type. Filters that reference entire struct columns
//! rather than individual fields cannot be pushed down and are instead
//! evaluated after the full batches are materialized.
//!
//! For example, given a struct column `s {name: Utf8, value: Int32}`:
//! - `WHERE s['value'] > 5` — pushed down (accesses a primitive leaf)
//! - `WHERE s IS NOT NULL`  — not pushed down (references the whole struct)

use std::sync::Arc;
use std::time::Duration;

use arrow::array::BooleanArray;
use arrow::buffer::BooleanBuffer;
use arrow::datatypes::{Schema, SchemaRef};
use arrow::error::{ArrowError, Result as ArrowResult};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{ArrowPredicate, RowFilter};
use parquet::file::metadata::ParquetMetaData;

use datafusion_common::Result;
use datafusion_common::cast::as_boolean_array;
use datafusion_common::config::OptionalFilterMode;
use datafusion_common::tree_node::TreeNode;
use datafusion_physical_expr::expressions::OptionalFilterPhysicalExpr;
use datafusion_physical_expr::optional_filter_gate::{GateDecision, OptionalFilterGate};
use datafusion_physical_expr::utils::{is_optional_filter, reassign_expr_columns};
use datafusion_physical_expr::{PhysicalExpr, split_conjunction};

use datafusion_physical_plan::metrics::{self, ExecutionPlanMetricsSet};
use parking_lot::Mutex;

use super::ParquetFileMetrics;
use super::supported_predicates::supports_list_predicates;
use crate::filter_placement::ConjunctStats;
use crate::metrics::OptionalFilterMetrics;
use crate::optional_filter::{
    OptionalFilterOptions, OptionalFilterSaving, compressed_bytes_per_row,
};
use crate::projection_read_plan::{
    ParquetReadPlan, PushdownChecker, PushdownColumns, assemble_read_plan,
    build_read_plan_with_cast_clipping,
};

/// A "compiled" predicate passed to `ParquetRecordBatchStream` to perform
/// row-level filtering during parquet decoding.
///
/// See the module level documentation for more information.
///
/// Implements the `ArrowPredicate` trait used by the parquet decoder
///
/// An expression can be evaluated as a `DatafusionArrowPredicate` if it:
/// * Does not reference any projected columns
/// * References either primitive columns or list columns used by
///   supported predicates (such as `array_has_all` or NULL checks).
/// * References struct fields via `get_field` where the accessed leaf
///   is a primitive type (e.g. `get_field(struct_col, 'field') > 5`).
///   Direct references to whole struct columns are still evaluated after
///   decoding.
#[derive(Debug)]
pub(crate) struct DatafusionArrowPredicate {
    /// the filter expression
    physical_expr: Arc<dyn PhysicalExpr>,
    /// Path to the leaf columns in the parquet schema required to evaluate the
    /// expression
    projection_mask: ProjectionMask,
    /// how many rows were filtered out by this predicate
    rows_pruned: metrics::Count,
    /// how many rows passed this predicate
    rows_matched: metrics::Count,
    /// how long was spent evaluating this predicate
    time: metrics::Time,
    /// The gate of an optional filter in [`OptionalFilterMode::Adaptive`].
    /// `None` for all other filters.
    gate: Option<SharedOptionalFilterGate>,
    /// The measurements of the conjunct for the adaptive filter placement
    /// (see [`crate::filter_placement`]). `None` if the placement does not
    /// manage the conjunct.
    placement_stats: Option<Arc<ConjunctStats>>,
}

/// The [`OptionalFilterGate`] of one optional filter in one file, with its
/// metrics.
///
/// A file can build more than one `RowFilter` (see
/// [`RowFilterContext::build_row_filter`](crate::push_decoder::RowFilterContext)),
/// but only one is in use at a time. All of them share this state, thus the
/// gate keeps its state across row groups.
#[derive(Debug)]
pub(crate) struct OptionalFilterGateState {
    gate: OptionalFilterGate,
    metrics: OptionalFilterMetrics,
    /// Value of [`OptionalFilterGate::pauses`] already added to `metrics`.
    reported_pauses: usize,
}

pub(crate) type SharedOptionalFilterGate = Arc<Mutex<OptionalFilterGateState>>;

impl OptionalFilterGateState {
    fn begin_batch(&mut self, num_rows: usize) -> GateDecision {
        let decision = self.gate.begin_batch();
        if decision == GateDecision::Skip {
            self.metrics.add_rows_skipped(num_rows);
        }
        self.report_pauses();
        decision
    }

    fn record(&mut self, rows_in: usize, rows_out: usize, elapsed: Duration) {
        self.gate.record(rows_in, rows_out, elapsed);
        self.metrics.add_eval_time(elapsed);
        self.report_pauses();
    }

    /// True if the gate skips the next batch.
    pub(crate) fn is_paused(&self) -> bool {
        self.gate.is_paused()
    }

    /// Counts down up to `batches` batches of `rows_per_batch` rows that the
    /// scan did not evaluate the filter on, while the gate is paused. The
    /// adaptive filter placement calls this when it removed the paused
    /// filter from the `RowFilter` (see [`crate::filter_placement`]). Stops
    /// when the gate stops the pause, for example because the filter
    /// changed.
    pub(crate) fn skip_batches(&mut self, batches: usize, rows_per_batch: usize) {
        for _ in 0..batches {
            if !self.gate.is_paused() {
                break;
            }
            self.begin_batch(rows_per_batch);
        }
    }

    fn report_pauses(&mut self) {
        let pauses = self.gate.pauses();
        self.metrics
            .add_pauses(pauses.saturating_sub(self.reported_pauses));
        self.reported_pauses = pauses;
    }
}

impl DatafusionArrowPredicate {
    /// Create a new `DatafusionArrowPredicate` from a `FilterCandidate`.
    ///
    /// Production code goes through [`prebuild_row_filter_candidates`] +
    /// [`row_filter_from_prebuilt`]; this constructor remains as a test
    /// convenience for exercising a single candidate.
    #[cfg(test)]
    pub fn try_new(
        candidate: FilterCandidate,
        rows_pruned: metrics::Count,
        rows_matched: metrics::Count,
        time: metrics::Time,
    ) -> Result<Self> {
        let physical_expr =
            reassign_expr_columns(candidate.expr, &candidate.read_plan.projected_schema)?;

        Ok(Self {
            physical_expr,
            projection_mask: candidate.read_plan.projection_mask,
            rows_pruned,
            rows_matched,
            time,
            gate: None,
            placement_stats: None,
        })
    }
}

impl ArrowPredicate for DatafusionArrowPredicate {
    fn projection(&self) -> &ProjectionMask {
        &self.projection_mask
    }

    fn evaluate(&mut self, batch: RecordBatch) -> ArrowResult<BooleanArray> {
        // scoped timer updates on drop
        let mut timer = self.time.timer();
        let num_rows = batch.num_rows();

        // An optional filter that the gate skips lets all rows pass.
        let mut gate = self.gate.as_ref().map(|gate| gate.lock());
        if let Some(gate) = gate.as_mut()
            && gate.begin_batch(num_rows) == GateDecision::Skip
        {
            self.rows_matched.add(num_rows);
            timer.stop();
            return Ok(BooleanArray::new(BooleanBuffer::new_set(num_rows), None));
        }
        // The gate measures the evaluation time with its clock.
        let start = gate.as_ref().map(|gate| gate.gate.clock().now_nanos());

        self.physical_expr
            .evaluate(&batch)
            .and_then(|v| v.into_array(num_rows))
            .and_then(|array| {
                let bool_arr = as_boolean_array(&array)?.clone();
                let num_matched = bool_arr.true_count();
                let num_pruned = bool_arr.len() - num_matched;
                self.rows_pruned.add(num_pruned);
                self.rows_matched.add(num_matched);
                if let Some(stats) = &self.placement_stats {
                    stats.record_evaluation(&bool_arr);
                }
                if let (Some(gate), Some(start)) = (gate.as_mut(), start) {
                    let elapsed = gate.gate.clock().now_nanos().saturating_sub(start);
                    gate.record(num_rows, num_matched, Duration::from_nanos(elapsed));
                }
                timer.stop();
                Ok(bool_arr)
            })
            // `ExternalError` is the only `ArrowError` variant that keeps a
            // source, and therefore the only one that leaves the original error
            // recoverable (see `DataFusionError::find_root`)
            .map_err(|e| {
                ArrowError::ExternalError(Box::new(
                    e.context("Error evaluating filter predicate"),
                ))
            })
    }
}

/// A candidate expression for creating a `RowFilter`.
///
/// Each candidate contains the expression as well as data to estimate the cost
/// of evaluating the resulting expression.
///
/// See the module level documentation for more information.
pub(crate) struct FilterCandidate {
    expr: Arc<dyn PhysicalExpr>,
    /// Estimate for the total number of bytes that will need to be processed
    /// to evaluate this filter. This is used to estimate the cost of evaluating
    /// the filter and to order the filters when `reorder_predicates` is true.
    /// This is generated by summing the compressed size of all columns that the filter references.
    required_bytes: usize,
    /// The resolved Parquet read plan (leaf indices + projected schema).
    read_plan: ParquetReadPlan,
}

/// Helper to build a `FilterCandidate`.
///
/// This will do several things:
/// 1. Determine the columns required to evaluate the expression
/// 2. Calculate data required to estimate the cost of evaluating the filter
///
/// Note: This does *not* handle any adaptation of the expression to the file schema.
/// The expression must already be adapted before being passed in here, generally using
/// [`PhysicalExprAdapter`](datafusion_physical_expr_adapter::PhysicalExprAdapter).
struct FilterCandidateBuilder {
    expr: Arc<dyn PhysicalExpr>,
    /// The Arrow schema of this parquet file (the result of converting the
    /// parquet schema to Arrow, potentially with type coercions applied).
    file_schema: SchemaRef,
}

impl FilterCandidateBuilder {
    pub fn new(expr: Arc<dyn PhysicalExpr>, file_schema: Arc<Schema>) -> Self {
        Self { expr, file_schema }
    }

    /// Attempt to build a `FilterCandidate` from the expression
    ///
    /// # Return values
    ///
    /// * `Ok(Some(candidate))` if the expression can be used as an ArrowFilter
    /// * `Ok(None)` if the expression cannot be used as an ArrowFilter
    /// * `Err(e)` if an error occurs while building the candidate
    pub fn build(self, metadata: &ParquetMetaData) -> Result<Option<FilterCandidate>> {
        Ok(
            build_parquet_read_plan(&self.expr, &self.file_schema, metadata)?.map(
                |(read_plan, required_bytes)| FilterCandidate {
                    expr: self.expr,
                    required_bytes,
                    read_plan,
                },
            ),
        )
    }
}

/// Checks if a given expression can be pushed down to the parquet decoder.
///
/// Returns `Some(PushdownColumns)` if the expression can be pushed down,
/// where the struct contains the indices into the file schema of all columns
/// required to evaluate the expression.
///
/// Returns `None` if the expression cannot be pushed down (e.g., references
/// unsupported nested types or columns not in the file).
/// Struct casts are accepted only after schema adaptation, not while planning
/// against the table schema: adaptation may insert another cast underneath an
/// explicit cast, leaving an expression the runtime checker cannot handle.
fn pushdown_columns(
    expr: &Arc<dyn PhysicalExpr>,
    file_schema: &Schema,
    allow_struct_casts: bool,
) -> Result<Option<PushdownColumns>> {
    let allow_list_columns = supports_list_predicates(expr);
    let mut checker =
        PushdownChecker::new(file_schema, allow_list_columns, allow_struct_casts);
    expr.visit(&mut checker)?;
    Ok((!checker.prevents_pushdown()).then(|| checker.into_sorted_columns()))
}

/// Resolves which Parquet leaf columns and Arrow schema fields are needed
/// to evaluate `expr` against a Parquet file
///
/// Returns `Ok(Some((plan, required_bytes)))` when the expression can be
/// evaluated using only pushdown-compatible columns. `Ok(None)` when it
/// cannot (it references whole struct columns or columns missing from disk).
///
/// The `required_bytes` is the total compressed size of all referenced columns
/// across all row groups, used to estimate filter evaluation cost.
///
/// Note: this is a shared entry point used by both row filter construction and
/// the opener's projection logic
pub(crate) fn build_parquet_read_plan(
    expr: &Arc<dyn PhysicalExpr>,
    file_schema: &Schema,
    metadata: &ParquetMetaData,
) -> Result<Option<(ParquetReadPlan, usize)>> {
    let schema_descr = metadata.file_metadata().schema_descr();

    let Some(required_columns) = pushdown_columns(expr, file_schema, true)? else {
        return Ok(None);
    };

    // A retained Struct cast names the fields its conversion touches, so the
    // read is clipped to those leaves rather than decoding the whole root.
    // A cast whose target covers every leaf, or that cannot be clipped safely,
    // falls back to a full read of that root inside the helper.
    let (read_plan, leaf_indices) = if required_columns.cast_accesses.is_empty() {
        assemble_read_plan(
            &required_columns.required_columns,
            &required_columns.struct_field_accesses,
            file_schema,
            schema_descr,
        )
    } else {
        build_read_plan_with_cast_clipping(
            file_schema,
            schema_descr,
            &required_columns.required_columns,
            &required_columns.struct_field_accesses,
            &required_columns.cast_accesses,
        )
    };

    let required_bytes = size_of_columns(&leaf_indices, metadata)?;

    Ok(Some((read_plan, required_bytes)))
}

/// Checks if a predicate expression can be pushed down to the parquet decoder.
///
/// Returns `true` if all columns referenced by the expression:
/// - Exist in the provided schema
/// - Are primitive types OR list columns with supported predicates
///   (e.g., `array_has`, `array_has_all`, `array_has_any`, IS NULL, IS NOT NULL)
/// - Are struct columns accessed via `get_field` where the leaf type is primitive
/// - Direct references to whole struct columns will prevent pushdown
///
/// # Arguments
/// * `expr` - The filter expression to check
/// * `file_schema` - The Arrow schema of the parquet file (or table schema when
///   the file schema is not yet available during planning)
///
/// # Examples
///
/// Primitive column filters can be pushed down:
/// ```ignore
/// use datafusion_expr::{col, Expr};
/// use datafusion_common::ScalarValue;
/// use arrow::datatypes::{DataType, Field, Schema};
/// use std::sync::Arc;
///
/// let schema = Arc::new(Schema::new(vec![
///     Field::new("age", DataType::Int32, false),
/// ]));
///
/// // Primitive filter: can be pushed down
/// let expr = col("age").gt(Expr::Literal(ScalarValue::Int32(Some(30)), None));
/// let expr = logical2physical(&expr, &schema);
/// assert!(can_expr_be_pushed_down_with_schemas(&expr, &schema));
/// ```
///
/// Struct column filters cannot be pushed down:
/// ```ignore
/// use arrow::datatypes::Fields;
///
/// let schema = Arc::new(Schema::new(vec![
///     Field::new("person", DataType::Struct(
///         Fields::from(vec![Field::new("name", DataType::Utf8, true)])
///     ), true),
/// ]));
///
/// // Struct filter: cannot be pushed down
/// let expr = col("person").is_not_null();
/// let expr = logical2physical(&expr, &schema);
/// assert!(!can_expr_be_pushed_down_with_schemas(&expr, &schema));
/// ```
///
/// List column filters with supported predicates can be pushed down:
/// ```ignore
/// use datafusion_functions_nested::expr_fn::{array_has_all, make_array};
///
/// let schema = Arc::new(Schema::new(vec![
///     Field::new("tags", DataType::List(
///         Arc::new(Field::new("item", DataType::Utf8, true))
///     ), true),
/// ]));
///
/// // Array filter with supported predicate: can be pushed down
/// let expr = array_has_all(col("tags"), make_array(vec![
///     Expr::Literal(ScalarValue::Utf8(Some("rust".to_string())), None)
/// ]));
/// let expr = logical2physical(&expr, &schema);
/// assert!(can_expr_be_pushed_down_with_schemas(&expr, &schema));
/// ```
pub fn can_expr_be_pushed_down_with_schemas(
    expr: &Arc<dyn PhysicalExpr>,
    file_schema: &Schema,
) -> bool {
    match pushdown_columns(expr, file_schema, false) {
        Ok(Some(_)) => true,
        Ok(None) | Err(_) => false,
    }
}

/// Calculate the total compressed size of all leaf columns required for
/// predicate `Expr`.
///
/// This value represents the total amount of IO required to evaluate the
/// predicate.
fn size_of_columns(columns: &[usize], metadata: &ParquetMetaData) -> Result<usize> {
    let mut total_size = 0;
    let row_groups = metadata.row_groups();
    for idx in columns {
        for rg in row_groups.iter() {
            total_size += rg.column(*idx).compressed_size() as usize;
        }
    }

    Ok(total_size)
}

/// Build a [`RowFilter`] from the given predicate expression if possible.
///
/// # Arguments
/// * `expr` - The filter predicate, already adapted to reference columns in `file_schema`
/// * `file_schema` - The Arrow schema of the parquet file (the result of converting
///   the parquet schema to Arrow, potentially with type coercions applied)
/// * `metadata` - Parquet file metadata used for cost estimation
/// * `reorder_predicates` - If true, reorder predicates to minimize I/O
/// * `file_metrics` - Metrics for tracking filter performance
///
/// # Returns
///
/// `Ok((row_filter, rejected))` where:
/// * `row_filter` is `Some` if at least one conjunct can be evaluated as an
///   `ArrowPredicate`, `None` otherwise.
/// * `rejected` holds the required conjuncts that *cannot* be evaluated as an
///   `ArrowPredicate` (for example whole-struct references or columns missing
///   from this file's physical schema). The caller MUST apply these elsewhere
///   — e.g. as a post-scan filter — otherwise the predicate is relaxed and the
///   query returns wrong results. Optional conjuncts (see
///   [`split_optional`](datafusion_physical_expr::utils::split_optional))
///   that cannot be evaluated are not used and are not in `rejected`: they
///   are not needed for correctness.
///
/// `Err(e)` if an error occurs while building the filter.
///
/// For example, if the expression is `a = 1 AND b = 2 AND c = 3` and `b = 2`
/// cannot be evaluated as an `ArrowPredicate`, the returned `RowFilter`
/// contains `a = 1` and `c = 3` and `rejected` contains `b = 2`.
// The tuple captures two related but distinct outputs; a type alias would
// obscure rather than clarify the public signature.
#[expect(clippy::type_complexity)]
pub fn build_row_filter(
    expr: &Arc<dyn PhysicalExpr>,
    file_schema: &SchemaRef,
    metadata: &ParquetMetaData,
    reorder_predicates: bool,
    file_metrics: &ParquetFileMetrics,
) -> Result<(Option<RowFilter>, Vec<Arc<dyn PhysicalExpr>>)> {
    // Implemented on top of the prebuild split so there is a single place
    // that splits conjuncts, orders candidates, and wires metrics — callers
    // that build once per file go through the same code as the per-row-group
    // rebuild path in `RowFilterContext`.
    //
    // Optional conjuncts are used like all other conjuncts here (the
    // `always` optional filter mode).
    let (prebuilt, rejected) =
        prebuild_row_filter_candidates(expr, file_schema, metadata, None)?;
    let row_filter = prebuilt.map(|prebuilt| {
        row_filter_from_prebuilt(&prebuilt, reorder_predicates, file_metrics)
    });
    Ok((row_filter, rejected))
}

/// A precomputed [`FilterCandidate`] with its expression column-reassigned to
/// the projected schema, ready to be wrapped into a [`DatafusionArrowPredicate`]
/// on demand.
///
/// Extracting this from [`build_row_filter`] lets callers pay the tree-walk +
/// column-resolution + `reassign_expr_columns` cost **once per file** instead
/// of once per row group, which is the hot path for
/// [`RowFilterContext::build_row_filter`](crate::push_decoder::RowFilterContext) rebuilds
/// on `fully_matched → not-fully-matched` boundaries.
#[derive(Clone, Debug)]
pub(crate) struct PrebuiltRowFilterCandidate {
    /// The predicate expression with all `Column` indices rewritten to point
    /// into the projected file schema.
    physical_expr: Arc<dyn PhysicalExpr>,
    /// Projection mask over the parquet leaf columns needed to evaluate this
    /// predicate.
    projection_mask: ProjectionMask,
    /// Precomputed sum-of-compressed-bytes for the referenced columns across
    /// all row groups in the file. Used to sort predicates when
    /// `reorder_predicates` is enabled. Stable across row groups within a
    /// file, so we cache it once.
    required_bytes: usize,
    /// The gate of an optional filter in [`OptionalFilterMode::Adaptive`],
    /// shared by all `RowFilter`s built for the file. `None` for all other
    /// filters.
    gate: Option<SharedOptionalFilterGate>,
    /// The measured saving that the gate reads, if the output projection of
    /// the file is known. See [`crate::optional_filter`].
    saving: Option<OptionalFilterSaving>,
    /// Position of the conjunct in the root `AND` chain of the file
    /// predicate.
    position: usize,
    /// The conjunct in terms of the file schema (the inner expression for a
    /// gated optional filter).
    source_expr: Arc<dyn PhysicalExpr>,
    /// See [`DatafusionArrowPredicate::placement_stats`].
    placement_stats: Option<Arc<ConjunctStats>>,
}

impl PrebuiltRowFilterCandidate {
    pub(crate) fn reads_leaf(&self, leaf_idx: usize) -> bool {
        self.projection_mask.leaf_included(leaf_idx)
    }

    /// The measured saving of a gated optional filter.
    pub(crate) fn optional_saving(&self) -> Option<&OptionalFilterSaving> {
        self.saving.as_ref()
    }

    /// True if this is a gated optional filter.
    pub(crate) fn is_optional(&self) -> bool {
        self.gate.is_some()
    }

    /// The gate of a gated optional filter.
    pub(crate) fn gate(&self) -> Option<&SharedOptionalFilterGate> {
        self.gate.as_ref()
    }

    /// Position of the conjunct in the root `AND` chain of the file
    /// predicate.
    pub(crate) fn position(&self) -> usize {
        self.position
    }

    /// The conjunct in terms of the file schema (the inner expression for a
    /// gated optional filter).
    pub(crate) fn source_expr(&self) -> &Arc<dyn PhysicalExpr> {
        &self.source_expr
    }

    /// Records the evaluations of this candidate in `stats`, for the
    /// adaptive filter placement.
    pub(crate) fn set_placement_stats(&mut self, stats: Arc<ConjunctStats>) {
        self.placement_stats = Some(stats);
    }
}

/// What [`prebuild_row_filter_candidates`] needs to handle optional
/// conjuncts (conjuncts wrapped in an `OptionalFilterPhysicalExpr`) for one
/// file.
#[derive(Debug, Clone, Copy)]
pub(crate) struct OptionalFilterRowFilterContext<'a> {
    /// The optional filter settings of the scan.
    pub(crate) options: &'a OptionalFilterOptions,
    /// Metrics of the scan, for the lazily-registered gate metrics.
    pub(crate) metrics: &'a ExecutionPlanMetricsSet,
    /// Execution partition index.
    pub(crate) partition: usize,
    /// Name of the file.
    pub(crate) filename: &'a str,
    /// The columns that the decoder produces for the file. The gates use it
    /// to estimate the decode time that a removed row saves (see
    /// [`crate::optional_filter`]). If `None`, the gates use only the
    /// configured minimum saving.
    pub(crate) output_projection: Option<&'a ProjectionMask>,
}

/// Precompute the list of [`PrebuiltRowFilterCandidate`]s for a predicate.
///
/// This is the expensive part of [`build_row_filter`]: split into conjuncts,
/// resolve columns for each conjunct against the file schema, reassign
/// `Column` indices, and compute the sort-order metadata. Doing it once per
/// file (and reusing across row groups) avoids repeated `TreeNode::transform`
/// walks and `Arc<PhysicalExpr>` allocations that showed up as top hot spots
/// in TPCH profiles.
///
/// The first element is `None` when the predicate has no push-downable
/// conjuncts, in which case callers should skip installing a `RowFilter`
/// entirely. The second element holds the required conjuncts that cannot be
/// evaluated as an `ArrowPredicate` on this file; see [`build_row_filter`]
/// for why the caller must apply them elsewhere.
///
/// # Optional conjuncts
///
/// `optional` controls the conjuncts of the root `AND` chain that are
/// wrapped in an `OptionalFilterPhysicalExpr` (see
/// [`split_optional`](datafusion_physical_expr::utils::split_optional)).
/// The other (required) conjuncts are always used.
///
/// * `None` or [`OptionalFilterMode::Always`]: optional conjuncts are used
///   like required conjuncts (the `OptionalFilterPhysicalExpr` wrapper is
///   transparent).
/// * [`OptionalFilterMode::PruningOnly`]: optional conjuncts are not used.
///   Statistics pruning still uses them, because it uses the complete
///   predicate.
/// * [`OptionalFilterMode::Adaptive`]: each optional conjunct is a separate
///   candidate with an [`OptionalFilterGate`], which skips the conjunct while
///   it costs more than it saves. Each file has its own gates. Optional
///   candidates come after all required
///   candidates, see [`row_filter_from_prebuilt`].
///
/// An optional conjunct that cannot be evaluated as an `ArrowPredicate` for
/// this file (for example because of schema evolution) is not used, in all
/// modes. It is not in the rejected conjuncts either, thus the caller does
/// not evaluate it after the scan. This is always safe.
#[expect(clippy::type_complexity)]
pub(crate) fn prebuild_row_filter_candidates(
    expr: &Arc<dyn PhysicalExpr>,
    file_schema: &SchemaRef,
    metadata: &ParquetMetaData,
    optional: Option<OptionalFilterRowFilterContext<'_>>,
) -> Result<(
    Option<Vec<PrebuiltRowFilterCandidate>>,
    Vec<Arc<dyn PhysicalExpr>>,
)> {
    let mode = optional.map_or(OptionalFilterMode::Always, |o| o.options.mode);

    // Split into conjuncts, with their position in the root `AND` chain:
    // `a = 1 AND b = 2 AND c = 3` -> [`a = 1`, `b = 2`, `c = 3`]
    let mut required = vec![];
    let mut optional_conjuncts = vec![];
    for (position, conjunct) in split_conjunction(expr).into_iter().enumerate() {
        match (conjunct.downcast_ref::<OptionalFilterPhysicalExpr>(), mode) {
            (None, _) | (Some(_), OptionalFilterMode::Always) => {
                required.push((position, Arc::clone(conjunct)))
            }
            (Some(_), OptionalFilterMode::PruningOnly) => {}
            (Some(optional), OptionalFilterMode::Adaptive) => {
                optional_conjuncts.push((position, Arc::clone(optional.inner())))
            }
        }
    }

    // Required conjuncts that cannot be evaluated as ArrowPredicates are
    // returned to the caller so they are never silently dropped.
    let mut prebuilt = Vec::with_capacity(required.len() + optional_conjuncts.len());
    let mut rejected: Vec<Arc<dyn PhysicalExpr>> = Vec::new();
    for (position, conjunct) in required {
        // In the `always` mode, `required` also holds the optional conjuncts
        // (still wrapped). An optional conjunct that cannot be pushed down
        // for this file is not needed for correctness: do not use it, and do
        // not send it to the post-scan filter.
        if is_optional_filter(&conjunct) {
            match FilterCandidateBuilder::new(conjunct, Arc::clone(file_schema))
                .build(metadata)
            {
                Ok(Some(candidate)) => prebuilt
                    .push(PrebuiltRowFilterCandidate::try_new(candidate, position)?),
                Ok(None) => {}
                Err(e) => {
                    log::debug!(
                        "Ignoring optional filter that cannot be pushed down: {e}"
                    );
                }
            }
            continue;
        }
        match FilterCandidateBuilder::new(Arc::clone(&conjunct), Arc::clone(file_schema))
            .build(metadata)?
        {
            Some(candidate) => {
                prebuilt.push(PrebuiltRowFilterCandidate::try_new(candidate, position)?)
            }
            None => rejected.push(conjunct),
        }
    }

    if let Some(optional) = optional {
        for (position, conjunct) in optional_conjuncts {
            // An optional conjunct that cannot be pushed down for this file
            // (or that fails to build) is not needed for correctness, thus it
            // is simply not used.
            let candidate =
                match FilterCandidateBuilder::new(conjunct, Arc::clone(file_schema))
                    .build(metadata)
                {
                    Ok(Some(candidate)) => candidate,
                    Ok(None) => continue,
                    Err(e) => {
                        log::debug!(
                            "Ignoring optional filter that cannot be pushed down: {e}"
                        );
                        continue;
                    }
                };
            let mut candidate = PrebuiltRowFilterCandidate::try_new(candidate, position)?;
            // The gate watches the dynamic filters in the same (live)
            // expression that the predicate evaluates, thus it sees their
            // updates.
            let mut gate = OptionalFilterGate::new(
                Arc::clone(&candidate.physical_expr),
                optional.options.gate_config,
            );
            // A removed row saves the decode of the output columns that the
            // filter does not read.
            if let Some(output) = optional.output_projection {
                let unread_bytes_per_row = compressed_bytes_per_row(metadata, |leaf| {
                    output.leaf_included(leaf)
                        && !candidate.projection_mask.leaf_included(leaf)
                });
                let saving = OptionalFilterSaving::new(
                    unread_bytes_per_row,
                    &optional.options.decode_cost,
                );
                gate = gate.with_measured_saving(Arc::clone(&saving.saving));
                candidate.saving = Some(saving);
            }
            candidate.gate = Some(Arc::new(Mutex::new(OptionalFilterGateState {
                gate,
                metrics: OptionalFilterMetrics::new(
                    optional.metrics,
                    optional.partition,
                    optional.filename,
                ),
                reported_pauses: 0,
            })));
            prebuilt.push(candidate);
        }
    }

    if prebuilt.is_empty() {
        return Ok((None, rejected));
    }
    Ok((Some(prebuilt), rejected))
}

impl PrebuiltRowFilterCandidate {
    /// A candidate without a gate, for the conjunct at `position` in the
    /// root `AND` chain of the file predicate. Its expression is
    /// column-reassigned to the projected schema of the candidate.
    fn try_new(candidate: FilterCandidate, position: usize) -> Result<Self> {
        let source_expr = Arc::clone(&candidate.expr);
        let physical_expr =
            reassign_expr_columns(candidate.expr, &candidate.read_plan.projected_schema)?;
        Ok(Self {
            physical_expr,
            projection_mask: candidate.read_plan.projection_mask,
            required_bytes: candidate.required_bytes,
            gate: None,
            saving: None,
            position,
            source_expr,
            placement_stats: None,
        })
    }
}

/// Returns the candidates in evaluation order: the order of `prebuilt`, or
/// ordered by `required_bytes` if `reorder_predicates` is true. Gated
/// optional candidates always come last (see [`row_filter_from_prebuilt`]).
fn order_candidates<'a>(
    prebuilt: impl IntoIterator<Item = &'a PrebuiltRowFilterCandidate>,
    reorder_predicates: bool,
) -> Vec<&'a PrebuiltRowFilterCandidate> {
    // Collect references into a working list we can sort without disturbing
    // the shared cache.
    let mut ordered: Vec<&PrebuiltRowFilterCandidate> = prebuilt.into_iter().collect();
    if reorder_predicates {
        ordered.sort_unstable_by_key(|c| (c.is_optional(), c.required_bytes));
    } else {
        // `prebuild_row_filter_candidates` already puts the optional
        // candidates last. A stable sort keeps the other order.
        ordered.sort_by_key(|c| c.is_optional());
    }
    ordered
}

/// Wrap a list of prebuilt candidates into a fresh [`RowFilter`], assigning
/// per-predicate metric counters and (optionally) reordering by
/// `required_bytes`. This is the cheap per-row-group rebuild path — no tree
/// walks, no column resolution, only counter allocation.
///
/// Gated optional candidates (see [`prebuild_row_filter_candidates`]) always
/// come after the required candidates, also when `reorder_predicates` is
/// true. The required candidates must run in all cases. When they run first,
/// the optional candidates see only the rows that the required candidates
/// let pass: the gate then measures the selectivity that the optional filter
/// adds, and a skipped optional filter costs nothing.
pub(crate) fn row_filter_from_prebuilt<'a>(
    prebuilt: impl IntoIterator<Item = &'a PrebuiltRowFilterCandidate>,
    reorder_predicates: bool,
    file_metrics: &ParquetFileMetrics,
) -> RowFilter {
    let rows_pruned = &file_metrics.pushdown_rows_pruned;
    let rows_matched = &file_metrics.pushdown_rows_matched;
    let time = &file_metrics.row_pushdown_eval_time;

    let ordered = order_candidates(prebuilt, reorder_predicates);
    let total = ordered.len();
    let filters: Vec<Box<dyn ArrowPredicate>> = ordered
        .into_iter()
        .enumerate()
        .map(|(idx, candidate)| {
            let is_last = idx == total - 1;
            let predicate_rows_pruned = rows_pruned.clone();
            let predicate_rows_matched = if is_last {
                rows_matched.clone()
            } else {
                metrics::Count::new()
            };
            Box::new(DatafusionArrowPredicate {
                physical_expr: Arc::clone(&candidate.physical_expr),
                projection_mask: candidate.projection_mask.clone(),
                rows_pruned: predicate_rows_pruned,
                rows_matched: predicate_rows_matched,
                time: time.clone(),
                gate: candidate.gate.clone(),
                placement_stats: candidate.placement_stats.clone(),
            }) as Box<dyn ArrowPredicate>
        })
        .collect();
    RowFilter::new(filters)
}

#[cfg(test)]
mod test {
    use super::*;
    use arrow::datatypes::{DataType, Fields};
    use datafusion_common::{DataFusionError, ScalarValue};

    use arrow::array::{
        Int32Array, ListBuilder, StringArray, StringBuilder, StructArray,
    };
    use arrow::datatypes::{Field, TimeUnit::Nanosecond};
    use datafusion_expr::{Cast, Expr, col, lit};
    use datafusion_functions::core::get_field;
    use datafusion_functions_nested::array_has::{
        array_has_all_udf, array_has_any_udf, array_has_udf,
    };
    use datafusion_functions_nested::expr_fn::{
        array_has, array_has_all, array_has_any, make_array,
    };
    use datafusion_physical_expr::expressions::Column;
    use datafusion_physical_expr::planner::logical2physical;
    use datafusion_physical_expr_adapter::{
        DefaultPhysicalExprAdapterFactory, PhysicalExprAdapterFactory,
    };
    use datafusion_physical_plan::metrics::{Count, ExecutionPlanMetricsSet, Time};

    use parquet::arrow::ArrowWriter;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::arrow::parquet_to_arrow_schema;
    use parquet::file::reader::{FileReader, SerializedFileReader};
    use tempfile::NamedTempFile;

    // List predicates used by the decoder should be accepted for pushdown
    #[test]
    fn test_filter_candidate_builder_supports_list_types() {
        let testdata = datafusion_common::test_util::parquet_test_data();
        let file = std::fs::File::open(format!("{testdata}/list_columns.parquet"))
            .expect("opening file");

        let reader = SerializedFileReader::new(file).expect("creating reader");

        let metadata = reader.metadata();

        let table_schema =
            parquet_to_arrow_schema(metadata.file_metadata().schema_descr(), None)
                .expect("parsing schema");

        let expr = col("int64_list").is_not_null();
        let expr = logical2physical(&expr, &table_schema);

        let table_schema = Arc::new(table_schema.clone());

        let list_index = table_schema
            .index_of("int64_list")
            .expect("list column should exist");

        let candidate = FilterCandidateBuilder::new(expr, table_schema)
            .build(metadata)
            .expect("building candidate")
            .expect("list pushdown should be supported");

        let expected_mask =
            ProjectionMask::leaves(metadata.file_metadata().schema_descr(), [list_index]);
        assert_eq!(candidate.read_plan.projection_mask, expected_mask);
    }

    #[test]
    fn test_filter_type_coercion() {
        let testdata = datafusion_common::test_util::parquet_test_data();
        let file = std::fs::File::open(format!("{testdata}/alltypes_plain.parquet"))
            .expect("opening file");

        let parquet_reader_builder =
            ParquetRecordBatchReaderBuilder::try_new(file).expect("creating reader");
        let metadata = parquet_reader_builder.metadata().clone();
        let file_schema = parquet_reader_builder.schema().clone();

        // This is the schema we would like to coerce to,
        // which is different from the physical schema of the file.
        let table_schema = Schema::new(vec![Field::new(
            "timestamp_col",
            DataType::Timestamp(Nanosecond, Some(Arc::from("UTC"))),
            false,
        )]);

        // Test all should fail
        let expr = col("timestamp_col").lt(Expr::Literal(
            ScalarValue::TimestampNanosecond(Some(1), Some(Arc::from("UTC"))),
            None,
        ));
        let expr = logical2physical(&expr, &table_schema);
        let expr = DefaultPhysicalExprAdapterFactory {}
            .create(Arc::new(table_schema.clone()), Arc::clone(&file_schema))
            .expect("creating expr adapter")
            .rewrite(expr)
            .expect("rewriting expression");
        let candidate = FilterCandidateBuilder::new(expr, file_schema.clone())
            .build(&metadata)
            .expect("building candidate")
            .expect("candidate expected");

        let mut row_filter = DatafusionArrowPredicate::try_new(
            candidate,
            Count::new(),
            Count::new(),
            Time::new(),
        )
        .expect("creating filter predicate");

        let mut parquet_reader = parquet_reader_builder
            .with_projection(row_filter.projection().clone())
            .build()
            .expect("building reader");

        // Parquet file is small, we only need 1 record batch
        let first_rb = parquet_reader
            .next()
            .expect("expected record batch")
            .expect("expected error free record batch");

        let filtered = row_filter.evaluate(first_rb.clone());
        assert!(matches!(filtered, Ok(a) if a == BooleanArray::from(vec![false; 8])));

        // Test all should pass
        let expr = col("timestamp_col").gt(Expr::Literal(
            ScalarValue::TimestampNanosecond(Some(0), Some(Arc::from("UTC"))),
            None,
        ));
        let expr = logical2physical(&expr, &table_schema);
        // Rewrite the expression to add CastExpr for type coercion
        let expr = DefaultPhysicalExprAdapterFactory {}
            .create(Arc::new(table_schema), Arc::clone(&file_schema))
            .expect("creating expr adapter")
            .rewrite(expr)
            .expect("rewriting expression");
        let candidate = FilterCandidateBuilder::new(expr, file_schema)
            .build(&metadata)
            .expect("building candidate")
            .expect("candidate expected");

        let mut row_filter = DatafusionArrowPredicate::try_new(
            candidate,
            Count::new(),
            Count::new(),
            Time::new(),
        )
        .expect("creating filter predicate");

        let filtered = row_filter.evaluate(first_rb);
        assert!(matches!(filtered, Ok(a) if a == BooleanArray::from(vec![true; 8])));
    }

    /// A predicate that fails while it is being evaluated must report the
    /// original error, not an opaque string, so that callers can still tell a
    /// user error apart from an internal one.
    #[test]
    fn evaluate_reports_the_original_error() {
        let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(StringArray::from(vec!["not_an_int"]))],
        )
        .expect("record batch");

        let file = NamedTempFile::new().expect("temp file");
        let mut writer =
            ArrowWriter::try_new(file.reopen().unwrap(), Arc::clone(&schema), None)
                .expect("writer");
        writer.write(&batch).expect("write batch");
        writer.close().expect("close writer");

        let parquet_reader_builder =
            ParquetRecordBatchReaderBuilder::try_new(file.reopen().expect("reopen file"))
                .expect("reader builder");
        let metadata = parquet_reader_builder.metadata().clone();
        let file_schema = parquet_reader_builder.schema().clone();

        // Casting the column in the file to `Int32` fails on this data
        let expr = Expr::Cast(Cast::new(Box::new(col("s")), DataType::Int32)).eq(lit(1));
        let expr = logical2physical(&expr, &file_schema);
        let candidate = FilterCandidateBuilder::new(expr, Arc::clone(&file_schema))
            .build(&metadata)
            .expect("building candidate")
            .expect("candidate expected");

        let mut predicate = DatafusionArrowPredicate::try_new(
            candidate,
            Count::new(),
            Count::new(),
            Time::new(),
        )
        .expect("creating filter predicate");

        let mut parquet_reader = parquet_reader_builder
            .with_projection(predicate.projection().clone())
            .build()
            .expect("building reader");
        let first_rb = parquet_reader
            .next()
            .expect("expected record batch")
            .expect("expected error free record batch");

        let err = predicate
            .evaluate(first_rb)
            .expect_err("evaluating the predicate should fail");

        // The cast failure is still reachable, rather than being flattened into
        // an untyped `ArrowError::ComputeError`
        let err = DataFusionError::from(err);
        let root = err.find_root();
        assert!(
            matches!(
                root,
                DataFusionError::ArrowError(inner, _)
                    if matches!(inner.as_ref(), ArrowError::CastError(_))
            ),
            "expected the original cast error, got {root:?}"
        );

        // and the message still says where the failure happened
        let message = err.to_string();
        assert!(
            message.contains("Error evaluating filter predicate"),
            "{message}"
        );
        assert!(message.contains("Cannot cast string"), "{message}");
    }

    #[test]
    fn struct_data_structures_prevent_pushdown() {
        let table_schema = Arc::new(Schema::new(vec![Field::new(
            "struct_col",
            DataType::Struct(
                vec![Arc::new(Field::new("a", DataType::Int32, true))].into(),
            ),
            true,
        )]));

        let expr = col("struct_col").is_not_null();
        let expr = logical2physical(&expr, &table_schema);

        assert!(!can_expr_be_pushed_down_with_schemas(&expr, &table_schema));
    }

    #[test]
    fn mixed_primitive_and_struct_prevents_pushdown() {
        // Even when a predicate contains both primitive and unsupported nested columns,
        // the entire predicate should not be pushed down because the struct column
        // cannot be evaluated during Parquet decoding.
        let table_schema = Arc::new(Schema::new(vec![
            Field::new(
                "struct_col",
                DataType::Struct(
                    vec![Arc::new(Field::new("a", DataType::Int32, true))].into(),
                ),
                true,
            ),
            Field::new("int_col", DataType::Int32, false),
        ]));

        // Expression: (struct_col IS NOT NULL) AND (int_col = 5)
        // Even though int_col is primitive, the presence of struct_col in the
        // conjunction should prevent pushdown of the entire expression.
        let expr = col("struct_col")
            .is_not_null()
            .and(col("int_col").eq(Expr::Literal(ScalarValue::Int32(Some(5)), None)));
        let expr = logical2physical(&expr, &table_schema);

        // The entire expression should not be pushed down
        assert!(!can_expr_be_pushed_down_with_schemas(&expr, &table_schema));

        // However, just the int_col predicate alone should be pushable
        let expr_int_only =
            col("int_col").eq(Expr::Literal(ScalarValue::Int32(Some(5)), None));
        let expr_int_only = logical2physical(&expr_int_only, &table_schema);
        assert!(can_expr_be_pushed_down_with_schemas(
            &expr_int_only,
            &table_schema
        ));
    }

    #[test]
    fn nested_lists_allow_pushdown_checks() {
        let table_schema = Arc::new(get_lists_table_schema());

        let expr = col("utf8_list").is_not_null();
        let expr = logical2physical(&expr, &table_schema);
        check_expression_can_evaluate_against_schema(&expr, &table_schema);

        assert!(can_expr_be_pushed_down_with_schemas(&expr, &table_schema));
    }

    #[test]
    fn array_has_all_pushdown_filters_rows() {
        // Test array_has_all: checks if array contains all of ["c"]
        // Rows with "c": row 1 and row 2
        let expr = array_has_all(
            col("letters"),
            make_array(vec![Expr::Literal(
                ScalarValue::Utf8(Some("c".to_string())),
                None,
            )]),
        );
        test_array_predicate_pushdown("array_has_all", expr, 1, 2, true);
    }

    /// Helper function to test array predicate pushdown functionality.
    ///
    /// Creates a Parquet file with a list column, applies the given predicate,
    /// and verifies that rows are correctly filtered during decoding.
    fn test_array_predicate_pushdown(
        func_name: &str,
        predicate_expr: Expr,
        expected_pruned: usize,
        expected_matched: usize,
        expect_list_support: bool,
    ) {
        let item_field = Arc::new(Field::new("item", DataType::Utf8, true));
        let schema = Arc::new(Schema::new(vec![Field::new(
            "letters",
            DataType::List(item_field),
            true,
        )]));

        let mut builder = ListBuilder::new(StringBuilder::new());
        // Row 0: ["a", "b"]
        builder.values().append_value("a");
        builder.values().append_value("b");
        builder.append(true);

        // Row 1: ["c"]
        builder.values().append_value("c");
        builder.append(true);

        // Row 2: ["c", "d"]
        builder.values().append_value("c");
        builder.values().append_value("d");
        builder.append(true);

        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(builder.finish())])
                .expect("record batch");

        let file = NamedTempFile::new().expect("temp file");
        let mut writer =
            ArrowWriter::try_new(file.reopen().unwrap(), schema, None).expect("writer");
        writer.write(&batch).expect("write batch");
        writer.close().expect("close writer");

        let reader_file = file.reopen().expect("reopen file");
        let parquet_reader_builder =
            ParquetRecordBatchReaderBuilder::try_new(reader_file)
                .expect("reader builder");
        let metadata = parquet_reader_builder.metadata().clone();
        let file_schema = parquet_reader_builder.schema().clone();

        let expr = logical2physical(&predicate_expr, &file_schema);
        if expect_list_support {
            assert!(supports_list_predicates(&expr));
        }

        let metrics = ExecutionPlanMetricsSet::new();
        let file_metrics =
            ParquetFileMetrics::new(0, &format!("{func_name}.parquet"), &metrics);

        let (row_filter, rejected) =
            build_row_filter(&expr, &file_schema, &metadata, false, &file_metrics)
                .expect("building row filter");
        assert!(
            rejected.is_empty(),
            "expected no rejected conjuncts, got {rejected:?}"
        );
        let row_filter = row_filter.expect("row filter should exist");

        let reader = parquet_reader_builder
            .with_row_filter(row_filter)
            .build()
            .expect("build reader");

        let mut total_rows = 0;
        for batch in reader {
            let batch = batch.expect("record batch");
            total_rows += batch.num_rows();
        }

        assert_eq!(
            file_metrics.pushdown_rows_pruned.value(),
            expected_pruned,
            "{func_name}: expected {expected_pruned} pruned rows"
        );
        assert_eq!(
            file_metrics.pushdown_rows_matched.value(),
            expected_matched,
            "{func_name}: expected {expected_matched} matched rows"
        );
        assert_eq!(
            total_rows, expected_matched,
            "{func_name}: expected {expected_matched} total rows"
        );
    }

    #[test]
    fn array_has_pushdown_filters_rows() {
        // Test array_has: checks if "c" is in the array
        // Rows with "c": row 1 and row 2
        let expr = array_has(
            col("letters"),
            Expr::Literal(ScalarValue::Utf8(Some("c".to_string())), None),
        );
        test_array_predicate_pushdown("array_has", expr, 1, 2, true);
    }

    #[test]
    fn array_has_any_pushdown_filters_rows() {
        // Test array_has_any: checks if array contains any of ["a", "d"]
        // Row 0 has "a", row 2 has "d" - both should match
        let expr = array_has_any(
            col("letters"),
            make_array(vec![
                Expr::Literal(ScalarValue::Utf8(Some("a".to_string())), None),
                Expr::Literal(ScalarValue::Utf8(Some("d".to_string())), None),
            ]),
        );
        test_array_predicate_pushdown("array_has_any", expr, 1, 2, true);
    }

    #[test]
    fn array_has_udf_pushdown_filters_rows() {
        let expr = array_has_udf().call(vec![
            col("letters"),
            Expr::Literal(ScalarValue::Utf8(Some("c".to_string())), None),
        ]);

        test_array_predicate_pushdown("array_has_udf", expr, 1, 2, true);
    }

    #[test]
    fn array_has_all_udf_pushdown_filters_rows() {
        let expr = array_has_all_udf().call(vec![
            col("letters"),
            make_array(vec![Expr::Literal(
                ScalarValue::Utf8(Some("c".to_string())),
                None,
            )]),
        ]);

        test_array_predicate_pushdown("array_has_all_udf", expr, 1, 2, true);
    }

    #[test]
    fn array_has_any_udf_pushdown_filters_rows() {
        let expr = array_has_any_udf().call(vec![
            col("letters"),
            make_array(vec![
                Expr::Literal(ScalarValue::Utf8(Some("a".to_string())), None),
                Expr::Literal(ScalarValue::Utf8(Some("d".to_string())), None),
            ]),
        ]);

        test_array_predicate_pushdown("array_has_any_udf", expr, 1, 2, true);
    }

    #[test]
    fn projected_columns_prevent_pushdown() {
        let table_schema = get_basic_table_schema();

        let expr =
            Arc::new(Column::new("nonexistent_column", 0)) as Arc<dyn PhysicalExpr>;

        assert!(!can_expr_be_pushed_down_with_schemas(&expr, &table_schema));
    }

    #[test]
    fn basic_expr_doesnt_prevent_pushdown() {
        let table_schema = get_basic_table_schema();

        let expr = col("string_col").is_null();
        let expr = logical2physical(&expr, &table_schema);

        assert!(can_expr_be_pushed_down_with_schemas(&expr, &table_schema));
    }

    #[test]
    fn complex_expr_doesnt_prevent_pushdown() {
        let table_schema = get_basic_table_schema();

        let expr = col("string_col")
            .is_not_null()
            .or(col("bigint_col").gt(Expr::Literal(ScalarValue::Int64(Some(5)), None)));
        let expr = logical2physical(&expr, &table_schema);

        assert!(can_expr_be_pushed_down_with_schemas(&expr, &table_schema));
    }

    fn get_basic_table_schema() -> Schema {
        let testdata = datafusion_common::test_util::parquet_test_data();
        let file = std::fs::File::open(format!("{testdata}/alltypes_plain.parquet"))
            .expect("opening file");

        let reader = SerializedFileReader::new(file).expect("creating reader");

        let metadata = reader.metadata();

        parquet_to_arrow_schema(metadata.file_metadata().schema_descr(), None)
            .expect("parsing schema")
    }

    fn get_lists_table_schema() -> Schema {
        let testdata = datafusion_common::test_util::parquet_test_data();
        let file = std::fs::File::open(format!("{testdata}/list_columns.parquet"))
            .expect("opening file");

        let reader = SerializedFileReader::new(file).expect("creating reader");

        let metadata = reader.metadata();

        parquet_to_arrow_schema(metadata.file_metadata().schema_descr(), None)
            .expect("parsing schema")
    }

    /// Regression test: when a schema has Struct columns, Arrow field indices diverge
    /// from Parquet leaf indices (Struct children become separate leaves). The
    /// `PrimitiveOnly` fast-path in `leaf_indices_for_roots` assumes they are equal,
    /// so a filter on a primitive column *after* a Struct gets the wrong leaf index.
    ///
    /// Schema:
    ///   Arrow indices:   col_a=0  struct_col=1  col_b=2
    ///   Parquet leaves:  col_a=0  struct_col.x=1  struct_col.y=2  col_b=3
    ///
    /// A filter on col_b should project Parquet leaf 3, but the bug causes it to
    /// project leaf 2 (struct_col.y).
    #[test]
    fn test_filter_pushdown_leaf_index_with_struct_in_schema() {
        use arrow::array::{Int32Array, StringArray, StructArray};

        let schema = Arc::new(Schema::new(vec![
            Field::new("col_a", DataType::Int32, false),
            Field::new(
                "struct_col",
                DataType::Struct(
                    vec![
                        Arc::new(Field::new("x", DataType::Int32, true)),
                        Arc::new(Field::new("y", DataType::Int32, true)),
                    ]
                    .into(),
                ),
                true,
            ),
            Field::new("col_b", DataType::Utf8, false),
        ]));

        let col_a = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let struct_col = Arc::new(StructArray::from(vec![
            (
                Arc::new(Field::new("x", DataType::Int32, true)),
                Arc::new(Int32Array::from(vec![10, 20, 30])) as _,
            ),
            (
                Arc::new(Field::new("y", DataType::Int32, true)),
                Arc::new(Int32Array::from(vec![100, 200, 300])) as _,
            ),
        ]));
        let col_b = Arc::new(StringArray::from(vec!["aaa", "target", "zzz"]));

        let batch =
            RecordBatch::try_new(Arc::clone(&schema), vec![col_a, struct_col, col_b])
                .unwrap();

        let file = NamedTempFile::new().expect("temp file");
        let mut writer =
            ArrowWriter::try_new(file.reopen().unwrap(), Arc::clone(&schema), None)
                .expect("writer");
        writer.write(&batch).expect("write batch");
        writer.close().expect("close writer");

        let reader_file = file.reopen().expect("reopen file");
        let builder = ParquetRecordBatchReaderBuilder::try_new(reader_file)
            .expect("reader builder");
        let metadata = builder.metadata().clone();
        let file_schema = builder.schema().clone();

        // sanity check: 4 Parquet leaves, 3 Arrow fields
        assert_eq!(metadata.file_metadata().schema_descr().num_columns(), 4);
        assert_eq!(file_schema.fields().len(), 3);

        // build a filter candidate for `col_b = 'target'` through the public API
        let expr = col("col_b").eq(Expr::Literal(
            ScalarValue::Utf8(Some("target".to_string())),
            None,
        ));
        let expr = logical2physical(&expr, &file_schema);

        let candidate = FilterCandidateBuilder::new(expr, file_schema)
            .build(&metadata)
            .expect("building candidate")
            .expect("filter on primitive col_b should be pushable");

        // col_b is Parquet leaf 3 (shifted by struct_col's two children).
        let expected_mask =
            ProjectionMask::leaves(metadata.file_metadata().schema_descr(), [3]);
        assert_eq!(
            candidate.read_plan.projection_mask, expected_mask,
            "projection_mask should select only leaf 3 for col_b"
        );
    }

    /// get_field(struct_col, 'a') on a struct with a primitive leaf should allow pushdown.
    #[test]
    fn get_field_on_struct_allows_pushdown() {
        let table_schema = Arc::new(Schema::new(vec![Field::new(
            "struct_col",
            DataType::Struct(
                vec![Arc::new(Field::new("a", DataType::Int32, true))].into(),
            ),
            true,
        )]));

        // get_field(struct_col, 'a') > 5
        let get_field_expr = get_field().call(vec![
            col("struct_col"),
            Expr::Literal(ScalarValue::Utf8(Some("a".to_string())), None),
        ]);
        let expr = get_field_expr.gt(Expr::Literal(ScalarValue::Int32(Some(5)), None));
        let expr = logical2physical(&expr, &table_schema);

        assert!(can_expr_be_pushed_down_with_schemas(&expr, &table_schema));
    }

    /// get_field on a struct field that resolves to a nested type should still block pushdown.
    #[test]
    fn get_field_on_nested_leaf_prevents_pushdown() {
        let inner_struct = DataType::Struct(
            vec![Arc::new(Field::new("x", DataType::Int32, true))].into(),
        );
        let table_schema = Arc::new(Schema::new(vec![Field::new(
            "struct_col",
            DataType::Struct(
                vec![Arc::new(Field::new("nested", inner_struct, true))].into(),
            ),
            true,
        )]));

        // get_field(struct_col, 'nested') IS NOT NULL — the leaf is still a struct
        let get_field_expr = get_field().call(vec![
            col("struct_col"),
            Expr::Literal(ScalarValue::Utf8(Some("nested".to_string())), None),
        ]);
        let expr = get_field_expr.is_not_null();
        let expr = logical2physical(&expr, &table_schema);

        assert!(!can_expr_be_pushed_down_with_schemas(&expr, &table_schema));
    }

    /// get_field returning a list inside a struct should allow pushdown when
    /// wrapped in a supported list predicate like `array_has_any`.
    /// e.g. `array_has_any(get_field(s, 'items'), make_array('x'))`
    #[test]
    fn get_field_list_leaf_with_array_predicate_allows_pushdown() {
        let item_field = Arc::new(Field::new("item", DataType::Utf8, true));
        let table_schema = Arc::new(Schema::new(vec![Field::new(
            "s",
            DataType::Struct(
                vec![
                    Arc::new(Field::new("id", DataType::Int32, true)),
                    Arc::new(Field::new("items", DataType::List(item_field), true)),
                ]
                .into(),
            ),
            true,
        )]));

        // array_has_any(get_field(s, 'items'), make_array('x'))
        let get_field_expr = get_field().call(vec![
            col("s"),
            Expr::Literal(ScalarValue::Utf8(Some("items".to_string())), None),
        ]);
        let expr = array_has_any(
            get_field_expr,
            make_array(vec![Expr::Literal(
                ScalarValue::Utf8(Some("x".to_string())),
                None,
            )]),
        );
        let expr = logical2physical(&expr, &table_schema);

        assert!(can_expr_be_pushed_down_with_schemas(&expr, &table_schema));
    }

    /// get_field on a struct produces correct Parquet leaf indices.
    #[test]
    fn get_field_filter_candidate_has_correct_leaf_indices() {
        use arrow::array::{Int32Array, StringArray, StructArray};

        // Schema: id (Int32), s (Struct{value: Int32, label: Utf8, unused: Utf8})
        // Parquet leaves: id=0, s.value=1, s.label=2, s.unused=3
        let struct_fields: Fields = vec![
            Arc::new(Field::new("value", DataType::Int32, false)),
            Arc::new(Field::new("label", DataType::Utf8, false)),
            Arc::new(Field::new("unused", DataType::Utf8, false)),
        ]
        .into();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("s", DataType::Struct(struct_fields.clone()), false),
        ]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StructArray::new(
                    struct_fields,
                    vec![
                        Arc::new(Int32Array::from(vec![10, 20, 30])) as _,
                        Arc::new(StringArray::from(vec!["a", "b", "c"])) as _,
                        Arc::new(StringArray::from(vec![
                            "unused-a", "unused-b", "unused-c",
                        ])) as _,
                    ],
                    None,
                )),
            ],
        )
        .unwrap();

        let file = NamedTempFile::new().expect("temp file");
        let mut writer =
            ArrowWriter::try_new(file.reopen().unwrap(), Arc::clone(&schema), None)
                .expect("writer");
        writer.write(&batch).expect("write batch");
        writer.close().expect("close writer");

        let reader_file = file.reopen().expect("reopen file");
        let builder = ParquetRecordBatchReaderBuilder::try_new(reader_file)
            .expect("reader builder");
        let metadata = builder.metadata().clone();
        let file_schema = builder.schema().clone();

        // get_field(s, 'value') > 5
        let get_field_expr = get_field().call(vec![
            col("s"),
            Expr::Literal(ScalarValue::Utf8(Some("value".to_string())), None),
        ]);
        let expr = get_field_expr.gt(Expr::Literal(ScalarValue::Int32(Some(5)), None));
        let expr = logical2physical(&expr, &file_schema);

        let candidate =
            FilterCandidateBuilder::new(Arc::clone(&expr), Arc::clone(&file_schema))
                .build(&metadata)
                .expect("building candidate")
                .expect("get_field filter on struct should be pushable");

        // The filter accesses only s.value, so only Parquet leaf 1 is needed.
        // Neither sibling is read, reducing unnecessary I/O.
        let expected_mask =
            ProjectionMask::leaves(metadata.file_metadata().schema_descr(), [1]);
        assert_eq!(
            candidate.read_plan.projection_mask, expected_mask,
            "projection_mask should select only the accessed struct field leaf"
        );

        // Schema adaptation retains Struct ancestors for some decimal conversions
        // so an all-null parent can skip child conversion. Runtime filters must
        // preserve every conversion named by the retained cast target.
        // This target includes `label`, so its conversion must still run even
        // though get_field selects only `value`. Planning keeps a residual filter
        // for explicit Struct casts.
        let cast_type = DataType::Struct(
            vec![
                Field::new("value", DataType::Int32, false),
                Field::new("label", DataType::Int32, true),
            ]
            .into(),
        );
        let cast_field = get_field().call(vec![
            datafusion_expr::cast(col("s"), cast_type),
            lit("value"),
        ]);
        let projection = logical2physical(&cast_field, &file_schema);
        let cast_predicate = logical2physical(&cast_field.gt(lit(5)), &file_schema);
        assert!(!can_expr_be_pushed_down_with_schemas(
            &cast_predicate,
            &file_schema
        ));
        let candidate =
            FilterCandidateBuilder::new(cast_predicate, Arc::clone(&file_schema))
                .build(&metadata)
                .expect("building cast candidate")
                .expect("an adapted struct cast must remain evaluable");
        // Clip the unused sibling, but preserve the failing `label` conversion.
        let expected_mask =
            ProjectionMask::leaves(metadata.file_metadata().schema_descr(), [1, 2]);
        assert_eq!(candidate.read_plan.projection_mask, expected_mask);
        let DataType::Struct(physical_fields) = file_schema.field(1).data_type() else {
            unreachable!("s is a struct")
        };
        let clipped_field =
            file_schema
                .field(1)
                .clone()
                .with_data_type(DataType::Struct(
                    physical_fields.iter().take(2).cloned().collect(),
                ));
        assert_eq!(
            candidate.read_plan.projected_schema.as_ref(),
            &Schema::new(vec![clipped_field])
        );
        assert_eq!(
            candidate.required_bytes,
            (metadata.row_group(0).column(1).compressed_size()
                + metadata.row_group(0).column(2).compressed_size()) as usize,
            "filter cost must count only the leaves the cast target reads"
        );

        // A simultaneous direct access must not prune siblings that the cast
        // needs in the output projection either.
        let projection_plan = crate::projection_read_plan::build_projection_read_plan(
            [expr, projection],
            &file_schema,
            metadata.file_metadata().schema_descr(),
        );
        assert_eq!(projection_plan.projection_mask, expected_mask);
        assert_eq!(
            projection_plan.projected_schema,
            candidate.read_plan.projected_schema
        );

        let mut row_filter = DatafusionArrowPredicate::try_new(
            candidate,
            Count::new(),
            Count::new(),
            Time::new(),
        )
        .unwrap();
        let batch = builder
            .with_projection(row_filter.projection().clone())
            .build()
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        let error = row_filter.evaluate(batch).unwrap_err().to_string();
        datafusion_common::assert_contains!(error, "While casting struct field 'label'");

        // A retained cast whose target names only the selected field — the
        // shape `retain_field_path` produces for an evolved decimal — clips the
        // read to that field's leaf instead of decoding the whole root.
        let narrow_cast_type =
            DataType::Struct(vec![Field::new("value", DataType::Int32, false)].into());
        let narrow_field = get_field().call(vec![
            datafusion_expr::cast(col("s"), narrow_cast_type.clone()),
            lit("value"),
        ]);
        let narrow_predicate = logical2physical(&narrow_field.gt(lit(5)), &file_schema);
        let candidate =
            FilterCandidateBuilder::new(narrow_predicate, Arc::clone(&file_schema))
                .build(&metadata)
                .expect("building narrow cast candidate")
                .expect("a clipped struct cast must remain evaluable");
        assert_eq!(
            candidate.read_plan.projection_mask,
            ProjectionMask::leaves(metadata.file_metadata().schema_descr(), [1]),
            "the read must be clipped to the leaf the cast target names"
        );
        assert_eq!(candidate.read_plan.projected_schema.fields().len(), 1);
        assert_eq!(
            candidate.read_plan.projected_schema.field(0).data_type(),
            &narrow_cast_type,
            "sibling leaves must be pruned from the filter schema"
        );

        // The clipped schema must still evaluate: every row has value > 5.
        let mut row_filter = DatafusionArrowPredicate::try_new(
            candidate,
            Count::new(),
            Count::new(),
            Time::new(),
        )
        .unwrap();
        let batch = ParquetRecordBatchReaderBuilder::try_new(file.reopen().unwrap())
            .unwrap()
            .with_projection(row_filter.projection().clone())
            .build()
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        assert_eq!(
            row_filter.evaluate(batch).unwrap(),
            BooleanArray::from(vec![true, true, true])
        );
    }

    /// Deeply nested get_field: get_field(struct_col, 'outer', 'inner') where the
    /// leaf is primitive should allow pushdown. The logical simplifier flattens
    /// nested get_field(get_field(col, 'a'), 'b') into get_field(col, 'a', 'b').
    #[test]
    fn get_field_deeply_nested_allows_pushdown() {
        let table_schema = Arc::new(Schema::new(vec![Field::new(
            "s",
            DataType::Struct(
                vec![Arc::new(Field::new(
                    "outer",
                    DataType::Struct(
                        vec![Arc::new(Field::new("inner", DataType::Int32, true))].into(),
                    ),
                    true,
                ))]
                .into(),
            ),
            true,
        )]));

        // s['outer']['inner'] > 5
        let get_field_expr = get_field().call(vec![
            col("s"),
            Expr::Literal(ScalarValue::Utf8(Some("outer".to_string())), None),
            Expr::Literal(ScalarValue::Utf8(Some("inner".to_string())), None),
        ]);
        let expr = get_field_expr.gt(Expr::Literal(ScalarValue::Int32(Some(5)), None));
        let expr = logical2physical(&expr, &table_schema);

        assert!(can_expr_be_pushed_down_with_schemas(&expr, &table_schema));
    }

    /// End-to-end: deeply nested get_field filter produces correct leaf indices
    /// and the filter actually works against a Parquet file.
    #[test]
    fn get_field_deeply_nested_filter_candidate() {
        use arrow::array::{Int32Array, StringArray, StructArray};

        // Schema: id (Int32), s (Struct{outer: Struct{extra: Int32, inner: Int32}, tag: Utf8})
        // Parquet leaves: id=0, s.outer.extra=1, s.outer.inner=2, s.tag=3
        let inner_fields: Fields = vec![
            Arc::new(Field::new("extra", DataType::Int32, false)),
            Arc::new(Field::new("inner", DataType::Int32, false)),
        ]
        .into();
        let outer_fields: Fields = vec![
            Arc::new(Field::new(
                "outer",
                DataType::Struct(inner_fields.clone()),
                false,
            )),
            Arc::new(Field::new("tag", DataType::Utf8, false)),
        ]
        .into();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("s", DataType::Struct(outer_fields.clone()), false),
        ]));

        let inner_struct = StructArray::new(
            inner_fields,
            vec![
                Arc::new(Int32Array::from(vec![100, 200, 300])) as _,
                Arc::new(Int32Array::from(vec![10, 20, 30])) as _,
            ],
            None,
        );
        let outer_struct = StructArray::new(
            outer_fields,
            vec![
                Arc::new(inner_struct) as _,
                Arc::new(StringArray::from(vec!["x", "y", "z"])) as _,
            ],
            None,
        );
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(outer_struct),
            ],
        )
        .unwrap();

        let file = NamedTempFile::new().expect("temp file");
        let mut writer =
            ArrowWriter::try_new(file.reopen().unwrap(), Arc::clone(&schema), None)
                .expect("writer");
        writer.write(&batch).expect("write batch");
        writer.close().expect("close writer");

        let reader_file = file.reopen().expect("reopen file");
        let builder = ParquetRecordBatchReaderBuilder::try_new(reader_file)
            .expect("reader builder");
        let metadata = builder.metadata().clone();
        let file_schema = builder.schema().clone();

        // Parquet should have 4 leaves: id=0, s.outer.extra=1, s.outer.inner=2, s.tag=3
        assert_eq!(metadata.file_metadata().schema_descr().num_columns(), 4);

        // get_field(s, 'outer', 'inner') > 15
        // Should only need leaf 2 (s.outer.inner), not leaf 1 (s.outer.extra) or leaf 3 (s.tag).
        let get_field_expr = get_field().call(vec![
            col("s"),
            Expr::Literal(ScalarValue::Utf8(Some("outer".to_string())), None),
            Expr::Literal(ScalarValue::Utf8(Some("inner".to_string())), None),
        ]);
        let expr = get_field_expr.gt(Expr::Literal(ScalarValue::Int32(Some(15)), None));
        let expr = logical2physical(&expr, &file_schema);

        let candidate = FilterCandidateBuilder::new(expr, file_schema)
            .build(&metadata)
            .expect("building candidate")
            .expect("deeply nested get_field filter should be pushable");

        // Only s.outer.inner (leaf 2) should be projected,
        let expected_mask =
            ProjectionMask::leaves(metadata.file_metadata().schema_descr(), [2]);
        assert_eq!(
            candidate.read_plan.projection_mask, expected_mask,
            "projection_mask should select only leaf 2 for s.outer.inner, skipping sibling and cousin leaves"
        );
    }

    /// End-to-end: get_field filter on a struct column with multiple fields
    /// reads only the needed leaf and correctly filters rows during Parquet decoding.
    #[test]
    fn get_field_end_to_end_filters_rows() {
        // Schema: id (Int32), s (Struct{value: Int32, label: Utf8})
        // Parquet leaves: id=0, s.value=1, s.label=2
        let struct_fields: Fields = vec![
            Arc::new(Field::new("value", DataType::Int32, false)),
            Arc::new(Field::new("label", DataType::Utf8, false)),
        ]
        .into();

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("s", DataType::Struct(struct_fields.clone()), false),
        ]));

        // +----+--------------------------+
        // | id | s                        |
        // +----+--------------------------+
        // |  1 | {value: 10, label: "a"}  |
        // |  2 | {value: 20, label: "b"}  |
        // |  3 | {value: 30, label: "c"}  |
        // +----+--------------------------+
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StructArray::new(
                    struct_fields,
                    vec![
                        Arc::new(Int32Array::from(vec![10, 20, 30])) as _,
                        Arc::new(StringArray::from(vec!["a", "b", "c"])) as _,
                    ],
                    None,
                )),
            ],
        )
        .unwrap();

        let file = NamedTempFile::new().expect("temp file");
        let mut writer =
            ArrowWriter::try_new(file.reopen().unwrap(), Arc::clone(&schema), None)
                .expect("writer");
        writer.write(&batch).expect("write batch");
        writer.close().expect("close writer");

        let reader_file = file.reopen().expect("reopen file");
        let parquet_reader_builder =
            ParquetRecordBatchReaderBuilder::try_new(reader_file)
                .expect("reader builder");
        let metadata = parquet_reader_builder.metadata().clone();
        let file_schema = parquet_reader_builder.schema().clone();

        // get_field(s, 'value') > 15  — should match rows with value=20 and value=30
        let get_field_expr = get_field().call(vec![
            col("s"),
            Expr::Literal(ScalarValue::Utf8(Some("value".to_string())), None),
        ]);
        let predicate_expr =
            get_field_expr.gt(Expr::Literal(ScalarValue::Int32(Some(15)), None));
        let expr = logical2physical(&predicate_expr, &file_schema);

        let metrics = ExecutionPlanMetricsSet::new();
        let file_metrics = ParquetFileMetrics::new(0, "struct_e2e.parquet", &metrics);

        let (row_filter, rejected) =
            build_row_filter(&expr, &file_schema, &metadata, false, &file_metrics)
                .expect("building row filter");
        assert!(
            rejected.is_empty(),
            "expected no rejected conjuncts, got {rejected:?}"
        );
        let row_filter = row_filter.expect("row filter should exist");

        let reader = parquet_reader_builder
            .with_row_filter(row_filter)
            .build()
            .expect("build reader");

        let mut total_rows = 0;
        for batch in reader {
            let batch = batch.expect("record batch");
            total_rows += batch.num_rows();
        }

        assert_eq!(total_rows, 2, "expected 2 rows matching value > 15");
        assert_eq!(file_metrics.pushdown_rows_pruned.value(), 1);
        assert_eq!(file_metrics.pushdown_rows_matched.value(), 2);
    }

    /// Sanity check that the given expression could be evaluated against the given schema without any errors.
    /// This will fail if the expression references columns that are not in the schema or if the types of the columns are incompatible, etc.
    fn check_expression_can_evaluate_against_schema(
        expr: &Arc<dyn PhysicalExpr>,
        table_schema: &Arc<Schema>,
    ) -> bool {
        let batch = RecordBatch::new_empty(Arc::clone(table_schema));
        expr.evaluate(&batch).is_ok()
    }

    /// Multiple sibling fields under one struct root: `s['value'] AND s['label']`.
    /// The projection mask should include exactly those two leaves (not the third
    /// sibling), and the projected schema should be pruned to those siblings.
    #[test]
    fn get_field_multiple_fields_under_same_root_uses_only_those_leaves() {
        // Schema: s (Struct{value: Int32, label: Utf8, extra: Int32})
        // Parquet leaves: s.value=0, s.label=1, s.extra=2
        let struct_fields: Fields = vec![
            Arc::new(Field::new("value", DataType::Int32, false)),
            Arc::new(Field::new("label", DataType::Utf8, false)),
            Arc::new(Field::new("extra", DataType::Int32, false)),
        ]
        .into();

        let schema = Arc::new(Schema::new(vec![Field::new(
            "s",
            DataType::Struct(struct_fields.clone()),
            false,
        )]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(StructArray::new(
                struct_fields.clone(),
                vec![
                    Arc::new(Int32Array::from(vec![10, 20, 30])) as _,
                    Arc::new(StringArray::from(vec!["a", "b", "c"])) as _,
                    Arc::new(Int32Array::from(vec![100, 200, 300])) as _,
                ],
                None,
            ))],
        )
        .unwrap();

        let file = NamedTempFile::new().expect("temp file");
        let mut writer =
            ArrowWriter::try_new(file.reopen().unwrap(), Arc::clone(&schema), None)
                .expect("writer");
        writer.write(&batch).expect("write batch");
        writer.close().expect("close writer");

        let reader_file = file.reopen().expect("reopen file");
        let builder = ParquetRecordBatchReaderBuilder::try_new(reader_file)
            .expect("reader builder");
        let metadata = builder.metadata().clone();
        let file_schema = builder.schema().clone();

        // s['value'] > 5 AND s['label'] = 'b'
        let value_expr = get_field()
            .call(vec![
                col("s"),
                Expr::Literal(ScalarValue::Utf8(Some("value".to_string())), None),
            ])
            .gt(Expr::Literal(ScalarValue::Int32(Some(5)), None));
        let label_expr = get_field()
            .call(vec![
                col("s"),
                Expr::Literal(ScalarValue::Utf8(Some("label".to_string())), None),
            ])
            .eq(Expr::Literal(
                ScalarValue::Utf8(Some("b".to_string())),
                None,
            ));
        let expr = logical2physical(&value_expr.and(label_expr), &file_schema);

        let candidate = FilterCandidateBuilder::new(expr, Arc::clone(&file_schema))
            .build(&metadata)
            .expect("building candidate")
            .expect("conjunction of two get_field predicates should be pushable");

        // Only s.value (leaf 0) and s.label (leaf 1) should be projected; s.extra (leaf 2) skipped.
        let expected_mask =
            ProjectionMask::leaves(metadata.file_metadata().schema_descr(), [0, 1]);
        assert_eq!(
            candidate.read_plan.projection_mask, expected_mask,
            "projection_mask should include only the two accessed sibling leaves"
        );

        let s_field = candidate
            .read_plan
            .projected_schema
            .field_with_name("s")
            .unwrap();
        let expected_pruned: Fields = vec![
            Arc::new(Field::new("value", DataType::Int32, false)),
            Arc::new(Field::new("label", DataType::Utf8, false)),
        ]
        .into();
        assert_eq!(
            s_field.data_type(),
            &DataType::Struct(expected_pruned),
            "projected struct schema should drop the un-accessed `extra` sibling"
        );
    }

    /// Two predicates share a nested prefix: `s['outer']['a'] AND s['outer']['b']`.
    /// The projection mask should include exactly those two leaves and exclude
    /// the cousin under `s['other']` plus `s['outer']['c']`. The projected
    /// schema must mirror that shape.
    #[test]
    fn get_field_nested_shared_prefix_uses_only_prefix_leaves() {
        // Schema: s (Struct{outer: Struct{a, b, c}, other: Struct{x}})
        // Parquet leaves: s.outer.a=0, s.outer.b=1, s.outer.c=2, s.other.x=3
        let outer_fields: Fields = vec![
            Arc::new(Field::new("a", DataType::Int32, false)),
            Arc::new(Field::new("b", DataType::Int32, false)),
            Arc::new(Field::new("c", DataType::Int32, false)),
        ]
        .into();
        let other_fields: Fields =
            vec![Arc::new(Field::new("x", DataType::Int32, false))].into();
        let s_fields: Fields = vec![
            Arc::new(Field::new(
                "outer",
                DataType::Struct(outer_fields.clone()),
                false,
            )),
            Arc::new(Field::new(
                "other",
                DataType::Struct(other_fields.clone()),
                false,
            )),
        ]
        .into();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "s",
            DataType::Struct(s_fields.clone()),
            false,
        )]));

        let outer_arr = StructArray::new(
            outer_fields,
            vec![
                Arc::new(Int32Array::from(vec![1, 2])) as _,
                Arc::new(Int32Array::from(vec![10, 20])) as _,
                Arc::new(Int32Array::from(vec![100, 200])) as _,
            ],
            None,
        );
        let other_arr = StructArray::new(
            other_fields,
            vec![Arc::new(Int32Array::from(vec![7, 8])) as _],
            None,
        );
        let s_arr = StructArray::new(
            s_fields,
            vec![Arc::new(outer_arr) as _, Arc::new(other_arr) as _],
            None,
        );
        let batch =
            RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(s_arr)]).unwrap();

        let file = NamedTempFile::new().expect("temp file");
        let mut writer =
            ArrowWriter::try_new(file.reopen().unwrap(), Arc::clone(&schema), None)
                .expect("writer");
        writer.write(&batch).expect("write batch");
        writer.close().expect("close writer");

        let reader_file = file.reopen().expect("reopen file");
        let builder = ParquetRecordBatchReaderBuilder::try_new(reader_file)
            .expect("reader builder");
        let metadata = builder.metadata().clone();
        let file_schema = builder.schema().clone();

        // s['outer']['a'] > 0 AND s['outer']['b'] > 0
        let a_expr = get_field()
            .call(vec![
                col("s"),
                Expr::Literal(ScalarValue::Utf8(Some("outer".to_string())), None),
                Expr::Literal(ScalarValue::Utf8(Some("a".to_string())), None),
            ])
            .gt(Expr::Literal(ScalarValue::Int32(Some(0)), None));
        let b_expr = get_field()
            .call(vec![
                col("s"),
                Expr::Literal(ScalarValue::Utf8(Some("outer".to_string())), None),
                Expr::Literal(ScalarValue::Utf8(Some("b".to_string())), None),
            ])
            .gt(Expr::Literal(ScalarValue::Int32(Some(0)), None));
        let expr = logical2physical(&a_expr.and(b_expr), &file_schema);

        let candidate = FilterCandidateBuilder::new(expr, Arc::clone(&file_schema))
            .build(&metadata)
            .expect("building candidate")
            .expect("shared-prefix nested predicates should be pushable");

        // Only s.outer.a (0) and s.outer.b (1) — not s.outer.c (2), not s.other.x (3).
        let expected_mask =
            ProjectionMask::leaves(metadata.file_metadata().schema_descr(), [0, 1]);
        assert_eq!(
            candidate.read_plan.projection_mask, expected_mask,
            "projection_mask should drop cousin and un-accessed sibling leaves"
        );

        let s_field = candidate
            .read_plan
            .projected_schema
            .field_with_name("s")
            .unwrap();
        let expected_inner: Fields = vec![
            Arc::new(Field::new("a", DataType::Int32, false)),
            Arc::new(Field::new("b", DataType::Int32, false)),
        ]
        .into();
        let expected_outer: Fields = vec![Arc::new(Field::new(
            "outer",
            DataType::Struct(expected_inner),
            false,
        ))]
        .into();
        assert_eq!(
            s_field.data_type(),
            &DataType::Struct(expected_outer),
            "projected schema should keep only the shared-prefix subtree"
        );
    }

    /// Two predicates touch disjoint subtrees of the same struct root:
    /// `s['outer']['a'] AND s['other']['x']`. Both subtrees must be retained
    /// in the projection mask and in the projected schema.
    #[test]
    fn get_field_disjoint_subtrees_keep_both() {
        // Schema: s (Struct{outer: Struct{a, b}, other: Struct{x, y}})
        // Parquet leaves: s.outer.a=0, s.outer.b=1, s.other.x=2, s.other.y=3
        let outer_fields: Fields = vec![
            Arc::new(Field::new("a", DataType::Int32, false)),
            Arc::new(Field::new("b", DataType::Int32, false)),
        ]
        .into();
        let other_fields: Fields = vec![
            Arc::new(Field::new("x", DataType::Int32, false)),
            Arc::new(Field::new("y", DataType::Int32, false)),
        ]
        .into();
        let s_fields: Fields = vec![
            Arc::new(Field::new(
                "outer",
                DataType::Struct(outer_fields.clone()),
                false,
            )),
            Arc::new(Field::new(
                "other",
                DataType::Struct(other_fields.clone()),
                false,
            )),
        ]
        .into();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "s",
            DataType::Struct(s_fields.clone()),
            false,
        )]));

        let outer_arr = StructArray::new(
            outer_fields,
            vec![
                Arc::new(Int32Array::from(vec![1, 2])) as _,
                Arc::new(Int32Array::from(vec![3, 4])) as _,
            ],
            None,
        );
        let other_arr = StructArray::new(
            other_fields,
            vec![
                Arc::new(Int32Array::from(vec![5, 6])) as _,
                Arc::new(Int32Array::from(vec![7, 8])) as _,
            ],
            None,
        );
        let s_arr = StructArray::new(
            s_fields,
            vec![Arc::new(outer_arr) as _, Arc::new(other_arr) as _],
            None,
        );
        let batch =
            RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(s_arr)]).unwrap();

        let file = NamedTempFile::new().expect("temp file");
        let mut writer =
            ArrowWriter::try_new(file.reopen().unwrap(), Arc::clone(&schema), None)
                .expect("writer");
        writer.write(&batch).expect("write batch");
        writer.close().expect("close writer");

        let reader_file = file.reopen().expect("reopen file");
        let builder = ParquetRecordBatchReaderBuilder::try_new(reader_file)
            .expect("reader builder");
        let metadata = builder.metadata().clone();
        let file_schema = builder.schema().clone();

        // s['outer']['a'] > 0 AND s['other']['x'] > 0
        let a_expr = get_field()
            .call(vec![
                col("s"),
                Expr::Literal(ScalarValue::Utf8(Some("outer".to_string())), None),
                Expr::Literal(ScalarValue::Utf8(Some("a".to_string())), None),
            ])
            .gt(Expr::Literal(ScalarValue::Int32(Some(0)), None));
        let x_expr = get_field()
            .call(vec![
                col("s"),
                Expr::Literal(ScalarValue::Utf8(Some("other".to_string())), None),
                Expr::Literal(ScalarValue::Utf8(Some("x".to_string())), None),
            ])
            .gt(Expr::Literal(ScalarValue::Int32(Some(0)), None));
        let expr = logical2physical(&a_expr.and(x_expr), &file_schema);

        let candidate = FilterCandidateBuilder::new(expr, Arc::clone(&file_schema))
            .build(&metadata)
            .expect("building candidate")
            .expect("disjoint nested predicates should be pushable");

        // s.outer.a (0) and s.other.x (2); not s.outer.b (1), not s.other.y (3).
        let expected_mask =
            ProjectionMask::leaves(metadata.file_metadata().schema_descr(), [0, 2]);
        assert_eq!(
            candidate.read_plan.projection_mask, expected_mask,
            "projection_mask should keep one leaf from each disjoint subtree"
        );

        let s_field = candidate
            .read_plan
            .projected_schema
            .field_with_name("s")
            .unwrap();
        let expected_outer: Fields =
            vec![Arc::new(Field::new("a", DataType::Int32, false))].into();
        let expected_other: Fields =
            vec![Arc::new(Field::new("x", DataType::Int32, false))].into();
        let expected_s: Fields = vec![
            Arc::new(Field::new("outer", DataType::Struct(expected_outer), false)),
            Arc::new(Field::new("other", DataType::Struct(expected_other), false)),
        ]
        .into();
        assert_eq!(
            s_field.data_type(),
            &DataType::Struct(expected_s),
            "projected schema should keep one pruned field from each disjoint subtree"
        );
    }

    /// End-to-end: shared-prefix nested predicates filter rows correctly during
    /// Parquet decoding and report the expected pushdown metrics.
    #[test]
    fn get_field_end_to_end_shared_prefix_filters_rows() {
        // Schema: id (Int32), s (Struct{outer: Struct{a, b}})
        // Parquet leaves: id=0, s.outer.a=1, s.outer.b=2
        let outer_fields: Fields = vec![
            Arc::new(Field::new("a", DataType::Int32, false)),
            Arc::new(Field::new("b", DataType::Int32, false)),
        ]
        .into();
        let s_fields: Fields = vec![Arc::new(Field::new(
            "outer",
            DataType::Struct(outer_fields.clone()),
            false,
        ))]
        .into();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("s", DataType::Struct(s_fields.clone()), false),
        ]));

        // +----+--------------------------+
        // | id | s                        |
        // +----+--------------------------+
        // |  1 | {outer: {a: 10, b: 50}}  |  <- a>5 and b<100 → match
        // |  2 | {outer: {a:  0, b: 60}}  |  <- a>5 fails    → drop
        // |  3 | {outer: {a: 20, b: 80}}  |  <- a>5 and b<100 → match
        // |  4 | {outer: {a: 30, b: 200}} |  <- b<100 fails  → drop
        // +----+--------------------------+
        let outer_arr = StructArray::new(
            outer_fields,
            vec![
                Arc::new(Int32Array::from(vec![10, 0, 20, 30])) as _,
                Arc::new(Int32Array::from(vec![50, 60, 80, 200])) as _,
            ],
            None,
        );
        let s_arr = StructArray::new(s_fields, vec![Arc::new(outer_arr) as _], None);
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
                Arc::new(s_arr),
            ],
        )
        .unwrap();

        let file = NamedTempFile::new().expect("temp file");
        let mut writer =
            ArrowWriter::try_new(file.reopen().unwrap(), Arc::clone(&schema), None)
                .expect("writer");
        writer.write(&batch).expect("write batch");
        writer.close().expect("close writer");

        let reader_file = file.reopen().expect("reopen file");
        let parquet_reader_builder =
            ParquetRecordBatchReaderBuilder::try_new(reader_file)
                .expect("reader builder");
        let metadata = parquet_reader_builder.metadata().clone();
        let file_schema = parquet_reader_builder.schema().clone();

        // s['outer']['a'] > 5 AND s['outer']['b'] < 100
        let a_expr = get_field()
            .call(vec![
                col("s"),
                Expr::Literal(ScalarValue::Utf8(Some("outer".to_string())), None),
                Expr::Literal(ScalarValue::Utf8(Some("a".to_string())), None),
            ])
            .gt(Expr::Literal(ScalarValue::Int32(Some(5)), None));
        let b_expr = get_field()
            .call(vec![
                col("s"),
                Expr::Literal(ScalarValue::Utf8(Some("outer".to_string())), None),
                Expr::Literal(ScalarValue::Utf8(Some("b".to_string())), None),
            ])
            .lt(Expr::Literal(ScalarValue::Int32(Some(100)), None));
        let expr = logical2physical(&a_expr.and(b_expr), &file_schema);

        let metrics = ExecutionPlanMetricsSet::new();
        let file_metrics =
            ParquetFileMetrics::new(0, "shared_prefix_e2e.parquet", &metrics);

        let (row_filter, rejected) =
            build_row_filter(&expr, &file_schema, &metadata, false, &file_metrics)
                .expect("building row filter");
        assert!(
            rejected.is_empty(),
            "expected no rejected conjuncts, got {rejected:?}"
        );
        let row_filter = row_filter.expect("row filter should exist");

        let reader = parquet_reader_builder
            .with_row_filter(row_filter)
            .build()
            .expect("build reader");

        let mut total_rows = 0;
        for batch in reader {
            let batch = batch.expect("record batch");
            total_rows += batch.num_rows();
        }

        assert_eq!(
            total_rows, 2,
            "expected 2 rows matching s.outer.a > 5 AND s.outer.b < 100"
        );
        assert_eq!(file_metrics.pushdown_rows_pruned.value(), 2);
        assert_eq!(file_metrics.pushdown_rows_matched.value(), 2);
    }

    /// Regression test: a predicate `(s IS NOT NULL) AND (id = 1)` mixes a
    /// conjunct that the `RowFilter` machinery cannot evaluate (whole-struct
    /// reference — [`PushdownChecker`] flags it as non-primitive) with one
    /// that it can. `build_row_filter` must return the rejected conjunct in
    /// its second tuple element so the caller can re-route it to a post-scan
    /// filter; before this was fixed the rejected conjunct was silently
    /// dropped on the floor while the parent `FilterExec` had already been
    /// removed, relaxing the predicate and returning wrong results.
    #[test]
    fn build_row_filter_surfaces_rejected_struct_conjunct() {
        let struct_fields: Fields = vec![
            Arc::new(Field::new("value", DataType::Int32, false)),
            Arc::new(Field::new("label", DataType::Utf8, false)),
        ]
        .into();

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("s", DataType::Struct(struct_fields.clone()), false),
        ]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StructArray::new(
                    struct_fields,
                    vec![
                        Arc::new(Int32Array::from(vec![10, 20, 30])) as _,
                        Arc::new(StringArray::from(vec!["a", "b", "c"])) as _,
                    ],
                    None,
                )),
            ],
        )
        .unwrap();

        let file = NamedTempFile::new().expect("temp file");
        let mut writer =
            ArrowWriter::try_new(file.reopen().unwrap(), Arc::clone(&schema), None)
                .expect("writer");
        writer.write(&batch).expect("write batch");
        writer.close().expect("close writer");

        let reader_file = file.reopen().expect("reopen file");
        let parquet_reader_builder =
            ParquetRecordBatchReaderBuilder::try_new(reader_file)
                .expect("reader builder");
        let metadata = parquet_reader_builder.metadata().clone();
        let file_schema = parquet_reader_builder.schema().clone();

        // (s IS NOT NULL) AND (id = 1)
        // The first conjunct references a whole struct -> RowFilter rejects.
        // The second is a plain Int32 equality -> RowFilter accepts.
        let predicate_expr = col("s")
            .is_not_null()
            .and(col("id").eq(Expr::Literal(ScalarValue::Int32(Some(1)), None)));
        let expr = logical2physical(&predicate_expr, &file_schema);

        let metrics = ExecutionPlanMetricsSet::new();
        let file_metrics =
            ParquetFileMetrics::new(0, "build_row_filter_rejected.parquet", &metrics);

        let (row_filter, rejected) =
            build_row_filter(&expr, &file_schema, &metadata, false, &file_metrics)
                .expect("building row filter");

        // The plain id = 1 conjunct produced a RowFilter…
        assert!(
            row_filter.is_some(),
            "id = 1 should have produced a RowFilter"
        );
        // …and the struct IS NOT NULL conjunct must be surfaced as rejected,
        // never silently dropped.
        assert_eq!(
            rejected.len(),
            1,
            "expected exactly one rejected conjunct (s IS NOT NULL), got {rejected:?}"
        );
    }
}

#[cfg(test)]
mod optional_filter_tests {
    use super::*;
    use crate::optional_filter::OptionalFilterOptions;
    use arrow::array::{Array, Int32Array, StructArray};
    use arrow::datatypes::{DataType, Field, Fields};
    use datafusion_common::ScalarValue;
    use datafusion_expr::Operator;
    use datafusion_physical_expr::conjunction;
    use datafusion_physical_expr::expressions::{
        BinaryExpr, DynamicFilterPhysicalExpr, IsNotNullExpr, OptionalFilterPhysicalExpr,
        col, lit,
    };
    use datafusion_physical_expr::optional_filter_gate::OptionalFilterGateConfig;
    use datafusion_physical_expr::simplifier::PhysicalExprSimplifier;
    use datafusion_physical_expr_adapter::{
        DefaultPhysicalExprAdapterFactory, PhysicalExprAdapterFactory,
    };
    use parquet::arrow::ArrowWriter;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::file::properties::WriterProperties;
    use tempfile::NamedTempFile;

    const NUM_ROWS: usize = 10_000;
    const FILENAME: &str = "test.parquet";

    /// A file with `a` (many distinct values: the largest column), `b`
    /// (`i % 10`), `c` (`(i / 10) % 10`) and a struct column `s`.
    fn test_file() -> (NamedTempFile, Arc<ParquetMetaData>, SchemaRef) {
        let s_fields = Fields::from(vec![Field::new("x", DataType::Int32, true)]);
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
            Field::new("s", DataType::Struct(s_fields.clone()), true),
        ]));
        let a: Int32Array = (0..NUM_ROWS as i32).map(|i| i * 7919).collect();
        let b: Int32Array = (0..NUM_ROWS as i32).map(|i| i % 10).collect();
        let c: Int32Array = (0..NUM_ROWS as i32).map(|i| (i / 10) % 10).collect();
        let s = StructArray::new(
            s_fields,
            vec![Arc::new(Int32Array::from(vec![1; NUM_ROWS])) as _],
            None,
        );
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(a), Arc::new(b), Arc::new(c), Arc::new(s)],
        )
        .unwrap();
        let file = NamedTempFile::new().unwrap();
        // Dictionary encoding makes `b` and `c` much smaller than `a`.
        let props = WriterProperties::builder()
            .set_dictionary_enabled(true)
            .build();
        let mut writer = ArrowWriter::try_new(
            file.reopen().unwrap(),
            Arc::clone(&schema),
            Some(props),
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let builder =
            ParquetRecordBatchReaderBuilder::try_new(file.reopen().unwrap()).unwrap();
        let metadata = Arc::clone(builder.metadata());
        let file_schema = Arc::clone(builder.schema());
        (file, metadata, file_schema)
    }

    fn optional(inner: Arc<dyn PhysicalExpr>) -> Arc<dyn PhysicalExpr> {
        Arc::new(OptionalFilterPhysicalExpr::new(inner))
    }

    fn binary(
        left: Arc<dyn PhysicalExpr>,
        op: Operator,
        right: Arc<dyn PhysicalExpr>,
    ) -> Arc<dyn PhysicalExpr> {
        Arc::new(BinaryExpr::new(left, op, right))
    }

    fn col_op(
        name: &str,
        op: Operator,
        value: i32,
        schema: &Schema,
    ) -> Arc<dyn PhysicalExpr> {
        binary(
            col(name, schema).unwrap(),
            op,
            lit(ScalarValue::Int32(Some(value))),
        )
    }

    /// Options with a very large `min_saving_ns_per_row`: the cost check of
    /// the gates (which uses the wall clock) never pauses a filter that
    /// removes rows, thus the tests are deterministic. See
    /// [`options_with_saving`].
    fn options(mode: OptionalFilterMode) -> OptionalFilterOptions {
        options_with_saving(mode, 1e9)
    }

    fn options_with_saving(
        mode: OptionalFilterMode,
        min_saving_ns_per_row: f64,
    ) -> OptionalFilterOptions {
        OptionalFilterOptions {
            mode,
            gate_config: OptionalFilterGateConfig {
                min_saving_ns_per_row,
                ..Default::default()
            },
            decode_cost: Arc::default(),
        }
    }

    fn context<'a>(
        options: &'a OptionalFilterOptions,
        metrics: &'a ExecutionPlanMetricsSet,
    ) -> Option<OptionalFilterRowFilterContext<'a>> {
        Some(OptionalFilterRowFilterContext {
            options,
            metrics,
            partition: 0,
            filename: FILENAME,
            output_projection: None,
        })
    }

    fn prebuild(
        predicate: &Arc<dyn PhysicalExpr>,
        file_schema: &SchemaRef,
        metadata: &ParquetMetaData,
        options: &OptionalFilterOptions,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Vec<PrebuiltRowFilterCandidate> {
        prebuild_row_filter_candidates(
            predicate,
            file_schema,
            metadata,
            context(options, metrics),
        )
        .unwrap()
        .0
        .unwrap_or_default()
    }

    /// `(expression, is_optional)` of each candidate, in evaluation order.
    fn describe(
        candidates: &[PrebuiltRowFilterCandidate],
        reorder: bool,
    ) -> Vec<(String, bool)> {
        order_candidates(candidates, reorder)
            .into_iter()
            .map(|c| (c.physical_expr.to_string(), c.is_optional()))
            .collect()
    }

    fn entry(expr: &str, optional: bool) -> (String, bool) {
        (expr.to_string(), optional)
    }

    #[test]
    fn candidates_for_each_mode() {
        let (_file, metadata, file_schema) = test_file();
        let metrics = ExecutionPlanMetricsSet::new();
        // Optional(b > 3) AND a > 5 AND Optional(c < 10)
        let predicate = conjunction([
            optional(col_op("b", Operator::Gt, 3, &file_schema)),
            col_op("a", Operator::Gt, 5, &file_schema),
            optional(col_op("c", Operator::Lt, 10, &file_schema)),
        ]);

        // always: the same candidates as without optional filter handling.
        let options_always = options(OptionalFilterMode::Always);
        let candidates = prebuild(
            &predicate,
            &file_schema,
            &metadata,
            &options_always,
            &metrics,
        );
        assert_eq!(
            describe(&candidates, false),
            vec![
                entry("Optional(b@0 > 3)", false),
                entry("a@0 > 5", false),
                entry("Optional(c@0 < 10)", false),
            ]
        );
        let without_context =
            prebuild_row_filter_candidates(&predicate, &file_schema, &metadata, None)
                .unwrap()
                .0
                .unwrap();
        assert_eq!(
            describe(&without_context, false),
            describe(&candidates, false)
        );

        // pruning_only: only the required conjunct.
        let options_pruning = options(OptionalFilterMode::PruningOnly);
        let candidates = prebuild(
            &predicate,
            &file_schema,
            &metadata,
            &options_pruning,
            &metrics,
        );
        assert_eq!(describe(&candidates, false), vec![entry("a@0 > 5", false)]);

        // pruning_only with optional conjuncts only: no row filter.
        let only_optional = optional(col_op("b", Operator::Gt, 3, &file_schema));
        let prebuilt = prebuild_row_filter_candidates(
            &only_optional,
            &file_schema,
            &metadata,
            context(&options_pruning, &metrics),
        )
        .unwrap();
        assert!(prebuilt.0.is_none());
        assert!(prebuilt.1.is_empty());

        // adaptive: required first, then each optional inner with a gate.
        let options_adaptive = options(OptionalFilterMode::Adaptive);
        let candidates = prebuild(
            &predicate,
            &file_schema,
            &metadata,
            &options_adaptive,
            &metrics,
        );
        assert_eq!(
            describe(&candidates, false),
            vec![
                entry("a@0 > 5", false),
                entry("b@0 > 3", true),
                entry("c@0 < 10", true),
            ]
        );
    }

    #[test]
    fn optional_candidates_stay_last_when_reordered() {
        let (_file, metadata, file_schema) = test_file();
        let metrics = ExecutionPlanMetricsSet::new();
        // `a` is the largest column, thus a reorder puts it last among the
        // required conjuncts. The optional conjunct on the small column `b`
        // still comes after all required conjuncts.
        let predicate = conjunction([
            optional(col_op("b", Operator::Gt, 3, &file_schema)),
            col_op("a", Operator::Gt, 5, &file_schema),
            col_op("c", Operator::Lt, 10, &file_schema),
        ]);
        let options = options(OptionalFilterMode::Adaptive);
        let candidates =
            prebuild(&predicate, &file_schema, &metadata, &options, &metrics);
        assert_eq!(
            describe(&candidates, true),
            vec![
                entry("c@0 < 10", false),
                entry("a@0 > 5", false),
                entry("b@0 > 3", true),
            ]
        );
        assert_eq!(
            describe(&candidates, false),
            vec![
                entry("a@0 > 5", false),
                entry("c@0 < 10", false),
                entry("b@0 > 3", true),
            ]
        );
    }

    #[test]
    fn rejected_optional_conjunct_is_dropped() {
        let (_file, metadata, file_schema) = test_file();
        let metrics = ExecutionPlanMetricsSet::new();
        // A filter on a whole struct column cannot be pushed down.
        let struct_filter: Arc<dyn PhysicalExpr> =
            Arc::new(IsNotNullExpr::new(col("s", &file_schema).unwrap()));
        let predicate = conjunction([
            col_op("a", Operator::Gt, 5, &file_schema),
            optional(struct_filter),
            optional(col_op("b", Operator::Gt, 3, &file_schema)),
        ]);
        let options = options(OptionalFilterMode::Adaptive);
        let candidates =
            prebuild(&predicate, &file_schema, &metadata, &options, &metrics);
        assert_eq!(
            describe(&candidates, false),
            vec![entry("a@0 > 5", false), entry("b@0 > 3", true)]
        );
    }

    /// Read the file with a `RowFilter` from `candidates`, in batches of 100
    /// rows. Returns the number of output rows.
    fn read_with_row_filter(
        file: &NamedTempFile,
        candidates: &[PrebuiltRowFilterCandidate],
        file_metrics: &ParquetFileMetrics,
    ) -> usize {
        let row_filter = row_filter_from_prebuilt(candidates, false, file_metrics);
        let reader = ParquetRecordBatchReaderBuilder::try_new(file.reopen().unwrap())
            .unwrap()
            .with_batch_size(100)
            .with_row_filter(row_filter)
            .build()
            .unwrap();
        reader.map(|batch| batch.unwrap().num_rows()).sum()
    }

    fn metric(metrics: &ExecutionPlanMetricsSet, name: &str) -> usize {
        metrics
            .clone_inner()
            .sum_by_name(name)
            .map_or(0, |v| v.as_usize())
    }

    #[test]
    fn adaptive_pauses_optional_filter_that_removes_no_rows() {
        let (file, metadata, file_schema) = test_file();
        // All rows pass `b >= 0`: the filter removes no rows, thus the gate
        // pauses it.
        let predicate = optional(col_op("b", Operator::GtEq, 0, &file_schema));

        let metrics = ExecutionPlanMetricsSet::new();
        let file_metrics = ParquetFileMetrics::new(0, FILENAME, &metrics);
        let options_always = options(OptionalFilterMode::Always);
        let candidates = prebuild(
            &predicate,
            &file_schema,
            &metadata,
            &options_always,
            &metrics,
        );
        let rows_always = read_with_row_filter(&file, &candidates, &file_metrics);
        assert_eq!(rows_always, NUM_ROWS);
        assert_eq!(metric(&metrics, "optional_filter_rows_skipped"), 0);
        assert_eq!(metric(&metrics, "optional_filter_pauses"), 0);

        let metrics = ExecutionPlanMetricsSet::new();
        let file_metrics = ParquetFileMetrics::new(0, FILENAME, &metrics);
        let options_adaptive = options(OptionalFilterMode::Adaptive);
        let candidates = prebuild(
            &predicate,
            &file_schema,
            &metadata,
            &options_adaptive,
            &metrics,
        );
        let rows_adaptive = read_with_row_filter(&file, &candidates, &file_metrics);

        let skipped = metric(&metrics, "optional_filter_rows_skipped");
        assert!(skipped > 0, "expected skipped rows");
        assert!(metric(&metrics, "optional_filter_pauses") > 0);
        // Skipped rows pass without evaluation.
        assert_eq!(rows_adaptive, rows_always);
        assert_eq!(file_metrics.pushdown_rows_matched.value(), rows_adaptive);

        // Gates do not share state: the gate for the next file starts to
        // evaluate the filter.
        let candidates = prebuild(
            &predicate,
            &file_schema,
            &metadata,
            &options_adaptive,
            &metrics,
        );
        let gate = candidates[0].gate.as_ref().unwrap();
        assert!(!gate.lock().gate.is_paused());
    }

    #[test]
    fn adaptive_keeps_selective_optional_filter() {
        let (file, metadata, file_schema) = test_file();
        // 10% of the rows pass `b = 1`.
        let predicate = optional(col_op("b", Operator::Eq, 1, &file_schema));
        let metrics = ExecutionPlanMetricsSet::new();
        let file_metrics = ParquetFileMetrics::new(0, FILENAME, &metrics);
        let options = options(OptionalFilterMode::Adaptive);
        let candidates =
            prebuild(&predicate, &file_schema, &metadata, &options, &metrics);
        let rows = read_with_row_filter(&file, &candidates, &file_metrics);
        assert_eq!(rows, NUM_ROWS / 10);
        assert_eq!(metric(&metrics, "optional_filter_rows_skipped"), 0);
        assert_eq!(metric(&metrics, "optional_filter_pauses"), 0);
    }

    /// The cost check pauses a selective optional filter that costs more
    /// than it saves. With no output projection (thus no measured saving)
    /// and `min_saving_ns_per_row = 0`, a removed row saves nothing and any
    /// evaluation time is too much: the gate pauses `b = 1` although it
    /// keeps only 10% of the rows.
    #[test]
    fn adaptive_pauses_expensive_optional_filter() {
        let (file, metadata, file_schema) = test_file();
        let predicate = optional(col_op("b", Operator::Eq, 1, &file_schema));
        let metrics = ExecutionPlanMetricsSet::new();
        let file_metrics = ParquetFileMetrics::new(0, FILENAME, &metrics);
        let options = options_with_saving(OptionalFilterMode::Adaptive, 0.0);
        let candidates =
            prebuild(&predicate, &file_schema, &metadata, &options, &metrics);
        let rows = read_with_row_filter(&file, &candidates, &file_metrics);
        let skipped = metric(&metrics, "optional_filter_rows_skipped");
        assert!(skipped > 0, "expected skipped rows");
        assert!(metric(&metrics, "optional_filter_pauses") > 0);
        assert!(metric(&metrics, "optional_filter_eval_time") > 0);
        // Skipped rows pass without evaluation.
        assert_eq!(rows, NUM_ROWS / 10 + skipped * 9 / 10);
    }

    /// With the output projection of the file, the gate of an optional
    /// filter adds the decode time of the output columns that the filter
    /// does not read to the configured minimum saving.
    #[test]
    fn saving_includes_decode_of_unread_output_columns() {
        use crate::optional_filter::{
            DEFAULT_DECODE_NS_PER_BYTE, OptionalFilterSavings, compressed_bytes_per_row,
        };
        let (_file, metadata, file_schema) = test_file();
        // The output columns are `a`, `b` and `c` (leaves 0, 1 and 2). The
        // optional filter reads `b`.
        let output =
            ProjectionMask::leaves(metadata.file_metadata().schema_descr(), [0, 1, 2]);
        let predicate = optional(col_op("b", Operator::Eq, 1, &file_schema));
        let metrics = ExecutionPlanMetricsSet::new();
        let options = options_with_saving(OptionalFilterMode::Adaptive, 5.0);
        let mut context = context(&options, &metrics).unwrap();
        context.output_projection = Some(&output);
        let candidates = prebuild_row_filter_candidates(
            &predicate,
            &file_schema,
            &metadata,
            Some(context),
        )
        .unwrap()
        .0
        .unwrap();
        let saving = candidates[0].optional_saving().unwrap();
        let rows = NUM_ROWS as f64;
        let column_bytes =
            |leaf: usize| metadata.row_group(0).column(leaf).compressed_size() as f64;
        let unread = (column_bytes(0) + column_bytes(2)) / rows;
        assert!(unread > 0.0);
        assert_eq!(saving.unread_bytes_per_row, unread);
        assert_eq!(
            compressed_bytes_per_row(&metadata, |leaf| leaf == 0 || leaf == 2),
            unread
        );
        // Before any measurement: the default decode speed.
        let gate = candidates[0].gate.as_ref().unwrap();
        let expected = 5.0 + DEFAULT_DECODE_NS_PER_BYTE * unread;
        assert!((gate.lock().gate.saving_ns_per_row() - expected).abs() < 1e-9);

        // Output batches of 1000 rows that take 4 ns for each compressed
        // byte of the output columns.
        let output_bytes = (column_bytes(0) + column_bytes(1) + column_bytes(2)) / rows;
        let savings = OptionalFilterSavings::try_new(
            Arc::clone(&options.decode_cost),
            &metadata,
            &output,
            vec![saving.clone()],
        )
        .unwrap();
        let nanos = (4.0 * 1000.0 * output_bytes) as u64;
        savings.record_output_batch(1000, Duration::from_nanos(nanos));
        let ns_per_byte = options.decode_cost.ns_per_byte();
        assert!((ns_per_byte - 4.0).abs() < 0.01, "{ns_per_byte}");
        let expected = 5.0 + ns_per_byte * unread;
        assert!((gate.lock().gate.saving_ns_per_row() - expected).abs() < 1e-9);

        // No gated optional filter: nothing to measure.
        assert!(
            OptionalFilterSavings::try_new(
                Arc::clone(&options.decode_cost),
                &metadata,
                &output,
                vec![],
            )
            .is_none()
        );
    }

    /// The per-file rewrite (schema adaptation and simplification) keeps
    /// the `Optional` wrapper on the root AND chain, and a dynamic filter in
    /// it stays live in the row filter: the gate sees its updates.
    #[test]
    fn optional_dynamic_filter_survives_rewrite_and_stays_live() {
        let (_file, metadata, file_schema) = test_file();
        // The table has `b` as Int64 and the file has Int32: the adapter
        // inserts a cast.
        let table_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int64, false),
        ]));
        let b = col("b", &table_schema).unwrap();
        let b_gt = |value: i64| {
            binary(
                Arc::clone(&b),
                Operator::Gt,
                lit(ScalarValue::Int64(Some(value))),
            )
        };
        let dynamic = Arc::new(DynamicFilterPhysicalExpr::new(
            vec![Arc::clone(&b)],
            b_gt(0),
        ));
        let predicate = conjunction([
            col_op("a", Operator::Gt, 5, &table_schema),
            optional(Arc::clone(&dynamic) as Arc<dyn PhysicalExpr>),
        ]);

        let rewriter = DefaultPhysicalExprAdapterFactory
            .create(Arc::clone(&table_schema), Arc::clone(&file_schema))
            .unwrap();
        let simplifier = PhysicalExprSimplifier::new(&file_schema);
        let rewritten = simplifier
            .simplify(rewriter.rewrite(Arc::clone(&predicate)).unwrap())
            .unwrap();
        let (required, optional_filters) =
            datafusion_physical_expr::utils::split_optional(&rewritten);
        assert_eq!(required.len(), 1);
        assert_eq!(optional_filters.len(), 1);

        let metrics = ExecutionPlanMetricsSet::new();
        let options = options(OptionalFilterMode::Adaptive);
        let candidates =
            prebuild(&rewritten, &file_schema, &metadata, &options, &metrics);
        assert_eq!(candidates.len(), 2);
        let optional_candidate = candidates.iter().find(|c| c.is_optional()).unwrap();
        let gate = optional_candidate.gate.as_ref().unwrap();

        // Pause the gate with non-selective batches.
        {
            let mut state = gate.lock();
            for _ in 0..10 {
                if state.gate.is_paused() {
                    break;
                }
                assert_eq!(state.gate.begin_batch(), GateDecision::Evaluate);
                state.gate.record(100, 100, Duration::ZERO);
            }
            assert!(state.gate.is_paused());
        }

        // The gate sees an update of the original dynamic filter: the pause
        // ends at once. The row filter evaluates the new expression.
        dynamic.update(b_gt(4)).unwrap();
        {
            let mut state = gate.lock();
            assert_eq!(state.gate.begin_batch(), GateDecision::Evaluate);
            assert!(!state.gate.is_paused());
        }

        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("b", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![3, 4, 5]))],
        )
        .unwrap();
        let result = optional_candidate
            .physical_expr
            .evaluate(&batch)
            .unwrap()
            .into_array(3)
            .unwrap();
        let result = as_boolean_array(&result).unwrap();
        assert_eq!(result.null_count(), 0);
        assert_eq!(
            result.iter().collect::<Vec<_>>(),
            vec![Some(false), Some(false), Some(true)]
        );
    }

    /// The adaptive filter placement of a file with a required conjunct and
    /// a gated optional conjunct. The gate decisions use the durations
    /// passed to `record`, thus the test is deterministic.
    #[test]
    fn file_placement_follows_gate_and_measurements() {
        use crate::filter_placement::{
            FilePlacement, Placement, PlacementOptions, PlacementSites,
        };
        use arrow::array::BooleanArray;
        use datafusion_physical_plan::metrics::Count;

        let (_file, metadata, file_schema) = test_file();
        // a > 5 AND Optional(b >= 0). The optional conjunct removes no row.
        let predicate = conjunction([
            col_op("a", Operator::Gt, 5, &file_schema),
            optional(col_op("b", Operator::GtEq, 0, &file_schema)),
        ]);
        let options = options(OptionalFilterMode::Adaptive);
        let metrics = ExecutionPlanMetricsSet::new();
        let mut candidates =
            prebuild(&predicate, &file_schema, &metadata, &options, &metrics);
        // A slow decode: a row filter that skips rows saves a lot.
        let sites = Arc::new(PlacementSites::with_fixed_costs(1000.0, 0.0));
        let placement_options = PlacementOptions::new(true, sites, Some(&predicate));
        // The output columns are `b`, `c` and `s`: the required conjunct
        // reads no output column (no second decode in a row filter).
        let output =
            ProjectionMask::leaves(metadata.file_metadata().schema_descr(), [1, 2, 3]);
        let batch_size = 100;
        let changes = Count::new();
        let mut placement = FilePlacement::try_new(
            &mut candidates,
            &predicate,
            &placement_options,
            &output,
            &metadata,
            batch_size,
            1000,
            changes.clone(),
        )
        .unwrap();
        let required = candidates.iter().position(|c| !c.is_optional()).unwrap();
        let gated = candidates.iter().position(|c| c.is_optional()).unwrap();
        // Without measurements, the required conjunct starts in the
        // post-scan filter. The gate is not paused.
        assert!(!placement.in_row_filter(required));
        assert!(placement.in_row_filter(gated));

        // The gate pauses the optional filter: it removed no row in its
        // first window (`sample_batches` = 2).
        let gate = Arc::clone(candidates[gated].gate().unwrap());
        for _ in 0..2 {
            let mut gate = gate.lock();
            assert_eq!(gate.begin_batch(batch_size), GateDecision::Evaluate);
            gate.record(batch_size, batch_size, Duration::from_micros(1));
        }
        assert!(gate.lock().is_paused());
        assert!(placement.decide(1000));
        assert!(!placement.in_row_filter(gated));
        assert!(!placement.in_row_filter(required));
        assert_eq!(changes.value(), 1);

        // The pause (`initial_pause_batches` = 4) ends while the scan skips
        // the next row group (10 batches): the filter is a row filter again.
        assert!(placement.decide(1000));
        assert!(placement.in_row_filter(gated));
        assert!(!gate.lock().is_paused());

        // The post-scan filter measures the required conjunct.
        let post_scan = placement.post_scan_conjuncts();
        assert_eq!(post_scan.len(), 1);
        assert_eq!(post_scan[0].expr.to_string(), "a@0 > 5");
        let stats = post_scan[0].stats.clone().unwrap();

        // It removes every second row: no run of removed rows that the
        // decoder can skip, thus it stays in the post-scan filter.
        let scattered: BooleanArray = (0..20_000).map(|i| Some(i % 2 == 0)).collect();
        stats.record_evaluation(&scattered);
        assert!(!placement.decide(1000));
        assert!(!placement.in_row_filter(required));

        // More rows where it removes a long run of rows: it moves to the
        // row filter.
        let clustered: BooleanArray = (0..40_000).map(|i| Some(i >= 30_000)).collect();
        stats.record_evaluation(&clustered);
        assert!(placement.decide(1000));
        assert!(placement.in_row_filter(required));
        assert!(placement.post_scan_conjuncts().is_empty());

        // A frozen placement does not change.
        let before = placement.placements();
        placement.restore_and_freeze(&[Placement::PostScan, Placement::PostScan]);
        assert!(!placement.decide(1000));
        assert_ne!(placement.placements(), before);
        assert_eq!(changes.value(), 3);
    }
}
