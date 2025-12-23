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

use std::any::Any;
use std::fmt;
use std::fmt::{Debug, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use crate::utils::scatter;

use arrow::array::{ArrayRef, BooleanArray, new_empty_array};
use arrow::compute::filter_record_batch;
use arrow::datatypes::{DataType, Field, FieldRef, Schema};
use arrow::record_batch::RecordBatch;
use datafusion_common::pruning::PruningStatistics;
use datafusion_common::tree_node::{
    Transformed, TransformedResult, TreeNode, TreeNodeRecursion,
};
use datafusion_common::{
    Result, ScalarValue, assert_eq_or_internal_err, exec_err, not_impl_err,
};
use datafusion_expr_common::columnar_value::ColumnarValue;
use datafusion_expr_common::interval_arithmetic::Interval;
use datafusion_expr_common::sort_properties::ExprProperties;
use datafusion_expr_common::statistics::Distribution;

use itertools::izip;

/// Shared [`PhysicalExpr`].
pub type PhysicalExprRef = Arc<dyn PhysicalExpr>;

/// [`PhysicalExpr`]s represent expressions such as `A + 1` or `CAST(c1 AS int)`.
///
/// `PhysicalExpr` knows its type, nullability and can be evaluated directly on
/// a [`RecordBatch`] (see [`Self::evaluate`]).
///
/// `PhysicalExpr` are the physical counterpart to [`Expr`] used in logical
/// planning. They are typically created from [`Expr`] by a [`PhysicalPlanner`]
/// invoked from a higher level API
///
/// Some important examples of `PhysicalExpr` are:
/// * [`Column`]: Represents a column at a given index in a RecordBatch
///
/// To create `PhysicalExpr` from  `Expr`, see
/// * [`SessionContext::create_physical_expr`]: A high level API
/// * [`create_physical_expr`]: A low level API
///
/// # Formatting `PhysicalExpr` as strings
/// There are three ways to format `PhysicalExpr` as a string:
/// * [`Debug`]: Standard Rust debugging format (e.g. `Constant { value: ... }`)
/// * [`Display`]: Detailed SQL-like format that shows expression structure (e.g. (`Utf8 ("foobar")`). This is often used for debugging and tests
/// * [`Self::fmt_sql`]: SQL-like human readable format (e.g. ('foobar')`), See also [`sql_fmt`]
///
/// [`SessionContext::create_physical_expr`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.create_physical_expr
/// [`PhysicalPlanner`]: https://docs.rs/datafusion/latest/datafusion/physical_planner/trait.PhysicalPlanner.html
/// [`Expr`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.Expr.html
/// [`create_physical_expr`]: https://docs.rs/datafusion/latest/datafusion/physical_expr/fn.create_physical_expr.html
/// [`Column`]: https://docs.rs/datafusion/latest/datafusion/physical_expr/expressions/struct.Column.html
pub trait PhysicalExpr: Any + Send + Sync + Display + Debug + DynEq + DynHash {
    /// Returns the physical expression as [`Any`] so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;
    /// Get the data type of this expression, given the schema of the input
    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        Ok(self.return_field(input_schema)?.data_type().to_owned())
    }
    /// Determine whether this expression is nullable, given the schema of the input
    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        Ok(self.return_field(input_schema)?.is_nullable())
    }
    /// Evaluate an expression against a RecordBatch
    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue>;
    /// The output field associated with this expression
    fn return_field(&self, input_schema: &Schema) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(
            format!("{self}"),
            self.data_type(input_schema)?,
            self.nullable(input_schema)?,
        )))
    }
    /// Evaluate an expression against a RecordBatch after first applying a validity array
    ///
    /// # Errors
    ///
    /// Returns an `Err` if the expression could not be evaluated or if the length of the
    /// `selection` validity array and the number of row in `batch` is not equal.
    fn evaluate_selection(
        &self,
        batch: &RecordBatch,
        selection: &BooleanArray,
    ) -> Result<ColumnarValue> {
        let row_count = batch.num_rows();
        if row_count != selection.len() {
            return exec_err!(
                "Selection array length does not match batch row count: {} != {row_count}",
                selection.len()
            );
        }

        let selection_count = selection.true_count();

        // First, check if we can avoid filtering altogether.
        if selection_count == row_count {
            // All values from the `selection` filter are true and match the input batch.
            // No need to perform any filtering.
            return self.evaluate(batch);
        }

        // Next, prepare the result array for each 'true' row in the selection vector.
        let filtered_result = if selection_count == 0 {
            // Do not call `evaluate` when the selection is empty.
            // `evaluate_selection` is used to conditionally evaluate expressions.
            // When the expression in question is fallible, evaluating it with an empty
            // record batch may trigger a runtime error (e.g. division by zero).
            //
            // Instead, create an empty array matching the expected return type.
            let datatype = self.data_type(batch.schema_ref().as_ref())?;
            ColumnarValue::Array(new_empty_array(&datatype))
        } else {
            // If we reach this point, there's no other option than to filter the batch.
            // This is a fairly costly operation since it requires creating partial copies
            // (worst case of length `row_count - 1`) of all the arrays in the record batch.
            // The resulting `filtered_batch` will contain `selection_count` rows.
            let filtered_batch = filter_record_batch(batch, selection)?;
            self.evaluate(&filtered_batch)?
        };

        // Finally, scatter the filtered result array so that the indices match the input rows again.
        match &filtered_result {
            ColumnarValue::Array(a) => {
                scatter(selection, a.as_ref()).map(ColumnarValue::Array)
            }
            ColumnarValue::Scalar(ScalarValue::Boolean(value)) => {
                // When the scalar is true or false, skip the scatter process
                if let Some(v) = value {
                    if *v {
                        Ok(ColumnarValue::from(Arc::new(selection.clone()) as ArrayRef))
                    } else {
                        Ok(filtered_result)
                    }
                } else {
                    let array = BooleanArray::from(vec![None; row_count]);
                    scatter(selection, &array).map(ColumnarValue::Array)
                }
            }
            ColumnarValue::Scalar(_) => Ok(filtered_result),
        }
    }

    /// Get a list of child PhysicalExpr that provide the input for this expr.
    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>>;

    /// Returns a new PhysicalExpr where all children were replaced by new exprs.
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>>;

    /// Computes the output interval for the expression, given the input
    /// intervals.
    ///
    /// # Parameters
    ///
    /// * `children` are the intervals for the children (inputs) of this
    ///   expression.
    ///
    /// # Returns
    ///
    /// A `Result` containing the output interval for the expression in
    /// case of success, or an error object in case of failure.
    ///
    /// # Example
    ///
    /// If the expression is `a + b`, and the input intervals are `a: [1, 2]`
    /// and `b: [3, 4]`, then the output interval would be `[4, 6]`.
    fn evaluate_bounds(&self, _children: &[&Interval]) -> Result<Interval> {
        not_impl_err!("Not implemented for {self}")
    }

    /// Updates bounds for child expressions, given a known interval for this
    /// expression.
    ///
    /// This is used to propagate constraints down through an expression tree.
    ///
    /// # Parameters
    ///
    /// * `interval` is the currently known interval for this expression.
    /// * `children` are the current intervals for the children of this expression.
    ///
    /// # Returns
    ///
    /// A `Result` containing a `Vec` of new intervals for the children (in order)
    /// in case of success, or an error object in case of failure.
    ///
    /// If constraint propagation reveals an infeasibility for any child, returns
    /// [`None`]. If none of the children intervals change as a result of
    /// propagation, may return an empty vector instead of cloning `children`.
    /// This is the default (and conservative) return value.
    ///
    /// # Example
    ///
    /// If the expression is `a + b`, the current `interval` is `[4, 5]` and the
    /// inputs `a` and `b` are respectively given as `[0, 2]` and `[-∞, 4]`, then
    /// propagation would return `[0, 2]` and `[2, 4]` as `b` must be at least
    /// `2` to make the output at least `4`.
    fn propagate_constraints(
        &self,
        _interval: &Interval,
        _children: &[&Interval],
    ) -> Result<Option<Vec<Interval>>> {
        Ok(Some(vec![]))
    }

    /// Computes the output statistics for the expression, given the input
    /// statistics.
    ///
    /// # Parameters
    ///
    /// * `children` are the statistics for the children (inputs) of this
    ///   expression.
    ///
    /// # Returns
    ///
    /// A `Result` containing the output statistics for the expression in
    /// case of success, or an error object in case of failure.
    ///
    /// Expressions (should) implement this function and utilize the independence
    /// assumption, match on children distribution types and compute the output
    /// statistics accordingly. The default implementation simply creates an
    /// unknown output distribution by combining input ranges. This logic loses
    /// distribution information, but is a safe default.
    fn evaluate_statistics(&self, children: &[&Distribution]) -> Result<Distribution> {
        let children_ranges = children
            .iter()
            .map(|c| c.range())
            .collect::<Result<Vec<_>>>()?;
        let children_ranges_refs = children_ranges.iter().collect::<Vec<_>>();
        let output_interval = self.evaluate_bounds(children_ranges_refs.as_slice())?;
        let dt = output_interval.data_type();
        if dt.eq(&DataType::Boolean) {
            let p = if output_interval.eq(&Interval::TRUE) {
                ScalarValue::new_one(&dt)
            } else if output_interval.eq(&Interval::FALSE) {
                ScalarValue::new_zero(&dt)
            } else {
                ScalarValue::try_from(&dt)
            }?;
            Distribution::new_bernoulli(p)
        } else {
            Distribution::new_from_interval(output_interval)
        }
    }

    /// Updates children statistics using the given parent statistic for this
    /// expression.
    ///
    /// This is used to propagate statistics down through an expression tree.
    ///
    /// # Parameters
    ///
    /// * `parent` is the currently known statistics for this expression.
    /// * `children` are the current statistics for the children of this expression.
    ///
    /// # Returns
    ///
    /// A `Result` containing a `Vec` of new statistics for the children (in order)
    /// in case of success, or an error object in case of failure.
    ///
    /// If statistics propagation reveals an infeasibility for any child, returns
    /// [`None`]. If none of the children statistics change as a result of
    /// propagation, may return an empty vector instead of cloning `children`.
    /// This is the default (and conservative) return value.
    ///
    /// Expressions (should) implement this function and apply Bayes rule to
    /// reconcile and update parent/children statistics. This involves utilizing
    /// the independence assumption, and matching on distribution types. The
    /// default implementation simply creates an unknown distribution if it can
    /// narrow the range by propagating ranges. This logic loses distribution
    /// information, but is a safe default.
    fn propagate_statistics(
        &self,
        parent: &Distribution,
        children: &[&Distribution],
    ) -> Result<Option<Vec<Distribution>>> {
        let children_ranges = children
            .iter()
            .map(|c| c.range())
            .collect::<Result<Vec<_>>>()?;
        let children_ranges_refs = children_ranges.iter().collect::<Vec<_>>();
        let parent_range = parent.range()?;
        let Some(propagated_children) =
            self.propagate_constraints(&parent_range, children_ranges_refs.as_slice())?
        else {
            return Ok(None);
        };
        izip!(propagated_children.into_iter(), children_ranges, children)
            .map(|(new_interval, old_interval, child)| {
                if new_interval == old_interval {
                    // We weren't able to narrow the range, preserve the old statistics.
                    Ok((*child).clone())
                } else if new_interval.data_type().eq(&DataType::Boolean) {
                    let dt = old_interval.data_type();
                    let p = if new_interval.eq(&Interval::TRUE) {
                        ScalarValue::new_one(&dt)
                    } else if new_interval.eq(&Interval::FALSE) {
                        ScalarValue::new_zero(&dt)
                    } else {
                        unreachable!("Given that we have a range reduction for a boolean interval, we should have certainty")
                    }?;
                    Distribution::new_bernoulli(p)
                } else {
                    Distribution::new_from_interval(new_interval)
                }
            })
            .collect::<Result<_>>()
            .map(Some)
    }

    /// Calculates the properties of this [`PhysicalExpr`] based on its
    /// children's properties (i.e. order and range), recursively aggregating
    /// the information from its children. In cases where the [`PhysicalExpr`]
    /// has no children (e.g., `Literal` or `Column`), these properties should
    /// be specified externally, as the function defaults to unknown properties.
    fn get_properties(&self, _children: &[ExprProperties]) -> Result<ExprProperties> {
        Ok(ExprProperties::new_unknown())
    }

    /// Format this `PhysicalExpr` in nice human readable "SQL" format
    ///
    /// Specifically, this format is designed to be readable by humans, at the
    /// expense of details. Use `Display` or `Debug` for more detailed
    /// representation.
    ///
    /// See the [`fmt_sql`] function for an example of printing `PhysicalExpr`s as SQL.
    fn fmt_sql(&self, f: &mut Formatter<'_>) -> fmt::Result;

    /// Take a snapshot of this `PhysicalExpr`, if it is dynamic.
    ///
    /// "Dynamic" in this case means containing references to structures that may change
    /// during plan execution, such as hash tables.
    ///
    /// This method is used to capture the current state of `PhysicalExpr`s that may contain
    /// dynamic references to other operators in order to serialize it over the wire
    /// or treat it via downcast matching.
    ///
    /// You should not call this method directly as it does not handle recursion.
    /// Instead use [`snapshot_physical_expr`] to handle recursion and capture the
    /// full state of the `PhysicalExpr`.
    ///
    /// This is expected to return "simple" expressions that do not have mutable state
    /// and are composed of DataFusion's built-in `PhysicalExpr` implementations.
    /// Callers however should *not* assume anything about the returned expressions
    /// since callers and implementers may not agree on what "simple" or "built-in"
    /// means.
    /// In other words, if you need to serialize a `PhysicalExpr` across the wire
    /// you should call this method and then try to serialize the result,
    /// but you should handle unknown or unexpected `PhysicalExpr` implementations gracefully
    /// just as if you had not called this method at all.
    ///
    /// In particular, consider:
    /// * A `PhysicalExpr` that references the current state of a `datafusion::physical_plan::TopK`
    ///   that is involved in a query with `SELECT * FROM t1 ORDER BY a LIMIT 10`.
    ///   This function may return something like `a >= 12`.
    /// * A `PhysicalExpr` that references the current state of a `datafusion::physical_plan::joins::HashJoinExec`
    ///   from a query such as `SELECT * FROM t1 JOIN t2 ON t1.a = t2.b`.
    ///   This function may return something like `t2.b IN (1, 5, 7)`.
    ///
    /// A system or function that can only deal with a hardcoded set of `PhysicalExpr` implementations
    /// or needs to serialize this state to bytes may not be able to handle these dynamic references.
    /// In such cases, we should return a simplified version of the `PhysicalExpr` that does not
    /// contain these dynamic references.
    ///
    /// Systems that implement remote execution of plans, e.g. serialize a portion of the query plan
    /// and send it across the wire to a remote executor may want to call this method after
    /// every batch on the source side and broadcast / update the current snapshot to the remote executor.
    ///
    /// Note for implementers: this method should *not* handle recursion.
    /// Recursion is handled in [`snapshot_physical_expr`].
    fn snapshot(&self) -> Result<Option<Arc<dyn PhysicalExpr>>> {
        // By default, we return None to indicate that this PhysicalExpr does not
        // have any dynamic references or state.
        // This is a safe default behavior.
        Ok(None)
    }

    /// Returns the generation of this `PhysicalExpr` for snapshotting purposes.
    /// The generation is an arbitrary u64 that can be used to track changes
    /// in the state of the `PhysicalExpr` over time without having to do an exhaustive comparison.
    /// This is useful to avoid unnecessary computation or serialization if there are no changes to the expression.
    /// In particular, dynamic expressions that may change over time; this allows cheap checks for changes.
    /// Static expressions that do not change over time should return 0, as does the default implementation.
    /// You should not call this method directly as it does not handle recursion.
    /// Instead use [`snapshot_generation`] to handle recursion and capture the
    /// full state of the `PhysicalExpr`.
    fn snapshot_generation(&self) -> u64 {
        // By default, we return 0 to indicate that this PhysicalExpr does not
        // have any dynamic references or state.
        // Since the recursive algorithm XORs the generations of all children the overall
        // generation will be 0 if no children have a non-zero generation, meaning that
        // static expressions will always return 0.
        0
    }

    /// Returns true if the expression node is volatile, i.e. whether it can return
    /// different results when evaluated multiple times with the same input.
    ///
    /// Note: unlike [`is_volatile`], this function does not consider inputs:
    /// - `random()` returns `true`,
    /// - `a + random()` returns `false` (because the operation `+` itself is not volatile.)
    ///
    /// The default to this function was set to `false` when it was created
    /// to avoid imposing API churn on implementers, but this is not a safe default in general.
    /// It is highly recommended that volatile expressions implement this method and return `true`.
    /// This default may be removed in the future if it causes problems or we decide to
    /// eat the cost of the breaking change and require all implementers to make a choice.
    fn is_volatile_node(&self) -> bool {
        false
    }

    // ---------------
    // Pruning related
    // ---------------
    // None means propagation not implemented/supported for this node
    fn propagate_range_stats(
        &self,
        _child_range_stats: &[RangeStats],
    ) -> Result<Option<RangeStats>> {
        Ok(None)
    }

    fn propagate_null_stats(
        &self,
        _child_range_stats: &[NullStats],
    ) -> Result<Option<NullStats>> {
        Ok(None)
    }

    fn evaluate_pruning(&self, ctx: Arc<PruningContext>) -> Result<PruningIntermediate> {
        // Default impl for stats-propagation nodes (e.g. arithmetic expressions):
        // 1) Evaluate pruning for all children.
        // 2) If every child produced range/null stats, propagate them.
        // 3) If no stats can be propagated, fall back to `Unsupported`.
        let children = self.children();
        if children.is_empty() {
            return Ok(PruningIntermediate::empty_stats());
        }

        let mut range_complete = true;
        let mut null_complete = true;
        let mut child_range_stats = Vec::with_capacity(children.len());
        let mut child_null_stats = Vec::with_capacity(children.len());

        for child in children {
            match child.evaluate_pruning(Arc::clone(&ctx))? {
                PruningIntermediate::IntermediateStats(stats) => {
                    match stats.range_stats {
                        Some(range_stats) if range_complete => {
                            child_range_stats.push(range_stats);
                        }
                        _ => {
                            range_complete = false;
                        }
                    }

                    match stats.null_stats {
                        Some(null_stats) if null_complete => {
                            child_null_stats.push(null_stats);
                        }
                        _ => {
                            null_complete = false;
                        }
                    }
                }
                // Without node-specific semantics, we can't combine a final pruning result here.
                other => return Ok(other),
            }
        }

        let range_stats = if range_complete && !child_range_stats.is_empty() {
            if let Some((first, rest)) = child_range_stats.split_first() {
                for stats in rest {
                    assert_eq_or_internal_err!(
                        first.len(),
                        stats.len(),
                        "Range stats length mismatch between pruning children"
                    );
                }
            }
            self.propagate_range_stats(&child_range_stats)?
        } else {
            None
        };

        let null_stats = if null_complete && !child_null_stats.is_empty() {
            if let Some((first, rest)) = child_null_stats.split_first() {
                for stats in rest {
                    assert_eq_or_internal_err!(
                        first.length,
                        stats.length,
                        "Null stats length mismatch between pruning children"
                    );
                }
            }
            self.propagate_null_stats(&child_null_stats)?
        } else {
            None
        };

        if let (Some(range_stats), Some(null_stats)) =
            (range_stats.as_ref(), null_stats.as_ref())
        {
            assert_eq_or_internal_err!(
                range_stats.len(),
                null_stats.length,
                "Range and null stats length mismatch for pruning"
            );
        }

        Ok(PruningIntermediate::IntermediateStats(ColumnStats {
            range_stats,
            null_stats,
        }))
    }
}

// Pruner Common
/// e.g. for x > 5
/// bucket 1 has stat [10,15] -> AlwaysTrue
/// bucket 2 has stat [0,5] -> AlwaysFalse
/// bucket 3 has stat [0,10] -> Unknown
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PruningResult {
    AlwaysTrue,
    AlwaysFalse,
    Unknown,
}

#[derive(Debug, Clone)]
pub enum RangeStats {
    Values {
        mins: Option<ArrayRef>,
        maxs: Option<ArrayRef>,
        length: usize,
    },
    /// Represents a uniform literal value across all containers.
    /// This variant make it easy to compare between literals and normal ranges representing
    /// each containers' value range.
    ///
    /// TODO: remove length -- seems redundant
    Scalar { value: ScalarValue, length: usize },
}

#[derive(Debug, Clone)]
pub struct NullStats {
    null_counts: Option<ArrayRef>,
    row_counts: Option<ArrayRef>,
    length: usize,
}

#[derive(Debug, Clone)]
pub struct ColumnStats {
    pub range_stats: Option<RangeStats>,
    pub null_stats: Option<NullStats>,
}

impl RangeStats {
    pub fn new(
        mins: Option<ArrayRef>,
        maxs: Option<ArrayRef>,
        length: usize,
    ) -> Result<Self> {
        if let Some(ref mins) = mins {
            assert_eq_or_internal_err!(
                mins.len(),
                length,
                "Range mins length mismatch for pruning statistics"
            );
        }
        if let Some(ref maxs) = maxs {
            assert_eq_or_internal_err!(
                maxs.len(),
                length,
                "Range maxs length mismatch for pruning statistics"
            );
        }
        Ok(Self::Values { mins, maxs, length })
    }

    /// Create range stats for a constant literal across all containers.
    ///
    pub fn new_scalar(value: ScalarValue, length: usize) -> Result<Self> {
        ScalarValue::iter_to_array(std::iter::repeat(value.clone()).take(length))?;
        Ok(Self::Scalar { value, length })
    }

    pub fn len(&self) -> usize {
        match self {
            RangeStats::Values { length, .. } | RangeStats::Scalar { length, .. } => {
                *length
            }
        }
    }
}

pub struct PruningContext {
    stats: Arc<dyn PruningStatistics>,
}

impl PruningContext {
    pub fn new(stats: Arc<dyn PruningStatistics>) -> Self {
        Self { stats }
    }

    pub fn statistics(&self) -> &Arc<dyn PruningStatistics> {
        &self.stats
    }
}

impl NullStats {
    pub fn new(
        null_counts: Option<ArrayRef>,
        row_counts: Option<ArrayRef>,
        length: usize,
    ) -> Result<Self> {
        if let Some(ref null_counts) = null_counts {
            assert_eq_or_internal_err!(
                null_counts.len(),
                length,
                "Null counts length mismatch for pruning statistics"
            );
        }
        if let Some(ref row_counts) = row_counts {
            assert_eq_or_internal_err!(
                row_counts.len(),
                length,
                "Row counts length mismatch for pruning statistics"
            );
        }
        Ok(Self {
            null_counts,
            row_counts,
            length,
        })
    }

    pub fn len(&self) -> usize {
        self.length
    }

    pub fn null_counts(&self) -> Option<&ArrayRef> {
        self.null_counts.as_ref()
    }

    pub fn row_counts(&self) -> Option<&ArrayRef> {
        self.row_counts.as_ref()
    }
}

impl ColumnStats {
    pub fn new(range_stats: Option<RangeStats>, null_stats: Option<NullStats>) -> Self {
        Self {
            range_stats,
            null_stats,
        }
    }

    pub fn range_stats(&self) -> Option<&RangeStats> {
        self.range_stats.as_ref()
    }

    pub fn null_stats(&self) -> Option<&NullStats> {
        self.null_stats.as_ref()
    }
}

#[derive(Debug, Clone)]
pub enum PruningIntermediate {
    IntermediateStats(ColumnStats),
    IntermediateResult(PruningResult),
}

impl PruningIntermediate {
    /// Create an `IntermediateStats` variant with no range or null statistics.
    pub fn empty_stats() -> Self {
        Self::IntermediateStats(ColumnStats::new(None, None))
    }
}

#[deprecated(
    since = "50.0.0",
    note = "Use `datafusion_expr_common::dyn_eq` instead"
)]
pub use datafusion_expr_common::dyn_eq::{DynEq, DynHash};

impl PartialEq for dyn PhysicalExpr {
    fn eq(&self, other: &Self) -> bool {
        self.dyn_eq(other.as_any())
    }
}
impl Eq for dyn PhysicalExpr {}

impl Hash for dyn PhysicalExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.dyn_hash(state);
    }
}

/// Returns a copy of this expr if we change any child according to the pointer comparison.
/// The size of `children` must be equal to the size of `PhysicalExpr::children()`.
pub fn with_new_children_if_necessary(
    expr: Arc<dyn PhysicalExpr>,
    children: Vec<Arc<dyn PhysicalExpr>>,
) -> Result<Arc<dyn PhysicalExpr>> {
    let old_children = expr.children();
    assert_eq_or_internal_err!(
        children.len(),
        old_children.len(),
        "PhysicalExpr: Wrong number of children"
    );

    if children.is_empty()
        || children
            .iter()
            .zip(old_children.iter())
            .any(|(c1, c2)| !Arc::ptr_eq(c1, c2))
    {
        Ok(expr.with_new_children(children)?)
    } else {
        Ok(expr)
    }
}

/// Returns [`Display`] able a list of [`PhysicalExpr`]
///
/// Example output: `[a + 1, b]`
pub fn format_physical_expr_list<T>(exprs: T) -> impl Display
where
    T: IntoIterator,
    T::Item: Display,
    T::IntoIter: Clone,
{
    struct DisplayWrapper<I>(I)
    where
        I: Iterator + Clone,
        I::Item: Display;

    impl<I> Display for DisplayWrapper<I>
    where
        I: Iterator + Clone,
        I::Item: Display,
    {
        fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
            let mut iter = self.0.clone();
            write!(f, "[")?;
            if let Some(expr) = iter.next() {
                write!(f, "{expr}")?;
            }
            for expr in iter {
                write!(f, ", {expr}")?;
            }
            write!(f, "]")?;
            Ok(())
        }
    }

    DisplayWrapper(exprs.into_iter())
}

/// Prints a [`PhysicalExpr`] in a SQL-like format
///
/// # Example
/// ```
/// # // The boilerplate needed to create a `PhysicalExpr` for the example
/// # use std::any::Any;
/// use std::collections::HashMap;
/// # use std::fmt::Formatter;
/// # use std::sync::Arc;
/// # use arrow::array::RecordBatch;
/// # use arrow::datatypes::{DataType, Field, FieldRef, Schema};
/// # use datafusion_common::Result;
/// # use datafusion_expr_common::columnar_value::ColumnarValue;
/// # use datafusion_physical_expr_common::physical_expr::{fmt_sql, DynEq, PhysicalExpr};
/// # #[derive(Debug, PartialEq, Eq, Hash)]
/// # struct MyExpr {}
/// # impl PhysicalExpr for MyExpr {fn as_any(&self) -> &dyn Any { unimplemented!() }
/// # fn data_type(&self, input_schema: &Schema) -> Result<DataType> { unimplemented!() }
/// # fn nullable(&self, input_schema: &Schema) -> Result<bool> { unimplemented!() }
/// # fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> { unimplemented!() }
/// # fn return_field(&self, input_schema: &Schema) -> Result<FieldRef> { unimplemented!() }
/// # fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>>{ unimplemented!() }
/// # fn with_new_children(self: Arc<Self>, children: Vec<Arc<dyn PhysicalExpr>>) -> Result<Arc<dyn PhysicalExpr>> { unimplemented!() }
/// # fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result { write!(f, "CASE a > b THEN 1 ELSE 0 END") }
/// # }
/// # impl std::fmt::Display for MyExpr {fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result { unimplemented!() } }
/// # fn make_physical_expr() -> Arc<dyn PhysicalExpr> { Arc::new(MyExpr{}) }
/// let expr: Arc<dyn PhysicalExpr> = make_physical_expr();
/// // wrap the expression in `sql_fmt` which can be used with
/// // `format!`, `to_string()`, etc
/// let expr_as_sql = fmt_sql(expr.as_ref());
/// assert_eq!(
///   "The SQL: CASE a > b THEN 1 ELSE 0 END",
///   format!("The SQL: {expr_as_sql}")
/// );
/// ```
pub fn fmt_sql(expr: &dyn PhysicalExpr) -> impl Display + '_ {
    struct Wrapper<'a> {
        expr: &'a dyn PhysicalExpr,
    }

    impl Display for Wrapper<'_> {
        fn fmt(&self, f: &mut Formatter) -> fmt::Result {
            self.expr.fmt_sql(f)?;
            Ok(())
        }
    }

    Wrapper { expr }
}

/// Take a snapshot of the given `PhysicalExpr` if it is dynamic.
///
/// Take a snapshot of this `PhysicalExpr` if it is dynamic.
/// This is used to capture the current state of `PhysicalExpr`s that may contain
/// dynamic references to other operators in order to serialize it over the wire
/// or treat it via downcast matching.
///
/// See the documentation of [`PhysicalExpr::snapshot`] for more details.
///
/// # Returns
///
/// Returns a snapshot of the `PhysicalExpr` if it is dynamic, otherwise
/// returns itself.
pub fn snapshot_physical_expr(
    expr: Arc<dyn PhysicalExpr>,
) -> Result<Arc<dyn PhysicalExpr>> {
    snapshot_physical_expr_opt(expr).data()
}

/// Take a snapshot of the given `PhysicalExpr` if it is dynamic.
///
/// Take a snapshot of this `PhysicalExpr` if it is dynamic.
/// This is used to capture the current state of `PhysicalExpr`s that may contain
/// dynamic references to other operators in order to serialize it over the wire
/// or treat it via downcast matching.
///
/// See the documentation of [`PhysicalExpr::snapshot`] for more details.
///
/// # Returns
///
/// Returns a `[`Transformed`] indicating whether a snapshot was taken,
/// along with the resulting `PhysicalExpr`.
pub fn snapshot_physical_expr_opt(
    expr: Arc<dyn PhysicalExpr>,
) -> Result<Transformed<Arc<dyn PhysicalExpr>>> {
    expr.transform_up(|e| {
        if let Some(snapshot) = e.snapshot()? {
            Ok(Transformed::yes(snapshot))
        } else {
            Ok(Transformed::no(Arc::clone(&e)))
        }
    })
}

/// Check the generation of this `PhysicalExpr`.
/// Dynamic `PhysicalExpr`s may have a generation that is incremented
/// every time the state of the `PhysicalExpr` changes.
/// If the generation changes that means this `PhysicalExpr` or one of its children
/// has changed since the last time it was evaluated.
///
/// This algorithm will not produce collisions as long as the structure of the
/// `PhysicalExpr` does not change and no `PhysicalExpr` decrements its own generation.
pub fn snapshot_generation(expr: &Arc<dyn PhysicalExpr>) -> u64 {
    let mut generation = 0u64;
    expr.apply(|e| {
        // Add the current generation of the `PhysicalExpr` to our global generation.
        generation = generation.wrapping_add(e.snapshot_generation());
        Ok(TreeNodeRecursion::Continue)
    })
    .expect("this traversal is infallible");

    generation
}

/// Check if the given `PhysicalExpr` is dynamic.
/// Internally this calls [`snapshot_generation`] to check if the generation is non-zero,
/// any dynamic `PhysicalExpr` should have a non-zero generation.
pub fn is_dynamic_physical_expr(expr: &Arc<dyn PhysicalExpr>) -> bool {
    // If the generation is non-zero, then this `PhysicalExpr` is dynamic.
    snapshot_generation(expr) != 0
}

/// Returns true if the expression is volatile, i.e. whether it can return different
/// results when evaluated multiple times with the same input.
///
/// For example the function call `RANDOM()` is volatile as each call will
/// return a different value.
///
/// This method recursively checks if any sub-expression is volatile, for example
/// `1 + RANDOM()` will return `true`.
pub fn is_volatile(expr: &Arc<dyn PhysicalExpr>) -> bool {
    if expr.is_volatile_node() {
        return true;
    }
    let mut is_volatile = false;
    expr.apply(|e| {
        if e.is_volatile_node() {
            is_volatile = true;
            Ok(TreeNodeRecursion::Stop)
        } else {
            Ok(TreeNodeRecursion::Continue)
        }
    })
    .expect("infallible closure should not fail");
    is_volatile
}

#[cfg(test)]
mod test {
    use crate::physical_expr::{PhysicalExpr, RangeStats};
    use arrow::array::{Array, BooleanArray, Int64Array, RecordBatch};
    use arrow::datatypes::{DataType, Schema};
    use datafusion_common::ScalarValue;
    use datafusion_expr_common::columnar_value::ColumnarValue;
    use std::fmt::{Display, Formatter};
    use std::sync::Arc;

    #[derive(Debug, PartialEq, Eq, Hash)]
    struct TestExpr {}

    impl PhysicalExpr for TestExpr {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn data_type(&self, _schema: &Schema) -> datafusion_common::Result<DataType> {
            Ok(DataType::Int64)
        }

        fn nullable(&self, _schema: &Schema) -> datafusion_common::Result<bool> {
            Ok(false)
        }

        fn evaluate(
            &self,
            batch: &RecordBatch,
        ) -> datafusion_common::Result<ColumnarValue> {
            let data = vec![1; batch.num_rows()];
            Ok(ColumnarValue::Array(Arc::new(Int64Array::from(data))))
        }

        fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
            vec![]
        }

        fn with_new_children(
            self: Arc<Self>,
            _children: Vec<Arc<dyn PhysicalExpr>>,
        ) -> datafusion_common::Result<Arc<dyn PhysicalExpr>> {
            Ok(Arc::new(Self {}))
        }

        fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            f.write_str("TestExpr")
        }
    }

    impl Display for TestExpr {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            self.fmt_sql(f)
        }
    }

    macro_rules! assert_arrays_eq {
        ($EXPECTED: expr, $ACTUAL: expr, $MESSAGE: expr) => {
            let expected = $EXPECTED.to_array(1).unwrap();
            let actual = $ACTUAL;

            let actual_array = actual.to_array(expected.len()).unwrap();
            let actual_ref = actual_array.as_ref();
            let expected_ref = expected.as_ref();
            assert!(
                actual_ref == expected_ref,
                "{}: expected: {:?}, actual: {:?}",
                $MESSAGE,
                $EXPECTED,
                actual_ref
            );
        };
    }

    fn test_evaluate_selection(
        batch: &RecordBatch,
        selection: &BooleanArray,
        expected: &ColumnarValue,
    ) {
        let expr = TestExpr {};

        // First check that the `evaluate_selection` is the expected one
        let selection_result = expr.evaluate_selection(batch, selection).unwrap();
        assert_eq!(
            expected.to_array(1).unwrap().len(),
            selection_result.to_array(1).unwrap().len(),
            "evaluate_selection should output row count should match input record batch"
        );
        assert_arrays_eq!(
            expected,
            &selection_result,
            "evaluate_selection returned unexpected value"
        );

        // If we're selecting all rows, the result should be the same as calling `evaluate`
        // with the full record batch.
        if (0..batch.num_rows())
            .all(|row_idx| row_idx < selection.len() && selection.value(row_idx))
        {
            let empty_result = expr.evaluate(batch).unwrap();

            assert_arrays_eq!(
                empty_result,
                &selection_result,
                "evaluate_selection does not match unfiltered evaluate result"
            );
        }
    }

    fn test_evaluate_selection_error(batch: &RecordBatch, selection: &BooleanArray) {
        let expr = TestExpr {};

        // First check that the `evaluate_selection` is the expected one
        let selection_result = expr.evaluate_selection(batch, selection);
        assert!(selection_result.is_err(), "evaluate_selection should fail");
    }

    #[test]
    pub fn test_evaluate_selection_with_empty_record_batch() {
        test_evaluate_selection(
            &RecordBatch::new_empty(Arc::new(Schema::empty())),
            &BooleanArray::from(vec![false; 0]),
            &ColumnarValue::Array(Arc::new(Int64Array::new_null(0))),
        );
    }

    #[test]
    pub fn test_evaluate_selection_with_empty_record_batch_with_larger_false_selection() {
        test_evaluate_selection_error(
            &RecordBatch::new_empty(Arc::new(Schema::empty())),
            &BooleanArray::from(vec![false; 10]),
        );
    }

    #[test]
    pub fn test_evaluate_selection_with_empty_record_batch_with_larger_true_selection() {
        test_evaluate_selection_error(
            &RecordBatch::new_empty(Arc::new(Schema::empty())),
            &BooleanArray::from(vec![true; 10]),
        );
    }

    #[test]
    pub fn test_evaluate_selection_with_non_empty_record_batch() {
        test_evaluate_selection(
            &unsafe { RecordBatch::new_unchecked(Arc::new(Schema::empty()), vec![], 10) },
            &BooleanArray::from(vec![true; 10]),
            &ColumnarValue::Array(Arc::new(Int64Array::from(vec![1; 10]))),
        );
    }

    #[test]
    pub fn test_evaluate_selection_with_non_empty_record_batch_with_larger_false_selection()
     {
        test_evaluate_selection_error(
            &unsafe { RecordBatch::new_unchecked(Arc::new(Schema::empty()), vec![], 10) },
            &BooleanArray::from(vec![false; 20]),
        );
    }

    #[test]
    pub fn test_evaluate_selection_with_non_empty_record_batch_with_larger_true_selection()
     {
        test_evaluate_selection_error(
            &unsafe { RecordBatch::new_unchecked(Arc::new(Schema::empty()), vec![], 10) },
            &BooleanArray::from(vec![true; 20]),
        );
    }

    #[test]
    pub fn test_evaluate_selection_with_non_empty_record_batch_with_smaller_false_selection()
     {
        test_evaluate_selection_error(
            &unsafe { RecordBatch::new_unchecked(Arc::new(Schema::empty()), vec![], 10) },
            &BooleanArray::from(vec![false; 5]),
        );
    }

    #[test]
    pub fn test_evaluate_selection_with_non_empty_record_batch_with_smaller_true_selection()
     {
        test_evaluate_selection_error(
            &unsafe { RecordBatch::new_unchecked(Arc::new(Schema::empty()), vec![], 10) },
            &BooleanArray::from(vec![true; 5]),
        );
    }

    #[test]
    fn range_stats_scalar_variant() {
        let stats = RangeStats::new_scalar(ScalarValue::Int64(Some(42)), 3).unwrap();
        assert_eq!(stats.len(), 3);

        let (mins, maxs) = match &stats {
            RangeStats::Scalar { value, length } => {
                let arr = ScalarValue::iter_to_array(
                    std::iter::repeat(value.clone()).take(*length),
                )
                .unwrap();
                (arr.clone(), arr)
            }
            RangeStats::Values { mins, maxs, .. } => {
                (mins.clone().expect("mins"), maxs.clone().expect("maxs"))
            }
        };

        let mins = mins.as_any().downcast_ref::<Int64Array>().unwrap();
        let maxs = maxs.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(mins, &Int64Array::from(vec![Some(42), Some(42), Some(42)]));
        assert_eq!(maxs, &Int64Array::from(vec![Some(42), Some(42), Some(42)]));
    }
}
