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

//! [`PruningPredicate`] to apply filter [`Expr`] to prune "containers"
//! based on statistics (e.g. Parquet Row Groups)
//!
//! [`Expr`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.Expr.html
use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::AsArray;
use arrow::{
    array::{new_null_array, ArrayRef, BooleanArray},
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::{RecordBatch, RecordBatchOptions},
};
// pub use for backwards compatibility
pub use datafusion_common::pruning::PruningStatistics;
use datafusion_physical_expr::simplifier::PhysicalExprSimplifier;
use datafusion_physical_plan::metrics::Count;
use log::{debug, trace};

use datafusion_common::error::Result;
use datafusion_common::tree_node::TransformedResult;
use datafusion_common::{assert_eq_or_internal_err, Column, DFSchema};
use datafusion_common::{
    internal_datafusion_err, plan_datafusion_err, plan_err,
    tree_node::{Transformed, TreeNode},
    ScalarValue,
};
use datafusion_expr_common::operator::Operator;
use datafusion_physical_expr::utils::{collect_columns, Guarantee, LiteralGuarantee};
use datafusion_physical_expr::{expressions as phys_expr, PhysicalExprRef};
use datafusion_physical_expr_common::physical_expr::snapshot_physical_expr;
use datafusion_physical_plan::{ColumnarValue, PhysicalExpr};

/// Used to prove that arbitrary predicates (boolean expression) can not
/// possibly evaluate to `true` given information about a column provided by
/// [`PruningStatistics`].
///
/// # Introduction
///
/// `PruningPredicate` analyzes filter expressions using statistics such as
/// min/max values and null counts, attempting to prove a "container" (e.g.
/// Parquet Row Group) can be skipped without reading the actual data,
/// potentially leading to significant performance improvements.
///
/// For example, `PruningPredicate`s are used to prune Parquet Row Groups based
/// on the min/max values found in the Parquet metadata. If the
/// `PruningPredicate` can prove that the filter can never evaluate to `true`
/// for any row in the Row Group, the entire Row Group is skipped during query
/// execution.
///
/// The `PruningPredicate` API is general, and can be used for pruning other
/// types of containers (e.g. files) based on statistics that may be known from
/// external catalogs (e.g. Delta Lake) or other sources. How this works is a
/// subtle topic.  See the Background and Implementation section for details.
///
/// `PruningPredicate` supports:
///
/// 1. Arbitrary expressions (including user defined functions)
///
/// 2. Vectorized evaluation (provide more than one set of statistics at a time)
///    so it is suitable for pruning 1000s of containers.
///
/// 3. Any source of information that implements the [`PruningStatistics`] trait
///    (not just Parquet metadata).
///
/// # Example
///
/// See the [`pruning.rs` example in the `datafusion-examples`] for a complete
/// example of how to use `PruningPredicate` to prune files based on min/max
/// values.
///
/// [`pruning.rs` example in the `datafusion-examples`]: https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/query_planning/pruning.rs
///
/// Given an expression like `x = 5` and statistics for 3 containers (Row
/// Groups, files, etc) `A`, `B`, and `C`:
///
/// ```text
///   A: {x_min = 0, x_max = 4}
///   B: {x_min = 2, x_max = 10}
///   C: {x_min = 5, x_max = 8}
/// ```
///
/// `PruningPredicate` will conclude that the rows in container `A` can never
/// be true (as the maximum value is only `4`), so it can be pruned:
///
/// ```text
/// A: false (no rows could possibly match x = 5)
/// B: true  (rows might match x = 5)
/// C: true  (rows might match x = 5)
/// ```
///
/// See [`PruningPredicate::try_new`] and [`PruningPredicate::prune`] for more information.
///
/// # Background
///
/// ## Boolean Tri-state logic
///
/// To understand the details of the rest of this documentation, it is important
/// to understand how the tri-state boolean logic in SQL works. As this is
/// somewhat esoteric, we review it here.
///
/// SQL has a notion of `NULL` that represents the value is `“unknown”` and this
/// uncertainty propagates through expressions. SQL `NULL` behaves very
/// differently than the `NULL` in most other languages where it is a special,
/// sentinel value (e.g. `0` in `C/C++`). While representing uncertainty with
/// `NULL` is powerful and elegant, SQL `NULL`s are often deeply confusing when
/// first encountered as they behave differently than most programmers may
/// expect.
///
/// In most other programming languages,
/// * `a == NULL` evaluates to `true` if `a` also had the value `NULL`
/// * `a == NULL` evaluates to `false` if `a` has any other value
///
/// However, in SQL `a = NULL` **always** evaluates to `NULL` (never `true` or
/// `false`):
///
/// Expression    | Result
/// ------------- | ---------
/// `1 = NULL`    | `NULL`
/// `NULL = NULL` | `NULL`
///
/// Also important is how `AND` and `OR` works with tri-state boolean logic as
/// (perhaps counterintuitively) the result is **not** always NULL. While
/// consistent with the notion of `NULL` representing “unknown”, this is again,
/// often deeply confusing 🤯 when first encountered.
///
/// Expression       | Result    | Intuition
/// ---------------  | --------- | -----------
/// `NULL AND true`  |   `NULL`  | The `NULL` stands for “unknown” and if it were `true` or `false` the overall expression value could change
/// `NULL AND false` |  `false`  | If the `NULL` was either `true` or `false` the overall expression is still `false`
/// `NULL AND NULL`  | `NULL`    |
///
/// Expression      | Result    | Intuition
/// --------------- | --------- | ----------
/// `NULL OR true`  | `true`    |  If the `NULL` was either `true` or `false` the overall expression is still `true`
/// `NULL OR false` | `NULL`    |  The `NULL` stands for “unknown” and if it were `true` or `false` the overall expression value could change
/// `NULL OR NULL`  |  `NULL`   |
///
/// ## SQL Filter Semantics
///
/// The SQL `WHERE` clause has a boolean expression, often called a filter or
/// predicate. The semantics of this predicate are that the query evaluates the
/// predicate for each row in the input tables and:
///
/// * Rows that evaluate to `true` are returned in the query results
///
/// * Rows that evaluate to `false` are not returned (“filtered out” or “pruned” or “skipped”).
///
/// * Rows that evaluate to `NULL` are **NOT** returned (also “filtered out”).
///   Note: *this treatment of `NULL` is **DIFFERENT** than how `NULL` is treated
///   in the rewritten predicate described below.*
///
/// # `PruningPredicate` Implementation
///
/// Armed with the information in the Background section, we can now understand
/// how the `PruningPredicate` logic works.
///
/// ## Interface
///
/// **Inputs**
/// 1. An input schema describing what columns exist
///
/// 2. A predicate (expression that evaluates to a boolean)
///
/// 3. [`PruningStatistics`] that provides information about columns in that
///    schema, for multiple “containers”. For each column in each container, it
///    provides optional information on contained values, min_values, max_values,
///    null_counts counts, and row_counts counts.
///
/// **Outputs**:
/// A (non null) boolean value for each container:
/// * `true`: There MAY be rows that match the predicate
///
/// * `false`: There are no rows that could possibly match the predicate (the
///   predicate can never possibly be true). The container can be pruned (skipped)
///   entirely.
///
/// While `PruningPredicate` will never return a `NULL` value, the
/// rewritten predicate (as returned by `build_predicate_expression` and used internally
/// by `PruningPredicate`) may evaluate to `NULL` when some of the min/max values
/// or null / row counts are not known.
///
/// In order to be correct, `PruningPredicate` must return false
/// **only** if it can determine that for all rows in the container, the
/// predicate could never evaluate to `true` (always evaluates to either `NULL`
/// or `false`).
///
/// ## Contains Analysis and Min/Max Rewrite
///
/// `PruningPredicate` works by first analyzing the predicate to see what
/// [`LiteralGuarantee`] must hold for the predicate to be true.
///
/// Then, the `PruningPredicate` rewrites the original predicate into an
/// expression that references the min/max values of each column in the original
/// predicate.
///
/// When the min/max values are actually substituted in to this expression and
/// evaluated, the result means
///
/// * `true`: there MAY be rows that pass the predicate, **KEEPS** the container
///
/// * `NULL`: there MAY be rows that pass the predicate, **KEEPS** the container
///   Note that rewritten predicate can evaluate to NULL when some of
///   the min/max values are not known. *Note that this is different than
///   the SQL filter semantics where `NULL` means the row is filtered
///   out.*
///
/// * `false`: there are no rows that could possibly match the predicate,
///   **PRUNES** the container
///
/// For example, given a column `x`, the `x_min`, `x_max`, `x_null_count`, and
/// `x_row_count` represent the minimum and maximum values, the null count of
/// column `x`, and the row count of column `x`, provided by the `PruningStatistics`.
/// `x_null_count` and `x_row_count` are used to handle the case where the column `x`
/// is known to be all `NULL`s. Note this is different from knowing nothing about
/// the column `x`, which confusingly is encoded by returning `NULL` for the min/max
/// values from [`PruningStatistics::max_values`] and [`PruningStatistics::min_values`].
///
/// Here are some examples of the rewritten predicates:
///
/// Original Predicate | Rewritten Predicate
/// ------------------ | --------------------
/// `x = 5` | `x_null_count != x_row_count AND (x_min <= 5 AND 5 <= x_max)`
/// `x < 5` | `x_null_count != x_row_count AND (x_min < 5)`
/// `x = 5 AND y = 10` | `x_null_count != x_row_count AND (x_min <= 5 AND 5 <= x_max) AND y_null_count != y_row_count (y_min <= 10 AND 10 <= y_max)`
/// `x IS NULL`  | `x_null_count > 0`
/// `x IS NOT NULL`  | `x_null_count != row_count`
/// `CAST(x as int) = 5` | `x_null_count != x_row_count (CAST(x_min as int) <= 5 AND 5 <= CAST(x_max as int))`
///
/// ## Predicate Evaluation
/// The PruningPredicate works in two passes
///
/// **First pass**:  For each `LiteralGuarantee` calls
/// [`PruningStatistics::contained`] and rules out containers where the
/// LiteralGuarantees are not satisfied
///
/// **Second Pass**: Evaluates the rewritten expression using the
/// min/max/null_counts/row_counts values for each column for each container. For any
/// container that this expression evaluates to `false`, it rules out those
/// containers.
///
///
/// ### Example 1
///
/// Given the predicate, `x = 5 AND y = 10`, the rewritten predicate would look like:
///
/// ```sql
/// x_null_count != x_row_count AND (x_min <= 5 AND 5 <= x_max)
/// AND
/// y_null_count != y_row_count AND (y_min <= 10 AND 10 <= y_max)
/// ```
///
/// If we know that for a given container, `x` is between `1 and 100` and we know that
/// `y` is between `4` and `7`, we know nothing about the null count and row count of
/// `x` and `y`, the input statistics might look like:
///
/// Column   | Value
/// -------- | -----
/// `x_min`  | `1`
/// `x_max`  | `100`
/// `x_null_count` | `null`
/// `x_row_count`  | `null`
/// `y_min`  | `4`
/// `y_max`  | `7`
/// `y_null_count` | `null`
/// `y_row_count`  | `null`
///
/// When these statistics values are substituted in to the rewritten predicate and
/// simplified, the result is `false`:
///
/// * `null != null AND (1 <= 5 AND 5 <= 100) AND null != null AND (4 <= 10 AND 10 <= 7)`
/// * `null = null` is `null` which is not true, so the AND moves on to the next clause
/// * `null and (1 <= 5 AND 5 <= 100) AND null AND (4 <= 10 AND 10 <= 7)`
/// * evaluating the clauses further we get:
/// * `null and true and null and false`
/// * `null and false`
/// * `false`
///
/// Returning `false` means the container can be pruned, which matches the
/// intuition that  `x = 5 AND y = 10` can’t be true for any row if all values of `y`
/// are `7` or less.
///
/// Note that if we had ended up with `null AND true AND null AND true` the result
/// would have been `null`.
/// `null` is treated the same as`true`, because we can't prove that the predicate is `false.`
///
/// If, for some other container, we knew `y` was between the values `4` and
/// `15`, then the rewritten predicate evaluates to `true` (verifying this is
/// left as an exercise to the reader -- are you still here?), and the container
/// **could not** be pruned. The intuition is that there may be rows where the
/// predicate *might* evaluate to `true`, and the only way to find out is to do
/// more analysis, for example by actually reading the data and evaluating the
/// predicate row by row.
///
/// ### Example 2
///
/// Given the same predicate, `x = 5 AND y = 10`, the rewritten predicate would
/// look like the same as example 1:
///
/// ```sql
/// x_null_count != x_row_count AND (x_min <= 5 AND 5 <= x_max)
/// AND
/// y_null_count != y_row_count AND (y_min <= 10 AND 10 <= y_max)
/// ```
///
/// If we know that for another given container, `x_min` is NULL and `x_max` is
/// NULL (the min/max values are unknown), `x_null_count` is `100` and `x_row_count`
///  is `100`; we know that `y` is between `4` and `7`, but we know nothing about
/// the null count and row count of `y`. The input statistics might look like:
///
/// Column   | Value
/// -------- | -----
/// `x_min`  | `null`
/// `x_max`  | `null`
/// `x_null_count` | `100`
/// `x_row_count`  | `100`
/// `y_min`  | `4`
/// `y_max`  | `7`
/// `y_null_count` | `null`
/// `y_row_count`  | `null`
///
/// When these statistics values are substituted in to the rewritten predicate and
/// simplified, the result is `false`:
///
/// * `100 != 100 AND (null <= 5 AND 5 <= null) AND null = null AND (4 <= 10 AND 10 <= 7)`
/// * `false AND null AND null AND false`
/// * `false AND false`
/// * `false`
///
/// Returning `false` means the container can be pruned, which matches the
/// intuition that  `x = 5 AND y = 10` can’t be true because all values in `x`
/// are known to be NULL.
///
/// # Related Work
///
/// [`PruningPredicate`] implements the type of min/max pruning described in
/// Section `3.3.3` of the [`Snowflake SIGMOD Paper`]. The technique is
/// described by various research such as [small materialized aggregates], [zone
/// maps], and [data skipping].
///
/// [`Snowflake SIGMOD Paper`]: https://dl.acm.org/doi/10.1145/2882903.2903741
/// [small materialized aggregates]: https://www.vldb.org/conf/1998/p476.pdf
/// [zone maps]: https://dl.acm.org/doi/10.1007/978-3-642-03730-6_10
/// [data skipping]: https://dl.acm.org/doi/10.1145/2588555.2610515
#[derive(Debug, Clone)]
pub struct PruningPredicate {
    /// The input schema against which the predicate will be evaluated
    schema: SchemaRef,
    /// A min/max pruning predicate (rewritten in terms of column min/max
    /// values, which are supplied by statistics)
    predicate_expr: Arc<dyn PhysicalExpr>,
    /// Description of which statistics are required to evaluate `predicate_expr`
    required_columns: RequiredColumns,
    /// Original physical predicate from which this predicate expr is derived
    /// (required for serialization)
    orig_expr: Arc<dyn PhysicalExpr>,
    /// [`LiteralGuarantee`]s used to try and prove a predicate can not possibly
    /// evaluate to `true`.
    ///
    /// See [`PruningPredicate::literal_guarantees`] for more details.
    literal_guarantees: Vec<LiteralGuarantee>,
}

/// Build a pruning predicate from an optional predicate expression.
/// If the predicate is None or the predicate cannot be converted to a pruning
/// predicate, return None.
/// If there is an error creating the pruning predicate it is recorded by incrementing
/// the `predicate_creation_errors` counter.
pub fn build_pruning_predicate(
    predicate: Arc<dyn PhysicalExpr>,
    file_schema: &SchemaRef,
    predicate_creation_errors: &Count,
) -> Option<Arc<PruningPredicate>> {
    match PruningPredicate::try_new(predicate, Arc::clone(file_schema)) {
        Ok(pruning_predicate) => {
            if !pruning_predicate.always_true() {
                return Some(Arc::new(pruning_predicate));
            }
        }
        Err(e) => {
            debug!("Could not create pruning predicate for: {e}");
            predicate_creation_errors.add(1);
        }
    }
    None
}

/// Rewrites predicates that [`PredicateRewriter`] can not handle, e.g. certain
/// complex expressions or predicates that reference columns that are not in the
/// schema.
pub trait UnhandledPredicateHook {
    /// Called when a predicate can not be rewritten in terms of statistics or
    /// references a column that is not in the schema.
    fn handle(&self, expr: &Arc<dyn PhysicalExpr>) -> Arc<dyn PhysicalExpr>;
}

/// The default handling for unhandled predicates is to return a constant `true`
/// (meaning don't prune the container)
#[derive(Debug, Clone)]
struct ConstantUnhandledPredicateHook {
    default: Arc<dyn PhysicalExpr>,
}

impl Default for ConstantUnhandledPredicateHook {
    fn default() -> Self {
        Self {
            default: Arc::new(phys_expr::Literal::new(ScalarValue::from(true))),
        }
    }
}

impl UnhandledPredicateHook for ConstantUnhandledPredicateHook {
    fn handle(&self, _expr: &Arc<dyn PhysicalExpr>) -> Arc<dyn PhysicalExpr> {
        Arc::clone(&self.default)
    }
}

impl PruningPredicate {
    /// Try to create a new instance of [`PruningPredicate`]
    ///
    /// This will translate the provided `expr` filter expression into
    /// a *pruning predicate*.
    ///
    /// A pruning predicate is one that has been rewritten in terms of
    /// the min and max values of column references and that evaluates
    /// to FALSE if the filter predicate would evaluate FALSE *for
    /// every row* whose values fell within the min / max ranges (aka
    /// could be pruned).
    ///
    /// The pruning predicate evaluates to TRUE or NULL
    /// if the filter predicate *might* evaluate to TRUE for at least
    /// one row whose values fell within the min/max ranges (in other
    /// words they might pass the predicate)
    ///
    /// For example, the filter expression `(column / 2) = 4` becomes
    /// the pruning predicate
    /// `(column_min / 2) <= 4 && 4 <= (column_max / 2))`
    ///
    /// See the struct level documentation on [`PruningPredicate`] for more
    /// details.
    pub fn try_new(expr: Arc<dyn PhysicalExpr>, schema: SchemaRef) -> Result<Self> {
        // Get a (simpler) snapshot of the physical expr here to use with `PruningPredicate`
        // which does not handle dynamic exprs in general
        let expr = snapshot_physical_expr(expr)?;
        let unhandled_hook = Arc::new(ConstantUnhandledPredicateHook::default()) as _;

        // build predicate expression once
        let mut required_columns = RequiredColumns::new();
        let predicate_expr = build_predicate_expression(
            &expr,
            &schema,
            &mut required_columns,
            &unhandled_hook,
        );
        let predicate_schema = required_columns.schema();
        // Simplify the newly created predicate to get rid of redundant casts, comparisons, etc.
        let predicate_expr =
            PhysicalExprSimplifier::new(&predicate_schema).simplify(predicate_expr)?;

        let literal_guarantees = LiteralGuarantee::analyze(&expr);

        Ok(Self {
            schema,
            predicate_expr,
            required_columns,
            orig_expr: expr,
            literal_guarantees,
        })
    }

    /// For each set of statistics, evaluates the pruning predicate
    /// and returns a `bool` with the following meaning for a
    /// all rows whose values match the statistics:
    ///
    /// `true`: There MAY be rows that match the predicate
    ///
    /// `false`: There are no rows that could possibly match the predicate
    ///
    /// Note: the predicate passed to `prune` should already be simplified as
    /// much as possible (e.g. this pass doesn't handle some
    /// expressions like `b = false`, but it does handle the
    /// simplified version `b`. See [`ExprSimplifier`] to simplify expressions.
    ///
    /// [`ExprSimplifier`]: https://docs.rs/datafusion/latest/datafusion/optimizer/simplify_expressions/struct.ExprSimplifier.html
    pub fn prune<S: PruningStatistics + ?Sized>(
        &self,
        statistics: &S,
    ) -> Result<Vec<bool>> {
        let mut builder = BoolVecBuilder::new(statistics.num_containers());

        // Try to prove the predicate can't be true for the containers based on
        // literal guarantees
        for literal_guarantee in &self.literal_guarantees {
            let LiteralGuarantee {
                column,
                guarantee,
                literals,
            } = literal_guarantee;
            if let Some(results) = statistics.contained(column, literals) {
                match guarantee {
                    // `In` means the values in the column must be one of the
                    // values in the set for the predicate to evaluate to true.
                    // If `contained` returns false, that means the column is
                    // not any of the values so we can prune the container
                    Guarantee::In => builder.combine_array(&results),
                    // `NotIn` means the values in the column must not be
                    // any of the values in the set for the predicate to
                    // evaluate to true. If `contained` returns true, it means the
                    // column is only in the set of values so we can prune the
                    // container
                    Guarantee::NotIn => {
                        builder.combine_array(&arrow::compute::not(&results)?)
                    }
                }
                // if all containers are pruned (has rows that DEFINITELY DO NOT pass the predicate)
                // can return early without evaluating the rest of predicates.
                if builder.check_all_pruned() {
                    return Ok(builder.build());
                }
            }
        }

        // Next, try to prove the predicate can't be true for the containers based
        // on min/max values

        // build a RecordBatch that contains the min/max values in the
        // appropriate statistics columns for the min/max predicate
        let statistics_batch =
            build_statistics_record_batch(statistics, &self.required_columns)?;

        // Evaluate the pruning predicate on that record batch and append any results to the builder
        builder.combine_value(self.predicate_expr.evaluate(&statistics_batch)?);

        Ok(builder.build())
    }

    /// Return a reference to the input schema
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Returns a reference to the physical expr used to construct this pruning predicate
    pub fn orig_expr(&self) -> &Arc<dyn PhysicalExpr> {
        &self.orig_expr
    }

    /// Returns a reference to the predicate expr
    pub fn predicate_expr(&self) -> &Arc<dyn PhysicalExpr> {
        &self.predicate_expr
    }

    /// Returns a reference to the literal guarantees
    ///
    /// Note that **All** `LiteralGuarantee`s must be satisfied for the
    /// expression to possibly be `true`. If any is not satisfied, the
    /// expression is guaranteed to be `null` or `false`.
    pub fn literal_guarantees(&self) -> &[LiteralGuarantee] {
        &self.literal_guarantees
    }

    /// Returns true if this pruning predicate can not prune anything.
    ///
    /// This happens if the predicate is a literal `true`  and
    /// literal_guarantees is empty.
    ///
    /// This can happen when a predicate is simplified to a constant `true`
    pub fn always_true(&self) -> bool {
        is_always_true(&self.predicate_expr) && self.literal_guarantees.is_empty()
    }

    // this is only used by `parquet` feature right now
    #[allow(dead_code)]
    pub fn required_columns(&self) -> &RequiredColumns {
        &self.required_columns
    }

    /// Names of the columns that are known to be / not be in a set
    /// of literals (constants). These are the columns the that may be passed to
    /// [`PruningStatistics::contained`] during pruning.
    ///
    /// This is useful to avoid fetching statistics for columns that will not be
    /// used in the predicate. For example, it can be used to avoid reading
    /// unneeded bloom filters (a non trivial operation).
    pub fn literal_columns(&self) -> Vec<String> {
        let mut seen = HashSet::new();
        self.literal_guarantees
            .iter()
            .map(|e| &e.column.name)
            // avoid duplicates
            .filter(|name| seen.insert(*name))
            .map(|s| s.to_string())
            .collect()
    }
}

/// Builds the return `Vec` for [`PruningPredicate::prune`].
#[derive(Debug)]
struct BoolVecBuilder {
    /// One element per container. Each element is
    /// * `true`: if the container has row that may pass the predicate
    /// * `false`: if the container has rows that DEFINITELY DO NOT pass the predicate
    inner: Vec<bool>,
}

impl BoolVecBuilder {
    /// Create a new `BoolVecBuilder` with `num_containers` elements
    fn new(num_containers: usize) -> Self {
        Self {
            // assume by default all containers may pass the predicate
            inner: vec![true; num_containers],
        }
    }

    /// Combines result `array` for a conjunct (e.g. `AND` clause) of a
    /// predicate into the currently in progress array.
    ///
    /// Each `array` element is:
    /// * `true`: container has row that may pass the predicate
    /// * `false`: all container rows DEFINITELY DO NOT pass the predicate
    /// * `null`: container may or may not have rows that pass the predicate
    fn combine_array(&mut self, array: &BooleanArray) {
        assert_eq!(array.len(), self.inner.len());
        for (cur, new) in self.inner.iter_mut().zip(array.iter()) {
            // `false` for this conjunct means we know for sure no rows could
            // pass the predicate and thus we set the corresponding container
            // location to false.
            if let Some(false) = new {
                *cur = false;
            }
        }
    }

    /// Combines the results in the [`ColumnarValue`] to the currently in
    /// progress array, following the same rules as [`Self::combine_array`].
    ///
    /// # Panics
    /// If `value` is not boolean
    fn combine_value(&mut self, value: ColumnarValue) {
        match value {
            ColumnarValue::Array(array) => {
                self.combine_array(array.as_boolean());
            }
            ColumnarValue::Scalar(ScalarValue::Boolean(Some(false))) => {
                // False means all containers can not pass the predicate
                self.inner = vec![false; self.inner.len()];
            }
            _ => {
                // Null or true means the rows in container may pass this
                // conjunct so we can't prune any containers based on that
            }
        }
    }

    /// Convert this builder into a Vec of bools
    fn build(self) -> Vec<bool> {
        self.inner
    }

    /// Check all containers has rows that DEFINITELY DO NOT pass the predicate
    fn check_all_pruned(&self) -> bool {
        self.inner.iter().all(|&x| !x)
    }
}

fn is_always_true(expr: &Arc<dyn PhysicalExpr>) -> bool {
    expr.as_any()
        .downcast_ref::<phys_expr::Literal>()
        .map(|l| matches!(l.value(), ScalarValue::Boolean(Some(true))))
        .unwrap_or_default()
}

fn is_always_false(expr: &Arc<dyn PhysicalExpr>) -> bool {
    expr.as_any()
        .downcast_ref::<phys_expr::Literal>()
        .map(|l| matches!(l.value(), ScalarValue::Boolean(Some(false))))
        .unwrap_or_default()
}

/// Describes which columns statistics are necessary to evaluate a
/// [`PruningPredicate`].
///
/// This structure permits reading and creating the minimum number statistics,
/// which is important since statistics may be non trivial to read (e.g. large
/// strings or when there are 1000s of columns).
///
/// Handles creating references to the min/max statistics
/// for columns as well as recording which statistics are needed
#[derive(Debug, Default, Clone)]
pub struct RequiredColumns {
    /// The statistics required to evaluate this predicate:
    /// * The unqualified column in the input schema
    /// * Statistics type (e.g. Min or Max or Null_Count)
    /// * The field the statistics value should be placed in for
    ///   pruning predicate evaluation (e.g. `min_value` or `max_value`)
    columns: Vec<(phys_expr::Column, StatisticsType, Field)>,
}

impl RequiredColumns {
    fn new() -> Self {
        Self::default()
    }

    /// Returns Some(column) if this is a single column predicate.
    ///
    /// Returns None if this is a multi-column predicate.
    ///
    /// Examples:
    /// * `a > 5 OR a < 10` returns `Some(a)`
    /// * `a > 5 OR b < 10` returns `None`
    /// * `true` returns None
    #[allow(dead_code)]
    // this fn is only used by `parquet` feature right now, thus the `allow(dead_code)`
    pub fn single_column(&self) -> Option<&phys_expr::Column> {
        if self.columns.windows(2).all(|w| {
            // check if all columns are the same (ignoring statistics and field)
            let c1 = &w[0].0;
            let c2 = &w[1].0;
            c1 == c2
        }) {
            self.columns.first().map(|r| &r.0)
        } else {
            None
        }
    }

    /// Returns a schema that describes the columns required to evaluate this
    /// pruning predicate.
    /// The schema contains the fields for each column in `self.columns` with
    /// the appropriate data type for the statistics.
    /// Order matters, this same order is used to evaluate the
    /// pruning predicate.
    fn schema(&self) -> Schema {
        let fields = self
            .columns
            .iter()
            .map(|(_c, _t, f)| f.clone())
            .collect::<Vec<_>>();
        Schema::new(fields)
    }

    /// Returns an iterator over items in columns (see doc on
    /// `self.columns` for details)
    pub(crate) fn iter(
        &self,
    ) -> impl Iterator<Item = &(phys_expr::Column, StatisticsType, Field)> {
        self.columns.iter()
    }

    fn find_stat_column(
        &self,
        column: &phys_expr::Column,
        statistics_type: StatisticsType,
    ) -> Option<usize> {
        match statistics_type {
            StatisticsType::RowCount => {
                // Use the first row count we find, if any
                self.columns
                    .iter()
                    .enumerate()
                    .find(|(_i, (_c, t, _f))| t == &statistics_type)
                    .map(|(i, (_c, _t, _f))| i)
            }
            _ => self
                .columns
                .iter()
                .enumerate()
                .find(|(_i, (c, t, _f))| c == column && t == &statistics_type)
                .map(|(i, (_c, _t, _f))| i),
        }
    }

    /// Rewrites column_expr so that all appearances of column
    /// are replaced with a reference to either the min or max
    /// statistics column, while keeping track that a reference to the statistics
    /// column is required
    ///
    /// for example, an expression like `col("foo") > 5`, when called
    /// with Max would result in an expression like `col("foo_max") >
    /// 5` with the appropriate entry noted in self.columns
    fn stat_column_expr(
        &mut self,
        column: &phys_expr::Column,
        column_expr: &Arc<dyn PhysicalExpr>,
        field: &Field,
        stat_type: StatisticsType,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let (idx, need_to_insert) = match self.find_stat_column(column, stat_type) {
            Some(idx) => (idx, false),
            None => (self.columns.len(), true),
        };

        let column_name = column.name();
        let stat_column_name = match stat_type {
            StatisticsType::Min => format!("{column_name}_min"),
            StatisticsType::Max => format!("{column_name}_max"),
            StatisticsType::NullCount => format!("{column_name}_null_count"),
            StatisticsType::RowCount => "row_count".to_string(),
        };

        let stat_column = phys_expr::Column::new(&stat_column_name, idx);

        // only add statistics column if not previously added
        if need_to_insert {
            // may be null if statistics are not present
            let nullable = true;
            let stat_field =
                Field::new(stat_column.name(), field.data_type().clone(), nullable);
            self.columns.push((column.clone(), stat_type, stat_field));
        }
        rewrite_column_expr(Arc::clone(column_expr), column, &stat_column)
    }

    /// rewrite col --> col_min
    fn min_column_expr(
        &mut self,
        column: &phys_expr::Column,
        column_expr: &Arc<dyn PhysicalExpr>,
        field: &Field,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        self.stat_column_expr(column, column_expr, field, StatisticsType::Min)
    }

    /// rewrite col --> col_max
    fn max_column_expr(
        &mut self,
        column: &phys_expr::Column,
        column_expr: &Arc<dyn PhysicalExpr>,
        field: &Field,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        self.stat_column_expr(column, column_expr, field, StatisticsType::Max)
    }

    /// rewrite col --> col_null_count
    fn null_count_column_expr(
        &mut self,
        column: &phys_expr::Column,
        column_expr: &Arc<dyn PhysicalExpr>,
        field: &Field,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        self.stat_column_expr(column, column_expr, field, StatisticsType::NullCount)
    }

    /// rewrite col --> col_row_count
    fn row_count_column_expr(
        &mut self,
        column: &phys_expr::Column,
        column_expr: &Arc<dyn PhysicalExpr>,
        field: &Field,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        self.stat_column_expr(column, column_expr, field, StatisticsType::RowCount)
    }
}

impl From<Vec<(phys_expr::Column, StatisticsType, Field)>> for RequiredColumns {
    fn from(columns: Vec<(phys_expr::Column, StatisticsType, Field)>) -> Self {
        Self { columns }
    }
}

/// Build a RecordBatch from a list of statistics, creating arrays,
/// with one row for each PruningStatistics and columns specified in
/// the required_columns parameter.
///
/// For example, if the requested columns are
/// ```text
/// ("s1", Min, Field:s1_min)
/// ("s2", Max, field:s2_max)
/// ```
///
/// And the input statistics had
/// ```text
/// S1(Min: 5, Max: 10)
/// S2(Min: 99, Max: 1000)
/// S3(Min: 1, Max: 2)
/// ```
///
/// Then this function would build a record batch with 2 columns and
/// one row s1_min and s2_max as follows (s3 is not requested):
///
/// ```text
/// s1_min | s2_max
/// -------+--------
///   5    | 1000
/// ```
fn build_statistics_record_batch<S: PruningStatistics + ?Sized>(
    statistics: &S,
    required_columns: &RequiredColumns,
) -> Result<RecordBatch> {
    let mut arrays = Vec::<ArrayRef>::new();
    // For each needed statistics column:
    for (column, statistics_type, stat_field) in required_columns.iter() {
        let column = Column::from_name(column.name());
        let data_type = stat_field.data_type();

        let num_containers = statistics.num_containers();

        let array = match statistics_type {
            StatisticsType::Min => statistics.min_values(&column),
            StatisticsType::Max => statistics.max_values(&column),
            StatisticsType::NullCount => statistics.null_counts(&column),
            StatisticsType::RowCount => statistics.row_counts(&column),
        };
        let array = array.unwrap_or_else(|| new_null_array(data_type, num_containers));

        assert_eq_or_internal_err!(
            num_containers,
            array.len(),
            "mismatched statistics length. Expected {}, got {}",
            num_containers,
            array.len()
        );

        // cast statistics array to required data type (e.g. parquet
        // provides timestamp statistics as "Int64")
        let array = arrow::compute::cast(&array, data_type)?;

        arrays.push(array);
    }

    let schema = Arc::new(required_columns.schema());
    // provide the count in case there were no needed statistics
    let mut options = RecordBatchOptions::default();
    options.row_count = Some(statistics.num_containers());

    trace!("Creating statistics batch for {required_columns:#?} with {arrays:#?}");

    RecordBatch::try_new_with_options(schema, arrays, &options).map_err(|err| {
        plan_datafusion_err!("Can not create statistics record batch: {err}")
    })
}

struct PruningExpressionBuilder<'a> {
    column: phys_expr::Column,
    column_expr: Arc<dyn PhysicalExpr>,
    op: Operator,
    scalar_expr: Arc<dyn PhysicalExpr>,
    field: &'a Field,
    required_columns: &'a mut RequiredColumns,
}

impl<'a> PruningExpressionBuilder<'a> {
    fn try_new(
        left: &'a Arc<dyn PhysicalExpr>,
        right: &'a Arc<dyn PhysicalExpr>,
        op: Operator,
        schema: &'a SchemaRef,
        required_columns: &'a mut RequiredColumns,
    ) -> Result<Self> {
        // find column name; input could be a more complicated expression
        let left_columns = collect_columns(left);
        let right_columns = collect_columns(right);
        let (column_expr, scalar_expr, columns, correct_operator) =
            match (left_columns.len(), right_columns.len()) {
                (1, 0) => (left, right, left_columns, op),
                (0, 1) => (right, left, right_columns, reverse_operator(op)?),
                _ => {
                    // if more than one column used in expression - not supported
                    return plan_err!(
                        "Multi-column expressions are not currently supported"
                    );
                }
            };

        let df_schema = DFSchema::try_from(Arc::clone(schema))?;
        let (column_expr, correct_operator, scalar_expr) = rewrite_expr_to_prunable(
            column_expr,
            correct_operator,
            scalar_expr,
            df_schema,
        )?;
        let column = columns.iter().next().unwrap().clone();
        let field = match schema.column_with_name(column.name()) {
            Some((_, f)) => f,
            _ => {
                return plan_err!("Field not found in schema");
            }
        };

        Ok(Self {
            column,
            column_expr,
            op: correct_operator,
            scalar_expr,
            field,
            required_columns,
        })
    }

    fn op(&self) -> Operator {
        self.op
    }

    fn scalar_expr(&self) -> &Arc<dyn PhysicalExpr> {
        &self.scalar_expr
    }

    fn min_column_expr(&mut self) -> Result<Arc<dyn PhysicalExpr>> {
        self.required_columns
            .min_column_expr(&self.column, &self.column_expr, self.field)
    }

    fn max_column_expr(&mut self) -> Result<Arc<dyn PhysicalExpr>> {
        self.required_columns
            .max_column_expr(&self.column, &self.column_expr, self.field)
    }

    /// This function is to simply retune the `null_count` physical expression no matter what the
    /// predicate expression is
    ///
    /// i.e., x > 5 => x_null_count,
    ///       cast(x as int) < 10 => x_null_count,
    ///       try_cast(x as float) < 10.0 => x_null_count
    fn null_count_column_expr(&mut self) -> Result<Arc<dyn PhysicalExpr>> {
        // Retune to [`phys_expr::Column`]
        let column_expr = Arc::new(self.column.clone()) as _;

        // null_count is DataType::UInt64, which is different from the column's data type (i.e. self.field)
        let null_count_field = &Field::new(self.field.name(), DataType::UInt64, true);

        self.required_columns.null_count_column_expr(
            &self.column,
            &column_expr,
            null_count_field,
        )
    }

    /// This function is to simply retune the `row_count` physical expression no matter what the
    /// predicate expression is
    ///
    /// i.e., x > 5 => x_row_count,
    ///       cast(x as int) < 10 => x_row_count,
    ///       try_cast(x as float) < 10.0 => x_row_count
    fn row_count_column_expr(&mut self) -> Result<Arc<dyn PhysicalExpr>> {
        // Retune to [`phys_expr::Column`]
        let column_expr = Arc::new(self.column.clone()) as _;

        // row_count is DataType::UInt64, which is different from the column's data type (i.e. self.field)
        let row_count_field = &Field::new(self.field.name(), DataType::UInt64, true);

        self.required_columns.row_count_column_expr(
            &self.column,
            &column_expr,
            row_count_field,
        )
    }
}

/// This function is designed to rewrite the column_expr to
/// ensure the column_expr is monotonically increasing.
///
/// For example,
/// 1. `col > 10`
/// 2. `-col > 10` should be rewritten to `col < -10`
/// 3. `!col = true` would be rewritten to `col = !true`
/// 4. `abs(a - 10) > 0` not supported
/// 5. `cast(can_prunable_expr) > 10`
/// 6. `try_cast(can_prunable_expr) > 10`
///
/// More rewrite rules are still in progress.
fn rewrite_expr_to_prunable(
    column_expr: &PhysicalExprRef,
    op: Operator,
    scalar_expr: &PhysicalExprRef,
    schema: DFSchema,
) -> Result<(PhysicalExprRef, Operator, PhysicalExprRef)> {
    if !is_compare_op(op) {
        return plan_err!("rewrite_expr_to_prunable only support compare expression");
    }

    let column_expr_any = column_expr.as_any();

    if column_expr_any
        .downcast_ref::<phys_expr::Column>()
        .is_some()
    {
        // `col op lit()`
        Ok((Arc::clone(column_expr), op, Arc::clone(scalar_expr)))
    } else if let Some(cast) = column_expr_any.downcast_ref::<phys_expr::CastExpr>() {
        // `cast(col) op lit()`
        let arrow_schema = schema.as_arrow();
        let from_type = cast.expr().data_type(arrow_schema)?;
        verify_support_type_for_prune(&from_type, cast.cast_type())?;
        let (left, op, right) =
            rewrite_expr_to_prunable(cast.expr(), op, scalar_expr, schema)?;
        let left = Arc::new(phys_expr::CastExpr::new(
            left,
            cast.cast_type().clone(),
            None,
        ));
        Ok((left, op, right))
    } else if let Some(try_cast) =
        column_expr_any.downcast_ref::<phys_expr::TryCastExpr>()
    {
        // `try_cast(col) op lit()`
        let arrow_schema = schema.as_arrow();
        let from_type = try_cast.expr().data_type(arrow_schema)?;
        verify_support_type_for_prune(&from_type, try_cast.cast_type())?;
        let (left, op, right) =
            rewrite_expr_to_prunable(try_cast.expr(), op, scalar_expr, schema)?;
        let left = Arc::new(phys_expr::TryCastExpr::new(
            left,
            try_cast.cast_type().clone(),
        ));
        Ok((left, op, right))
    } else if let Some(neg) = column_expr_any.downcast_ref::<phys_expr::NegativeExpr>() {
        // `-col > lit()`  --> `col < -lit()`
        let (left, op, right) =
            rewrite_expr_to_prunable(neg.arg(), op, scalar_expr, schema)?;
        let right = Arc::new(phys_expr::NegativeExpr::new(right));
        Ok((left, reverse_operator(op)?, right))
    } else if let Some(not) = column_expr_any.downcast_ref::<phys_expr::NotExpr>() {
        // `!col = true` --> `col = !true`
        if op != Operator::Eq && op != Operator::NotEq {
            return plan_err!("Not with operator other than Eq / NotEq is not supported");
        }
        if not
            .arg()
            .as_any()
            .downcast_ref::<phys_expr::Column>()
            .is_some()
        {
            let left = Arc::clone(not.arg());
            let right = Arc::new(phys_expr::NotExpr::new(Arc::clone(scalar_expr)));
            Ok((left, reverse_operator(op)?, right))
        } else {
            plan_err!("Not with complex expression {column_expr:?} is not supported")
        }
    } else {
        plan_err!("column expression {column_expr:?} is not supported")
    }
}

fn is_compare_op(op: Operator) -> bool {
    matches!(
        op,
        Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq
            | Operator::LikeMatch
            | Operator::NotLikeMatch
    )
}

fn is_string_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

// The pruning logic is based on the comparing the min/max bounds.
// Must make sure the two type has order.
// For example, casts from string to numbers is not correct.
// Because the "13" is less than "3" with UTF8 comparison order.
fn verify_support_type_for_prune(from_type: &DataType, to_type: &DataType) -> Result<()> {
    // Dictionary casts are always supported as long as the value types are supported
    let from_type = match from_type {
        DataType::Dictionary(_, t) => {
            return verify_support_type_for_prune(t.as_ref(), to_type)
        }
        _ => from_type,
    };
    let to_type = match to_type {
        DataType::Dictionary(_, t) => {
            return verify_support_type_for_prune(from_type, t.as_ref())
        }
        _ => to_type,
    };
    // If both types are strings or both are not strings (number, timestamp, etc)
    // then we can compare them.
    // PruningPredicate does not support casting of strings to numbers and such.
    if is_string_type(from_type) == is_string_type(to_type) {
        Ok(())
    } else {
        plan_err!(
            "Try Cast/Cast with from type {from_type} to type {to_type} is not supported"
        )
    }
}

/// replaces a column with an old name with a new name in an expression
fn rewrite_column_expr(
    e: Arc<dyn PhysicalExpr>,
    column_old: &phys_expr::Column,
    column_new: &phys_expr::Column,
) -> Result<Arc<dyn PhysicalExpr>> {
    e.transform(|expr| {
        if let Some(column) = expr.as_any().downcast_ref::<phys_expr::Column>() {
            if column == column_old {
                return Ok(Transformed::yes(Arc::new(column_new.clone())));
            }
        }

        Ok(Transformed::no(expr))
    })
    .data()
}

fn reverse_operator(op: Operator) -> Result<Operator> {
    op.swap().ok_or_else(|| {
        internal_datafusion_err!(
            "Could not reverse operator {op} while building pruning predicate"
        )
    })
}

/// Given a column reference to `column`, returns a pruning
/// expression in terms of the min and max that will evaluate to true
/// if the column may contain values, and false if definitely does not
/// contain values
fn build_single_column_expr(
    column: &phys_expr::Column,
    schema: &Schema,
    required_columns: &mut RequiredColumns,
    is_not: bool, // if true, treat as !col
) -> Option<Arc<dyn PhysicalExpr>> {
    let field = schema.field_with_name(column.name()).ok()?;

    if matches!(field.data_type(), &DataType::Boolean) {
        let col_ref = Arc::new(column.clone()) as _;

        let min = required_columns
            .min_column_expr(column, &col_ref, field)
            .ok()?;
        let max = required_columns
            .max_column_expr(column, &col_ref, field)
            .ok()?;

        // remember -- we want an expression that is:
        // TRUE: if there may be rows that match
        // FALSE: if there are no rows that match
        if is_not {
            // The only way we know a column couldn't match is if both the min and max are true
            // !(min && max)
            Some(Arc::new(phys_expr::NotExpr::new(Arc::new(
                phys_expr::BinaryExpr::new(min, Operator::And, max),
            ))))
        } else {
            // the only way we know a column couldn't match is if both the min and max are false
            // !(!min && !max) --> min || max
            Some(Arc::new(phys_expr::BinaryExpr::new(min, Operator::Or, max)))
        }
    } else {
        None
    }
}

/// Given an expression reference to `expr`, if `expr` is a column expression,
/// returns a pruning expression in terms of IsNull that will evaluate to true
/// if the column may contain null, and false if definitely does not
/// contain null.
/// If `with_not` is true, build a pruning expression for `col IS NOT NULL`: `col_count != col_null_count`
/// The pruning expression evaluates to true ONLY if the column definitely CONTAINS
/// at least one NULL value.  In this case we can know that `IS NOT NULL` can not be true and
/// thus can prune the row group / value
fn build_is_null_column_expr(
    expr: &Arc<dyn PhysicalExpr>,
    schema: &Schema,
    required_columns: &mut RequiredColumns,
    with_not: bool,
) -> Option<Arc<dyn PhysicalExpr>> {
    if let Some(col) = expr.as_any().downcast_ref::<phys_expr::Column>() {
        let field = schema.field_with_name(col.name()).ok()?;

        let null_count_field = &Field::new(field.name(), DataType::UInt64, true);
        if with_not {
            if let Ok(row_count_expr) =
                required_columns.row_count_column_expr(col, expr, null_count_field)
            {
                required_columns
                    .null_count_column_expr(col, expr, null_count_field)
                    .map(|null_count_column_expr| {
                        // IsNotNull(column) => null_count != row_count
                        Arc::new(phys_expr::BinaryExpr::new(
                            null_count_column_expr,
                            Operator::NotEq,
                            row_count_expr,
                        )) as _
                    })
                    .ok()
            } else {
                None
            }
        } else {
            required_columns
                .null_count_column_expr(col, expr, null_count_field)
                .map(|null_count_column_expr| {
                    // IsNull(column) => null_count > 0
                    Arc::new(phys_expr::BinaryExpr::new(
                        null_count_column_expr,
                        Operator::Gt,
                        Arc::new(phys_expr::Literal::new(ScalarValue::UInt64(Some(0)))),
                    )) as _
                })
                .ok()
        }
    } else {
        None
    }
}

/// The maximum number of entries in an `InList` that might be rewritten into
/// an OR chain
const MAX_LIST_VALUE_SIZE_REWRITE: usize = 20;

/// Rewrite a predicate expression in terms of statistics (min/max/null_counts)
/// for use as a [`PruningPredicate`].
pub struct PredicateRewriter {
    unhandled_hook: Arc<dyn UnhandledPredicateHook>,
}

impl Default for PredicateRewriter {
    fn default() -> Self {
        Self {
            unhandled_hook: Arc::new(ConstantUnhandledPredicateHook::default()),
        }
    }
}

impl PredicateRewriter {
    /// Create a new `PredicateRewriter`
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the unhandled hook to be used when a predicate can not be rewritten
    pub fn with_unhandled_hook(
        self,
        unhandled_hook: Arc<dyn UnhandledPredicateHook>,
    ) -> Self {
        Self { unhandled_hook }
    }

    /// Translate logical filter expression into pruning predicate
    /// expression that will evaluate to FALSE if it can be determined no
    /// rows between the min/max values could pass the predicates.
    ///
    /// Any predicates that can not be translated will be passed to `unhandled_hook`.
    ///
    /// Returns the pruning predicate as an [`PhysicalExpr`]
    ///
    /// Notice: Does not handle [`phys_expr::InListExpr`] greater than 20, which will fall back to calling `unhandled_hook`
    pub fn rewrite_predicate_to_statistics_predicate(
        &self,
        expr: &Arc<dyn PhysicalExpr>,
        schema: &Schema,
    ) -> Arc<dyn PhysicalExpr> {
        let mut required_columns = RequiredColumns::new();
        build_predicate_expression(
            expr,
            &Arc::new(schema.clone()),
            &mut required_columns,
            &self.unhandled_hook,
        )
    }
}

/// Translate logical filter expression into pruning predicate
/// expression that will evaluate to FALSE if it can be determined no
/// rows between the min/max values could pass the predicates.
///
/// Any predicates that can not be translated will be passed to `unhandled_hook`.
///
/// Returns the pruning predicate as an [`PhysicalExpr`]
///
/// Notice: Does not handle [`phys_expr::InListExpr`] greater than 20, which will fall back to calling `unhandled_hook`
fn build_predicate_expression(
    expr: &Arc<dyn PhysicalExpr>,
    schema: &SchemaRef,
    required_columns: &mut RequiredColumns,
    unhandled_hook: &Arc<dyn UnhandledPredicateHook>,
) -> Arc<dyn PhysicalExpr> {
    if is_always_false(expr) {
        // Shouldn't return `unhandled_hook.handle(expr)`
        // Because it will transfer false to true.
        return Arc::clone(expr);
    }
    // predicate expression can only be a binary expression
    let expr_any = expr.as_any();
    if let Some(is_null) = expr_any.downcast_ref::<phys_expr::IsNullExpr>() {
        return build_is_null_column_expr(is_null.arg(), schema, required_columns, false)
            .unwrap_or_else(|| unhandled_hook.handle(expr));
    }
    if let Some(is_not_null) = expr_any.downcast_ref::<phys_expr::IsNotNullExpr>() {
        return build_is_null_column_expr(
            is_not_null.arg(),
            schema,
            required_columns,
            true,
        )
        .unwrap_or_else(|| unhandled_hook.handle(expr));
    }
    if let Some(col) = expr_any.downcast_ref::<phys_expr::Column>() {
        return build_single_column_expr(col, schema, required_columns, false)
            .unwrap_or_else(|| unhandled_hook.handle(expr));
    }
    if let Some(not) = expr_any.downcast_ref::<phys_expr::NotExpr>() {
        // match !col (don't do so recursively)
        if let Some(col) = not.arg().as_any().downcast_ref::<phys_expr::Column>() {
            return build_single_column_expr(col, schema, required_columns, true)
                .unwrap_or_else(|| unhandled_hook.handle(expr));
        } else {
            return unhandled_hook.handle(expr);
        }
    }
    if let Some(in_list) = expr_any.downcast_ref::<phys_expr::InListExpr>() {
        if !in_list.list().is_empty()
            && in_list.list().len() <= MAX_LIST_VALUE_SIZE_REWRITE
        {
            let eq_op = if in_list.negated() {
                Operator::NotEq
            } else {
                Operator::Eq
            };
            let re_op = if in_list.negated() {
                Operator::And
            } else {
                Operator::Or
            };
            let change_expr = in_list
                .list()
                .iter()
                .map(|e| {
                    Arc::new(phys_expr::BinaryExpr::new(
                        Arc::clone(in_list.expr()),
                        eq_op,
                        Arc::clone(e),
                    )) as _
                })
                .reduce(|a, b| Arc::new(phys_expr::BinaryExpr::new(a, re_op, b)) as _)
                .unwrap();
            return build_predicate_expression(
                &change_expr,
                schema,
                required_columns,
                unhandled_hook,
            );
        } else {
            return unhandled_hook.handle(expr);
        }
    }

    let (left, op, right) = {
        if let Some(bin_expr) = expr_any.downcast_ref::<phys_expr::BinaryExpr>() {
            (
                Arc::clone(bin_expr.left()),
                *bin_expr.op(),
                Arc::clone(bin_expr.right()),
            )
        } else if let Some(like_expr) = expr_any.downcast_ref::<phys_expr::LikeExpr>() {
            if like_expr.case_insensitive() {
                return unhandled_hook.handle(expr);
            }
            let op = match (like_expr.negated(), like_expr.case_insensitive()) {
                (false, false) => Operator::LikeMatch,
                (true, false) => Operator::NotLikeMatch,
                (false, true) => Operator::ILikeMatch,
                (true, true) => Operator::NotILikeMatch,
            };
            (
                Arc::clone(like_expr.expr()),
                op,
                Arc::clone(like_expr.pattern()),
            )
        } else {
            return unhandled_hook.handle(expr);
        }
    };

    if op == Operator::And || op == Operator::Or {
        let left_expr =
            build_predicate_expression(&left, schema, required_columns, unhandled_hook);
        let right_expr =
            build_predicate_expression(&right, schema, required_columns, unhandled_hook);
        // simplify boolean expression if applicable
        let expr = match (&left_expr, op, &right_expr) {
            (left, Operator::And, right)
                if is_always_false(left) || is_always_false(right) =>
            {
                Arc::new(phys_expr::Literal::new(ScalarValue::Boolean(Some(false))))
            }
            (left, Operator::And, _) if is_always_true(left) => right_expr,
            (_, Operator::And, right) if is_always_true(right) => left_expr,
            (left, Operator::Or, right)
                if is_always_true(left) || is_always_true(right) =>
            {
                Arc::new(phys_expr::Literal::new(ScalarValue::Boolean(Some(true))))
            }
            (left, Operator::Or, _) if is_always_false(left) => right_expr,
            (_, Operator::Or, right) if is_always_false(right) => left_expr,

            _ => Arc::new(phys_expr::BinaryExpr::new(left_expr, op, right_expr)),
        };
        return expr;
    }

    let expr_builder =
        PruningExpressionBuilder::try_new(&left, &right, op, schema, required_columns);
    let mut expr_builder = match expr_builder {
        Ok(builder) => builder,
        // allow partial failure in predicate expression generation
        // this can still produce a useful predicate when multiple conditions are joined using AND
        Err(e) => {
            debug!("Error building pruning expression: {e}");
            return unhandled_hook.handle(expr);
        }
    };

    build_statistics_expr(&mut expr_builder)
        .unwrap_or_else(|_| unhandled_hook.handle(expr))
}

fn build_statistics_expr(
    expr_builder: &mut PruningExpressionBuilder,
) -> Result<Arc<dyn PhysicalExpr>> {
    let statistics_expr: Arc<dyn PhysicalExpr> = match expr_builder.op() {
        Operator::NotEq => {
            // column != literal => (min, max) = literal =>
            // !(min != literal && max != literal) ==>
            // min != literal || literal != max
            let min_column_expr = expr_builder.min_column_expr()?;
            let max_column_expr = expr_builder.max_column_expr()?;
            Arc::new(phys_expr::BinaryExpr::new(
                Arc::new(phys_expr::BinaryExpr::new(
                    min_column_expr,
                    Operator::NotEq,
                    Arc::clone(expr_builder.scalar_expr()),
                )),
                Operator::Or,
                Arc::new(phys_expr::BinaryExpr::new(
                    Arc::clone(expr_builder.scalar_expr()),
                    Operator::NotEq,
                    max_column_expr,
                )),
            ))
        }
        Operator::Eq => {
            // column = literal => (min, max) = literal => min <= literal && literal <= max
            // (column / 2) = 4 => (column_min / 2) <= 4 && 4 <= (column_max / 2)
            let min_column_expr = expr_builder.min_column_expr()?;
            let max_column_expr = expr_builder.max_column_expr()?;
            Arc::new(phys_expr::BinaryExpr::new(
                Arc::new(phys_expr::BinaryExpr::new(
                    min_column_expr,
                    Operator::LtEq,
                    Arc::clone(expr_builder.scalar_expr()),
                )),
                Operator::And,
                Arc::new(phys_expr::BinaryExpr::new(
                    Arc::clone(expr_builder.scalar_expr()),
                    Operator::LtEq,
                    max_column_expr,
                )),
            ))
        }
        Operator::NotLikeMatch => build_not_like_match(expr_builder)?,
        Operator::LikeMatch => build_like_match(expr_builder).ok_or_else(|| {
            plan_datafusion_err!(
                "LIKE expression with wildcard at the beginning is not supported"
            )
        })?,
        Operator::Gt => {
            // column > literal => (min, max) > literal => max > literal
            Arc::new(phys_expr::BinaryExpr::new(
                expr_builder.max_column_expr()?,
                Operator::Gt,
                Arc::clone(expr_builder.scalar_expr()),
            ))
        }
        Operator::GtEq => {
            // column >= literal => (min, max) >= literal => max >= literal
            Arc::new(phys_expr::BinaryExpr::new(
                expr_builder.max_column_expr()?,
                Operator::GtEq,
                Arc::clone(expr_builder.scalar_expr()),
            ))
        }
        Operator::Lt => {
            // column < literal => (min, max) < literal => min < literal
            Arc::new(phys_expr::BinaryExpr::new(
                expr_builder.min_column_expr()?,
                Operator::Lt,
                Arc::clone(expr_builder.scalar_expr()),
            ))
        }
        Operator::LtEq => {
            // column <= literal => (min, max) <= literal => min <= literal
            Arc::new(phys_expr::BinaryExpr::new(
                expr_builder.min_column_expr()?,
                Operator::LtEq,
                Arc::clone(expr_builder.scalar_expr()),
            ))
        }
        // other expressions are not supported
        _ => {
            return plan_err!(
                "expressions other than (neq, eq, gt, gteq, lt, lteq) are not supported"
            );
        }
    };
    let statistics_expr = wrap_null_count_check_expr(statistics_expr, expr_builder)?;
    Ok(statistics_expr)
}

/// returns the string literal of the scalar value if it is a string
fn unpack_string(s: &ScalarValue) -> Option<&str> {
    s.try_as_str().flatten()
}

fn extract_string_literal(expr: &Arc<dyn PhysicalExpr>) -> Option<&str> {
    if let Some(lit) = expr.as_any().downcast_ref::<phys_expr::Literal>() {
        let s = unpack_string(lit.value())?;
        return Some(s);
    }
    None
}

/// Convert `column LIKE literal` where P is a constant prefix of the literal
/// to a range check on the column: `P <= column && column < P'`, where P' is the
/// lowest string after all P* strings.
fn build_like_match(
    expr_builder: &mut PruningExpressionBuilder,
) -> Option<Arc<dyn PhysicalExpr>> {
    // column LIKE literal => (min, max) LIKE literal split at % => min <= split literal && split literal <= max
    // column LIKE 'foo%' => min <= 'foo' && 'foo' <= max
    // column LIKE '%foo' => min <= '' && '' <= max => true
    // column LIKE '%foo%' => min <= '' && '' <= max => true
    // column LIKE 'foo' => min <= 'foo' && 'foo' <= max

    // TODO Handle ILIKE perhaps by making the min lowercase and max uppercase
    //  this may involve building the physical expressions that call lower() and upper()
    let min_column_expr = expr_builder.min_column_expr().ok()?;
    let max_column_expr = expr_builder.max_column_expr().ok()?;
    let scalar_expr = expr_builder.scalar_expr();
    // check that the scalar is a string literal
    let s = extract_string_literal(scalar_expr)?;
    // ANSI SQL specifies two wildcards: % and _. % matches zero or more characters, _ matches exactly one character.
    let first_wildcard_index = s.find(['%', '_']);
    if first_wildcard_index == Some(0) {
        // there's no filtering we could possibly do, return an error and have this be handled by the unhandled hook
        return None;
    }
    let (lower_bound, upper_bound) = if let Some(wildcard_index) = first_wildcard_index {
        let prefix = &s[..wildcard_index];
        let lower_bound_lit = Arc::new(phys_expr::Literal::new(ScalarValue::Utf8(Some(
            prefix.to_string(),
        ))));
        let upper_bound_lit = Arc::new(phys_expr::Literal::new(ScalarValue::Utf8(Some(
            increment_utf8(prefix)?,
        ))));
        (lower_bound_lit, upper_bound_lit)
    } else {
        // the like expression is a literal and can be converted into a comparison
        let bound = Arc::new(phys_expr::Literal::new(ScalarValue::Utf8(Some(
            s.to_string(),
        ))));
        (Arc::clone(&bound), bound)
    };
    let lower_bound_expr = Arc::new(phys_expr::BinaryExpr::new(
        lower_bound,
        Operator::LtEq,
        Arc::clone(&max_column_expr),
    ));
    let upper_bound_expr = Arc::new(phys_expr::BinaryExpr::new(
        Arc::clone(&min_column_expr),
        Operator::LtEq,
        upper_bound,
    ));
    let combined = Arc::new(phys_expr::BinaryExpr::new(
        upper_bound_expr,
        Operator::And,
        lower_bound_expr,
    ));
    Some(combined)
}

// For predicate `col NOT LIKE 'const_prefix%'`, we rewrite it as `(col_min NOT LIKE 'const_prefix%' OR col_max NOT LIKE 'const_prefix%')`.
//
// The intuition is that if both `col_min` and `col_max` begin with `const_prefix` that means
// **all** data in this row group begins with `const_prefix` as well (and therefore the predicate
// looking for rows that don't begin with `const_prefix` can never be true)
fn build_not_like_match(
    expr_builder: &mut PruningExpressionBuilder<'_>,
) -> Result<Arc<dyn PhysicalExpr>> {
    // col NOT LIKE 'const_prefix%' -> !(col_min LIKE 'const_prefix%' && col_max LIKE 'const_prefix%') -> (col_min NOT LIKE 'const_prefix%' || col_max NOT LIKE 'const_prefix%')

    let min_column_expr = expr_builder.min_column_expr()?;
    let max_column_expr = expr_builder.max_column_expr()?;

    let scalar_expr = expr_builder.scalar_expr();

    let pattern = extract_string_literal(scalar_expr).ok_or_else(|| {
        plan_datafusion_err!("cannot extract literal from NOT LIKE expression")
    })?;

    let (const_prefix, remaining) = split_constant_prefix(pattern);
    if const_prefix.is_empty() || remaining != "%" {
        // we can not handle `%` at the beginning or in the middle of the pattern
        // Example: For pattern "foo%bar", the row group might include values like
        // ["foobar", "food", "foodbar"], making it unsafe to prune.
        // Even if the min/max values in the group (e.g., "foobar" and "foodbar")
        // match the pattern, intermediate values like "food" may not
        // match the full pattern "foo%bar", making pruning unsafe.
        // (truncate foo%bar to foo% have same problem)

        // we can not handle pattern containing `_`
        // Example: For pattern "foo_", row groups might contain ["fooa", "fooaa", "foob"],
        // which means not every row is guaranteed to match the pattern.
        return Err(plan_datafusion_err!(
            "NOT LIKE expressions only support constant_prefix+wildcard`%`"
        ));
    }

    let min_col_not_like_epxr = Arc::new(phys_expr::LikeExpr::new(
        true,
        false,
        Arc::clone(&min_column_expr),
        Arc::clone(scalar_expr),
    ));

    let max_col_not_like_expr = Arc::new(phys_expr::LikeExpr::new(
        true,
        false,
        Arc::clone(&max_column_expr),
        Arc::clone(scalar_expr),
    ));

    Ok(Arc::new(phys_expr::BinaryExpr::new(
        min_col_not_like_epxr,
        Operator::Or,
        max_col_not_like_expr,
    )))
}

/// Returns unescaped constant prefix of a LIKE pattern (possibly empty) and the remaining pattern (possibly empty)
fn split_constant_prefix(pattern: &str) -> (&str, &str) {
    let char_indices = pattern.char_indices().collect::<Vec<_>>();
    for i in 0..char_indices.len() {
        let (idx, char) = char_indices[i];
        if char == '%' || char == '_' {
            if i != 0 && char_indices[i - 1].1 == '\\' {
                // ecsaped by `\`
                continue;
            }
            return (&pattern[..idx], &pattern[idx..]);
        }
    }
    (pattern, "")
}

/// Increment a UTF8 string by one, returning `None` if it can't be incremented.
/// This makes it so that the returned string will always compare greater than the input string
/// or any other string with the same prefix.
/// This is necessary since the statistics may have been truncated: if we have a min statistic
/// of "fo" that may have originally been "foz" or anything else with the prefix "fo".
/// E.g. `increment_utf8("foo") >= "foo"` and `increment_utf8("foo") >= "fooz"`
/// In this example `increment_utf8("foo") == "fop"
fn increment_utf8(data: &str) -> Option<String> {
    // Helper function to check if a character is valid to use
    fn is_valid_unicode(c: char) -> bool {
        let cp = c as u32;

        // Filter out non-characters (https://www.unicode.org/versions/corrigendum9.html)
        if [0xFFFE, 0xFFFF].contains(&cp) || (0xFDD0..=0xFDEF).contains(&cp) {
            return false;
        }

        // Filter out private use area
        if cp >= 0x110000 {
            return false;
        }

        true
    }

    // Convert string to vector of code points
    let mut code_points: Vec<char> = data.chars().collect();

    // Work backwards through code points
    for idx in (0..code_points.len()).rev() {
        let original = code_points[idx] as u32;

        // Try incrementing the code point
        if let Some(next_char) = char::from_u32(original + 1) {
            if is_valid_unicode(next_char) {
                code_points[idx] = next_char;
                // truncate the string to the current index
                code_points.truncate(idx + 1);
                return Some(code_points.into_iter().collect());
            }
        }
    }

    None
}

/// Wrap the statistics expression in a check that skips the expression if the column is all nulls.
///
/// This is important not only as an optimization but also because statistics may not be
/// accurate for columns that are all nulls.
/// For example, for an `int` column `x` with all nulls, the min/max/null_count statistics
/// might be set to 0 and evaluating `x = 0` would incorrectly include the column.
///
/// For example:
///
/// `x_min <= 10 AND 10 <= x_max`
///
/// will become
///
/// ```sql
/// x_null_count != x_row_count AND (x_min <= 10 AND 10 <= x_max)
/// ````
///
/// If the column is known to be all nulls, then the expression
/// `x_null_count = x_row_count` will be true, which will cause the
/// boolean expression to return false. Therefore, prune out the container.
fn wrap_null_count_check_expr(
    statistics_expr: Arc<dyn PhysicalExpr>,
    expr_builder: &mut PruningExpressionBuilder,
) -> Result<Arc<dyn PhysicalExpr>> {
    // x_null_count != x_row_count
    let not_when_null_count_eq_row_count = Arc::new(phys_expr::BinaryExpr::new(
        expr_builder.null_count_column_expr()?,
        Operator::NotEq,
        expr_builder.row_count_column_expr()?,
    ));

    // (x_null_count != x_row_count) AND (<statistics_expr>)
    Ok(Arc::new(phys_expr::BinaryExpr::new(
        not_when_null_count_eq_row_count,
        Operator::And,
        statistics_expr,
    )))
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) enum StatisticsType {
    Min,
    Max,
    NullCount,
    RowCount,
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::ops::{Not, Rem};

    use super::*;
    use datafusion_common::test_util::batches_to_string;
    use datafusion_expr::{and, col, lit, or};
    use insta::assert_snapshot;

    use arrow::array::Decimal128Array;
    use arrow::{
        array::{BinaryArray, Int32Array, Int64Array, StringArray, UInt64Array},
        datatypes::TimeUnit,
    };
    use datafusion_expr::expr::InList;
    use datafusion_expr::{cast, is_null, try_cast, Expr};
    use datafusion_functions_nested::expr_fn::{array_has, make_array};
    use datafusion_physical_expr::expressions as phys_expr;
    use datafusion_physical_expr::planner::logical2physical;

    #[derive(Debug, Default)]
    /// Mock statistic provider for tests
    ///
    /// Each row represents the statistics for a "container" (which
    /// might represent an entire parquet file, or directory of files,
    /// or some other collection of data for which we had statistics)
    ///
    /// Note All `ArrayRefs` must be the same size.
    struct ContainerStats {
        min: Option<ArrayRef>,
        max: Option<ArrayRef>,
        /// Optional values
        null_counts: Option<ArrayRef>,
        row_counts: Option<ArrayRef>,
        /// Optional known values (e.g. mimic a bloom filter)
        /// (value, contained)
        /// If present, all BooleanArrays must be the same size as min/max
        contained: Vec<(HashSet<ScalarValue>, BooleanArray)>,
    }

    impl ContainerStats {
        fn new() -> Self {
            Default::default()
        }
        fn new_decimal128(
            min: impl IntoIterator<Item = Option<i128>>,
            max: impl IntoIterator<Item = Option<i128>>,
            precision: u8,
            scale: i8,
        ) -> Self {
            Self::new()
                .with_min(Arc::new(
                    min.into_iter()
                        .collect::<Decimal128Array>()
                        .with_precision_and_scale(precision, scale)
                        .unwrap(),
                ))
                .with_max(Arc::new(
                    max.into_iter()
                        .collect::<Decimal128Array>()
                        .with_precision_and_scale(precision, scale)
                        .unwrap(),
                ))
        }

        fn new_i64(
            min: impl IntoIterator<Item = Option<i64>>,
            max: impl IntoIterator<Item = Option<i64>>,
        ) -> Self {
            Self::new()
                .with_min(Arc::new(min.into_iter().collect::<Int64Array>()))
                .with_max(Arc::new(max.into_iter().collect::<Int64Array>()))
        }

        fn new_i32(
            min: impl IntoIterator<Item = Option<i32>>,
            max: impl IntoIterator<Item = Option<i32>>,
        ) -> Self {
            Self::new()
                .with_min(Arc::new(min.into_iter().collect::<Int32Array>()))
                .with_max(Arc::new(max.into_iter().collect::<Int32Array>()))
        }

        fn new_utf8<'a>(
            min: impl IntoIterator<Item = Option<&'a str>>,
            max: impl IntoIterator<Item = Option<&'a str>>,
        ) -> Self {
            Self::new()
                .with_min(Arc::new(min.into_iter().collect::<StringArray>()))
                .with_max(Arc::new(max.into_iter().collect::<StringArray>()))
        }

        fn new_bool(
            min: impl IntoIterator<Item = Option<bool>>,
            max: impl IntoIterator<Item = Option<bool>>,
        ) -> Self {
            Self::new()
                .with_min(Arc::new(min.into_iter().collect::<BooleanArray>()))
                .with_max(Arc::new(max.into_iter().collect::<BooleanArray>()))
        }

        fn min(&self) -> Option<ArrayRef> {
            self.min.clone()
        }

        fn max(&self) -> Option<ArrayRef> {
            self.max.clone()
        }

        fn null_counts(&self) -> Option<ArrayRef> {
            self.null_counts.clone()
        }

        fn row_counts(&self) -> Option<ArrayRef> {
            self.row_counts.clone()
        }

        /// return an iterator over all arrays in this statistics
        fn arrays(&self) -> Vec<ArrayRef> {
            let contained_arrays = self
                .contained
                .iter()
                .map(|(_values, contained)| Arc::new(contained.clone()) as ArrayRef);

            [
                self.min.as_ref().cloned(),
                self.max.as_ref().cloned(),
                self.null_counts.as_ref().cloned(),
                self.row_counts.as_ref().cloned(),
            ]
            .into_iter()
            .flatten()
            .chain(contained_arrays)
            .collect()
        }

        /// Returns the number of containers represented by this statistics This
        /// picks the length of the first array as all arrays must have the same
        /// length (which is verified by `assert_invariants`).
        fn len(&self) -> usize {
            // pick the first non zero length
            self.arrays().iter().map(|a| a.len()).next().unwrap_or(0)
        }

        /// Ensure that the lengths of all arrays are consistent
        fn assert_invariants(&self) {
            let mut prev_len = None;

            for len in self.arrays().iter().map(|a| a.len()) {
                // Get a length, if we don't already have one
                match prev_len {
                    None => {
                        prev_len = Some(len);
                    }
                    Some(prev_len) => {
                        assert_eq!(prev_len, len);
                    }
                }
            }
        }

        /// Add min values
        fn with_min(mut self, min: ArrayRef) -> Self {
            self.min = Some(min);
            self
        }

        /// Add max values
        fn with_max(mut self, max: ArrayRef) -> Self {
            self.max = Some(max);
            self
        }

        /// Add null counts. There must be the same number of null counts as
        /// there are containers
        fn with_null_counts(
            mut self,
            counts: impl IntoIterator<Item = Option<u64>>,
        ) -> Self {
            let null_counts: ArrayRef =
                Arc::new(counts.into_iter().collect::<UInt64Array>());

            self.assert_invariants();
            self.null_counts = Some(null_counts);
            self
        }

        /// Add row counts. There must be the same number of row counts as
        /// there are containers
        fn with_row_counts(
            mut self,
            counts: impl IntoIterator<Item = Option<u64>>,
        ) -> Self {
            let row_counts: ArrayRef =
                Arc::new(counts.into_iter().collect::<UInt64Array>());

            self.assert_invariants();
            self.row_counts = Some(row_counts);
            self
        }

        /// Add contained information.
        pub fn with_contained(
            mut self,
            values: impl IntoIterator<Item = ScalarValue>,
            contained: impl IntoIterator<Item = Option<bool>>,
        ) -> Self {
            let contained: BooleanArray = contained.into_iter().collect();
            let values: HashSet<_> = values.into_iter().collect();

            self.contained.push((values, contained));
            self.assert_invariants();
            self
        }

        /// get any contained information for the specified values
        fn contained(&self, find_values: &HashSet<ScalarValue>) -> Option<BooleanArray> {
            // find the one with the matching values
            self.contained
                .iter()
                .find(|(values, _contained)| values == find_values)
                .map(|(_values, contained)| contained.clone())
        }
    }

    #[derive(Debug, Default)]
    struct TestStatistics {
        // key: column name
        stats: HashMap<Column, ContainerStats>,
    }

    impl TestStatistics {
        fn new() -> Self {
            Self::default()
        }

        fn with(
            mut self,
            name: impl Into<String>,
            container_stats: ContainerStats,
        ) -> Self {
            let col = Column::from_name(name.into());
            self.stats.insert(col, container_stats);
            self
        }

        /// Add null counts for the specified column.
        /// There must be the same number of null counts as
        /// there are containers
        fn with_null_counts(
            mut self,
            name: impl Into<String>,
            counts: impl IntoIterator<Item = Option<u64>>,
        ) -> Self {
            let col = Column::from_name(name.into());

            // take stats out and update them
            let container_stats = self
                .stats
                .remove(&col)
                .unwrap_or_default()
                .with_null_counts(counts);

            // put stats back in
            self.stats.insert(col, container_stats);
            self
        }

        /// Add row counts for the specified column.
        /// There must be the same number of row counts as
        /// there are containers
        fn with_row_counts(
            mut self,
            name: impl Into<String>,
            counts: impl IntoIterator<Item = Option<u64>>,
        ) -> Self {
            let col = Column::from_name(name.into());

            // take stats out and update them
            let container_stats = self
                .stats
                .remove(&col)
                .unwrap_or_default()
                .with_row_counts(counts);

            // put stats back in
            self.stats.insert(col, container_stats);
            self
        }

        /// Add contained information for the specified column.
        fn with_contained(
            mut self,
            name: impl Into<String>,
            values: impl IntoIterator<Item = ScalarValue>,
            contained: impl IntoIterator<Item = Option<bool>>,
        ) -> Self {
            let col = Column::from_name(name.into());

            // take stats out and update them
            let container_stats = self
                .stats
                .remove(&col)
                .unwrap_or_default()
                .with_contained(values, contained);

            // put stats back in
            self.stats.insert(col, container_stats);
            self
        }
    }

    impl PruningStatistics for TestStatistics {
        fn min_values(&self, column: &Column) -> Option<ArrayRef> {
            self.stats
                .get(column)
                .map(|container_stats| container_stats.min())
                .unwrap_or(None)
        }

        fn max_values(&self, column: &Column) -> Option<ArrayRef> {
            self.stats
                .get(column)
                .map(|container_stats| container_stats.max())
                .unwrap_or(None)
        }

        fn num_containers(&self) -> usize {
            self.stats
                .values()
                .next()
                .map(|container_stats| container_stats.len())
                .unwrap_or(0)
        }

        fn null_counts(&self, column: &Column) -> Option<ArrayRef> {
            self.stats
                .get(column)
                .map(|container_stats| container_stats.null_counts())
                .unwrap_or(None)
        }

        fn row_counts(&self, column: &Column) -> Option<ArrayRef> {
            self.stats
                .get(column)
                .map(|container_stats| container_stats.row_counts())
                .unwrap_or(None)
        }

        fn contained(
            &self,
            column: &Column,
            values: &HashSet<ScalarValue>,
        ) -> Option<BooleanArray> {
            self.stats
                .get(column)
                .and_then(|container_stats| container_stats.contained(values))
        }
    }

    /// Returns the specified min/max container values
    struct OneContainerStats {
        min_values: Option<ArrayRef>,
        max_values: Option<ArrayRef>,
        num_containers: usize,
    }

    impl PruningStatistics for OneContainerStats {
        fn min_values(&self, _column: &Column) -> Option<ArrayRef> {
            self.min_values.clone()
        }

        fn max_values(&self, _column: &Column) -> Option<ArrayRef> {
            self.max_values.clone()
        }

        fn num_containers(&self) -> usize {
            self.num_containers
        }

        fn null_counts(&self, _column: &Column) -> Option<ArrayRef> {
            None
        }

        fn row_counts(&self, _column: &Column) -> Option<ArrayRef> {
            None
        }

        fn contained(
            &self,
            _column: &Column,
            _values: &HashSet<ScalarValue>,
        ) -> Option<BooleanArray> {
            None
        }
    }

    /// Row count should only be referenced once in the pruning expression, even if we need the row count
    /// for multiple columns.
    #[test]
    fn test_unique_row_count_field_and_column() {
        // c1 = 100 AND c2 = 200
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("c1", DataType::Int32, true),
            Field::new("c2", DataType::Int32, true),
        ]));
        let expr = col("c1").eq(lit(100)).and(col("c2").eq(lit(200)));
        let expr = logical2physical(&expr, &schema);
        let p = PruningPredicate::try_new(expr, Arc::clone(&schema)).unwrap();
        // note pruning expression refers to row_count twice
        assert_eq!(
            "c1_null_count@2 != row_count@3 AND c1_min@0 <= 100 AND 100 <= c1_max@1 AND c2_null_count@6 != row_count@3 AND c2_min@4 <= 200 AND 200 <= c2_max@5",
            p.predicate_expr.to_string()
        );

        // Fields in required schema should be unique, otherwise when creating batches
        // it will fail because of duplicate field names
        let mut fields = HashSet::new();
        for (_col, _ty, field) in p.required_columns().iter() {
            let was_new = fields.insert(field);
            if !was_new {
                panic!(
                    "Duplicate field in required schema: {field:?}. Previous fields:\n{fields:#?}"
                );
            }
        }
    }

    #[test]
    fn prune_all_rows_null_counts() {
        // if null_count = row_count then we should prune the container for i = 0
        // regardless of the statistics
        let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int32, true)]));
        let statistics = TestStatistics::new().with(
            "i",
            ContainerStats::new_i32(
                vec![Some(0)], // min
                vec![Some(0)], // max
            )
            .with_null_counts(vec![Some(1)])
            .with_row_counts(vec![Some(1)]),
        );
        let expected_ret = &[false];
        prune_with_expr(col("i").eq(lit(0)), &schema, &statistics, expected_ret);

        // this should be true even if the container stats are missing
        let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int32, true)]));
        let container_stats = ContainerStats {
            min: Some(Arc::new(Int32Array::from(vec![None]))),
            max: Some(Arc::new(Int32Array::from(vec![None]))),
            null_counts: Some(Arc::new(UInt64Array::from(vec![Some(1)]))),
            row_counts: Some(Arc::new(UInt64Array::from(vec![Some(1)]))),
            ..ContainerStats::default()
        };
        let statistics = TestStatistics::new().with("i", container_stats);
        let expected_ret = &[false];
        prune_with_expr(col("i").eq(lit(0)), &schema, &statistics, expected_ret);

        // If the null counts themselves are missing we should be able to fall back to the stats
        let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int32, true)]));
        let container_stats = ContainerStats {
            min: Some(Arc::new(Int32Array::from(vec![Some(0)]))),
            max: Some(Arc::new(Int32Array::from(vec![Some(0)]))),
            null_counts: Some(Arc::new(UInt64Array::from(vec![None]))),
            row_counts: Some(Arc::new(UInt64Array::from(vec![Some(1)]))),
            ..ContainerStats::default()
        };
        let statistics = TestStatistics::new().with("i", container_stats);
        let expected_ret = &[true];
        prune_with_expr(col("i").eq(lit(0)), &schema, &statistics, expected_ret);
        let expected_ret = &[false];
        prune_with_expr(col("i").gt(lit(0)), &schema, &statistics, expected_ret);

        // Same for the row counts
        let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int32, true)]));
        let container_stats = ContainerStats {
            min: Some(Arc::new(Int32Array::from(vec![Some(0)]))),
            max: Some(Arc::new(Int32Array::from(vec![Some(0)]))),
            null_counts: Some(Arc::new(UInt64Array::from(vec![Some(1)]))),
            row_counts: Some(Arc::new(UInt64Array::from(vec![None]))),
            ..ContainerStats::default()
        };
        let statistics = TestStatistics::new().with("i", container_stats);
        let expected_ret = &[true];
        prune_with_expr(col("i").eq(lit(0)), &schema, &statistics, expected_ret);
        let expected_ret = &[false];
        prune_with_expr(col("i").gt(lit(0)), &schema, &statistics, expected_ret);
    }

    #[test]
    fn prune_missing_statistics() {
        // If the min or max stats are missing we should not prune
        // (unless we know all rows are null, see `prune_all_rows_null_counts`)
        let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int32, true)]));
        let container_stats = ContainerStats {
            min: Some(Arc::new(Int32Array::from(vec![None, Some(0)]))),
            max: Some(Arc::new(Int32Array::from(vec![Some(0), None]))),
            null_counts: Some(Arc::new(UInt64Array::from(vec![Some(0), Some(0)]))),
            row_counts: Some(Arc::new(UInt64Array::from(vec![Some(1), Some(1)]))),
            ..ContainerStats::default()
        };
        let statistics = TestStatistics::new().with("i", container_stats);
        let expected_ret = &[true, true];
        prune_with_expr(col("i").eq(lit(0)), &schema, &statistics, expected_ret);
        let expected_ret = &[false, true];
        prune_with_expr(col("i").gt(lit(0)), &schema, &statistics, expected_ret);
        let expected_ret = &[true, false];
        prune_with_expr(col("i").lt(lit(0)), &schema, &statistics, expected_ret);
    }

    #[test]
    fn prune_null_stats() {
        // if null_count = row_count then we should prune the container for i = 0
        // regardless of the statistics
        let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int32, true)]));

        let statistics = TestStatistics::new().with(
            "i",
            ContainerStats::new_i32(
                vec![Some(0)], // min
                vec![Some(0)], // max
            )
            .with_null_counts(vec![Some(1)])
            .with_row_counts(vec![Some(1)]),
        );

        let expected_ret = &[false];

        // i = 0
        prune_with_expr(col("i").eq(lit(0)), &schema, &statistics, expected_ret);
    }

    #[test]
    fn test_build_statistics_record_batch() {
        // Request a record batch with of s1_min, s2_max, s3_max, s3_min
        let required_columns = RequiredColumns::from(vec![
            // min of original column s1, named s1_min
            (
                phys_expr::Column::new("s1", 1),
                StatisticsType::Min,
                Field::new("s1_min", DataType::Int32, true),
            ),
            // max of original column s2, named s2_max
            (
                phys_expr::Column::new("s2", 2),
                StatisticsType::Max,
                Field::new("s2_max", DataType::Int32, true),
            ),
            // max of original column s3, named s3_max
            (
                phys_expr::Column::new("s3", 3),
                StatisticsType::Max,
                Field::new("s3_max", DataType::Utf8, true),
            ),
            // min of original column s3, named s3_min
            (
                phys_expr::Column::new("s3", 3),
                StatisticsType::Min,
                Field::new("s3_min", DataType::Utf8, true),
            ),
        ]);

        let statistics = TestStatistics::new()
            .with(
                "s1",
                ContainerStats::new_i32(
                    vec![None, None, Some(9), None],  // min
                    vec![Some(10), None, None, None], // max
                ),
            )
            .with(
                "s2",
                ContainerStats::new_i32(
                    vec![Some(2), None, None, None],  // min
                    vec![Some(20), None, None, None], // max
                ),
            )
            .with(
                "s3",
                ContainerStats::new_utf8(
                    vec![Some("a"), None, None, None],      // min
                    vec![Some("q"), None, Some("r"), None], // max
                ),
            );

        let batch =
            build_statistics_record_batch(&statistics, &required_columns).unwrap();
        assert_snapshot!(batches_to_string(&[batch]), @r"
        +--------+--------+--------+--------+
        | s1_min | s2_max | s3_max | s3_min |
        +--------+--------+--------+--------+
        |        | 20     | q      | a      |
        |        |        |        |        |
        | 9      |        | r      |        |
        |        |        |        |        |
        +--------+--------+--------+--------+
        ");
    }

    #[test]
    fn test_build_statistics_casting() {
        // Test requesting a Timestamp column, but getting statistics as Int64
        // which is what Parquet does

        // Request a record batch with of s1_min as a timestamp
        let required_columns = RequiredColumns::from(vec![(
            phys_expr::Column::new("s3", 3),
            StatisticsType::Min,
            Field::new(
                "s1_min",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
        )]);

        // Note the statistics pass back i64 (not timestamp)
        let statistics = OneContainerStats {
            min_values: Some(Arc::new(Int64Array::from(vec![Some(10)]))),
            max_values: Some(Arc::new(Int64Array::from(vec![Some(20)]))),
            num_containers: 1,
        };

        let batch =
            build_statistics_record_batch(&statistics, &required_columns).unwrap();

        assert_snapshot!(batches_to_string(&[batch]), @r"
        +-------------------------------+
        | s1_min                        |
        +-------------------------------+
        | 1970-01-01T00:00:00.000000010 |
        +-------------------------------+
        ");
    }

    #[test]
    fn test_build_statistics_no_required_stats() {
        let required_columns = RequiredColumns::new();

        let statistics = OneContainerStats {
            min_values: Some(Arc::new(Int64Array::from(vec![Some(10)]))),
            max_values: Some(Arc::new(Int64Array::from(vec![Some(20)]))),
            num_containers: 1,
        };

        let batch =
            build_statistics_record_batch(&statistics, &required_columns).unwrap();
        assert_eq!(batch.num_rows(), 1); // had 1 container
    }

    #[test]
    fn test_build_statistics_inconsistent_types() {
        // Test requesting a Utf8 column when the stats return some other type

        // Request a record batch with of s1_min as a timestamp
        let required_columns = RequiredColumns::from(vec![(
            phys_expr::Column::new("s3", 3),
            StatisticsType::Min,
            Field::new("s1_min", DataType::Utf8, true),
        )]);

        // Note the statistics return an invalid UTF-8 sequence which will be converted to null
        let statistics = OneContainerStats {
            min_values: Some(Arc::new(BinaryArray::from(vec![&[255u8] as &[u8]]))),
            max_values: None,
            num_containers: 1,
        };

        let batch =
            build_statistics_record_batch(&statistics, &required_columns).unwrap();
        assert_snapshot!(batches_to_string(&[batch]), @r"
        +--------+
        | s1_min |
        +--------+
        |        |
        +--------+
        ");
    }

    #[test]
    fn test_build_statistics_inconsistent_length() {
        // return an inconsistent length to the actual statistics arrays
        let required_columns = RequiredColumns::from(vec![(
            phys_expr::Column::new("s1", 3),
            StatisticsType::Min,
            Field::new("s1_min", DataType::Int64, true),
        )]);

        // Note the statistics pass back i64 (not timestamp)
        let statistics = OneContainerStats {
            min_values: Some(Arc::new(Int64Array::from(vec![Some(10)]))),
            max_values: Some(Arc::new(Int64Array::from(vec![Some(20)]))),
            num_containers: 3,
        };

        let result =
            build_statistics_record_batch(&statistics, &required_columns).unwrap_err();
        assert!(
            result
                .to_string()
                .contains("mismatched statistics length. Expected 3, got 1"),
            "{}",
            result
        );
    }

    #[test]
    fn row_group_predicate_eq() -> Result<()> {
        let schema = Schema::new(vec![Field::new("c1", DataType::Int32, false)]);
        let expected_expr =
            "c1_null_count@2 != row_count@3 AND c1_min@0 <= 1 AND 1 <= c1_max@1";

        // test column on the left
        let expr = col("c1").eq(lit(1));
        let predicate_expr =
            test_build_predicate_expression(&expr, &schema, &mut RequiredColumns::new());
        assert_eq!(predicate_expr.to_string(), expected_expr);

        // test column on the right
        let expr = lit(1).eq(col("c1"));
        let predicate_expr =
            test_build_predicate_expression(&expr, &schema, &mut RequiredColumns::new());
        assert_eq!(predicate_expr.to_string(), expected_expr);

        Ok(())
    }

    #[test]
    fn row_group_predicate_not_eq() -> Result<()> {
        let schema = Schema::new(vec![Field::new("c1", DataType::Int32, false)]);
        let expected_expr =
            "c1_null_count@2 != row_count@3 AND (c1_min@0 != 1 OR 1 != c1_max@1)";

        // test column on the left
        let expr = col("c1").not_eq(lit(1));
        let predicate_expr =
            test_build_predicate_expression(&expr, &schema, &mut RequiredColumns::new());
        assert_eq!(predicate_expr.to_string(), expected_expr);

        // test column on the right
        let expr = lit(1).not_eq(col("c1"));
        let predicate_expr =
            test_build_predicate_expression(&expr, &schema, &mut RequiredColumns::new());
        assert_eq!(predicate_expr.to_string(), expected_expr);

        Ok(())
    }

    #[test]
    fn row_group_predicate_gt() -> Result<()> {
        let schema = Schema::new(vec![Field::new("c1", DataType::Int32, false)]);
        let expected_expr = "c1_null_count@1 != row_count@2 AND c1_max@0 > 1";

        // test column on the left
        let expr = col("c1").gt(lit(1));
        let predicate_expr =
            test_build_predicate_expression(&expr, &schema, &mut RequiredColumns::new());
        assert_eq!(predicate_expr.to_string(), expected_expr);

        // test column on the right
        let expr = lit(1).lt(col("c1"));
        let predicate_expr =
            test_build_predicate_expression(&expr, &schema, &mut RequiredColumns::new());
        assert_eq!(predicate_expr.to_string(), expected_expr);

        Ok(())
    }

    #[test]
    fn row_group_predicate_gt_eq() -> Result<()> {
        let schema = Schema::new(vec![Field::new("c1", DataType::Int32, false)]);
        let expected_expr = "c1_null_count@1 != row_count@2 AND c1_max@0 >= 1";

        // test column on the left
        let expr = col("c1").gt_eq(lit(1));
        let predicate_expr =
            test_build_predicate_expression(&expr, &schema, &mut RequiredColumns::new());
        assert_eq!(predicate_expr.to_string(), expected_expr);
        // test column on the right
        let expr = lit(1).lt_eq(col("c1"));
        let predicate_expr =
            test_build_predicate_expression(&expr, &schema, &mut RequiredColumns::new());
        assert_eq!(predicate_expr.to_string(), expected_expr);

        Ok(())
    }

    #[test]
    fn row_group_predicate_lt() -> Result<()> {
        let schema = Schema::new(vec![Field::new("c1", DataType::Int32, false)]);
        let expected_expr = "c1_null_count@1 != row_count@2 AND c1_min@0 < 1";

        // test column on the left
        let expr = col("c1").lt(lit(1));
        let predicate_expr =
            test_build_predicate_expression(&expr, &schema, &mut RequiredColumns::new());
        assert_eq!(predicate_expr.to_string(), expected_expr);

        // test column on the right
        let expr = lit(1).gt(col("c1"));
        let predicate_expr =
            test_build_predicate_expression(&expr, &schema, &mut RequiredColumns::new());
        assert_eq!(predicate_expr.to_string(), expected_expr);

        Ok(())
    }

    #[test]
    fn row_group_predicate_lt_eq() -> Result<()> {
        let schema = Schema::new(vec![Field::new("c1", DataType::Int32, false)]);
        let expected_expr = "c1_null_count@1 != row_count@2 AND c1_min@0 <= 1";

        // test column on the left
        let expr = col("c1").lt_eq(lit(1));
        let predicate_expr =
            test_build_predicate_expression(&expr, &schema, &mut RequiredColumns::new());
        assert_eq!(predicate_expr.to_string(), expected_expr);
        // test column on the right
        let expr = lit(1).gt_eq(col("c1"));
        let predicate_expr =
            test_build_predicate_expression(&expr, &schema, &mut RequiredColumns::new());
        assert_eq!(predicate_expr.to_string(), expected_expr);

        Ok(())
    }

    #[test]
    fn row_group_predicate_and() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("c1", DataType::Int32, false),
            Field::new("c2", DataType::Int32, false),
            Field::new("c3", DataType::Int32, false),
        ]);
        // test AND operator joining supported c1 < 1 expression and unsupported c2 > c3 expression
        let expr = col("c1").lt(lit(1)).and(col("c2").lt(col("c3")));
        let expected_expr = "c1_null_count@1 != row_count@2 AND c1_min@0 < 1";
        let predicate_expr =
            test_build_predicate_expression(&expr, &schema, &mut RequiredColumns::new());
        assert_eq!(predicate_expr.to_string(), expected_expr);

        Ok(())
    }

    #[test]
    fn row_group_predicate_or() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("c1", DataType::Int32, false),
            Field::new("c2", DataType::Int32, false),
        ]);
        // test OR operator joining supported c1 < 1 expression and unsupported c2 % 2 = 0 expression
        let expr = col("c1").lt(lit(1)).or(col("c2").rem(lit(2)).eq(lit(0)));
        let expected_expr = "true";
        let predicate_expr =
            test_build_predicate_expression(&expr, &schema, &mut RequiredColumns::new());
        assert_eq!(predicate_expr.to_string(), expected_expr);

        Ok(())
    }

    #[test]
    fn row_group_predicate_not() -> Result<()> {
        let schema = Schema::new(vec![Field::new("c1", DataType::Int32, false)]);
        let expected_expr = "true";

        let expr = col("c1").not();
        let predicate_expr =
            test_build_predicate_expression(&expr, &schema, &mut RequiredColumns::new());
        assert_eq!(predicate_expr.to_string(), expected_expr);

        Ok(())
    }

    #[test]
    fn row_group_predicate_not_bool() -> Result<()> {
        let schema = Schema::new(vec![Field::new("c1", DataType::Boolean, false)]);
        let expected_expr = "NOT c1_min@0 AND c1_max@1";

        let expr = col("c1").not();
        let predicate_expr =
            test_build_predicate_expression(&expr, &schema, &mut RequiredColumns::new());
        assert_eq!(predicate_expr.to_string(), expected_expr);

        Ok(())
    }

    #[test]
    fn row_group_predicate_bool() -> Result<()> {
        let schema = Schema::new(vec![Field::new("c1", DataType::Boolean, false)]);
        let expected_expr = "c1_min@0 OR c1_max@1";

        let expr = col("c1");
        let predicate_expr =
            test_build_predicate_expression(&expr, &schema, &mut RequiredColumns::new());
        assert_eq!(predicate_expr.to_string(), expected_expr);

        Ok(())
    }

    #[test]
    fn row_group_predicate_lt_bool() -> Result<()> {
        let schema = Schema::new(vec![Field::new("c1", DataType::Boolean, false)]);
        let expected_expr = "c1_null_count@1 != row_count@2 AND c1_min@0 < true";

        // DF doesn't support arithmetic on boolean columns so
        // this predicate will error when evaluated
        let expr = col("c1").lt(lit(true));
        let predicate_expr =
            test_build_predicate_expression(&expr, &schema, &mut RequiredColumns::new());
        assert_eq!(predicate_expr.to_string(), expected_expr);

        Ok(())
    }

    #[test]
    fn row_group_predicate_required_columns() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("c1", DataType::Int32, false),
            Field::new("c2", DataType::Int32, false),
        ]);
        let mut required_columns = RequiredColumns::new();
        // c1 < 1 and (c2 = 2 or c2 = 3)
        let expr = col("c1")
            .lt(lit(1))
            .and(col("c2").eq(lit(2)).or(col("c2").eq(lit(3))));
        let expected_expr = "c1_null_count@1 != row_count@2 AND c1_min@0 < 1 AND (c2_null_count@5 != row_count@2 AND c2_min@3 <= 2 AND 2 <= c2_max@4 OR c2_null_count@5 != row_count@2 AND c2_min@3 <= 3 AND 3 <= c2_max@4)";
        let predicate_expr =
            test_build_predicate_expression(&expr, &schema, &mut required_columns);
        assert_eq!(predicate_expr.to_string(), expected_expr);
        println!("required_columns: {required_columns:#?}"); // for debugging assertions below
                                                             // c1 < 1 should add c1_min
        let c1_min_field = Field::new("c1_min", DataType::Int32, false);
        assert_eq!(
            required_columns.columns[0],
            (
                phys_expr::Column::new("c1", 0),
                StatisticsType::Min,
                c1_min_field.with_nullable(true) // could be nullable if stats are not present
            )
        );
        // c1 < 1 should add c1_null_count
        let c1_null_count_field = Field::new("c1_null_count", DataType::UInt64, false);
        assert_eq!(
            required_columns.columns[1],
            (
                phys_expr::Column::new("c1", 0),
                StatisticsType::NullCount,
                c1_null_count_field.with_nullable(true) // could be nullable if stats are not present
            )
        );
        // c1 < 1 should add row_count
        let row_count_field = Field::new("row_count", DataType::UInt64, false);
        assert_eq!(
            required_columns.columns[2],
            (
                phys_expr::Column::new("c1", 0),
                StatisticsType::RowCount,
                row_count_field.with_nullable(true) // could be nullable if stats are not present
            )
        );
        // c2 = 2 should add c2_min and c2_max
        let c2_min_field = Field::new("c2_min", DataType::Int32, false);
        assert_eq!(
            required_columns.columns[3],
            (
                phys_expr::Column::new("c2", 1),
                StatisticsType::Min,
                c2_min_field.with_nullable(true) // could be nullable if stats are not present
            )
        );
        let c2_max_field = Field::new("c2_max", DataType::Int32, false);
        assert_eq!(
            required_columns.columns[4],
            (
                phys_expr::Column::new("c2", 1),
                StatisticsType::Max,
                c2_max_field.with_nullable(true) // could be nullable if stats are not present
            )
        );
        // c2 = 2 should add c2_null_count
        let c2_null_count_field = Field::new("c2_null_count", DataType::UInt64, false);
        assert_eq!(
            required_columns.columns[5],
            (
                phys_expr::Column::new("c2", 1),
                StatisticsType::NullCount,
                c2_null_count_field.with_nullable(true) // could be nullable if stats are not present
            )
        );
        // c2 = 1 should add row_count
        let row_count_field = Field::new("row_count", DataType::UInt64, false);
        assert_eq!(
            required_columns.columns[2],
            (
                phys_expr::Column::new("c1", 0),
                StatisticsType::RowCount,
                row_count_field.with_nullable(true) // could be nullable if stats are not present
            )
        );
        // c2 = 3 shouldn't add any new statistics fields
        assert_eq!(required_columns.columns.len(), 6);

        Ok(())
    }

    #[test]
    fn row_group_predicate_in_list() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("c1", DataType::Int32, false),
            Field::new("c2", DataType::Int32, false),
        ]);
        // test c1 in(1, 2, 3)
        let expr = Expr::InList(InList::new(
            Box::new(col("c1")),
            vec![lit(1), lit(2), lit(3)],
            false,
        ));
        let expected_expr = "c1_null_count@2 != row_count@3 AND c1_min@0 <= 1 AND 1 <= c1_max@1 OR c1_null_count@2 != row_count@3 AND c1_min@0 <= 2 AND 2 <= c1_max@1 OR c1_null_count@2 != row_count@3 AND c1_min@0 <= 3 AND 3 <= c1_max@1";
        let predicate_expr =
            test_build_predicate_expression(&expr, &schema, &mut RequiredColumns::new());
        assert_eq!(predicate_expr.to_string(), expected_expr);

        Ok(())
    }

    #[test]
    fn row_group_predicate_in_list_empty() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("c1", DataType::Int32, false),
            Field::new("c2", DataType::Int32, false),
        ]);
        // test c1 in()
        let expr = Expr::InList(InList::new(Box::new(col("c1")), vec![], false));
        let expected_expr = "true";
        let predicate_expr =
            test_build_predicate_expression(&expr, &schema, &mut RequiredColumns::new());
        assert_eq!(predicate_expr.to_string(), expected_expr);

        Ok(())
    }

    #[test]
    fn row_group_predicate_in_list_negated() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("c1", DataType::Int32, false),
            Field::new("c2", DataType::Int32, false),
        ]);
        // test c1 not in(1, 2, 3)
        let expr = Expr::InList(InList::new(
            Box::new(col("c1")),
            vec![lit(1), lit(2), lit(3)],
            true,
        ));
        let expected_expr = "c1_null_count@2 != row_count@3 AND (c1_min@0 != 1 OR 1 != c1_max@1) AND c1_null_count@2 != row_count@3 AND (c1_min@0 != 2 OR 2 != c1_max@1) AND c1_null_count@2 != row_count@3 AND (c1_min@0 != 3 OR 3 != c1_max@1)";
        let predicate_expr =
            test_build_predicate_expression(&expr, &schema, &mut RequiredColumns::new());
        assert_eq!(predicate_expr.to_string(), expected_expr);

        Ok(())
    }

    #[test]
    fn row_group_predicate_between() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("c1", DataType::Int32, false),
            Field::new("c2", DataType::Int32, false),
        ]);

        // test c1 BETWEEN 1 AND 5
        let expr1 = col("c1").between(lit(1), lit(5));

        // test 1 <= c1 <= 5
        let expr2 = col("c1").gt_eq(lit(1)).and(col("c1").lt_eq(lit(5)));

        let predicate_expr1 =
            test_build_predicate_expression(&expr1, &schema, &mut RequiredColumns::new());

        let predicate_expr2 =
            test_build_predicate_expression(&expr2, &schema, &mut RequiredColumns::new());
        assert_eq!(predicate_expr1.to_string(), predicate_expr2.to_string());

        Ok(())
    }

    #[test]
    fn row_group_predicate_between_with_in_list() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("c1", DataType::Int32, false),
            Field::new("c2", DataType::Int32, false),
        ]);
        // test c1 in(1, 2)
        let expr1 = col("c1").in_list(vec![lit(1), lit(2)], false);

        // test c2 BETWEEN 4 AND 5
        let expr2 = col("c2").between(lit(4), lit(5));

        // test c1 in(1, 2) and c2 BETWEEN 4 AND 5
        let expr3 = expr1.and(expr2);

        let expected_expr = "(c1_null_count@2 != row_count@3 AND c1_min@0 <= 1 AND 1 <= c1_max@1 OR c1_null_count@2 != row_count@3 AND c1_min@0 <= 2 AND 2 <= c1_max@1) AND c2_null_count@5 != row_count@3 AND c2_max@4 >= 4 AND c2_null_count@5 != row_count@3 AND c2_min@6 <= 5";
        let predicate_expr =
            test_build_predicate_expression(&expr3, &schema, &mut RequiredColumns::new());
        assert_eq!(predicate_expr.to_string(), expected_expr);

        Ok(())
    }

    #[test]
    fn row_group_predicate_in_list_to_many_values() -> Result<()> {
        let schema = Schema::new(vec![Field::new("c1", DataType::Int32, false)]);
        // test c1 in(1..21)
        // in pruning.rs has MAX_LIST_VALUE_SIZE_REWRITE = 20, more than this value will be rewrite
        // always true
        let expr = col("c1").in_list((1..=21).map(lit).collect(), false);

        let expected_expr = "true";
        let predicate_expr =
            test_build_predicate_expression(&expr, &schema, &mut RequiredColumns::new());
        assert_eq!(predicate_expr.to_string(), expected_expr);

        Ok(())
    }

    #[test]
    fn row_group_predicate_cast_int_int() -> Result<()> {
        let schema = Schema::new(vec![Field::new("c1", DataType::Int32, false)]);
        let expected_expr = "c1_null_count@2 != row_count@3 AND CAST(c1_min@0 AS Int64) <= 1 AND 1 <= CAST(c1_max@1 AS Int64)";

        // test cast(c1 as int64) = 1
        // test column on the left
        let expr = cast(col("c1"), DataType::Int64).eq(lit(ScalarValue::Int64(Some(1))));
        let predicate_expr =
            test_build_predicate_expression(&expr, &schema, &mut RequiredColumns::new());
        assert_eq!(predicate_expr.to_string(), expected_expr);

        // test column on the right
        let expr = lit(ScalarValue::Int64(Some(1))).eq(cast(col("c1"), DataType::Int64));
        let predicate_expr =
            test_build_predicate_expression(&expr, &schema, &mut RequiredColumns::new());
        assert_eq!(predicate_expr.to_string(), expected_expr);

        let expected_expr =
            "c1_null_count@1 != row_count@2 AND TRY_CAST(c1_max@0 AS Int64) > 1";

        // test column on the left
        let expr =
            try_cast(col("c1"), DataType::Int64).gt(lit(ScalarValue::Int64(Some(1))));
        let predicate_expr =
            test_build_predicate_expression(&expr, &schema, &mut RequiredColumns::new());
        assert_eq!(predicate_expr.to_string(), expected_expr);

        // test column on the right
        let expr =
            lit(ScalarValue::Int64(Some(1))).lt(try_cast(col("c1"), DataType::Int64));
        let predicate_expr =
            test_build_predicate_expression(&expr, &schema, &mut RequiredColumns::new());
        assert_eq!(predicate_expr.to_string(), expected_expr);

        Ok(())
    }

    #[test]
    fn row_group_predicate_cast_string_string() -> Result<()> {
        let schema = Schema::new(vec![Field::new("c1", DataType::Utf8View, false)]);
        let expected_expr = "c1_null_count@2 != row_count@3 AND CAST(c1_min@0 AS Utf8) <= 1 AND 1 <= CAST(c1_max@1 AS Utf8)";

        // test column on the left
        let expr = cast(col("c1"), DataType::Utf8)
            .eq(lit(ScalarValue::Utf8(Some("1".to_string()))));
        let predicate_expr =
            test_build_predicate_expression(&expr, &schema, &mut RequiredColumns::new());
        assert_eq!(predicate_expr.to_string(), expected_expr);

        // test column on the right
        let expr = lit(ScalarValue::Utf8(Some("1".to_string())))
            .eq(cast(col("c1"), DataType::Utf8));
        let predicate_expr =
            test_build_predicate_expression(&expr, &schema, &mut RequiredColumns::new());
        assert_eq!(predicate_expr.to_string(), expected_expr);

        Ok(())
    }

    #[test]
    fn row_group_predicate_cast_string_int() -> Result<()> {
        let schema = Schema::new(vec![Field::new("c1", DataType::Utf8View, false)]);
        let expected_expr = "true";

        // test column on the left
        let expr = cast(col("c1"), DataType::Int32).eq(lit(ScalarValue::Int32(Some(1))));
        let predicate_expr =
            test_build_predicate_expression(&expr, &schema, &mut RequiredColumns::new());
        assert_eq!(predicate_expr.to_string(), expected_expr);

        // test column on the right
        let expr = lit(ScalarValue::Int32(Some(1))).eq(cast(col("c1"), DataType::Int32));
        let predicate_expr =
            test_build_predicate_expression(&expr, &schema, &mut RequiredColumns::new());
        assert_eq!(predicate_expr.to_string(), expected_expr);

        Ok(())
    }

    #[test]
    fn row_group_predicate_cast_int_string() -> Result<()> {
        let schema = Schema::new(vec![Field::new("c1", DataType::Int32, false)]);
        let expected_expr = "true";

        // test column on the left
        let expr = cast(col("c1"), DataType::Utf8)
            .eq(lit(ScalarValue::Utf8(Some("1".to_string()))));
        let predicate_expr =
            test_build_predicate_expression(&expr, &schema, &mut RequiredColumns::new());
        assert_eq!(predicate_expr.to_string(), expected_expr);

        // test column on the right
        let expr = lit(ScalarValue::Utf8(Some("1".to_string())))
            .eq(cast(col("c1"), DataType::Utf8));
        let predicate_expr =
            test_build_predicate_expression(&expr, &schema, &mut RequiredColumns::new());
        assert_eq!(predicate_expr.to_string(), expected_expr);

        Ok(())
    }

    #[test]
    fn row_group_predicate_date_date() -> Result<()> {
        let schema = Schema::new(vec![Field::new("c1", DataType::Date32, false)]);
        let expected_expr = "c1_null_count@2 != row_count@3 AND CAST(c1_min@0 AS Date64) <= 1970-01-01 AND 1970-01-01 <= CAST(c1_max@1 AS Date64)";

        // test column on the left
        let expr =
            cast(col("c1"), DataType::Date64).eq(lit(ScalarValue::Date64(Some(123))));
        let predicate_expr =
            test_build_predicate_expression(&expr, &schema, &mut RequiredColumns::new());
        assert_eq!(predicate_expr.to_string(), expected_expr);

        // test column on the right
        let expr =
            lit(ScalarValue::Date64(Some(123))).eq(cast(col("c1"), DataType::Date64));
        let predicate_expr =
            test_build_predicate_expression(&expr, &schema, &mut RequiredColumns::new());
        assert_eq!(predicate_expr.to_string(), expected_expr);

        Ok(())
    }

    #[test]
    fn row_group_predicate_dict_string_date() -> Result<()> {
        // Test with Dictionary<UInt8, Utf8> for the literal
        let schema = Schema::new(vec![Field::new("c1", DataType::Date32, false)]);
        let expected_expr = "true";

        // test column on the left
        let expr = cast(
            col("c1"),
            DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::Utf8)),
        )
        .eq(lit(ScalarValue::Utf8(Some("2024-01-01".to_string()))));
        let predicate_expr =
            test_build_predicate_expression(&expr, &schema, &mut RequiredColumns::new());
        assert_eq!(predicate_expr.to_string(), expected_expr);

        // test column on the right
        let expr = lit(ScalarValue::Utf8(Some("2024-01-01".to_string()))).eq(cast(
            col("c1"),
            DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::Utf8)),
        ));
        let predicate_expr =
            test_build_predicate_expression(&expr, &schema, &mut RequiredColumns::new());
        assert_eq!(predicate_expr.to_string(), expected_expr);

        Ok(())
    }

    #[test]
    fn row_group_predicate_date_dict_string() -> Result<()> {
        // Test with Dictionary<UInt8, Utf8> for the column
        let schema = Schema::new(vec![Field::new(
            "c1",
            DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::Utf8)),
            false,
        )]);
        let expected_expr = "true";

        // test column on the left
        let expr =
            cast(col("c1"), DataType::Date32).eq(lit(ScalarValue::Date32(Some(123))));
        let predicate_expr =
            test_build_predicate_expression(&expr, &schema, &mut RequiredColumns::new());
        assert_eq!(predicate_expr.to_string(), expected_expr);

        // test column on the right
        let expr =
            lit(ScalarValue::Date32(Some(123))).eq(cast(col("c1"), DataType::Date32));
        let predicate_expr =
            test_build_predicate_expression(&expr, &schema, &mut RequiredColumns::new());
        assert_eq!(predicate_expr.to_string(), expected_expr);

        Ok(())
    }

    #[test]
    fn row_group_predicate_dict_dict_same_value_type() -> Result<()> {
        // Test with Dictionary types that have the same value type but different key types
        let schema = Schema::new(vec![Field::new(
            "c1",
            DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::Utf8)),
            false,
        )]);

        // Direct comparison with no cast
        let expr = col("c1").eq(lit(ScalarValue::Utf8(Some("test".to_string()))));
        let predicate_expr =
            test_build_predicate_expression(&expr, &schema, &mut RequiredColumns::new());
        let expected_expr =
            "c1_null_count@2 != row_count@3 AND c1_min@0 <= test AND test <= c1_max@1";
        assert_eq!(predicate_expr.to_string(), expected_expr);

        // Test with column cast to a dictionary with different key type
        let expr = cast(
            col("c1"),
            DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::Utf8)),
        )
        .eq(lit(ScalarValue::Utf8(Some("test".to_string()))));
        let predicate_expr =
            test_build_predicate_expression(&expr, &schema, &mut RequiredColumns::new());
        let expected_expr = "c1_null_count@2 != row_count@3 AND CAST(c1_min@0 AS Dictionary(UInt16, Utf8)) <= test AND test <= CAST(c1_max@1 AS Dictionary(UInt16, Utf8))";
        assert_eq!(predicate_expr.to_string(), expected_expr);

        Ok(())
    }

    #[test]
    fn row_group_predicate_dict_dict_different_value_type() -> Result<()> {
        // Test with Dictionary types that have different value types
        let schema = Schema::new(vec![Field::new(
            "c1",
            DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::Int32)),
            false,
        )]);
        let expected_expr = "c1_null_count@2 != row_count@3 AND CAST(c1_min@0 AS Int64) <= 123 AND 123 <= CAST(c1_max@1 AS Int64)";

        // Test with literal of a different type
        let expr =
            cast(col("c1"), DataType::Int64).eq(lit(ScalarValue::Int64(Some(123))));
        let predicate_expr =
            test_build_predicate_expression(&expr, &schema, &mut RequiredColumns::new());
        assert_eq!(predicate_expr.to_string(), expected_expr);

        Ok(())
    }

    #[test]
    fn row_group_predicate_nested_dict() -> Result<()> {
        // Test with nested Dictionary types
        let schema = Schema::new(vec![Field::new(
            "c1",
            DataType::Dictionary(
                Box::new(DataType::UInt8),
                Box::new(DataType::Dictionary(
                    Box::new(DataType::UInt16),
                    Box::new(DataType::Utf8),
                )),
            ),
            false,
        )]);
        let expected_expr =
            "c1_null_count@2 != row_count@3 AND c1_min@0 <= test AND test <= c1_max@1";

        // Test with a simple literal
        let expr = col("c1").eq(lit(ScalarValue::Utf8(Some("test".to_string()))));
        let predicate_expr =
            test_build_predicate_expression(&expr, &schema, &mut RequiredColumns::new());
        assert_eq!(predicate_expr.to_string(), expected_expr);

        Ok(())
    }

    #[test]
    fn row_group_predicate_dict_date_dict_date() -> Result<()> {
        // Test with dictionary-wrapped date types for both sides
        let schema = Schema::new(vec![Field::new(
            "c1",
            DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::Date32)),
            false,
        )]);
        let expected_expr = "c1_null_count@2 != row_count@3 AND CAST(c1_min@0 AS Dictionary(UInt16, Date64)) <= 1970-01-01 AND 1970-01-01 <= CAST(c1_max@1 AS Dictionary(UInt16, Date64))";

        // Test with a cast to a different date type
        let expr = cast(
            col("c1"),
            DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::Date64)),
        )
        .eq(lit(ScalarValue::Date64(Some(123))));
        let predicate_expr =
            test_build_predicate_expression(&expr, &schema, &mut RequiredColumns::new());
        assert_eq!(predicate_expr.to_string(), expected_expr);

        Ok(())
    }

    #[test]
    fn row_group_predicate_date_string() -> Result<()> {
        let schema = Schema::new(vec![Field::new("c1", DataType::Utf8, false)]);
        let expected_expr = "true";

        // test column on the left
        let expr =
            cast(col("c1"), DataType::Date32).eq(lit(ScalarValue::Date32(Some(123))));
        let predicate_expr =
            test_build_predicate_expression(&expr, &schema, &mut RequiredColumns::new());
        assert_eq!(predicate_expr.to_string(), expected_expr);

        // test column on the right
        let expr =
            lit(ScalarValue::Date32(Some(123))).eq(cast(col("c1"), DataType::Date32));
        let predicate_expr =
            test_build_predicate_expression(&expr, &schema, &mut RequiredColumns::new());
        assert_eq!(predicate_expr.to_string(), expected_expr);

        Ok(())
    }

    #[test]
    fn row_group_predicate_string_date() -> Result<()> {
        let schema = Schema::new(vec![Field::new("c1", DataType::Date32, false)]);
        let expected_expr = "true";

        // test column on the left
        let expr = cast(col("c1"), DataType::Utf8)
            .eq(lit(ScalarValue::Utf8(Some("2024-01-01".to_string()))));
        let predicate_expr =
            test_build_predicate_expression(&expr, &schema, &mut RequiredColumns::new());
        assert_eq!(predicate_expr.to_string(), expected_expr);

        // test column on the right
        let expr = lit(ScalarValue::Utf8(Some("2024-01-01".to_string())))
            .eq(cast(col("c1"), DataType::Utf8));
        let predicate_expr =
            test_build_predicate_expression(&expr, &schema, &mut RequiredColumns::new());
        assert_eq!(predicate_expr.to_string(), expected_expr);

        Ok(())
    }

    #[test]
    fn row_group_predicate_cast_list() -> Result<()> {
        let schema = Schema::new(vec![Field::new("c1", DataType::Int32, false)]);
        // test cast(c1 as int64) in int64(1, 2, 3)
        let expr = Expr::InList(InList::new(
            Box::new(cast(col("c1"), DataType::Int64)),
            vec![
                lit(ScalarValue::Int64(Some(1))),
                lit(ScalarValue::Int64(Some(2))),
                lit(ScalarValue::Int64(Some(3))),
            ],
            false,
        ));
        let expected_expr = "c1_null_count@2 != row_count@3 AND CAST(c1_min@0 AS Int64) <= 1 AND 1 <= CAST(c1_max@1 AS Int64) OR c1_null_count@2 != row_count@3 AND CAST(c1_min@0 AS Int64) <= 2 AND 2 <= CAST(c1_max@1 AS Int64) OR c1_null_count@2 != row_count@3 AND CAST(c1_min@0 AS Int64) <= 3 AND 3 <= CAST(c1_max@1 AS Int64)";
        let predicate_expr =
            test_build_predicate_expression(&expr, &schema, &mut RequiredColumns::new());
        assert_eq!(predicate_expr.to_string(), expected_expr);

        let expr = Expr::InList(InList::new(
            Box::new(cast(col("c1"), DataType::Int64)),
            vec![
                lit(ScalarValue::Int64(Some(1))),
                lit(ScalarValue::Int64(Some(2))),
                lit(ScalarValue::Int64(Some(3))),
            ],
            true,
        ));
        let expected_expr = "c1_null_count@2 != row_count@3 AND (CAST(c1_min@0 AS Int64) != 1 OR 1 != CAST(c1_max@1 AS Int64)) AND c1_null_count@2 != row_count@3 AND (CAST(c1_min@0 AS Int64) != 2 OR 2 != CAST(c1_max@1 AS Int64)) AND c1_null_count@2 != row_count@3 AND (CAST(c1_min@0 AS Int64) != 3 OR 3 != CAST(c1_max@1 AS Int64))";
        let predicate_expr =
            test_build_predicate_expression(&expr, &schema, &mut RequiredColumns::new());
        assert_eq!(predicate_expr.to_string(), expected_expr);

        Ok(())
    }

    #[test]
    fn prune_decimal_data() {
        // decimal(9,2)
        let schema = Arc::new(Schema::new(vec![Field::new(
            "s1",
            DataType::Decimal128(9, 2),
            true,
        )]));

        prune_with_expr(
            // s1 > 5
            col("s1").gt(lit(ScalarValue::Decimal128(Some(500), 9, 2))),
            &schema,
            // If the data is written by spark, the physical data type is INT32 in the parquet
            // So we use the INT32 type of statistic.
            &TestStatistics::new().with(
                "s1",
                ContainerStats::new_i32(
                    vec![Some(0), Some(4), None, Some(3)], // min
                    vec![Some(5), Some(6), Some(4), None], // max
                ),
            ),
            &[false, true, false, true],
        );

        prune_with_expr(
            // with cast column to other type
            cast(col("s1"), DataType::Decimal128(14, 3))
                .gt(lit(ScalarValue::Decimal128(Some(5000), 14, 3))),
            &schema,
            &TestStatistics::new().with(
                "s1",
                ContainerStats::new_i32(
                    vec![Some(0), Some(4), None, Some(3)], // min
                    vec![Some(5), Some(6), Some(4), None], // max
                ),
            ),
            &[false, true, false, true],
        );

        prune_with_expr(
            // with try cast column to other type
            try_cast(col("s1"), DataType::Decimal128(14, 3))
                .gt(lit(ScalarValue::Decimal128(Some(5000), 14, 3))),
            &schema,
            &TestStatistics::new().with(
                "s1",
                ContainerStats::new_i32(
                    vec![Some(0), Some(4), None, Some(3)], // min
                    vec![Some(5), Some(6), Some(4), None], // max
                ),
            ),
            &[false, true, false, true],
        );

        // decimal(18,2)
        let schema = Arc::new(Schema::new(vec![Field::new(
            "s1",
            DataType::Decimal128(18, 2),
            true,
        )]));
        prune_with_expr(
            // s1 > 5
            col("s1").gt(lit(ScalarValue::Decimal128(Some(500), 18, 2))),
            &schema,
            // If the data is written by spark, the physical data type is INT64 in the parquet
            // So we use the INT32 type of statistic.
            &TestStatistics::new().with(
                "s1",
                ContainerStats::new_i64(
                    vec![Some(0), Some(4), None, Some(3)], // min
                    vec![Some(5), Some(6), Some(4), None], // max
                ),
            ),
            &[false, true, false, true],
        );

        // decimal(23,2)
        let schema = Arc::new(Schema::new(vec![Field::new(
            "s1",
            DataType::Decimal128(23, 2),
            true,
        )]));

        prune_with_expr(
            // s1 > 5
            col("s1").gt(lit(ScalarValue::Decimal128(Some(500), 23, 2))),
            &schema,
            &TestStatistics::new().with(
                "s1",
                ContainerStats::new_decimal128(
                    vec![Some(0), Some(400), None, Some(300)], // min
                    vec![Some(500), Some(600), Some(400), None], // max
                    23,
                    2,
                ),
            ),
            &[false, true, false, true],
        );
    }

    #[test]
    fn prune_api() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("s1", DataType::Utf8, true),
            Field::new("s2", DataType::Int32, true),
        ]));

        let statistics = TestStatistics::new().with(
            "s2",
            ContainerStats::new_i32(
                vec![Some(0), Some(4), None, Some(3)], // min
                vec![Some(5), Some(6), None, None],    // max
            ),
        );
        prune_with_expr(
            // Prune using s2 > 5
            col("s2").gt(lit(5)),
            &schema,
            &statistics,
            // s2 [0, 5] ==> no rows should pass
            // s2 [4, 6] ==> some rows could pass
            // No stats for s2 ==> some rows could pass
            // s2 [3, None] (null max) ==> some rows could pass
            &[false, true, true, true],
        );

        prune_with_expr(
            // filter with cast
            cast(col("s2"), DataType::Int64).gt(lit(ScalarValue::Int64(Some(5)))),
            &schema,
            &statistics,
            &[false, true, true, true],
        );
    }

    #[test]
    fn prune_not_eq_data() {
        let schema = Arc::new(Schema::new(vec![Field::new("s1", DataType::Utf8, true)]));

        prune_with_expr(
            // Prune using s2 != 'M'
            col("s1").not_eq(lit("M")),
            &schema,
            &TestStatistics::new().with(
                "s1",
                ContainerStats::new_utf8(
                    vec![Some("A"), Some("A"), Some("N"), Some("M"), None, Some("A")], // min
                    vec![Some("Z"), Some("L"), Some("Z"), Some("M"), None, None], // max
                ),
            ),
            // s1 [A, Z] ==> might have values that pass predicate
            // s1 [A, L] ==> all rows pass the predicate
            // s1 [N, Z] ==> all rows pass the predicate
            // s1 [M, M] ==> all rows do not pass the predicate
            // No stats for s2 ==> some rows could pass
            // s2 [3, None] (null max) ==> some rows could pass
            &[true, true, true, false, true, true],
        );
    }

    /// Creates setup for boolean chunk pruning
    ///
    /// For predicate "b1" (boolean expr)
    /// b1 [false, false] ==> no rows can pass (not keep)
    /// b1 [false, true] ==> some rows could pass (must keep)
    /// b1 [true, true] ==> all rows must pass (must keep)
    /// b1 [NULL, NULL]  ==> unknown (must keep)
    /// b1 [false, NULL]  ==> unknown (must keep)
    ///
    /// For predicate "!b1" (boolean expr)
    /// b1 [false, false] ==> all rows pass (must keep)
    /// b1 [false, true] ==> some rows could pass (must keep)
    /// b1 [true, true] ==> no rows can pass (not keep)
    /// b1 [NULL, NULL]  ==> unknown (must keep)
    /// b1 [false, NULL]  ==> unknown (must keep)
    fn bool_setup() -> (SchemaRef, TestStatistics, Vec<bool>, Vec<bool>) {
        let schema =
            Arc::new(Schema::new(vec![Field::new("b1", DataType::Boolean, true)]));

        let statistics = TestStatistics::new().with(
            "b1",
            ContainerStats::new_bool(
                vec![Some(false), Some(false), Some(true), None, Some(false)], // min
                vec![Some(false), Some(true), Some(true), None, None],         // max
            ),
        );
        let expected_true = vec![false, true, true, true, true];
        let expected_false = vec![true, true, false, true, true];

        (schema, statistics, expected_true, expected_false)
    }

    #[test]
    fn prune_bool_const_expr() {
        let (schema, statistics, _, _) = bool_setup();

        prune_with_expr(
            // true
            lit(true),
            &schema,
            &statistics,
            &[true, true, true, true, true],
        );

        prune_with_expr(
            // false
            lit(false),
            &schema,
            &statistics,
            &[false, false, false, false, false],
        );
    }

    #[test]
    fn prune_bool_column() {
        let (schema, statistics, expected_true, _) = bool_setup();

        prune_with_expr(
            // b1
            col("b1"),
            &schema,
            &statistics,
            &expected_true,
        );
    }

    #[test]
    fn prune_bool_not_column() {
        let (schema, statistics, _, expected_false) = bool_setup();

        prune_with_expr(
            // !b1
            col("b1").not(),
            &schema,
            &statistics,
            &expected_false,
        );
    }

    #[test]
    fn prune_bool_column_eq_true() {
        let (schema, statistics, expected_true, _) = bool_setup();

        prune_with_expr(
            // b1 = true
            col("b1").eq(lit(true)),
            &schema,
            &statistics,
            &expected_true,
        );
    }

    #[test]
    fn prune_bool_not_column_eq_true() {
        let (schema, statistics, _, expected_false) = bool_setup();

        prune_with_expr(
            // !b1 = true
            col("b1").not().eq(lit(true)),
            &schema,
            &statistics,
            &expected_false,
        );
    }

    /// Creates a setup for chunk pruning, modeling a int32 column "i"
    /// with 5 different containers (e.g. RowGroups). They have [min,
    /// max]:
    ///
    /// i [-5, 5]
    /// i [1, 11]
    /// i [-11, -1]
    /// i [NULL, NULL]
    /// i [1, NULL]
    fn int32_setup() -> (SchemaRef, TestStatistics) {
        let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int32, true)]));

        let statistics = TestStatistics::new().with(
            "i",
            ContainerStats::new_i32(
                vec![Some(-5), Some(1), Some(-11), None, Some(1)], // min
                vec![Some(5), Some(11), Some(-1), None, None],     // max
            ),
        );
        (schema, statistics)
    }

    #[test]
    fn prune_int32_col_gt_zero() {
        let (schema, statistics) = int32_setup();

        // Expression "i > 0" and "-i < 0"
        // i [-5, 5] ==> some rows could pass (must keep)
        // i [1, 11] ==> all rows must pass (must keep)
        // i [-11, -1] ==>  no rows can pass (not keep)
        // i [NULL, NULL]  ==> unknown (must keep)
        // i [1, NULL]  ==> unknown (must keep)
        let expected_ret = &[true, true, false, true, true];

        // i > 0
        prune_with_expr(col("i").gt(lit(0)), &schema, &statistics, expected_ret);

        // -i < 0
        prune_with_expr(
            Expr::Negative(Box::new(col("i"))).lt(lit(0)),
            &schema,
            &statistics,
            expected_ret,
        );
    }

    #[test]
    fn prune_int32_col_lte_zero() {
        let (schema, statistics) = int32_setup();

        // Expression "i <= 0" and "-i >= 0"
        // i [-5, 5] ==> some rows could pass (must keep)
        // i [1, 11] ==> no rows can pass (not keep)
        // i [-11, -1] ==>  all rows must pass (must keep)
        // i [NULL, NULL]  ==> unknown (must keep)
        // i [1, NULL]  ==> no rows can pass (not keep)
        let expected_ret = &[true, false, true, true, false];

        prune_with_expr(
            // i <= 0
            col("i").lt_eq(lit(0)),
            &schema,
            &statistics,
            expected_ret,
        );

        prune_with_expr(
            // -i >= 0
            Expr::Negative(Box::new(col("i"))).gt_eq(lit(0)),
            &schema,
            &statistics,
            expected_ret,
        );
    }

    #[test]
    fn prune_int32_col_lte_zero_cast() {
        let (schema, statistics) = int32_setup();

        // Expression "cast(i as utf8) <= '0'"
        // i [-5, 5] ==> some rows could pass (must keep)
        // i [1, 11] ==> no rows can pass in theory, -0.22 (conservatively keep)
        // i [-11, -1] ==>  no rows could pass in theory (conservatively keep)
        // i [NULL, NULL]  ==> unknown (must keep)
        // i [1, NULL]  ==> no rows can pass (conservatively keep)
        let expected_ret = &[true, true, true, true, true];

        prune_with_expr(
            // cast(i as utf8) <= 0
            cast(col("i"), DataType::Utf8).lt_eq(lit("0")),
            &schema,
            &statistics,
            expected_ret,
        );

        prune_with_expr(
            // try_cast(i as utf8) <= 0
            try_cast(col("i"), DataType::Utf8).lt_eq(lit("0")),
            &schema,
            &statistics,
            expected_ret,
        );

        prune_with_expr(
            // cast(-i as utf8) >= 0
            cast(Expr::Negative(Box::new(col("i"))), DataType::Utf8).gt_eq(lit("0")),
            &schema,
            &statistics,
            expected_ret,
        );

        prune_with_expr(
            // try_cast(-i as utf8) >= 0
            try_cast(Expr::Negative(Box::new(col("i"))), DataType::Utf8).gt_eq(lit("0")),
            &schema,
            &statistics,
            expected_ret,
        );
    }

    #[test]
    fn prune_int32_col_eq_zero() {
        let (schema, statistics) = int32_setup();

        // Expression "i = 0"
        // i [-5, 5] ==> some rows could pass (must keep)
        // i [1, 11] ==> no rows can pass (not keep)
        // i [-11, -1] ==>  no rows can pass (not keep)
        // i [NULL, NULL]  ==> unknown (must keep)
        // i [1, NULL]  ==> no rows can pass (not keep)
        let expected_ret = &[true, false, false, true, false];

        prune_with_expr(
            // i = 0
            col("i").eq(lit(0)),
            &schema,
            &statistics,
            expected_ret,
        );
    }

    #[test]
    fn prune_int32_col_eq_zero_cast() {
        let (schema, statistics) = int32_setup();

        // Expression "cast(i as int64) = 0"
        // i [-5, 5] ==> some rows could pass (must keep)
        // i [1, 11] ==> no rows can pass (not keep)
        // i [-11, -1] ==>  no rows can pass (not keep)
        // i [NULL, NULL]  ==> unknown (must keep)
        // i [1, NULL]  ==> no rows can pass (not keep)
        let expected_ret = &[true, false, false, true, false];

        prune_with_expr(
            cast(col("i"), DataType::Int64).eq(lit(0i64)),
            &schema,
            &statistics,
            expected_ret,
        );

        prune_with_expr(
            try_cast(col("i"), DataType::Int64).eq(lit(0i64)),
            &schema,
            &statistics,
            expected_ret,
        );
    }

    #[test]
    fn prune_int32_col_eq_zero_cast_as_str() {
        let (schema, statistics) = int32_setup();

        // Note the cast is to a string where sorting properties are
        // not the same as integers
        //
        // Expression "cast(i as utf8) = '0'"
        // i [-5, 5] ==> some rows could pass (keep)
        // i [1, 11] ==> no rows can pass  (could keep)
        // i [-11, -1] ==>  no rows can pass (could keep)
        // i [NULL, NULL]  ==> unknown (keep)
        // i [1, NULL]  ==> no rows can pass (could keep)
        let expected_ret = &[true, true, true, true, true];

        prune_with_expr(
            cast(col("i"), DataType::Utf8).eq(lit("0")),
            &schema,
            &statistics,
            expected_ret,
        );
    }

    #[test]
    fn prune_int32_col_lt_neg_one() {
        let (schema, statistics) = int32_setup();

        // Expression "i > -1" and "-i < 1"
        // i [-5, 5] ==> some rows could pass (must keep)
        // i [1, 11] ==> all rows must pass (must keep)
        // i [-11, -1] ==>  no rows can pass (not keep)
        // i [NULL, NULL]  ==> unknown (must keep)
        // i [1, NULL]  ==> all rows must pass (must keep)
        let expected_ret = &[true, true, false, true, true];

        prune_with_expr(
            // i > -1
            col("i").gt(lit(-1)),
            &schema,
            &statistics,
            expected_ret,
        );

        prune_with_expr(
            // -i < 1
            Expr::Negative(Box::new(col("i"))).lt(lit(1)),
            &schema,
            &statistics,
            expected_ret,
        );
    }

    #[test]
    fn prune_int32_is_null() {
        let (schema, statistics) = int32_setup();

        // Expression "i IS NULL" when there are no null statistics,
        // should all be kept
        let expected_ret = &[true, true, true, true, true];

        prune_with_expr(
            // i IS NULL, no null statistics
            col("i").is_null(),
            &schema,
            &statistics,
            expected_ret,
        );

        // provide null counts for each column
        let statistics = statistics.with_null_counts(
            "i",
            vec![
                Some(0), // no nulls (don't keep)
                Some(1), // 1 null
                None,    // unknown nulls
                None, // unknown nulls (min/max are both null too, like no stats at all)
                Some(0), // 0 nulls (max=null too which means no known max) (don't keep)
            ],
        );

        let expected_ret = &[false, true, true, true, false];

        prune_with_expr(
            // i IS NULL, with actual null statistics
            col("i").is_null(),
            &schema,
            &statistics,
            expected_ret,
        );
    }

    #[test]
    fn prune_int32_column_is_known_all_null() {
        let (schema, statistics) = int32_setup();

        // Expression "i < 0"
        // i [-5, 5] ==> some rows could pass (must keep)
        // i [1, 11] ==> no rows can pass (not keep)
        // i [-11, -1] ==>  all rows must pass (must keep)
        // i [NULL, NULL]  ==> unknown (must keep)
        // i [1, NULL]  ==> no rows can pass (not keep)
        let expected_ret = &[true, false, true, true, false];

        prune_with_expr(
            // i < 0
            col("i").lt(lit(0)),
            &schema,
            &statistics,
            expected_ret,
        );

        // provide row counts for each column
        let statistics = statistics.with_row_counts(
            "i",
            vec![
                Some(10), // 10 rows of data
                Some(9),  // 9 rows of data
                None,     // unknown row counts
                Some(4),
                Some(10),
            ],
        );

        // pruning result is still the same if we only know row counts
        prune_with_expr(
            // i < 0, with only row counts statistics
            col("i").lt(lit(0)),
            &schema,
            &statistics,
            expected_ret,
        );

        // provide null counts for each column
        let statistics = statistics.with_null_counts(
            "i",
            vec![
                Some(0), // no nulls
                Some(1), // 1 null
                None,    // unknown nulls
                Some(4), // 4 nulls, which is the same as the row counts, i.e. this column is all null (don't keep)
                Some(0), // 0 nulls (max=null too which means no known max)
            ],
        );

        // Expression "i < 0" with actual null and row counts statistics
        // col | min, max     | row counts | null counts |
        // ----+--------------+------------+-------------+
        //  i  | [-5, 5]      | 10         | 0           | ==> Some rows could pass (must keep)
        //  i  | [1, 11]      | 9          | 1           | ==> No rows can pass (not keep)
        //  i  | [-11,-1]     | Unknown    | Unknown     | ==> All rows must pass (must keep)
        //  i  | [NULL, NULL] | 4          | 4           | ==> The column is all null (not keep)
        //  i  | [1, NULL]    | 10         | 0           | ==> No rows can pass (not keep)
        let expected_ret = &[true, false, true, false, false];

        prune_with_expr(
            // i < 0, with actual null and row counts statistics
            col("i").lt(lit(0)),
            &schema,
            &statistics,
            expected_ret,
        );
    }

    #[test]
    fn prune_cast_column_scalar() {
        // The data type of column i is INT32
        let (schema, statistics) = int32_setup();
        let expected_ret = &[true, true, false, true, true];

        prune_with_expr(
            // i > int64(0)
            col("i").gt(cast(lit(ScalarValue::Int64(Some(0))), DataType::Int32)),
            &schema,
            &statistics,
            expected_ret,
        );

        prune_with_expr(
            // cast(i as int64) > int64(0)
            cast(col("i"), DataType::Int64).gt(lit(ScalarValue::Int64(Some(0)))),
            &schema,
            &statistics,
            expected_ret,
        );

        prune_with_expr(
            // try_cast(i as int64) > int64(0)
            try_cast(col("i"), DataType::Int64).gt(lit(ScalarValue::Int64(Some(0)))),
            &schema,
            &statistics,
            expected_ret,
        );

        prune_with_expr(
            // `-cast(i as int64) < 0` convert to `cast(i as int64) > -0`
            Expr::Negative(Box::new(cast(col("i"), DataType::Int64)))
                .lt(lit(ScalarValue::Int64(Some(0)))),
            &schema,
            &statistics,
            expected_ret,
        );
    }

    #[test]
    fn test_increment_utf8() {
        // Basic ASCII
        assert_eq!(increment_utf8("abc").unwrap(), "abd");
        assert_eq!(increment_utf8("abz").unwrap(), "ab{");

        // Test around ASCII 127 (DEL)
        assert_eq!(increment_utf8("~").unwrap(), "\u{7f}"); // 126 -> 127
        assert_eq!(increment_utf8("\u{7f}").unwrap(), "\u{80}"); // 127 -> 128

        // Test 2-byte UTF-8 sequences
        assert_eq!(increment_utf8("ß").unwrap(), "à"); // U+00DF -> U+00E0

        // Test 3-byte UTF-8 sequences
        assert_eq!(increment_utf8("℣").unwrap(), "ℤ"); // U+2123 -> U+2124

        // Test at UTF-8 boundaries
        assert_eq!(increment_utf8("\u{7FF}").unwrap(), "\u{800}"); // 2-byte to 3-byte boundary
        assert_eq!(increment_utf8("\u{FFFF}").unwrap(), "\u{10000}"); // 3-byte to 4-byte boundary

        // Test that if we can't increment we return None
        assert!(increment_utf8("").is_none());
        assert!(increment_utf8("\u{10FFFF}").is_none()); // U+10FFFF is the max code point

        // Test that if we can't increment the last character we do the previous one and truncate
        assert_eq!(increment_utf8("a\u{10FFFF}").unwrap(), "b");

        // Test surrogate pair range (0xD800..=0xDFFF)
        assert_eq!(increment_utf8("a\u{D7FF}").unwrap(), "b");
        assert!(increment_utf8("\u{D7FF}").is_none());

        // Test non-characters range (0xFDD0..=0xFDEF)
        assert_eq!(increment_utf8("a\u{FDCF}").unwrap(), "b");
        assert!(increment_utf8("\u{FDCF}").is_none());

        // Test private use area limit (>= 0x110000)
        assert_eq!(increment_utf8("a\u{10FFFF}").unwrap(), "b");
        assert!(increment_utf8("\u{10FFFF}").is_none()); // Can't increment past max valid codepoint
    }

    /// Creates a setup for chunk pruning, modeling a utf8 column "s1"
    /// with 5 different containers (e.g. RowGroups). They have [min,
    /// max]:
    /// s1 ["A", "Z"]
    /// s1 ["A", "L"]
    /// s1 ["N", "Z"]
    /// s1 [NULL, NULL]
    /// s1 ["A", NULL]
    /// s1 ["", "A"]
    /// s1 ["", ""]
    /// s1 ["AB", "A\u{10ffff}"]
    /// s1 ["A\u{10ffff}\u{10ffff}\u{10ffff}", "A\u{10ffff}\u{10ffff}"]
    fn utf8_setup() -> (SchemaRef, TestStatistics) {
        let schema = Arc::new(Schema::new(vec![Field::new("s1", DataType::Utf8, true)]));

        let statistics = TestStatistics::new().with(
            "s1",
            ContainerStats::new_utf8(
                vec![
                    Some("A"),
                    Some("A"),
                    Some("N"),
                    Some("M"),
                    None,
                    Some("A"),
                    Some(""),
                    Some(""),
                    Some("AB"),
                    Some("A\u{10ffff}\u{10ffff}"),
                ], // min
                vec![
                    Some("Z"),
                    Some("L"),
                    Some("Z"),
                    Some("M"),
                    None,
                    None,
                    Some("A"),
                    Some(""),
                    Some("A\u{10ffff}\u{10ffff}\u{10ffff}"),
                    Some("A\u{10ffff}\u{10ffff}"),
                ], // max
            ),
        );
        (schema, statistics)
    }

    #[test]
    fn prune_utf8_eq() {
        let (schema, statistics) = utf8_setup();

        let expr = col("s1").eq(lit("A"));
        #[rustfmt::skip]
        let expected_ret = &[
            // s1 ["A", "Z"] ==> some rows could pass (must keep)
            true,
            // s1 ["A", "L"] ==> some rows could pass (must keep)
            true,
            // s1 ["N", "Z"] ==> no rows can pass (not keep)
            false,
            // s1 ["M", "M"] ==> no rows can pass (not keep)
            false,
            // s1 [NULL, NULL]  ==> unknown (must keep)
            true,
            // s1 ["A", NULL]  ==> unknown (must keep)
            true,
            // s1 ["", "A"]  ==> some rows could pass (must keep)
            true,
            // s1 ["", ""]  ==> no rows can pass (not keep)
            false,
            // s1 ["AB", "A\u{10ffff}\u{10ffff}\u{10ffff}"]  ==> no rows can pass (not keep)
            false,
            // s1 ["A\u{10ffff}\u{10ffff}", "A\u{10ffff}\u{10ffff}"]  ==> no rows can pass (not keep)
            false,
        ];
        prune_with_expr(expr, &schema, &statistics, expected_ret);

        let expr = col("s1").eq(lit(""));
        #[rustfmt::skip]
        let expected_ret = &[
            // s1 ["A", "Z"] ==> no rows can pass (not keep)
            false,
            // s1 ["A", "L"] ==> no rows can pass (not keep)
            false,
            // s1 ["N", "Z"] ==> no rows can pass (not keep)
            false,
            // s1 ["M", "M"] ==> no rows can pass (not keep)
            false,
            // s1 [NULL, NULL]  ==> unknown (must keep)
            true,
            // s1 ["A", NULL]  ==> no rows can pass (not keep)
            false,
            // s1 ["", "A"]  ==> some rows could pass (must keep)
            true,
            // s1 ["", ""]  ==> all rows must pass (must keep)
            true,
            // s1 ["AB", "A\u{10ffff}\u{10ffff}\u{10ffff}"]  ==> no rows can pass (not keep)
            false,
            // s1 ["A\u{10ffff}\u{10ffff}", "A\u{10ffff}\u{10ffff}"]  ==> no rows can pass (not keep)
            false,
        ];
        prune_with_expr(expr, &schema, &statistics, expected_ret);
    }

    #[test]
    fn prune_utf8_not_eq() {
        let (schema, statistics) = utf8_setup();

        let expr = col("s1").not_eq(lit("A"));
        #[rustfmt::skip]
        let expected_ret = &[
            // s1 ["A", "Z"] ==> some rows could pass (must keep)
            true,
            // s1 ["A", "L"] ==> some rows could pass (must keep)
            true,
            // s1 ["N", "Z"] ==> all rows must pass (must keep)
            true,
            // s1 ["M", "M"] ==> all rows must pass (must keep)
            true,
            // s1 [NULL, NULL]  ==> unknown (must keep)
            true,
            // s1 ["A", NULL]  ==> unknown (must keep)
            true,
            // s1 ["", "A"]  ==> some rows could pass (must keep)
            true,
            // s1 ["", ""]  ==> all rows must pass (must keep)
            true,
            // s1 ["AB", "A\u{10ffff}\u{10ffff}"]  ==> all rows must pass (must keep)
            true,
            // s1 ["A\u{10ffff}\u{10ffff}", "A\u{10ffff}\u{10ffff}"]  ==> all rows must pass (must keep)
            true,
        ];
        prune_with_expr(expr, &schema, &statistics, expected_ret);

        let expr = col("s1").not_eq(lit(""));
        #[rustfmt::skip]
        let expected_ret = &[
            // s1 ["A", "Z"] ==> all rows must pass (must keep)
            true,
            // s1 ["A", "L"] ==> all rows must pass (must keep)
            true,
            // s1 ["N", "Z"] ==> all rows must pass (must keep)
            true,
            // s1 ["M", "M"] ==> all rows must pass (must keep)
            true,
            // s1 [NULL, NULL]  ==> unknown (must keep)
            true,
            // s1 ["A", NULL]  ==> unknown (must keep)
            true,
            // s1 ["", "A"]  ==> some rows could pass (must keep)
            true,
            // s1 ["", ""]  ==> no rows can pass (not keep)
            false,
            // s1 ["AB", "A\u{10ffff}\u{10ffff}\u{10ffff}"]  ==> all rows must pass (must keep)
            true,
            // s1 ["A\u{10ffff}\u{10ffff}", "A\u{10ffff}\u{10ffff}"]  ==> all rows must pass (must keep)
            true,
        ];
        prune_with_expr(expr, &schema, &statistics, expected_ret);
    }

    #[test]
    fn prune_utf8_like_one() {
        let (schema, statistics) = utf8_setup();

        let expr = col("s1").like(lit("A_"));
        #[rustfmt::skip]
        let expected_ret = &[
            // s1 ["A", "Z"] ==> some rows could pass (must keep)
            true,
            // s1 ["A", "L"] ==> some rows could pass (must keep)
            true,
            // s1 ["N", "Z"] ==> no rows can pass (not keep)
            false,
            // s1 ["M", "M"] ==> no rows can pass (not keep)
            false,
            // s1 [NULL, NULL]  ==> unknown (must keep)
            true,
            // s1 ["A", NULL]  ==> unknown (must keep)
            true,
            // s1 ["", "A"]  ==> some rows could pass (must keep)
            true,
            // s1 ["", ""]  ==> no rows can pass (not keep)
            false,
            // s1 ["AB", "A\u{10ffff}\u{10ffff}\u{10ffff}"]  ==> some rows could pass (must keep)
            true,
            // s1 ["A\u{10ffff}\u{10ffff}", "A\u{10ffff}\u{10ffff}"]  ==> some rows could pass (must keep)
            true,
        ];
        prune_with_expr(expr, &schema, &statistics, expected_ret);

        let expr = col("s1").like(lit("_A_"));
        #[rustfmt::skip]
        let expected_ret = &[
            // s1 ["A", "Z"] ==> some rows could pass (must keep)
            true,
            // s1 ["A", "L"] ==> some rows could pass (must keep)
            true,
            // s1 ["N", "Z"] ==> some rows could pass (must keep)
            true,
            // s1 ["M", "M"] ==> some rows could pass (must keep)
            true,
            // s1 [NULL, NULL]  ==> unknown (must keep)
            true,
            // s1 ["A", NULL]  ==> unknown (must keep)
            true,
            // s1 ["", "A"]  ==> some rows could pass (must keep)
            true,
            // s1 ["", ""]  ==> some rows could pass (must keep)
            true,
            // s1 ["AB", "A\u{10ffff}\u{10ffff}\u{10ffff}"]  ==> some rows could pass (must keep)
            true,
            // s1 ["A\u{10ffff}\u{10ffff}", "A\u{10ffff}\u{10ffff}"]  ==> some rows could pass (must keep)
            true,
        ];
        prune_with_expr(expr, &schema, &statistics, expected_ret);

        let expr = col("s1").like(lit("_"));
        #[rustfmt::skip]
        let expected_ret = &[
            // s1 ["A", "Z"] ==> all rows must pass (must keep)
            true,
            // s1 ["A", "L"] ==> all rows must pass (must keep)
            true,
            // s1 ["N", "Z"] ==> all rows must pass (must keep)
            true,
            // s1 ["M", "M"] ==> all rows must pass (must keep)
            true,
            // s1 [NULL, NULL]  ==> unknown (must keep)
            true,
            // s1 ["A", NULL]  ==> unknown (must keep)
            true,
            // s1 ["", "A"]  ==> all rows must pass (must keep)
            true,
            // s1 ["", ""]  ==> all rows must pass (must keep)
            true,
            // s1 ["AB", "A\u{10ffff}\u{10ffff}\u{10ffff}"]  ==> all rows must pass (must keep)
            true,
            // s1 ["A\u{10ffff}\u{10ffff}", "A\u{10ffff}\u{10ffff}"]  ==> all rows must pass (must keep)
            true,
        ];
        prune_with_expr(expr, &schema, &statistics, expected_ret);

        let expr = col("s1").like(lit(""));
        #[rustfmt::skip]
        let expected_ret = &[
            // s1 ["A", "Z"] ==> no rows can pass (not keep)
            false,
            // s1 ["A", "L"] ==> no rows can pass (not keep)
            false,
            // s1 ["N", "Z"] ==> no rows can pass (not keep)
            false,
            // s1 ["M", "M"] ==> no rows can pass (not keep)
            false,
            // s1 [NULL, NULL]  ==> unknown (must keep)
            true,
            // s1 ["A", NULL]  ==> no rows can pass (not keep)
            false,
            // s1 ["", "A"]  ==> some rows could pass (must keep)
            true,
            // s1 ["", ""]  ==> all rows must pass (must keep)
            true,
            // s1 ["AB", "A\u{10ffff}\u{10ffff}\u{10ffff}"]  ==> no rows can pass (not keep)
            false,
            // s1 ["A\u{10ffff}\u{10ffff}", "A\u{10ffff}\u{10ffff}"]  ==> no rows can pass (not keep)
            false,
        ];
        prune_with_expr(expr, &schema, &statistics, expected_ret);
    }

    #[test]
    fn prune_utf8_like_many() {
        let (schema, statistics) = utf8_setup();

        let expr = col("s1").like(lit("A%"));
        #[rustfmt::skip]
        let expected_ret = &[
            // s1 ["A", "Z"] ==> some rows could pass (must keep)
            true,
            // s1 ["A", "L"] ==> some rows could pass (must keep)
            true,
            // s1 ["N", "Z"] ==> no rows can pass (not keep)
            false,
            // s1 ["M", "M"] ==> no rows can pass (not keep)
            false,
            // s1 [NULL, NULL]  ==> unknown (must keep)
            true,
            // s1 ["A", NULL]  ==> unknown (must keep)
            true,
            // s1 ["", "A"]  ==> some rows could pass (must keep)
            true,
            // s1 ["", ""]  ==> no rows can pass (not keep)
            false,
            // s1 ["AB", "A\u{10ffff}\u{10ffff}\u{10ffff}"]  ==> some rows could pass (must keep)
            true,
            // s1 ["A\u{10ffff}\u{10ffff}", "A\u{10ffff}\u{10ffff}"]  ==> some rows could pass (must keep)
            true,
        ];
        prune_with_expr(expr, &schema, &statistics, expected_ret);

        let expr = col("s1").like(lit("%A%"));
        #[rustfmt::skip]
        let expected_ret = &[
            // s1 ["A", "Z"] ==> some rows could pass (must keep)
            true,
            // s1 ["A", "L"] ==> some rows could pass (must keep)
            true,
            // s1 ["N", "Z"] ==> some rows could pass (must keep)
            true,
            // s1 ["M", "M"] ==> some rows could pass (must keep)
            true,
            // s1 [NULL, NULL]  ==> unknown (must keep)
            true,
            // s1 ["A", NULL]  ==> unknown (must keep)
            true,
            // s1 ["", "A"]  ==> some rows could pass (must keep)
            true,
            // s1 ["", ""]  ==> some rows could pass (must keep)
            true,
            // s1 ["AB", "A\u{10ffff}\u{10ffff}\u{10ffff}"]  ==> some rows could pass (must keep)
            true,
            // s1 ["A\u{10ffff}\u{10ffff}", "A\u{10ffff}\u{10ffff}"]  ==> some rows could pass (must keep)
            true,
        ];
        prune_with_expr(expr, &schema, &statistics, expected_ret);

        let expr = col("s1").like(lit("%"));
        #[rustfmt::skip]
        let expected_ret = &[
            // s1 ["A", "Z"] ==> all rows must pass (must keep)
            true,
            // s1 ["A", "L"] ==> all rows must pass (must keep)
            true,
            // s1 ["N", "Z"] ==> all rows must pass (must keep)
            true,
            // s1 ["M", "M"] ==> all rows must pass (must keep)
            true,
            // s1 [NULL, NULL]  ==> unknown (must keep)
            true,
            // s1 ["A", NULL]  ==> unknown (must keep)
            true,
            // s1 ["", "A"]  ==> all rows must pass (must keep)
            true,
            // s1 ["", ""]  ==> all rows must pass (must keep)
            true,
            // s1 ["AB", "A\u{10ffff}\u{10ffff}\u{10ffff}"]  ==> all rows must pass (must keep)
            true,
            // s1 ["A\u{10ffff}\u{10ffff}", "A\u{10ffff}\u{10ffff}"]  ==> all rows must pass (must keep)
            true,
        ];
        prune_with_expr(expr, &schema, &statistics, expected_ret);

        let expr = col("s1").like(lit(""));
        #[rustfmt::skip]
        let expected_ret = &[
            // s1 ["A", "Z"] ==> no rows can pass (not keep)
            false,
            // s1 ["A", "L"] ==> no rows can pass (not keep)
            false,
            // s1 ["N", "Z"] ==> no rows can pass (not keep)
            false,
            // s1 ["M", "M"] ==> no rows can pass (not keep)
            false,
            // s1 [NULL, NULL]  ==> unknown (must keep)
            true,
            // s1 ["A", NULL]  ==> no rows can pass (not keep)
            false,
            // s1 ["", "A"]  ==> some rows could pass (must keep)
            true,
            // s1 ["", ""]  ==> all rows must pass (must keep)
            true,
            // s1 ["AB", "A\u{10ffff}\u{10ffff}\u{10ffff}"]  ==> no rows can pass (not keep)
            false,
            // s1 ["A\u{10ffff}\u{10ffff}", "A\u{10ffff}\u{10ffff}"]  ==> no rows can pass (not keep)
            false,
        ];
        prune_with_expr(expr, &schema, &statistics, expected_ret);
    }

    #[test]
    fn prune_utf8_not_like_one() {
        let (schema, statistics) = utf8_setup();

        let expr = col("s1").not_like(lit("A\u{10ffff}_"));
        #[rustfmt::skip]
        let expected_ret = &[
            // s1 ["A", "Z"] ==> some rows could pass (must keep)
            true,
            // s1 ["A", "L"] ==> some rows could pass (must keep)
            true,
            // s1 ["N", "Z"] ==> some rows could pass (must keep)
            true,
            // s1 ["M", "M"] ==> some rows could pass (must keep)
            true,
            // s1 [NULL, NULL]  ==> unknown (must keep)
            true,
            // s1 ["A", NULL]  ==> some rows could pass (must keep)
            true,
            // s1 ["", "A"]  ==> some rows could pass (must keep)
            true,
            // s1 ["", ""]  ==> some rows could pass (must keep)
            true,
            // s1 ["AB", "A\u{10ffff}\u{10ffff}\u{10ffff}"]  ==> some rows could pass (must keep)
            true,
            // s1 ["A\u{10ffff}\u{10ffff}", "A\u{10ffff}\u{10ffff}"]  ==> no row match. (min, max) maybe truncate 
            // original (min, max) maybe ("A\u{10ffff}\u{10ffff}\u{10ffff}", "A\u{10ffff}\u{10ffff}\u{10ffff}\u{10ffff}")
            true,
        ];
        prune_with_expr(expr, &schema, &statistics, expected_ret);
    }

    #[test]
    fn prune_utf8_not_like_many() {
        let (schema, statistics) = utf8_setup();

        let expr = col("s1").not_like(lit("A\u{10ffff}%"));
        #[rustfmt::skip]
        let expected_ret = &[
            // s1 ["A", "Z"] ==> some rows could pass (must keep)
            true,
            // s1 ["A", "L"] ==> some rows could pass (must keep)
            true,
            // s1 ["N", "Z"] ==> some rows could pass (must keep)
            true,
            // s1 ["M", "M"] ==> some rows could pass (must keep)
            true,
            // s1 [NULL, NULL]  ==> unknown (must keep)
            true,
            // s1 ["A", NULL]  ==> some rows could pass (must keep)
            true,
            // s1 ["", "A"]  ==> some rows could pass (must keep)
            true,
            // s1 ["", ""]  ==> some rows could pass (must keep)
            true,
            // s1 ["AB", "A\u{10ffff}\u{10ffff}\u{10ffff}"]  ==> some rows could pass (must keep)
            true,
            // s1 ["A\u{10ffff}\u{10ffff}", "A\u{10ffff}\u{10ffff}"]  ==> no row match
            false,
        ];
        prune_with_expr(expr, &schema, &statistics, expected_ret);

        let expr = col("s1").not_like(lit("A\u{10ffff}%\u{10ffff}"));
        #[rustfmt::skip]
        let expected_ret = &[
            // s1 ["A", "Z"] ==> some rows could pass (must keep)
            true,
            // s1 ["A", "L"] ==> some rows could pass (must keep)
            true,
            // s1 ["N", "Z"] ==> some rows could pass (must keep)
            true,
            // s1 ["M", "M"] ==> some rows could pass (must keep)
            true,
            // s1 [NULL, NULL]  ==> unknown (must keep)
            true,
            // s1 ["A", NULL]  ==> some rows could pass (must keep)
            true,
            // s1 ["", "A"]  ==> some rows could pass (must keep)
            true,
            // s1 ["", ""]  ==> some rows could pass (must keep)
            true,
            // s1 ["AB", "A\u{10ffff}\u{10ffff}\u{10ffff}"]  ==> some rows could pass (must keep)
            true,
            // s1 ["A\u{10ffff}\u{10ffff}", "A\u{10ffff}\u{10ffff}"]  ==> some rows could pass (must keep)
            true,
        ];
        prune_with_expr(expr, &schema, &statistics, expected_ret);

        let expr = col("s1").not_like(lit("A\u{10ffff}%\u{10ffff}_"));
        #[rustfmt::skip]
        let expected_ret = &[
            // s1 ["A", "Z"] ==> some rows could pass (must keep)
            true,
            // s1 ["A", "L"] ==> some rows could pass (must keep)
            true,
            // s1 ["N", "Z"] ==> some rows could pass (must keep)
            true,
            // s1 ["M", "M"] ==> some rows could pass (must keep)
            true,
            // s1 [NULL, NULL]  ==> unknown (must keep)
            true,
            // s1 ["A", NULL]  ==> some rows could pass (must keep)
            true,
            // s1 ["", "A"]  ==> some rows could pass (must keep)
            true,
            // s1 ["", ""]  ==> some rows could pass (must keep)
            true,
            // s1 ["AB", "A\u{10ffff}\u{10ffff}\u{10ffff}"]  ==> some rows could pass (must keep)
            true,
            // s1 ["A\u{10ffff}\u{10ffff}", "A\u{10ffff}\u{10ffff}"]  ==> some rows could pass (must keep)
            true,
        ];
        prune_with_expr(expr, &schema, &statistics, expected_ret);

        let expr = col("s1").not_like(lit("A\\%%"));
        let statistics = TestStatistics::new().with(
            "s1",
            ContainerStats::new_utf8(
                vec![Some("A%a"), Some("A")],
                vec![Some("A%c"), Some("A")],
            ),
        );
        let expected_ret = &[false, true];
        prune_with_expr(expr, &schema, &statistics, expected_ret);
    }

    #[test]
    fn test_rewrite_expr_to_prunable() {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
        let df_schema = DFSchema::try_from(schema.clone()).unwrap();

        // column op lit
        let left_input = col("a");
        let left_input = logical2physical(&left_input, &schema);
        let right_input = lit(ScalarValue::Int32(Some(12)));
        let right_input = logical2physical(&right_input, &schema);
        let (result_left, _, result_right) = rewrite_expr_to_prunable(
            &left_input,
            Operator::Eq,
            &right_input,
            df_schema.clone(),
        )
        .unwrap();
        assert_eq!(result_left.to_string(), left_input.to_string());
        assert_eq!(result_right.to_string(), right_input.to_string());

        // cast op lit
        let left_input = cast(col("a"), DataType::Decimal128(20, 3));
        let left_input = logical2physical(&left_input, &schema);
        let right_input = lit(ScalarValue::Decimal128(Some(12), 20, 3));
        let right_input = logical2physical(&right_input, &schema);
        let (result_left, _, result_right) = rewrite_expr_to_prunable(
            &left_input,
            Operator::Gt,
            &right_input,
            df_schema.clone(),
        )
        .unwrap();
        assert_eq!(result_left.to_string(), left_input.to_string());
        assert_eq!(result_right.to_string(), right_input.to_string());

        // try_cast op lit
        let left_input = try_cast(col("a"), DataType::Int64);
        let left_input = logical2physical(&left_input, &schema);
        let right_input = lit(ScalarValue::Int64(Some(12)));
        let right_input = logical2physical(&right_input, &schema);
        let (result_left, _, result_right) =
            rewrite_expr_to_prunable(&left_input, Operator::Gt, &right_input, df_schema)
                .unwrap();
        assert_eq!(result_left.to_string(), left_input.to_string());
        assert_eq!(result_right.to_string(), right_input.to_string());

        // TODO: add test for other case and op
    }

    #[test]
    fn test_rewrite_expr_to_prunable_custom_unhandled_hook() {
        struct CustomUnhandledHook;

        impl UnhandledPredicateHook for CustomUnhandledHook {
            /// This handles an arbitrary case of a column that doesn't exist in the schema
            /// by renaming it to yet another column that doesn't exist in the schema
            /// (the transformation is arbitrary, the point is that it can do whatever it wants)
            fn handle(&self, _expr: &Arc<dyn PhysicalExpr>) -> Arc<dyn PhysicalExpr> {
                Arc::new(phys_expr::Literal::new(ScalarValue::Int32(Some(42))))
            }
        }

        let schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
        let schema_with_b = Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
        ]);

        let rewriter = PredicateRewriter::new()
            .with_unhandled_hook(Arc::new(CustomUnhandledHook {}));

        let transform_expr = |expr| {
            let expr = logical2physical(&expr, &schema_with_b);
            rewriter.rewrite_predicate_to_statistics_predicate(&expr, &schema)
        };

        // transform an arbitrary valid expression that we know is handled
        let known_expression = col("a").eq(lit(12));
        let known_expression_transformed = PredicateRewriter::new()
            .rewrite_predicate_to_statistics_predicate(
                &logical2physical(&known_expression, &schema),
                &schema,
            );

        // an expression referencing an unknown column (that is not in the schema) gets passed to the hook
        let input = col("b").eq(lit(12));
        let expected = logical2physical(&lit(42), &schema);
        let transformed = transform_expr(input.clone());
        assert_eq!(transformed.to_string(), expected.to_string());

        // more complex case with unknown column
        let input = known_expression.clone().and(input.clone());
        let expected = phys_expr::BinaryExpr::new(
            Arc::<dyn PhysicalExpr>::clone(&known_expression_transformed),
            Operator::And,
            logical2physical(&lit(42), &schema),
        );
        let transformed = transform_expr(input.clone());
        assert_eq!(transformed.to_string(), expected.to_string());

        // an unknown expression gets passed to the hook
        let input = array_has(make_array(vec![lit(1)]), col("a"));
        let expected = logical2physical(&lit(42), &schema);
        let transformed = transform_expr(input.clone());
        assert_eq!(transformed.to_string(), expected.to_string());

        // more complex case with unknown expression
        let input = known_expression.and(input);
        let expected = phys_expr::BinaryExpr::new(
            Arc::<dyn PhysicalExpr>::clone(&known_expression_transformed),
            Operator::And,
            logical2physical(&lit(42), &schema),
        );
        let transformed = transform_expr(input.clone());
        assert_eq!(transformed.to_string(), expected.to_string());
    }

    #[test]
    fn test_rewrite_expr_to_prunable_error() {
        // cast string value to numeric value
        // this cast is not supported
        let schema = Schema::new(vec![Field::new("a", DataType::Utf8, true)]);
        let df_schema = DFSchema::try_from(schema.clone()).unwrap();
        let left_input = cast(col("a"), DataType::Int64);
        let left_input = logical2physical(&left_input, &schema);
        let right_input = lit(ScalarValue::Int64(Some(12)));
        let right_input = logical2physical(&right_input, &schema);
        let result = rewrite_expr_to_prunable(
            &left_input,
            Operator::Gt,
            &right_input,
            df_schema.clone(),
        );
        assert!(result.is_err());

        // other expr
        let left_input = is_null(col("a"));
        let left_input = logical2physical(&left_input, &schema);
        let right_input = lit(ScalarValue::Int64(Some(12)));
        let right_input = logical2physical(&right_input, &schema);
        let result =
            rewrite_expr_to_prunable(&left_input, Operator::Gt, &right_input, df_schema);
        assert!(result.is_err());
        // TODO: add other negative test for other case and op
    }

    #[test]
    fn prune_with_contained_one_column() {
        let schema = Arc::new(Schema::new(vec![Field::new("s1", DataType::Utf8, true)]));

        // Model having information like a bloom filter for s1
        let statistics = TestStatistics::new()
            .with_contained(
                "s1",
                [ScalarValue::from("foo")],
                [
                    // container 0 known to only contain "foo"",
                    Some(true),
                    // container 1 known to not contain "foo"
                    Some(false),
                    // container 2 unknown about "foo"
                    None,
                    // container 3 known to only contain "foo"
                    Some(true),
                    // container 4 known to not contain "foo"
                    Some(false),
                    // container 5 unknown about "foo"
                    None,
                    // container 6 known to only contain "foo"
                    Some(true),
                    // container 7 known to not contain "foo"
                    Some(false),
                    // container 8 unknown about "foo"
                    None,
                ],
            )
            .with_contained(
                "s1",
                [ScalarValue::from("bar")],
                [
                    // containers 0,1,2 known to only contain "bar"
                    Some(true),
                    Some(true),
                    Some(true),
                    // container 3,4,5 known to not contain "bar"
                    Some(false),
                    Some(false),
                    Some(false),
                    // container 6,7,8 unknown about "bar"
                    None,
                    None,
                    None,
                ],
            )
            .with_contained(
                // the way the tests are setup, this data is
                // consulted if the "foo" and "bar" are being checked at the same time
                "s1",
                [ScalarValue::from("foo"), ScalarValue::from("bar")],
                [
                    // container 0,1,2 unknown about ("foo, "bar")
                    None,
                    None,
                    None,
                    // container 3,4,5 known to contain only either "foo" and "bar"
                    Some(true),
                    Some(true),
                    Some(true),
                    // container 6,7,8  known to contain  neither "foo" and "bar"
                    Some(false),
                    Some(false),
                    Some(false),
                ],
            );

        // s1 = 'foo'
        prune_with_expr(
            col("s1").eq(lit("foo")),
            &schema,
            &statistics,
            // rule out containers ('false) where we know foo is not present
            &[true, false, true, true, false, true, true, false, true],
        );

        // s1 = 'bar'
        prune_with_expr(
            col("s1").eq(lit("bar")),
            &schema,
            &statistics,
            // rule out containers where we know bar is not present
            &[true, true, true, false, false, false, true, true, true],
        );

        // s1 = 'baz' (unknown value)
        prune_with_expr(
            col("s1").eq(lit("baz")),
            &schema,
            &statistics,
            // can't rule out anything
            &[true, true, true, true, true, true, true, true, true],
        );

        // s1 = 'foo' AND s1 = 'bar'
        prune_with_expr(
            col("s1").eq(lit("foo")).and(col("s1").eq(lit("bar"))),
            &schema,
            &statistics,
            // logically this predicate can't possibly be true (the column can't
            // take on both values) but we could rule it out if the stats tell
            // us that both values are not present
            &[true, true, true, true, true, true, true, true, true],
        );

        // s1 = 'foo' OR s1 = 'bar'
        prune_with_expr(
            col("s1").eq(lit("foo")).or(col("s1").eq(lit("bar"))),
            &schema,
            &statistics,
            // can rule out containers that we know contain neither foo nor bar
            &[true, true, true, true, true, true, false, false, false],
        );

        // s1 = 'foo' OR s1 = 'baz'
        prune_with_expr(
            col("s1").eq(lit("foo")).or(col("s1").eq(lit("baz"))),
            &schema,
            &statistics,
            // can't rule out anything container
            &[true, true, true, true, true, true, true, true, true],
        );

        // s1 = 'foo' OR s1 = 'bar' OR s1 = 'baz'
        prune_with_expr(
            col("s1")
                .eq(lit("foo"))
                .or(col("s1").eq(lit("bar")))
                .or(col("s1").eq(lit("baz"))),
            &schema,
            &statistics,
            // can rule out any containers based on knowledge of s1 and `foo`,
            // `bar` and (`foo`, `bar`)
            &[true, true, true, true, true, true, true, true, true],
        );

        // s1 != foo
        prune_with_expr(
            col("s1").not_eq(lit("foo")),
            &schema,
            &statistics,
            // rule out containers we know for sure only contain foo
            &[false, true, true, false, true, true, false, true, true],
        );

        // s1 != bar
        prune_with_expr(
            col("s1").not_eq(lit("bar")),
            &schema,
            &statistics,
            // rule out when we know for sure s1 has the value bar
            &[false, false, false, true, true, true, true, true, true],
        );

        // s1 != foo AND s1 != bar
        prune_with_expr(
            col("s1")
                .not_eq(lit("foo"))
                .and(col("s1").not_eq(lit("bar"))),
            &schema,
            &statistics,
            // can rule out any container where we know s1 does not have either 'foo' or 'bar'
            &[true, true, true, false, false, false, true, true, true],
        );

        // s1 != foo AND s1 != bar AND s1 != baz
        prune_with_expr(
            col("s1")
                .not_eq(lit("foo"))
                .and(col("s1").not_eq(lit("bar")))
                .and(col("s1").not_eq(lit("baz"))),
            &schema,
            &statistics,
            // can't rule out any container based on  knowledge of s1,s2
            &[true, true, true, true, true, true, true, true, true],
        );

        // s1 != foo OR s1 != bar
        prune_with_expr(
            col("s1")
                .not_eq(lit("foo"))
                .or(col("s1").not_eq(lit("bar"))),
            &schema,
            &statistics,
            // cant' rule out anything based on contains information
            &[true, true, true, true, true, true, true, true, true],
        );

        // s1 != foo OR s1 != bar OR s1 != baz
        prune_with_expr(
            col("s1")
                .not_eq(lit("foo"))
                .or(col("s1").not_eq(lit("bar")))
                .or(col("s1").not_eq(lit("baz"))),
            &schema,
            &statistics,
            // cant' rule out anything based on contains information
            &[true, true, true, true, true, true, true, true, true],
        );
    }

    #[test]
    fn prune_with_contained_two_columns() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("s1", DataType::Utf8, true),
            Field::new("s2", DataType::Utf8, true),
        ]));

        // Model having information like bloom filters for s1 and s2
        let statistics = TestStatistics::new()
            .with_contained(
                "s1",
                [ScalarValue::from("foo")],
                [
                    // container 0, s1 known to only contain "foo"",
                    Some(true),
                    // container 1, s1 known to not contain "foo"
                    Some(false),
                    // container 2, s1 unknown about "foo"
                    None,
                    // container 3, s1 known to only contain "foo"
                    Some(true),
                    // container 4, s1 known to not contain "foo"
                    Some(false),
                    // container 5, s1 unknown about "foo"
                    None,
                    // container 6, s1 known to only contain "foo"
                    Some(true),
                    // container 7, s1 known to not contain "foo"
                    Some(false),
                    // container 8, s1 unknown about "foo"
                    None,
                ],
            )
            .with_contained(
                "s2", // for column s2
                [ScalarValue::from("bar")],
                [
                    // containers 0,1,2 s2 known to only contain "bar"
                    Some(true),
                    Some(true),
                    Some(true),
                    // container 3,4,5 s2 known to not contain "bar"
                    Some(false),
                    Some(false),
                    Some(false),
                    // container 6,7,8 s2 unknown about "bar"
                    None,
                    None,
                    None,
                ],
            );

        // s1 = 'foo'
        prune_with_expr(
            col("s1").eq(lit("foo")),
            &schema,
            &statistics,
            // rule out containers where we know s1 is not present
            &[true, false, true, true, false, true, true, false, true],
        );

        // s1 = 'foo' OR s2 = 'bar'
        let expr = col("s1").eq(lit("foo")).or(col("s2").eq(lit("bar")));
        prune_with_expr(
            expr,
            &schema,
            &statistics,
            //  can't rule out any container (would need to prove that s1 != foo AND s2 != bar)
            &[true, true, true, true, true, true, true, true, true],
        );

        // s1 = 'foo' AND s2 != 'bar'
        prune_with_expr(
            col("s1").eq(lit("foo")).and(col("s2").not_eq(lit("bar"))),
            &schema,
            &statistics,
            // can only rule out container where we know either:
            // 1. s1 doesn't have the value 'foo` or
            // 2. s2 has only the value of 'bar'
            &[false, false, false, true, false, true, true, false, true],
        );

        // s1 != 'foo' AND s2 != 'bar'
        prune_with_expr(
            col("s1")
                .not_eq(lit("foo"))
                .and(col("s2").not_eq(lit("bar"))),
            &schema,
            &statistics,
            // Can  rule out any container where we know either
            // 1. s1 has only the value 'foo'
            // 2. s2 has only the value 'bar'
            &[false, false, false, false, true, true, false, true, true],
        );

        // s1 != 'foo' AND (s2 = 'bar' OR s2 = 'baz')
        prune_with_expr(
            col("s1")
                .not_eq(lit("foo"))
                .and(col("s2").eq(lit("bar")).or(col("s2").eq(lit("baz")))),
            &schema,
            &statistics,
            // Can rule out any container where we know s1 has only the value
            // 'foo'. Can't use knowledge of s2 and bar to rule out anything
            &[false, true, true, false, true, true, false, true, true],
        );

        // s1 like '%foo%bar%'
        prune_with_expr(
            col("s1").like(lit("foo%bar%")),
            &schema,
            &statistics,
            // cant rule out anything with information we know
            &[true, true, true, true, true, true, true, true, true],
        );

        // s1 like '%foo%bar%' AND s2 = 'bar'
        prune_with_expr(
            col("s1")
                .like(lit("foo%bar%"))
                .and(col("s2").eq(lit("bar"))),
            &schema,
            &statistics,
            // can rule out any container where we know s2 does not have the value 'bar'
            &[true, true, true, false, false, false, true, true, true],
        );

        // s1 like '%foo%bar%' OR s2 = 'bar'
        prune_with_expr(
            col("s1").like(lit("foo%bar%")).or(col("s2").eq(lit("bar"))),
            &schema,
            &statistics,
            // can't rule out anything (we would have to prove that both the
            // like and the equality must be false)
            &[true, true, true, true, true, true, true, true, true],
        );
    }

    #[test]
    fn prune_with_range_and_contained() {
        // Setup mimics range information for i, a bloom filter for s
        let schema = Arc::new(Schema::new(vec![
            Field::new("i", DataType::Int32, true),
            Field::new("s", DataType::Utf8, true),
        ]));

        let statistics = TestStatistics::new()
            .with(
                "i",
                ContainerStats::new_i32(
                    // Container 0, 3, 6: [-5 to 5]
                    // Container 1, 4, 7: [10 to 20]
                    // Container 2, 5, 9: unknown
                    vec![
                        Some(-5),
                        Some(10),
                        None,
                        Some(-5),
                        Some(10),
                        None,
                        Some(-5),
                        Some(10),
                        None,
                    ], // min
                    vec![
                        Some(5),
                        Some(20),
                        None,
                        Some(5),
                        Some(20),
                        None,
                        Some(5),
                        Some(20),
                        None,
                    ], // max
                ),
            )
            // Add contained  information about the s and "foo"
            .with_contained(
                "s",
                [ScalarValue::from("foo")],
                [
                    // container 0,1,2 known to only contain "foo"
                    Some(true),
                    Some(true),
                    Some(true),
                    // container 3,4,5 known to not contain "foo"
                    Some(false),
                    Some(false),
                    Some(false),
                    // container 6,7,8 unknown about "foo"
                    None,
                    None,
                    None,
                ],
            );

        // i = 0 and s = 'foo'
        prune_with_expr(
            col("i").eq(lit(0)).and(col("s").eq(lit("foo"))),
            &schema,
            &statistics,
            // Can rule out container where we know that either:
            // 1. 0 is outside the min/max range of i
            // 1. s does not contain foo
            // (range is false, and contained  is false)
            &[true, false, true, false, false, false, true, false, true],
        );

        // i = 0 and s != 'foo'
        prune_with_expr(
            col("i").eq(lit(0)).and(col("s").not_eq(lit("foo"))),
            &schema,
            &statistics,
            // Can rule out containers where either:
            // 1. 0 is outside the min/max range of i
            // 2. s only contains foo
            &[false, false, false, true, false, true, true, false, true],
        );

        // i = 0 OR s = 'foo'
        prune_with_expr(
            col("i").eq(lit(0)).or(col("s").eq(lit("foo"))),
            &schema,
            &statistics,
            // in theory could rule out containers if we had min/max values for
            // s as well. But in this case we don't so we can't rule out anything
            &[true, true, true, true, true, true, true, true, true],
        );
    }

    /// prunes the specified expr with the specified schema and statistics, and
    /// ensures it returns expected.
    ///
    /// `expected` is a vector of bools, where true means the row group should
    /// be kept, and false means it should be pruned.
    // TODO refactor other tests to use this to reduce boiler plate
    fn prune_with_expr(
        expr: Expr,
        schema: &SchemaRef,
        statistics: &TestStatistics,
        expected: &[bool],
    ) {
        println!("Pruning with expr: {expr}");
        let expr = logical2physical(&expr, schema);
        let p = PruningPredicate::try_new(expr, Arc::<Schema>::clone(schema)).unwrap();
        let result = p.prune(statistics).unwrap();
        assert_eq!(result, expected);
    }

    fn test_build_predicate_expression(
        expr: &Expr,
        schema: &Schema,
        required_columns: &mut RequiredColumns,
    ) -> Arc<dyn PhysicalExpr> {
        let expr = logical2physical(expr, schema);
        let unhandled_hook = Arc::new(ConstantUnhandledPredicateHook::default()) as _;
        build_predicate_expression(
            &expr,
            &Arc::new(schema.clone()),
            required_columns,
            &unhandled_hook,
        )
    }

    #[test]
    fn test_build_predicate_expression_with_false() {
        let expr = lit(ScalarValue::Boolean(Some(false)));
        let schema = Schema::empty();
        let res =
            test_build_predicate_expression(&expr, &schema, &mut RequiredColumns::new());
        let expected = logical2physical(&expr, &schema);
        assert_eq!(&res, &expected);
    }

    #[test]
    fn test_build_predicate_expression_with_and_false() {
        let schema = Schema::new(vec![Field::new("c1", DataType::Utf8View, false)]);
        let expr = and(
            col("c1").eq(lit("a")),
            lit(ScalarValue::Boolean(Some(false))),
        );
        let res =
            test_build_predicate_expression(&expr, &schema, &mut RequiredColumns::new());
        let expected = logical2physical(&lit(ScalarValue::Boolean(Some(false))), &schema);
        assert_eq!(&res, &expected);
    }

    #[test]
    fn test_build_predicate_expression_with_or_false() {
        let schema = Schema::new(vec![Field::new("c1", DataType::Utf8View, false)]);
        let left_expr = col("c1").eq(lit("a"));
        let right_expr = lit(ScalarValue::Boolean(Some(false)));
        let res = test_build_predicate_expression(
            &or(left_expr.clone(), right_expr.clone()),
            &schema,
            &mut RequiredColumns::new(),
        );
        let expected =
            "c1_null_count@2 != row_count@3 AND c1_min@0 <= a AND a <= c1_max@1";
        assert_eq!(res.to_string(), expected);
    }
}
