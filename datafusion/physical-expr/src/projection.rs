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

//! [`ProjectionExpr`] and [`ProjectionExprs`] for representing projections.

use std::ops::Deref;
use std::sync::Arc;

use crate::PhysicalExpr;
use crate::expressions::{Column, Literal};
use crate::utils::collect_columns;

use arrow::array::{RecordBatch, RecordBatchOptions};
use arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion_common::stats::{ColumnStatistics, Precision};
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::{
    Result, ScalarValue, assert_or_internal_err, internal_datafusion_err, plan_err,
};

use datafusion_physical_expr_common::metrics::ExecutionPlanMetricsSet;
use datafusion_physical_expr_common::metrics::ExpressionEvaluatorMetrics;
use datafusion_physical_expr_common::physical_expr::fmt_sql;
use datafusion_physical_expr_common::sort_expr::{LexOrdering, PhysicalSortExpr};
use datafusion_physical_expr_common::utils::evaluate_expressions_to_arrays_with_metrics;
use hashbrown::HashSet;
use indexmap::IndexMap;
use itertools::Itertools;

/// An expression used by projection operations.
///
/// The expression is evaluated and the result is stored in a column
/// with the name specified by `alias`.
///
/// For example, the SQL expression `a + b AS sum_ab` would be represented
/// as a `ProjectionExpr` where `expr` is the expression `a + b`
/// and `alias` is the string `sum_ab`.
///
/// See [`ProjectionExprs`] for a collection of projection expressions.
#[derive(Debug, Clone)]
pub struct ProjectionExpr {
    /// The expression that will be evaluated.
    pub expr: Arc<dyn PhysicalExpr>,
    /// The name of the output column for use an output schema.
    pub alias: String,
}

impl PartialEq for ProjectionExpr {
    fn eq(&self, other: &Self) -> bool {
        let ProjectionExpr { expr, alias } = self;
        expr.eq(&other.expr) && *alias == other.alias
    }
}

impl Eq for ProjectionExpr {}

impl std::fmt::Display for ProjectionExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.expr.to_string() == self.alias {
            write!(f, "{}", self.alias)
        } else {
            write!(f, "{} AS {}", self.expr, self.alias)
        }
    }
}

impl ProjectionExpr {
    /// Create a new projection expression
    pub fn new(expr: Arc<dyn PhysicalExpr>, alias: impl Into<String>) -> Self {
        let alias = alias.into();
        Self { expr, alias }
    }

    /// Create a new projection expression from an expression and a schema using the expression's output field name as alias.
    pub fn new_from_expression(
        expr: Arc<dyn PhysicalExpr>,
        schema: &Schema,
    ) -> Result<Self> {
        let field = expr.return_field(schema)?;
        Ok(Self {
            expr,
            alias: field.name().to_string(),
        })
    }
}

impl From<(Arc<dyn PhysicalExpr>, String)> for ProjectionExpr {
    fn from(value: (Arc<dyn PhysicalExpr>, String)) -> Self {
        Self::new(value.0, value.1)
    }
}

impl From<&(Arc<dyn PhysicalExpr>, String)> for ProjectionExpr {
    fn from(value: &(Arc<dyn PhysicalExpr>, String)) -> Self {
        Self::new(Arc::clone(&value.0), value.1.clone())
    }
}

impl From<ProjectionExpr> for (Arc<dyn PhysicalExpr>, String) {
    fn from(value: ProjectionExpr) -> Self {
        (value.expr, value.alias)
    }
}

/// A collection of  [`ProjectionExpr`] instances, representing a complete
/// projection operation.
///
/// Projection operations are used in query plans to select specific columns or
/// compute new columns based on existing ones.
///
/// See [`ProjectionExprs::from_indices`] to select a subset of columns by
/// indices.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionExprs {
    exprs: Vec<ProjectionExpr>,
}

/// Classification of how beneficial a projection expression is for pushdown.
///
/// This is used to determine whether an expression should be pushed down
/// below other operators in the query plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PushdownBenefit {
    /// Field accessors - reduce data size, should be pushed down.
    /// Examples: `struct_col['field']`, nested field access.
    Beneficial,
    /// Column references - neutral, can be pushed if needed.
    /// Examples: `col_a`, `col_b@1`
    Neutral,
    /// Literals and computed expressions - add data, should NOT be pushed down.
    /// Examples: `42`, `'hello'`, `a + b`
    NonBeneficial,
}

/// Result of splitting a projection for optimized pushdown.
///
/// When a projection contains a mix of beneficial and non-beneficial expressions,
/// we can split it into two projections:
/// - `inner`: Pushed down below operators (contains beneficial exprs + needed columns)
/// - `outer`: Stays above operators (references inner outputs + adds non-beneficial exprs)
///
/// Example:
/// ```text
/// -- Original:
/// Projection: struct['a'] AS f1, 42 AS const, col_b
///   FilterExec: predicate
///
/// -- After splitting:
/// Projection: f1, 42 AS const, col_b       <- outer (keeps literal, refs inner)
///   Projection: struct['a'] AS f1, col_b   <- inner (pushed down)
///     FilterExec: predicate
/// ```
#[derive(Debug, Clone)]
pub struct ProjectionSplit {
    /// The inner projection to be pushed down (beneficial exprs + columns needed by outer)
    pub inner: ProjectionExprs,
    /// The outer projection to keep above (refs to inner outputs + non-beneficial exprs)
    pub outer: ProjectionExprs,
}

impl std::fmt::Display for ProjectionExprs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let exprs: Vec<String> = self.exprs.iter().map(|e| e.to_string()).collect();
        write!(f, "Projection[{}]", exprs.join(", "))
    }
}

impl From<Vec<ProjectionExpr>> for ProjectionExprs {
    fn from(value: Vec<ProjectionExpr>) -> Self {
        Self { exprs: value }
    }
}

impl From<&[ProjectionExpr]> for ProjectionExprs {
    fn from(value: &[ProjectionExpr]) -> Self {
        Self {
            exprs: value.to_vec(),
        }
    }
}

impl FromIterator<ProjectionExpr> for ProjectionExprs {
    fn from_iter<T: IntoIterator<Item = ProjectionExpr>>(exprs: T) -> Self {
        Self {
            exprs: exprs.into_iter().collect::<Vec<_>>(),
        }
    }
}

impl AsRef<[ProjectionExpr]> for ProjectionExprs {
    fn as_ref(&self) -> &[ProjectionExpr] {
        &self.exprs
    }
}

impl ProjectionExprs {
    pub fn new<I>(exprs: I) -> Self
    where
        I: IntoIterator<Item = ProjectionExpr>,
    {
        Self {
            exprs: exprs.into_iter().collect::<Vec<_>>(),
        }
    }

    /// Creates a [`ProjectionExpr`] from a list of column indices.
    ///
    /// This is a convenience method for creating simple column-only projections, where each projection expression is a reference to a column
    /// in the input schema.
    ///
    /// # Behavior
    /// - Ordering: the output projection preserves the exact order of indices provided in the input slice
    ///   For example, `[2, 0, 1]` will produce projections for columns 2, 0, then 1 in that order
    /// - Duplicates: Duplicate indices are allowed and will create multiple projection expressions referencing the same source column
    ///   For example, `[0, 0]` creates 2 separate projections both referencing column 0
    ///
    /// # Panics
    /// Panics if any index in `indices` is out of bounds for the provided schema.
    ///
    /// # Example
    ///
    /// ```rust
    /// use arrow::datatypes::{DataType, Field, Schema};
    /// use datafusion_physical_expr::projection::ProjectionExprs;
    /// use std::sync::Arc;
    ///
    /// // Create a schema with three columns
    /// let schema = Arc::new(Schema::new(vec![
    ///     Field::new("a", DataType::Int32, false),
    ///     Field::new("b", DataType::Utf8, false),
    ///     Field::new("c", DataType::Float64, false),
    /// ]));
    ///
    /// // Project columns at indices 2 and 0 (c and a) - ordering is preserved
    /// let projection = ProjectionExprs::from_indices(&[2, 0], &schema);
    ///
    /// // This creates: SELECT c@2 AS c, a@0 AS a
    /// assert_eq!(projection.as_ref().len(), 2);
    /// assert_eq!(projection.as_ref()[0].alias, "c");
    /// assert_eq!(projection.as_ref()[1].alias, "a");
    ///
    /// // Duplicate indices are allowed
    /// let projection_with_dups = ProjectionExprs::from_indices(&[0, 0, 1], &schema);
    /// assert_eq!(projection_with_dups.as_ref().len(), 3);
    /// assert_eq!(projection_with_dups.as_ref()[0].alias, "a");
    /// assert_eq!(projection_with_dups.as_ref()[1].alias, "a"); // duplicate
    /// assert_eq!(projection_with_dups.as_ref()[2].alias, "b");
    /// ```
    pub fn from_indices(indices: &[usize], schema: &Schema) -> Self {
        let projection_exprs = indices.iter().map(|&i| {
            let field = schema.field(i);
            ProjectionExpr {
                expr: Arc::new(Column::new(field.name(), i)),
                alias: field.name().clone(),
            }
        });

        Self::from_iter(projection_exprs)
    }

    /// Returns an iterator over the projection expressions
    pub fn iter(&self) -> impl Iterator<Item = &ProjectionExpr> {
        self.exprs.iter()
    }

    /// Checks if all of the projection expressions are trivial.
    pub fn is_trivial(&self) -> bool {
        self.exprs.iter().all(|p| p.expr.is_trivial())
    }

    /// Classifies a single expression for pushdown benefit.
    ///
    /// - Literals are `NonBeneficial` (they add data)
    /// - Non-trivial expressions are `NonBeneficial` (they add computation)
    /// - Column references are `Neutral` (no cost/benefit)
    /// - Trivial non-column expressions (e.g., field accessors) are `Beneficial`
    fn classify_expr(expr: &Arc<dyn PhysicalExpr>) -> PushdownBenefit {
        // Literals add data size downstream - don't push
        if expr.as_any().is::<Literal>() {
            return PushdownBenefit::NonBeneficial;
        }
        // Column references are neutral
        if expr.as_any().is::<Column>() {
            return PushdownBenefit::Neutral;
        }

        if expr.is_trivial() {
            // Trivial non-column expressions (field accessors) reduce data - push
            PushdownBenefit::Beneficial
        } else {
            // Any other expression is considered non-beneficial
            PushdownBenefit::NonBeneficial
        }
    }

    /// Check if this projection benefits from being pushed down lower in the plan.
    ///
    /// The goal is to push down projections that reduce the amount of data processed
    /// by subsequent operations and are "compute neutral" (i.e., do not add computation overhead).
    ///
    /// Some "compute neutral" projections include:
    /// - Dropping unneeded columns (e.g., `SELECT a, b` from `a, b, c`). Subsequent filters or joins
    ///   now process fewer columns.
    /// - Struct field access (e.g., `SELECT struct_col.field1 AS f1`). This reduces the data size
    ///   processed downstream and is a cheap reference / metadata only clone of the inner array.
    ///
    /// Examples of projections that do NOT benefit from pushdown:
    /// - Literal projections (e.g., `SELECT 42 AS const_col`). These add constant columns
    ///   that increase data size downstream.
    /// - Computed expressions (e.g., `SELECT a + b AS sum_ab`). These add computation overhead
    ///   and may increase data size downstream, so they should be applied after filters or joins.
    pub fn benefits_from_pushdown(&self) -> bool {
        // All expressions must be trivial for pushdown to be beneficial
        if !self.is_trivial() {
            // Contains computed expressions, function calls, etc. - do not push down
            return false;
        }

        // Check if all expressions are just columns or literals (no field accessors)
        // If so, there's no benefit to pushing down because:
        // - Columns are just references (no data reduction)
        // - Literals add data (definitely don't want to push down)
        let all_columns_or_literals = self
            .exprs
            .iter()
            .all(|p| p.expr.as_any().is::<Column>() || p.expr.as_any().is::<Literal>());

        // Only benefit from pushdown if we have field accessors (or other beneficial trivial exprs)
        !all_columns_or_literals
    }

    /// Determines whether this projection should be pushed through an operator.
    ///
    /// A projection should be pushed through when it is:
    /// 1. Trivial (no expensive computations to duplicate)
    /// 2. AND provides some benefit:
    ///    - Either narrows the schema (fewer output columns than input columns)
    ///    - Or has beneficial expressions like field accessors that reduce data size
    ///    - Or has literal expressions that can be absorbed by the datasource
    ///
    /// Column-only projections that just rename without narrowing the schema are NOT
    /// pushed through, as they provide no benefit.
    ///
    /// # Arguments
    /// * `input_field_count` - Number of fields in the input schema
    pub fn should_push_through_operator(&self, input_field_count: usize) -> bool {
        // Must be trivial (no expensive computations)
        if !self.is_trivial() {
            return false;
        }

        // Must provide some benefit:
        // - Either narrows schema (fewer output columns than input columns)
        // - Or has field accessors that reduce data size
        // - Or has literals that can be absorbed by datasource
        let narrows_schema = self.exprs.len() < input_field_count;
        let has_beneficial_exprs = self.benefits_from_pushdown();
        let has_literals = self.exprs.iter().any(|p| p.expr.as_any().is::<Literal>());

        narrows_schema || has_beneficial_exprs || has_literals
    }

    /// Attempts to split this projection into beneficial and non-beneficial parts.
    ///
    /// When a projection contains both beneficial expressions (field accessors) and
    /// non-beneficial expressions (literals), this method splits it so that the
    /// beneficial parts can be pushed down while non-beneficial parts stay above.
    ///
    /// # Returns
    /// - `Ok(Some(split))` - The projection was split successfully
    /// - `Ok(None)` - No split needed (all expressions are the same category)
    ///
    /// # Arguments
    /// * `input_schema` - The schema of the input to this projection
    pub fn split_for_pushdown(
        &self,
        input_schema: &Schema,
    ) -> Result<Option<ProjectionSplit>> {
        // Classify all expressions
        let classifications: Vec<_> = self
            .exprs
            .iter()
            .map(|p| (p, Self::classify_expr(&p.expr)))
            .collect();

        let has_beneficial = classifications
            .iter()
            .any(|(_, c)| *c == PushdownBenefit::Beneficial);
        let has_non_beneficial = classifications
            .iter()
            .any(|(_, c)| *c == PushdownBenefit::NonBeneficial);

        // If no beneficial expressions, nothing to push down
        if !has_beneficial {
            return Ok(None);
        }

        // If no non-beneficial expressions, push the entire projection (no split needed)
        if !has_non_beneficial {
            return Ok(None);
        }

        // We need to split: beneficial + columns needed by non-beneficial go to inner,
        // references to inner + non-beneficial expressions go to outer

        // Collect columns needed by non-beneficial expressions
        let mut columns_needed_by_outer: HashSet<usize> = HashSet::new();
        for (proj, class) in &classifications {
            if *class == PushdownBenefit::NonBeneficial {
                for col in collect_columns(&proj.expr) {
                    columns_needed_by_outer.insert(col.index());
                }
            }
        }

        // Build inner projection: beneficial exprs + columns needed by outer
        let mut inner_exprs: Vec<ProjectionExpr> = Vec::new();
        // Track where each original expression ends up in inner projection
        // Maps original index -> inner index (if it goes to inner)
        let mut original_to_inner: IndexMap<usize, usize> = IndexMap::new();

        // First add beneficial expressions
        for (orig_idx, (proj, class)) in classifications.iter().enumerate() {
            if *class == PushdownBenefit::Beneficial || *class == PushdownBenefit::Neutral
            {
                original_to_inner.insert(orig_idx, inner_exprs.len());
                inner_exprs.push((*proj).clone());
            }
        }

        // Add columns needed by non-beneficial expressions (if not already present)
        // Build mapping from input column index -> inner projection index
        let mut col_index_to_inner: IndexMap<usize, usize> = IndexMap::new();
        for (proj_idx, (proj, _)) in classifications.iter().enumerate() {
            if let Some(col) = proj.expr.as_any().downcast_ref::<Column>()
                && let Some(&inner_idx) = original_to_inner.get(&proj_idx)
            {
                col_index_to_inner.insert(col.index(), inner_idx);
            }
        }

        // Track columns we need to add to inner for outer's non-beneficial exprs
        for col_idx in &columns_needed_by_outer {
            if !col_index_to_inner.contains_key(col_idx) {
                // Add this column to inner
                let field = input_schema.field(*col_idx);
                let col_expr = ProjectionExpr::new(
                    Arc::new(Column::new(field.name(), *col_idx)),
                    field.name().clone(),
                );
                col_index_to_inner.insert(*col_idx, inner_exprs.len());
                inner_exprs.push(col_expr);
            }
        }

        // Build inner schema (for rewriting outer expressions)
        let inner_schema = self.build_schema_for_exprs(&inner_exprs, input_schema)?;

        // Build outer projection: references to inner outputs + non-beneficial exprs
        let mut outer_exprs: Vec<ProjectionExpr> = Vec::new();

        for (orig_idx, (proj, class)) in classifications.iter().enumerate() {
            match class {
                PushdownBenefit::Beneficial | PushdownBenefit::Neutral => {
                    // Reference the inner projection output
                    let inner_idx = original_to_inner[&orig_idx];
                    let col_expr = ProjectionExpr::new(
                        Arc::new(Column::new(&proj.alias, inner_idx)),
                        proj.alias.clone(),
                    );
                    outer_exprs.push(col_expr);
                }
                PushdownBenefit::NonBeneficial => {
                    // Keep the expression but rewrite column references to point to inner
                    let rewritten_expr = self.rewrite_columns_for_inner(
                        &proj.expr,
                        &col_index_to_inner,
                        &inner_schema,
                    )?;
                    outer_exprs
                        .push(ProjectionExpr::new(rewritten_expr, proj.alias.clone()));
                }
            }
        }

        Ok(Some(ProjectionSplit {
            inner: ProjectionExprs::new(inner_exprs),
            outer: ProjectionExprs::new(outer_exprs),
        }))
    }

    /// Helper to build a schema from projection expressions
    fn build_schema_for_exprs(
        &self,
        exprs: &[ProjectionExpr],
        input_schema: &Schema,
    ) -> Result<Schema> {
        let fields: Result<Vec<Field>> = exprs
            .iter()
            .map(|p| {
                let field = p.expr.return_field(input_schema)?;
                Ok(Field::new(
                    &p.alias,
                    field.data_type().clone(),
                    field.is_nullable(),
                ))
            })
            .collect();
        Ok(Schema::new(fields?))
    }

    /// Rewrite column references in an expression to point to inner projection outputs
    fn rewrite_columns_for_inner(
        &self,
        expr: &Arc<dyn PhysicalExpr>,
        col_index_to_inner: &IndexMap<usize, usize>,
        inner_schema: &Schema,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Arc::clone(expr)
            .transform(|e| {
                if let Some(col) = e.as_any().downcast_ref::<Column>()
                    && let Some(&inner_idx) = col_index_to_inner.get(&col.index())
                {
                    let inner_field = inner_schema.field(inner_idx);
                    return Ok(Transformed::yes(Arc::new(Column::new(
                        inner_field.name(),
                        inner_idx,
                    ))
                        as Arc<dyn PhysicalExpr>));
                }
                Ok(Transformed::no(e))
            })
            .data()
    }

    /// Creates a ProjectionMapping from this projection
    pub fn projection_mapping(
        &self,
        input_schema: &SchemaRef,
    ) -> Result<ProjectionMapping> {
        ProjectionMapping::try_new(
            self.exprs
                .iter()
                .map(|p| (Arc::clone(&p.expr), p.alias.clone())),
            input_schema,
        )
    }

    /// Iterate over a clone of the projection expressions.
    pub fn expr_iter(&self) -> impl Iterator<Item = Arc<dyn PhysicalExpr>> + '_ {
        self.exprs.iter().map(|e| Arc::clone(&e.expr))
    }

    /// Apply a fallible transformation to the [`PhysicalExpr`] of each projection.
    ///
    /// This method transforms the expression in each [`ProjectionExpr`] while preserving
    /// the alias. This is useful for rewriting expressions, such as when adapting
    /// expressions to a different schema.
    ///
    /// # Example
    ///
    /// ```rust
    /// use std::sync::Arc;
    /// use arrow::datatypes::{DataType, Field, Schema};
    /// use datafusion_common::Result;
    /// use datafusion_physical_expr::expressions::Column;
    /// use datafusion_physical_expr::projection::ProjectionExprs;
    /// use datafusion_physical_expr::PhysicalExpr;
    ///
    /// // Create a schema and projection
    /// let schema = Arc::new(Schema::new(vec![
    ///     Field::new("a", DataType::Int32, false),
    ///     Field::new("b", DataType::Int32, false),
    /// ]));
    /// let projection = ProjectionExprs::from_indices(&[0, 1], &schema);
    ///
    /// // Transform each expression (this example just clones them)
    /// let transformed = projection.try_map_exprs(|expr| Ok(expr))?;
    /// assert_eq!(transformed.as_ref().len(), 2);
    /// # Ok::<(), datafusion_common::DataFusionError>(())
    /// ```
    pub fn try_map_exprs<F>(self, mut f: F) -> Result<Self>
    where
        F: FnMut(Arc<dyn PhysicalExpr>) -> Result<Arc<dyn PhysicalExpr>>,
    {
        let exprs = self
            .exprs
            .into_iter()
            .map(|mut proj| {
                proj.expr = f(proj.expr)?;
                Ok(proj)
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self::new(exprs))
    }

    /// Apply another projection on top of this projection, returning the combined projection.
    /// For example, if this projection is `SELECT c@2 AS x, b@1 AS y, a@0 as z` and the other projection is `SELECT x@0 + 1 AS c1, y@1 + z@2 as c2`,
    /// we return a projection equivalent to `SELECT c@2 + 1 AS c1, b@1 + a@0 as c2`.
    ///
    /// # Example
    ///
    /// ```rust
    /// use datafusion_common::{Result, ScalarValue};
    /// use datafusion_expr::Operator;
    /// use datafusion_physical_expr::expressions::{BinaryExpr, Column, Literal};
    /// use datafusion_physical_expr::projection::{ProjectionExpr, ProjectionExprs};
    /// use std::sync::Arc;
    ///
    /// fn main() -> Result<()> {
    ///     // Example from the docstring:
    ///     // Base projection: SELECT c@2 AS x, b@1 AS y, a@0 AS z
    ///     let base = ProjectionExprs::new(vec![
    ///         ProjectionExpr {
    ///             expr: Arc::new(Column::new("c", 2)),
    ///             alias: "x".to_string(),
    ///         },
    ///         ProjectionExpr {
    ///             expr: Arc::new(Column::new("b", 1)),
    ///             alias: "y".to_string(),
    ///         },
    ///         ProjectionExpr {
    ///             expr: Arc::new(Column::new("a", 0)),
    ///             alias: "z".to_string(),
    ///         },
    ///     ]);
    ///
    ///     // Top projection: SELECT x@0 + 1 AS c1, y@1 + z@2 AS c2
    ///     let top = ProjectionExprs::new(vec![
    ///         ProjectionExpr {
    ///             expr: Arc::new(BinaryExpr::new(
    ///                 Arc::new(Column::new("x", 0)),
    ///                 Operator::Plus,
    ///                 Arc::new(Literal::new(ScalarValue::Int32(Some(1)))),
    ///             )),
    ///             alias: "c1".to_string(),
    ///         },
    ///         ProjectionExpr {
    ///             expr: Arc::new(BinaryExpr::new(
    ///                 Arc::new(Column::new("y", 1)),
    ///                 Operator::Plus,
    ///                 Arc::new(Column::new("z", 2)),
    ///             )),
    ///             alias: "c2".to_string(),
    ///         },
    ///     ]);
    ///
    ///     // Expected result: SELECT c@2 + 1 AS c1, b@1 + a@0 AS c2
    ///     let result = base.try_merge(&top)?;
    ///
    ///     assert_eq!(result.as_ref().len(), 2);
    ///     assert_eq!(result.as_ref()[0].alias, "c1");
    ///     assert_eq!(result.as_ref()[1].alias, "c2");
    ///
    ///     Ok(())
    /// }
    /// ```
    ///
    /// # Errors
    /// This function returns an error if any expression in the `other` projection cannot be
    /// applied on top of this projection.
    pub fn try_merge(&self, other: &ProjectionExprs) -> Result<ProjectionExprs> {
        let mut new_exprs = Vec::with_capacity(other.exprs.len());
        for proj_expr in &other.exprs {
            let new_expr = update_expr(&proj_expr.expr, &self.exprs, true)?
                .ok_or_else(|| {
                    internal_datafusion_err!(
                        "Failed to combine projections: expression {} could not be applied on top of existing projections {}",
                        proj_expr.expr,
                        self.exprs.iter().map(|e| format!("{e}")).join(", ")
                    )
                })?;
            new_exprs.push(ProjectionExpr {
                expr: new_expr,
                alias: proj_expr.alias.clone(),
            });
        }
        Ok(ProjectionExprs::new(new_exprs))
    }

    /// Extract the column indices used in this projection.
    /// For example, for a projection `SELECT a AS x, b + 1 AS y`, where `a` is at index 0 and `b` is at index 1,
    /// this function would return `[0, 1]`.
    /// Repeated indices are returned only once, and the order is ascending.
    pub fn column_indices(&self) -> Vec<usize> {
        self.exprs
            .iter()
            .flat_map(|e| collect_columns(&e.expr).into_iter().map(|col| col.index()))
            .sorted_unstable()
            .dedup()
            .collect_vec()
    }

    /// Extract the ordered column indices for a column-only projection.
    ///
    /// This function assumes that all expressions in the projection are simple column references.
    /// It returns the column indices in the order they appear in the projection.
    ///
    /// # Panics
    ///
    /// Panics if any expression in the projection is not a simple column reference. This includes:
    /// - Computed expressions (e.g., `a + 1`, `CAST(a AS INT)`)
    /// - Function calls (e.g., `UPPER(name)`, `SUM(amount)`)
    /// - Literals (e.g., `42`, `'hello'`)
    /// - Complex nested expressions (e.g., `CASE WHEN ... THEN ... END`)
    ///
    /// # Returns
    ///
    /// A vector of column indices in projection order. Unlike [`column_indices()`](Self::column_indices),
    /// this function:
    /// - Preserves the projection order (does not sort)
    /// - Preserves duplicates (does not deduplicate)
    ///
    /// # Example
    ///
    /// For a projection `SELECT c, a, c` where `a` is at index 0 and `c` is at index 2,
    /// this function would return `[2, 0, 2]`.
    ///
    /// Use [`column_indices()`](Self::column_indices) instead if the projection may contain
    /// non-column expressions or if you need a deduplicated sorted list.
    ///
    /// # Panics
    ///
    /// Panics if any expression in the projection is not a simple column reference.
    #[deprecated(
        since = "52.0.0",
        note = "Use column_indices() instead. This method will be removed in 58.0.0 or 6 months after 52.0.0 is released, whichever comes first."
    )]
    pub fn ordered_column_indices(&self) -> Vec<usize> {
        self.exprs
            .iter()
            .map(|e| {
                e.expr
                    .as_any()
                    .downcast_ref::<Column>()
                    .expect("Expected column reference in projection")
                    .index()
            })
            .collect()
    }

    /// Project a schema according to this projection.
    /// For example, for a projection `SELECT a AS x, b + 1 AS y`, where `a` is at index 0 and `b` is at index 1,
    /// if the input schema is `[a: Int32, b: Int32, c: Int32]`, the output schema would be `[x: Int32, y: Int32]`.
    /// Fields' metadata are preserved from the input schema.
    pub fn project_schema(&self, input_schema: &Schema) -> Result<Schema> {
        let fields: Result<Vec<Field>> = self
            .exprs
            .iter()
            .map(|proj_expr| {
                let metadata = proj_expr
                    .expr
                    .return_field(input_schema)?
                    .metadata()
                    .clone();

                let field = Field::new(
                    &proj_expr.alias,
                    proj_expr.expr.data_type(input_schema)?,
                    proj_expr.expr.nullable(input_schema)?,
                )
                .with_metadata(metadata);

                Ok(field)
            })
            .collect();

        Ok(Schema::new_with_metadata(
            fields?,
            input_schema.metadata().clone(),
        ))
    }

    /// Create a new [`Projector`] from this projection and an input schema.
    ///
    /// A [`Projector`] can be used to apply this projection to record batches.
    ///
    /// # Errors
    /// This function returns an error if the output schema cannot be constructed from the input schema
    /// with the given projection expressions.
    /// For example, if an expression only works with integer columns but the input schema has a string column at that index.
    pub fn make_projector(&self, input_schema: &Schema) -> Result<Projector> {
        let output_schema = Arc::new(self.project_schema(input_schema)?);
        Ok(Projector {
            projection: self.clone(),
            output_schema,
            expression_metrics: None,
        })
    }

    pub fn create_expression_metrics(
        &self,
        metrics: &ExecutionPlanMetricsSet,
        partition: usize,
    ) -> ExpressionEvaluatorMetrics {
        let labels: Vec<String> = self
            .exprs
            .iter()
            .map(|proj_expr| {
                let expr_sql = fmt_sql(proj_expr.expr.as_ref()).to_string();
                if proj_expr.expr.to_string() == proj_expr.alias {
                    expr_sql
                } else {
                    format!("{expr_sql} AS {}", proj_expr.alias)
                }
            })
            .collect();
        ExpressionEvaluatorMetrics::new(metrics, partition, labels)
    }

    /// Project statistics according to this projection.
    /// For example, for a projection `SELECT a AS x, b + 1 AS y`, where `a` is at index 0 and `b` is at index 1,
    /// if the input statistics has column statistics for columns `a`, `b`, and `c`, the output statistics would have column statistics for columns `x` and `y`.
    ///
    /// # Example
    ///
    /// ```rust
    /// use arrow::datatypes::{DataType, Field, Schema};
    /// use datafusion_common::stats::{ColumnStatistics, Precision, Statistics};
    /// use datafusion_physical_expr::projection::ProjectionExprs;
    /// use datafusion_common::Result;
    /// use datafusion_common::ScalarValue;
    /// use std::sync::Arc;
    ///
    /// fn main() -> Result<()> {
    ///     // Input schema: a: Int32, b: Int32, c: Int32
    ///     let input_schema = Arc::new(Schema::new(vec![
    ///         Field::new("a", DataType::Int32, false),
    ///         Field::new("b", DataType::Int32, false),
    ///         Field::new("c", DataType::Int32, false),
    ///     ]));
    ///
    ///     // Input statistics with column stats for a, b, c
    ///     let input_stats = Statistics {
    ///         num_rows: Precision::Exact(100),
    ///         total_byte_size: Precision::Exact(1200),
    ///         column_statistics: vec![
    ///             // Column a stats
    ///             ColumnStatistics::new_unknown()
    ///                 .with_null_count(Precision::Exact(0))
    ///                 .with_min_value(Precision::Exact(ScalarValue::Int32(Some(0))))
    ///                 .with_max_value(Precision::Exact(ScalarValue::Int32(Some(100))))
    ///                 .with_distinct_count(Precision::Exact(100)),
    ///             // Column b stats
    ///             ColumnStatistics::new_unknown()
    ///                 .with_null_count(Precision::Exact(0))
    ///                 .with_min_value(Precision::Exact(ScalarValue::Int32(Some(10))))
    ///                 .with_max_value(Precision::Exact(ScalarValue::Int32(Some(60))))
    ///                 .with_distinct_count(Precision::Exact(50)),
    ///             // Column c stats
    ///             ColumnStatistics::new_unknown()
    ///                 .with_null_count(Precision::Exact(5))
    ///                 .with_min_value(Precision::Exact(ScalarValue::Int32(Some(-10))))
    ///                 .with_max_value(Precision::Exact(ScalarValue::Int32(Some(200))))
    ///                 .with_distinct_count(Precision::Exact(25)),
    ///         ],
    ///     };
    ///
    ///     // Create a projection that selects columns c and a (indices 2 and 0)
    ///     let projection = ProjectionExprs::from_indices(&[2, 0], &input_schema);
    ///
    ///     // Compute output schema
    ///     let output_schema = projection.project_schema(&input_schema)?;
    ///
    ///     // Project the statistics
    ///     let output_stats = projection.project_statistics(input_stats, &output_schema)?;
    ///
    ///     // The output should have 2 column statistics (for c and a, in that order)
    ///     assert_eq!(output_stats.column_statistics.len(), 2);
    ///
    ///     // First column in output is c (was at index 2)
    ///     assert_eq!(
    ///         output_stats.column_statistics[0].min_value,
    ///         Precision::Exact(ScalarValue::Int32(Some(-10)))
    ///     );
    ///     assert_eq!(
    ///         output_stats.column_statistics[0].null_count,
    ///         Precision::Exact(5)
    ///     );
    ///
    ///     // Second column in output is a (was at index 0)
    ///     assert_eq!(
    ///         output_stats.column_statistics[1].min_value,
    ///         Precision::Exact(ScalarValue::Int32(Some(0)))
    ///     );
    ///     assert_eq!(
    ///         output_stats.column_statistics[1].distinct_count,
    ///         Precision::Exact(100)
    ///     );
    ///
    ///     // Total byte size is recalculated based on projected columns
    ///     assert_eq!(
    ///         output_stats.total_byte_size,
    ///         Precision::Exact(800), // each Int32 column is 4 bytes * 100 rows * 2 columns
    ///     );
    ///
    ///     // Number of rows remains the same
    ///     assert_eq!(output_stats.num_rows, Precision::Exact(100));
    ///
    ///     Ok(())
    /// }
    /// ```
    pub fn project_statistics(
        &self,
        mut stats: datafusion_common::Statistics,
        output_schema: &Schema,
    ) -> Result<datafusion_common::Statistics> {
        let mut column_statistics = vec![];

        for proj_expr in &self.exprs {
            let expr = &proj_expr.expr;
            let col_stats = if let Some(col) = expr.as_any().downcast_ref::<Column>() {
                std::mem::take(&mut stats.column_statistics[col.index()])
            } else if let Some(literal) = expr.as_any().downcast_ref::<Literal>() {
                // Handle literal expressions (constants) by calculating proper statistics
                let data_type = expr.data_type(output_schema)?;

                if literal.value().is_null() {
                    let null_count = match stats.num_rows {
                        Precision::Exact(num_rows) => Precision::Exact(num_rows),
                        _ => Precision::Absent,
                    };

                    ColumnStatistics {
                        min_value: Precision::Exact(literal.value().clone()),
                        max_value: Precision::Exact(literal.value().clone()),
                        distinct_count: Precision::Exact(1),
                        null_count,
                        sum_value: Precision::Exact(literal.value().clone()),
                        byte_size: Precision::Exact(0),
                    }
                } else {
                    let value = literal.value();
                    let distinct_count = Precision::Exact(1);
                    let null_count = Precision::Exact(0);

                    let byte_size = if let Some(byte_width) = data_type.primitive_width()
                    {
                        stats.num_rows.multiply(&Precision::Exact(byte_width))
                    } else {
                        // Complex types depend on array encoding, so set to Absent
                        Precision::Absent
                    };

                    let sum_value = Precision::<ScalarValue>::from(stats.num_rows)
                        .cast_to(&value.data_type())
                        .ok()
                        .map(|row_count| {
                            Precision::Exact(value.clone()).multiply(&row_count)
                        })
                        .unwrap_or(Precision::Absent);

                    ColumnStatistics {
                        min_value: Precision::Exact(value.clone()),
                        max_value: Precision::Exact(value.clone()),
                        distinct_count,
                        null_count,
                        sum_value,
                        byte_size,
                    }
                }
            } else {
                // TODO stats: estimate more statistics from expressions
                // (expressions should compute their statistics themselves)
                ColumnStatistics::new_unknown()
            };
            column_statistics.push(col_stats);
        }
        stats.calculate_total_byte_size(output_schema);
        stats.column_statistics = column_statistics;
        Ok(stats)
    }
}

impl<'a> IntoIterator for &'a ProjectionExprs {
    type Item = &'a ProjectionExpr;
    type IntoIter = std::slice::Iter<'a, ProjectionExpr>;

    fn into_iter(self) -> Self::IntoIter {
        self.exprs.iter()
    }
}

/// Applies a projection to record batches.
///
/// A [`Projector`] uses a set of projection expressions to transform
/// and a pre-computed output schema to project record batches accordingly.
///
/// The main reason to use a `Projector` is to avoid repeatedly computing
/// the output schema for each batch, which can be costly if the projection
/// expressions are complex.
#[derive(Clone, Debug)]
pub struct Projector {
    projection: ProjectionExprs,
    output_schema: SchemaRef,
    /// If `Some`, metrics will be tracked for projection evaluation.
    expression_metrics: Option<ExpressionEvaluatorMetrics>,
}

impl Projector {
    /// Construct the projector with metrics. After execution, related metrics will
    /// be tracked inside `ExecutionPlanMetricsSet`
    ///
    /// See [`ExpressionEvaluatorMetrics`] for details.
    pub fn with_metrics(
        &self,
        metrics: &ExecutionPlanMetricsSet,
        partition: usize,
    ) -> Self {
        let expr_metrics = self
            .projection
            .create_expression_metrics(metrics, partition);
        Self {
            expression_metrics: Some(expr_metrics),
            projection: self.projection.clone(),
            output_schema: Arc::clone(&self.output_schema),
        }
    }

    /// Project a record batch according to this projector's expressions.
    ///
    /// # Errors
    /// This function returns an error if any expression evaluation fails
    /// or if the output schema of the resulting record batch does not match
    /// the pre-computed output schema of the projector.
    pub fn project_batch(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        let arrays = evaluate_expressions_to_arrays_with_metrics(
            self.projection.exprs.iter().map(|p| &p.expr),
            batch,
            self.expression_metrics.as_ref(),
        )?;

        if arrays.is_empty() {
            let options =
                RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
            RecordBatch::try_new_with_options(
                Arc::clone(&self.output_schema),
                arrays,
                &options,
            )
            .map_err(Into::into)
        } else {
            RecordBatch::try_new(Arc::clone(&self.output_schema), arrays)
                .map_err(Into::into)
        }
    }

    pub fn output_schema(&self) -> &SchemaRef {
        &self.output_schema
    }

    pub fn projection(&self) -> &ProjectionExprs {
        &self.projection
    }
}

impl IntoIterator for ProjectionExprs {
    type Item = ProjectionExpr;
    type IntoIter = std::vec::IntoIter<ProjectionExpr>;

    fn into_iter(self) -> Self::IntoIter {
        self.exprs.into_iter()
    }
}

/// The function operates in two modes:
///
/// 1) When `sync_with_child` is `true`:
///
///    The function updates the indices of `expr` if the expression resides
///    in the input plan. For instance, given the expressions `a@1 + b@2`
///    and `c@0` with the input schema `c@2, a@0, b@1`, the expressions are
///    updated to `a@0 + b@1` and `c@2`.
///
/// 2) When `sync_with_child` is `false`:
///
///    The function determines how the expression would be updated if a projection
///    was placed before the plan associated with the expression. If the expression
///    cannot be rewritten after the projection, it returns `None`. For example,
///    given the expressions `c@0`, `a@1` and `b@2`, and the projection with
///    an output schema of `a, c_new`, then `c@0` becomes `c_new@1`, `a@1` becomes
///    `a@0`, but `b@2` results in `None` since the projection does not include `b`.
///
/// # Errors
/// This function returns an error if `sync_with_child` is `true` and if any expression references
/// an index that is out of bounds for `projected_exprs`.
/// For example:
///
/// - `expr` is `a@3`
/// - `projected_exprs` is \[`a@0`, `b@1`\]
///
/// In this case, `a@3` references index 3, which is out of bounds for `projected_exprs` (which has length 2).
pub fn update_expr(
    expr: &Arc<dyn PhysicalExpr>,
    projected_exprs: &[ProjectionExpr],
    sync_with_child: bool,
) -> Result<Option<Arc<dyn PhysicalExpr>>> {
    #[derive(PartialEq)]
    enum RewriteState {
        /// The expression is unchanged.
        Unchanged,
        /// Some part of the expression has been rewritten
        RewrittenValid,
        /// Some part of the expression has been rewritten, but some column
        /// references could not be.
        RewrittenInvalid,
    }

    // Track Arc pointers of columns created by pass 1.
    // These should not be modified by pass 2.
    // We use Arc pointer addresses (not name/index) to distinguish pass-1-created columns
    // from original columns that happen to have the same name and index.
    let mut pass1_created: HashSet<usize> = HashSet::new();

    // First pass: try to rewrite the expression in terms of the projected expressions.
    // For example, if the expression is `a + b > 5` and the projection is `a + b AS sum_ab`,
    // we can rewrite the expression to `sum_ab > 5` directly.
    //
    // This optimization only applies when sync_with_child=false, meaning we want the
    // expression to use OUTPUT references (e.g., when pushing projection down and the
    // expression will be above the projection). Pass 1 creates OUTPUT column references.
    //
    // When sync_with_child=true, we want INPUT references (expanding OUTPUT to INPUT),
    // so pass 1 doesn't apply.
    let new_expr = if !sync_with_child {
        Arc::clone(expr)
            .transform_down(&mut |expr: Arc<dyn PhysicalExpr>| {
                // If expr is equal to one of the projected expressions, we can short-circuit the rewrite:
                for (idx, projected_expr) in projected_exprs.iter().enumerate() {
                    if expr.eq(&projected_expr.expr) {
                        // Create new column and track its Arc pointer so pass 2 doesn't modify it
                        let new_col = Arc::new(Column::new(&projected_expr.alias, idx))
                            as Arc<dyn PhysicalExpr>;
                        // Use data pointer for trait object (ignores vtable)
                        pass1_created.insert(Arc::as_ptr(&new_col) as *const () as usize);
                        return Ok(Transformed::yes(new_col));
                    }
                }
                Ok(Transformed::no(expr))
            })?
            .data
    } else {
        Arc::clone(expr)
    };

    // Second pass: rewrite remaining column references based on the projection.
    // Skip columns that were introduced by pass 1.
    let mut state = RewriteState::Unchanged;
    let new_expr = new_expr
        .transform_up(&mut |expr: Arc<dyn PhysicalExpr>| {
            if state == RewriteState::RewrittenInvalid {
                return Ok(Transformed::no(expr));
            }

            let Some(column) = expr.as_any().downcast_ref::<Column>() else {
                return Ok(Transformed::no(expr));
            };

            // Skip columns introduced by pass 1 - they're already valid OUTPUT references.
            // Mark state as valid since pass 1 successfully handled this column.
            // We check the Arc pointer address to distinguish pass-1-created columns from
            // original columns that might have the same name and index.
            if pass1_created.contains(&(Arc::as_ptr(&expr) as *const () as usize)) {
                state = RewriteState::RewrittenValid;
                return Ok(Transformed::no(expr));
            }

            if sync_with_child {
                state = RewriteState::RewrittenValid;
                // Update the index of `column`:
                let projected_expr = projected_exprs.get(column.index()).ok_or_else(|| {
                    internal_datafusion_err!(
                        "Column index {} out of bounds for projected expressions of length {}",
                        column.index(),
                        projected_exprs.len()
                    )
                })?;
                Ok(Transformed::yes(Arc::clone(&projected_expr.expr)))
            } else {
                // default to invalid, in case we can't find the relevant column
                state = RewriteState::RewrittenInvalid;
                // Determine how to update `column` to accommodate `projected_exprs`
                projected_exprs
                    .iter()
                    .enumerate()
                    .find_map(|(index, proj_expr)| {
                        proj_expr.expr.as_any().downcast_ref::<Column>().and_then(
                            |projected_column| {
                                (column.name().eq(projected_column.name())
                                    && column.index() == projected_column.index())
                                .then(|| {
                                    state = RewriteState::RewrittenValid;
                                    Arc::new(Column::new(&proj_expr.alias, index)) as _
                                })
                            },
                        )
                    })
                    .map_or_else(
                        || Ok(Transformed::no(expr)),
                        |c| Ok(Transformed::yes(c)),
                    )
            }
        })
        .data()?;

    match state {
        RewriteState::RewrittenInvalid => Ok(None),
        // Both Unchanged and RewrittenValid are valid:
        // - Unchanged means no columns to rewrite (e.g., literals)
        // - RewrittenValid means columns were successfully rewritten
        RewriteState::Unchanged | RewriteState::RewrittenValid => Ok(Some(new_expr)),
    }
}

/// Stores target expressions, along with their indices, that associate with a
/// source expression in a projection mapping.
#[derive(Clone, Debug, Default)]
pub struct ProjectionTargets {
    /// A non-empty vector of pairs of target expressions and their indices.
    /// Consider using a special non-empty collection type in the future (e.g.
    /// if Rust provides one in the standard library).
    exprs_indices: Vec<(Arc<dyn PhysicalExpr>, usize)>,
}

impl ProjectionTargets {
    /// Returns the first target expression and its index.
    pub fn first(&self) -> &(Arc<dyn PhysicalExpr>, usize) {
        // Since the vector is non-empty, we can safely unwrap:
        self.exprs_indices.first().unwrap()
    }

    /// Adds a target expression and its index to the list of targets.
    pub fn push(&mut self, target: (Arc<dyn PhysicalExpr>, usize)) {
        self.exprs_indices.push(target);
    }
}

impl Deref for ProjectionTargets {
    type Target = [(Arc<dyn PhysicalExpr>, usize)];

    fn deref(&self) -> &Self::Target {
        &self.exprs_indices
    }
}

impl From<Vec<(Arc<dyn PhysicalExpr>, usize)>> for ProjectionTargets {
    fn from(exprs_indices: Vec<(Arc<dyn PhysicalExpr>, usize)>) -> Self {
        Self { exprs_indices }
    }
}

/// Stores the mapping between source expressions and target expressions for a
/// projection.
#[derive(Clone, Debug)]
pub struct ProjectionMapping {
    /// Mapping between source expressions and target expressions.
    /// Vector indices correspond to the indices after projection.
    map: IndexMap<Arc<dyn PhysicalExpr>, ProjectionTargets>,
}

impl ProjectionMapping {
    /// Constructs the mapping between a projection's input and output
    /// expressions.
    ///
    /// For example, given the input projection expressions (`a + b`, `c + d`)
    /// and an output schema with two columns `"c + d"` and `"a + b"`, the
    /// projection mapping would be:
    ///
    /// ```text
    ///  [0]: (c + d, [(col("c + d"), 0)])
    ///  [1]: (a + b, [(col("a + b"), 1)])
    /// ```
    ///
    /// where `col("c + d")` means the column named `"c + d"`.
    pub fn try_new(
        expr: impl IntoIterator<Item = (Arc<dyn PhysicalExpr>, String)>,
        input_schema: &SchemaRef,
    ) -> Result<Self> {
        // Construct a map from the input expressions to the output expression of the projection:
        let mut map = IndexMap::<_, ProjectionTargets>::new();
        for (expr_idx, (expr, name)) in expr.into_iter().enumerate() {
            let target_expr = Arc::new(Column::new(&name, expr_idx)) as _;
            let source_expr = expr.transform_down(|e| match e.as_any().downcast_ref::<Column>() {
                Some(col) => {
                    // Sometimes, an expression and its name in the input_schema
                    // doesn't match. This can cause problems, so we make sure
                    // that the expression name matches with the name in `input_schema`.
                    // Conceptually, `source_expr` and `expression` should be the same.
                    let idx = col.index();
                    let matching_field = input_schema.field(idx);
                    let matching_name = matching_field.name();
                    assert_or_internal_err!(
                        col.name() == matching_name,
                        "Input field name {matching_name} does not match with the projection expression {}",
                        col.name()
                    );
                    let matching_column = Column::new(matching_name, idx);
                    Ok(Transformed::yes(Arc::new(matching_column)))
                }
                None => Ok(Transformed::no(e)),
            })
            .data()?;
            map.entry(source_expr)
                .or_default()
                .push((target_expr, expr_idx));
        }
        Ok(Self { map })
    }

    /// Constructs a subset mapping using the provided indices.
    ///
    /// This is used when the output is a subset of the input without any
    /// other transformations. The indices are for columns in the schema.
    pub fn from_indices(indices: &[usize], schema: &SchemaRef) -> Result<Self> {
        let projection_exprs = indices.iter().map(|index| {
            let field = schema.field(*index);
            let column = Arc::new(Column::new(field.name(), *index));
            (column as _, field.name().clone())
        });
        ProjectionMapping::try_new(projection_exprs, schema)
    }
}

impl Deref for ProjectionMapping {
    type Target = IndexMap<Arc<dyn PhysicalExpr>, ProjectionTargets>;

    fn deref(&self) -> &Self::Target {
        &self.map
    }
}

impl FromIterator<(Arc<dyn PhysicalExpr>, ProjectionTargets)> for ProjectionMapping {
    fn from_iter<T: IntoIterator<Item = (Arc<dyn PhysicalExpr>, ProjectionTargets)>>(
        iter: T,
    ) -> Self {
        Self {
            map: IndexMap::from_iter(iter),
        }
    }
}

/// Projects a slice of [LexOrdering]s onto the given schema.
///
/// This is a convenience wrapper that applies [project_ordering] to each
/// input ordering and collects the successful projections:
/// - For each input ordering, the result of [project_ordering] is appended to
///   the output if it is `Some(...)`.
/// - Order is preserved and no deduplication is attempted.
/// - If none of the input orderings can be projected, an empty `Vec` is
///   returned.
///
/// See [project_ordering] for the semantics of projecting a single
/// [LexOrdering].
pub fn project_orderings(
    orderings: &[LexOrdering],
    schema: &SchemaRef,
) -> Vec<LexOrdering> {
    let mut projected_orderings = vec![];

    for ordering in orderings {
        projected_orderings.extend(project_ordering(ordering, schema));
    }

    projected_orderings
}

/// Projects a single [LexOrdering] onto the given schema.
///
/// This function attempts to rewrite every [PhysicalSortExpr] in the provided
/// [LexOrdering] so that any [Column] expressions point at the correct field
/// indices in `schema`.
///
/// Key details:
/// - Columns are matched by name, not by index. The index of each matched
///   column is looked up with [Schema::column_with_name](arrow::datatypes::Schema::column_with_name) and a new
///   [Column] with the correct [index](Column::index) is substituted.
/// - If an expression references a column name that does not exist in
///   `schema`, projection of the current ordering stops and only the already
///   rewritten prefix is kept. This models the fact that a lexicographical
///   ordering remains valid for any leading prefix whose expressions are
///   present in the projected schema.
/// - If no expressions can be projected (i.e. the first one is missing), the
///   function returns `None`.
///
/// Return value:
/// - `Some(LexOrdering)` if at least one sort expression could be projected.
///   The returned ordering may be a strict prefix of the input ordering.
/// - `None` if no part of the ordering can be projected onto `schema`.
///
/// Example
///
/// Suppose we have an input ordering `[col("a@0"), col("b@1")]` but the projected
/// schema only contains b and not a. The result will be `Some([col("a@0")])`. In other
/// words, the column reference is reindexed to match the projected schema.
/// If neither a nor b is present, the result will be None.
pub fn project_ordering(
    ordering: &LexOrdering,
    schema: &SchemaRef,
) -> Option<LexOrdering> {
    let mut projected_exprs = vec![];
    for PhysicalSortExpr { expr, options } in ordering.iter() {
        let transformed = Arc::clone(expr).transform_up(|expr| {
            let Some(col) = expr.as_any().downcast_ref::<Column>() else {
                return Ok(Transformed::no(expr));
            };

            let name = col.name();
            if let Some((idx, _)) = schema.column_with_name(name) {
                // Compute the new column expression (with correct index) after projection:
                Ok(Transformed::yes(Arc::new(Column::new(name, idx))))
            } else {
                // Cannot find expression in the projected_schema,
                // signal this using an Err result
                plan_err!("")
            }
        });

        match transformed {
            Ok(transformed) => {
                projected_exprs.push(PhysicalSortExpr::new(transformed.data, *options));
            }
            Err(_) => {
                // Err result indicates an expression could not be found in the
                // projected_schema, stop iterating since rest of the orderings are violated
                break;
            }
        }
    }

    LexOrdering::new(projected_exprs)
}

#[cfg(test)]
pub(crate) mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::equivalence::{EquivalenceProperties, convert_to_orderings};
    use crate::expressions::{BinaryExpr, Literal, col};
    use crate::utils::tests::TestScalarUDF;
    use crate::{PhysicalExprRef, ScalarFunctionExpr};

    use arrow::compute::SortOptions;
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion_common::config::ConfigOptions;
    use datafusion_common::stats::Precision;
    use datafusion_common::{ScalarValue, Statistics};
    use datafusion_expr::{Operator, ScalarUDF};
    use insta::assert_snapshot;

    pub(crate) fn output_schema(
        mapping: &ProjectionMapping,
        input_schema: &Arc<Schema>,
    ) -> Result<SchemaRef> {
        // Calculate output schema:
        let mut fields = vec![];
        for (source, targets) in mapping.iter() {
            let data_type = source.data_type(input_schema)?;
            let nullable = source.nullable(input_schema)?;
            for (target, _) in targets.iter() {
                let Some(column) = target.as_any().downcast_ref::<Column>() else {
                    return plan_err!("Expects to have column");
                };
                fields.push(Field::new(column.name(), data_type.clone(), nullable));
            }
        }

        let output_schema = Arc::new(Schema::new_with_metadata(
            fields,
            input_schema.metadata().clone(),
        ));

        Ok(output_schema)
    }

    #[test]
    fn project_orderings() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
            Field::new("c", DataType::Int32, true),
            Field::new("d", DataType::Int32, true),
            Field::new("e", DataType::Int32, true),
            Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
        ]));
        let col_a = &col("a", &schema)?;
        let col_b = &col("b", &schema)?;
        let col_c = &col("c", &schema)?;
        let col_d = &col("d", &schema)?;
        let col_e = &col("e", &schema)?;
        let col_ts = &col("ts", &schema)?;
        let a_plus_b = Arc::new(BinaryExpr::new(
            Arc::clone(col_a),
            Operator::Plus,
            Arc::clone(col_b),
        )) as Arc<dyn PhysicalExpr>;
        let b_plus_d = Arc::new(BinaryExpr::new(
            Arc::clone(col_b),
            Operator::Plus,
            Arc::clone(col_d),
        )) as Arc<dyn PhysicalExpr>;
        let b_plus_e = Arc::new(BinaryExpr::new(
            Arc::clone(col_b),
            Operator::Plus,
            Arc::clone(col_e),
        )) as Arc<dyn PhysicalExpr>;
        let c_plus_d = Arc::new(BinaryExpr::new(
            Arc::clone(col_c),
            Operator::Plus,
            Arc::clone(col_d),
        )) as Arc<dyn PhysicalExpr>;

        let option_asc = SortOptions {
            descending: false,
            nulls_first: false,
        };
        let option_desc = SortOptions {
            descending: true,
            nulls_first: true,
        };

        let test_cases = vec![
            // ---------- TEST CASE 1 ------------
            (
                // orderings
                vec![
                    // [b ASC]
                    vec![(col_b, option_asc)],
                ],
                // projection exprs
                vec![(col_b, "b_new".to_string()), (col_a, "a_new".to_string())],
                // expected
                vec![
                    // [b_new ASC]
                    vec![("b_new", option_asc)],
                ],
            ),
            // ---------- TEST CASE 2 ------------
            (
                // orderings
                vec![
                    // empty ordering
                ],
                // projection exprs
                vec![(col_c, "c_new".to_string()), (col_b, "b_new".to_string())],
                // expected
                vec![
                    // no ordering at the output
                ],
            ),
            // ---------- TEST CASE 3 ------------
            (
                // orderings
                vec![
                    // [ts ASC]
                    vec![(col_ts, option_asc)],
                ],
                // projection exprs
                vec![
                    (col_b, "b_new".to_string()),
                    (col_a, "a_new".to_string()),
                    (col_ts, "ts_new".to_string()),
                ],
                // expected
                vec![
                    // [ts_new ASC]
                    vec![("ts_new", option_asc)],
                ],
            ),
            // ---------- TEST CASE 4 ------------
            (
                // orderings
                vec![
                    // [a ASC, ts ASC]
                    vec![(col_a, option_asc), (col_ts, option_asc)],
                    // [b ASC, ts ASC]
                    vec![(col_b, option_asc), (col_ts, option_asc)],
                ],
                // projection exprs
                vec![
                    (col_b, "b_new".to_string()),
                    (col_a, "a_new".to_string()),
                    (col_ts, "ts_new".to_string()),
                ],
                // expected
                vec![
                    // [a_new ASC, ts_new ASC]
                    vec![("a_new", option_asc), ("ts_new", option_asc)],
                    // [b_new ASC, ts_new ASC]
                    vec![("b_new", option_asc), ("ts_new", option_asc)],
                ],
            ),
            // ---------- TEST CASE 5 ------------
            (
                // orderings
                vec![
                    // [a + b ASC]
                    vec![(&a_plus_b, option_asc)],
                ],
                // projection exprs
                vec![
                    (col_b, "b_new".to_string()),
                    (col_a, "a_new".to_string()),
                    (&a_plus_b, "a+b".to_string()),
                ],
                // expected
                vec![
                    // [a + b ASC]
                    vec![("a+b", option_asc)],
                ],
            ),
            // ---------- TEST CASE 6 ------------
            (
                // orderings
                vec![
                    // [a + b ASC, c ASC]
                    vec![(&a_plus_b, option_asc), (col_c, option_asc)],
                ],
                // projection exprs
                vec![
                    (col_b, "b_new".to_string()),
                    (col_a, "a_new".to_string()),
                    (col_c, "c_new".to_string()),
                    (&a_plus_b, "a+b".to_string()),
                ],
                // expected
                vec![
                    // [a + b ASC, c_new ASC]
                    vec![("a+b", option_asc), ("c_new", option_asc)],
                ],
            ),
            // ------- TEST CASE 7 ----------
            (
                vec![
                    // [a ASC, b ASC, c ASC]
                    vec![(col_a, option_asc), (col_b, option_asc)],
                    // [a ASC, d ASC]
                    vec![(col_a, option_asc), (col_d, option_asc)],
                ],
                // b as b_new, a as a_new, d as d_new b+d
                vec![
                    (col_b, "b_new".to_string()),
                    (col_a, "a_new".to_string()),
                    (col_d, "d_new".to_string()),
                    (&b_plus_d, "b+d".to_string()),
                ],
                // expected
                vec![
                    // [a_new ASC, b_new ASC]
                    vec![("a_new", option_asc), ("b_new", option_asc)],
                    // [a_new ASC, d_new ASC]
                    vec![("a_new", option_asc), ("d_new", option_asc)],
                    // [a_new ASC, b+d ASC]
                    vec![("a_new", option_asc), ("b+d", option_asc)],
                ],
            ),
            // ------- TEST CASE 8 ----------
            (
                // orderings
                vec![
                    // [b+d ASC]
                    vec![(&b_plus_d, option_asc)],
                ],
                // proj exprs
                vec![
                    (col_b, "b_new".to_string()),
                    (col_a, "a_new".to_string()),
                    (col_d, "d_new".to_string()),
                    (&b_plus_d, "b+d".to_string()),
                ],
                // expected
                vec![
                    // [b+d ASC]
                    vec![("b+d", option_asc)],
                ],
            ),
            // ------- TEST CASE 9 ----------
            (
                // orderings
                vec![
                    // [a ASC, d ASC, b ASC]
                    vec![
                        (col_a, option_asc),
                        (col_d, option_asc),
                        (col_b, option_asc),
                    ],
                    // [c ASC]
                    vec![(col_c, option_asc)],
                ],
                // proj exprs
                vec![
                    (col_b, "b_new".to_string()),
                    (col_a, "a_new".to_string()),
                    (col_d, "d_new".to_string()),
                    (col_c, "c_new".to_string()),
                ],
                // expected
                vec![
                    // [a_new ASC, d_new ASC, b_new ASC]
                    vec![
                        ("a_new", option_asc),
                        ("d_new", option_asc),
                        ("b_new", option_asc),
                    ],
                    // [c_new ASC],
                    vec![("c_new", option_asc)],
                ],
            ),
            // ------- TEST CASE 10 ----------
            (
                vec![
                    // [a ASC, b ASC, c ASC]
                    vec![
                        (col_a, option_asc),
                        (col_b, option_asc),
                        (col_c, option_asc),
                    ],
                    // [a ASC, d ASC]
                    vec![(col_a, option_asc), (col_d, option_asc)],
                ],
                // proj exprs
                vec![
                    (col_b, "b_new".to_string()),
                    (col_a, "a_new".to_string()),
                    (col_c, "c_new".to_string()),
                    (&c_plus_d, "c+d".to_string()),
                ],
                // expected
                vec![
                    // [a_new ASC, b_new ASC, c_new ASC]
                    vec![
                        ("a_new", option_asc),
                        ("b_new", option_asc),
                        ("c_new", option_asc),
                    ],
                    // [a_new ASC, b_new ASC, c+d ASC]
                    vec![
                        ("a_new", option_asc),
                        ("b_new", option_asc),
                        ("c+d", option_asc),
                    ],
                ],
            ),
            // ------- TEST CASE 11 ----------
            (
                // orderings
                vec![
                    // [a ASC, b ASC]
                    vec![(col_a, option_asc), (col_b, option_asc)],
                    // [a ASC, d ASC]
                    vec![(col_a, option_asc), (col_d, option_asc)],
                ],
                // proj exprs
                vec![
                    (col_b, "b_new".to_string()),
                    (col_a, "a_new".to_string()),
                    (&b_plus_d, "b+d".to_string()),
                ],
                // expected
                vec![
                    // [a_new ASC, b_new ASC]
                    vec![("a_new", option_asc), ("b_new", option_asc)],
                    // [a_new ASC, b + d ASC]
                    vec![("a_new", option_asc), ("b+d", option_asc)],
                ],
            ),
            // ------- TEST CASE 12 ----------
            (
                // orderings
                vec![
                    // [a ASC, b ASC, c ASC]
                    vec![
                        (col_a, option_asc),
                        (col_b, option_asc),
                        (col_c, option_asc),
                    ],
                ],
                // proj exprs
                vec![(col_c, "c_new".to_string()), (col_a, "a_new".to_string())],
                // expected
                vec![
                    // [a_new ASC]
                    vec![("a_new", option_asc)],
                ],
            ),
            // ------- TEST CASE 13 ----------
            (
                // orderings
                vec![
                    // [a ASC, b ASC, c ASC]
                    vec![
                        (col_a, option_asc),
                        (col_b, option_asc),
                        (col_c, option_asc),
                    ],
                    // [a ASC, a + b ASC, c ASC]
                    vec![
                        (col_a, option_asc),
                        (&a_plus_b, option_asc),
                        (col_c, option_asc),
                    ],
                ],
                // proj exprs
                vec![
                    (col_c, "c_new".to_string()),
                    (col_b, "b_new".to_string()),
                    (col_a, "a_new".to_string()),
                    (&a_plus_b, "a+b".to_string()),
                ],
                // expected
                vec![
                    // [a_new ASC, b_new ASC, c_new ASC]
                    vec![
                        ("a_new", option_asc),
                        ("b_new", option_asc),
                        ("c_new", option_asc),
                    ],
                    // [a_new ASC, a+b ASC, c_new ASC]
                    vec![
                        ("a_new", option_asc),
                        ("a+b", option_asc),
                        ("c_new", option_asc),
                    ],
                ],
            ),
            // ------- TEST CASE 14 ----------
            (
                // orderings
                vec![
                    // [a ASC, b ASC]
                    vec![(col_a, option_asc), (col_b, option_asc)],
                    // [c ASC, b ASC]
                    vec![(col_c, option_asc), (col_b, option_asc)],
                    // [d ASC, e ASC]
                    vec![(col_d, option_asc), (col_e, option_asc)],
                ],
                // proj exprs
                vec![
                    (col_c, "c_new".to_string()),
                    (col_d, "d_new".to_string()),
                    (col_a, "a_new".to_string()),
                    (&b_plus_e, "b+e".to_string()),
                ],
                // expected
                vec![
                    // [a_new ASC, d_new ASC, b+e ASC]
                    vec![
                        ("a_new", option_asc),
                        ("d_new", option_asc),
                        ("b+e", option_asc),
                    ],
                    // [d_new ASC, a_new ASC, b+e ASC]
                    vec![
                        ("d_new", option_asc),
                        ("a_new", option_asc),
                        ("b+e", option_asc),
                    ],
                    // [c_new ASC, d_new ASC, b+e ASC]
                    vec![
                        ("c_new", option_asc),
                        ("d_new", option_asc),
                        ("b+e", option_asc),
                    ],
                    // [d_new ASC, c_new ASC, b+e ASC]
                    vec![
                        ("d_new", option_asc),
                        ("c_new", option_asc),
                        ("b+e", option_asc),
                    ],
                ],
            ),
            // ------- TEST CASE 15 ----------
            (
                // orderings
                vec![
                    // [a ASC, c ASC, b ASC]
                    vec![
                        (col_a, option_asc),
                        (col_c, option_asc),
                        (col_b, option_asc),
                    ],
                ],
                // proj exprs
                vec![
                    (col_c, "c_new".to_string()),
                    (col_a, "a_new".to_string()),
                    (&a_plus_b, "a+b".to_string()),
                ],
                // expected
                vec![
                    // [a_new ASC, d_new ASC, b+e ASC]
                    vec![
                        ("a_new", option_asc),
                        ("c_new", option_asc),
                        ("a+b", option_asc),
                    ],
                ],
            ),
            // ------- TEST CASE 16 ----------
            (
                // orderings
                vec![
                    // [a ASC, b ASC]
                    vec![(col_a, option_asc), (col_b, option_asc)],
                    // [c ASC, b DESC]
                    vec![(col_c, option_asc), (col_b, option_desc)],
                    // [e ASC]
                    vec![(col_e, option_asc)],
                ],
                // proj exprs
                vec![
                    (col_c, "c_new".to_string()),
                    (col_a, "a_new".to_string()),
                    (col_b, "b_new".to_string()),
                    (&b_plus_e, "b+e".to_string()),
                ],
                // expected
                vec![
                    // [a_new ASC, b_new ASC]
                    vec![("a_new", option_asc), ("b_new", option_asc)],
                    // [a_new ASC, b_new ASC]
                    vec![("a_new", option_asc), ("b+e", option_asc)],
                    // [c_new ASC, b_new DESC]
                    vec![("c_new", option_asc), ("b_new", option_desc)],
                ],
            ),
        ];

        for (idx, (orderings, proj_exprs, expected)) in test_cases.into_iter().enumerate()
        {
            let mut eq_properties = EquivalenceProperties::new(Arc::clone(&schema));

            let orderings = convert_to_orderings(&orderings);
            eq_properties.add_orderings(orderings);

            let proj_exprs = proj_exprs
                .into_iter()
                .map(|(expr, name)| (Arc::clone(expr), name));
            let projection_mapping = ProjectionMapping::try_new(proj_exprs, &schema)?;
            let output_schema = output_schema(&projection_mapping, &schema)?;

            let expected = expected
                .into_iter()
                .map(|ordering| {
                    ordering
                        .into_iter()
                        .map(|(name, options)| {
                            (col(name, &output_schema).unwrap(), options)
                        })
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>();
            let expected = convert_to_orderings(&expected);

            let projected_eq = eq_properties.project(&projection_mapping, output_schema);
            let orderings = projected_eq.oeq_class();

            let err_msg = format!(
                "test_idx: {idx:?}, actual: {orderings:?}, expected: {expected:?}, projection_mapping: {projection_mapping:?}"
            );

            assert_eq!(orderings.len(), expected.len(), "{err_msg}");
            for expected_ordering in &expected {
                assert!(orderings.contains(expected_ordering), "{}", err_msg)
            }
        }

        Ok(())
    }

    #[test]
    fn project_orderings2() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
            Field::new("c", DataType::Int32, true),
            Field::new("d", DataType::Int32, true),
            Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
        ]));
        let col_a = &col("a", &schema)?;
        let col_b = &col("b", &schema)?;
        let col_c = &col("c", &schema)?;
        let col_ts = &col("ts", &schema)?;
        let a_plus_b = Arc::new(BinaryExpr::new(
            Arc::clone(col_a),
            Operator::Plus,
            Arc::clone(col_b),
        )) as Arc<dyn PhysicalExpr>;

        let test_fun = Arc::new(ScalarUDF::new_from_impl(TestScalarUDF::new()));

        let round_c = Arc::new(ScalarFunctionExpr::try_new(
            test_fun,
            vec![Arc::clone(col_c)],
            &schema,
            Arc::new(ConfigOptions::default()),
        )?) as PhysicalExprRef;

        let option_asc = SortOptions {
            descending: false,
            nulls_first: false,
        };

        let proj_exprs = vec![
            (col_b, "b_new".to_string()),
            (col_a, "a_new".to_string()),
            (col_c, "c_new".to_string()),
            (&round_c, "round_c_res".to_string()),
        ];
        let proj_exprs = proj_exprs
            .into_iter()
            .map(|(expr, name)| (Arc::clone(expr), name));
        let projection_mapping = ProjectionMapping::try_new(proj_exprs, &schema)?;
        let output_schema = output_schema(&projection_mapping, &schema)?;

        let col_a_new = &col("a_new", &output_schema)?;
        let col_b_new = &col("b_new", &output_schema)?;
        let col_c_new = &col("c_new", &output_schema)?;
        let col_round_c_res = &col("round_c_res", &output_schema)?;
        let a_new_plus_b_new = Arc::new(BinaryExpr::new(
            Arc::clone(col_a_new),
            Operator::Plus,
            Arc::clone(col_b_new),
        )) as Arc<dyn PhysicalExpr>;

        let test_cases = [
            // ---------- TEST CASE 1 ------------
            (
                // orderings
                vec![
                    // [a ASC]
                    vec![(col_a, option_asc)],
                ],
                // expected
                vec![
                    // [b_new ASC]
                    vec![(col_a_new, option_asc)],
                ],
            ),
            // ---------- TEST CASE 2 ------------
            (
                // orderings
                vec![
                    // [a+b ASC]
                    vec![(&a_plus_b, option_asc)],
                ],
                // expected
                vec![
                    // [b_new ASC]
                    vec![(&a_new_plus_b_new, option_asc)],
                ],
            ),
            // ---------- TEST CASE 3 ------------
            (
                // orderings
                vec![
                    // [a ASC, ts ASC]
                    vec![(col_a, option_asc), (col_ts, option_asc)],
                ],
                // expected
                vec![
                    // [a_new ASC, date_bin_res ASC]
                    vec![(col_a_new, option_asc)],
                ],
            ),
            // ---------- TEST CASE 4 ------------
            (
                // orderings
                vec![
                    // [a ASC, ts ASC, b ASC]
                    vec![
                        (col_a, option_asc),
                        (col_ts, option_asc),
                        (col_b, option_asc),
                    ],
                ],
                // expected
                vec![
                    // [a_new ASC, date_bin_res ASC]
                    vec![(col_a_new, option_asc)],
                ],
            ),
            // ---------- TEST CASE 5 ------------
            (
                // orderings
                vec![
                    // [a ASC, c ASC]
                    vec![(col_a, option_asc), (col_c, option_asc)],
                ],
                // expected
                vec![
                    // [a_new ASC, round_c_res ASC, c_new ASC]
                    vec![(col_a_new, option_asc), (col_round_c_res, option_asc)],
                    // [a_new ASC, c_new ASC]
                    vec![(col_a_new, option_asc), (col_c_new, option_asc)],
                ],
            ),
            // ---------- TEST CASE 6 ------------
            (
                // orderings
                vec![
                    // [c ASC, b ASC]
                    vec![(col_c, option_asc), (col_b, option_asc)],
                ],
                // expected
                vec![
                    // [round_c_res ASC]
                    vec![(col_round_c_res, option_asc)],
                    // [c_new ASC, b_new ASC]
                    vec![(col_c_new, option_asc), (col_b_new, option_asc)],
                ],
            ),
            // ---------- TEST CASE 7 ------------
            (
                // orderings
                vec![
                    // [a+b ASC, c ASC]
                    vec![(&a_plus_b, option_asc), (col_c, option_asc)],
                ],
                // expected
                vec![
                    // [a+b ASC, round(c) ASC, c_new ASC]
                    vec![
                        (&a_new_plus_b_new, option_asc),
                        (col_round_c_res, option_asc),
                    ],
                    // [a+b ASC, c_new ASC]
                    vec![(&a_new_plus_b_new, option_asc), (col_c_new, option_asc)],
                ],
            ),
        ];

        for (idx, (orderings, expected)) in test_cases.iter().enumerate() {
            let mut eq_properties = EquivalenceProperties::new(Arc::clone(&schema));

            let orderings = convert_to_orderings(orderings);
            eq_properties.add_orderings(orderings);

            let expected = convert_to_orderings(expected);

            let projected_eq =
                eq_properties.project(&projection_mapping, Arc::clone(&output_schema));
            let orderings = projected_eq.oeq_class();

            let err_msg = format!(
                "test idx: {idx:?}, actual: {orderings:?}, expected: {expected:?}, projection_mapping: {projection_mapping:?}"
            );

            assert_eq!(orderings.len(), expected.len(), "{err_msg}");
            for expected_ordering in &expected {
                assert!(orderings.contains(expected_ordering), "{}", err_msg)
            }
        }
        Ok(())
    }

    #[test]
    fn project_orderings3() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
            Field::new("c", DataType::Int32, true),
            Field::new("d", DataType::Int32, true),
            Field::new("e", DataType::Int32, true),
            Field::new("f", DataType::Int32, true),
        ]));
        let col_a = &col("a", &schema)?;
        let col_b = &col("b", &schema)?;
        let col_c = &col("c", &schema)?;
        let col_d = &col("d", &schema)?;
        let col_e = &col("e", &schema)?;
        let col_f = &col("f", &schema)?;
        let a_plus_b = Arc::new(BinaryExpr::new(
            Arc::clone(col_a),
            Operator::Plus,
            Arc::clone(col_b),
        )) as Arc<dyn PhysicalExpr>;

        let option_asc = SortOptions {
            descending: false,
            nulls_first: false,
        };

        let proj_exprs = vec![
            (col_c, "c_new".to_string()),
            (col_d, "d_new".to_string()),
            (&a_plus_b, "a+b".to_string()),
        ];
        let proj_exprs = proj_exprs
            .into_iter()
            .map(|(expr, name)| (Arc::clone(expr), name));
        let projection_mapping = ProjectionMapping::try_new(proj_exprs, &schema)?;
        let output_schema = output_schema(&projection_mapping, &schema)?;

        let col_a_plus_b_new = &col("a+b", &output_schema)?;
        let col_c_new = &col("c_new", &output_schema)?;
        let col_d_new = &col("d_new", &output_schema)?;

        let test_cases = vec![
            // ---------- TEST CASE 1 ------------
            (
                // orderings
                vec![
                    // [d ASC, b ASC]
                    vec![(col_d, option_asc), (col_b, option_asc)],
                    // [c ASC, a ASC]
                    vec![(col_c, option_asc), (col_a, option_asc)],
                ],
                // equal conditions
                vec![],
                // expected
                vec![
                    // [d_new ASC, c_new ASC, a+b ASC]
                    vec![
                        (col_d_new, option_asc),
                        (col_c_new, option_asc),
                        (col_a_plus_b_new, option_asc),
                    ],
                    // [c_new ASC, d_new ASC, a+b ASC]
                    vec![
                        (col_c_new, option_asc),
                        (col_d_new, option_asc),
                        (col_a_plus_b_new, option_asc),
                    ],
                ],
            ),
            // ---------- TEST CASE 2 ------------
            (
                // orderings
                vec![
                    // [d ASC, b ASC]
                    vec![(col_d, option_asc), (col_b, option_asc)],
                    // [c ASC, e ASC], Please note that a=e
                    vec![(col_c, option_asc), (col_e, option_asc)],
                ],
                // equal conditions
                vec![(col_e, col_a)],
                // expected
                vec![
                    // [d_new ASC, c_new ASC, a+b ASC]
                    vec![
                        (col_d_new, option_asc),
                        (col_c_new, option_asc),
                        (col_a_plus_b_new, option_asc),
                    ],
                    // [c_new ASC, d_new ASC, a+b ASC]
                    vec![
                        (col_c_new, option_asc),
                        (col_d_new, option_asc),
                        (col_a_plus_b_new, option_asc),
                    ],
                ],
            ),
            // ---------- TEST CASE 3 ------------
            (
                // orderings
                vec![
                    // [d ASC, b ASC]
                    vec![(col_d, option_asc), (col_b, option_asc)],
                    // [c ASC, e ASC], Please note that a=f
                    vec![(col_c, option_asc), (col_e, option_asc)],
                ],
                // equal conditions
                vec![(col_a, col_f)],
                // expected
                vec![
                    // [d_new ASC]
                    vec![(col_d_new, option_asc)],
                    // [c_new ASC]
                    vec![(col_c_new, option_asc)],
                ],
            ),
        ];
        for (orderings, equal_columns, expected) in test_cases {
            let mut eq_properties = EquivalenceProperties::new(Arc::clone(&schema));
            for (lhs, rhs) in equal_columns {
                eq_properties.add_equal_conditions(Arc::clone(lhs), Arc::clone(rhs))?;
            }

            let orderings = convert_to_orderings(&orderings);
            eq_properties.add_orderings(orderings);

            let expected = convert_to_orderings(&expected);

            let projected_eq =
                eq_properties.project(&projection_mapping, Arc::clone(&output_schema));
            let orderings = projected_eq.oeq_class();

            let err_msg = format!(
                "actual: {orderings:?}, expected: {expected:?}, projection_mapping: {projection_mapping:?}"
            );

            assert_eq!(orderings.len(), expected.len(), "{err_msg}");
            for expected_ordering in &expected {
                assert!(orderings.contains(expected_ordering), "{}", err_msg)
            }
        }

        Ok(())
    }

    fn get_stats() -> Statistics {
        Statistics {
            num_rows: Precision::Exact(5),
            total_byte_size: Precision::Exact(23),
            column_statistics: vec![
                ColumnStatistics {
                    distinct_count: Precision::Exact(5),
                    max_value: Precision::Exact(ScalarValue::Int64(Some(21))),
                    min_value: Precision::Exact(ScalarValue::Int64(Some(-4))),
                    sum_value: Precision::Exact(ScalarValue::Int64(Some(42))),
                    null_count: Precision::Exact(0),
                    byte_size: Precision::Absent,
                },
                ColumnStatistics {
                    distinct_count: Precision::Exact(1),
                    max_value: Precision::Exact(ScalarValue::from("x")),
                    min_value: Precision::Exact(ScalarValue::from("a")),
                    sum_value: Precision::Absent,
                    null_count: Precision::Exact(3),
                    byte_size: Precision::Absent,
                },
                ColumnStatistics {
                    distinct_count: Precision::Absent,
                    max_value: Precision::Exact(ScalarValue::Float32(Some(1.1))),
                    min_value: Precision::Exact(ScalarValue::Float32(Some(0.1))),
                    sum_value: Precision::Exact(ScalarValue::Float32(Some(5.5))),
                    null_count: Precision::Absent,
                    byte_size: Precision::Absent,
                },
            ],
        }
    }

    fn get_schema() -> Schema {
        let field_0 = Field::new("col0", DataType::Int64, false);
        let field_1 = Field::new("col1", DataType::Utf8, false);
        let field_2 = Field::new("col2", DataType::Float32, false);
        Schema::new(vec![field_0, field_1, field_2])
    }

    #[test]
    fn test_stats_projection_columns_only() {
        let source = get_stats();
        let schema = get_schema();

        let projection = ProjectionExprs::new(vec![
            ProjectionExpr {
                expr: Arc::new(Column::new("col1", 1)),
                alias: "col1".to_string(),
            },
            ProjectionExpr {
                expr: Arc::new(Column::new("col0", 0)),
                alias: "col0".to_string(),
            },
        ]);

        let result = projection
            .project_statistics(source, &projection.project_schema(&schema).unwrap())
            .unwrap();

        let expected = Statistics {
            num_rows: Precision::Exact(5),
            // Because there is a variable length Utf8 column we cannot calculate exact byte size after projection
            // Thus we set it to Inexact (originally it was Exact(23))
            total_byte_size: Precision::Inexact(23),
            column_statistics: vec![
                ColumnStatistics {
                    distinct_count: Precision::Exact(1),
                    max_value: Precision::Exact(ScalarValue::from("x")),
                    min_value: Precision::Exact(ScalarValue::from("a")),
                    sum_value: Precision::Absent,
                    null_count: Precision::Exact(3),
                    byte_size: Precision::Absent,
                },
                ColumnStatistics {
                    distinct_count: Precision::Exact(5),
                    max_value: Precision::Exact(ScalarValue::Int64(Some(21))),
                    min_value: Precision::Exact(ScalarValue::Int64(Some(-4))),
                    sum_value: Precision::Exact(ScalarValue::Int64(Some(42))),
                    null_count: Precision::Exact(0),
                    byte_size: Precision::Absent,
                },
            ],
        };

        assert_eq!(result, expected);
    }

    #[test]
    fn test_stats_projection_column_with_primitive_width_only() {
        let source = get_stats();
        let schema = get_schema();

        let projection = ProjectionExprs::new(vec![
            ProjectionExpr {
                expr: Arc::new(Column::new("col2", 2)),
                alias: "col2".to_string(),
            },
            ProjectionExpr {
                expr: Arc::new(Column::new("col0", 0)),
                alias: "col0".to_string(),
            },
        ]);

        let result = projection
            .project_statistics(source, &projection.project_schema(&schema).unwrap())
            .unwrap();

        let expected = Statistics {
            num_rows: Precision::Exact(5),
            total_byte_size: Precision::Exact(60),
            column_statistics: vec![
                ColumnStatistics {
                    distinct_count: Precision::Absent,
                    max_value: Precision::Exact(ScalarValue::Float32(Some(1.1))),
                    min_value: Precision::Exact(ScalarValue::Float32(Some(0.1))),
                    sum_value: Precision::Exact(ScalarValue::Float32(Some(5.5))),
                    null_count: Precision::Absent,
                    byte_size: Precision::Absent,
                },
                ColumnStatistics {
                    distinct_count: Precision::Exact(5),
                    max_value: Precision::Exact(ScalarValue::Int64(Some(21))),
                    min_value: Precision::Exact(ScalarValue::Int64(Some(-4))),
                    sum_value: Precision::Exact(ScalarValue::Int64(Some(42))),
                    null_count: Precision::Exact(0),
                    byte_size: Precision::Absent,
                },
            ],
        };

        assert_eq!(result, expected);
    }

    // Tests for Projection struct

    #[test]
    fn test_projection_new() -> Result<()> {
        let exprs = vec![
            ProjectionExpr {
                expr: Arc::new(Column::new("a", 0)),
                alias: "a".to_string(),
            },
            ProjectionExpr {
                expr: Arc::new(Column::new("b", 1)),
                alias: "b".to_string(),
            },
        ];
        let projection = ProjectionExprs::new(exprs.clone());
        assert_eq!(projection.as_ref().len(), 2);
        Ok(())
    }

    #[test]
    fn test_projection_from_vec() -> Result<()> {
        let exprs = vec![ProjectionExpr {
            expr: Arc::new(Column::new("x", 0)),
            alias: "x".to_string(),
        }];
        let projection: ProjectionExprs = exprs.clone().into();
        assert_eq!(projection.as_ref().len(), 1);
        Ok(())
    }

    #[test]
    fn test_projection_as_ref() -> Result<()> {
        let exprs = vec![
            ProjectionExpr {
                expr: Arc::new(Column::new("col1", 0)),
                alias: "col1".to_string(),
            },
            ProjectionExpr {
                expr: Arc::new(Column::new("col2", 1)),
                alias: "col2".to_string(),
            },
        ];
        let projection = ProjectionExprs::new(exprs);
        let as_ref: &[ProjectionExpr] = projection.as_ref();
        assert_eq!(as_ref.len(), 2);
        Ok(())
    }

    #[test]
    fn test_column_indices_multiple_columns() -> Result<()> {
        // Test with reversed column order to ensure proper reordering
        let projection = ProjectionExprs::new(vec![
            ProjectionExpr {
                expr: Arc::new(Column::new("c", 5)),
                alias: "c".to_string(),
            },
            ProjectionExpr {
                expr: Arc::new(Column::new("b", 2)),
                alias: "b".to_string(),
            },
            ProjectionExpr {
                expr: Arc::new(Column::new("a", 0)),
                alias: "a".to_string(),
            },
        ]);
        // Should return sorted indices regardless of projection order
        assert_eq!(projection.column_indices(), vec![0, 2, 5]);
        Ok(())
    }

    #[test]
    fn test_column_indices_duplicates() -> Result<()> {
        // Test that duplicate column indices appear only once
        let projection = ProjectionExprs::new(vec![
            ProjectionExpr {
                expr: Arc::new(Column::new("a", 1)),
                alias: "a".to_string(),
            },
            ProjectionExpr {
                expr: Arc::new(Column::new("b", 3)),
                alias: "b".to_string(),
            },
            ProjectionExpr {
                expr: Arc::new(Column::new("a2", 1)), // duplicate index
                alias: "a2".to_string(),
            },
        ]);
        assert_eq!(projection.column_indices(), vec![1, 3]);
        Ok(())
    }

    #[test]
    fn test_column_indices_unsorted() -> Result<()> {
        // Test that column indices are sorted in the output
        let projection = ProjectionExprs::new(vec![
            ProjectionExpr {
                expr: Arc::new(Column::new("c", 5)),
                alias: "c".to_string(),
            },
            ProjectionExpr {
                expr: Arc::new(Column::new("a", 1)),
                alias: "a".to_string(),
            },
            ProjectionExpr {
                expr: Arc::new(Column::new("b", 3)),
                alias: "b".to_string(),
            },
        ]);
        assert_eq!(projection.column_indices(), vec![1, 3, 5]);
        Ok(())
    }

    #[test]
    fn test_column_indices_complex_expr() -> Result<()> {
        // Test with complex expressions containing multiple columns
        let expr = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("a", 1)),
            Operator::Plus,
            Arc::new(Column::new("b", 4)),
        ));
        let projection = ProjectionExprs::new(vec![
            ProjectionExpr {
                expr,
                alias: "sum".to_string(),
            },
            ProjectionExpr {
                expr: Arc::new(Column::new("c", 2)),
                alias: "c".to_string(),
            },
        ]);
        // Should return [1, 2, 4] - all columns used, sorted and deduplicated
        assert_eq!(projection.column_indices(), vec![1, 2, 4]);
        Ok(())
    }

    #[test]
    fn test_column_indices_empty() -> Result<()> {
        let projection = ProjectionExprs::new(vec![]);
        assert_eq!(projection.column_indices(), Vec::<usize>::new());
        Ok(())
    }

    #[test]
    fn test_merge_simple_columns() -> Result<()> {
        // First projection: SELECT c@2 AS x, b@1 AS y, a@0 AS z
        let base_projection = ProjectionExprs::new(vec![
            ProjectionExpr {
                expr: Arc::new(Column::new("c", 2)),
                alias: "x".to_string(),
            },
            ProjectionExpr {
                expr: Arc::new(Column::new("b", 1)),
                alias: "y".to_string(),
            },
            ProjectionExpr {
                expr: Arc::new(Column::new("a", 0)),
                alias: "z".to_string(),
            },
        ]);

        // Second projection: SELECT y@1 AS col2, x@0 AS col1
        let top_projection = ProjectionExprs::new(vec![
            ProjectionExpr {
                expr: Arc::new(Column::new("y", 1)),
                alias: "col2".to_string(),
            },
            ProjectionExpr {
                expr: Arc::new(Column::new("x", 0)),
                alias: "col1".to_string(),
            },
        ]);

        // Merge should produce: SELECT b@1 AS col2, c@2 AS col1
        let merged = base_projection.try_merge(&top_projection)?;
        assert_snapshot!(format!("{merged}"), @"Projection[b@1 AS col2, c@2 AS col1]");

        Ok(())
    }

    #[test]
    fn test_merge_with_expressions() -> Result<()> {
        // First projection: SELECT c@2 AS x, b@1 AS y, a@0 AS z
        let base_projection = ProjectionExprs::new(vec![
            ProjectionExpr {
                expr: Arc::new(Column::new("c", 2)),
                alias: "x".to_string(),
            },
            ProjectionExpr {
                expr: Arc::new(Column::new("b", 1)),
                alias: "y".to_string(),
            },
            ProjectionExpr {
                expr: Arc::new(Column::new("a", 0)),
                alias: "z".to_string(),
            },
        ]);

        // Second projection: SELECT y@1 + z@2 AS c2, x@0 + 1 AS c1
        let top_projection = ProjectionExprs::new(vec![
            ProjectionExpr {
                expr: Arc::new(BinaryExpr::new(
                    Arc::new(Column::new("y", 1)),
                    Operator::Plus,
                    Arc::new(Column::new("z", 2)),
                )),
                alias: "c2".to_string(),
            },
            ProjectionExpr {
                expr: Arc::new(BinaryExpr::new(
                    Arc::new(Column::new("x", 0)),
                    Operator::Plus,
                    Arc::new(Literal::new(ScalarValue::Int32(Some(1)))),
                )),
                alias: "c1".to_string(),
            },
        ]);

        // Merge should produce: SELECT b@1 + a@0 AS c2, c@2 + 1 AS c1
        let merged = base_projection.try_merge(&top_projection)?;
        assert_snapshot!(format!("{merged}"), @"Projection[b@1 + a@0 AS c2, c@2 + 1 AS c1]");

        Ok(())
    }

    #[test]
    fn try_merge_error() {
        // Create a base projection
        let base = ProjectionExprs::new(vec![
            ProjectionExpr {
                expr: Arc::new(Column::new("a", 0)),
                alias: "x".to_string(),
            },
            ProjectionExpr {
                expr: Arc::new(Column::new("b", 1)),
                alias: "y".to_string(),
            },
        ]);

        // Create a top projection that references a non-existent column index
        let top = ProjectionExprs::new(vec![ProjectionExpr {
            expr: Arc::new(Column::new("z", 5)), // Invalid index
            alias: "result".to_string(),
        }]);

        // Attempt to merge and expect an error
        let err_msg = base.try_merge(&top).unwrap_err().to_string();
        assert!(
            err_msg.contains("Internal error: Column index 5 out of bounds for projected expressions of length 2"),
            "Unexpected error message: {err_msg}",
        );
    }

    #[test]
    fn test_merge_empty_projection_with_literal() -> Result<()> {
        // This test reproduces the issue from roundtrip_empty_projection test
        // Query like: SELECT 1 FROM table
        // where the file scan needs no columns (empty projection)
        // but we project a literal on top

        // Empty base projection (no columns needed from file)
        let base_projection = ProjectionExprs::new(vec![]);

        // Top projection with a literal expression: SELECT 1
        let top_projection = ProjectionExprs::new(vec![ProjectionExpr {
            expr: Arc::new(Literal::new(ScalarValue::Int64(Some(1)))),
            alias: "Int64(1)".to_string(),
        }]);

        // This should succeed - literals don't reference columns so they should
        // pass through unchanged when merged with an empty projection
        let merged = base_projection.try_merge(&top_projection)?;
        assert_snapshot!(format!("{merged}"), @"Projection[1 AS Int64(1)]");

        Ok(())
    }

    #[test]
    fn test_update_expr_with_literal() -> Result<()> {
        // Test that update_expr correctly handles expressions without column references
        let literal_expr: Arc<dyn PhysicalExpr> =
            Arc::new(Literal::new(ScalarValue::Int64(Some(42))));
        let empty_projection: Vec<ProjectionExpr> = vec![];

        // Updating a literal with an empty projection should return the literal unchanged
        let result = update_expr(&literal_expr, &empty_projection, true)?;
        assert!(result.is_some(), "Literal expression should be valid");

        let result_expr = result.unwrap();
        assert_eq!(
            result_expr
                .as_any()
                .downcast_ref::<Literal>()
                .unwrap()
                .value(),
            &ScalarValue::Int64(Some(42))
        );

        Ok(())
    }

    #[test]
    fn test_update_expr_with_complex_literal_expr() -> Result<()> {
        // Test update_expr with an expression containing both literals and a column
        // This tests the case where we have: literal + column
        let expr: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(Literal::new(ScalarValue::Int64(Some(10)))),
            Operator::Plus,
            Arc::new(Column::new("x", 0)),
        ));

        // Base projection that maps column 0 to a different expression
        let base_projection = vec![ProjectionExpr {
            expr: Arc::new(Column::new("a", 5)),
            alias: "x".to_string(),
        }];

        // The expression should be updated: 10 + x@0 becomes 10 + a@5
        let result = update_expr(&expr, &base_projection, true)?;
        assert!(result.is_some(), "Expression should be valid");

        let result_expr = result.unwrap();
        let binary = result_expr
            .as_any()
            .downcast_ref::<BinaryExpr>()
            .expect("Should be a BinaryExpr");

        // Left side should still be the literal
        assert!(binary.left().as_any().downcast_ref::<Literal>().is_some());

        // Right side should be updated to reference column at index 5
        let right_col = binary
            .right()
            .as_any()
            .downcast_ref::<Column>()
            .expect("Right should be a Column");
        assert_eq!(right_col.index(), 5);

        Ok(())
    }

    #[test]
    fn test_update_expr_matches_projected_expr() -> Result<()> {
        // Test that when filter expression exactly matches a projected expression,
        // update_expr short-circuits and rewrites to use the projected column.
        // e.g., projection: a * 2 AS a_times_2, filter: a * 2 > 4
        // should become: a_times_2 > 4

        // Create the computed expression: a@0 * 2
        let computed_expr: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("a", 0)),
            Operator::Multiply,
            Arc::new(Literal::new(ScalarValue::Int32(Some(2)))),
        ));

        // Create projection with the computed expression aliased as "a_times_2"
        let projection = vec![ProjectionExpr {
            expr: Arc::clone(&computed_expr),
            alias: "a_times_2".to_string(),
        }];

        // Create filter predicate: a * 2 > 4 (same expression as projection)
        let filter_predicate: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::clone(&computed_expr),
            Operator::Gt,
            Arc::new(Literal::new(ScalarValue::Int32(Some(4)))),
        ));

        // Update the expression - should rewrite a * 2 to a_times_2@0
        // sync_with_child=false because we want OUTPUT references (filter will be above projection)
        let result = update_expr(&filter_predicate, &projection, false)?;
        assert!(result.is_some(), "Filter predicate should be valid");

        let result_expr = result.unwrap();
        let binary = result_expr
            .as_any()
            .downcast_ref::<BinaryExpr>()
            .expect("Should be a BinaryExpr");
        // Left side should now be a column reference to a_times_2@0
        let left_col = binary
            .left()
            .as_any()
            .downcast_ref::<Column>()
            .expect("Left should be rewritten to a Column");
        assert_eq!(left_col.name(), "a_times_2");
        assert_eq!(left_col.index(), 0);

        // Right side should still be the literal 4
        let right_lit = binary
            .right()
            .as_any()
            .downcast_ref::<Literal>()
            .expect("Right should be a Literal");
        assert_eq!(right_lit.value(), &ScalarValue::Int32(Some(4)));

        Ok(())
    }

    #[test]
    fn test_update_expr_partial_match() -> Result<()> {
        // Test that when only part of an expression matches, we still handle
        // the rest correctly. e.g., `a + b > 2 AND c > 3` with projection
        // `a + b AS sum_ab, c AS c_out` should become `sum_ab > 2 AND c_out > 3`

        // Create computed expression: a@0 + b@1
        let sum_expr: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("a", 0)),
            Operator::Plus,
            Arc::new(Column::new("b", 1)),
        ));

        // Projection: [a + b AS sum_ab, c AS c_out]
        let projection = vec![
            ProjectionExpr {
                expr: Arc::clone(&sum_expr),
                alias: "sum_ab".to_string(),
            },
            ProjectionExpr {
                expr: Arc::new(Column::new("c", 2)),
                alias: "c_out".to_string(),
            },
        ];

        // Filter: (a + b > 2) AND (c > 3)
        let filter_predicate: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(BinaryExpr::new(
                Arc::clone(&sum_expr),
                Operator::Gt,
                Arc::new(Literal::new(ScalarValue::Int32(Some(2)))),
            )),
            Operator::And,
            Arc::new(BinaryExpr::new(
                Arc::new(Column::new("c", 2)),
                Operator::Gt,
                Arc::new(Literal::new(ScalarValue::Int32(Some(3)))),
            )),
        ));

        // With sync_with_child=false: columns reference input schema, need to map to output
        let result = update_expr(&filter_predicate, &projection, false)?;
        assert!(result.is_some(), "Filter predicate should be valid");

        let result_expr = result.unwrap();
        // Should be: sum_ab@0 > 2 AND c_out@1 > 3
        assert_eq!(result_expr.to_string(), "sum_ab@0 > 2 AND c_out@1 > 3");

        Ok(())
    }

    #[test]
    fn test_update_expr_partial_match_with_unresolved_column() -> Result<()> {
        // Test that when part of an expression matches but other columns can't be
        // resolved, we return None. e.g., `a + b > 2 AND c > 3` with projection
        // `a + b AS sum_ab` (note: no 'c' column!) should return None.

        // Create computed expression: a@0 + b@1
        let sum_expr: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("a", 0)),
            Operator::Plus,
            Arc::new(Column::new("b", 1)),
        ));

        // Projection: [a + b AS sum_ab] - note: NO 'c' column!
        let projection = vec![ProjectionExpr {
            expr: Arc::clone(&sum_expr),
            alias: "sum_ab".to_string(),
        }];

        // Filter: (a + b > 2) AND (c > 3) - 'c' is not in projection!
        let filter_predicate: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(BinaryExpr::new(
                Arc::clone(&sum_expr),
                Operator::Gt,
                Arc::new(Literal::new(ScalarValue::Int32(Some(2)))),
            )),
            Operator::And,
            Arc::new(BinaryExpr::new(
                Arc::new(Column::new("c", 2)),
                Operator::Gt,
                Arc::new(Literal::new(ScalarValue::Int32(Some(3)))),
            )),
        ));

        // With sync_with_child=false: should return None because 'c' can't be mapped
        let result = update_expr(&filter_predicate, &projection, false)?;
        assert!(
            result.is_none(),
            "Should return None when some columns can't be resolved"
        );

        // On the other hand if the projection is `c AS c_out, a + b AS sum_ab` we should succeed
        let projection = vec![
            ProjectionExpr {
                expr: Arc::new(Column::new("c", 2)),
                alias: "c_out".to_string(),
            },
            ProjectionExpr {
                expr: Arc::clone(&sum_expr),
                alias: "sum_ab".to_string(),
            },
        ];
        let result = update_expr(&filter_predicate, &projection, false)?;
        assert!(result.is_some(), "Filter predicate should be valid now");

        Ok(())
    }

    #[test]
    fn test_update_expr_nested_match() -> Result<()> {
        // Test matching a sub-expression within a larger expression.
        // e.g., `(a + b) * 2 > 10` with projection `a + b AS sum_ab`
        // should become `sum_ab * 2 > 10`

        // Create computed expression: a@0 + b@1
        let sum_expr: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("a", 0)),
            Operator::Plus,
            Arc::new(Column::new("b", 1)),
        ));

        // Projection: [a + b AS sum_ab]
        let projection = vec![ProjectionExpr {
            expr: Arc::clone(&sum_expr),
            alias: "sum_ab".to_string(),
        }];

        // Filter: (a + b) * 2 > 10
        let filter_predicate: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(BinaryExpr::new(
                Arc::clone(&sum_expr),
                Operator::Multiply,
                Arc::new(Literal::new(ScalarValue::Int32(Some(2)))),
            )),
            Operator::Gt,
            Arc::new(Literal::new(ScalarValue::Int32(Some(10)))),
        ));

        // With sync_with_child=false: should rewrite a+b to sum_ab
        let result = update_expr(&filter_predicate, &projection, false)?;
        assert!(result.is_some(), "Filter predicate should be valid");

        let result_expr = result.unwrap();
        // Should be: sum_ab@0 * 2 > 10
        assert_eq!(result_expr.to_string(), "sum_ab@0 * 2 > 10");

        Ok(())
    }

    #[test]
    fn test_update_expr_no_match_returns_none() -> Result<()> {
        // Test that when columns can't be resolved, we return None (with sync_with_child=false)

        // Projection: [a AS a_out]
        let projection = vec![ProjectionExpr {
            expr: Arc::new(Column::new("a", 0)),
            alias: "a_out".to_string(),
        }];

        // Filter references column 'd' which is not in projection
        let filter_predicate: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("d", 3)), // Not in projection
            Operator::Gt,
            Arc::new(Literal::new(ScalarValue::Int32(Some(5)))),
        ));

        // With sync_with_child=false: should return None because 'd' can't be mapped
        let result = update_expr(&filter_predicate, &projection, false)?;
        assert!(
            result.is_none(),
            "Should return None when column can't be resolved"
        );

        Ok(())
    }

    #[test]
    fn test_update_expr_column_name_collision() -> Result<()> {
        // Regression test for a bug where an original column with the same (name, index)
        // as a pass-1-rewritten column would incorrectly be considered "already handled".
        //
        // Example from SQLite tests:
        // - Input schema: [col0, col1, col2]
        // - Projection: [col2 AS col0]  (col2@2 becomes col0@0)
        // - Filter: col0 - col2 <= col2 / col2
        //
        // The bug: when pass 1 rewrites col2@2 to col0@0, it added ("col0", 0) to
        // valid_columns. Then in pass 2, the ORIGINAL col0@0 in the filter would
        // match ("col0", 0) and be incorrectly skipped, resulting in:
        //   col0 - col0 <= col0 / col0 = 0 - 0 <= 0 / 0 = always true (or NaN)
        // instead of flagging the expression as invalid.

        // Projection: [col2 AS col0] - note the alias matches another input column's name!
        let projection = vec![ProjectionExpr {
            expr: Arc::new(Column::new("col2", 2)),
            alias: "col0".to_string(), // Alias collides with original col0!
        }];

        // Filter: col0@0 - col2@2 <= col2@2 / col2@2
        // After correct rewrite, col2@2 becomes col0@0 (via pass 1 match)
        // But col0@0 (the original) can't be resolved since the projection
        // doesn't include it - should return None
        let filter_predicate: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(BinaryExpr::new(
                Arc::new(Column::new("col0", 0)), // Original col0 - NOT in projection!
                Operator::Minus,
                Arc::new(Column::new("col2", 2)), // This will be rewritten to col0@0
            )),
            Operator::LtEq,
            Arc::new(BinaryExpr::new(
                Arc::new(Column::new("col2", 2)),
                Operator::Divide,
                Arc::new(Column::new("col2", 2)),
            )),
        ));

        // With sync_with_child=false: should return None because original col0@0
        // can't be resolved (only col2 is in projection, aliased as col0)
        let result = update_expr(&filter_predicate, &projection, false)?;
        assert!(
            result.is_none(),
            "Should return None when original column collides with rewritten alias but isn't in projection"
        );

        Ok(())
    }

    #[test]
    fn test_project_schema_simple_columns() -> Result<()> {
        // Input schema: [col0: Int64, col1: Utf8, col2: Float32]
        let input_schema = get_schema();

        // Projection: SELECT col2 AS c, col0 AS a
        let projection = ProjectionExprs::new(vec![
            ProjectionExpr {
                expr: Arc::new(Column::new("col2", 2)),
                alias: "c".to_string(),
            },
            ProjectionExpr {
                expr: Arc::new(Column::new("col0", 0)),
                alias: "a".to_string(),
            },
        ]);

        let output_schema = projection.project_schema(&input_schema)?;

        // Should have 2 fields
        assert_eq!(output_schema.fields().len(), 2);

        // First field should be "c" with Float32 type
        assert_eq!(output_schema.field(0).name(), "c");
        assert_eq!(output_schema.field(0).data_type(), &DataType::Float32);

        // Second field should be "a" with Int64 type
        assert_eq!(output_schema.field(1).name(), "a");
        assert_eq!(output_schema.field(1).data_type(), &DataType::Int64);

        Ok(())
    }

    #[test]
    fn test_project_schema_with_expressions() -> Result<()> {
        // Input schema: [col0: Int64, col1: Utf8, col2: Float32]
        let input_schema = get_schema();

        // Projection: SELECT col0 + 1 AS incremented
        let projection = ProjectionExprs::new(vec![ProjectionExpr {
            expr: Arc::new(BinaryExpr::new(
                Arc::new(Column::new("col0", 0)),
                Operator::Plus,
                Arc::new(Literal::new(ScalarValue::Int64(Some(1)))),
            )),
            alias: "incremented".to_string(),
        }]);

        let output_schema = projection.project_schema(&input_schema)?;

        // Should have 1 field
        assert_eq!(output_schema.fields().len(), 1);

        // Field should be "incremented" with Int64 type
        assert_eq!(output_schema.field(0).name(), "incremented");
        assert_eq!(output_schema.field(0).data_type(), &DataType::Int64);

        Ok(())
    }

    #[test]
    fn test_project_schema_preserves_metadata() -> Result<()> {
        // Create schema with metadata
        let mut metadata = HashMap::new();
        metadata.insert("key".to_string(), "value".to_string());
        let field_with_metadata =
            Field::new("col0", DataType::Int64, false).with_metadata(metadata.clone());
        let input_schema = Schema::new(vec![
            field_with_metadata,
            Field::new("col1", DataType::Utf8, false),
        ]);

        // Projection: SELECT col0 AS renamed
        let projection = ProjectionExprs::new(vec![ProjectionExpr {
            expr: Arc::new(Column::new("col0", 0)),
            alias: "renamed".to_string(),
        }]);

        let output_schema = projection.project_schema(&input_schema)?;

        // Should have 1 field
        assert_eq!(output_schema.fields().len(), 1);

        // Field should be "renamed" with metadata preserved
        assert_eq!(output_schema.field(0).name(), "renamed");
        assert_eq!(output_schema.field(0).metadata(), &metadata);

        Ok(())
    }

    #[test]
    fn test_project_schema_empty() -> Result<()> {
        let input_schema = get_schema();
        let projection = ProjectionExprs::new(vec![]);

        let output_schema = projection.project_schema(&input_schema)?;

        assert_eq!(output_schema.fields().len(), 0);

        Ok(())
    }

    #[test]
    fn test_project_statistics_columns_only() -> Result<()> {
        let input_stats = get_stats();
        let input_schema = get_schema();

        // Projection: SELECT col1 AS text, col0 AS num
        let projection = ProjectionExprs::new(vec![
            ProjectionExpr {
                expr: Arc::new(Column::new("col1", 1)),
                alias: "text".to_string(),
            },
            ProjectionExpr {
                expr: Arc::new(Column::new("col0", 0)),
                alias: "num".to_string(),
            },
        ]);

        let output_stats = projection.project_statistics(
            input_stats,
            &projection.project_schema(&input_schema)?,
        )?;

        // Row count should be preserved
        assert_eq!(output_stats.num_rows, Precision::Exact(5));

        // Should have 2 column statistics (reordered from input)
        assert_eq!(output_stats.column_statistics.len(), 2);

        // First column (col1 from input)
        assert_eq!(
            output_stats.column_statistics[0].distinct_count,
            Precision::Exact(1)
        );
        assert_eq!(
            output_stats.column_statistics[0].max_value,
            Precision::Exact(ScalarValue::from("x"))
        );

        // Second column (col0 from input)
        assert_eq!(
            output_stats.column_statistics[1].distinct_count,
            Precision::Exact(5)
        );
        assert_eq!(
            output_stats.column_statistics[1].max_value,
            Precision::Exact(ScalarValue::Int64(Some(21)))
        );

        Ok(())
    }

    #[test]
    fn test_project_statistics_with_expressions() -> Result<()> {
        let input_stats = get_stats();
        let input_schema = get_schema();

        // Projection with expression: SELECT col0 + 1 AS incremented, col1 AS text
        let projection = ProjectionExprs::new(vec![
            ProjectionExpr {
                expr: Arc::new(BinaryExpr::new(
                    Arc::new(Column::new("col0", 0)),
                    Operator::Plus,
                    Arc::new(Literal::new(ScalarValue::Int64(Some(1)))),
                )),
                alias: "incremented".to_string(),
            },
            ProjectionExpr {
                expr: Arc::new(Column::new("col1", 1)),
                alias: "text".to_string(),
            },
        ]);

        let output_stats = projection.project_statistics(
            input_stats,
            &projection.project_schema(&input_schema)?,
        )?;

        // Row count should be preserved
        assert_eq!(output_stats.num_rows, Precision::Exact(5));

        // Should have 2 column statistics
        assert_eq!(output_stats.column_statistics.len(), 2);

        // First column (expression) should have unknown statistics
        assert_eq!(
            output_stats.column_statistics[0].distinct_count,
            Precision::Absent
        );
        assert_eq!(
            output_stats.column_statistics[0].max_value,
            Precision::Absent
        );

        // Second column (col1) should preserve statistics
        assert_eq!(
            output_stats.column_statistics[1].distinct_count,
            Precision::Exact(1)
        );

        Ok(())
    }

    #[test]
    fn test_project_statistics_primitive_width_only() -> Result<()> {
        let input_stats = get_stats();
        let input_schema = get_schema();

        // Projection with only primitive width columns: SELECT col2 AS f, col0 AS i
        let projection = ProjectionExprs::new(vec![
            ProjectionExpr {
                expr: Arc::new(Column::new("col2", 2)),
                alias: "f".to_string(),
            },
            ProjectionExpr {
                expr: Arc::new(Column::new("col0", 0)),
                alias: "i".to_string(),
            },
        ]);

        let output_stats = projection.project_statistics(
            input_stats,
            &projection.project_schema(&input_schema)?,
        )?;

        // Row count should be preserved
        assert_eq!(output_stats.num_rows, Precision::Exact(5));

        // Total byte size should be recalculated for primitive types
        // Float32 (4 bytes) + Int64 (8 bytes) = 12 bytes per row, 5 rows = 60 bytes
        assert_eq!(output_stats.total_byte_size, Precision::Exact(60));

        // Should have 2 column statistics
        assert_eq!(output_stats.column_statistics.len(), 2);

        Ok(())
    }

    #[test]
    fn test_project_statistics_empty() -> Result<()> {
        let input_stats = get_stats();
        let input_schema = get_schema();

        let projection = ProjectionExprs::new(vec![]);

        let output_stats = projection.project_statistics(
            input_stats,
            &projection.project_schema(&input_schema)?,
        )?;

        // Row count should be preserved
        assert_eq!(output_stats.num_rows, Precision::Exact(5));

        // Should have no column statistics
        assert_eq!(output_stats.column_statistics.len(), 0);

        // Total byte size should be 0 for empty projection
        assert_eq!(output_stats.total_byte_size, Precision::Exact(0));

        Ok(())
    }

    // Test statistics calculation for non-null literal (numeric constant)
    #[test]
    fn test_project_statistics_with_literal() -> Result<()> {
        let input_stats = get_stats();
        let input_schema = get_schema();

        // Projection with literal: SELECT 42 AS constant, col0 AS num
        let projection = ProjectionExprs::new(vec![
            ProjectionExpr {
                expr: Arc::new(Literal::new(ScalarValue::Int64(Some(42)))),
                alias: "constant".to_string(),
            },
            ProjectionExpr {
                expr: Arc::new(Column::new("col0", 0)),
                alias: "num".to_string(),
            },
        ]);

        let output_stats = projection.project_statistics(
            input_stats,
            &projection.project_schema(&input_schema)?,
        )?;

        // Row count should be preserved
        assert_eq!(output_stats.num_rows, Precision::Exact(5));

        // Should have 2 column statistics
        assert_eq!(output_stats.column_statistics.len(), 2);

        // First column (literal 42) should have proper constant statistics
        assert_eq!(
            output_stats.column_statistics[0].min_value,
            Precision::Exact(ScalarValue::Int64(Some(42)))
        );
        assert_eq!(
            output_stats.column_statistics[0].max_value,
            Precision::Exact(ScalarValue::Int64(Some(42)))
        );
        assert_eq!(
            output_stats.column_statistics[0].distinct_count,
            Precision::Exact(1)
        );
        assert_eq!(
            output_stats.column_statistics[0].null_count,
            Precision::Exact(0)
        );
        // Int64 is 8 bytes, 5 rows = 40 bytes
        assert_eq!(
            output_stats.column_statistics[0].byte_size,
            Precision::Exact(40)
        );
        // For a constant column, sum_value = value * num_rows = 42 * 5 = 210
        assert_eq!(
            output_stats.column_statistics[0].sum_value,
            Precision::Exact(ScalarValue::Int64(Some(210)))
        );

        // Second column (col0) should preserve statistics
        assert_eq!(
            output_stats.column_statistics[1].distinct_count,
            Precision::Exact(5)
        );
        assert_eq!(
            output_stats.column_statistics[1].max_value,
            Precision::Exact(ScalarValue::Int64(Some(21)))
        );

        Ok(())
    }

    // Test statistics calculation for NULL literal (constant NULL column)
    #[test]
    fn test_project_statistics_with_null_literal() -> Result<()> {
        let input_stats = get_stats();
        let input_schema = get_schema();

        // Projection with NULL literal: SELECT NULL AS null_col, col0 AS num
        let projection = ProjectionExprs::new(vec![
            ProjectionExpr {
                expr: Arc::new(Literal::new(ScalarValue::Int64(None))),
                alias: "null_col".to_string(),
            },
            ProjectionExpr {
                expr: Arc::new(Column::new("col0", 0)),
                alias: "num".to_string(),
            },
        ]);

        let output_stats = projection.project_statistics(
            input_stats,
            &projection.project_schema(&input_schema)?,
        )?;

        // Row count should be preserved
        assert_eq!(output_stats.num_rows, Precision::Exact(5));

        // Should have 2 column statistics
        assert_eq!(output_stats.column_statistics.len(), 2);

        // First column (NULL literal) should have proper constant NULL statistics
        assert_eq!(
            output_stats.column_statistics[0].min_value,
            Precision::Exact(ScalarValue::Int64(None))
        );
        assert_eq!(
            output_stats.column_statistics[0].max_value,
            Precision::Exact(ScalarValue::Int64(None))
        );
        assert_eq!(
            output_stats.column_statistics[0].distinct_count,
            Precision::Exact(1) // All NULLs are considered the same
        );
        assert_eq!(
            output_stats.column_statistics[0].null_count,
            Precision::Exact(5) // All rows are NULL
        );
        assert_eq!(
            output_stats.column_statistics[0].byte_size,
            Precision::Exact(0)
        );
        assert_eq!(
            output_stats.column_statistics[0].sum_value,
            Precision::Exact(ScalarValue::Int64(None))
        );

        // Second column (col0) should preserve statistics
        assert_eq!(
            output_stats.column_statistics[1].distinct_count,
            Precision::Exact(5)
        );
        assert_eq!(
            output_stats.column_statistics[1].max_value,
            Precision::Exact(ScalarValue::Int64(Some(21)))
        );

        Ok(())
    }

    // Test statistics calculation for complex type literal (e.g., Utf8 string)
    #[test]
    fn test_project_statistics_with_complex_type_literal() -> Result<()> {
        let input_stats = get_stats();
        let input_schema = get_schema();

        // Projection with Utf8 literal (complex type): SELECT 'hello' AS text, col0 AS num
        let projection = ProjectionExprs::new(vec![
            ProjectionExpr {
                expr: Arc::new(Literal::new(ScalarValue::Utf8(Some(
                    "hello".to_string(),
                )))),
                alias: "text".to_string(),
            },
            ProjectionExpr {
                expr: Arc::new(Column::new("col0", 0)),
                alias: "num".to_string(),
            },
        ]);

        let output_stats = projection.project_statistics(
            input_stats,
            &projection.project_schema(&input_schema)?,
        )?;

        // Row count should be preserved
        assert_eq!(output_stats.num_rows, Precision::Exact(5));

        // Should have 2 column statistics
        assert_eq!(output_stats.column_statistics.len(), 2);

        // First column (Utf8 literal 'hello') should have proper constant statistics
        // but byte_size should be Absent for complex types
        assert_eq!(
            output_stats.column_statistics[0].min_value,
            Precision::Exact(ScalarValue::Utf8(Some("hello".to_string())))
        );
        assert_eq!(
            output_stats.column_statistics[0].max_value,
            Precision::Exact(ScalarValue::Utf8(Some("hello".to_string())))
        );
        assert_eq!(
            output_stats.column_statistics[0].distinct_count,
            Precision::Exact(1)
        );
        assert_eq!(
            output_stats.column_statistics[0].null_count,
            Precision::Exact(0)
        );
        // Complex types (Utf8, List, etc.) should have byte_size = Absent
        // because we can't calculate exact size without knowing the actual data
        assert_eq!(
            output_stats.column_statistics[0].byte_size,
            Precision::Absent
        );
        // Non-numeric types (Utf8) should have sum_value = Absent
        // because sum is only meaningful for numeric types
        assert_eq!(
            output_stats.column_statistics[0].sum_value,
            Precision::Absent
        );

        // Second column (col0) should preserve statistics
        assert_eq!(
            output_stats.column_statistics[1].distinct_count,
            Precision::Exact(5)
        );
        assert_eq!(
            output_stats.column_statistics[1].max_value,
            Precision::Exact(ScalarValue::Int64(Some(21)))
        );

        Ok(())
    }
}
