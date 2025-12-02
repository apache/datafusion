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

use std::ops::Deref;
use std::sync::Arc;

use crate::expressions::Column;
use crate::utils::collect_columns;
use crate::PhysicalExpr;

use arrow::array::{RecordBatch, RecordBatchOptions};
use arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion_common::stats::{ColumnStatistics, Precision};
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::{
    assert_or_internal_err, internal_datafusion_err, plan_err, Result,
};

use datafusion_physical_expr_common::sort_expr::{LexOrdering, PhysicalSortExpr};
use datafusion_physical_expr_common::utils::evaluate_expressions_to_arrays;
use indexmap::IndexMap;
use itertools::Itertools;

/// A projection expression as used by projection operations.
///
/// The expression is evaluated and the result is stored in a column
/// with the name specified by `alias`.
///
/// For example, the SQL expression `a + b AS sum_ab` would be represented
/// as a `ProjectionExpr` where `expr` is the expression `a + b`
/// and `alias` is the string `sum_ab`.
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
    pub fn new(expr: Arc<dyn PhysicalExpr>, alias: String) -> Self {
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

/// A collection of projection expressions.
///
/// This struct encapsulates multiple `ProjectionExpr` instances,
/// representing a complete projection operation and provides
/// methods to manipulate and analyze the projection as a whole.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionExprs {
    exprs: Vec<ProjectionExpr>,
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
        })
    }

    /// Project statistics according to this projection.
    /// For example, for a projection `SELECT a AS x, b + 1 AS y`, where `a` is at index 0 and `b` is at index 1,
    /// if the input statistics has column statistics for columns `a`, `b`, and `c`, the output statistics would have column statistics for columns `x` and `y`.
    pub fn project_statistics(
        &self,
        mut stats: datafusion_common::Statistics,
        input_schema: &Schema,
    ) -> Result<datafusion_common::Statistics> {
        let mut primitive_row_size = 0;
        let mut primitive_row_size_possible = true;
        let mut column_statistics = vec![];

        for proj_expr in &self.exprs {
            let expr = &proj_expr.expr;
            let col_stats = if let Some(col) = expr.as_any().downcast_ref::<Column>() {
                stats.column_statistics[col.index()].clone()
            } else {
                // TODO stats: estimate more statistics from expressions
                // (expressions should compute their statistics themselves)
                ColumnStatistics::new_unknown()
            };
            column_statistics.push(col_stats);
            let data_type = expr.data_type(input_schema)?;
            if let Some(value) = data_type.primitive_width() {
                primitive_row_size += value;
                continue;
            }
            primitive_row_size_possible = false;
        }

        if primitive_row_size_possible {
            stats.total_byte_size =
                Precision::Exact(primitive_row_size).multiply(&stats.num_rows);
        }
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
}

impl Projector {
    /// Project a record batch according to this projector's expressions.
    ///
    /// # Errors
    /// This function returns an error if any expression evaluation fails
    /// or if the output schema of the resulting record batch does not match
    /// the pre-computed output schema of the projector.
    pub fn project_batch(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        let arrays = evaluate_expressions_to_arrays(
            self.projection.exprs.iter().map(|p| &p.expr),
            batch,
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
    #[derive(Debug, PartialEq)]
    enum RewriteState {
        /// The expression is unchanged.
        Unchanged,
        /// Some part of the expression has been rewritten
        RewrittenValid,
        /// Some part of the expression has been rewritten, but some column
        /// references could not be.
        RewrittenInvalid,
    }

    let mut state = RewriteState::Unchanged;

    let new_expr = Arc::clone(expr)
        .transform_up(|expr| {
            if state == RewriteState::RewrittenInvalid {
                return Ok(Transformed::no(expr));
            }

            let Some(column) = expr.as_any().downcast_ref::<Column>() else {
                return Ok(Transformed::no(expr));
            };
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
    use crate::equivalence::{convert_to_orderings, EquivalenceProperties};
    use crate::expressions::{col, BinaryExpr, Literal};
    use crate::utils::tests::TestScalarUDF;
    use crate::{PhysicalExprRef, ScalarFunctionExpr};

    use arrow::compute::SortOptions;
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion_common::config::ConfigOptions;
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
                },
                ColumnStatistics {
                    distinct_count: Precision::Exact(1),
                    max_value: Precision::Exact(ScalarValue::from("x")),
                    min_value: Precision::Exact(ScalarValue::from("a")),
                    sum_value: Precision::Absent,
                    null_count: Precision::Exact(3),
                },
                ColumnStatistics {
                    distinct_count: Precision::Absent,
                    max_value: Precision::Exact(ScalarValue::Float32(Some(1.1))),
                    min_value: Precision::Exact(ScalarValue::Float32(Some(0.1))),
                    sum_value: Precision::Exact(ScalarValue::Float32(Some(5.5))),
                    null_count: Precision::Absent,
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

        let result = projection.project_statistics(source, &schema).unwrap();

        let expected = Statistics {
            num_rows: Precision::Exact(5),
            total_byte_size: Precision::Exact(23),
            column_statistics: vec![
                ColumnStatistics {
                    distinct_count: Precision::Exact(1),
                    max_value: Precision::Exact(ScalarValue::from("x")),
                    min_value: Precision::Exact(ScalarValue::from("a")),
                    sum_value: Precision::Absent,
                    null_count: Precision::Exact(3),
                },
                ColumnStatistics {
                    distinct_count: Precision::Exact(5),
                    max_value: Precision::Exact(ScalarValue::Int64(Some(21))),
                    min_value: Precision::Exact(ScalarValue::Int64(Some(-4))),
                    sum_value: Precision::Exact(ScalarValue::Int64(Some(42))),
                    null_count: Precision::Exact(0),
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

        let result = projection.project_statistics(source, &schema).unwrap();

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
                },
                ColumnStatistics {
                    distinct_count: Precision::Exact(5),
                    max_value: Precision::Exact(ScalarValue::Int64(Some(21))),
                    min_value: Precision::Exact(ScalarValue::Int64(Some(-4))),
                    sum_value: Precision::Exact(ScalarValue::Int64(Some(42))),
                    null_count: Precision::Exact(0),
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

        let output_stats = projection.project_statistics(input_stats, &input_schema)?;

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

        let output_stats = projection.project_statistics(input_stats, &input_schema)?;

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

        let output_stats = projection.project_statistics(input_stats, &input_schema)?;

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

        let output_stats = projection.project_statistics(input_stats, &input_schema)?;

        // Row count should be preserved
        assert_eq!(output_stats.num_rows, Precision::Exact(5));

        // Should have no column statistics
        assert_eq!(output_stats.column_statistics.len(), 0);

        // Total byte size should be 0 for empty projection
        assert_eq!(output_stats.total_byte_size, Precision::Exact(0));

        Ok(())
    }
}
