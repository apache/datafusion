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

use arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion_common::stats::{ColumnStatistics, Precision};
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::{internal_datafusion_err, internal_err, plan_err, Result};

use datafusion_physical_expr_common::sort_expr::{LexOrdering, PhysicalSortExpr};
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
#[derive(Debug, Clone)]
pub struct Projection {
    exprs: Vec<ProjectionExpr>,
}

impl From<Vec<ProjectionExpr>> for Projection {
    fn from(value: Vec<ProjectionExpr>) -> Self {
        Self { exprs: value }
    }
}

impl From<&[ProjectionExpr]> for Projection {
    fn from(value: &[ProjectionExpr]) -> Self {
        Self {
            exprs: value.to_vec(),
        }
    }
}

impl AsRef<[ProjectionExpr]> for Projection {
    fn as_ref(&self) -> &[ProjectionExpr] {
        &self.exprs
    }
}

impl Projection {
    pub fn new(exprs: Vec<ProjectionExpr>) -> Self {
        Self { exprs }
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

    /// Apply another projection on top of this projection, returning the combined projection.
    /// For example, if this projection is `SELECT c@2 AS x, b@1 AS y, a@0 as z` and the other projection is `SELECT x@0 + 1 AS c1, y@1 + z@2 as c2`,
    /// we return a projection equivalent to `SELECT c@2 + 1 AS c1, b@1 + a@0 as c2`.
    ///
    /// # Errors
    /// This function returns an error if any expression in the `other` projection cannot be
    /// applied on top of this projection.
    pub fn try_merge(&self, other: &Projection) -> Result<Projection> {
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
        Ok(Projection::new(new_exprs))
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

impl<'a> IntoIterator for &'a Projection {
    type Item = &'a ProjectionExpr;
    type IntoIter = std::slice::Iter<'a, ProjectionExpr>;

    fn into_iter(self) -> Self::IntoIter {
        self.exprs.iter()
    }
}

impl IntoIterator for Projection {
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

    Ok((state == RewriteState::RewrittenValid).then_some(new_expr))
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
                    if col.name() != matching_name {
                        return internal_err!(
                            "Input field name {} does not match with the projection expression {}",
                            matching_name,
                            col.name()
                        );
                    }
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
    use super::*;
    use crate::equivalence::{convert_to_orderings, EquivalenceProperties};
    use crate::expressions::{col, BinaryExpr};
    use crate::utils::tests::TestScalarUDF;
    use crate::{PhysicalExprRef, ScalarFunctionExpr};

    use arrow::compute::SortOptions;
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion_common::config::ConfigOptions;
    use datafusion_expr::{Operator, ScalarUDF};

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

        let test_cases = vec![
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
}
