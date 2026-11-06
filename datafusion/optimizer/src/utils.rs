// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Utility functions leveraged by the query optimizer rules

use std::collections::{BTreeSet, HashMap, HashSet};

use crate::analyzer::type_coercion::TypeCoercionRewriter;
use arrow::array::{Array, RecordBatch, new_null_array};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion_common::TableReference;
use datafusion_common::cast::as_boolean_array;
use datafusion_common::tree_node::{
    Transformed, TransformedResult, TreeNode, TreeNodeRecursion,
};
use datafusion_common::{
    Column, ColumnId, ColumnIdGenerator, DFSchema, Result, ScalarValue,
};
use datafusion_expr::execution_props::ExecutionProps;
use datafusion_expr::expr::{Exists, InSubquery, SetComparison};
use datafusion_expr::expr_rewriter::replace_col;
use datafusion_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion_expr::{
    Aggregate, ColumnarValue, DistinctHandling, Expr, LogicalPlanBuilder, NamedExpr,
    Projection, WriteOp, logical_plan::LogicalPlan,
};

use crate::OptimizerConfig;
use datafusion_physical_expr::create_physical_expr;
use log::{debug, trace};
use std::sync::Arc;

/// Re-export of `NamesPreserver` for backwards compatibility,
/// as it was initially placed here and then moved elsewhere.
pub use datafusion_expr::expr_rewriter::NamePreserver;

/// Returns `plan`, whose expressions a rule rewrote, with its schema
/// recomputed if its outputs are named after its expressions (a projection,
/// an aggregate, a window or a `DISTINCT ON`).
///
/// A rewrite keeps the ids of the outputs, and may rename them: nothing in
/// the plan refers to an output by name. The optimizer restores the names of
/// the root's outputs, and relabels the references when it ends.
pub(crate) fn with_recomputed_output_names(
    plan: Transformed<LogicalPlan>,
) -> Result<Transformed<LogicalPlan>> {
    if !plan.transformed {
        return Ok(plan);
    }
    plan.map_data(|plan| match plan {
        LogicalPlan::Projection(_)
        | LogicalPlan::Aggregate(_)
        | LogicalPlan::Window(_)
        | LogicalPlan::Distinct(datafusion_expr::Distinct::On(_)) => {
            plan.recompute_schema()
        }
        plan => Ok(plan),
    })
}

/// Whether an expression is free of volatile functions and subqueries.
/// Subqueries are conservative barriers because their plans may contain
/// volatile expressions that [`Expr::is_volatile`] does not visit.
/// A [`LogicalPlanBuilder`] of `plan` that allocates column ids with the
/// generator of `config`: the outputs that a rule builds with it get ids that
/// do not collide with the rest of the plan
pub(crate) fn plan_builder(
    plan: impl Into<Arc<LogicalPlan>>,
    config: &dyn OptimizerConfig,
) -> LogicalPlanBuilder {
    LogicalPlanBuilder::new_from_arc(plan.into())
        .with_column_id_generator(Some(Arc::clone(config.column_id_generator())))
}

/// A [`LogicalPlanBuilder`] of `plan` that allocates column ids with `ids`
/// (the generator of the optimizer config, see [`plan_builder`])
pub(crate) fn ids_builder(
    ids: &Arc<ColumnIdGenerator>,
    plan: impl Into<Arc<LogicalPlan>>,
) -> LogicalPlanBuilder {
    LogicalPlanBuilder::new_from_arc(plan.into())
        .with_column_id_generator(Some(Arc::clone(ids)))
}

/// Returns `plan`, a projection whose first outputs rebuild the outputs
/// `entries` (one output per entry, in the same order, possibly followed by
/// outputs it adds), where each rebuilt output keeps the id of its entry (see
/// [`NamedExpr::with_rewritten_expr`])
pub(crate) fn with_output_ids_of(
    plan: LogicalPlan,
    entries: &[NamedExpr],
    config: &dyn OptimizerConfig,
) -> Result<LogicalPlan> {
    let LogicalPlan::Projection(projection) = plan else {
        return Ok(plan);
    };
    if projection.expr.len() < entries.len() {
        return Ok(LogicalPlan::Projection(projection));
    }
    let mut entries = entries.iter();
    let expr = projection
        .expr
        .into_iter()
        .map(|new| match entries.next() {
            Some(entry) => entry
                .clone()
                .with_rewritten_expr(new.expr, config.column_id_generator()),
            None => new,
        })
        .collect();
    Projection::try_new_with_schema(expr, projection.input, projection.schema)
        .map(LogicalPlan::Projection)
}

/// Returns a projection over `plan` that passes its outputs through, except
/// those for which `redefine(i, column)` (`column` reads output `i`) returns
/// an expression: that output is computed by the expression, and keeps the id
/// the output of `plan` had, which the plan above refers to. The column it
/// replaces gets a new id inside `plan` (see
/// [`LogicalPlan::with_output_column_ids`]), and the references of the new
/// expressions follow it. References without an id in the new expressions
/// are bound by name to the outputs of `plan`.
///
/// Used where a rule wraps the columns of a plan that replaces a node (count
/// bug compensation): the plan above reads the wrapped values.
pub(crate) fn project_redefining_outputs(
    plan: LogicalPlan,
    ids: &ColumnIdGenerator,
    mut redefine: impl FnMut(usize, Expr) -> Result<Option<Expr>>,
) -> Result<LogicalPlan> {
    let schema = Arc::clone(plan.schema());
    let columns = schema.columns();
    let bind = |expr: Expr| -> Result<Expr> {
        expr.transform(|expr| match expr {
            Expr::Column(col) if col.id.is_none() => {
                let id = schema
                    .maybe_index_of_column(&col)
                    .and_then(|i| schema.field_id(i));
                Ok(Transformed::new_transformed(
                    Expr::Column(col.with_id(id)),
                    id.is_some(),
                ))
            }
            _ => Ok(Transformed::no(expr)),
        })
        .data()
    };
    let mut redefined = Vec::with_capacity(columns.len());
    for (i, column) in columns.iter().enumerate() {
        redefined.push(
            redefine(i, Expr::Column(column.clone()))?
                .map(bind)
                .transpose()?,
        );
    }
    if redefined.iter().all(Option::is_none) {
        return Ok(plan);
    }
    // the redefined outputs of `plan` take new ids
    let defined = plan.defined_column_ids();
    let mut renames: HashMap<ColumnId, ColumnId> = HashMap::new();
    let new_ids: Vec<Option<ColumnId>> = schema
        .field_ids()
        .iter()
        .zip(&redefined)
        .map(|(id, expr)| match (id, expr) {
            (Some(id), Some(_)) if defined.contains(id) => {
                Some(*renames.entry(*id).or_insert_with(|| ids.next()))
            }
            _ => *id,
        })
        .collect();
    let plan = plan.with_output_column_ids(&new_ids)?.data;
    let renamed_schema = Arc::clone(plan.schema());
    let rename = |expr: Expr| -> Result<Expr> {
        expr.transform(|expr| match expr {
            Expr::Column(col) => match col.id.and_then(|id| renames.get(&id)) {
                Some(new_id) => {
                    let new_id = Some(*new_id);
                    Ok(Transformed::yes(Expr::Column(col.with_id(new_id))))
                }
                None => Ok(Transformed::no(Expr::Column(col))),
            },
            _ => Ok(Transformed::no(expr)),
        })
        .data()
    };
    let entries = redefined
        .into_iter()
        .enumerate()
        .map(|(i, expr)| {
            let old_id = schema.field_id(i);
            let new_id = renamed_schema.field_id(i);
            match expr {
                // the output keeps its id if the column below was renamed
                Some(expr) => {
                    let id = match (old_id, new_id) {
                        (Some(old), Some(new)) if old != new => old,
                        _ => ids.next(),
                    };
                    Ok(NamedExpr::new(rename(expr)?, id))
                }
                None => {
                    let column = Expr::Column(
                        Column::from(renamed_schema.qualified_field(i)).with_id(new_id),
                    );
                    Ok(match new_id {
                        Some(id) => NamedExpr::new(column, id),
                        None => NamedExpr::allocate(column, &renamed_schema, ids),
                    })
                }
            }
        })
        .collect::<Result<Vec<_>>>()?;
    Projection::try_new(entries, Arc::new(plan)).map(LogicalPlan::Projection)
}

/// Returns `plan`, an aggregate that rebuilds `old` with possibly more
/// grouping expressions (the aggregate expressions of `old` first, possibly
/// followed by more), where the outputs it keeps keep their ids: a grouping
/// expression equal to one of `old`, the grouping id column, and
/// the aggregate expressions of `old`, by position (see
/// [`NamedExpr::with_rewritten_expr`])
pub(crate) fn with_aggregate_ids_of(
    plan: LogicalPlan,
    old: &Aggregate,
    generator: &ColumnIdGenerator,
) -> Result<LogicalPlan> {
    let LogicalPlan::Aggregate(aggregate) = plan else {
        return Ok(plan);
    };
    if aggregate.aggr_expr.len() < old.aggr_expr.len() {
        return Ok(LogicalPlan::Aggregate(aggregate));
    }
    let Aggregate {
        input,
        group_expr,
        grouping_sets,
        aggr_expr,
        schema,
        ..
    } = aggregate;
    let group_expr = group_expr
        .into_iter()
        .map(
            |new| match old.group_expr.iter().find(|entry| entry.expr == new.expr) {
                Some(entry) => entry.clone().with_rewritten_expr(new.expr, generator),
                None => new,
            },
        )
        .collect();
    // the grouping id column keeps its id
    let grouping_sets = grouping_sets.map(|mut grouping_sets| {
        if let Some(old) = &old.grouping_sets {
            grouping_sets.grouping_id = old.grouping_id;
        }
        grouping_sets
    });
    let mut old_aggr = old.aggr_expr.iter();
    let aggr_expr = aggr_expr
        .into_iter()
        .map(|new| match old_aggr.next() {
            Some(entry) => entry.clone().with_rewritten_expr(new.expr, generator),
            None => new,
        })
        .collect();
    Aggregate::try_new_with_schema(input, group_expr, grouping_sets, aggr_expr, schema)
        .map(LogicalPlan::Aggregate)
}

pub(crate) fn is_repeatable(expr: &Expr) -> bool {
    !expr
        .exists(|expr| {
            Ok(expr.is_volatile_node()
                || matches!(
                    expr,
                    Expr::Exists(_)
                        | Expr::InSubquery(_)
                        | Expr::SetComparison(_)
                        | Expr::ScalarSubquery(_)
                ))
        })
        .expect("expression traversal is infallible")
}

/// Whether an aggregate can safely ignore repeated input rows. This holds when
/// the function declares [`DistinctHandling::Insensitive`], or when it is
/// called with `DISTINCT` and declares [`DistinctHandling::Sensitive`], so that
/// its accumulator removes the repeated rows itself. Arguments, FILTER, and
/// ORDER BY must also be repeatable: MIN(random()) still observes repetitions.
pub(crate) fn is_duplicate_insensitive_aggregate(mut expr: &Expr) -> bool {
    while let Expr::Alias(alias) = expr {
        expr = &alias.expr;
    }
    let Expr::AggregateFunction(aggregate) = expr else {
        return false;
    };
    let ignores_duplicates = match aggregate.func.distinct_handling() {
        DistinctHandling::Insensitive => true,
        DistinctHandling::Sensitive => aggregate.params.distinct,
        // The accumulator does not implement `DISTINCT` and may silently
        // compute the non-distinct answer, so the flag proves nothing.
        // Variants added in the future are treated the same way.
        _ => false,
    };
    ignores_duplicates && is_repeatable(expr)
}

/// Return the expression schema for a MERGE DML node.
pub(crate) fn merge_into_schema(plan: &LogicalPlan) -> Result<Option<DFSchema>> {
    let LogicalPlan::Dml(dml) = plan else {
        return Ok(None);
    };
    let WriteOp::MergeInto(_) = &dml.op else {
        return Ok(None);
    };
    dml.merge_schema().map(Some)
}

/// Invokes `f` with the index, within `schema`, of every column referenced by
/// `expr` — including columns reached through a correlated subquery's outer
/// references. Columns absent from `schema` are skipped.
///
/// A subquery's own plan is intentionally not traversed: its internal columns
/// index into its own schema, not `schema`; only the outer (correlated) columns
/// it references from `schema` are relevant. The comparison expression of an
/// `IN`/set-comparison subquery is reached by the normal expression walk.
///
/// This is the shared primitive behind the top-down "which of a node's output
/// columns does an ancestor still need" analyses, namely
/// [`OptimizeProjections`](crate::optimize_projections::OptimizeProjections)
/// and [`EliminateJoin`](crate::eliminate_join::EliminateJoin). The two keep
/// their own required-index containers (an ordered set vs. a hash set), so this
/// reports indices through a callback rather than populating a shared type.
pub(crate) fn for_each_referenced_index(
    expr: &Expr,
    schema: &DFSchema,
    mut f: impl FnMut(usize),
) -> Result<()> {
    visit_referenced_indices(expr, schema, &mut f)
}

fn visit_referenced_indices(
    expr: &Expr,
    schema: &DFSchema,
    f: &mut dyn FnMut(usize),
) -> Result<()> {
    expr.apply(|expr| {
        match expr {
            Expr::Column(column) | Expr::OuterReferenceColumn(_, column) => {
                if let Some(idx) = schema.maybe_index_of_column(column) {
                    f(idx);
                }
            }
            Expr::Exists(Exists { subquery, .. })
            | Expr::InSubquery(InSubquery { subquery, .. })
            | Expr::SetComparison(SetComparison { subquery, .. })
            | Expr::ScalarSubquery(subquery) => {
                for outer in &subquery.outer_ref_columns {
                    visit_referenced_indices(outer, schema, f)?;
                }
            }
            _ => {}
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(())
}

pub(crate) fn replace_qualified_name(
    expr: Expr,
    cols: &BTreeSet<Column>,
    subquery_alias: &str,
) -> Result<Expr> {
    let alias_cols: Vec<Column> = cols
        .iter()
        .map(|col| Column::new(Some(subquery_alias), &col.name))
        .collect();
    let replace_map: HashMap<&Column, &Column> =
        cols.iter().zip(alias_cols.iter()).collect();

    replace_col(expr, &replace_map)
}

/// Finds the output column of a node that a reference reads, as a lookup in
/// the node's schema does: by id when the reference has one (and the node's
/// outputs have ids), otherwise by qualifier and name. Unlike
/// [`DFSchema::maybe_index_of_column`], the name must match exactly (as
/// [`Column`] equality): an unqualified reference does not find a qualified
/// output, so that rules that substitute the outputs they find keep the
/// references they did not find by name before ids.
///
/// The first output wins when several have the same id (`SELECT a, a`, they
/// hold the same values) or the same name.
#[derive(Debug)]
pub(crate) struct OutputColumns {
    by_id: HashMap<ColumnId, usize>,
    by_column: HashMap<Column, usize>,
}

impl OutputColumns {
    /// The outputs of a node whose schema is `schema`
    pub(crate) fn new(schema: &DFSchema) -> Self {
        let mut by_id = HashMap::with_capacity(schema.fields().len());
        let mut by_column = HashMap::with_capacity(schema.fields().len());
        for (i, col) in schema.columns().into_iter().enumerate() {
            if let Some(id) = col.id {
                by_id.entry(id).or_insert(i);
            }
            // by qualifier and name only
            by_column.entry(col.with_id(None)).or_insert(i);
        }
        Self { by_id, by_column }
    }

    /// The index of the output that `col` reads, if any
    pub(crate) fn index_of(&self, col: &Column) -> Option<usize> {
        match col.id {
            // by id only (when the outputs have ids)
            Some(id) if !self.by_id.is_empty() => self.by_id.get(&id).copied(),
            Some(_) => self.by_column.get(&col.clone().with_id(None)).copied(),
            None => self.by_column.get(col).copied(),
        }
    }

    /// Returns `expr` with each column that reads an output `i` for which
    /// `replacement(i)` is `Some` replaced by that expression
    pub(crate) fn replace(
        &self,
        expr: Expr,
        mut replacement: impl FnMut(usize) -> Option<Expr>,
    ) -> Result<Expr> {
        expr.transform_up(|expr| {
            if let Expr::Column(col) = &expr
                && let Some(new_expr) = self.index_of(col).and_then(&mut replacement)
            {
                Ok(Transformed::yes(new_expr))
            } else {
                Ok(Transformed::no(expr))
            }
        })
        .data()
    }

    /// Whether `expr` reads an output `i` for which `f(i)` is true (columns
    /// only, not the outer references of its subqueries)
    pub(crate) fn reads_any(
        &self,
        expr: &Expr,
        mut f: impl FnMut(usize) -> bool,
    ) -> bool {
        let mut found = false;
        expr.apply(|expr| {
            if let Expr::Column(col) = expr
                && self.index_of(col).is_some_and(&mut f)
            {
                found = true;
                return Ok(TreeNodeRecursion::Stop);
            }
            Ok(TreeNodeRecursion::Continue)
        })
        .expect("traversal is infallible");
        found
    }
}

/// The columns of the inputs of a node, to find the input whose columns an
/// expression reads: a column with an id that one of the inputs has belongs
/// to that input, the others are found by qualifier and name (also
/// unqualified, see [`schema_columns`]).
pub(crate) struct InputColumns<'a> {
    /// The columns of each input, by name
    columns: Vec<HashSet<ColumnReference<'a>>>,
    /// The input each id of the inputs is a column of
    ids: HashMap<ColumnId, usize>,
}

impl<'a> InputColumns<'a> {
    pub(crate) fn new(schemas: impl IntoIterator<Item = &'a DFSchema>) -> Self {
        let mut columns = vec![];
        let mut ids = HashMap::new();
        for (idx, schema) in schemas.into_iter().enumerate() {
            for id in schema.field_ids().iter().flatten() {
                ids.entry(*id).or_insert(idx);
            }
            columns.push(schema_columns(schema));
        }
        Self { columns, ids }
    }

    /// Whether `col` is a column of the input `idx`
    pub(crate) fn has(&self, idx: usize, col: &Column) -> bool {
        match col.id.and_then(|id| self.ids.get(&id)) {
            Some(owner) => *owner == idx,
            None => self.columns[idx]
                .contains(&ColumnReference::new(col.relation.as_ref(), col.name())),
        }
    }

    /// The index of the input whose columns cover the column references of
    /// `expr`. Returns `None` if the expression references columns from
    /// multiple inputs or if multiple inputs match (ambiguous, e.g.
    /// unqualified columns present in both sides of a join).
    pub(crate) fn owner(&self, expr: &Expr) -> Option<usize> {
        let column_refs = expr.column_refs();
        let mut found = None;
        for idx in 0..self.columns.len() {
            if column_refs.iter().all(|col| self.has(idx, col)) {
                if found.is_some() {
                    // Ambiguous — multiple inputs match
                    return None;
                }
                found = Some(idx);
            }
        }
        found
    }
}

/// Column reference to avoid copying string around
#[derive(PartialEq, Eq, Hash, Debug)]
pub(crate) struct ColumnReference<'a> {
    pub relation: Option<&'a TableReference>,
    pub name: &'a str,
}

impl<'a> ColumnReference<'a> {
    pub fn new(relation: Option<&'a TableReference>, name: &'a str) -> Self {
        Self { relation, name }
    }

    pub fn new_unqualified(name: &'a str) -> Self {
        Self {
            relation: None,
            name,
        }
    }
}

/// Returns references to all columns in the schema
pub(crate) fn schema_columns(schema: &DFSchema) -> HashSet<ColumnReference<'_>> {
    schema
        .iter()
        .flat_map(|(qualifier, field)| {
            [
                ColumnReference::new(qualifier, field.name()),
                // we need to push down filter using unqualified column as well
                ColumnReference::new_unqualified(field.name()),
            ]
        })
        .collect::<HashSet<_>>()
}

/// Log the plan in debug/tracing mode after some part of the optimizer runs
pub fn log_plan(description: &str, plan: &LogicalPlan) {
    debug!("{description}:\n{}\n", plan.display_indent());
    trace!("{description}::\n{}\n", plan.display_indent_schema());
}

/// Determine whether a predicate can restrict NULLs. e.g.
/// `c0 > 8` return true;
/// `c0 IS NULL` return false.
pub fn is_restrict_null_predicate<'a>(
    predicate: Expr,
    join_cols_of_predicate: impl IntoIterator<Item = &'a Column>,
) -> Result<bool> {
    if matches!(predicate, Expr::Column(_)) {
        return Ok(true);
    }

    // If result is single `true`, return false;
    // If result is single `NULL` or `false`, return true;
    Ok(
        match evaluate_expr_with_null_column(predicate, join_cols_of_predicate)? {
            ColumnarValue::Array(array) => {
                if array.len() == 1 {
                    let boolean_array = as_boolean_array(&array)?;
                    boolean_array.is_null(0) || !boolean_array.value(0)
                } else {
                    false
                }
            }
            ColumnarValue::Scalar(scalar) => matches!(
                scalar,
                ScalarValue::Boolean(None) | ScalarValue::Boolean(Some(false))
            ),
        },
    )
}

/// Determines if an expression will always evaluate to null.
/// `c0 + 8` return true
/// `c0 IS NULL` return false
/// `CASE WHEN c0 > 1 then 0 else 1` return false
pub fn evaluates_to_null<'a>(
    predicate: Expr,
    null_columns: impl IntoIterator<Item = &'a Column>,
) -> Result<bool> {
    if matches!(predicate, Expr::Column(_)) {
        return Ok(true);
    }

    Ok(
        match evaluate_expr_with_null_column(predicate, null_columns)? {
            ColumnarValue::Array(_) => false,
            ColumnarValue::Scalar(scalar) => scalar.is_null(),
        },
    )
}

fn evaluate_expr_with_null_column<'a>(
    predicate: Expr,
    null_columns: impl IntoIterator<Item = &'a Column>,
) -> Result<ColumnarValue> {
    static DUMMY_COL_NAME: &str = "?";
    let schema = Arc::new(Schema::new(vec![Field::new(
        DUMMY_COL_NAME,
        DataType::Null,
        true,
    )]));
    let input_schema = DFSchema::try_from(Arc::clone(&schema))?;
    let column = new_null_array(&DataType::Null, 1);
    let input_batch = RecordBatch::try_new(schema, vec![column])?;
    let execution_props = ExecutionProps::default();
    let null_column = Column::from_name(DUMMY_COL_NAME);

    let join_cols_to_replace = null_columns
        .into_iter()
        .map(|column| (column, &null_column))
        .collect::<HashMap<_, _>>();

    let replaced_predicate = replace_col(predicate, &join_cols_to_replace)?;
    let coerced_predicate = coerce(replaced_predicate, &input_schema)?;
    create_physical_expr(
        &coerced_predicate,
        &input_schema,
        &execution_props,
        &PhysicalPlanningContext::default(),
    )?
    .evaluate(&input_batch)
}

/// No session timezone is threaded in here on purpose. This runs over a plan the
/// `TypeCoercion` analyzer has already coerced, so every binary operand pair
/// already shares a type; the only type this helper introduces is the `Null` of
/// the dummy column in `evaluate_expr_with_null_column`, and `Null` against a
/// timestamp is short-circuited by `null_coercion` before the aware/naive rule.
fn coerce(expr: Expr, schema: &DFSchema) -> Result<Expr> {
    let mut expr_rewrite = TypeCoercionRewriter::new(schema);
    expr.rewrite(&mut expr_rewrite).data()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::test_table_scan_with_name;
    use datafusion_common::Spans;
    use datafusion_expr::expr::SetQuantifier;
    use datafusion_expr::logical_plan::builder::LogicalPlanBuilder;
    use datafusion_expr::{
        Operator, Subquery, binary_expr, case, col, in_list, is_null, lit,
    };

    #[test]
    fn set_comparison_is_not_repeatable() {
        let subquery = LogicalPlanBuilder::from(test_table_scan_with_name("t2").unwrap())
            .project(vec![col("b")])
            .unwrap()
            .build()
            .unwrap();
        let expr = Expr::SetComparison(SetComparison::new(
            Box::new(col("a")),
            Subquery {
                subquery: Arc::new(subquery),
                outer_ref_columns: vec![],
                spans: Spans::new(),
            },
            Operator::Gt,
            SetQuantifier::Any,
        ));

        assert!(!is_repeatable(&expr));
    }

    #[test]
    fn expr_is_restrict_null_predicate() -> Result<()> {
        let test_cases = vec![
            // a
            (col("a"), true),
            // a IS NULL
            (is_null(col("a")), false),
            // a IS NOT NULL
            (Expr::IsNotNull(Box::new(col("a"))), true),
            // a = NULL
            (
                binary_expr(
                    col("a"),
                    Operator::Eq,
                    Expr::Literal(ScalarValue::Null, None),
                ),
                true,
            ),
            // a > 8
            (binary_expr(col("a"), Operator::Gt, lit(8i64)), true),
            // a <= 8
            (binary_expr(col("a"), Operator::LtEq, lit(8i32)), true),
            // CASE a WHEN 1 THEN true WHEN 0 THEN false ELSE NULL END
            (
                case(col("a"))
                    .when(lit(1i64), lit(true))
                    .when(lit(0i64), lit(false))
                    .otherwise(lit(ScalarValue::Null))?,
                true,
            ),
            // CASE a WHEN 1 THEN true ELSE false END
            (
                case(col("a"))
                    .when(lit(1i64), lit(true))
                    .otherwise(lit(false))?,
                true,
            ),
            // CASE a WHEN 0 THEN false ELSE true END
            (
                case(col("a"))
                    .when(lit(0i64), lit(false))
                    .otherwise(lit(true))?,
                false,
            ),
            // (CASE a WHEN 0 THEN false ELSE true END) OR false
            (
                binary_expr(
                    case(col("a"))
                        .when(lit(0i64), lit(false))
                        .otherwise(lit(true))?,
                    Operator::Or,
                    lit(false),
                ),
                false,
            ),
            // (CASE a WHEN 0 THEN true ELSE false END) OR false
            (
                binary_expr(
                    case(col("a"))
                        .when(lit(0i64), lit(true))
                        .otherwise(lit(false))?,
                    Operator::Or,
                    lit(false),
                ),
                true,
            ),
            // a IN (1, 2, 3)
            (
                in_list(col("a"), vec![lit(1i64), lit(2i64), lit(3i64)], false),
                true,
            ),
            // a NOT IN (1, 2, 3)
            (
                in_list(col("a"), vec![lit(1i64), lit(2i64), lit(3i64)], true),
                true,
            ),
            // a IN (NULL)
            (
                in_list(
                    col("a"),
                    vec![Expr::Literal(ScalarValue::Null, None)],
                    false,
                ),
                true,
            ),
            // a NOT IN (NULL)
            (
                in_list(col("a"), vec![Expr::Literal(ScalarValue::Null, None)], true),
                true,
            ),
        ];

        let column_a = Column::from_name("a");
        for (predicate, expected) in test_cases {
            let join_cols_of_predicate = std::iter::once(&column_a);
            let actual =
                is_restrict_null_predicate(predicate.clone(), join_cols_of_predicate)?;
            assert_eq!(actual, expected, "{predicate}");
        }

        Ok(())
    }
}
