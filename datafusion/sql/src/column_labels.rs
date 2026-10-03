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

//! Readable labels for the output columns of a SQL query, enabled by
//! `datafusion.sql_parser.pretty_column_names`.
//!
//! The planner names an unaliased column with [`Expr::schema_name`], which is
//! also the key it uses to look up columns, so the name includes type
//! wrappers and table qualifiers (`t.a + Int64(1)`). This module computes a
//! readable label for each output column of a fully planned query (`a + 1`)
//! and renames the columns in a projection on top of the plan. Names inside
//! the plan don't change.

use std::collections::{HashMap, HashSet};
use std::fmt::Write;
use std::sync::Arc;

use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{Column, DFSchema, Dependency, Result};
use datafusion_expr::expr::{
    ExprListDisplay, SortListDisplay, WindowFunction, WindowFunctionParams,
};
use datafusion_expr::utils::grouping_set_to_exprlist;
use datafusion_expr::{
    Distinct, Expr, JoinConstraint, JoinType, LogicalPlan, Projection, SortExpr,
    WindowFrame, WindowFrameUnits,
};
use sqlparser::ast::{Expr as SQLExpr, SelectItem, SetExpr};

use crate::planner::{ContextProvider, SqlToRel};

impl<S: ContextProvider> SqlToRel<'_, S> {
    /// Returns the normalized names of the plain column references, such as
    /// `a` or `t.a`, in the outermost `SELECT` list of `body`. For a set
    /// operation, this is the `SELECT` list of its first input.
    pub(crate) fn typed_column_names(&self, body: &SetExpr) -> HashSet<String> {
        match body {
            SetExpr::Select(select) => select
                .projection
                .iter()
                .filter_map(|item| match item {
                    SelectItem::UnnamedExpr(SQLExpr::Identifier(ident)) => Some(ident),
                    SelectItem::UnnamedExpr(SQLExpr::CompoundIdentifier(idents)) => {
                        idents.last()
                    }
                    _ => None,
                })
                .map(|ident| self.ident_normalizer.normalize(ident.clone()))
                .collect(),
            SetExpr::Query(query) => self.typed_column_names(&query.body),
            SetExpr::SetOperation { left, .. } => self.typed_column_names(left),
            _ => HashSet::new(),
        }
    }
}

/// Renames the output columns of `plan` to their readable labels.
///
/// A column keeps its current name when:
/// - its name is in `typed_names`, the column references typed in the
///   outermost `SELECT` list;
/// - its label can't be computed (see [`trace_column`]);
/// - its label collides with the name of another output column.
///
/// Returns `plan` unchanged when no column gets a new name.
pub(crate) fn label_output_columns(
    plan: LogicalPlan,
    typed_names: &HashSet<String>,
) -> Result<LogicalPlan> {
    let schema = Arc::clone(plan.schema());
    let mut labels = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(idx, field)| {
            let name = field.name();
            if typed_names.contains(name) {
                return Ok(None);
            }
            let label = trace_column(&plan, idx)?
                .map(|expr| expr.human_display().to_string())
                .filter(|label| label != name);
            Ok(label)
        })
        .collect::<Result<Vec<_>>>()?;

    revert_colliding_labels(&schema, &mut labels);

    if labels.iter().all(Option::is_none) {
        return Ok(plan);
    }
    let exprs = schema
        .iter()
        .zip(labels)
        .map(|((qualifier, field), label)| {
            let column = Expr::Column(Column::from((qualifier, field)));
            match label {
                Some(label) => column.alias(label),
                None => column,
            }
        })
        .collect();
    Ok(LogicalPlan::Projection(Projection::try_new(
        exprs,
        Arc::new(plan),
    )?))
}

/// Drops each label that has the same name as another output column, so the
/// column keeps its current name. Dropping a label can create a new
/// collision with the current name, so this repeats until no labeled column
/// collides.
fn revert_colliding_labels(schema: &DFSchema, labels: &mut [Option<String>]) {
    loop {
        let colliding: Vec<usize> = {
            let names: Vec<&str> = schema
                .fields()
                .iter()
                .zip(labels.iter())
                .map(|(field, label)| label.as_deref().unwrap_or(field.name()))
                .collect();
            let mut counts: HashMap<&str, usize> = HashMap::new();
            for name in &names {
                *counts.entry(name).or_default() += 1;
            }
            (0..names.len())
                .filter(|&idx| labels[idx].is_some() && counts[names[idx]] > 1)
                .collect()
        };
        if colliding.is_empty() {
            return;
        }
        for idx in colliding {
            labels[idx] = None;
        }
    }
}

/// Follows output column `idx` of `plan` down to the expression that
/// produced it, and returns that expression with every column reference
/// replaced by its own origin. The result has no table qualifiers, and
/// [`Expr::human_display`] renders it as the label.
///
/// Returns `None` when the origin can't be traced, for example through
/// `UNNEST`, `VALUES`, a `USING` join, or a subquery expression.
fn trace_column(plan: &LogicalPlan, idx: usize) -> Result<Option<Expr>> {
    match plan {
        LogicalPlan::Projection(projection) => match &projection.expr[idx] {
            // Fast path for `SELECT *`: the column is usually at the same index
            // in the input
            Expr::Column(column) => {
                match column_index(projection.input.schema(), column, idx) {
                    Some(input_idx) => trace_column(&projection.input, input_idx),
                    None => Ok(None),
                }
            }
            expr => resolve_expr(expr, &projection.input),
        },
        LogicalPlan::Aggregate(aggregate) => {
            let group_exprs = grouping_set_to_exprlist(&aggregate.group_expr)?;
            if let Some(expr) = group_exprs.get(idx) {
                return resolve_expr(expr, &aggregate.input);
            }
            let mut idx = idx - group_exprs.len();
            // Grouping sets add an internal grouping id column
            if matches!(aggregate.group_expr.as_slice(), [Expr::GroupingSet(_)]) {
                if idx == 0 {
                    return Ok(None);
                }
                idx -= 1;
            }
            resolve_expr(&aggregate.aggr_expr[idx], &aggregate.input)
        }
        LogicalPlan::Window(window) => {
            let input_len = window.input.schema().fields().len();
            if idx < input_len {
                trace_column(&window.input, idx)
            } else {
                resolve_window_expr(&window.window_expr[idx - input_len], &window.input)
            }
        }
        LogicalPlan::SubqueryAlias(alias) => trace_column(&alias.input, idx),
        LogicalPlan::Filter(filter) => trace_column(&filter.input, idx),
        LogicalPlan::Sort(sort) => trace_column(&sort.input, idx),
        LogicalPlan::Limit(limit) => trace_column(&limit.input, idx),
        LogicalPlan::Distinct(Distinct::All(input)) => trace_column(input, idx),
        // SQL takes the output names of a set operation from its first input
        LogicalPlan::Union(union) => trace_column(&union.inputs[0], idx),
        LogicalPlan::Join(join) if join.join_constraint == JoinConstraint::On => {
            let left_len = join.left.schema().fields().len();
            match join.join_type {
                JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full => {
                    if idx < left_len {
                        trace_column(&join.left, idx)
                    } else {
                        trace_column(&join.right, idx - left_len)
                    }
                }
                JoinType::LeftSemi | JoinType::LeftAnti => trace_column(&join.left, idx),
                JoinType::RightSemi | JoinType::RightAnti => {
                    trace_column(&join.right, idx)
                }
                JoinType::LeftMark | JoinType::RightMark => Ok(None),
            }
        }
        LogicalPlan::TableScan(scan) => Ok(Some(Expr::Column(Column::new_unqualified(
            scan.projected_schema.field(idx).name(),
        )))),
        _ => Ok(None),
    }
}

/// Returns the index of `column` in `schema`, checking `hint` first.
fn column_index(schema: &DFSchema, column: &Column, hint: usize) -> Option<usize> {
    if hint < schema.fields().len() {
        let (qualifier, field) = schema.qualified_field(hint);
        if field.name() == &column.name && qualifier == column.relation.as_ref() {
            return Some(hint);
        }
    }
    schema.maybe_index_of_column(column)
}

/// Replaces each column reference in `expr`, an expression evaluated on
/// `input`, with its origin.
fn resolve_expr(expr: &Expr, input: &LogicalPlan) -> Result<Option<Expr>> {
    match expr {
        Expr::Alias(alias) => {
            if let Expr::Column(column) = alias.expr.as_ref()
                && is_count_star_window_alias(column, &alias.name)
                && let Some(Expr::Column(label)) = resolve_expr(&alias.expr, input)?
                && let Some(args) = label.name.strip_prefix("count(1)")
            {
                return Ok(Some(Expr::Column(Column::new_unqualified(format!(
                    "count(*){args}"
                )))));
            }
            // An explicit alias keeps its name
            Ok(Some(Expr::Column(Column::new_unqualified(
                alias.name.as_str(),
            ))))
        }
        Expr::Column(column) => match input.schema().maybe_index_of_column(column) {
            Some(idx) => trace_column(input, idx),
            None => Ok(None),
        },
        _ => {
            let has_subquery = expr.exists(|e| {
                Ok(matches!(
                    e,
                    Expr::Exists(_)
                        | Expr::InSubquery(_)
                        | Expr::SetComparison(_)
                        | Expr::ScalarSubquery(_)
                        | Expr::OuterReferenceColumn(..)
                ))
            })?;
            if has_subquery {
                return Ok(None);
            }
            let mut origins = HashMap::new();
            for column in expr.column_refs() {
                let Some(origin) = resolve_expr(&Expr::Column(column.clone()), input)?
                else {
                    return Ok(None);
                };
                origins.insert(column, origin);
            }
            let resolved = expr
                .clone()
                .transform_up(|e| match &e {
                    Expr::Column(column) => match origins.get(column) {
                        Some(origin) => Ok(Transformed::yes(origin.clone())),
                        None => Ok(Transformed::no(e)),
                    },
                    _ => Ok(Transformed::no(e)),
                })?
                .data;
            Ok(Some(resolved))
        }
    }
}

/// Returns true if `alias` is the name the planner gives
/// `count(*) OVER (...)`. The planner plans it as `count(1) OVER (...)` and
/// aliases the result column to keep the `count(*)` name.
fn is_count_star_window_alias(column: &Column, alias: &str) -> bool {
    const PLANNED: &str = "count(Int64(1))";
    column.name.starts_with(PLANNED)
        && column.name.len() > PLANNED.len()
        && alias == column.name.replacen(PLANNED, "count(*)", 1)
}

/// Like [`resolve_expr`] for an expression of a `Window` node. A window
/// function is rendered as `name(args) OVER (...)` without the default parts
/// of its window definition.
fn resolve_window_expr(expr: &Expr, input: &LogicalPlan) -> Result<Option<Expr>> {
    let Expr::WindowFunction(window_function) = expr else {
        return resolve_expr(expr, input);
    };
    let WindowFunction { fun, params } = window_function.as_ref();
    let WindowFunctionParams {
        args,
        partition_by,
        order_by,
        window_frame,
        filter,
        null_treatment,
        distinct,
    } = params;

    let frame_is_default =
        is_default_frame(window_frame, order_by, window_planning_schema(input));
    let resolve_all = |exprs: &[Expr]| -> Result<Option<Vec<Expr>>> {
        exprs
            .iter()
            .map(|e| resolve_expr(e, input))
            .collect::<Result<Option<Vec<_>>>>()
    };
    let Some(args) = resolve_all(args)? else {
        return Ok(None);
    };
    let Some(partition_by) = resolve_all(partition_by)? else {
        return Ok(None);
    };
    let mut resolved_order_by = Vec::with_capacity(order_by.len());
    for sort in order_by {
        // The planner adds a constant sort key to a RANGE frame without
        // ORDER BY, and a constant key doesn't change the result
        if matches!(sort.expr, Expr::Literal(..)) {
            continue;
        }
        let Some(expr) = resolve_expr(&sort.expr, input)? else {
            return Ok(None);
        };
        resolved_order_by.push(sort.with_expr(expr));
    }
    let order_by = resolved_order_by;
    let filter = match filter {
        Some(filter) => match resolve_expr(filter, input)? {
            Some(filter) => Some(filter),
            None => return Ok(None),
        },
        None => None,
    };

    let mut label = format!(
        "{}({}{})",
        fun.name(),
        if *distinct { "DISTINCT " } else { "" },
        ExprListDisplay::comma_separated(&args)
    );
    if let Some(null_treatment) = null_treatment {
        write!(label, " {null_treatment}")?;
    }
    if let Some(filter) = filter {
        write!(label, " FILTER (WHERE {})", filter.human_display())?;
    }
    let mut over = vec![];
    if !partition_by.is_empty() {
        over.push(format!(
            "PARTITION BY {}",
            ExprListDisplay::comma_separated(&partition_by)
        ));
    }
    if !order_by.is_empty() {
        over.push(format!("ORDER BY {}", SortListDisplay(&order_by)));
    }
    if !frame_is_default {
        over.push(window_frame.to_string());
    }
    write!(label, " OVER ({})", over.join(" "))?;

    Ok(Some(Expr::Column(Column::new_unqualified(label))))
}

/// Returns the schema that the SQL planner planned a window function against,
/// given the input of its `Window` node. The planner plans window functions
/// on the `FROM` and `WHERE` plan, before aggregation, so for a window on an
/// aggregate this is the schema of the aggregate's input.
fn window_planning_schema(input: &LogicalPlan) -> &DFSchema {
    match input {
        LogicalPlan::Window(window) => window_planning_schema(&window.input),
        // HAVING
        LogicalPlan::Filter(filter) => window_planning_schema(&filter.input),
        LogicalPlan::Aggregate(aggregate) => aggregate.input.schema(),
        _ => input.schema(),
    }
}

/// Returns true if `frame` is the frame the SQL planner uses for a window
/// without a frame clause, or is equivalent to it.
fn is_default_frame(
    frame: &WindowFrame,
    order_by: &[SortExpr],
    input_schema: &DFSchema,
) -> bool {
    let has_order_by = order_by
        .iter()
        .any(|sort| !matches!(sort.expr, Expr::Literal(..)));
    if !has_order_by {
        // Without ORDER BY (or with only constant keys) all rows are peers, so
        // a RANGE frame bounded by UNBOUNDED or CURRENT ROW covers the whole
        // partition
        return (frame.units == WindowFrameUnits::Range && frame.free_range())
            || frame.to_string() == WindowFrame::new(None).to_string();
    }
    // Same check as the SQL planner: the default frame is `ROWS` when the
    // first sort key is unique, and `RANGE` when it can have ties
    let is_ordering_strict =
        match &order_by[0].expr {
            Expr::Column(column) => input_schema
                .maybe_index_of_column(column)
                .is_some_and(|idx| {
                    input_schema.functional_dependencies().iter().any(|dep| {
                        dep.source_indices == vec![idx] && dep.mode == Dependency::Single
                    })
                }),
            _ => false,
        };
    frame.to_string() == WindowFrame::new(Some(is_ordering_strict)).to_string()
}
