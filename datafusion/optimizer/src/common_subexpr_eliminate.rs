// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! [`CommonSubexprEliminate`] to avoid redundant computation of common sub-expressions

use std::cell::Cell;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use crate::{OptimizerConfig, OptimizerRule};

use crate::optimizer::ApplyOrder;

use datafusion_common::cse::{CSE, CSEController, FoundCommonNodes};
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{
    Column, ColumnId, ColumnIdGenerator, DFSchema, DFSchemaRef, Result,
    assert_eq_or_internal_err,
};
use datafusion_expr::expr::{
    AggregateFunction, AggregateFunctionParams, HigherOrderFunction, ScalarFunction,
    WindowFunction, WindowFunctionParams,
};
use datafusion_expr::logical_plan::{
    Aggregate, Filter, LogicalPlan, Projection, Sort, Window,
};
use datafusion_expr::{
    BinaryExpr, Case, Expr, ExpressionPlacement, NamedExpr, Operator, SortExpr,
};

/// Performs Common Sub-expression Elimination optimization.
///
/// This optimization improves query performance by computing expressions that
/// appear more than once and reusing those results rather than re-computing the
/// same value
///
/// Currently only common sub-expressions within a single `LogicalPlan` are
/// eliminated.
///
/// # Example
///
/// Given a projection that computes the same expensive expression
/// multiple times such as parsing as string as a date with `to_date` twice:
///
/// ```text
/// ProjectionExec(expr=[extract (day from to_date(c1)), extract (year from to_date(c1))])
/// ```
///
/// This optimization will rewrite the plan to compute the common expression once
/// using a new `ProjectionExec` and then rewrite the original expressions to
/// refer to that new column.
///
/// ```text
/// ProjectionExec(exprs=[extract (day from new_col), extract (year from new_col)]) <-- reuse here
///   ProjectionExec(exprs=[to_date(c1) as new_col]) <-- compute to_date once
/// ```
#[derive(Debug)]
pub struct CommonSubexprEliminate {}

impl CommonSubexprEliminate {
    pub fn new() -> Self {
        Self {}
    }

    fn try_optimize_proj(
        &self,
        projection: Projection,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let Projection {
            expr: entries,
            input,
            schema,
            ..
        } = projection;
        let input = Arc::unwrap_or_clone(input);
        let expr = entries.iter().map(|e| e.expr.clone()).collect();
        let rewritten = self.try_unary_plan(expr, input, config)?;
        let transformed = rewritten.transformed;
        rewritten.map_data(|(new_expr, new_input)| {
            let new_input = Arc::new(new_input);
            // the outputs keep their ids (the columns they pass through
            // are passed through by the projection added below)
            let new_expr = entries
                .into_iter()
                .zip(new_expr)
                .map(|(entry, expr)| {
                    entry.with_rewritten_expr(expr, config.column_id_generator())
                })
                .collect();
            // The rewritten expressions keep their names, but their fields can
            // differ: a CASE whose `WHEN x IS NOT NULL THEN x` guard is split
            // into two common subexpressions can no longer be proven non-null.
            // Derive the schema from the new expressions so it matches what
            // the physical planner will compute from them.
            if transformed {
                Projection::try_new(new_expr, new_input)
            } else {
                Projection::try_new_with_schema(new_expr, new_input, schema)
            }
            .map(LogicalPlan::Projection)
        })
    }

    fn try_optimize_sort(
        &self,
        sort: Sort,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let Sort { expr, input, fetch } = sort;
        let input = Arc::unwrap_or_clone(input);
        let (sort_expressions, sort_params): (Vec<_>, Vec<(_, _)>) = expr
            .into_iter()
            .map(|sort| (sort.expr, (sort.asc, sort.nulls_first)))
            .unzip();
        let new_sort = self
            .try_unary_plan(sort_expressions, input, config)?
            .update_data(|(new_expr, new_input)| {
                LogicalPlan::Sort(Sort {
                    expr: new_expr
                        .into_iter()
                        .zip(sort_params)
                        .map(|(expr, (asc, nulls_first))| SortExpr {
                            expr,
                            asc,
                            nulls_first,
                        })
                        .collect(),
                    input: Arc::new(new_input),
                    fetch,
                })
            });
        Ok(new_sort)
    }

    fn try_optimize_filter(
        &self,
        filter: Filter,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let Filter {
            predicate, input, ..
        } = filter;
        let input = Arc::unwrap_or_clone(input);
        let expr = vec![predicate];
        self.try_unary_plan(expr, input, config)?
            .map_data(|(mut new_expr, new_input)| {
                assert_eq!(new_expr.len(), 1); // passed in vec![predicate]
                let new_predicate = new_expr.pop().unwrap();
                Filter::try_new(new_predicate, Arc::new(new_input))
                    .map(LogicalPlan::Filter)
            })
    }

    fn try_optimize_window(
        &self,
        window: Window,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        // Collects window expressions from consecutive `LogicalPlan::Window` nodes into
        // a list.
        let (window_expr_list, window_ids, window_schemas, input) =
            get_consecutive_window_exprs(window);

        // Extract common sub-expressions from the list.
        let mut common_ids = CommonExprIds::default();
        match extract_common_exprs(
            config,
            ExprMask::Normal,
            window_expr_list,
            &mut common_ids,
        )? {
            // If there are common sub-expressions, then the insert a projection node
            // with the common expressions between the new window nodes and the
            // original input.
            FoundCommonNodes::Yes {
                common_nodes: common_exprs,
                new_nodes_list: new_exprs_list,
                original_nodes_list: original_exprs_list,
            } => build_common_expr_project_plan(
                input,
                common_exprs,
                &mut common_ids,
                config,
            )
            .map(|new_input| {
                Transformed::yes((new_exprs_list, new_input, Some(original_exprs_list)))
            }),
            FoundCommonNodes::No {
                original_nodes_list: original_exprs_list,
            } => Ok(Transformed::no((original_exprs_list, input, None))),
        }?
        // Recurse into the new input.
        // (This is similar to what a `ApplyOrder::TopDown` optimizer rule would do.)
        .transform_data(|(new_window_expr_list, new_input, window_expr_list)| {
            self.rewrite(new_input, config)?.map_data(|new_input| {
                Ok((new_window_expr_list, new_input, window_expr_list))
            })
        })?
        // Rebuild the consecutive window nodes.
        .map_data(|(new_window_expr_list, new_input, window_expr_list)| {
            // If there were common expressions extracted, the window
            // expressions are rewritten: each keeps the id of its entry, and
            // may be renamed (`Window::try_new` names the outputs again)
            if window_expr_list.is_some() {
                new_window_expr_list.into_iter().zip(window_ids).try_rfold(
                    new_input,
                    |plan, (new_window_expr, ids)| {
                        let new_window_expr = new_window_expr
                            .into_iter()
                            .zip(ids)
                            .map(|(new_window_expr, id)| {
                                NamedExpr::new(new_window_expr, id)
                            })
                            .collect::<Vec<_>>();
                        Window::try_new(new_window_expr, Arc::new(plan))
                            .map(LogicalPlan::Window)
                    },
                )
            } else {
                new_window_expr_list
                    .into_iter()
                    .zip(window_ids)
                    .zip(window_schemas)
                    .try_rfold(new_input, |plan, ((new_window_expr, ids), schema)| {
                        let new_window_expr = new_window_expr
                            .into_iter()
                            .zip(ids)
                            .map(|(expr, id)| NamedExpr::new(expr, id))
                            .collect();
                        Window::try_new_with_schema(
                            new_window_expr,
                            Arc::new(plan),
                            schema,
                        )
                        .map(LogicalPlan::Window)
                    })
            }
        })
    }

    fn try_optimize_aggregate(
        &self,
        aggregate: Aggregate,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let group_by_exprs = aggregate.group_by_exprs();
        let Aggregate {
            group_expr,
            grouping_sets,
            aggr_expr,
            input,
            schema,
            ..
        } = aggregate;
        let input = Arc::unwrap_or_clone(input);
        // the entries, whose ids the rewritten outputs keep
        let group_entries = group_expr;
        let aggr_entries = aggr_expr;
        // grouping sets are rewritten as they are written (one
        // `Expr::GroupingSet`), and flattened again when rebuilt
        let group_expr = match &grouping_sets {
            Some(_) => group_by_exprs,
            None => group_entries.iter().map(|e| e.expr.clone()).collect(),
        };
        let aggr_expr = aggr_entries.iter().map(|e| e.expr.clone()).collect();
        let ids = config.column_id_generator();
        let rebuild = |entries: &[NamedExpr], exprs: Vec<Expr>| -> Vec<NamedExpr> {
            entries
                .iter()
                .zip(exprs)
                .map(|(entry, expr)| entry.clone().with_rewritten_expr(expr, ids))
                .collect()
        };
        let rebuild_group = |exprs: Vec<Expr>| -> Result<Vec<NamedExpr>> {
            let exprs = match exprs.as_slice() {
                [Expr::GroupingSet(grouping_set)] if grouping_sets.is_some() => {
                    grouping_set.distinct_expr().into_iter().cloned().collect()
                }
                _ => exprs,
            };
            assert_eq_or_internal_err!(
                exprs.len(),
                group_entries.len(),
                "CSE changed the number of grouping expressions"
            );
            Ok(rebuild(&group_entries, exprs))
        };
        // Extract common sub-expressions from the aggregate and grouping expressions.
        let mut common_ids = CommonExprIds::default();
        match extract_common_exprs(
            config,
            ExprMask::Normal,
            vec![group_expr, aggr_expr],
            &mut common_ids,
        )? {
            // If there are common sub-expressions, then insert a projection node
            // with the common expressions between the new aggregate node and the
            // original input.
            FoundCommonNodes::Yes {
                common_nodes: common_exprs,
                new_nodes_list: mut new_exprs_list,
                original_nodes_list: mut original_exprs_list,
            } => {
                let new_aggr_expr = new_exprs_list.pop().unwrap();
                let new_group_expr = new_exprs_list.pop().unwrap();

                build_common_expr_project_plan(
                    input,
                    common_exprs,
                    &mut common_ids,
                    config,
                )
                .map(|new_input| {
                    let aggr_expr = original_exprs_list.pop().unwrap();
                    Transformed::yes((
                        new_aggr_expr,
                        new_group_expr,
                        new_input,
                        Some(aggr_expr),
                    ))
                })
            }

            FoundCommonNodes::No {
                original_nodes_list: mut original_exprs_list,
            } => {
                let new_aggr_expr = original_exprs_list.pop().unwrap();
                let new_group_expr = original_exprs_list.pop().unwrap();

                Ok(Transformed::no((
                    new_aggr_expr,
                    new_group_expr,
                    input,
                    None,
                )))
            }
        }?
        // Recurse into the new input.
        // (This is similar to what a `ApplyOrder::TopDown` optimizer rule would do.)
        .transform_data(|(new_aggr_expr, new_group_expr, new_input, aggr_expr)| {
            self.rewrite(new_input, config)?.map_data(|new_input| {
                Ok((
                    new_aggr_expr,
                    new_group_expr,
                    aggr_expr,
                    Arc::new(new_input),
                ))
            })
        })?
        // Try extracting common aggregate expressions and rebuild the aggregate node.
        .transform_data(
            |(new_aggr_expr, new_group_expr, aggr_expr, new_input)| {
                // Extract common aggregate sub-expressions from the aggregate expressions.
                let mut common_ids = CommonExprIds::default();
                match extract_common_exprs(
                    config,
                    ExprMask::NormalAndAggregates,
                    vec![new_aggr_expr],
                    &mut common_ids,
                )? {
                    FoundCommonNodes::Yes {
                        common_nodes: common_exprs,
                        new_nodes_list: mut new_exprs_list,
                        original_nodes_list: mut original_exprs_list,
                    } => {
                        let rewritten_aggr_expr = new_exprs_list.pop().unwrap();
                        let new_aggr_expr = original_exprs_list.pop().unwrap();
                        // the common aggregates define the columns that the
                        // rewritten aggregates reference
                        let mut agg_exprs = common_exprs
                            .into_iter()
                            .map(|(expr, key)| {
                                let id = common_ids.id(&key, ids);
                                NamedExpr::new(expr, id)
                            })
                            .collect::<Vec<_>>();

                        // the outputs of the projection that replace the
                        // aggregate expressions keep the ids of the aggregate
                        // outputs they replace (the grouping outputs are
                        // passed through, below). An aggregate expression
                        // that was not rewritten stays in the aggregate, with
                        // its id, and the projection passes it through.
                        let mut proj_exprs: Vec<NamedExpr> =
                            Vec::with_capacity(rewritten_aggr_expr.len());
                        for ((expr_rewritten, expr_orig), entry) in rewritten_aggr_expr
                            .into_iter()
                            .zip(new_aggr_expr)
                            .zip(&aggr_entries)
                        {
                            if expr_rewritten == expr_orig {
                                let (qualifier, name) = expr_orig.qualified_name();
                                agg_exprs.push(NamedExpr::new(expr_orig, entry.id));
                                proj_exprs.push(NamedExpr::new(
                                    Expr::Column(
                                        Column::new(qualifier, name)
                                            .with_id(Some(entry.id)),
                                    ),
                                    entry.id,
                                ));
                            } else {
                                proj_exprs.push(NamedExpr::new(expr_rewritten, entry.id));
                            }
                        }

                        let agg = LogicalPlan::Aggregate(Aggregate::try_new(
                            new_input,
                            rebuild_group(new_group_expr)?,
                            grouping_sets.clone(),
                            agg_exprs,
                        )?);
                        // the grouping outputs (and the grouping id) pass
                        // through
                        let group_len = match &agg {
                            LogicalPlan::Aggregate(agg) => agg.group_expr_len()?,
                            _ => unreachable!(),
                        };
                        let proj_exprs = agg
                            .schema()
                            .columns()
                            .into_iter()
                            .take(group_len)
                            .map(|col| {
                                NamedExpr::allocate(Expr::Column(col), agg.schema(), ids)
                            })
                            .chain(proj_exprs)
                            .collect();
                        Projection::try_new(proj_exprs, Arc::new(agg))
                            .map(|p| Transformed::yes(LogicalPlan::Projection(p)))
                    }

                    // If there aren't any common aggregate sub-expressions, then just
                    // rebuild the aggregate node.
                    FoundCommonNodes::No {
                        original_nodes_list: mut original_exprs_list,
                    } => {
                        let rewritten_aggr_expr = original_exprs_list.pop().unwrap();

                        // If there were common expressions extracted, the
                        // aggregate expressions are rewritten: each keeps the
                        // id of its entry, and may be renamed
                        if aggr_expr.is_some() {
                            // Since `group_expr` may have changed, schema may also.
                            // Use `try_new()` method.
                            Aggregate::try_new(
                                new_input,
                                rebuild_group(new_group_expr)?,
                                grouping_sets.clone(),
                                rebuild(&aggr_entries, rewritten_aggr_expr),
                            )
                            .map(LogicalPlan::Aggregate)
                            .map(Transformed::no)
                        } else {
                            Aggregate::try_new_with_schema(
                                new_input,
                                rebuild_group(new_group_expr)?,
                                grouping_sets.clone(),
                                rebuild(&aggr_entries, rewritten_aggr_expr),
                                schema,
                            )
                            .map(LogicalPlan::Aggregate)
                            .map(Transformed::no)
                        }
                    }
                }
            },
        )
    }

    /// Rewrites the expr list and input to remove common subexpressions
    ///
    /// # Parameters
    ///
    /// * `exprs`: List of expressions in the node
    /// * `input`: input plan (that produces the columns referred to in `exprs`)
    ///
    /// # Return value
    ///
    ///  Returns `(rewritten_exprs, new_input)`. `new_input` is either:
    ///
    /// 1. The original `input` of no common subexpressions were extracted
    /// 2. A newly added projection on top of the original input
    ///    that computes the common subexpressions
    fn try_unary_plan(
        &self,
        exprs: Vec<Expr>,
        input: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<(Vec<Expr>, LogicalPlan)>> {
        // Extract common sub-expressions from the expressions.
        let mut common_ids = CommonExprIds::default();
        match extract_common_exprs(
            config,
            ExprMask::Normal,
            vec![exprs],
            &mut common_ids,
        )? {
            FoundCommonNodes::Yes {
                common_nodes: common_exprs,
                new_nodes_list: mut new_exprs_list,
                original_nodes_list: _,
            } => {
                let new_exprs = new_exprs_list.pop().unwrap();
                build_common_expr_project_plan(
                    input,
                    common_exprs,
                    &mut common_ids,
                    config,
                )
                .map(|new_input| Transformed::yes((new_exprs, new_input)))
            }
            FoundCommonNodes::No {
                original_nodes_list: mut original_exprs_list,
            } => {
                let new_exprs = original_exprs_list.pop().unwrap();
                Ok(Transformed::no((new_exprs, input)))
            }
        }?
        // Recurse into the new input.
        // (This is similar to what a `ApplyOrder::TopDown` optimizer rule would do.)
        .transform_data(|(new_exprs, new_input)| {
            self.rewrite(new_input, config)?
                .map_data(|new_input| Ok((new_exprs, new_input)))
        })
    }
}

/// The window expressions, their ids and the schemas of consecutive windows,
/// and the input of the last one (see [`get_consecutive_window_exprs`])
type ConsecutiveWindows = (
    Vec<Vec<Expr>>,
    Vec<Vec<ColumnId>>,
    Vec<DFSchemaRef>,
    LogicalPlan,
);

/// Get all window expressions inside the consecutive window operators.
///
/// Returns the window expressions, and the input to the deepest child
/// LogicalPlan.
///
/// For example, if the input window looks like
///
/// ```text
///   LogicalPlan::Window(exprs=[a, b, c])
///     LogicalPlan::Window(exprs=[d])
///       InputPlan
/// ```
///
/// Returns:
/// *  `window_exprs`: `[[a, b, c], [d]]`
/// * the ids of their output columns, in the same shape
/// * the schemas of the windows
/// * InputPlan
///
/// Consecutive window expressions may refer to same complex expression.
///
/// If same complex expression is referred more than once by subsequent
/// `WindowAggr`s, we can cache complex expression by evaluating it with a
/// projection before the first WindowAggr.
///
/// This enables us to cache complex expression "c3+c4" for following plan:
///
/// ```text
/// WindowAggr: windowExpr=[[sum(c9) ORDER BY [c3 + c4] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]
/// --WindowAggr: windowExpr=[[sum(c9) ORDER BY [c3 + c4] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]
/// ```
///
/// where, it is referred once by each `WindowAggr` (total of 2) in the plan.
fn get_consecutive_window_exprs(window: Window) -> ConsecutiveWindows {
    let mut window_expr_list = vec![];
    let mut window_ids = vec![];
    let mut window_schemas = vec![];
    let mut plan = LogicalPlan::Window(window);
    while let LogicalPlan::Window(Window {
        input,
        window_expr,
        schema,
    }) = plan
    {
        let (exprs, ids) = window_expr
            .into_iter()
            .map(|entry| (entry.expr, entry.id))
            .unzip();
        window_expr_list.push(exprs);
        window_ids.push(ids);
        window_schemas.push(schema);

        plan = Arc::unwrap_or_clone(input);
    }
    (window_expr_list, window_ids, window_schemas, plan)
}

impl OptimizerRule for CommonSubexprEliminate {
    fn apply_order(&self) -> Option<ApplyOrder> {
        // This rule handles recursion itself in a `ApplyOrder::TopDown` like manner.
        // This is because in some cases adjacent nodes are collected (e.g. `Window`) and
        // CSEd as a group, which can't be done in a simple `ApplyOrder::TopDown` rule.
        None
    }

    #[cfg_attr(feature = "recursive_protection", recursive::recursive)]
    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let original_schema = Arc::clone(plan.schema());
        let is_projection = matches!(plan, LogicalPlan::Projection(_));

        let optimized_plan = match plan {
            LogicalPlan::Projection(proj) => self.try_optimize_proj(proj, config)?,
            LogicalPlan::Sort(sort) => self.try_optimize_sort(sort, config)?,
            LogicalPlan::Filter(filter) => self.try_optimize_filter(filter, config)?,
            LogicalPlan::Window(window) => self.try_optimize_window(window, config)?,
            LogicalPlan::Aggregate(agg) => self.try_optimize_aggregate(agg, config)?,
            LogicalPlan::Join(_)
            | LogicalPlan::AsOfJoin(_)
            | LogicalPlan::Repartition(_)
            | LogicalPlan::Union(_)
            | LogicalPlan::TableScan(_)
            | LogicalPlan::Values(_)
            | LogicalPlan::EmptyRelation(_)
            | LogicalPlan::Subquery(_)
            | LogicalPlan::SubqueryAlias(_)
            | LogicalPlan::Limit(_)
            | LogicalPlan::Ddl(_)
            | LogicalPlan::Explain(_)
            | LogicalPlan::Analyze(_)
            | LogicalPlan::Statement(_)
            | LogicalPlan::Show(_)
            | LogicalPlan::Distinct(_)
            | LogicalPlan::Extension(_)
            | LogicalPlan::Dml(_)
            | LogicalPlan::Copy(_)
            | LogicalPlan::Unnest(_)
            | LogicalPlan::RecursiveQuery(_)
            | LogicalPlan::MaterializedCte(_) => {
                // This rule handles recursion itself in a `ApplyOrder::TopDown` like
                // manner. Process uncorrelated subqueries in expressions
                // (e.g., Expr::ScalarSubquery), then direct children.
                plan.map_uncorrelated_subqueries(|c| self.rewrite(c, config))?
                    .transform_sibling(|plan| {
                        plan.map_children(|c| self.rewrite(c, config))
                    })?
            }
        };

        // A projection keeps its outputs (their ids): their names and fields
        // follow the rewritten expressions, see `try_optimize_proj`.
        if is_projection
            && optimized_plan.data.schema().field_ids() == original_schema.field_ids()
        {
            return Ok(optimized_plan);
        }

        // If we rewrote the plan, ensure the schema stays the same
        if optimized_plan.transformed && optimized_plan.data.schema() != &original_schema
        {
            optimized_plan.map_data(|optimized_plan| {
                build_recover_project_plan(&original_schema, optimized_plan, config)
            })
        } else {
            Ok(optimized_plan)
        }
    }

    fn name(&self) -> &str {
        "common_sub_expression_eliminate"
    }
}

/// Which type of [expressions](Expr) should be considered for rewriting?
#[derive(Debug, Clone, Copy)]
enum ExprMask {
    /// Ignores:
    ///
    /// - [`Literal`](Expr::Literal)
    /// - [`Columns`](Expr::Column)
    /// - [`ScalarVariable`](Expr::ScalarVariable)
    /// - [`Alias`](Expr::Alias)
    /// - [`Wildcard`](Expr::Wildcard)
    /// - [`AggregateFunction`](Expr::AggregateFunction)
    Normal,

    /// Like [`Normal`](Self::Normal), but includes [`AggregateFunction`](Expr::AggregateFunction).
    NormalAndAggregates,
}

struct ExprCSEController<'a> {
    column_id_generator: &'a ColumnIdGenerator,
    mask: ExprMask,
    /// The ids of the columns of the common expressions, which the references
    /// that replace them take
    common_ids: &'a mut CommonExprIds,
}

impl<'a> ExprCSEController<'a> {
    fn new(
        config: &'a dyn OptimizerConfig,
        mask: ExprMask,
        common_ids: &'a mut CommonExprIds,
    ) -> Self {
        Self {
            column_id_generator: config.column_id_generator(),
            mask,
            common_ids,
        }
    }
}

/// The ids of the columns that compute the common expressions extracted by
/// [`extract_common_exprs`], by alias: the references that replace the
/// expressions, and the entries that compute them below, take the same id
#[derive(Debug, Default)]
struct CommonExprIds {
    ids: HashMap<String, ColumnId>,
    /// The number of keys handed out (see [`Self::next_key`])
    keys: Cell<usize>,
}

impl CommonExprIds {
    /// The id of the column of the common expression `key`, allocated from
    /// `generator` when it is first asked for
    fn id(&mut self, key: &str, generator: &ColumnIdGenerator) -> ColumnId {
        *self
            .ids
            .entry(key.to_owned())
            .or_insert_with(|| generator.next())
    }

    /// A new key for a common expression. It only identifies the expression
    /// within the rule: the column that computes it is named after the
    /// expression, and referred to by id.
    fn next_key(&self) -> String {
        let n = self.keys.get() + 1;
        self.keys.set(n);
        format!("#{n}")
    }
}

/// Extracts the common subexpressions of `exprs_list` (see [`CSE`]). The ids
/// of the columns that compute them are recorded in `common_ids`.
fn extract_common_exprs(
    config: &dyn OptimizerConfig,
    mask: ExprMask,
    exprs_list: Vec<Vec<Expr>>,
    common_ids: &mut CommonExprIds,
) -> Result<FoundCommonNodes<Expr>> {
    CSE::new(ExprCSEController::new(config, mask, common_ids))
        .extract_common_nodes(exprs_list)
}

impl CSEController for ExprCSEController<'_> {
    type Node = Expr;

    fn conditional_children(node: &Expr) -> Option<(Vec<&Expr>, Vec<&Expr>)> {
        match node {
            // In case of `ScalarFunction`s and `HigherOrderFunction`s we don't know which children are surely
            // executed so start visiting all children conditionally and stop the
            // recursion with `TreeNodeRecursion::Jump`.
            Expr::ScalarFunction(ScalarFunction { func, args }) => {
                func.conditional_arguments(args)
            }
            Expr::HigherOrderFunction(HigherOrderFunction { func, args }) => {
                func.conditional_arguments(args)
            }

            // In case of `And` and `Or` the first child is surely executed, but we
            // account subexpressions as conditional in the second.
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Operator::And | Operator::Or,
                right,
            }) => Some((vec![left.as_ref()], vec![right.as_ref()])),

            // In case of `Case` the optional base expression and the first when
            // expressions are surely executed, but we account subexpressions as
            // conditional in the others.
            Expr::Case(Case {
                expr,
                when_then_expr,
                else_expr,
            }) => Some((
                expr.iter()
                    .map(|e| e.as_ref())
                    .chain(when_then_expr.iter().take(1).map(|(when, _)| when.as_ref()))
                    .collect(),
                when_then_expr
                    .iter()
                    .take(1)
                    .map(|(_, then)| then.as_ref())
                    .chain(
                        when_then_expr
                            .iter()
                            .skip(1)
                            .flat_map(|(when, then)| [when.as_ref(), then.as_ref()]),
                    )
                    .chain(else_expr.iter().map(|e| e.as_ref()))
                    .collect(),
            )),

            // An aggregate function with a `FILTER` clause evaluates the filter for
            // every input row, but it evaluates its arguments only for the rows that
            // pass the filter. Extracting an argument subexpression into a projection
            // below the aggregate would evaluate it for the rejected rows too, which
            // can raise an error (for example a failed cast or a division by zero)
            // that the filter must prevent. The `ORDER BY` expressions of the
            // aggregate are conservatively handled like the arguments.
            Expr::AggregateFunction(AggregateFunction {
                params:
                    AggregateFunctionParams {
                        args,
                        filter: Some(filter),
                        order_by,
                        ..
                    },
                ..
            }) => Some((
                vec![filter.as_ref()],
                args.iter()
                    .chain(order_by.iter().map(|sort| &sort.expr))
                    .collect(),
            )),

            // The arguments of a window function with a `FILTER` clause are
            // conservatively handled like the arguments of a filtered aggregate.
            // The window executor does not guarantee the same today:
            // `AggregateWindowExpr::get_result_column` evaluates the arguments
            // before it applies the filter. This rule must not rely on that order,
            // so it does not extract a subexpression that occurs only in such
            // arguments. The `PARTITION BY` and `ORDER BY` expressions of the
            // window are evaluated for every input row.
            Expr::WindowFunction(window_function) => {
                let WindowFunction {
                    params:
                        WindowFunctionParams {
                            args,
                            partition_by,
                            order_by,
                            filter: Some(filter),
                            ..
                        },
                    ..
                } = window_function.as_ref()
                else {
                    return None;
                };
                Some((
                    partition_by
                        .iter()
                        .chain(order_by.iter().map(|sort| &sort.expr))
                        .chain([filter.as_ref()])
                        .collect(),
                    args.iter().collect(),
                ))
            }
            _ => None,
        }
    }

    fn is_valid(node: &Expr) -> bool {
        !node.is_volatile_node()
            && !matches!(node, Expr::Lambda(_) | Expr::LambdaVariable(_))
    }

    fn is_ignored(&self, node: &Expr) -> bool {
        // MoveTowardsLeafNodes expressions (e.g. get_field) are cheap struct
        // field accesses that the ExtractLeafExpressions / PushDownLeafProjections
        // rules deliberately duplicate when needed (one copy for a filter
        // predicate, another for an output column). CSE deduplicating them
        // creates intermediate projections that fight with those rules,
        // causing optimizer instability — ExtractLeafExpressions will undo
        // the dedup, creating an infinite loop that runs until the iteration
        // limit is hit. Skip them.
        if node.placement() == ExpressionPlacement::MoveTowardsLeafNodes {
            return true;
        }

        // TODO: remove the next line after `Expr::Wildcard` is removed
        #[expect(deprecated)]
        let is_normal_minus_aggregates = matches!(
            node,
            // TODO: there's an argument for removing `Literal` from here,
            // maybe using `Expr::placemement().should_push_to_leaves()` instead
            // so that we extract common literals and don't broadcast them to num_batch_rows multiple times.
            // However that currently breaks things like `percentile_cont()` which expect literal arguments
            // (and would instead be getting a column of a common expression).
            Expr::Literal(..)
                | Expr::Column(..)
                | Expr::ScalarVariable(..)
                | Expr::Alias(..)
                | Expr::Wildcard { .. }
                | Expr::Lambda(_)
                | Expr::LambdaVariable(_)
                | Expr::WindowFunction(..)
        );

        let is_aggr = matches!(node, Expr::AggregateFunction(..));

        match self.mask {
            ExprMask::Normal => is_normal_minus_aggregates || is_aggr,
            ExprMask::NormalAndAggregates => is_normal_minus_aggregates,
        }
    }

    fn generate_alias(&self) -> String {
        self.common_ids.next_key()
    }

    fn rewrite(&mut self, node: &Self::Node, key: &str) -> Self::Node {
        // a reference to the column that computes the common expression: it
        // is named after the expression, and read by id (an expression that
        // contains it is renamed, and keeps its id)
        let id = self.common_ids.id(key, self.column_id_generator);
        Expr::Column(Column::from_name(node.schema_name().to_string()).with_id(Some(id)))
    }
}

impl Default for CommonSubexprEliminate {
    fn default() -> Self {
        Self::new()
    }
}

/// Build the "intermediate" projection plan that evaluates the extracted common
/// expressions.
///
/// # Arguments
/// input: the input plan
///
/// common_exprs: which common subexpressions were used (and thus are added to
/// intermediate projection)
///
/// expr_stats: the set of common subexpressions
fn build_common_expr_project_plan(
    input: LogicalPlan,
    common_exprs: Vec<(Expr, String)>,
    common_ids: &mut CommonExprIds,
    config: &dyn OptimizerConfig,
) -> Result<LogicalPlan> {
    // the common expressions define the columns their references read
    // (named after the expressions)
    let mut project_exprs = common_exprs
        .into_iter()
        .map(|(expr, key)| {
            let id = common_ids.id(&key, config.column_id_generator());
            NamedExpr::new(expr, id)
        })
        .collect::<Vec<_>>();

    // the input columns pass through, with their ids (also one with the
    // name of a common expression: they are told apart by their ids)
    for col in input.schema().columns() {
        project_exprs.push(NamedExpr::allocate(
            Expr::Column(col),
            input.schema(),
            config.column_id_generator(),
        ));
    }

    Projection::try_new(project_exprs, Arc::new(input)).map(LogicalPlan::Projection)
}

/// Build the projection plan to eliminate unnecessary columns produced by
/// the "intermediate" projection plan built in [build_common_expr_project_plan].
///
/// This is required to keep the schema the same for plans that pass the input
/// on to the output, such as `Filter` or `Sort`.
fn build_recover_project_plan(
    schema: &DFSchema,
    input: LogicalPlan,
    config: &dyn OptimizerConfig,
) -> Result<LogicalPlan> {
    // the columns of `schema` (which have their ids) pass through
    let col_exprs = schema
        .columns()
        .into_iter()
        .map(|col| {
            NamedExpr::allocate(
                Expr::Column(col),
                input.schema(),
                config.column_id_generator(),
            )
        })
        .collect();
    Projection::try_new(col_exprs, Arc::new(input)).map(LogicalPlan::Projection)
}

#[cfg(test)]
mod test {

    use std::collections::HashSet;
    use std::iter;

    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_expr::expr_rewriter::normalize_col;
    use datafusion_expr::logical_plan::{JoinType, projection_schema, table_scan};
    use datafusion_expr::{
        AccumulatorFactoryFunction, AggregateUDF, ColumnarValue, ExprFunctionExt,
        ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, SimpleAggregateUDF,
        Volatility, WindowFunctionDefinition, grouping_set, is_null, not, when,
    };
    use datafusion_expr::{col, lit, logical_plan::builder::LogicalPlanBuilder};

    use super::*;
    use crate::assert_optimized_plan_eq_snapshot;
    use crate::optimizer::OptimizerContext;
    use crate::test::udfs::leaf_udf_expr;
    use crate::test::*;
    use datafusion_expr::test::function_stub::{avg, count, sum, sum_udaf};
    use datafusion_functions_window::row_number::row_number_udwf;

    macro_rules! assert_optimized_plan_equal {
        (
            $config:expr,
            $plan:expr,
            @ $expected:literal $(,)?
        ) => {{
            let rules: Vec<Arc<dyn crate::OptimizerRule + Send + Sync>> = vec![Arc::new(CommonSubexprEliminate::new())];
            assert_optimized_plan_eq_snapshot!(
                $config,
                rules,
                $plan,
                @ $expected,
            )
        }};

        (
            $plan:expr,
            @ $expected:literal $(,)?
        ) => {{
            let rules: Vec<Arc<dyn crate::OptimizerRule + Send + Sync>> = vec![Arc::new(CommonSubexprEliminate::new())];
            let optimizer_ctx = OptimizerContext::new();
            assert_optimized_plan_eq_snapshot!(
                optimizer_ctx,
                rules,
                $plan,
                @ $expected,
            )
        }};
    }

    #[test]
    fn tpch_q1_simplified() -> Result<()> {
        // SQL:
        //  select
        //      sum(a * (1 - b)),
        //      sum(a * (1 - b) * (1 + c))
        //  from T;
        //
        // The manual assembled logical plan don't contains the outermost `Projection`.

        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(
                iter::empty::<Expr>(),
                vec![
                    sum(col("a") * (lit(1) - col("b"))),
                    sum((col("a") * (lit(1) - col("b"))) * (lit(1) + col("c"))),
                ],
            )?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @ "
        Aggregate: groupBy=[[]], aggr=[[sum(test.a * Int32(1) - test.b), sum(test.a * Int32(1) - test.b * (Int32(1) + test.c))]]
          Projection: test.a * (Int32(1) - test.b), test.a, test.b, test.c
            TableScan: test
        "
        )
    }

    #[test]
    fn nested_aliases() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![
                (col("a") + col("b") - col("c")).alias("alias1") * (col("a") + col("b")),
                col("a") + col("b"),
            ])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @ "
        Projection: test.a + test.b - test.c AS alias1 * test.a + test.b, test.a + test.b
          Projection: test.a + test.b, test.a, test.b, test.c
            TableScan: test
        "
        )
    }

    #[test]
    fn aggregate() -> Result<()> {
        let table_scan = test_table_scan()?;

        let return_type = DataType::UInt32;
        let accumulator: AccumulatorFactoryFunction = Arc::new(|_| unimplemented!());
        let udf_agg = |inner: Expr| {
            Expr::AggregateFunction(AggregateFunction::new_udf(
                Arc::new(AggregateUDF::from(SimpleAggregateUDF::new_with_signature(
                    "my_agg",
                    Signature::exact(vec![DataType::UInt32], Volatility::Stable),
                    return_type.clone(),
                    Arc::clone(&accumulator),
                    vec![Field::new("value", DataType::UInt32, true).into()],
                ))),
                vec![inner],
                false,
                None,
                vec![],
                None,
            ))
        };

        // test: common aggregates
        let plan = LogicalPlanBuilder::from(table_scan.clone())
            .aggregate(
                iter::empty::<Expr>(),
                vec![
                    // common: avg(col("a"))
                    avg(col("a")).alias("col1"),
                    avg(col("a")).alias("col2"),
                    // no common
                    avg(col("b")).alias("col3"),
                    avg(col("c")),
                    // common: udf_agg(col("a"))
                    udf_agg(col("a")).alias("col4"),
                    udf_agg(col("a")).alias("col5"),
                    // no common
                    udf_agg(col("b")).alias("col6"),
                    udf_agg(col("c")),
                ],
            )?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @ "
        Projection: avg(test.a) AS col1, avg(test.a) AS col2, col3, avg(test.c), my_agg(test.a) AS col4, my_agg(test.a) AS col5, col6, my_agg(test.c)
          Aggregate: groupBy=[[]], aggr=[[avg(test.a), my_agg(test.a), avg(test.b) AS col3, avg(test.c), my_agg(test.b) AS col6, my_agg(test.c)]]
            TableScan: test
        "
        )?;

        // test: trafo after aggregate
        let plan = LogicalPlanBuilder::from(table_scan.clone())
            .aggregate(
                iter::empty::<Expr>(),
                vec![
                    lit(1) + avg(col("a")),
                    lit(1) - avg(col("a")),
                    lit(1) + udf_agg(col("a")),
                    lit(1) - udf_agg(col("a")),
                ],
            )?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @ "
        Projection: Int32(1) + avg(test.a), Int32(1) - avg(test.a), Int32(1) + my_agg(test.a), Int32(1) - my_agg(test.a)
          Aggregate: groupBy=[[]], aggr=[[avg(test.a), my_agg(test.a)]]
            TableScan: test
        "
        )?;

        // test: transformation before aggregate
        let plan = LogicalPlanBuilder::from(table_scan.clone())
            .aggregate(
                iter::empty::<Expr>(),
                vec![
                    avg(lit(1u32) + col("a")).alias("col1"),
                    udf_agg(lit(1u32) + col("a")).alias("col2"),
                ],
            )?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @ "
        Aggregate: groupBy=[[]], aggr=[[avg(UInt32(1) + test.a) AS col1, my_agg(UInt32(1) + test.a) AS col2]]
          Projection: UInt32(1) + test.a, test.a, test.b, test.c
            TableScan: test
        "
        )?;

        // test: common between agg and group
        let plan = LogicalPlanBuilder::from(table_scan.clone())
            .aggregate(
                vec![lit(1u32) + col("a")],
                vec![
                    avg(lit(1u32) + col("a")).alias("col1"),
                    udf_agg(lit(1u32) + col("a")).alias("col2"),
                ],
            )?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @ "
        Aggregate: groupBy=[[UInt32(1) + test.a]], aggr=[[avg(UInt32(1) + test.a) AS col1, my_agg(UInt32(1) + test.a) AS col2]]
          Projection: UInt32(1) + test.a, test.a, test.b, test.c
            TableScan: test
        "
        )?;

        // test: all mixed
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(
                vec![lit(1u32) + col("a")],
                vec![
                    (lit(1u32) + avg(lit(1u32) + col("a"))).alias("col1"),
                    (lit(1u32) - avg(lit(1u32) + col("a"))).alias("col2"),
                    avg(lit(1u32) + col("a")),
                    (lit(1u32) + udf_agg(lit(1u32) + col("a"))).alias("col3"),
                    (lit(1u32) - udf_agg(lit(1u32) + col("a"))).alias("col4"),
                    udf_agg(lit(1u32) + col("a")),
                ],
            )?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @ "
        Projection: UInt32(1) + test.a, UInt32(1) + avg(UInt32(1) + test.a) AS col1, UInt32(1) - avg(UInt32(1) + test.a) AS col2, avg(UInt32(1) + test.a), UInt32(1) + my_agg(UInt32(1) + test.a) AS col3, UInt32(1) - my_agg(UInt32(1) + test.a) AS col4, my_agg(UInt32(1) + test.a)
          Aggregate: groupBy=[[UInt32(1) + test.a]], aggr=[[avg(UInt32(1) + test.a), my_agg(UInt32(1) + test.a)]]
            Projection: UInt32(1) + test.a, test.a, test.b, test.c
              TableScan: test
        "
        )
    }

    #[test]
    fn aggregate_with_relations_and_dots() -> Result<()> {
        let schema = Schema::new(vec![Field::new("col.a", DataType::UInt32, false)]);
        let table_scan = table_scan(Some("table.test"), &schema, None)?.build()?;

        let col_a = Expr::Column(Column::new(Some("table.test"), "col.a"));

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(
                vec![col_a.clone()],
                vec![
                    (lit(1u32) + avg(lit(1u32) + col_a.clone())),
                    avg(lit(1u32) + col_a),
                ],
            )?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @ "
        Projection: table.test.col.a, UInt32(1) + avg(UInt32(1) + table.test.col.a), avg(UInt32(1) + table.test.col.a)
          Aggregate: groupBy=[[table.test.col.a]], aggr=[[avg(UInt32(1) + table.test.col.a)]]
            Projection: UInt32(1) + table.test.col.a, table.test.col.a
              TableScan: table.test
        "
        )
    }

    #[test]
    fn common_aggregate_grouping_set_preserves_internal_id() -> Result<()> {
        let plan = LogicalPlanBuilder::from(test_table_scan()?)
            .aggregate(
                vec![grouping_set(vec![vec![col("a")]])],
                vec![avg(col("b")).alias("first"), avg(col("b")).alias("second")],
            )?
            .filter(col(Aggregate::INTERNAL_GROUPING_ID).eq(lit(0_u8)))?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @ "
        Filter: __grouping_id = UInt8(0)
          Projection: test.a, __grouping_id, avg(test.b) AS first, avg(test.b) AS second
            Aggregate: groupBy=[[GROUPING SETS ((test.a))]], aggr=[[avg(test.b)]]
              TableScan: test
        "
        )
    }

    #[test]
    fn subexpr_in_same_order() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![
                (lit(1) + col("a")).alias("first"),
                (lit(1) + col("a")).alias("second"),
            ])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @ "
        Projection: Int32(1) + test.a AS first, Int32(1) + test.a AS second
          Projection: Int32(1) + test.a, test.a, test.b, test.c
            TableScan: test
        "
        )
    }

    #[test]
    fn subexpr_in_different_order() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![lit(1) + col("a"), col("a") + lit(1)])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @ "
        Projection: Int32(1) + test.a, Int32(1) + test.a AS test.a + Int32(1)
          Projection: Int32(1) + test.a, test.a, test.b, test.c
            TableScan: test
        "
        )
    }

    #[test]
    fn cross_plans_subexpr() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![lit(1) + col("a"), col("a")])?
            .project(vec![lit(1) + col("a")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @ r"
        Projection: Int32(1) + test.a
          Projection: Int32(1) + test.a, test.a
            TableScan: test
        "
        )
    }

    /// The columns of the common expressions are named after the
    /// expressions, beside the input columns (also ones with the same name):
    /// they are told apart by their ids
    #[test]
    fn common_expr_project_fields_have_distinct_ids() {
        let table_scan = test_table_scan().unwrap();
        let table_scan_1 = test_table_scan_with_name("test1").unwrap();
        let table_scan_2 = test_table_scan_with_name("test2").unwrap();
        let join = LogicalPlanBuilder::from(table_scan_1)
            .join(table_scan_2, JoinType::Inner, (vec!["a"], vec!["a"]), None)
            .unwrap()
            .build()
            .unwrap();
        for (input, c_plus_a, b_plus_a) in [
            (table_scan, col("c") + col("a"), col("b") + col("a")),
            (
                join,
                col("test1.c") + col("test1.a"),
                col("test1.b") + col("test1.a"),
            ),
        ] {
            let c_plus_a = normalize_col(c_plus_a, &input).unwrap();
            let b_plus_a = normalize_col(b_plus_a, &input).unwrap();
            let common_exprs = vec![
                (c_plus_a.clone(), "#1".to_string()),
                (b_plus_a.clone(), "#2".to_string()),
            ];
            // one generator, past the plan: the common expressions must not
            // reuse the ids of the scan
            let config = OptimizerContext::new();
            if let Some(max) = input.max_column_id() {
                config.column_id_generator().advance_past(max);
            }
            let input_width = input.schema().fields().len();
            let project = build_common_expr_project_plan(
                input,
                common_exprs,
                &mut CommonExprIds::default(),
                &config,
            )
            .unwrap();
            let schema = project.schema();
            assert_eq!(schema.fields().len(), input_width + 2);
            assert_eq!(schema.field(0).name(), &c_plus_a.schema_name().to_string());
            assert_eq!(schema.field(1).name(), &b_plus_a.schema_name().to_string());
            let ids = schema.field_ids().iter().flatten().collect::<HashSet<_>>();
            assert_eq!(ids.len(), schema.fields().len());
        }
    }

    #[test]
    fn eliminated_subexpr_datatype() {
        use datafusion_expr::cast;

        let schema = Schema::new(vec![
            Field::new("a", DataType::UInt64, false),
            Field::new("b", DataType::UInt64, false),
            Field::new("c", DataType::UInt64, false),
        ]);

        let plan = table_scan(Some("table"), &schema, None)
            .unwrap()
            .filter(
                cast(col("a"), DataType::Int64)
                    .lt(lit(1_i64))
                    .and(cast(col("a"), DataType::Int64).not_eq(lit(1_i64))),
            )
            .unwrap()
            .build()
            .unwrap();
        let rule = CommonSubexprEliminate::new();
        // the optimizer moves its generator past the ids of the plan before
        // running the rules
        let config = OptimizerContext::new();
        if let Some(max) = plan.max_column_id() {
            config.column_id_generator().advance_past(max);
        }
        let optimized_plan = rule.rewrite(plan, &config).unwrap();
        assert!(optimized_plan.transformed);
        let optimized_plan = optimized_plan.data;

        let schema = optimized_plan.schema();
        let fields_with_datatypes: Vec<_> = schema
            .fields()
            .iter()
            .map(|field| (field.name(), field.data_type()))
            .collect();
        let formatted_fields_with_datatype = format!("{fields_with_datatypes:#?}");
        let expected = r#"[
    (
        "a",
        UInt64,
    ),
    (
        "b",
        UInt64,
    ),
    (
        "c",
        UInt64,
    ),
]"#;
        assert_eq!(expected, formatted_fields_with_datatype);
    }

    #[test]
    fn filter_schema_changed() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter((lit(1) + col("a") - lit(10)).gt(lit(1) + col("a")))?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @ "
        Projection: test.a, test.b, test.c
          Filter: Int32(1) + test.a - Int32(10) > Int32(1) + test.a
            Projection: Int32(1) + test.a, test.a, test.b, test.c
              TableScan: test
        "
        )
    }

    #[test]
    fn test_alias_collision() -> Result<()> {
        let table_scan = test_table_scan()?;

        // user columns named like the old generated names
        let config = OptimizerContext::new();
        let common_expr_1 = "__common_expr_1".to_string();
        let plan = LogicalPlanBuilder::from(table_scan.clone())
            .project(vec![
                (col("a") + col("b")).alias(common_expr_1.clone()),
                col("c"),
            ])?
            .project(vec![
                col(common_expr_1.clone()).alias("c1"),
                col(common_expr_1).alias("c2"),
                (col("c") + lit(2)).alias("c3"),
                (col("c") + lit(2)).alias("c4"),
            ])?
            .build()?;

        assert_optimized_plan_equal!(
            config,
            plan,
            @ "
        Projection: __common_expr_1 AS c1, __common_expr_1 AS c2, test.c + Int32(2) AS c3, test.c + Int32(2) AS c4
          Projection: test.c + Int32(2), __common_expr_1, test.c
            Projection: test.a + test.b AS __common_expr_1, test.c
              TableScan: test
        "
        )?;

        let config = OptimizerContext::new();
        let common_expr_2 = "__common_expr_2".to_string();
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![
                (col("a") + col("b")).alias(common_expr_2.clone()),
                col("c"),
            ])?
            .project(vec![
                col(common_expr_2.clone()).alias("c1"),
                col(common_expr_2).alias("c2"),
                (col("c") + lit(2)).alias("c3"),
                (col("c") + lit(2)).alias("c4"),
            ])?
            .build()?;

        assert_optimized_plan_equal!(
            config,
            plan,
            @ "
        Projection: __common_expr_2 AS c1, __common_expr_2 AS c2, test.c + Int32(2) AS c3, test.c + Int32(2) AS c4
          Projection: test.c + Int32(2), __common_expr_2, test.c
            Projection: test.a + test.b AS __common_expr_2, test.c
              TableScan: test
        "
        )?;

        Ok(())
    }

    #[test]
    fn test_short_circuits() -> Result<()> {
        let table_scan = test_table_scan()?;

        let extracted_short_circuit = col("a").eq(lit(0)).or(col("b").eq(lit(0)));
        let extracted_short_circuit_leg_1 = (col("a") + col("b")).eq(lit(0));
        let not_extracted_short_circuit_leg_2 = (col("a") - col("b")).eq(lit(0));
        let extracted_short_circuit_leg_3 = (col("a") * col("b")).eq(lit(0));
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![
                extracted_short_circuit.clone().alias("c1"),
                extracted_short_circuit.alias("c2"),
                extracted_short_circuit_leg_1
                    .clone()
                    .or(not_extracted_short_circuit_leg_2.clone())
                    .alias("c3"),
                extracted_short_circuit_leg_1
                    .and(not_extracted_short_circuit_leg_2)
                    .alias("c4"),
                extracted_short_circuit_leg_3
                    .clone()
                    .or(extracted_short_circuit_leg_3)
                    .alias("c5"),
            ])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @ "
        Projection: test.a = Int32(0) OR test.b = Int32(0) AS c1, test.a = Int32(0) OR test.b = Int32(0) AS c2, test.a + test.b = Int32(0) OR test.a - test.b = Int32(0) AS c3, test.a + test.b = Int32(0) AND test.a - test.b = Int32(0) AS c4, test.a * test.b = Int32(0) OR test.a * test.b = Int32(0) AS c5
          Projection: test.a = Int32(0) OR test.b = Int32(0), test.a + test.b = Int32(0), test.a * test.b = Int32(0), test.a, test.b, test.c
            TableScan: test
        "
        )
    }

    #[test]
    fn test_volatile() -> Result<()> {
        let table_scan = test_table_scan()?;

        let extracted_child = col("a") + col("b");
        let rand = rand_func().call(vec![]);
        let not_extracted_volatile = extracted_child + rand;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![
                not_extracted_volatile.clone().alias("c1"),
                not_extracted_volatile.alias("c2"),
            ])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @ "
        Projection: test.a + test.b + random() AS c1, test.a + test.b + random() AS c2
          Projection: test.a + test.b, test.a, test.b, test.c
            TableScan: test
        "
        )
    }

    #[test]
    fn test_volatile_short_circuits() -> Result<()> {
        let table_scan = test_table_scan()?;

        let rand = rand_func().call(vec![]);
        let extracted_short_circuit_leg_1 = col("a").eq(lit(0));
        let not_extracted_volatile_short_circuit_1 =
            extracted_short_circuit_leg_1.or(rand.clone().eq(lit(0)));
        let not_extracted_short_circuit_leg_2 = col("b").eq(lit(0));
        let not_extracted_volatile_short_circuit_2 =
            rand.eq(lit(0)).or(not_extracted_short_circuit_leg_2);
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![
                not_extracted_volatile_short_circuit_1.clone().alias("c1"),
                not_extracted_volatile_short_circuit_1.alias("c2"),
                not_extracted_volatile_short_circuit_2.clone().alias("c3"),
                not_extracted_volatile_short_circuit_2.alias("c4"),
            ])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @ "
        Projection: test.a = Int32(0) OR random() = Int32(0) AS c1, test.a = Int32(0) OR random() = Int32(0) AS c2, random() = Int32(0) OR test.b = Int32(0) AS c3, random() = Int32(0) OR test.b = Int32(0) AS c4
          Projection: test.a = Int32(0), test.a, test.b, test.c
            TableScan: test
        "
        )
    }

    #[test]
    fn test_non_top_level_common_expression() -> Result<()> {
        let table_scan = test_table_scan()?;

        let common_expr = col("a") + col("b");
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![
                common_expr.clone().alias("c1"),
                common_expr.alias("c2"),
            ])?
            .project(vec![col("c1"), col("c2")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @ "
        Projection: c1, c2
          Projection: test.a + test.b AS c1, test.a + test.b AS c2
            Projection: test.a + test.b, test.a, test.b, test.c
              TableScan: test
        "
        )
    }

    #[test]
    fn test_nested_common_expression() -> Result<()> {
        let table_scan = test_table_scan()?;

        let nested_common_expr = col("a") + col("b");
        let common_expr = nested_common_expr.clone() * nested_common_expr;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![
                common_expr.clone().alias("c1"),
                common_expr.alias("c2"),
            ])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @ "
        Projection: test.a + test.b * test.a + test.b AS c1, test.a + test.b * test.a + test.b AS c2
          Projection: test.a + test.b * test.a + test.b, test.a, test.b, test.c
            Projection: test.a + test.b, test.a, test.b, test.c
              TableScan: test
        "
        )
    }

    #[test]
    fn test_normalize_add_expression() -> Result<()> {
        // a + b <=> b + a
        let table_scan = test_table_scan()?;
        let expr = ((col("a") + col("b")) * (col("b") + col("a"))).eq(lit(30));
        let plan = LogicalPlanBuilder::from(table_scan).filter(expr)?.build()?;

        assert_optimized_plan_equal!(
            plan,
            @ "
        Projection: test.a, test.b, test.c
          Filter: test.a + test.b * test.a + test.b = Int32(30)
            Projection: test.a + test.b, test.a, test.b, test.c
              TableScan: test
        "
        )
    }

    #[test]
    fn test_normalize_multi_expression() -> Result<()> {
        // a * b <=> b * a
        let table_scan = test_table_scan()?;
        let expr = ((col("a") * col("b")) + (col("b") * col("a"))).eq(lit(30));
        let plan = LogicalPlanBuilder::from(table_scan).filter(expr)?.build()?;

        assert_optimized_plan_equal!(
            plan,
            @ "
        Projection: test.a, test.b, test.c
          Filter: test.a * test.b + test.a * test.b = Int32(30)
            Projection: test.a * test.b, test.a, test.b, test.c
              TableScan: test
        "
        )
    }

    #[test]
    fn test_normalize_bitset_and_expression() -> Result<()> {
        // a & b <=> b & a
        let table_scan = test_table_scan()?;
        let expr = ((col("a") & col("b")) + (col("b") & col("a"))).eq(lit(30));
        let plan = LogicalPlanBuilder::from(table_scan).filter(expr)?.build()?;

        assert_optimized_plan_equal!(
            plan,
            @ "
        Projection: test.a, test.b, test.c
          Filter: test.a & test.b + test.a & test.b = Int32(30)
            Projection: test.a & test.b, test.a, test.b, test.c
              TableScan: test
        "
        )
    }

    #[test]
    fn test_normalize_bitset_or_expression() -> Result<()> {
        // a | b <=> b | a
        let table_scan = test_table_scan()?;
        let expr = ((col("a") | col("b")) + (col("b") | col("a"))).eq(lit(30));
        let plan = LogicalPlanBuilder::from(table_scan).filter(expr)?.build()?;

        assert_optimized_plan_equal!(
            plan,
            @ "
        Projection: test.a, test.b, test.c
          Filter: test.a | test.b + test.a | test.b = Int32(30)
            Projection: test.a | test.b, test.a, test.b, test.c
              TableScan: test
        "
        )
    }

    #[test]
    fn test_normalize_bitset_xor_expression() -> Result<()> {
        // a # b <=> b # a
        let table_scan = test_table_scan()?;
        let expr = ((col("a") ^ col("b")) + (col("b") ^ col("a"))).eq(lit(30));
        let plan = LogicalPlanBuilder::from(table_scan).filter(expr)?.build()?;

        assert_optimized_plan_equal!(
            plan,
            @ "
        Projection: test.a, test.b, test.c
          Filter: test.a BIT_XOR test.b + test.a BIT_XOR test.b = Int32(30)
            Projection: test.a BIT_XOR test.b, test.a, test.b, test.c
              TableScan: test
        "
        )
    }

    #[test]
    fn test_normalize_eq_expression() -> Result<()> {
        // a = b <=> b = a
        let table_scan = test_table_scan()?;
        let expr = (col("a").eq(col("b"))).and(col("b").eq(col("a")));
        let plan = LogicalPlanBuilder::from(table_scan).filter(expr)?.build()?;

        assert_optimized_plan_equal!(
            plan,
            @ "
        Projection: test.a, test.b, test.c
          Filter: test.a = test.b AND test.a = test.b
            Projection: test.a = test.b, test.a, test.b, test.c
              TableScan: test
        "
        )
    }

    #[test]
    fn test_normalize_ne_expression() -> Result<()> {
        // a != b <=> b != a
        let table_scan = test_table_scan()?;
        let expr = (col("a").not_eq(col("b"))).and(col("b").not_eq(col("a")));
        let plan = LogicalPlanBuilder::from(table_scan).filter(expr)?.build()?;

        assert_optimized_plan_equal!(
            plan,
            @ "
        Projection: test.a, test.b, test.c
          Filter: test.a != test.b AND test.a != test.b
            Projection: test.a != test.b, test.a, test.b, test.c
              TableScan: test
        "
        )
    }

    #[test]
    fn test_normalize_complex_expression() -> Result<()> {
        // case1: a + b * c <=> b * c + a
        let table_scan = test_table_scan()?;
        let expr = ((col("a") + col("b") * col("c")) - (col("b") * col("c") + col("a")))
            .eq(lit(30));
        let plan = LogicalPlanBuilder::from(table_scan).filter(expr)?.build()?;

        assert_optimized_plan_equal!(
            plan,
            @ "
        Projection: test.a, test.b, test.c
          Filter: test.a + test.b * test.c - test.a + test.b * test.c = Int32(30)
            Projection: test.a + test.b * test.c, test.a, test.b, test.c
              TableScan: test
        "
        )?;

        // ((c1 + c2 / c3) * c3 <=> c3 * (c2 / c3 + c1))
        let table_scan = test_table_scan()?;
        let expr = (((col("a") + col("b") / col("c")) * col("c"))
            / (col("c") * (col("b") / col("c") + col("a")))
            + col("a"))
        .eq(lit(30));
        let plan = LogicalPlanBuilder::from(table_scan).filter(expr)?.build()?;

        assert_optimized_plan_equal!(
            plan,
            @ "
        Projection: test.a, test.b, test.c
          Filter: test.a + test.b / test.c * test.c / test.a + test.b / test.c * test.c + test.a = Int32(30)
            Projection: (test.a + test.b / test.c) * test.c, test.a, test.b, test.c
              TableScan: test
        "
        )?;

        // c2 / (c1 + c3) <=> c2 / (c3 + c1)
        let table_scan = test_table_scan()?;
        let expr = ((col("b") / (col("a") + col("c")))
            * (col("b") / (col("c") + col("a"))))
        .eq(lit(30));
        let plan = LogicalPlanBuilder::from(table_scan).filter(expr)?.build()?;
        assert_optimized_plan_equal!(
            plan,
            @ "
        Projection: test.a, test.b, test.c
          Filter: test.b / test.a + test.c * test.b / test.a + test.c = Int32(30)
            Projection: test.b / (test.a + test.c), test.a, test.b, test.c
              TableScan: test
        "
        )?;

        Ok(())
    }

    #[derive(Debug, PartialEq, Eq, Hash)]
    pub struct TestUdf {
        signature: Signature,
    }

    impl TestUdf {
        pub fn new() -> Self {
            Self {
                signature: Signature::numeric(1, Volatility::Immutable),
            }
        }
    }

    impl ScalarUDFImpl for TestUdf {
        fn name(&self) -> &str {
            "my_udf"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, _: &[DataType]) -> Result<DataType> {
            Ok(DataType::Int32)
        }

        fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
            panic!("not implemented")
        }
    }

    #[test]
    fn test_normalize_inner_binary_expression() -> Result<()> {
        // Not(a == b) <=> Not(b == a)
        let table_scan = test_table_scan()?;
        let expr1 = not(col("a").eq(col("b")));
        let expr2 = not(col("b").eq(col("a")));
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![expr1, expr2])?
            .build()?;
        assert_optimized_plan_equal!(
            plan,
            @ "
        Projection: NOT test.a = test.b, NOT test.a = test.b AS NOT test.b = test.a
          Projection: NOT test.a = test.b, test.a, test.b, test.c
            TableScan: test
        "
        )?;

        // is_null(a == b) <=> is_null(b == a)
        let table_scan = test_table_scan()?;
        let expr1 = is_null(col("a").eq(col("b")));
        let expr2 = is_null(col("b").eq(col("a")));
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![expr1, expr2])?
            .build()?;
        assert_optimized_plan_equal!(
            plan,
            @ "
        Projection: test.a = test.b IS NULL, test.a = test.b IS NULL AS test.b = test.a IS NULL
          Projection: test.a = test.b IS NULL, test.a, test.b, test.c
            TableScan: test
        "
        )?;

        // a + b between 0 and 10 <=> b + a between 0 and 10
        let table_scan = test_table_scan()?;
        let expr1 = (col("a") + col("b")).between(lit(0), lit(10));
        let expr2 = (col("b") + col("a")).between(lit(0), lit(10));
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![expr1, expr2])?
            .build()?;
        assert_optimized_plan_equal!(
            plan,
            @ "
        Projection: test.a + test.b BETWEEN Int32(0) AND Int32(10), test.a + test.b BETWEEN Int32(0) AND Int32(10) AS test.b + test.a BETWEEN Int32(0) AND Int32(10)
          Projection: test.a + test.b BETWEEN Int32(0) AND Int32(10), test.a, test.b, test.c
            TableScan: test
        "
        )?;

        // c between a + b and 10 <=> c between b + a and 10
        let table_scan = test_table_scan()?;
        let expr1 = col("c").between(col("a") + col("b"), lit(10));
        let expr2 = col("c").between(col("b") + col("a"), lit(10));
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![expr1, expr2])?
            .build()?;
        assert_optimized_plan_equal!(
            plan,
            @ "
        Projection: test.c BETWEEN test.a + test.b AND Int32(10), test.c BETWEEN test.a + test.b AND Int32(10) AS test.c BETWEEN test.b + test.a AND Int32(10)
          Projection: test.c BETWEEN test.a + test.b AND Int32(10), test.a, test.b, test.c
            TableScan: test
        "
        )?;

        // function call with argument <=> function call with argument
        let udf = ScalarUDF::from(TestUdf::new());
        let table_scan = test_table_scan()?;
        let expr1 = udf.call(vec![col("a") + col("b")]);
        let expr2 = udf.call(vec![col("b") + col("a")]);
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![expr1, expr2])?
            .build()?;
        assert_optimized_plan_equal!(
            plan,
            @ "
        Projection: my_udf(test.a + test.b), my_udf(test.a + test.b) AS my_udf(test.b + test.a)
          Projection: my_udf(test.a + test.b), test.a, test.b, test.c
            TableScan: test
        "
        )
    }

    /// returns a "random" function that is marked volatile (aka each invocation
    /// returns a different value)
    ///
    /// Does not use datafusion_functions::rand to avoid introducing a
    /// dependency on that crate.
    fn rand_func() -> ScalarUDF {
        ScalarUDF::new_from_impl(RandomStub::new())
    }

    #[derive(Debug, PartialEq, Eq, Hash)]
    struct RandomStub {
        signature: Signature,
    }

    impl RandomStub {
        fn new() -> Self {
            Self {
                signature: Signature::exact(vec![], Volatility::Volatile),
            }
        }
    }
    impl ScalarUDFImpl for RandomStub {
        fn name(&self) -> &str {
            "random"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
            Ok(DataType::Float64)
        }

        fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
            panic!("dummy - not implemented")
        }
    }

    /// Identical MoveTowardsLeafNodes expressions should NOT be deduplicated
    /// by CSE — they are cheap (e.g. struct field access) and the extraction
    /// rules deliberately duplicate them. Deduplicating causes optimizer
    /// instability where one optimizer rule will undo the work of another,
    /// resulting in an infinite optimization loop until the
    /// we hit the max iteration limit and then give up.
    #[test]
    fn test_leaf_expression_not_extracted() -> Result<()> {
        let table_scan = test_table_scan()?;

        let leaf = leaf_udf_expr(col("a"));
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![leaf.clone().alias("c1"), leaf.alias("c2")])?
            .build()?;

        // Plan should be unchanged — no common expression extracted
        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: leaf_udf(test.a) AS c1, leaf_udf(test.a) AS c2
          TableScan: test
        "
        )
    }

    /// When a MoveTowardsLeafNodes expression appears as a sub-expression of
    /// a larger expression that IS duplicated, only the outer expression gets
    /// deduplicated; the leaf sub-expression stays inline.
    #[test]
    fn test_leaf_subexpression_not_extracted() -> Result<()> {
        let table_scan = test_table_scan()?;

        // leaf_udf(a) + b appears twice — the outer `+` is a common
        // sub-expression, but leaf_udf(a) by itself is MoveTowardsLeafNodes
        // and should not be extracted separately.
        let common = leaf_udf_expr(col("a")) + col("b");
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![common.clone().alias("c1"), common.alias("c2")])?
            .build()?;

        // The whole `leaf_udf(a) + b` gets deduplicated into one common column,
        // but leaf_udf(a) alone is NOT pulled out.
        assert_optimized_plan_equal!(
            plan,
            @"
        Projection: leaf_udf(test.a) + test.b AS c1, leaf_udf(test.a) + test.b AS c2
          Projection: leaf_udf(test.a) + test.b, test.a, test.b, test.c
            TableScan: test
        "
        )
    }

    #[test]
    fn test_window_function_is_not_extracted() -> Result<()> {
        let wexpr = row_number_udwf().call(vec![]);

        let plan = LogicalPlanBuilder::empty(true)
            .window(vec![wexpr.clone(), wexpr.alias("aliased")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        WindowAggr: windowExpr=[[row_number() ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING, row_number() ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING AS aliased]]
          EmptyRelation: rows=1
        "
        )
    }

    #[test]
    fn test_aggregate_filter_arguments() -> Result<()> {
        let table_scan = test_table_scan()?;

        // An aggregate evaluates its arguments only for the rows that pass its
        // `FILTER`. Thus `a + b` must stay in the aggregates. The filter `c > 0` is
        // evaluated for every row and is extracted.
        let filter = col("c").gt(lit(0));
        let plan = LogicalPlanBuilder::from(table_scan.clone())
            .aggregate(
                iter::empty::<Expr>(),
                vec![
                    sum(col("a") + col("b")).filter(filter.clone()).build()?,
                    count(col("a") + col("b")).filter(filter.clone()).build()?,
                ],
            )?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @ "
        Aggregate: groupBy=[[]], aggr=[[sum(test.a + test.b) FILTER (WHERE test.c > Int32(0)), COUNT(test.a + test.b) FILTER (WHERE test.c > Int32(0))]]
          Projection: test.c > Int32(0), test.a, test.b, test.c
            TableScan: test
        "
        )?;

        // `a + b` is surely evaluated by the aggregate without a `FILTER`, so it is
        // extracted.
        let plan = LogicalPlanBuilder::from(table_scan.clone())
            .aggregate(
                iter::empty::<Expr>(),
                vec![
                    sum(col("a") + col("b")).filter(filter.clone()).build()?,
                    count(col("a") + col("b")),
                ],
            )?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @ "
        Aggregate: groupBy=[[]], aggr=[[sum(test.a + test.b) FILTER (WHERE test.c > Int32(0)), COUNT(test.a + test.b)]]
          Projection: test.a + test.b, test.a, test.b, test.c
            TableScan: test
        "
        )?;

        // `a + b` is surely evaluated by the filter, so it is extracted.
        let plan = LogicalPlanBuilder::from(table_scan.clone())
            .aggregate(
                iter::empty::<Expr>(),
                vec![
                    sum(col("a") + col("b"))
                        .filter((col("a") + col("b")).gt(lit(0)))
                        .build()?,
                ],
            )?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @ "
        Aggregate: groupBy=[[]], aggr=[[sum(test.a + test.b) FILTER (WHERE test.a + test.b > Int32(0))]]
          Projection: test.a + test.b, test.a, test.b, test.c
            TableScan: test
        "
        )?;

        // Identical aggregates with a `FILTER` are still deduplicated.
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(
                iter::empty::<Expr>(),
                vec![
                    sum(col("a") + col("b"))
                        .filter(filter.clone())
                        .build()?
                        .alias("s1"),
                    sum(col("a") + col("b")).filter(filter).build()?.alias("s2"),
                ],
            )?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @ "
        Projection: sum(test.a + test.b) FILTER (WHERE test.c > Int32(0)) AS s1, sum(test.a + test.b) FILTER (WHERE test.c > Int32(0)) AS s2
          Aggregate: groupBy=[[]], aggr=[[sum(test.a + test.b) FILTER (WHERE test.c > Int32(0))]]
            Projection: test.c > Int32(0), test.a, test.b, test.c
              TableScan: test
        "
        )
    }

    #[test]
    fn test_aggregate_filter_order_by() -> Result<()> {
        let table_scan = test_table_scan()?;

        // The `ORDER BY` expressions of an aggregate with a `FILTER` are handled
        // like its arguments, so `a + b` is not extracted.
        let filter = col("c").gt(lit(0));
        let order_by = vec![(col("a") + col("b")).sort(true, false)];
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(
                iter::empty::<Expr>(),
                vec![
                    sum(col("a"))
                        .order_by(order_by.clone())
                        .filter(filter.clone())
                        .build()?,
                    sum(col("b")).order_by(order_by).filter(filter).build()?,
                ],
            )?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @ "
        Aggregate: groupBy=[[]], aggr=[[sum(test.a) FILTER (WHERE test.c > Int32(0)) ORDER BY [test.a + test.b ASC NULLS LAST], sum(test.b) FILTER (WHERE test.c > Int32(0)) ORDER BY [test.a + test.b ASC NULLS LAST]]]
          Projection: test.c > Int32(0), test.a, test.b, test.c
            TableScan: test
        "
        )
    }

    #[test]
    fn test_window_filter_arguments() -> Result<()> {
        let table_scan = test_table_scan()?;

        // CSE conservatively treats the arguments of a window function with a
        // `FILTER` as conditionally evaluated, although the window executor
        // evaluates them before it applies the filter today. The `PARTITION BY`
        // expressions and the filter are evaluated for every input row.
        let window_sum = |arg: Expr| {
            Expr::from(WindowFunction::new(
                WindowFunctionDefinition::AggregateUDF(sum_udaf()),
                vec![arg],
            ))
            .partition_by(vec![col("a") * col("b")])
            .filter(col("c").gt(lit(0)))
            .build()
        };
        let plan = LogicalPlanBuilder::from(table_scan)
            .window(vec![
                window_sum(col("a") + col("b"))?,
                window_sum((col("a") + col("b")) * lit(2))?,
            ])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @ "
        Projection: test.a, test.b, test.c, sum(test.a + test.b) FILTER (WHERE test.c > Int32(0)) PARTITION BY [test.a * test.b] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING, sum(test.a + test.b * Int32(2)) FILTER (WHERE test.c > Int32(0)) PARTITION BY [test.a * test.b] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
          WindowAggr: windowExpr=[[sum(test.a + test.b) FILTER (WHERE test.c > Int32(0)) PARTITION BY [test.a * test.b] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING, sum((test.a + test.b) * Int32(2)) FILTER (WHERE test.c > Int32(0)) PARTITION BY [test.a * test.b] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING]]
            Projection: test.a * test.b, test.c > Int32(0), test.a, test.b, test.c
              TableScan: test
        "
        )
    }

    /// A projection rewritten by CSE carries the schema its new expressions
    /// derive, not the one derived before the rewrite.
    #[test]
    fn projection_schema_follows_rewritten_exprs() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
        let table_scan = table_scan(Some("t"), &schema, None)?.build()?;
        let a = || col("a") + lit(1);
        let expr = when(a().is_not_null(), a())
            .when(a().is_not_null(), a())
            .otherwise(lit(0))?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![expr])?
            .build()?;
        // Before the rewrite the guard proves the CASE non-null.
        assert!(!plan.schema().field(0).is_nullable());

        let optimized = CommonSubexprEliminate::new()
            .rewrite(plan, &OptimizerContext::new())?
            .data;
        let LogicalPlan::Projection(projection) = &optimized else {
            panic!("expected a projection, got {optimized}");
        };
        let derived = projection_schema(&projection.input, &projection.expr)?;
        assert_eq!(projection.schema.as_ref(), derived.as_ref());
        Ok(())
    }
}
