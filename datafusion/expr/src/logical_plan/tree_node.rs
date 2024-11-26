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

//!  [`TreeNode`] based visiting and rewriting for [`LogicalPlan`]s
//!
//! Visiting (read only) APIs
//! * [`LogicalPlan::visit`]: recursively visit the node and all of its inputs
//! * [`LogicalPlan::visit_with_subqueries`]: recursively visit the node and all of its inputs, including subqueries
//! * [`LogicalPlan::apply_children`]: recursively visit all inputs of this node
//! * [`LogicalPlan::apply_expressions`]: (non recursively) visit all expressions of this node
//! * [`LogicalPlan::apply_subqueries`]: (non recursively) visit all subqueries of this node
//! * [`LogicalPlan::apply_with_subqueries`]: recursively visit all inputs and embedded subqueries.
//!
//! Rewriting (update) APIs:
//! * [`LogicalPlan::exists`]: search for an expression in a plan
//! * [`LogicalPlan::rewrite`]: recursively rewrite the node and all of its inputs
//! * [`LogicalPlan::map_children`]: recursively rewrite all inputs of this node
//! * [`LogicalPlan::map_expressions`]: (non recursively) visit all expressions of this node
//! * [`LogicalPlan::map_subqueries`]: (non recursively) rewrite all subqueries of this node
//! * [`LogicalPlan::rewrite_with_subqueries`]: recursively rewrite the node and all of its inputs, including subqueries
//!
//! (Re)creation APIs (these require substantial cloning and thus are slow):
//! * [`LogicalPlan::with_new_exprs`]: Create a new plan with different expressions
//! * [`LogicalPlan::expressions`]: Return a copy of the plan's expressions

use crate::{
    dml::CopyTo, Aggregate, Analyze, CreateMemoryTable, CreateView, DdlStatement,
    Distinct, DistinctOn, DmlStatement, Execute, Explain, Expr, Extension, Filter, Join,
    Limit, LogicalPlan, Partitioning, Prepare, Projection, RecursiveQuery, Repartition,
    Sort, Statement, Subquery, SubqueryAlias, TableScan, Union, Unnest,
    UserDefinedLogicalNode, Values, Window,
};
use datafusion_common::tree_node::TreeNodeRefContainer;
use recursive::recursive;

use crate::expr::{Exists, InSubquery};
use datafusion_common::tree_node::{
    Transformed, TreeNode, TreeNodeContainer, TreeNodeIterator, TreeNodeRecursion,
    TreeNodeRewriter, TreeNodeVisitor,
};
use datafusion_common::{internal_err, Result};
use enumset::{enum_set, EnumSet, EnumSetType};

#[derive(EnumSetType, Debug)]
pub enum LogicalPlanPattern {
    // [`Expr`] nodes
    // ExprAlias,
    ExprColumn,
    // ExprScalarVariable,
    ExprLiteral,
    ExprBinaryExpr,
    ExprLike,
    // ExprSimilarTo,
    ExprNot,
    ExprIsNotNull,
    ExprIsNull,
    // ExprIsTrue,
    // ExprIsFalse,
    ExprIsUnknown,
    // ExprIsNotTrue,
    // ExprIsNotFalse,
    ExprIsNotUnknown,
    ExprNegative,
    // ExprGetIndexedField,
    ExprBetween,
    ExprCase,
    ExprCast,
    ExprTryCast,
    ExprScalarFunction,
    ExprAggregateFunction,
    ExprWindowFunction,
    ExprInList,
    ExprExists,
    ExprInSubquery,
    ExprScalarSubquery,
    // ExprWildcard,
    // ExprGroupingSet,
    ExprPlaceholder,
    // ExprOuterReferenceColumn,
    // ExprUnnest,

    // [`LogicalPlan`] nodes
    LogicalPlanProjection,
    LogicalPlanFilter,
    LogicalPlanWindow,
    LogicalPlanAggregate,
    LogicalPlanSort,
    LogicalPlanJoin,
    // LogicalPlanCrossJoin,
    LogicalPlanRepartition,
    LogicalPlanUnion,
    // LogicalPlanTableScan,
    LogicalPlanEmptyRelation,
    // LogicalPlanSubquery,
    LogicalPlanSubqueryAlias,
    LogicalPlanLimit,
    // LogicalPlanStatement,
    // LogicalPlanValues,
    // LogicalPlanExplain,
    // LogicalPlanAnalyze,
    // LogicalPlanExtension,
    LogicalPlanDistinct,
    // LogicalPlanDml,
    // LogicalPlanDdl,
    // LogicalPlanCopy,
    // LogicalPlanDescribeTable,
    // LogicalPlanUnnest,
    // LogicalPlanRecursiveQuery,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub struct LogicalPlanStats {
    patterns: EnumSet<LogicalPlanPattern>,
}

impl LogicalPlanStats {
    pub(crate) fn new(patterns: EnumSet<LogicalPlanPattern>) -> Self {
        Self { patterns }
    }

    pub(crate) fn empty() -> Self {
        Self {
            patterns: EnumSet::empty(),
        }
    }

    pub(crate) fn merge(mut self, other: LogicalPlanStats) -> Self {
        self.patterns.insert_all(other.patterns);
        self
    }

    pub fn contains_pattern(&self, pattern: LogicalPlanPattern) -> bool {
        self.patterns.contains(pattern)
    }

    pub fn contains_all_patterns(&self, patterns: EnumSet<LogicalPlanPattern>) -> bool {
        self.patterns.is_superset(patterns)
    }

    pub fn contains_any_patterns(&self, patterns: EnumSet<LogicalPlanPattern>) -> bool {
        !self.patterns.is_disjoint(patterns)
    }
}

impl TreeNode for LogicalPlan {
    fn apply_children<'n, F: FnMut(&'n Self) -> Result<TreeNodeRecursion>>(
        &'n self,
        f: F,
    ) -> Result<TreeNodeRecursion> {
        self.inputs().apply_ref_elements(f)
    }

    /// Applies `f` to each child (input) of this plan node, rewriting them *in place.*
    ///
    /// # Notes
    ///
    /// Inputs include ONLY direct children, not embedded `LogicalPlan`s for
    /// subqueries, for example such as are in [`Expr::Exists`].
    ///
    /// [`Expr::Exists`]: crate::Expr::Exists
    fn map_children<F: FnMut(Self) -> Result<Transformed<Self>>>(
        self,
        f: F,
    ) -> Result<Transformed<Self>> {
        Ok(match self {
            LogicalPlan::Projection(
                Projection {
                    expr,
                    input,
                    schema,
                },
                _,
            ) => input.map_elements(f)?.update_data(|input| {
                LogicalPlan::projection(Projection {
                    expr,
                    input,
                    schema,
                })
            }),
            LogicalPlan::Filter(
                Filter {
                    predicate,
                    input,
                    having,
                },
                _,
            ) => input.map_elements(f)?.update_data(|input| {
                LogicalPlan::filter(Filter {
                    predicate,
                    input,
                    having,
                })
            }),
            LogicalPlan::Repartition(
                Repartition {
                    input,
                    partitioning_scheme,
                },
                _,
            ) => input.map_elements(f)?.update_data(|input| {
                LogicalPlan::repartition(Repartition {
                    input,
                    partitioning_scheme,
                })
            }),
            LogicalPlan::Window(
                Window {
                    input,
                    window_expr,
                    schema,
                },
                _,
            ) => input.map_elements(f)?.update_data(|input| {
                LogicalPlan::window(Window {
                    input,
                    window_expr,
                    schema,
                })
            }),
            LogicalPlan::Aggregate(
                Aggregate {
                    input,
                    group_expr,
                    aggr_expr,
                    schema,
                },
                _,
            ) => input.map_elements(f)?.update_data(|input| {
                LogicalPlan::aggregate(Aggregate {
                    input,
                    group_expr,
                    aggr_expr,
                    schema,
                })
            }),
            LogicalPlan::Sort(Sort { expr, input, fetch }, _) => input
                .map_elements(f)?
                .update_data(|input| LogicalPlan::sort(Sort { expr, input, fetch })),
            LogicalPlan::Join(
                Join {
                    left,
                    right,
                    on,
                    filter,
                    join_type,
                    join_constraint,
                    schema,
                    null_equals_null,
                },
                _,
            ) => (left, right).map_elements(f)?.update_data(|(left, right)| {
                LogicalPlan::join(Join {
                    left,
                    right,
                    on,
                    filter,
                    join_type,
                    join_constraint,
                    schema,
                    null_equals_null,
                })
            }),
            LogicalPlan::Limit(Limit { skip, fetch, input }, _) => input
                .map_elements(f)?
                .update_data(|input| LogicalPlan::limit(Limit { skip, fetch, input })),
            LogicalPlan::Subquery(
                Subquery {
                    subquery,
                    outer_ref_columns,
                },
                _,
            ) => subquery.map_elements(f)?.update_data(|subquery| {
                LogicalPlan::subquery(Subquery {
                    subquery,
                    outer_ref_columns,
                })
            }),
            LogicalPlan::SubqueryAlias(
                SubqueryAlias {
                    input,
                    alias,
                    schema,
                },
                _,
            ) => input.map_elements(f)?.update_data(|input| {
                LogicalPlan::subquery_alias(SubqueryAlias {
                    input,
                    alias,
                    schema,
                })
            }),
            LogicalPlan::Extension(extension, _) => {
                rewrite_extension_inputs(extension, f)?
                    .update_data(LogicalPlan::extension)
            }
            LogicalPlan::Union(Union { inputs, schema }, _) => inputs
                .map_elements(f)?
                .update_data(|inputs| LogicalPlan::union(Union { inputs, schema })),
            LogicalPlan::Distinct(distinct, _) => match distinct {
                Distinct::All(input) => input.map_elements(f)?.update_data(Distinct::All),
                Distinct::On(DistinctOn {
                    on_expr,
                    select_expr,
                    sort_expr,
                    input,
                    schema,
                }) => input.map_elements(f)?.update_data(|input| {
                    Distinct::On(DistinctOn {
                        on_expr,
                        select_expr,
                        sort_expr,
                        input,
                        schema,
                    })
                }),
            }
            .update_data(LogicalPlan::distinct),
            LogicalPlan::Explain(
                Explain {
                    verbose,
                    plan,
                    stringified_plans,
                    schema,
                    logical_optimization_succeeded,
                },
                _,
            ) => plan.map_elements(f)?.update_data(|plan| {
                LogicalPlan::explain(Explain {
                    verbose,
                    plan,
                    stringified_plans,
                    schema,
                    logical_optimization_succeeded,
                })
            }),
            LogicalPlan::Analyze(
                Analyze {
                    verbose,
                    input,
                    schema,
                },
                _,
            ) => input.map_elements(f)?.update_data(|input| {
                LogicalPlan::analyze(Analyze {
                    verbose,
                    input,
                    schema,
                })
            }),
            LogicalPlan::Dml(
                DmlStatement {
                    table_name,
                    table_schema,
                    op,
                    input,
                    output_schema,
                },
                _,
            ) => input.map_elements(f)?.update_data(|input| {
                LogicalPlan::dml(DmlStatement {
                    table_name,
                    table_schema,
                    op,
                    input,
                    output_schema,
                })
            }),
            LogicalPlan::Copy(
                CopyTo {
                    input,
                    output_url,
                    partition_by,
                    file_type,
                    options,
                },
                _,
            ) => input.map_elements(f)?.update_data(|input| {
                LogicalPlan::copy(CopyTo {
                    input,
                    output_url,
                    partition_by,
                    file_type,
                    options,
                })
            }),
            LogicalPlan::Ddl(ddl, _) => {
                match ddl {
                    DdlStatement::CreateMemoryTable(CreateMemoryTable {
                        name,
                        constraints,
                        input,
                        if_not_exists,
                        or_replace,
                        column_defaults,
                        temporary,
                    }) => input.map_elements(f)?.update_data(|input| {
                        DdlStatement::CreateMemoryTable(CreateMemoryTable {
                            name,
                            constraints,
                            input,
                            if_not_exists,
                            or_replace,
                            column_defaults,
                            temporary,
                        })
                    }),
                    DdlStatement::CreateView(CreateView {
                        name,
                        input,
                        or_replace,
                        definition,
                        temporary,
                    }) => input.map_elements(f)?.update_data(|input| {
                        DdlStatement::CreateView(CreateView {
                            name,
                            input,
                            or_replace,
                            definition,
                            temporary,
                        })
                    }),
                    // no inputs in these statements
                    DdlStatement::CreateExternalTable(_)
                    | DdlStatement::CreateCatalogSchema(_)
                    | DdlStatement::CreateCatalog(_)
                    | DdlStatement::CreateIndex(_)
                    | DdlStatement::DropTable(_)
                    | DdlStatement::DropView(_)
                    | DdlStatement::DropCatalogSchema(_)
                    | DdlStatement::CreateFunction(_)
                    | DdlStatement::DropFunction(_) => Transformed::no(ddl),
                }
                .update_data(LogicalPlan::ddl)
            }
            LogicalPlan::Unnest(
                Unnest {
                    input,
                    exec_columns: input_columns,
                    list_type_columns,
                    struct_type_columns,
                    dependency_indices,
                    schema,
                    options,
                },
                _,
            ) => input.map_elements(f)?.update_data(|input| {
                LogicalPlan::unnest(Unnest {
                    input,
                    exec_columns: input_columns,
                    dependency_indices,
                    list_type_columns,
                    struct_type_columns,
                    schema,
                    options,
                })
            }),
            LogicalPlan::RecursiveQuery(
                RecursiveQuery {
                    name,
                    static_term,
                    recursive_term,
                    is_distinct,
                },
                _,
            ) => (static_term, recursive_term).map_elements(f)?.update_data(
                |(static_term, recursive_term)| {
                    LogicalPlan::recursive_query(RecursiveQuery {
                        name,
                        static_term,
                        recursive_term,
                        is_distinct,
                    })
                },
            ),
            LogicalPlan::Statement(stmt, _) => match stmt {
                Statement::Prepare(p) => p
                    .input
                    .map_elements(f)?
                    .update_data(|input| Statement::Prepare(Prepare { input, ..p })),
                _ => Transformed::no(stmt),
            }
            .update_data(LogicalPlan::statement),
            // plans without inputs
            LogicalPlan::TableScan { .. }
            | LogicalPlan::EmptyRelation { .. }
            | LogicalPlan::Values { .. }
            | LogicalPlan::DescribeTable(_, _) => Transformed::no(self),
        })
    }
}

/// Rewrites all inputs for an Extension node "in place"
/// (it currently has to copy values because there are no APIs for in place modification)
///
/// Should be removed when we have an API for in place modifications of the
/// extension to avoid these copies
fn rewrite_extension_inputs<F: FnMut(LogicalPlan) -> Result<Transformed<LogicalPlan>>>(
    extension: Extension,
    f: F,
) -> Result<Transformed<Extension>> {
    let Extension { node } = extension;

    node.inputs()
        .into_iter()
        .cloned()
        .map_until_stop_and_collect(f)?
        .map_data(|new_inputs| {
            let exprs = node.expressions();
            Ok(Extension {
                node: node.with_exprs_and_inputs(exprs, new_inputs)?,
            })
        })
}

/// This macro is used to determine continuation during combined transforming
/// traversals.
macro_rules! handle_transform_recursion {
    ($F_DOWN:expr, $F_CHILD:expr, $F_UP:expr) => {{
        $F_DOWN?
            .transform_children(|n| {
                n.map_subqueries($F_CHILD)?
                    .transform_sibling(|n| n.map_children($F_CHILD))
            })?
            .transform_parent($F_UP)
    }};
}

impl LogicalPlan {
    /// Calls `f` on all expressions in the current `LogicalPlan` node.
    ///
    /// # Notes
    /// * Similar to [`TreeNode::apply`] but for this node's expressions.
    /// * Does not include expressions in input `LogicalPlan` nodes
    /// * Visits only the top level expressions (Does not recurse into each expression)
    pub fn apply_expressions<F: FnMut(&Expr) -> Result<TreeNodeRecursion>>(
        &self,
        mut f: F,
    ) -> Result<TreeNodeRecursion> {
        match self {
            LogicalPlan::Projection(Projection { expr, .. }, _) => expr.apply_elements(f),
            LogicalPlan::Values(Values { values, .. }, _) => values.apply_elements(f),
            LogicalPlan::Filter(Filter { predicate, .. }, _) => f(predicate),
            LogicalPlan::Repartition(
                Repartition {
                    partitioning_scheme,
                    ..
                },
                _,
            ) => match partitioning_scheme {
                Partitioning::Hash(expr, _) | Partitioning::DistributeBy(expr) => {
                    expr.apply_elements(f)
                }
                Partitioning::RoundRobinBatch(_) => Ok(TreeNodeRecursion::Continue),
            },
            LogicalPlan::Window(Window { window_expr, .. }, _) => {
                window_expr.apply_elements(f)
            }
            LogicalPlan::Aggregate(
                Aggregate {
                    group_expr,
                    aggr_expr,
                    ..
                },
                _,
            ) => (group_expr, aggr_expr).apply_ref_elements(f),
            // There are two part of expression for join, equijoin(on) and non-equijoin(filter).
            // 1. the first part is `on.len()` equijoin expressions, and the struct of each expr is `left-on = right-on`.
            // 2. the second part is non-equijoin(filter).
            LogicalPlan::Join(Join { on, filter, .. }, _) => {
                (on, filter).apply_ref_elements(f)
            }
            LogicalPlan::Sort(Sort { expr, .. }, _) => expr.apply_elements(f),
            LogicalPlan::Extension(extension, _) => {
                // would be nice to avoid this copy -- maybe can
                // update extension to just observer Exprs
                extension.node.expressions().apply_elements(f)
            }
            LogicalPlan::TableScan(TableScan { filters, .. }, _) => {
                filters.apply_elements(f)
            }
            LogicalPlan::Unnest(unnest, _) => {
                let columns = unnest.exec_columns.clone();

                let exprs = columns
                    .iter()
                    .map(|c| Expr::column(c.clone()))
                    .collect::<Vec<_>>();
                exprs.apply_elements(f)
            }
            LogicalPlan::Distinct(
                Distinct::On(DistinctOn {
                    on_expr,
                    select_expr,
                    sort_expr,
                    ..
                }),
                _,
            ) => (on_expr, select_expr, sort_expr).apply_ref_elements(f),
            LogicalPlan::Limit(Limit { skip, fetch, .. }, _) => {
                (skip, fetch).apply_ref_elements(f)
            }
            LogicalPlan::Statement(stmt, _) => match stmt {
                Statement::Execute(Execute { parameters, .. }) => {
                    parameters.apply_elements(f)
                }
                _ => Ok(TreeNodeRecursion::Continue),
            },
            // plans without expressions
            LogicalPlan::EmptyRelation(_, _)
            | LogicalPlan::RecursiveQuery(_, _)
            | LogicalPlan::Subquery(_, _)
            | LogicalPlan::SubqueryAlias(_, _)
            | LogicalPlan::Analyze(_, _)
            | LogicalPlan::Explain(_, _)
            | LogicalPlan::Union(_, _)
            | LogicalPlan::Distinct(Distinct::All(_), _)
            | LogicalPlan::Dml(_, _)
            | LogicalPlan::Ddl(_, _)
            | LogicalPlan::Copy(_, _)
            | LogicalPlan::DescribeTable(_, _) => Ok(TreeNodeRecursion::Continue),
        }
    }

    /// Rewrites all expressions in the current `LogicalPlan` node using `f`.
    ///
    /// Returns the current node.
    ///
    /// # Notes
    /// * Similar to [`TreeNode::map_children`] but for this node's expressions.
    /// * Visits only the top level expressions (Does not recurse into each expression)
    pub fn map_expressions<F: FnMut(Expr) -> Result<Transformed<Expr>>>(
        self,
        mut f: F,
    ) -> Result<Transformed<Self>> {
        Ok(match self {
            LogicalPlan::Projection(
                Projection {
                    expr,
                    input,
                    schema,
                },
                _,
            ) => expr.map_elements(f)?.update_data(|expr| {
                LogicalPlan::projection(Projection {
                    expr,
                    input,
                    schema,
                })
            }),
            LogicalPlan::Values(Values { schema, values }, _) => values
                .map_elements(f)?
                .update_data(|values| LogicalPlan::values(Values { schema, values })),
            LogicalPlan::Filter(
                Filter {
                    predicate,
                    input,
                    having,
                },
                _,
            ) => f(predicate)?.update_data(|predicate| {
                LogicalPlan::filter(Filter {
                    predicate,
                    input,
                    having,
                })
            }),
            LogicalPlan::Repartition(
                Repartition {
                    input,
                    partitioning_scheme,
                },
                _,
            ) => match partitioning_scheme {
                Partitioning::Hash(expr, usize) => expr
                    .map_elements(f)?
                    .update_data(|expr| Partitioning::Hash(expr, usize)),
                Partitioning::DistributeBy(expr) => expr
                    .map_elements(f)?
                    .update_data(Partitioning::DistributeBy),
                Partitioning::RoundRobinBatch(_) => Transformed::no(partitioning_scheme),
            }
            .update_data(|partitioning_scheme| {
                LogicalPlan::repartition(Repartition {
                    input,
                    partitioning_scheme,
                })
            }),
            LogicalPlan::Window(
                Window {
                    input,
                    window_expr,
                    schema,
                },
                _,
            ) => window_expr.map_elements(f)?.update_data(|window_expr| {
                LogicalPlan::window(Window {
                    input,
                    window_expr,
                    schema,
                })
            }),
            LogicalPlan::Aggregate(
                Aggregate {
                    input,
                    group_expr,
                    aggr_expr,
                    schema,
                },
                _,
            ) => (group_expr, aggr_expr).map_elements(f)?.update_data(
                |(group_expr, aggr_expr)| {
                    LogicalPlan::aggregate(Aggregate {
                        input,
                        group_expr,
                        aggr_expr,
                        schema,
                    })
                },
            ),

            // There are two part of expression for join, equijoin(on) and non-equijoin(filter).
            // 1. the first part is `on.len()` equijoin expressions, and the struct of each expr is `left-on = right-on`.
            // 2. the second part is non-equijoin(filter).
            LogicalPlan::Join(
                Join {
                    left,
                    right,
                    on,
                    filter,
                    join_type,
                    join_constraint,
                    schema,
                    null_equals_null,
                },
                _,
            ) => (on, filter).map_elements(f)?.update_data(|(on, filter)| {
                LogicalPlan::join(Join {
                    left,
                    right,
                    on,
                    filter,
                    join_type,
                    join_constraint,
                    schema,
                    null_equals_null,
                })
            }),
            LogicalPlan::Sort(Sort { expr, input, fetch }, _) => expr
                .map_elements(f)?
                .update_data(|expr| LogicalPlan::sort(Sort { expr, input, fetch })),
            LogicalPlan::Extension(Extension { node }, _) => {
                // would be nice to avoid this copy -- maybe can
                // update extension to just observer Exprs
                let exprs = node.expressions().map_elements(f)?;
                let plan = LogicalPlan::extension(Extension {
                    node: UserDefinedLogicalNode::with_exprs_and_inputs(
                        node.as_ref(),
                        exprs.data,
                        node.inputs().into_iter().cloned().collect::<Vec<_>>(),
                    )?,
                });
                Transformed::new(plan, exprs.transformed, exprs.tnr)
            }
            LogicalPlan::TableScan(
                TableScan {
                    table_name,
                    source,
                    projection,
                    projected_schema,
                    filters,
                    fetch,
                },
                _,
            ) => filters.map_elements(f)?.update_data(|filters| {
                LogicalPlan::table_scan(TableScan {
                    table_name,
                    source,
                    projection,
                    projected_schema,
                    filters,
                    fetch,
                })
            }),
            LogicalPlan::Distinct(
                Distinct::On(DistinctOn {
                    on_expr,
                    select_expr,
                    sort_expr,
                    input,
                    schema,
                }),
                _,
            ) => (on_expr, select_expr, sort_expr)
                .map_elements(f)?
                .update_data(|(on_expr, select_expr, sort_expr)| {
                    LogicalPlan::distinct(Distinct::On(DistinctOn {
                        on_expr,
                        select_expr,
                        sort_expr,
                        input,
                        schema,
                    }))
                }),
            LogicalPlan::Limit(Limit { skip, fetch, input }, _) => {
                (skip, fetch).map_elements(f)?.update_data(|(skip, fetch)| {
                    LogicalPlan::limit(Limit { skip, fetch, input })
                })
            }
            LogicalPlan::Statement(stmt, _) => match stmt {
                Statement::Execute(e) => {
                    e.parameters.map_elements(f)?.update_data(|parameters| {
                        Statement::Execute(Execute { parameters, ..e })
                    })
                }
                _ => Transformed::no(stmt),
            }
            .update_data(LogicalPlan::statement),
            // plans without expressions
            LogicalPlan::EmptyRelation(_, _)
            | LogicalPlan::Unnest(_, _)
            | LogicalPlan::RecursiveQuery(_, _)
            | LogicalPlan::Subquery(_, _)
            | LogicalPlan::SubqueryAlias(_, _)
            | LogicalPlan::Analyze(_, _)
            | LogicalPlan::Explain(_, _)
            | LogicalPlan::Union(_, _)
            | LogicalPlan::Distinct(Distinct::All(_), _)
            | LogicalPlan::Ddl(_, _)
            | LogicalPlan::Dml(_, _)
            | LogicalPlan::Copy(_, _)
            | LogicalPlan::DescribeTable(_, _) => Transformed::no(self),
        })
    }

    /// Visits a plan similarly to [`Self::visit`], including subqueries that
    /// may appear in expressions such as `IN (SELECT ...)`.
    #[recursive]
    pub fn visit_with_subqueries<V: for<'n> TreeNodeVisitor<'n, Node = Self>>(
        &self,
        visitor: &mut V,
    ) -> Result<TreeNodeRecursion> {
        visitor
            .f_down(self)?
            .visit_children(|| {
                self.apply_subqueries(|c| c.visit_with_subqueries(visitor))?
                    .visit_sibling(|| {
                        self.apply_children(|c| c.visit_with_subqueries(visitor))
                    })
            })?
            .visit_parent(|| visitor.f_up(self))
    }

    /// Similarly to [`Self::rewrite`], rewrites this node and its inputs using `f`,
    /// including subqueries that may appear in expressions such as `IN (SELECT
    /// ...)`.
    #[recursive]
    pub fn rewrite_with_subqueries<R: TreeNodeRewriter<Node = Self>>(
        self,
        rewriter: &mut R,
    ) -> Result<Transformed<Self>> {
        handle_transform_recursion!(
            rewriter.f_down(self),
            |c| c.rewrite_with_subqueries(rewriter),
            |n| rewriter.f_up(n)
        )
    }

    /// Similarly to [`Self::apply`], calls `f` on this node and all its inputs,
    /// including subqueries that may appear in expressions such as `IN (SELECT
    /// ...)`.
    pub fn apply_with_subqueries<F: FnMut(&Self) -> Result<TreeNodeRecursion>>(
        &self,
        mut f: F,
    ) -> Result<TreeNodeRecursion> {
        #[recursive]
        fn apply_with_subqueries_impl<
            F: FnMut(&LogicalPlan) -> Result<TreeNodeRecursion>,
        >(
            node: &LogicalPlan,
            f: &mut F,
        ) -> Result<TreeNodeRecursion> {
            f(node)?.visit_children(|| {
                node.apply_subqueries(|c| apply_with_subqueries_impl(c, f))?
                    .visit_sibling(|| {
                        node.apply_children(|c| apply_with_subqueries_impl(c, f))
                    })
            })
        }

        apply_with_subqueries_impl(self, &mut f)
    }

    /// Similarly to [`Self::transform`], rewrites this node and its inputs using `f`,
    /// including subqueries that may appear in expressions such as `IN (SELECT
    /// ...)`.
    pub fn transform_with_subqueries<F: FnMut(Self) -> Result<Transformed<Self>>>(
        self,
        f: F,
    ) -> Result<Transformed<Self>> {
        self.transform_up_with_subqueries(f)
    }

    /// Similarly to [`Self::transform_down`], rewrites this node and its inputs using `f`,
    /// including subqueries that may appear in expressions such as `IN (SELECT
    /// ...)`.
    pub fn transform_down_with_subqueries<F: FnMut(Self) -> Result<Transformed<Self>>>(
        self,
        mut f: F,
    ) -> Result<Transformed<Self>> {
        #[recursive]
        fn transform_down_with_subqueries_impl<
            F: FnMut(LogicalPlan) -> Result<Transformed<LogicalPlan>>,
        >(
            node: LogicalPlan,
            f: &mut F,
        ) -> Result<Transformed<LogicalPlan>> {
            f(node)?.transform_children(|n| {
                n.map_subqueries(|c| transform_down_with_subqueries_impl(c, f))?
                    .transform_sibling(|n| {
                        n.map_children(|c| transform_down_with_subqueries_impl(c, f))
                    })
            })
        }

        transform_down_with_subqueries_impl(self, &mut f)
    }

    /// Similarly to [`Self::transform_up`], rewrites this node and its inputs using `f`,
    /// including subqueries that may appear in expressions such as `IN (SELECT
    /// ...)`.
    pub fn transform_up_with_subqueries<F: FnMut(Self) -> Result<Transformed<Self>>>(
        self,
        mut f: F,
    ) -> Result<Transformed<Self>> {
        #[recursive]
        fn transform_up_with_subqueries_impl<
            F: FnMut(LogicalPlan) -> Result<Transformed<LogicalPlan>>,
        >(
            node: LogicalPlan,
            f: &mut F,
        ) -> Result<Transformed<LogicalPlan>> {
            node.map_subqueries(|c| transform_up_with_subqueries_impl(c, f))?
                .transform_sibling(|n| {
                    n.map_children(|c| transform_up_with_subqueries_impl(c, f))
                })?
                .transform_parent(f)
        }

        transform_up_with_subqueries_impl(self, &mut f)
    }

    /// Similarly to [`Self::transform_down`], rewrites this node and its inputs using `f`,
    /// including subqueries that may appear in expressions such as `IN (SELECT
    /// ...)`.
    pub fn transform_down_up_with_subqueries<
        FD: FnMut(Self) -> Result<Transformed<Self>>,
        FU: FnMut(Self) -> Result<Transformed<Self>>,
    >(
        self,
        mut f_down: FD,
        mut f_up: FU,
    ) -> Result<Transformed<Self>> {
        #[recursive]
        fn transform_down_up_with_subqueries_impl<
            FD: FnMut(LogicalPlan) -> Result<Transformed<LogicalPlan>>,
            FU: FnMut(LogicalPlan) -> Result<Transformed<LogicalPlan>>,
        >(
            node: LogicalPlan,
            f_down: &mut FD,
            f_up: &mut FU,
        ) -> Result<Transformed<LogicalPlan>> {
            handle_transform_recursion!(
                f_down(node),
                |c| transform_down_up_with_subqueries_impl(c, f_down, f_up),
                f_up
            )
        }

        transform_down_up_with_subqueries_impl(self, &mut f_down, &mut f_up)
    }

    /// Similarly to [`Self::apply`], calls `f` on  this node and its inputs
    /// including subqueries that may appear in expressions such as `IN (SELECT
    /// ...)`.
    pub fn apply_subqueries<F: FnMut(&Self) -> Result<TreeNodeRecursion>>(
        &self,
        mut f: F,
    ) -> Result<TreeNodeRecursion> {
        self.apply_expressions(|expr| {
            expr.apply(|expr| {
                if !expr.stats().contains_any_patterns(enum_set!(
                    LogicalPlanPattern::ExprExists
                        | LogicalPlanPattern::ExprInSubquery
                        | LogicalPlanPattern::ExprScalarSubquery
                )) {
                    return Ok(TreeNodeRecursion::Jump);
                }

                match expr {
                    Expr::Exists(Exists { subquery, .. }, _)
                    | Expr::InSubquery(InSubquery { subquery, .. }, _)
                    | Expr::ScalarSubquery(subquery, _) => {
                        // use a synthetic plan so the collector sees a
                        // LogicalPlan::Subquery (even though it is
                        // actually a Subquery alias)
                        f(&LogicalPlan::subquery(subquery.clone()))
                    }
                    _ => Ok(TreeNodeRecursion::Continue),
                }
            })
        })
    }

    /// Similarly to [`Self::map_children`], rewrites all subqueries that may
    /// appear in expressions such as `IN (SELECT ...)` using `f`.
    ///
    /// Returns the current node.
    pub fn map_subqueries<F: FnMut(Self) -> Result<Transformed<Self>>>(
        self,
        mut f: F,
    ) -> Result<Transformed<Self>> {
        self.map_expressions(|expr| {
            expr.transform_down(|expr| {
                if !expr.stats().contains_any_patterns(enum_set!(
                    LogicalPlanPattern::ExprExists
                        | LogicalPlanPattern::ExprInSubquery
                        | LogicalPlanPattern::ExprScalarSubquery
                )) {
                    return Ok(Transformed::jump(expr));
                }

                match expr {
                    Expr::Exists(Exists { subquery, negated }, _) => {
                        f(LogicalPlan::subquery(subquery))?.map_data(|s| match s {
                            LogicalPlan::Subquery(subquery, _) => {
                                Ok(Expr::exists(Exists { subquery, negated }))
                            }
                            _ => internal_err!("Transformation should return Subquery"),
                        })
                    }
                    Expr::InSubquery(
                        InSubquery {
                            expr,
                            subquery,
                            negated,
                        },
                        _,
                    ) => f(LogicalPlan::subquery(subquery))?.map_data(|s| match s {
                        LogicalPlan::Subquery(subquery, _) => {
                            Ok(Expr::in_subquery(InSubquery {
                                expr,
                                subquery,
                                negated,
                            }))
                        }
                        _ => internal_err!("Transformation should return Subquery"),
                    }),
                    Expr::ScalarSubquery(subquery, _) => {
                        f(LogicalPlan::subquery(subquery))?.map_data(|s| match s {
                            LogicalPlan::Subquery(subquery, _) => {
                                Ok(Expr::scalar_subquery(subquery))
                            }
                            _ => internal_err!("Transformation should return Subquery"),
                        })
                    }
                    _ => Ok(Transformed::no(expr)),
                }
            })
        })
    }

    pub fn stats(&self) -> LogicalPlanStats {
        match self {
            LogicalPlan::Projection(_, stats) => *stats,
            LogicalPlan::Filter(_, stats) => *stats,
            LogicalPlan::Window(_, stats) => *stats,
            LogicalPlan::Aggregate(_, stats) => *stats,
            LogicalPlan::Sort(_, stats) => *stats,
            LogicalPlan::Join(_, stats) => *stats,
            LogicalPlan::Repartition(_, stats) => *stats,
            LogicalPlan::Union(_, stats) => *stats,
            LogicalPlan::TableScan(_, stats) => *stats,
            LogicalPlan::EmptyRelation(_, stats) => *stats,
            LogicalPlan::Subquery(_, stats) => *stats,
            LogicalPlan::SubqueryAlias(_, stats) => *stats,
            LogicalPlan::Limit(_, stats) => *stats,
            LogicalPlan::Statement(_, stats) => *stats,
            LogicalPlan::Values(_, stats) => *stats,
            LogicalPlan::Explain(_, stats) => *stats,
            LogicalPlan::Analyze(_, stats) => *stats,
            LogicalPlan::Extension(_, stats) => *stats,
            LogicalPlan::Distinct(_, stats) => *stats,
            LogicalPlan::Dml(_, stats) => *stats,
            LogicalPlan::Ddl(_, stats) => *stats,
            LogicalPlan::Copy(_, stats) => *stats,
            LogicalPlan::DescribeTable(_, stats) => *stats,
            LogicalPlan::Unnest(_, stats) => *stats,
            LogicalPlan::RecursiveQuery(_, stats) => *stats,
        }
    }
}
