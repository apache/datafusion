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

use crate::expr::{Exists, InSubquery};
use datafusion_common::tree_node::{
    Transformed, TreeNode, TreeNodeContainer, TreeNodeIterator, TreeNodeRecursion,
    TreeNodeRewriter, TreeNodeVisitor,
};
use datafusion_common::{internal_err, Result};

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
            LogicalPlan::Projection(Projection {
                expr,
                input,
                schema,
            }) => input.map_elements(f)?.update_data(|input| {
                LogicalPlan::Projection(Projection {
                    expr,
                    input,
                    schema,
                })
            }),
            LogicalPlan::Filter(Filter { predicate, input }) => input
                .map_elements(f)?
                .update_data(|input| LogicalPlan::Filter(Filter { predicate, input })),
            LogicalPlan::Repartition(Repartition {
                input,
                partitioning_scheme,
            }) => input.map_elements(f)?.update_data(|input| {
                LogicalPlan::Repartition(Repartition {
                    input,
                    partitioning_scheme,
                })
            }),
            LogicalPlan::Window(Window {
                input,
                window_expr,
                schema,
            }) => input.map_elements(f)?.update_data(|input| {
                LogicalPlan::Window(Window {
                    input,
                    window_expr,
                    schema,
                })
            }),
            LogicalPlan::Aggregate(Aggregate {
                input,
                group_expr,
                aggr_expr,
                schema,
            }) => input.map_elements(f)?.update_data(|input| {
                LogicalPlan::Aggregate(Aggregate {
                    input,
                    group_expr,
                    aggr_expr,
                    schema,
                })
            }),
            LogicalPlan::Sort(Sort { expr, input, fetch }) => input
                .map_elements(f)?
                .update_data(|input| LogicalPlan::Sort(Sort { expr, input, fetch })),
            LogicalPlan::Join(Join {
                left,
                right,
                on,
                filter,
                join_type,
                join_constraint,
                schema,
                null_equality,
            }) => (left, right).map_elements(f)?.update_data(|(left, right)| {
                LogicalPlan::Join(Join {
                    left,
                    right,
                    on,
                    filter,
                    join_type,
                    join_constraint,
                    schema,
                    null_equality,
                })
            }),
            LogicalPlan::Limit(Limit { skip, fetch, input }) => input
                .map_elements(f)?
                .update_data(|input| LogicalPlan::Limit(Limit { skip, fetch, input })),
            LogicalPlan::Subquery(Subquery {
                subquery,
                outer_ref_columns,
                spans,
            }) => subquery.map_elements(f)?.update_data(|subquery| {
                LogicalPlan::Subquery(Subquery {
                    subquery,
                    outer_ref_columns,
                    spans,
                })
            }),
            LogicalPlan::SubqueryAlias(SubqueryAlias {
                input,
                alias,
                schema,
            }) => input.map_elements(f)?.update_data(|input| {
                LogicalPlan::SubqueryAlias(SubqueryAlias {
                    input,
                    alias,
                    schema,
                })
            }),
            LogicalPlan::Extension(extension) => rewrite_extension_inputs(extension, f)?
                .update_data(LogicalPlan::Extension),
            LogicalPlan::Union(Union { inputs, schema }) => inputs
                .map_elements(f)?
                .update_data(|inputs| LogicalPlan::Union(Union { inputs, schema })),
            LogicalPlan::Distinct(distinct) => match distinct {
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
            .update_data(LogicalPlan::Distinct),
            LogicalPlan::Explain(Explain {
                verbose,
                explain_format: format,
                plan,
                stringified_plans,
                schema,
                logical_optimization_succeeded,
            }) => plan.map_elements(f)?.update_data(|plan| {
                LogicalPlan::Explain(Explain {
                    verbose,
                    explain_format: format,
                    plan,
                    stringified_plans,
                    schema,
                    logical_optimization_succeeded,
                })
            }),
            LogicalPlan::Analyze(Analyze {
                verbose,
                input,
                schema,
            }) => input.map_elements(f)?.update_data(|input| {
                LogicalPlan::Analyze(Analyze {
                    verbose,
                    input,
                    schema,
                })
            }),
            LogicalPlan::Dml(DmlStatement {
                table_name,
                target,
                op,
                input,
                output_schema,
            }) => input.map_elements(f)?.update_data(|input| {
                LogicalPlan::Dml(DmlStatement {
                    table_name,
                    target,
                    op,
                    input,
                    output_schema,
                })
            }),
            LogicalPlan::Copy(CopyTo {
                input,
                output_url,
                partition_by,
                file_type,
                options,
                output_schema,
            }) => input.map_elements(f)?.update_data(|input| {
                LogicalPlan::Copy(CopyTo {
                    input,
                    output_url,
                    partition_by,
                    file_type,
                    options,
                    output_schema,
                })
            }),
            LogicalPlan::Ddl(ddl) => {
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
                .update_data(LogicalPlan::Ddl)
            }
            LogicalPlan::Unnest(Unnest {
                input,
                exec_columns: input_columns,
                list_type_columns,
                struct_type_columns,
                dependency_indices,
                schema,
                options,
            }) => input.map_elements(f)?.update_data(|input| {
                LogicalPlan::Unnest(Unnest {
                    input,
                    exec_columns: input_columns,
                    list_type_columns,
                    struct_type_columns,
                    dependency_indices,
                    schema,
                    options,
                })
            }),
            LogicalPlan::RecursiveQuery(RecursiveQuery {
                name,
                static_term,
                recursive_term,
                is_distinct,
            }) => (static_term, recursive_term).map_elements(f)?.update_data(
                |(static_term, recursive_term)| {
                    LogicalPlan::RecursiveQuery(RecursiveQuery {
                        name,
                        static_term,
                        recursive_term,
                        is_distinct,
                    })
                },
            ),
            LogicalPlan::Statement(stmt) => match stmt {
                Statement::Prepare(p) => p
                    .input
                    .map_elements(f)?
                    .update_data(|input| Statement::Prepare(Prepare { input, ..p })),
                _ => Transformed::no(stmt),
            }
            .update_data(LogicalPlan::Statement),
            // plans without inputs
            LogicalPlan::TableScan { .. }
            | LogicalPlan::EmptyRelation { .. }
            | LogicalPlan::Values { .. }
            | LogicalPlan::DescribeTable(_) => Transformed::no(self),
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
            LogicalPlan::Projection(Projection { expr, .. }) => expr.apply_elements(f),
            LogicalPlan::Values(Values { values, .. }) => values.apply_elements(f),
            LogicalPlan::Filter(Filter { predicate, .. }) => f(predicate),
            LogicalPlan::Repartition(Repartition {
                partitioning_scheme,
                ..
            }) => match partitioning_scheme {
                Partitioning::Hash(expr, _) | Partitioning::DistributeBy(expr) => {
                    expr.apply_elements(f)
                }
                Partitioning::RoundRobinBatch(_) => Ok(TreeNodeRecursion::Continue),
            },
            LogicalPlan::Window(Window { window_expr, .. }) => {
                window_expr.apply_elements(f)
            }
            LogicalPlan::Aggregate(Aggregate {
                group_expr,
                aggr_expr,
                ..
            }) => (group_expr, aggr_expr).apply_ref_elements(f),
            // There are two part of expression for join, equijoin(on) and non-equijoin(filter).
            // 1. the first part is `on.len()` equijoin expressions, and the struct of each expr is `left-on = right-on`.
            // 2. the second part is non-equijoin(filter).
            LogicalPlan::Join(Join { on, filter, .. }) => {
                (on, filter).apply_ref_elements(f)
            }
            LogicalPlan::Sort(Sort { expr, .. }) => expr.apply_elements(f),
            LogicalPlan::Extension(extension) => {
                // would be nice to avoid this copy -- maybe can
                // update extension to just observer Exprs
                extension.node.expressions().apply_elements(f)
            }
            LogicalPlan::TableScan(TableScan { filters, .. }) => {
                filters.apply_elements(f)
            }
            LogicalPlan::Unnest(unnest) => {
                let exprs = unnest
                    .exec_columns
                    .iter()
                    .cloned()
                    .map(Expr::Column)
                    .collect::<Vec<_>>();
                exprs.apply_elements(f)
            }
            LogicalPlan::Distinct(Distinct::On(DistinctOn {
                on_expr,
                select_expr,
                sort_expr,
                ..
            })) => (on_expr, select_expr, sort_expr).apply_ref_elements(f),
            LogicalPlan::Limit(Limit { skip, fetch, .. }) => {
                (skip, fetch).apply_ref_elements(f)
            }
            LogicalPlan::Statement(stmt) => match stmt {
                Statement::Execute(Execute { parameters, .. }) => {
                    parameters.apply_elements(f)
                }
                _ => Ok(TreeNodeRecursion::Continue),
            },
            // plans without expressions
            LogicalPlan::EmptyRelation(_)
            | LogicalPlan::RecursiveQuery(_)
            | LogicalPlan::Subquery(_)
            | LogicalPlan::SubqueryAlias(_)
            | LogicalPlan::Analyze(_)
            | LogicalPlan::Explain(_)
            | LogicalPlan::Union(_)
            | LogicalPlan::Distinct(Distinct::All(_))
            | LogicalPlan::Dml(_)
            | LogicalPlan::Ddl(_)
            | LogicalPlan::Copy(_)
            | LogicalPlan::DescribeTable(_) => Ok(TreeNodeRecursion::Continue),
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
            LogicalPlan::Projection(Projection {
                expr,
                input,
                schema,
            }) => expr.map_elements(f)?.update_data(|expr| {
                LogicalPlan::Projection(Projection {
                    expr,
                    input,
                    schema,
                })
            }),
            LogicalPlan::Values(Values { schema, values }) => values
                .map_elements(f)?
                .update_data(|values| LogicalPlan::Values(Values { schema, values })),
            LogicalPlan::Filter(Filter { predicate, input }) => f(predicate)?
                .update_data(|predicate| {
                    LogicalPlan::Filter(Filter { predicate, input })
                }),
            LogicalPlan::Repartition(Repartition {
                input,
                partitioning_scheme,
            }) => match partitioning_scheme {
                Partitioning::Hash(expr, usize) => expr
                    .map_elements(f)?
                    .update_data(|expr| Partitioning::Hash(expr, usize)),
                Partitioning::DistributeBy(expr) => expr
                    .map_elements(f)?
                    .update_data(Partitioning::DistributeBy),
                Partitioning::RoundRobinBatch(_) => Transformed::no(partitioning_scheme),
            }
            .update_data(|partitioning_scheme| {
                LogicalPlan::Repartition(Repartition {
                    input,
                    partitioning_scheme,
                })
            }),
            LogicalPlan::Window(Window {
                input,
                window_expr,
                schema,
            }) => window_expr.map_elements(f)?.update_data(|window_expr| {
                LogicalPlan::Window(Window {
                    input,
                    window_expr,
                    schema,
                })
            }),
            LogicalPlan::Aggregate(Aggregate {
                input,
                group_expr,
                aggr_expr,
                schema,
            }) => (group_expr, aggr_expr).map_elements(f)?.update_data(
                |(group_expr, aggr_expr)| {
                    LogicalPlan::Aggregate(Aggregate {
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
            LogicalPlan::Join(Join {
                left,
                right,
                on,
                filter,
                join_type,
                join_constraint,
                schema,
                null_equality,
            }) => (on, filter).map_elements(f)?.update_data(|(on, filter)| {
                LogicalPlan::Join(Join {
                    left,
                    right,
                    on,
                    filter,
                    join_type,
                    join_constraint,
                    schema,
                    null_equality,
                })
            }),
            LogicalPlan::Sort(Sort { expr, input, fetch }) => expr
                .map_elements(f)?
                .update_data(|expr| LogicalPlan::Sort(Sort { expr, input, fetch })),
            LogicalPlan::Extension(Extension { node }) => {
                // would be nice to avoid this copy -- maybe can
                // update extension to just observer Exprs
                let exprs = node.expressions().map_elements(f)?;
                let plan = LogicalPlan::Extension(Extension {
                    node: UserDefinedLogicalNode::with_exprs_and_inputs(
                        node.as_ref(),
                        exprs.data,
                        node.inputs().into_iter().cloned().collect::<Vec<_>>(),
                    )?,
                });
                Transformed::new(plan, exprs.transformed, exprs.tnr)
            }
            LogicalPlan::TableScan(TableScan {
                table_name,
                source,
                projection,
                projected_schema,
                filters,
                fetch,
            }) => filters.map_elements(f)?.update_data(|filters| {
                LogicalPlan::TableScan(TableScan {
                    table_name,
                    source,
                    projection,
                    projected_schema,
                    filters,
                    fetch,
                })
            }),
            LogicalPlan::Distinct(Distinct::On(DistinctOn {
                on_expr,
                select_expr,
                sort_expr,
                input,
                schema,
            })) => (on_expr, select_expr, sort_expr)
                .map_elements(f)?
                .update_data(|(on_expr, select_expr, sort_expr)| {
                    LogicalPlan::Distinct(Distinct::On(DistinctOn {
                        on_expr,
                        select_expr,
                        sort_expr,
                        input,
                        schema,
                    }))
                }),
            LogicalPlan::Limit(Limit { skip, fetch, input }) => {
                (skip, fetch).map_elements(f)?.update_data(|(skip, fetch)| {
                    LogicalPlan::Limit(Limit { skip, fetch, input })
                })
            }
            LogicalPlan::Statement(stmt) => match stmt {
                Statement::Execute(e) => {
                    e.parameters.map_elements(f)?.update_data(|parameters| {
                        Statement::Execute(Execute { parameters, ..e })
                    })
                }
                _ => Transformed::no(stmt),
            }
            .update_data(LogicalPlan::Statement),
            // plans without expressions
            LogicalPlan::EmptyRelation(_)
            | LogicalPlan::Unnest(_)
            | LogicalPlan::RecursiveQuery(_)
            | LogicalPlan::Subquery(_)
            | LogicalPlan::SubqueryAlias(_)
            | LogicalPlan::Analyze(_)
            | LogicalPlan::Explain(_)
            | LogicalPlan::Union(_)
            | LogicalPlan::Distinct(Distinct::All(_))
            | LogicalPlan::Dml(_)
            | LogicalPlan::Ddl(_)
            | LogicalPlan::Copy(_)
            | LogicalPlan::DescribeTable(_) => Transformed::no(self),
        })
    }

    /// Visits a plan similarly to [`Self::visit`], including subqueries that
    /// may appear in expressions such as `IN (SELECT ...)`.
    #[cfg_attr(feature = "recursive_protection", recursive::recursive)]
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
    #[cfg_attr(feature = "recursive_protection", recursive::recursive)]
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
        #[cfg_attr(feature = "recursive_protection", recursive::recursive)]
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
        #[cfg_attr(feature = "recursive_protection", recursive::recursive)]
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
        #[cfg_attr(feature = "recursive_protection", recursive::recursive)]
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
        #[cfg_attr(feature = "recursive_protection", recursive::recursive)]
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
            expr.apply(|expr| match expr {
                Expr::Exists(Exists { subquery, .. })
                | Expr::InSubquery(InSubquery { subquery, .. })
                | Expr::ScalarSubquery(subquery) => {
                    // use a synthetic plan so the collector sees a
                    // LogicalPlan::Subquery (even though it is
                    // actually a Subquery alias)
                    f(&LogicalPlan::Subquery(subquery.clone()))
                }
                _ => Ok(TreeNodeRecursion::Continue),
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
            expr.transform_down(|expr| match expr {
                Expr::Exists(Exists { subquery, negated }) => {
                    f(LogicalPlan::Subquery(subquery))?.map_data(|s| match s {
                        LogicalPlan::Subquery(subquery) => {
                            Ok(Expr::Exists(Exists { subquery, negated }))
                        }
                        _ => internal_err!("Transformation should return Subquery"),
                    })
                }
                Expr::InSubquery(InSubquery {
                    expr,
                    subquery,
                    negated,
                }) => f(LogicalPlan::Subquery(subquery))?.map_data(|s| match s {
                    LogicalPlan::Subquery(subquery) => Ok(Expr::InSubquery(InSubquery {
                        expr,
                        subquery,
                        negated,
                    })),
                    _ => internal_err!("Transformation should return Subquery"),
                }),
                Expr::ScalarSubquery(subquery) => f(LogicalPlan::Subquery(subquery))?
                    .map_data(|s| match s {
                        LogicalPlan::Subquery(subquery) => {
                            Ok(Expr::ScalarSubquery(subquery))
                        }
                        _ => internal_err!("Transformation should return Subquery"),
                    }),
                _ => Ok(Transformed::no(expr)),
            })
        })
    }
}
