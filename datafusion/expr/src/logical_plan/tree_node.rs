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

//! Tree node implementation for logical plan

use crate::{
    Aggregate, Analyze, CreateMemoryTable, CreateView, CrossJoin, DdlStatement, Distinct,
    DistinctOn, DmlStatement, Explain, Extension, Filter, Join, Limit, LogicalPlan,
    Prepare, Projection, RecursiveQuery, Repartition, Sort, Subquery, SubqueryAlias,
    Union, Unnest, Window,
};
use std::sync::Arc;

use crate::dml::CopyTo;
use datafusion_common::tree_node::{
    Transformed, TreeNode, TreeNodeIterator, TreeNodeRecursion,
};
use datafusion_common::{map_until_stop_and_collect, Result};

impl TreeNode for LogicalPlan {
    fn apply_children<F: FnMut(&Self) -> Result<TreeNodeRecursion>>(
        &self,
        f: F,
    ) -> Result<TreeNodeRecursion> {
        self.inputs().into_iter().apply_until_stop(f)
    }

    /// Applies `f` to each child (input) of this plan node, rewriting them *in place.*
    ///
    /// # Notes
    ///
    /// Inputs include ONLY direct children, not embedded `LogicalPlan`s for
    /// subqueries, for example such as are in [`Expr::Exists`].
    ///
    /// [`Expr::Exists`]: crate::Expr::Exists
    fn map_children<F>(self, mut f: F) -> Result<Transformed<Self>>
    where
        F: FnMut(Self) -> Result<Transformed<Self>>,
    {
        Ok(match self {
            LogicalPlan::Projection(Projection {
                expr,
                input,
                schema,
            }) => rewrite_arc(input, f)?.update_data(|input| {
                LogicalPlan::Projection(Projection {
                    expr,
                    input,
                    schema,
                })
            }),
            LogicalPlan::Filter(Filter { predicate, input }) => rewrite_arc(input, f)?
                .update_data(|input| LogicalPlan::Filter(Filter { predicate, input })),
            LogicalPlan::Repartition(Repartition {
                input,
                partitioning_scheme,
            }) => rewrite_arc(input, f)?.update_data(|input| {
                LogicalPlan::Repartition(Repartition {
                    input,
                    partitioning_scheme,
                })
            }),
            LogicalPlan::Window(Window {
                input,
                window_expr,
                schema,
            }) => rewrite_arc(input, f)?.update_data(|input| {
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
            }) => rewrite_arc(input, f)?.update_data(|input| {
                LogicalPlan::Aggregate(Aggregate {
                    input,
                    group_expr,
                    aggr_expr,
                    schema,
                })
            }),
            LogicalPlan::Sort(Sort { expr, input, fetch }) => rewrite_arc(input, f)?
                .update_data(|input| LogicalPlan::Sort(Sort { expr, input, fetch })),
            LogicalPlan::Join(Join {
                left,
                right,
                on,
                filter,
                join_type,
                join_constraint,
                schema,
                null_equals_null,
            }) => map_until_stop_and_collect!(
                rewrite_arc(left, &mut f),
                right,
                rewrite_arc(right, &mut f)
            )?
            .update_data(|(left, right)| {
                LogicalPlan::Join(Join {
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
            LogicalPlan::CrossJoin(CrossJoin {
                left,
                right,
                schema,
            }) => map_until_stop_and_collect!(
                rewrite_arc(left, &mut f),
                right,
                rewrite_arc(right, &mut f)
            )?
            .update_data(|(left, right)| {
                LogicalPlan::CrossJoin(CrossJoin {
                    left,
                    right,
                    schema,
                })
            }),
            LogicalPlan::Limit(Limit { skip, fetch, input }) => rewrite_arc(input, f)?
                .update_data(|input| LogicalPlan::Limit(Limit { skip, fetch, input })),
            LogicalPlan::Subquery(Subquery {
                subquery,
                outer_ref_columns,
            }) => rewrite_arc(subquery, f)?.update_data(|subquery| {
                LogicalPlan::Subquery(Subquery {
                    subquery,
                    outer_ref_columns,
                })
            }),
            LogicalPlan::SubqueryAlias(SubqueryAlias {
                input,
                alias,
                schema,
            }) => rewrite_arc(input, f)?.update_data(|input| {
                LogicalPlan::SubqueryAlias(SubqueryAlias {
                    input,
                    alias,
                    schema,
                })
            }),
            LogicalPlan::Extension(extension) => rewrite_extension_inputs(extension, f)?
                .update_data(LogicalPlan::Extension),
            LogicalPlan::Union(Union { inputs, schema }) => rewrite_arcs(inputs, f)?
                .update_data(|inputs| LogicalPlan::Union(Union { inputs, schema })),
            LogicalPlan::Distinct(distinct) => match distinct {
                Distinct::All(input) => rewrite_arc(input, f)?.update_data(Distinct::All),
                Distinct::On(DistinctOn {
                    on_expr,
                    select_expr,
                    sort_expr,
                    input,
                    schema,
                }) => rewrite_arc(input, f)?.update_data(|input| {
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
                plan,
                stringified_plans,
                schema,
                logical_optimization_succeeded,
            }) => rewrite_arc(plan, f)?.update_data(|plan| {
                LogicalPlan::Explain(Explain {
                    verbose,
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
            }) => rewrite_arc(input, f)?.update_data(|input| {
                LogicalPlan::Analyze(Analyze {
                    verbose,
                    input,
                    schema,
                })
            }),
            LogicalPlan::Dml(DmlStatement {
                table_name,
                table_schema,
                op,
                input,
            }) => rewrite_arc(input, f)?.update_data(|input| {
                LogicalPlan::Dml(DmlStatement {
                    table_name,
                    table_schema,
                    op,
                    input,
                })
            }),
            LogicalPlan::Copy(CopyTo {
                input,
                output_url,
                partition_by,
                format_options,
                options,
            }) => rewrite_arc(input, f)?.update_data(|input| {
                LogicalPlan::Copy(CopyTo {
                    input,
                    output_url,
                    partition_by,
                    format_options,
                    options,
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
                    }) => rewrite_arc(input, f)?.update_data(|input| {
                        DdlStatement::CreateMemoryTable(CreateMemoryTable {
                            name,
                            constraints,
                            input,
                            if_not_exists,
                            or_replace,
                            column_defaults,
                        })
                    }),
                    DdlStatement::CreateView(CreateView {
                        name,
                        input,
                        or_replace,
                        definition,
                    }) => rewrite_arc(input, f)?.update_data(|input| {
                        DdlStatement::CreateView(CreateView {
                            name,
                            input,
                            or_replace,
                            definition,
                        })
                    }),
                    // no inputs in these statements
                    DdlStatement::CreateExternalTable(_)
                    | DdlStatement::CreateCatalogSchema(_)
                    | DdlStatement::CreateCatalog(_)
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
                column,
                schema,
                options,
            }) => rewrite_arc(input, f)?.update_data(|input| {
                LogicalPlan::Unnest(Unnest {
                    input,
                    column,
                    schema,
                    options,
                })
            }),
            LogicalPlan::Prepare(Prepare {
                name,
                data_types,
                input,
            }) => rewrite_arc(input, f)?.update_data(|input| {
                LogicalPlan::Prepare(Prepare {
                    name,
                    data_types,
                    input,
                })
            }),
            LogicalPlan::RecursiveQuery(RecursiveQuery {
                name,
                static_term,
                recursive_term,
                is_distinct,
            }) => map_until_stop_and_collect!(
                rewrite_arc(static_term, &mut f),
                recursive_term,
                rewrite_arc(recursive_term, &mut f)
            )?
            .update_data(|(static_term, recursive_term)| {
                LogicalPlan::RecursiveQuery(RecursiveQuery {
                    name,
                    static_term,
                    recursive_term,
                    is_distinct,
                })
            }),
            // plans without inputs
            LogicalPlan::TableScan { .. }
            | LogicalPlan::Statement { .. }
            | LogicalPlan::EmptyRelation { .. }
            | LogicalPlan::Values { .. }
            | LogicalPlan::DescribeTable(_) => Transformed::no(self),
        })
    }
}

/// Converts a `Arc<LogicalPlan>` without copying, if possible. Copies the plan
/// if there is a shared reference
fn unwrap_arc(plan: Arc<LogicalPlan>) -> LogicalPlan {
    Arc::try_unwrap(plan)
        // if None is returned, there is another reference to this
        // LogicalPlan, so we can not own it, and must clone instead
        .unwrap_or_else(|node| node.as_ref().clone())
}

/// Applies `f` to rewrite a `Arc<LogicalPlan>` without copying, if possible
fn rewrite_arc<F>(
    plan: Arc<LogicalPlan>,
    mut f: F,
) -> Result<Transformed<Arc<LogicalPlan>>>
where
    F: FnMut(LogicalPlan) -> Result<Transformed<LogicalPlan>>,
{
    f(unwrap_arc(plan))?.map_data(|new_plan| Ok(Arc::new(new_plan)))
}

/// rewrite a `Vec` of `Arc<LogicalPlan>` without copying, if possible
fn rewrite_arcs<F>(
    input_plans: Vec<Arc<LogicalPlan>>,
    mut f: F,
) -> Result<Transformed<Vec<Arc<LogicalPlan>>>>
where
    F: FnMut(LogicalPlan) -> Result<Transformed<LogicalPlan>>,
{
    input_plans
        .into_iter()
        .map_until_stop_and_collect(|plan| rewrite_arc(plan, &mut f))
}

/// Rewrites all inputs for an Extension node "in place"
/// (it currently has to copy values because there are no APIs for in place modification)
///
/// Should be removed when we have an API for in place modifications of the
/// extension to avoid these copies
fn rewrite_extension_inputs<F>(
    extension: Extension,
    f: F,
) -> Result<Transformed<Extension>>
where
    F: FnMut(LogicalPlan) -> Result<Transformed<LogicalPlan>>,
{
    let Extension { node } = extension;

    node.inputs()
        .into_iter()
        .cloned()
        .map_until_stop_and_collect(f)?
        .map_data(|new_inputs| {
            let exprs = node.expressions();
            Ok(Extension {
                node: node.from_template(&exprs, &new_inputs),
            })
        })
}
