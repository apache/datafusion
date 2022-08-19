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

//! Physical query planner

use super::analyze::AnalyzeExec;
use super::{
    aggregates, empty::EmptyExec, hash_join::PartitionMode, udaf, union::UnionExec,
    values::ValuesExec, windows,
};
use crate::config::{OPT_EXPLAIN_LOGICAL_PLAN_ONLY, OPT_EXPLAIN_PHYSICAL_PLAN_ONLY};
use crate::datasource::source_as_provider;
use crate::execution::context::{ExecutionProps, SessionState};
use crate::logical_expr::utils::generate_sort_key;
use crate::logical_plan::plan::{
    Aggregate, Distinct, EmptyRelation, Filter, Join, Projection, Sort, SubqueryAlias,
    TableScan, Window,
};
use crate::logical_plan::{
    unalias, unnormalize_cols, CrossJoin, DFSchema, Expr, LogicalPlan,
    Partitioning as LogicalPartitioning, PlanType, Repartition, ToStringifiedPlan, Union,
    UserDefinedLogicalNode,
};
use crate::logical_plan::{Limit, Values};
use crate::physical_expr::create_physical_expr;
use crate::physical_optimizer::optimizer::PhysicalOptimizerRule;
use crate::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
use crate::physical_plan::cross_join::CrossJoinExec;
use crate::physical_plan::explain::ExplainExec;
use crate::physical_plan::expressions::{Column, PhysicalSortExpr};
use crate::physical_plan::filter::FilterExec;
use crate::physical_plan::hash_join::HashJoinExec;
use crate::physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use crate::physical_plan::projection::ProjectionExec;
use crate::physical_plan::repartition::RepartitionExec;
use crate::physical_plan::sorts::sort::SortExec;
use crate::physical_plan::windows::WindowAggExec;
use crate::physical_plan::{join_utils, Partitioning};
use crate::physical_plan::{AggregateExpr, ExecutionPlan, PhysicalExpr, WindowExpr};
use crate::{
    error::{DataFusionError, Result},
    physical_plan::displayable,
};
use arrow::compute::SortOptions;
use arrow::datatypes::DataType;
use arrow::datatypes::{Schema, SchemaRef};
use async_trait::async_trait;
use datafusion_common::ScalarValue;
use datafusion_expr::expr::GroupingSet;
use datafusion_expr::utils::{expand_wildcard, expr_to_columns};
use datafusion_physical_expr::expressions::Literal;
use datafusion_sql::utils::window_expr_common_partition_keys;
use futures::future::BoxFuture;
use futures::{FutureExt, StreamExt, TryStreamExt};
use itertools::Itertools;
use log::{debug, trace};
use std::collections::{HashMap, HashSet};
use std::fmt::Write;
use std::sync::Arc;

fn create_function_physical_name(
    fun: &str,
    distinct: bool,
    args: &[Expr],
) -> Result<String> {
    let names: Vec<String> = args
        .iter()
        .map(|e| create_physical_name(e, false))
        .collect::<Result<_>>()?;

    let distinct_str = match distinct {
        true => "DISTINCT ",
        false => "",
    };
    Ok(format!("{}({}{})", fun, distinct_str, names.join(",")))
}

fn physical_name(e: &Expr) -> Result<String> {
    create_physical_name(e, true)
}

fn create_physical_name(e: &Expr, is_first_expr: bool) -> Result<String> {
    match e {
        Expr::Column(c) => {
            if is_first_expr {
                Ok(c.name.clone())
            } else {
                Ok(c.flat_name())
            }
        }
        Expr::Alias(_, name) => Ok(name.clone()),
        Expr::ScalarVariable(_, variable_names) => Ok(variable_names.join(".")),
        Expr::Literal(value) => Ok(format!("{:?}", value)),
        Expr::BinaryExpr { left, op, right } => {
            let left = create_physical_name(left, false)?;
            let right = create_physical_name(right, false)?;
            Ok(format!("{} {:?} {}", left, op, right))
        }
        Expr::Case {
            expr,
            when_then_expr,
            else_expr,
        } => {
            let mut name = "CASE ".to_string();
            if let Some(e) = expr {
                let _ = write!(name, "{:?} ", e);
            }
            for (w, t) in when_then_expr {
                let _ = write!(name, "WHEN {:?} THEN {:?} ", w, t);
            }
            if let Some(e) = else_expr {
                let _ = write!(name, "ELSE {:?} ", e);
            }
            name += "END";
            Ok(name)
        }
        Expr::Cast { expr, data_type } => {
            let expr = create_physical_name(expr, false)?;
            Ok(format!("CAST({} AS {:?})", expr, data_type))
        }
        Expr::TryCast { expr, data_type } => {
            let expr = create_physical_name(expr, false)?;
            Ok(format!("TRY_CAST({} AS {:?})", expr, data_type))
        }
        Expr::Not(expr) => {
            let expr = create_physical_name(expr, false)?;
            Ok(format!("NOT {}", expr))
        }
        Expr::Negative(expr) => {
            let expr = create_physical_name(expr, false)?;
            Ok(format!("(- {})", expr))
        }
        Expr::IsNull(expr) => {
            let expr = create_physical_name(expr, false)?;
            Ok(format!("{} IS NULL", expr))
        }
        Expr::IsNotNull(expr) => {
            let expr = create_physical_name(expr, false)?;
            Ok(format!("{} IS NOT NULL", expr))
        }
        Expr::GetIndexedField { expr, key } => {
            let expr = create_physical_name(expr, false)?;
            Ok(format!("{}[{}]", expr, key))
        }
        Expr::ScalarFunction { fun, args, .. } => {
            create_function_physical_name(&fun.to_string(), false, args)
        }
        Expr::ScalarUDF { fun, args, .. } => {
            create_function_physical_name(&fun.name, false, args)
        }
        Expr::WindowFunction { fun, args, .. } => {
            create_function_physical_name(&fun.to_string(), false, args)
        }
        Expr::AggregateFunction {
            fun,
            distinct,
            args,
            ..
        } => create_function_physical_name(&fun.to_string(), *distinct, args),
        Expr::AggregateUDF { fun, args } => {
            let mut names = Vec::with_capacity(args.len());
            for e in args {
                names.push(create_physical_name(e, false)?);
            }
            Ok(format!("{}({})", fun.name, names.join(",")))
        }
        Expr::GroupingSet(grouping_set) => match grouping_set {
            GroupingSet::Rollup(exprs) => Ok(format!(
                "ROLLUP ({})",
                exprs
                    .iter()
                    .map(|e| create_physical_name(e, false))
                    .collect::<Result<Vec<_>>>()?
                    .join(", ")
            )),
            GroupingSet::Cube(exprs) => Ok(format!(
                "CUBE ({})",
                exprs
                    .iter()
                    .map(|e| create_physical_name(e, false))
                    .collect::<Result<Vec<_>>>()?
                    .join(", ")
            )),
            GroupingSet::GroupingSets(lists_of_exprs) => {
                let mut strings = vec![];
                for exprs in lists_of_exprs {
                    let exprs_str = exprs
                        .iter()
                        .map(|e| create_physical_name(e, false))
                        .collect::<Result<Vec<_>>>()?
                        .join(", ");
                    strings.push(format!("({})", exprs_str));
                }
                Ok(format!("GROUPING SETS ({})", strings.join(", ")))
            }
        },

        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let expr = create_physical_name(expr, false)?;
            let list = list.iter().map(|expr| create_physical_name(expr, false));
            if *negated {
                Ok(format!("{} NOT IN ({:?})", expr, list))
            } else {
                Ok(format!("{} IN ({:?})", expr, list))
            }
        }
        Expr::Exists { .. } => Err(DataFusionError::NotImplemented(
            "EXISTS is not yet supported in the physical plan".to_string(),
        )),
        Expr::InSubquery { .. } => Err(DataFusionError::NotImplemented(
            "IN subquery is not yet supported in the physical plan".to_string(),
        )),
        Expr::ScalarSubquery(_) => Err(DataFusionError::NotImplemented(
            "Scalar subqueries are not yet supported in the physical plan".to_string(),
        )),
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => {
            let expr = create_physical_name(expr, false)?;
            let low = create_physical_name(low, false)?;
            let high = create_physical_name(high, false)?;
            if *negated {
                Ok(format!("{} NOT BETWEEN {} AND {}", expr, low, high))
            } else {
                Ok(format!("{} BETWEEN {} AND {}", expr, low, high))
            }
        }
        Expr::Sort { .. } => Err(DataFusionError::Internal(
            "Create physical name does not support sort expression".to_string(),
        )),
        Expr::Wildcard => Err(DataFusionError::Internal(
            "Create physical name does not support wildcard".to_string(),
        )),
        Expr::QualifiedWildcard { .. } => Err(DataFusionError::Internal(
            "Create physical name does not support qualified wildcard".to_string(),
        )),
    }
}

/// Physical query planner that converts a `LogicalPlan` to an
/// `ExecutionPlan` suitable for execution.
#[async_trait]
pub trait PhysicalPlanner: Send + Sync {
    /// Create a physical plan from a logical plan
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>>;

    /// Create a physical expression from a logical expression
    /// suitable for evaluation
    ///
    /// `expr`: the expression to convert
    ///
    /// `input_dfschema`: the logical plan schema for evaluating `expr`
    ///
    /// `input_schema`: the physical schema for evaluating `expr`
    fn create_physical_expr(
        &self,
        expr: &Expr,
        input_dfschema: &DFSchema,
        input_schema: &Schema,
        session_state: &SessionState,
    ) -> Result<Arc<dyn PhysicalExpr>>;
}

/// This trait exposes the ability to plan an [`ExecutionPlan`] out of a [`LogicalPlan`].
#[async_trait]
pub trait ExtensionPlanner {
    /// Create a physical plan for a [`UserDefinedLogicalNode`].
    ///
    /// `input_dfschema`: the logical plan schema for the inputs to this node
    ///
    /// Returns an error when the planner knows how to plan the concrete
    /// implementation of `node` but errors while doing so.
    ///
    /// Returns `None` when the planner does not know how to plan the
    /// `node` and wants to delegate the planning to another
    /// [`ExtensionPlanner`].
    async fn plan_extension(
        &self,
        planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>>;
}

/// Default single node physical query planner that converts a
/// `LogicalPlan` to an `ExecutionPlan` suitable for execution.
#[derive(Default)]
pub struct DefaultPhysicalPlanner {
    extension_planners: Vec<Arc<dyn ExtensionPlanner + Send + Sync>>,
}

#[async_trait]
impl PhysicalPlanner for DefaultPhysicalPlanner {
    /// Create a physical plan from a logical plan
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match self.handle_explain(logical_plan, session_state).await? {
            Some(plan) => Ok(plan),
            None => {
                let plan = self
                    .create_initial_plan(logical_plan, session_state)
                    .await?;
                self.optimize_internal(plan, session_state, |_, _| {})
            }
        }
    }

    /// Create a physical expression from a logical expression
    /// suitable for evaluation
    ///
    /// `e`: the expression to convert
    ///
    /// `input_dfschema`: the logical plan schema for evaluating `e`
    ///
    /// `input_schema`: the physical schema for evaluating `e`
    fn create_physical_expr(
        &self,
        expr: &Expr,
        input_dfschema: &DFSchema,
        input_schema: &Schema,
        session_state: &SessionState,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        create_physical_expr(
            expr,
            input_dfschema,
            input_schema,
            &session_state.execution_props,
        )
    }
}

impl DefaultPhysicalPlanner {
    /// Create a physical planner that uses `extension_planners` to
    /// plan user-defined logical nodes [`LogicalPlan::Extension`].
    /// The planner uses the first [`ExtensionPlanner`] to return a non-`None`
    /// plan.
    pub fn with_extension_planners(
        extension_planners: Vec<Arc<dyn ExtensionPlanner + Send + Sync>>,
    ) -> Self {
        Self { extension_planners }
    }

    /// Create a physical plan from a logical plan
    fn create_initial_plan<'a>(
        &'a self,
        logical_plan: &'a LogicalPlan,
        session_state: &'a SessionState,
    ) -> BoxFuture<'a, Result<Arc<dyn ExecutionPlan>>> {
        async move {
            let exec_plan: Result<Arc<dyn ExecutionPlan>> = match logical_plan {
                LogicalPlan::TableScan (TableScan {
                    source,
                    projection,
                    filters,
                    fetch,
                    ..
                }) => {
                    let source = source_as_provider(source)?;
                    // Remove all qualifiers from the scan as the provider
                    // doesn't know (nor should care) how the relation was
                    // referred to in the query
                    let filters = unnormalize_cols(filters.iter().cloned());
                    let unaliased: Vec<Expr> = filters.into_iter().map(unalias).collect();
                    source.scan(session_state, projection, &unaliased, *fetch).await
                }
                LogicalPlan::Values(Values {
                    values,
                    schema,
                }) => {
                    let exec_schema = schema.as_ref().to_owned().into();
                    let exprs = values.iter()
                        .map(|row| {
                            row.iter().map(|expr|{
                                self.create_physical_expr(
                                    expr,
                                    schema,
                                    &exec_schema,
                                    session_state,
                                )
                            })
                            .collect::<Result<Vec<Arc<dyn PhysicalExpr>>>>()
                        })
                        .collect::<Result<Vec<_>>>()?;
                    let value_exec = ValuesExec::try_new(
                        SchemaRef::new(exec_schema),
                        exprs
                    )?;
                    Ok(Arc::new(value_exec))
                }
                LogicalPlan::Window(Window {
                    input, window_expr, ..
                }) => {
                    if window_expr.is_empty() {
                        return Err(DataFusionError::Internal(
                            "Impossibly got empty window expression".to_owned(),
                        ));
                    }

                    let input_exec = self.create_initial_plan(input, session_state).await?;

                    // at this moment we are guaranteed by the logical planner
                    // to have all the window_expr to have equal sort key
                    let partition_keys = window_expr_common_partition_keys(window_expr)?;

                    let can_repartition = !partition_keys.is_empty()
                        && session_state.config.target_partitions > 1
                        && session_state.config.repartition_windows;

                    let input_exec = if can_repartition {
                        let partition_keys = partition_keys
                            .iter()
                            .map(|e| {
                                self.create_physical_expr(
                                    e,
                                    input.schema(),
                                    &input_exec.schema(),
                                    session_state,
                                )
                            })
                            .collect::<Result<Vec<Arc<dyn PhysicalExpr>>>>()?;
                        Arc::new(RepartitionExec::try_new(
                            input_exec,
                            Partitioning::Hash(
                                partition_keys,
                                session_state.config.target_partitions,
                            ),
                        )?)
                    } else {
                        input_exec
                    };

                    // add a sort phase
                    let get_sort_keys = |expr: &Expr| match expr {
                        Expr::WindowFunction {
                            ref partition_by,
                            ref order_by,
                            ..
                        } => generate_sort_key(partition_by, order_by),
                        _ => unreachable!(),
                    };
                    let sort_keys = get_sort_keys(&window_expr[0]);
                    if window_expr.len() > 1 {
                        debug_assert!(
                            window_expr[1..]
                                .iter()
                                .all(|expr| get_sort_keys(expr) == sort_keys),
                            "all window expressions shall have the same sort keys, as guaranteed by logical planning"
                        );
                    }

                    let logical_input_schema = input.schema();

                    let input_exec = if sort_keys.is_empty() {
                        input_exec
                    } else {
                        let physical_input_schema = input_exec.schema();
                        let sort_keys = sort_keys
                            .iter()
                            .map(|e| match e {
                                Expr::Sort {
                                    expr,
                                    asc,
                                    nulls_first,
                                } => create_physical_sort_expr(
                                    expr,
                                    logical_input_schema,
                                    &physical_input_schema,
                                    SortOptions {
                                        descending: !*asc,
                                        nulls_first: *nulls_first,
                                    },
                                    &session_state.execution_props,
                                ),
                                _ => unreachable!(),
                            })
                            .collect::<Result<Vec<_>>>()?;
                        Arc::new(if can_repartition {
                            SortExec::new_with_partitioning(sort_keys, input_exec, true)
                        } else {
                            SortExec::try_new(sort_keys, input_exec)?
                        })
                    };

                    let physical_input_schema = input_exec.schema();
                    let window_expr = window_expr
                        .iter()
                        .map(|e| {
                            create_window_expr(
                                e,
                                logical_input_schema,
                                &physical_input_schema,
                                &session_state.execution_props,
                            )
                        })
                        .collect::<Result<Vec<_>>>()?;

                    Ok(Arc::new(WindowAggExec::try_new(
                        window_expr,
                        input_exec,
                        physical_input_schema,
                    )?) )
                }
                LogicalPlan::Aggregate(Aggregate {
                    input,
                    group_expr,
                    aggr_expr,
                    ..
                }) => {
                    // Initially need to perform the aggregate and then merge the partitions
                    let input_exec = self.create_initial_plan(input, session_state).await?;
                    let physical_input_schema = input_exec.schema();
                    let logical_input_schema = input.as_ref().schema();

                    let groups = self.create_grouping_physical_expr(
                        group_expr,
                        logical_input_schema,
                        &physical_input_schema,
                        session_state)?;

                    let aggregates = aggr_expr
                        .iter()
                        .map(|e| {
                            create_aggregate_expr(
                                e,
                                logical_input_schema,
                                &physical_input_schema,
                                &session_state.execution_props,
                            )
                        })
                        .collect::<Result<Vec<_>>>()?;

                    let initial_aggr = Arc::new(AggregateExec::try_new(
                        AggregateMode::Partial,
                        groups.clone(),
                        aggregates.clone(),
                        input_exec,
                        physical_input_schema.clone(),
                    )?);

                    // update group column indices based on partial aggregate plan evaluation
                    let final_group: Vec<Arc<dyn PhysicalExpr>> = initial_aggr.output_group_expr();

                    // TODO: dictionary type not yet supported in Hash Repartition
                    let contains_dict = groups
                        .expr()
                        .iter()
                        .flat_map(|x| x.0.data_type(physical_input_schema.as_ref()))
                        .any(|x| matches!(x, DataType::Dictionary(_, _)));

                    let can_repartition = !groups.is_empty()
                        && session_state.config.target_partitions > 1
                        && session_state.config.repartition_aggregations
                        && !contains_dict;

                    let (initial_aggr, next_partition_mode): (
                        Arc<dyn ExecutionPlan>,
                        AggregateMode,
                    ) = if can_repartition {
                        // Divide partial hash aggregates into multiple partitions by hash key
                        let hash_repartition = Arc::new(RepartitionExec::try_new(
                            initial_aggr,
                            Partitioning::Hash(
                                final_group.clone(),
                                session_state.config.target_partitions,
                            ),
                        )?);
                        // Combine hash aggregates within the partition
                        (hash_repartition, AggregateMode::FinalPartitioned)
                    } else {
                        // construct a second aggregation, keeping the final column name equal to the
                        // first aggregation and the expressions corresponding to the respective aggregate
                        (initial_aggr, AggregateMode::Final)
                    };

                    let final_grouping_set = PhysicalGroupBy::new_single(
                        final_group
                            .iter()
                            .enumerate()
                            .map(|(i, expr)| (expr.clone(), groups.expr()[i].1.clone()))
                            .collect()
                    );

                    Ok(Arc::new(AggregateExec::try_new(
                        next_partition_mode,
                        final_grouping_set,
                        aggregates,
                        initial_aggr,
                        physical_input_schema.clone(),
                    )?) )
                }
                LogicalPlan::Distinct(Distinct {input}) => {
                    // Convert distinct to groupby with no aggregations
                    let group_expr = expand_wildcard(input.schema(), input)?;
                    let aggregate =  LogicalPlan::Aggregate(Aggregate {
                            input: input.clone(),
                            group_expr,
                            aggr_expr: vec![],
                            schema: input.schema().clone()
                        }
                    );
                    Ok(self.create_initial_plan(&aggregate, session_state).await?)
                }
                LogicalPlan::Projection(Projection { input, expr, .. }) => {
                    let input_exec = self.create_initial_plan(input, session_state).await?;
                    let input_schema = input.as_ref().schema();

                    let physical_exprs = expr
                        .iter()
                        .map(|e| {
                            // For projections, SQL planner and logical plan builder may convert user
                            // provided expressions into logical Column expressions if their results
                            // are already provided from the input plans. Because we work with
                            // qualified columns in logical plane, derived columns involve operators or
                            // functions will contain qualifers as well. This will result in logical
                            // columns with names like `SUM(t1.c1)`, `t1.c1 + t1.c2`, etc.
                            //
                            // If we run these logical columns through physical_name function, we will
                            // get physical names with column qualifiers, which violates DataFusion's
                            // field name semantics. To account for this, we need to derive the
                            // physical name from physical input instead.
                            //
                            // This depends on the invariant that logical schema field index MUST match
                            // with physical schema field index.
                            let physical_name = if let Expr::Column(col) = e {
                                match input_schema.index_of_column(col) {
                                    Ok(idx) => {
                                        // index physical field using logical field index
                                        Ok(input_exec.schema().field(idx).name().to_string())
                                    }
                                    // logical column is not a derived column, safe to pass along to
                                    // physical_name
                                    Err(_) => physical_name(e),
                                }
                            } else {
                                physical_name(e)
                            };

                            tuple_err((
                                self.create_physical_expr(
                                    e,
                                    input_schema,
                                    &input_exec.schema(),
                                    session_state,
                                ),
                                physical_name,
                            ))
                        })
                        .collect::<Result<Vec<_>>>()?;

                    Ok(Arc::new(ProjectionExec::try_new(
                        physical_exprs,
                        input_exec,
                    )?) )
                }
                LogicalPlan::Filter(Filter {
                    input, predicate, ..
                }) => {
                    let physical_input = self.create_initial_plan(input, session_state).await?;
                    let input_schema = physical_input.as_ref().schema();
                    let input_dfschema = input.as_ref().schema();

                    let runtime_expr = self.create_physical_expr(
                        predicate,
                        input_dfschema,
                        &input_schema,
                        session_state,
                    )?;
                    Ok(Arc::new(FilterExec::try_new(runtime_expr, physical_input)?) )
                }
                LogicalPlan::Union(Union { inputs, .. }) => {
                    let physical_plans = futures::stream::iter(inputs)
                        .then(|lp| self.create_initial_plan(lp, session_state))
                        .try_collect::<Vec<_>>()
                        .await?;
                    Ok(Arc::new(UnionExec::new(physical_plans)) )
                }
                LogicalPlan::Repartition(Repartition {
                    input,
                    partitioning_scheme,
                }) => {
                    let physical_input = self.create_initial_plan(input, session_state).await?;
                    let input_schema = physical_input.schema();
                    let input_dfschema = input.as_ref().schema();
                    let physical_partitioning = match partitioning_scheme {
                        LogicalPartitioning::RoundRobinBatch(n) => {
                            Partitioning::RoundRobinBatch(*n)
                        }
                        LogicalPartitioning::Hash(expr, n) => {
                            let runtime_expr = expr
                                .iter()
                                .map(|e| {
                                    self.create_physical_expr(
                                        e,
                                        input_dfschema,
                                        &input_schema,
                                        session_state,
                                    )
                                })
                                .collect::<Result<Vec<_>>>()?;
                            Partitioning::Hash(runtime_expr, *n)
                        }
                        LogicalPartitioning::DistributeBy(_) => {
                            return Err(DataFusionError::NotImplemented("Physical plan does not support DistributeBy partitioning".to_string()))
                        }
                    };
                    Ok(Arc::new(RepartitionExec::try_new(
                        physical_input,
                        physical_partitioning,
                    )?) )
                }
                LogicalPlan::Sort(Sort { expr, input, .. }) => {
                    let physical_input = self.create_initial_plan(input, session_state).await?;
                    let input_schema = physical_input.as_ref().schema();
                    let input_dfschema = input.as_ref().schema();
                    let sort_expr = expr
                        .iter()
                        .map(|e| match e {
                            Expr::Sort {
                                expr,
                                asc,
                                nulls_first,
                            } => create_physical_sort_expr(
                                expr,
                                input_dfschema,
                                &input_schema,
                                SortOptions {
                                    descending: !*asc,
                                    nulls_first: *nulls_first,
                                },
                                &session_state.execution_props,
                            ),
                            _ => Err(DataFusionError::Plan(
                                "Sort only accepts sort expressions".to_string(),
                            )),
                        })
                        .collect::<Result<Vec<_>>>()?;
                    Ok(Arc::new(SortExec::try_new(sort_expr, physical_input)?) )
                }
                LogicalPlan::Join(Join {
                    left,
                    right,
                    on: keys,
                    filter,
                    join_type,
                    null_equals_null,
                    ..
                }) => {
                    let left_df_schema = left.schema();
                    let physical_left = self.create_initial_plan(left, session_state).await?;
                    let right_df_schema = right.schema();
                    let physical_right = self.create_initial_plan(right, session_state).await?;
                    let join_on = keys
                        .iter()
                        .map(|(l, r)| {
                            Ok((
                                Column::new(&l.name, left_df_schema.index_of_column(l)?),
                                Column::new(&r.name, right_df_schema.index_of_column(r)?),
                            ))
                        })
                        .collect::<Result<join_utils::JoinOn>>()?;

                    let join_filter = match filter {
                        Some(expr) => {
                            // Extract columns from filter expression
                            let mut cols = HashSet::new();
                            expr_to_columns(expr, &mut cols)?;

                            // Collect left & right field indices
                            let left_field_indices = cols.iter()
                                .filter_map(|c| match left_df_schema.index_of_column(c) {
                                    Ok(idx) => Some(idx),
                                    _ => None,
                                })
                                .collect::<Vec<_>>();
                            let right_field_indices = cols.iter()
                                .filter_map(|c| match right_df_schema.index_of_column(c) {
                                    Ok(idx) => Some(idx),
                                    _ => None,
                                })
                                .collect::<Vec<_>>();

                            // Collect DFFields and Fields required for intermediate schemas
                            let (filter_df_fields, filter_fields): (Vec<_>, Vec<_>) = left_field_indices.clone()
                                .into_iter()
                                .map(|i| (
                                    left_df_schema.field(i).clone(),
                                    physical_left.schema().field(i).clone(),
                                ))
                                .chain(
                                    right_field_indices.clone()
                                        .into_iter()
                                        .map(|i| (
                                            right_df_schema.field(i).clone(),
                                            physical_right.schema().field(i).clone(),
                                        ))
                                )
                                .unzip();


                            // Construct intermediate schemas used for filtering data and
                            // convert logical expression to physical according to filter schema
                            let filter_df_schema = DFSchema::new_with_metadata(filter_df_fields, HashMap::new())?;
                            let filter_schema = Schema::new_with_metadata(filter_fields, HashMap::new());
                            let filter_expr = create_physical_expr(
                                expr,
                                &filter_df_schema,
                                &filter_schema,
                                &session_state.execution_props
                            )?;
                            let column_indices = join_utils::JoinFilter::build_column_indices(left_field_indices, right_field_indices);

                            Some(join_utils::JoinFilter::new(
                                filter_expr,
                                column_indices,
                                filter_schema
                            ))
                        }
                        _ => None
                    };

                    if session_state.config.target_partitions > 1
                        && session_state.config.repartition_joins
                    {
                        let (left_expr, right_expr) = join_on
                            .iter()
                            .map(|(l, r)| {
                                (
                                    Arc::new(l.clone()) as Arc<dyn PhysicalExpr>,
                                    Arc::new(r.clone()) as Arc<dyn PhysicalExpr>,
                                )
                            })
                            .unzip();

                        // Use hash partition by default to parallelize hash joins
                        Ok(Arc::new(HashJoinExec::try_new(
                            Arc::new(RepartitionExec::try_new(
                                physical_left,
                                Partitioning::Hash(
                                    left_expr,
                                    session_state.config.target_partitions,
                                ),
                            )?),
                            Arc::new(RepartitionExec::try_new(
                                physical_right,
                                Partitioning::Hash(
                                    right_expr,
                                    session_state.config.target_partitions,
                                ),
                            )?),
                            join_on,
                            join_filter,
                            join_type,
                            PartitionMode::Partitioned,
                            null_equals_null,
                        )?))
                    } else {
                        Ok(Arc::new(HashJoinExec::try_new(
                            physical_left,
                            physical_right,
                            join_on,
                            join_filter,
                            join_type,
                            PartitionMode::CollectLeft,
                            null_equals_null,
                        )?))
                    }
                }
                LogicalPlan::CrossJoin(CrossJoin { left, right, .. }) => {
                    let left = self.create_initial_plan(left, session_state).await?;
                    let right = self.create_initial_plan(right, session_state).await?;
                    Ok(Arc::new(CrossJoinExec::try_new(left, right)?))
                }
                LogicalPlan::Subquery(_) => todo!(),
                LogicalPlan::EmptyRelation(EmptyRelation {
                    produce_one_row,
                    schema,
                }) => Ok(Arc::new(EmptyExec::new(
                    *produce_one_row,
                    SchemaRef::new(schema.as_ref().to_owned().into()),
                ))),
                LogicalPlan::SubqueryAlias(SubqueryAlias { input,.. }) => {
                    match input.as_ref() {
                        LogicalPlan::TableScan(..) => {
                            self.create_initial_plan(input, session_state).await
                        }
                        _ => Err(DataFusionError::Plan("SubqueryAlias should only wrap TableScan".to_string()))
                    }
                }
                LogicalPlan::Limit(Limit { input, skip, fetch,.. }) => {
                    let input = self.create_initial_plan(input, session_state).await?;

                    // GlobalLimitExec requires a single partition for input
                    let input = if input.output_partitioning().partition_count() == 1 {
                        input
                    } else {
                        // Apply a LocalLimitExec to each partition. The optimizer will also insert
                        // a CoalescePartitionsExec between the GlobalLimitExec and LocalLimitExec
                        if let Some(fetch) = fetch {
                            Arc::new(LocalLimitExec::new(input, *fetch + skip.unwrap_or(0)))
                        } else {
                            input
                        }
                    };

                    Ok(Arc::new(GlobalLimitExec::new(input, *skip, *fetch)))
                }
                LogicalPlan::CreateExternalTable(_) => {
                    // There is no default plan for "CREATE EXTERNAL
                    // TABLE" -- it must be handled at a higher level (so
                    // that the appropriate table can be registered with
                    // the context)
                    Err(DataFusionError::Internal(
                        "Unsupported logical plan: CreateExternalTable".to_string(),
                    ))
                }
                LogicalPlan::CreateCatalogSchema(_) => {
                    // There is no default plan for "CREATE SCHEMA".
                    // It must be handled at a higher level (so
                    // that the schema can be registered with
                    // the context)
                    Err(DataFusionError::Internal(
                        "Unsupported logical plan: CreateCatalogSchema".to_string(),
                    ))
                }
                LogicalPlan::CreateCatalog(_) => {
                    // There is no default plan for "CREATE DATABASE".
                    // It must be handled at a higher level (so
                    // that the schema can be registered with
                    // the context)
                    Err(DataFusionError::Internal(
                        "Unsupported logical plan: CreateCatalog".to_string(),
                    ))
                }
                | LogicalPlan::CreateMemoryTable(_) | LogicalPlan::DropTable (_) | LogicalPlan::CreateView(_) => {
                    // Create a dummy exec.
                    Ok(Arc::new(EmptyExec::new(
                        false,
                        SchemaRef::new(Schema::empty()),
                    )))
                }
                LogicalPlan::Explain (_) => Err(DataFusionError::Internal(
                    "Unsupported logical plan: Explain must be root of the plan".to_string(),
                )),
                LogicalPlan::Analyze(a) => {
                    let input = self.create_initial_plan(&a.input, session_state).await?;
                    let schema = SchemaRef::new((*a.schema).clone().into());
                    Ok(Arc::new(AnalyzeExec::new(a.verbose, input, schema)))
                }
                LogicalPlan::Extension(e) => {
                    let physical_inputs = futures::stream::iter(e.node.inputs())
                        .then(|lp| self.create_initial_plan(lp, session_state))
                        .try_collect::<Vec<_>>()
                        .await?;

                    let mut maybe_plan = None;
                    for planner in &self.extension_planners {
                        if maybe_plan.is_some() {
                            break;
                        }

                        let logical_input = e.node.inputs();
                        let plan = planner.plan_extension(
                            self,
                            e.node.as_ref(),
                            &logical_input,
                            &physical_inputs,
                            session_state,
                        );
                        let plan = plan.await;
                        if plan.is_err() {
                            continue;
                        }
                        maybe_plan = plan.unwrap();
                    }

                    let plan = maybe_plan.ok_or_else(|| DataFusionError::Plan(format!(
                        "No installed planner was able to convert the custom node to an execution plan: {:?}", e.node
                    )))?;

                    // Ensure the ExecutionPlan's schema matches the
                    // declared logical schema to catch and warn about
                    // logic errors when creating user defined plans.
                    if !e.node.schema().matches_arrow_schema(&plan.schema()) {
                        Err(DataFusionError::Plan(format!(
                            "Extension planner for {:?} created an ExecutionPlan with mismatched schema. \
                            LogicalPlan schema: {:?}, ExecutionPlan schema: {:?}",
                            e.node, e.node.schema(), plan.schema()
                        )))
                    } else {
                        Ok(plan)
                    }
                }
            };
            exec_plan
        }.boxed()
    }

    fn create_grouping_physical_expr(
        &self,
        group_expr: &[Expr],
        input_dfschema: &DFSchema,
        input_schema: &Schema,
        session_state: &SessionState,
    ) -> Result<PhysicalGroupBy> {
        if group_expr.len() == 1 {
            match &group_expr[0] {
                Expr::GroupingSet(GroupingSet::GroupingSets(grouping_sets)) => {
                    merge_grouping_set_physical_expr(
                        grouping_sets,
                        input_dfschema,
                        input_schema,
                        session_state,
                    )
                }
                Expr::GroupingSet(GroupingSet::Cube(exprs)) => create_cube_physical_expr(
                    exprs,
                    input_dfschema,
                    input_schema,
                    session_state,
                ),
                Expr::GroupingSet(GroupingSet::Rollup(exprs)) => {
                    create_rollup_physical_expr(
                        exprs,
                        input_dfschema,
                        input_schema,
                        session_state,
                    )
                }
                expr => Ok(PhysicalGroupBy::new_single(vec![tuple_err((
                    self.create_physical_expr(
                        expr,
                        input_dfschema,
                        input_schema,
                        session_state,
                    ),
                    physical_name(expr),
                ))?])),
            }
        } else {
            Ok(PhysicalGroupBy::new_single(
                group_expr
                    .iter()
                    .map(|e| {
                        tuple_err((
                            self.create_physical_expr(
                                e,
                                input_dfschema,
                                input_schema,
                                session_state,
                            ),
                            physical_name(e),
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?,
            ))
        }
    }
}

/// Expand and align  a GROUPING SET expression.
/// (see https://www.postgresql.org/docs/current/queries-table-expressions.html#QUERIES-GROUPING-SETS)
///
/// This will take a list of grouping sets and ensure that each group is
/// properly aligned for the physical execution plan. We do this by
/// identifying all unique expression in each group and conforming each
/// group to the same set of expression types and ordering.
/// For example, if we have something like `GROUPING SETS ((a,b,c),(a),(b),(b,c))`
/// we would expand this to `GROUPING SETS ((a,b,c),(a,NULL,NULL),(NULL,b,NULL),(NULL,b,c))
/// (see https://www.postgresql.org/docs/current/queries-table-expressions.html#QUERIES-GROUPING-SETS)
fn merge_grouping_set_physical_expr(
    grouping_sets: &[Vec<Expr>],
    input_dfschema: &DFSchema,
    input_schema: &Schema,
    session_state: &SessionState,
) -> Result<PhysicalGroupBy> {
    let num_groups = grouping_sets.len();
    let mut all_exprs: Vec<Expr> = vec![];
    let mut grouping_set_expr: Vec<(Arc<dyn PhysicalExpr>, String)> = vec![];
    let mut null_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = vec![];

    for expr in grouping_sets.iter().flatten() {
        if !all_exprs.contains(expr) {
            all_exprs.push(expr.clone());

            grouping_set_expr.push(get_physical_expr_pair(
                expr,
                input_dfschema,
                input_schema,
                session_state,
            )?);

            null_exprs.push(get_null_physical_expr_pair(
                expr,
                input_dfschema,
                input_schema,
                session_state,
            )?);
        }
    }

    let mut merged_sets: Vec<Vec<bool>> = Vec::with_capacity(num_groups);

    for expr_group in grouping_sets.iter() {
        let group: Vec<bool> = all_exprs
            .iter()
            .map(|expr| !expr_group.contains(expr))
            .collect();

        merged_sets.push(group)
    }

    Ok(PhysicalGroupBy::new(
        grouping_set_expr,
        null_exprs,
        merged_sets,
    ))
}

/// Expand and align a CUBE expression. This is a special case of GROUPING SETS
/// (see https://www.postgresql.org/docs/current/queries-table-expressions.html#QUERIES-GROUPING-SETS)
fn create_cube_physical_expr(
    exprs: &[Expr],
    input_dfschema: &DFSchema,
    input_schema: &Schema,
    session_state: &SessionState,
) -> Result<PhysicalGroupBy> {
    let num_of_exprs = exprs.len();
    let num_groups = num_of_exprs * num_of_exprs;

    let mut null_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> =
        Vec::with_capacity(num_of_exprs);
    let mut all_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> =
        Vec::with_capacity(num_of_exprs);

    for expr in exprs {
        null_exprs.push(get_null_physical_expr_pair(
            expr,
            input_dfschema,
            input_schema,
            session_state,
        )?);

        all_exprs.push(get_physical_expr_pair(
            expr,
            input_dfschema,
            input_schema,
            session_state,
        )?)
    }

    let mut groups: Vec<Vec<bool>> = Vec::with_capacity(num_groups);

    groups.push(vec![false; num_of_exprs]);

    for null_count in 1..=num_of_exprs {
        for null_idx in (0..num_of_exprs).combinations(null_count) {
            let mut next_group: Vec<bool> = vec![false; num_of_exprs];
            null_idx.into_iter().for_each(|i| next_group[i] = true);
            groups.push(next_group);
        }
    }

    Ok(PhysicalGroupBy::new(all_exprs, null_exprs, groups))
}

/// Expand and align a ROLLUP expression. This is a special case of GROUPING SETS
/// (see https://www.postgresql.org/docs/current/queries-table-expressions.html#QUERIES-GROUPING-SETS)
fn create_rollup_physical_expr(
    exprs: &[Expr],
    input_dfschema: &DFSchema,
    input_schema: &Schema,
    session_state: &SessionState,
) -> Result<PhysicalGroupBy> {
    let num_of_exprs = exprs.len();

    let mut null_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> =
        Vec::with_capacity(num_of_exprs);
    let mut all_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> =
        Vec::with_capacity(num_of_exprs);

    let mut groups: Vec<Vec<bool>> = Vec::with_capacity(num_of_exprs + 1);

    for expr in exprs {
        null_exprs.push(get_null_physical_expr_pair(
            expr,
            input_dfschema,
            input_schema,
            session_state,
        )?);

        all_exprs.push(get_physical_expr_pair(
            expr,
            input_dfschema,
            input_schema,
            session_state,
        )?)
    }

    for total in 0..=num_of_exprs {
        let mut group: Vec<bool> = Vec::with_capacity(num_of_exprs);

        for index in 0..num_of_exprs {
            if index < total {
                group.push(false);
            } else {
                group.push(true);
            }
        }

        groups.push(group)
    }

    Ok(PhysicalGroupBy::new(all_exprs, null_exprs, groups))
}

/// For a given logical expr, get a properly typed NULL ScalarValue physical expression
fn get_null_physical_expr_pair(
    expr: &Expr,
    input_dfschema: &DFSchema,
    input_schema: &Schema,
    session_state: &SessionState,
) -> Result<(Arc<dyn PhysicalExpr>, String)> {
    let physical_expr = create_physical_expr(
        expr,
        input_dfschema,
        input_schema,
        &session_state.execution_props,
    )?;
    let physical_name = physical_name(&expr.clone())?;

    let data_type = physical_expr.data_type(input_schema)?;
    let null_value: ScalarValue = (&data_type).try_into()?;

    let null_value = Literal::new(null_value);
    Ok((Arc::new(null_value), physical_name))
}

fn get_physical_expr_pair(
    expr: &Expr,
    input_dfschema: &DFSchema,
    input_schema: &Schema,
    session_state: &SessionState,
) -> Result<(Arc<dyn PhysicalExpr>, String)> {
    let physical_expr = create_physical_expr(
        expr,
        input_dfschema,
        input_schema,
        &session_state.execution_props,
    )?;
    let physical_name = physical_name(expr)?;
    Ok((physical_expr, physical_name))
}

/// Create a window expression with a name from a logical expression
pub fn create_window_expr_with_name(
    e: &Expr,
    name: impl Into<String>,
    logical_input_schema: &DFSchema,
    physical_input_schema: &Schema,
    execution_props: &ExecutionProps,
) -> Result<Arc<dyn WindowExpr>> {
    let name = name.into();
    match e {
        Expr::WindowFunction {
            fun,
            args,
            partition_by,
            order_by,
            window_frame,
        } => {
            let args = args
                .iter()
                .map(|e| {
                    create_physical_expr(
                        e,
                        logical_input_schema,
                        physical_input_schema,
                        execution_props,
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            let partition_by = partition_by
                .iter()
                .map(|e| {
                    create_physical_expr(
                        e,
                        logical_input_schema,
                        physical_input_schema,
                        execution_props,
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            let order_by = order_by
                .iter()
                .map(|e| match e {
                    Expr::Sort {
                        expr,
                        asc,
                        nulls_first,
                    } => create_physical_sort_expr(
                        expr,
                        logical_input_schema,
                        physical_input_schema,
                        SortOptions {
                            descending: !*asc,
                            nulls_first: *nulls_first,
                        },
                        execution_props,
                    ),
                    _ => Err(DataFusionError::Plan(
                        "Sort only accepts sort expressions".to_string(),
                    )),
                })
                .collect::<Result<Vec<_>>>()?;
            if window_frame.is_some() {
                return Err(DataFusionError::NotImplemented(
                    "window expression with window frame definition is not yet supported"
                        .to_owned(),
                ));
            }
            windows::create_window_expr(
                fun,
                name,
                &args,
                &partition_by,
                &order_by,
                *window_frame,
                physical_input_schema,
            )
        }
        other => Err(DataFusionError::Internal(format!(
            "Invalid window expression '{:?}'",
            other
        ))),
    }
}

/// Create a window expression from a logical expression or an alias
pub fn create_window_expr(
    e: &Expr,
    logical_input_schema: &DFSchema,
    physical_input_schema: &Schema,
    execution_props: &ExecutionProps,
) -> Result<Arc<dyn WindowExpr>> {
    // unpack aliased logical expressions, e.g. "sum(col) over () as total"
    let (name, e) = match e {
        Expr::Alias(sub_expr, alias) => (alias.clone(), sub_expr.as_ref()),
        _ => (physical_name(e)?, e),
    };
    create_window_expr_with_name(
        e,
        name,
        logical_input_schema,
        physical_input_schema,
        execution_props,
    )
}

/// Create an aggregate expression with a name from a logical expression
pub fn create_aggregate_expr_with_name(
    e: &Expr,
    name: impl Into<String>,
    logical_input_schema: &DFSchema,
    physical_input_schema: &Schema,
    execution_props: &ExecutionProps,
) -> Result<Arc<dyn AggregateExpr>> {
    match e {
        Expr::AggregateFunction {
            fun,
            distinct,
            args,
            ..
        } => {
            let args = args
                .iter()
                .map(|e| {
                    create_physical_expr(
                        e,
                        logical_input_schema,
                        physical_input_schema,
                        execution_props,
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            aggregates::create_aggregate_expr(
                fun,
                *distinct,
                &args,
                physical_input_schema,
                name,
            )
        }
        Expr::AggregateUDF { fun, args, .. } => {
            let args = args
                .iter()
                .map(|e| {
                    create_physical_expr(
                        e,
                        logical_input_schema,
                        physical_input_schema,
                        execution_props,
                    )
                })
                .collect::<Result<Vec<_>>>()?;

            udaf::create_aggregate_expr(fun, &args, physical_input_schema, name)
        }
        other => Err(DataFusionError::Internal(format!(
            "Invalid aggregate expression '{:?}'",
            other
        ))),
    }
}

/// Create an aggregate expression from a logical expression or an alias
pub fn create_aggregate_expr(
    e: &Expr,
    logical_input_schema: &DFSchema,
    physical_input_schema: &Schema,
    execution_props: &ExecutionProps,
) -> Result<Arc<dyn AggregateExpr>> {
    // unpack (nested) aliased logical expressions, e.g. "sum(col) as total"
    let (name, e) = match e {
        Expr::Alias(sub_expr, alias) => (alias.clone(), sub_expr.as_ref()),
        _ => (physical_name(e)?, e),
    };

    create_aggregate_expr_with_name(
        e,
        name,
        logical_input_schema,
        physical_input_schema,
        execution_props,
    )
}

/// Create a physical sort expression from a logical expression
pub fn create_physical_sort_expr(
    e: &Expr,
    input_dfschema: &DFSchema,
    input_schema: &Schema,
    options: SortOptions,
    execution_props: &ExecutionProps,
) -> Result<PhysicalSortExpr> {
    Ok(PhysicalSortExpr {
        expr: create_physical_expr(e, input_dfschema, input_schema, execution_props)?,
        options,
    })
}

impl DefaultPhysicalPlanner {
    /// Handles capturing the various plans for EXPLAIN queries
    ///
    /// Returns
    /// Some(plan) if optimized, and None if logical_plan was not an
    /// explain (and thus needs to be optimized as normal)
    async fn handle_explain(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        if let LogicalPlan::Explain(e) = logical_plan {
            use PlanType::*;
            let mut stringified_plans = vec![];

            if !session_state
                .config
                .config_options
                .get_bool(OPT_EXPLAIN_PHYSICAL_PLAN_ONLY)
            {
                stringified_plans = e.stringified_plans.clone();

                stringified_plans.push(e.plan.to_stringified(FinalLogicalPlan));
            }

            if !session_state
                .config
                .config_options
                .get_bool(OPT_EXPLAIN_LOGICAL_PLAN_ONLY)
            {
                let input = self
                    .create_initial_plan(e.plan.as_ref(), session_state)
                    .await?;

                stringified_plans.push(
                    displayable(input.as_ref()).to_stringified(InitialPhysicalPlan),
                );

                let input =
                    self.optimize_internal(input, session_state, |plan, optimizer| {
                        let optimizer_name = optimizer.name().to_string();
                        let plan_type = OptimizedPhysicalPlan { optimizer_name };
                        stringified_plans
                            .push(displayable(plan).to_stringified(plan_type));
                    })?;

                stringified_plans
                    .push(displayable(input.as_ref()).to_stringified(FinalPhysicalPlan));
            }

            Ok(Some(Arc::new(ExplainExec::new(
                SchemaRef::new(e.schema.as_ref().to_owned().into()),
                stringified_plans,
                e.verbose,
            ))))
        } else {
            Ok(None)
        }
    }

    /// Optimize a physical plan by applying each physical optimizer,
    /// calling observer(plan, optimizer after each one)
    fn optimize_internal<F>(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        session_state: &SessionState,
        mut observer: F,
    ) -> Result<Arc<dyn ExecutionPlan>>
    where
        F: FnMut(&dyn ExecutionPlan, &dyn PhysicalOptimizerRule),
    {
        let optimizers = &session_state.physical_optimizers;
        debug!(
            "Input physical plan:\n{}\n",
            displayable(plan.as_ref()).indent()
        );
        trace!("Detailed input physical plan:\n{:?}", plan);

        let mut new_plan = plan;
        for optimizer in optimizers {
            new_plan = optimizer.optimize(new_plan, &session_state.config)?;
            observer(new_plan.as_ref(), optimizer.as_ref())
        }
        debug!(
            "Optimized physical plan:\n{}\n",
            displayable(new_plan.as_ref()).indent()
        );
        trace!("Detailed optimized physical plan:\n{:?}", new_plan);
        Ok(new_plan)
    }
}

fn tuple_err<T, R>(value: (Result<T>, Result<R>)) -> Result<(T, R)> {
    match value {
        (Ok(e), Ok(e1)) => Ok((e, e1)),
        (Err(e), Ok(_)) => Err(e),
        (Ok(_), Err(e1)) => Err(e1),
        (Err(e), Err(_)) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::assert_contains;
    use crate::execution::context::TaskContext;
    use crate::execution::options::CsvReadOptions;
    use crate::execution::runtime_env::RuntimeEnv;
    use crate::logical_plan::plan::Extension;
    use crate::physical_plan::{
        expressions, DisplayFormatType, Partitioning, Statistics,
    };
    use crate::prelude::{SessionConfig, SessionContext};
    use crate::scalar::ScalarValue;
    use crate::test_util::{scan_empty, scan_empty_with_partitions};
    use crate::{
        logical_plan::LogicalPlanBuilder, physical_plan::SendableRecordBatchStream,
    };
    use arrow::datatypes::{DataType, Field, SchemaRef};
    use datafusion_common::{DFField, DFSchema, DFSchemaRef};
    use datafusion_expr::expr::GroupingSet;
    use datafusion_expr::sum;
    use datafusion_expr::{col, lit};
    use fmt::Debug;
    use std::collections::HashMap;
    use std::convert::TryFrom;
    use std::{any::Any, fmt};

    fn make_session_state() -> SessionState {
        let runtime = Arc::new(RuntimeEnv::default());
        SessionState::with_config_rt(SessionConfig::new(), runtime)
    }

    async fn plan(logical_plan: &LogicalPlan) -> Result<Arc<dyn ExecutionPlan>> {
        let mut session_state = make_session_state();
        session_state.config.target_partitions = 4;
        let planner = DefaultPhysicalPlanner::default();
        planner
            .create_physical_plan(logical_plan, &session_state)
            .await
    }

    #[tokio::test]
    async fn test_all_operators() -> Result<()> {
        let logical_plan = test_csv_scan()
            .await?
            // filter clause needs the type coercion rule applied
            .filter(col("c7").lt(lit(5_u8)))?
            .project(vec![col("c1"), col("c2")])?
            .aggregate(vec![col("c1")], vec![sum(col("c2"))])?
            .sort(vec![col("c1").sort(true, true)])?
            .limit(Some(3), Some(10))?
            .build()?;

        let plan = plan(&logical_plan).await?;

        // verify that the plan correctly casts u8 to i64
        // the cast here is implicit so has CastOptions with safe=true
        let expected = "BinaryExpr { left: Column { name: \"c7\", index: 6 }, op: Lt, right: TryCastExpr { expr: Literal { value: UInt8(5) }, cast_type: Int64 } }";
        assert!(format!("{:?}", plan).contains(expected));

        Ok(())
    }

    #[tokio::test]
    async fn test_create_cube_expr() -> Result<()> {
        let logical_plan = test_csv_scan().await?.build()?;

        let plan = plan(&logical_plan).await?;

        let exprs = vec![col("c1"), col("c2"), col("c3")];

        let physical_input_schema = plan.schema();
        let physical_input_schema = physical_input_schema.as_ref();
        let logical_input_schema = logical_plan.schema();
        let session_state = make_session_state();

        let cube = create_cube_physical_expr(
            &exprs,
            logical_input_schema,
            physical_input_schema,
            &session_state,
        );

        let expected = r#"Ok(PhysicalGroupBy { expr: [(Column { name: "c1", index: 0 }, "c1"), (Column { name: "c2", index: 1 }, "c2"), (Column { name: "c3", index: 2 }, "c3")], null_expr: [(Literal { value: Utf8(NULL) }, "c1"), (Literal { value: Int64(NULL) }, "c2"), (Literal { value: Int64(NULL) }, "c3")], groups: [[false, false, false], [true, false, false], [false, true, false], [false, false, true], [true, true, false], [true, false, true], [false, true, true], [true, true, true]] })"#;

        assert_eq!(format!("{:?}", cube), expected);

        Ok(())
    }

    #[tokio::test]
    async fn test_create_rollup_expr() -> Result<()> {
        let logical_plan = test_csv_scan().await?.build()?;

        let plan = plan(&logical_plan).await?;

        let exprs = vec![col("c1"), col("c2"), col("c3")];

        let physical_input_schema = plan.schema();
        let physical_input_schema = physical_input_schema.as_ref();
        let logical_input_schema = logical_plan.schema();
        let session_state = make_session_state();

        let rollup = create_rollup_physical_expr(
            &exprs,
            logical_input_schema,
            physical_input_schema,
            &session_state,
        );

        let expected = r#"Ok(PhysicalGroupBy { expr: [(Column { name: "c1", index: 0 }, "c1"), (Column { name: "c2", index: 1 }, "c2"), (Column { name: "c3", index: 2 }, "c3")], null_expr: [(Literal { value: Utf8(NULL) }, "c1"), (Literal { value: Int64(NULL) }, "c2"), (Literal { value: Int64(NULL) }, "c3")], groups: [[true, true, true], [false, true, true], [false, false, true], [false, false, false]] })"#;

        assert_eq!(format!("{:?}", rollup), expected);

        Ok(())
    }

    #[tokio::test]
    async fn test_create_not() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Boolean, true)]);
        let dfschema = DFSchema::try_from(schema.clone())?;

        let planner = DefaultPhysicalPlanner::default();

        let expr = planner.create_physical_expr(
            &col("a").not(),
            &dfschema,
            &schema,
            &make_session_state(),
        )?;
        let expected = expressions::not(expressions::col("a", &schema)?)?;

        assert_eq!(format!("{:?}", expr), format!("{:?}", expected));

        Ok(())
    }

    #[tokio::test]
    async fn test_with_csv_plan() -> Result<()> {
        let logical_plan = test_csv_scan()
            .await?
            .filter(col("c7").lt(col("c12")))?
            .limit(Some(3), None)?
            .build()?;

        let plan = plan(&logical_plan).await?;

        // c12 is f64, c7 is u8 -> cast c7 to f64
        // the cast here is implicit so has CastOptions with safe=true
        let _expected = "predicate: BinaryExpr { left: TryCastExpr { expr: Column { name: \"c7\", index: 6 }, cast_type: Float64 }, op: Lt, right: Column { name: \"c12\", index: 11 } }";
        let plan_debug_str = format!("{:?}", plan);
        assert!(plan_debug_str.contains("GlobalLimitExec"));
        assert!(plan_debug_str.contains("skip: Some(3)"));
        Ok(())
    }

    #[tokio::test]
    async fn test_with_zero_offset_plan() -> Result<()> {
        let logical_plan = test_csv_scan().await?.limit(Some(0), None)?.build()?;
        let plan = plan(&logical_plan).await?;
        assert!(format!("{:?}", plan).contains("GlobalLimitExec"));
        assert!(format!("{:?}", plan).contains("skip: Some(0)"));
        Ok(())
    }

    #[tokio::test]
    async fn test_limit_with_partitions() -> Result<()> {
        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);

        let logical_plan = scan_empty_with_partitions(Some("test"), &schema, None, 2)?
            .limit(Some(3), Some(5))?
            .build()?;
        let plan = plan(&logical_plan).await?;

        assert!(format!("{:?}", plan).contains("GlobalLimitExec"));
        assert!(format!("{:?}", plan).contains("skip: Some(3), fetch: Some(5)"));

        // LocalLimitExec adjusts the `fetch`
        assert!(format!("{:?}", plan).contains("LocalLimitExec"));
        assert!(format!("{:?}", plan).contains("fetch: 8"));
        Ok(())
    }

    #[tokio::test]
    async fn errors() -> Result<()> {
        let bool_expr = col("c1").eq(col("c1"));
        let cases = vec![
            // utf8 AND utf8
            col("c1").and(col("c1")),
            // u8 AND u8
            col("c3").and(col("c3")),
            // utf8 = bool
            col("c1").eq(bool_expr.clone()),
            // u32 AND bool
            col("c2").and(bool_expr),
            // utf8 LIKE u32
            col("c1").like(col("c2")),
        ];
        for case in cases {
            let logical_plan = test_csv_scan().await?.project(vec![case.clone()]);
            let message = format!(
                "Expression {:?} expected to error due to impossible coercion",
                case
            );
            assert!(logical_plan.is_err(), "{}", message);
        }
        Ok(())
    }

    #[tokio::test]
    async fn default_extension_planner() {
        let session_state = make_session_state();
        let planner = DefaultPhysicalPlanner::default();
        let logical_plan = LogicalPlan::Extension(Extension {
            node: Arc::new(NoOpExtensionNode::default()),
        });
        let plan = planner
            .create_physical_plan(&logical_plan, &session_state)
            .await;

        let expected_error =
            "No installed planner was able to convert the custom node to an execution plan: NoOp";
        match plan {
            Ok(_) => panic!("Expected planning failure"),
            Err(e) => assert!(
                e.to_string().contains(expected_error),
                "Error '{}' did not contain expected error '{}'",
                e,
                expected_error
            ),
        }
    }

    #[tokio::test]
    async fn bad_extension_planner() {
        // Test that creating an execution plan whose schema doesn't
        // match the logical plan's schema generates an error.
        let session_state = make_session_state();
        let planner = DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(
            BadExtensionPlanner {},
        )]);

        let logical_plan = LogicalPlan::Extension(Extension {
            node: Arc::new(NoOpExtensionNode::default()),
        });
        let plan = planner
            .create_physical_plan(&logical_plan, &session_state)
            .await;

        let expected_error: &str = "Error during planning: \
        Extension planner for NoOp created an ExecutionPlan with mismatched schema. \
        LogicalPlan schema: DFSchema { fields: [\
            DFField { qualifier: None, field: Field { \
                name: \"a\", \
                data_type: Int32, \
                nullable: false, \
                dict_id: 0, \
                dict_is_ordered: false, \
                metadata: None } }\
        ], metadata: {} }, \
        ExecutionPlan schema: Schema { fields: [\
            Field { \
                name: \"b\", \
                data_type: Int32, \
                nullable: false, \
                dict_id: 0, \
                dict_is_ordered: false, \
                metadata: None }\
        ], metadata: {} }";
        match plan {
            Ok(_) => panic!("Expected planning failure"),
            Err(e) => assert!(
                e.to_string().contains(expected_error),
                "Error '{}' did not contain expected error '{}'",
                e,
                expected_error
            ),
        }
    }

    #[tokio::test]
    async fn in_list_types() -> Result<()> {
        // expression: "a in ('a', 1)"
        let list = vec![lit("a"), lit(1i64)];
        let logical_plan = test_csv_scan()
            .await?
            // filter clause needs the type coercion rule applied
            .filter(col("c12").lt(lit(0.05)))?
            .project(vec![col("c1").in_list(list, false)])?
            .build()?;
        let execution_plan = plan(&logical_plan).await?;
        // verify that the plan correctly adds cast from Int64(1) to Utf8
        let expected = "expr: [(InListExpr { expr: Column { name: \"c1\", index: 0 }, list: [Literal { value: Utf8(\"a\") }, TryCastExpr { expr: Literal { value: Int64(1) }, cast_type: Utf8 }], negated: false, set: None }";
        assert!(format!("{:?}", execution_plan).contains(expected));

        // expression: "a in (struct::null, 'a')"
        let list = vec![struct_literal(), lit("a")];

        let logical_plan = test_csv_scan()
            .await?
            // filter clause needs the type coercion rule applied
            .filter(col("c12").lt(lit(0.05)))?
            .project(vec![col("c12").lt_eq(lit(0.025)).in_list(list, false)])?
            .build()?;
        let execution_plan = plan(&logical_plan).await;

        let e = execution_plan.unwrap_err().to_string();
        assert_contains!(&e, "Can not find compatible types to compare Boolean with [Struct([Field { name: \"foo\", data_type: Boolean, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: None }]), Utf8]");

        Ok(())
    }

    /// Return a `null` literal representing a struct type like: `{ a: bool }`
    fn struct_literal() -> Expr {
        let struct_literal = ScalarValue::Struct(
            None,
            Box::new(vec![Field::new("foo", DataType::Boolean, false)]),
        );
        lit(struct_literal)
    }

    #[tokio::test]
    async fn in_set_test() -> Result<()> {
        // OPTIMIZER_INSET_THRESHOLD = 10
        // expression: "a in ('a', 1, 2, ..30)"
        let mut list = vec![Expr::Literal(ScalarValue::Utf8(Some("a".to_string())))];
        for i in 1..31 {
            list.push(Expr::Literal(ScalarValue::Int64(Some(i))));
        }
        let logical_plan = test_csv_scan()
            .await?
            .filter(col("c12").lt(lit(0.05)))?
            .project(vec![col("c1").in_list(list, false)])?
            .build()?;
        let execution_plan = plan(&logical_plan).await?;
        let expected = "expr: [(InListExpr { expr: Column { name: \"c1\", index: 0 }, list: [Literal { value: Utf8(\"a\") }, TryCastExpr { expr: Literal { value: Int64(1) }, cast_type: Utf8 }, TryCastExpr { expr: Literal { value: Int64(2) }, cast_type: Utf8 }, TryCastExpr { expr: Literal { value: Int64(3) }, cast_type: Utf8 }, TryCastExpr { expr: Literal { value: Int64(4) }, cast_type: Utf8 }, TryCastExpr { expr: Literal { value: Int64(5) }, cast_type: Utf8 }, TryCastExpr { expr: Literal { value: Int64(6) }, cast_type: Utf8 }, TryCastExpr { expr: Literal { value: Int64(7) }, cast_type: Utf8 }, TryCastExpr { expr: Literal { value: Int64(8) }, cast_type: Utf8 }, TryCastExpr { expr: Literal { value: Int64(9) }, cast_type: Utf8 }, TryCastExpr { expr: Literal { value: Int64(10) }, cast_type: Utf8 }, TryCastExpr { expr: Literal { value: Int64(11) }, cast_type: Utf8 }, TryCastExpr { expr: Literal { value: Int64(12) }, cast_type: Utf8 }, TryCastExpr { expr: Literal { value: Int64(13) }, cast_type: Utf8 }, TryCastExpr { expr: Literal { value: Int64(14) }, cast_type: Utf8 }, TryCastExpr { expr: Literal { value: Int64(15) }, cast_type: Utf8 }, TryCastExpr { expr: Literal { value: Int64(16) }, cast_type: Utf8 }, TryCastExpr { expr: Literal { value: Int64(17) }, cast_type: Utf8 }, TryCastExpr { expr: Literal { value: Int64(18) }, cast_type: Utf8 }, TryCastExpr { expr: Literal { value: Int64(19) }, cast_type: Utf8 }, TryCastExpr { expr: Literal { value: Int64(20) }, cast_type: Utf8 }, TryCastExpr { expr: Literal { value: Int64(21) }, cast_type: Utf8 }, TryCastExpr { expr: Literal { value: Int64(22) }, cast_type: Utf8 }, TryCastExpr { expr: Literal { value: Int64(23) }, cast_type: Utf8 }, TryCastExpr { expr: Literal { value: Int64(24) }, cast_type: Utf8 }, TryCastExpr { expr: Literal { value: Int64(25) }, cast_type: Utf8 }, TryCastExpr { expr: Literal { value: Int64(26) }, cast_type: Utf8 }, TryCastExpr { expr: Literal { value: Int64(27) }, cast_type: Utf8 }, TryCastExpr { expr: Literal { value: Int64(28) }, cast_type: Utf8 }, TryCastExpr { expr: Literal { value: Int64(29) }, cast_type: Utf8 }, TryCastExpr { expr: Literal { value: Int64(30) }, cast_type: Utf8 }], negated: false, set: Some(InSet { set: ";
        assert!(format!("{:?}", execution_plan).contains(expected));
        Ok(())
    }

    #[tokio::test]
    async fn in_set_null_test() -> Result<()> {
        // test NULL
        let mut list = vec![Expr::Literal(ScalarValue::Int64(None))];
        for i in 1..31 {
            list.push(Expr::Literal(ScalarValue::Int64(Some(i))));
        }

        let logical_plan = test_csv_scan()
            .await?
            .filter(col("c12").lt(lit(0.05)))?
            .project(vec![col("c1").in_list(list, false)])?
            .build()?;
        let execution_plan = plan(&logical_plan).await?;
        let expected = "expr: [(InListExpr { expr: Column { name: \"c1\", index: 0 }, list: [TryCastExpr { expr: Literal { value: Int64(NULL) }, cast_type: Utf8 }, TryCastExpr { expr: Literal { value: Int64(1) }, cast_type: Utf8 }, TryCastExpr { expr: Literal { value: Int64(2) }, cast_type: Utf8 }, TryCastExpr { expr: Literal { value: Int64(3) }, cast_type: Utf8 }, TryCastExpr { expr: Literal { value: Int64(4) }, cast_type: Utf8 }, TryCastExpr { expr: Literal { value: Int64(5) }, cast_type: Utf8 }, TryCastExpr { expr: Literal { value: Int64(6) }, cast_type: Utf8 }, TryCastExpr { expr: Literal { value: Int64(7) }, cast_type: Utf8 }, TryCastExpr { expr: Literal { value: Int64(8) }, cast_type: Utf8 }, TryCastExpr { expr: Literal { value: Int64(9) }, cast_type: Utf8 }, TryCastExpr { expr: Literal { value: Int64(10) }, cast_type: Utf8 }, TryCastExpr { expr: Literal { value: Int64(11) }, cast_type: Utf8 }, TryCastExpr { expr: Literal { value: Int64(12) }, cast_type: Utf8 }, TryCastExpr { expr: Literal { value: Int64(13) }, cast_type: Utf8 }, TryCastExpr { expr: Literal { value: Int64(14) }, cast_type: Utf8 }, TryCastExpr { expr: Literal { value: Int64(15) }, cast_type: Utf8 }, TryCastExpr { expr: Literal { value: Int64(16) }, cast_type: Utf8 }, TryCastExpr { expr: Literal { value: Int64(17) }, cast_type: Utf8 }, TryCastExpr { expr: Literal { value: Int64(18) }, cast_type: Utf8 }, TryCastExpr { expr: Literal { value: Int64(19) }, cast_type: Utf8 }, TryCastExpr { expr: Literal { value: Int64(20) }, cast_type: Utf8 }, TryCastExpr { expr: Literal { value: Int64(21) }, cast_type: Utf8 }, TryCastExpr { expr: Literal { value: Int64(22) }, cast_type: Utf8 }, TryCastExpr { expr: Literal { value: Int64(23) }, cast_type: Utf8 }, TryCastExpr { expr: Literal { value: Int64(24) }, cast_type: Utf8 }, TryCastExpr { expr: Literal { value: Int64(25) }, cast_type: Utf8 }, TryCastExpr { expr: Literal { value: Int64(26) }, cast_type: Utf8 }, TryCastExpr { expr: Literal { value: Int64(27) }, cast_type: Utf8 }, TryCastExpr { expr: Literal { value: Int64(28) }, cast_type: Utf8 }, TryCastExpr { expr: Literal { value: Int64(29) }, cast_type: Utf8 }, TryCastExpr { expr: Literal { value: Int64(30) }, cast_type: Utf8 }], negated: false, set: Some(InSet {";
        assert!(format!("{:?}", execution_plan).contains(expected));
        Ok(())
    }

    #[tokio::test]
    async fn hash_agg_input_schema() -> Result<()> {
        let logical_plan = test_csv_scan_with_name("aggregate_test_100")
            .await?
            .aggregate(vec![col("c1")], vec![sum(col("c2"))])?
            .build()?;

        let execution_plan = plan(&logical_plan).await?;
        let final_hash_agg = execution_plan
            .as_any()
            .downcast_ref::<AggregateExec>()
            .expect("hash aggregate");
        assert_eq!(
            "SUM(aggregate_test_100.c2)",
            final_hash_agg.schema().field(1).name()
        );
        // we need access to the input to the partial aggregate so that other projects can
        // implement serde
        assert_eq!("c2", final_hash_agg.input_schema().field(1).name());

        Ok(())
    }

    #[tokio::test]
    async fn hash_agg_grouping_set_input_schema() -> Result<()> {
        let grouping_set_expr = Expr::GroupingSet(GroupingSet::GroupingSets(vec![
            vec![col("c1")],
            vec![col("c2")],
            vec![col("c1"), col("c2")],
        ]));
        let logical_plan = test_csv_scan_with_name("aggregate_test_100")
            .await?
            .aggregate(vec![grouping_set_expr], vec![sum(col("c3"))])?
            .build()?;

        let execution_plan = plan(&logical_plan).await?;
        let final_hash_agg = execution_plan
            .as_any()
            .downcast_ref::<AggregateExec>()
            .expect("hash aggregate");
        assert_eq!(
            "SUM(aggregate_test_100.c3)",
            final_hash_agg.schema().field(2).name()
        );
        // we need access to the input to the partial aggregate so that other projects can
        // implement serde
        assert_eq!("c3", final_hash_agg.input_schema().field(2).name());

        Ok(())
    }

    #[tokio::test]
    async fn hash_agg_group_by_partitioned() -> Result<()> {
        let logical_plan = test_csv_scan()
            .await?
            .aggregate(vec![col("c1")], vec![sum(col("c2"))])?
            .build()?;

        let execution_plan = plan(&logical_plan).await?;
        let formatted = format!("{:?}", execution_plan);

        // Make sure the plan contains a FinalPartitioned, which means it will not use the Final
        // mode in Aggregate (which is slower)
        assert!(formatted.contains("FinalPartitioned"));

        Ok(())
    }

    #[tokio::test]
    async fn hash_agg_grouping_set_by_partitioned() -> Result<()> {
        let grouping_set_expr = Expr::GroupingSet(GroupingSet::GroupingSets(vec![
            vec![col("c1")],
            vec![col("c2")],
            vec![col("c1"), col("c2")],
        ]));
        let logical_plan = test_csv_scan()
            .await?
            .aggregate(vec![grouping_set_expr], vec![sum(col("c3"))])?
            .build()?;

        let execution_plan = plan(&logical_plan).await?;
        let formatted = format!("{:?}", execution_plan);

        // Make sure the plan contains a FinalPartitioned, which means it will not use the Final
        // mode in Aggregate (which is slower)
        assert!(formatted.contains("FinalPartitioned"));

        Ok(())
    }

    #[tokio::test]
    async fn test_explain() {
        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);

        let logical_plan = scan_empty(Some("employee"), &schema, None)
            .unwrap()
            .explain(true, false)
            .unwrap()
            .build()
            .unwrap();

        let plan = plan(&logical_plan).await.unwrap();
        if let Some(plan) = plan.as_any().downcast_ref::<ExplainExec>() {
            let stringified_plans = plan.stringified_plans();
            assert!(stringified_plans.len() >= 4);
            assert!(stringified_plans
                .iter()
                .any(|p| matches!(p.plan_type, PlanType::FinalLogicalPlan)));
            assert!(stringified_plans
                .iter()
                .any(|p| matches!(p.plan_type, PlanType::InitialPhysicalPlan)));
            assert!(stringified_plans
                .iter()
                .any(|p| matches!(p.plan_type, PlanType::OptimizedPhysicalPlan { .. })));
            assert!(stringified_plans
                .iter()
                .any(|p| matches!(p.plan_type, PlanType::FinalPhysicalPlan)));
        } else {
            panic!(
                "Plan was not an explain plan: {}",
                displayable(plan.as_ref()).indent()
            );
        }
    }

    /// An example extension node that doesn't do anything
    struct NoOpExtensionNode {
        schema: DFSchemaRef,
    }

    impl Default for NoOpExtensionNode {
        fn default() -> Self {
            Self {
                schema: DFSchemaRef::new(
                    DFSchema::new_with_metadata(
                        vec![DFField::new(None, "a", DataType::Int32, false)],
                        HashMap::new(),
                    )
                    .unwrap(),
                ),
            }
        }
    }

    impl Debug for NoOpExtensionNode {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "NoOp")
        }
    }

    impl UserDefinedLogicalNode for NoOpExtensionNode {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn inputs(&self) -> Vec<&LogicalPlan> {
            vec![]
        }

        fn schema(&self) -> &DFSchemaRef {
            &self.schema
        }

        fn expressions(&self) -> Vec<Expr> {
            vec![]
        }

        fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "NoOp")
        }

        fn from_template(
            &self,
            _exprs: &[Expr],
            _inputs: &[LogicalPlan],
        ) -> Arc<dyn UserDefinedLogicalNode> {
            unimplemented!("NoOp");
        }
    }

    #[derive(Debug)]
    struct NoOpExecutionPlan {
        schema: SchemaRef,
    }

    impl ExecutionPlan for NoOpExecutionPlan {
        /// Return a reference to Any that can be used for downcasting
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn schema(&self) -> SchemaRef {
            self.schema.clone()
        }

        fn output_partitioning(&self) -> Partitioning {
            Partitioning::UnknownPartitioning(1)
        }

        fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
            None
        }

        fn relies_on_input_order(&self) -> bool {
            false
        }

        fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
            vec![]
        }

        fn with_new_children(
            self: Arc<Self>,
            _children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            unimplemented!("NoOpExecutionPlan::with_new_children");
        }

        fn execute(
            &self,
            _partition: usize,
            _context: Arc<TaskContext>,
        ) -> Result<SendableRecordBatchStream> {
            unimplemented!("NoOpExecutionPlan::execute");
        }

        fn fmt_as(
            &self,
            t: DisplayFormatType,
            f: &mut std::fmt::Formatter,
        ) -> std::fmt::Result {
            match t {
                DisplayFormatType::Default => {
                    write!(f, "NoOpExecutionPlan")
                }
            }
        }

        fn statistics(&self) -> Statistics {
            unimplemented!("NoOpExecutionPlan::statistics");
        }
    }

    //  Produces an execution plan where the schema is mismatched from
    //  the logical plan node.
    struct BadExtensionPlanner {}

    #[async_trait]
    impl ExtensionPlanner for BadExtensionPlanner {
        /// Create a physical plan for an extension node
        async fn plan_extension(
            &self,
            _planner: &dyn PhysicalPlanner,
            _node: &dyn UserDefinedLogicalNode,
            _logical_inputs: &[&LogicalPlan],
            _physical_inputs: &[Arc<dyn ExecutionPlan>],
            _session_state: &SessionState,
        ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
            Ok(Some(Arc::new(NoOpExecutionPlan {
                schema: SchemaRef::new(Schema::new(vec![Field::new(
                    "b",
                    DataType::Int32,
                    false,
                )])),
            })))
        }
    }

    async fn test_csv_scan_with_name(name: &str) -> Result<LogicalPlanBuilder> {
        let ctx = SessionContext::new();
        let testdata = crate::test_util::arrow_test_data();
        let path = format!("{}/csv/aggregate_test_100.csv", testdata);
        let options = CsvReadOptions::new().schema_infer_max_records(100);
        let logical_plan = match ctx.read_csv(path, options).await?.to_logical_plan()? {
            LogicalPlan::TableScan(ref scan) => {
                let mut scan = scan.clone();
                scan.table_name = name.to_string();
                let new_schema = scan
                    .projected_schema
                    .as_ref()
                    .clone()
                    .replace_qualifier(name);
                scan.projected_schema = Arc::new(new_schema);
                LogicalPlan::TableScan(scan)
            }
            _ => unimplemented!(),
        };
        Ok(LogicalPlanBuilder::from(logical_plan))
    }

    async fn test_csv_scan() -> Result<LogicalPlanBuilder> {
        let ctx = SessionContext::new();
        let testdata = crate::test_util::arrow_test_data();
        let path = format!("{}/csv/aggregate_test_100.csv", testdata);
        let options = CsvReadOptions::new().schema_infer_max_records(100);
        Ok(LogicalPlanBuilder::from(
            ctx.read_csv(path, options).await?.to_logical_plan()?,
        ))
    }
}
