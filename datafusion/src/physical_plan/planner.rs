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

use super::{
    aggregates, cross_join::CrossJoinExec, empty::EmptyExec, expressions::binary,
    functions, hash_join::PartitionMode, udaf, union::UnionExec, windows,
};
use crate::execution::context::ExecutionContextState;
use crate::logical_plan::{
    unnormalize_cols, DFSchema, Expr, LogicalPlan, Operator,
    Partitioning as LogicalPartitioning, PlanType, StringifiedPlan,
    UserDefinedLogicalNode,
};
use crate::physical_plan::explain::ExplainExec;
use crate::physical_plan::expressions;
use crate::physical_plan::expressions::{CaseExpr, Column, Literal, PhysicalSortExpr};
use crate::physical_plan::filter::FilterExec;
use crate::physical_plan::hash_aggregate::{AggregateMode, HashAggregateExec};
use crate::physical_plan::hash_join::HashJoinExec;
use crate::physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use crate::physical_plan::projection::ProjectionExec;
use crate::physical_plan::repartition::RepartitionExec;
use crate::physical_plan::sort::SortExec;
use crate::physical_plan::udf;
use crate::physical_plan::windows::WindowAggExec;
use crate::physical_plan::{hash_utils, Partitioning};
use crate::physical_plan::{AggregateExpr, ExecutionPlan, PhysicalExpr, WindowExpr};
use crate::scalar::ScalarValue;
use crate::sql::utils::{generate_sort_key, window_expr_common_partition_keys};
use crate::variable::VarType;
use crate::{
    error::{DataFusionError, Result},
    physical_plan::displayable,
};
use arrow::compute::SortOptions;
use arrow::datatypes::{Schema, SchemaRef};
use arrow::{compute::can_cast_types, datatypes::DataType};
use expressions::col;
use log::debug;
use std::sync::Arc;

fn create_function_physical_name(
    fun: &str,
    distinct: bool,
    args: &[Expr],
    input_schema: &DFSchema,
) -> Result<String> {
    let names: Vec<String> = args
        .iter()
        .map(|e| physical_name(e, input_schema))
        .collect::<Result<_>>()?;

    let distinct_str = match distinct {
        true => "DISTINCT ",
        false => "",
    };
    Ok(format!("{}({}{})", fun, distinct_str, names.join(",")))
}

fn physical_name(e: &Expr, input_schema: &DFSchema) -> Result<String> {
    match e {
        Expr::Column(c) => Ok(c.name.clone()),
        Expr::Alias(_, name) => Ok(name.clone()),
        Expr::ScalarVariable(variable_names) => Ok(variable_names.join(".")),
        Expr::Literal(value) => Ok(format!("{:?}", value)),
        Expr::BinaryExpr { left, op, right } => {
            let left = physical_name(left, input_schema)?;
            let right = physical_name(right, input_schema)?;
            Ok(format!("{} {:?} {}", left, op, right))
        }
        Expr::Case {
            expr,
            when_then_expr,
            else_expr,
        } => {
            let mut name = "CASE ".to_string();
            if let Some(e) = expr {
                name += &format!("{:?} ", e);
            }
            for (w, t) in when_then_expr {
                name += &format!("WHEN {:?} THEN {:?} ", w, t);
            }
            if let Some(e) = else_expr {
                name += &format!("ELSE {:?} ", e);
            }
            name += "END";
            Ok(name)
        }
        Expr::Cast { expr, data_type } => {
            let expr = physical_name(expr, input_schema)?;
            Ok(format!("CAST({} AS {:?})", expr, data_type))
        }
        Expr::TryCast { expr, data_type } => {
            let expr = physical_name(expr, input_schema)?;
            Ok(format!("TRY_CAST({} AS {:?})", expr, data_type))
        }
        Expr::Not(expr) => {
            let expr = physical_name(expr, input_schema)?;
            Ok(format!("NOT {}", expr))
        }
        Expr::Negative(expr) => {
            let expr = physical_name(expr, input_schema)?;
            Ok(format!("(- {})", expr))
        }
        Expr::IsNull(expr) => {
            let expr = physical_name(expr, input_schema)?;
            Ok(format!("{} IS NULL", expr))
        }
        Expr::IsNotNull(expr) => {
            let expr = physical_name(expr, input_schema)?;
            Ok(format!("{} IS NOT NULL", expr))
        }
        Expr::ScalarFunction { fun, args, .. } => {
            create_function_physical_name(&fun.to_string(), false, args, input_schema)
        }
        Expr::ScalarUDF { fun, args, .. } => {
            create_function_physical_name(&fun.name, false, args, input_schema)
        }
        Expr::WindowFunction { fun, args, .. } => {
            create_function_physical_name(&fun.to_string(), false, args, input_schema)
        }
        Expr::AggregateFunction {
            fun,
            distinct,
            args,
            ..
        } => {
            create_function_physical_name(&fun.to_string(), *distinct, args, input_schema)
        }
        Expr::AggregateUDF { fun, args } => {
            let mut names = Vec::with_capacity(args.len());
            for e in args {
                names.push(physical_name(e, input_schema)?);
            }
            Ok(format!("{}({})", fun.name, names.join(",")))
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let expr = physical_name(expr, input_schema)?;
            let list = list.iter().map(|expr| physical_name(expr, input_schema));
            if *negated {
                Ok(format!("{} NOT IN ({:?})", expr, list))
            } else {
                Ok(format!("{} IN ({:?})", expr, list))
            }
        }
        other => Err(DataFusionError::NotImplemented(format!(
            "Cannot derive physical field name for logical expression {:?}",
            other
        ))),
    }
}

/// Physical query planner that converts a `LogicalPlan` to an
/// `ExecutionPlan` suitable for execution.
pub trait PhysicalPlanner {
    /// Create a physical plan from a logical plan
    fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        ctx_state: &ExecutionContextState,
    ) -> Result<Arc<dyn ExecutionPlan>>;

    /// Create a physical expression from a logical expression
    /// suitable for evaluation
    ///
    /// `expr`: the expression to convert
    ///
    /// `input_dfschema`: the logical plan schema for evaluating `e`
    ///
    /// `input_schema`: the physical schema for evaluating `e`
    fn create_physical_expr(
        &self,
        expr: &Expr,
        input_dfschema: &DFSchema,
        input_schema: &Schema,
        ctx_state: &ExecutionContextState,
    ) -> Result<Arc<dyn PhysicalExpr>>;
}

/// This trait exposes the ability to plan an [`ExecutionPlan`] out of a [`LogicalPlan`].
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
    fn plan_extension(
        &self,
        planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        ctx_state: &ExecutionContextState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>>;
}

/// Default single node physical query planner that converts a
/// `LogicalPlan` to an `ExecutionPlan` suitable for execution.
pub struct DefaultPhysicalPlanner {
    extension_planners: Vec<Arc<dyn ExtensionPlanner + Send + Sync>>,
}

impl Default for DefaultPhysicalPlanner {
    fn default() -> Self {
        Self {
            extension_planners: vec![],
        }
    }
}

impl PhysicalPlanner for DefaultPhysicalPlanner {
    /// Create a physical plan from a logical plan
    fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        ctx_state: &ExecutionContextState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let plan = self.create_initial_plan(logical_plan, ctx_state)?;
        self.optimize_plan(plan, ctx_state)
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
        ctx_state: &ExecutionContextState,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        DefaultPhysicalPlanner::create_physical_expr(
            self,
            expr,
            input_dfschema,
            input_schema,
            ctx_state,
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

    /// Optimize a physical plan
    fn optimize_plan(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        ctx_state: &ExecutionContextState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let optimizers = &ctx_state.config.physical_optimizers;
        debug!("Physical plan:\n{:?}", plan);

        let mut new_plan = plan;
        for optimizer in optimizers {
            new_plan = optimizer.optimize(new_plan, &ctx_state.config)?;
        }
        debug!("Optimized physical plan:\n{:?}", new_plan);
        Ok(new_plan)
    }

    /// Create a physical plan from a logical plan
    fn create_initial_plan(
        &self,
        logical_plan: &LogicalPlan,
        ctx_state: &ExecutionContextState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let batch_size = ctx_state.config.batch_size;

        match logical_plan {
            LogicalPlan::TableScan {
                source,
                projection,
                filters,
                limit,
                ..
            } => {
                // Remove all qualifiers from the scan as the provider
                // doesn't know (nor should care) how the relation was
                // referred to in the query
                let filters = unnormalize_cols(filters.iter().cloned());
                source.scan(projection, batch_size, &filters, *limit)
            }
            LogicalPlan::Window {
                input, window_expr, ..
            } => {
                if window_expr.is_empty() {
                    return Err(DataFusionError::Internal(
                        "Impossibly got empty window expression".to_owned(),
                    ));
                }

                let input_exec = self.create_initial_plan(input, ctx_state)?;

                // at this moment we are guaranteed by the logical planner
                // to have all the window_expr to have equal sort key
                let partition_keys = window_expr_common_partition_keys(window_expr)?;

                let can_repartition = !partition_keys.is_empty()
                    && ctx_state.config.concurrency > 1
                    && ctx_state.config.repartition_windows;

                let input_exec = if can_repartition {
                    let partition_keys = partition_keys
                        .iter()
                        .map(|e| {
                            self.create_physical_expr(
                                e,
                                input.schema(),
                                &input_exec.schema(),
                                ctx_state,
                            )
                        })
                        .collect::<Result<Vec<Arc<dyn PhysicalExpr>>>>()?;
                    Arc::new(RepartitionExec::try_new(
                        input_exec,
                        Partitioning::Hash(partition_keys, ctx_state.config.concurrency),
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
                            } => self.create_physical_sort_expr(
                                expr,
                                logical_input_schema,
                                &physical_input_schema,
                                SortOptions {
                                    descending: !*asc,
                                    nulls_first: *nulls_first,
                                },
                                ctx_state,
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
                        self.create_window_expr(
                            e,
                            logical_input_schema,
                            &physical_input_schema,
                            ctx_state,
                        )
                    })
                    .collect::<Result<Vec<_>>>()?;

                Ok(Arc::new(WindowAggExec::try_new(
                    window_expr,
                    input_exec,
                    physical_input_schema,
                )?))
            }
            LogicalPlan::Aggregate {
                input,
                group_expr,
                aggr_expr,
                ..
            } => {
                // Initially need to perform the aggregate and then merge the partitions
                let input_exec = self.create_initial_plan(input, ctx_state)?;
                let physical_input_schema = input_exec.schema();
                let logical_input_schema = input.as_ref().schema();

                let groups = group_expr
                    .iter()
                    .map(|e| {
                        tuple_err((
                            self.create_physical_expr(
                                e,
                                logical_input_schema,
                                &physical_input_schema,
                                ctx_state,
                            ),
                            physical_name(e, logical_input_schema),
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?;
                let aggregates = aggr_expr
                    .iter()
                    .map(|e| {
                        self.create_aggregate_expr(
                            e,
                            logical_input_schema,
                            &physical_input_schema,
                            ctx_state,
                        )
                    })
                    .collect::<Result<Vec<_>>>()?;

                let initial_aggr = Arc::new(HashAggregateExec::try_new(
                    AggregateMode::Partial,
                    groups.clone(),
                    aggregates.clone(),
                    input_exec,
                    physical_input_schema.clone(),
                )?);

                // update group column indices based on partial aggregate plan evaluation
                let final_group: Vec<Arc<dyn PhysicalExpr>> = (0..groups.len())
                    .map(|i| col(&groups[i].1, &initial_aggr.schema()))
                    .collect::<Result<_>>()?;

                // TODO: dictionary type not yet supported in Hash Repartition
                let contains_dict = groups
                    .iter()
                    .flat_map(|x| x.0.data_type(physical_input_schema.as_ref()))
                    .any(|x| matches!(x, DataType::Dictionary(_, _)));

                let can_repartition = !groups.is_empty()
                    && ctx_state.config.concurrency > 1
                    && ctx_state.config.repartition_aggregations
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
                            ctx_state.config.concurrency,
                        ),
                    )?);
                    // Combine hash aggregates within the partition
                    (hash_repartition, AggregateMode::FinalPartitioned)
                } else {
                    // construct a second aggregation, keeping the final column name equal to the
                    // first aggregation and the expressions corresponding to the respective aggregate
                    (initial_aggr, AggregateMode::Final)
                };

                Ok(Arc::new(HashAggregateExec::try_new(
                    next_partition_mode,
                    final_group
                        .iter()
                        .enumerate()
                        .map(|(i, expr)| (expr.clone(), groups[i].1.clone()))
                        .collect(),
                    aggregates,
                    initial_aggr,
                    physical_input_schema.clone(),
                )?))
            }
            LogicalPlan::Projection { input, expr, .. } => {
                let input_exec = self.create_initial_plan(input, ctx_state)?;
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
                        // get physical names with column qualifiers, which violates Datafusion's
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
                                Err(_) => physical_name(e, input_schema),
                            }
                        } else {
                            physical_name(e, input_schema)
                        };

                        tuple_err((
                            self.create_physical_expr(
                                e,
                                input_schema,
                                &input_exec.schema(),
                                ctx_state,
                            ),
                            physical_name,
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?;

                Ok(Arc::new(ProjectionExec::try_new(
                    physical_exprs,
                    input_exec,
                )?))
            }
            LogicalPlan::Filter {
                input, predicate, ..
            } => {
                let physical_input = self.create_initial_plan(input, ctx_state)?;
                let input_schema = physical_input.as_ref().schema();
                let input_dfschema = input.as_ref().schema();
                let runtime_expr = self.create_physical_expr(
                    predicate,
                    input_dfschema,
                    &input_schema,
                    ctx_state,
                )?;
                Ok(Arc::new(FilterExec::try_new(runtime_expr, physical_input)?))
            }
            LogicalPlan::Union { inputs, .. } => {
                let physical_plans = inputs
                    .iter()
                    .map(|input| self.create_initial_plan(input, ctx_state))
                    .collect::<Result<Vec<_>>>()?;
                Ok(Arc::new(UnionExec::new(physical_plans)))
            }
            LogicalPlan::Repartition {
                input,
                partitioning_scheme,
            } => {
                let physical_input = self.create_initial_plan(input, ctx_state)?;
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
                                    ctx_state,
                                )
                            })
                            .collect::<Result<Vec<_>>>()?;
                        Partitioning::Hash(runtime_expr, *n)
                    }
                };
                Ok(Arc::new(RepartitionExec::try_new(
                    physical_input,
                    physical_partitioning,
                )?))
            }
            LogicalPlan::Sort { expr, input, .. } => {
                let physical_input = self.create_initial_plan(input, ctx_state)?;
                let input_schema = physical_input.as_ref().schema();
                let input_dfschema = input.as_ref().schema();

                let sort_expr = expr
                    .iter()
                    .map(|e| match e {
                        Expr::Sort {
                            expr,
                            asc,
                            nulls_first,
                        } => self.create_physical_sort_expr(
                            expr,
                            input_dfschema,
                            &input_schema,
                            SortOptions {
                                descending: !*asc,
                                nulls_first: *nulls_first,
                            },
                            ctx_state,
                        ),
                        _ => Err(DataFusionError::Plan(
                            "Sort only accepts sort expressions".to_string(),
                        )),
                    })
                    .collect::<Result<Vec<_>>>()?;

                Ok(Arc::new(SortExec::try_new(sort_expr, physical_input)?))
            }
            LogicalPlan::Join {
                left,
                right,
                on: keys,
                join_type,
                ..
            } => {
                let left_df_schema = left.schema();
                let physical_left = self.create_initial_plan(left, ctx_state)?;
                let right_df_schema = right.schema();
                let physical_right = self.create_initial_plan(right, ctx_state)?;
                let join_on = keys
                    .iter()
                    .map(|(l, r)| {
                        Ok((
                            Column::new(&l.name, left_df_schema.index_of_column(l)?),
                            Column::new(&r.name, right_df_schema.index_of_column(r)?),
                        ))
                    })
                    .collect::<Result<hash_utils::JoinOn>>()?;

                if ctx_state.config.concurrency > 1 && ctx_state.config.repartition_joins
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
                            Partitioning::Hash(left_expr, ctx_state.config.concurrency),
                        )?),
                        Arc::new(RepartitionExec::try_new(
                            physical_right,
                            Partitioning::Hash(right_expr, ctx_state.config.concurrency),
                        )?),
                        join_on,
                        join_type,
                        PartitionMode::Partitioned,
                    )?))
                } else {
                    Ok(Arc::new(HashJoinExec::try_new(
                        physical_left,
                        physical_right,
                        join_on,
                        join_type,
                        PartitionMode::CollectLeft,
                    )?))
                }
            }
            LogicalPlan::CrossJoin { left, right, .. } => {
                let left = self.create_initial_plan(left, ctx_state)?;
                let right = self.create_initial_plan(right, ctx_state)?;
                Ok(Arc::new(CrossJoinExec::try_new(left, right)?))
            }
            LogicalPlan::EmptyRelation {
                produce_one_row,
                schema,
            } => Ok(Arc::new(EmptyExec::new(
                *produce_one_row,
                SchemaRef::new(schema.as_ref().to_owned().into()),
            ))),
            LogicalPlan::Limit { input, n, .. } => {
                let limit = *n;
                let input = self.create_initial_plan(input, ctx_state)?;

                // GlobalLimitExec requires a single partition for input
                let input = if input.output_partitioning().partition_count() == 1 {
                    input
                } else {
                    // Apply a LocalLimitExec to each partition. The optimizer will also insert
                    // a CoalescePartitionsExec between the GlobalLimitExec and LocalLimitExec
                    Arc::new(LocalLimitExec::new(input, limit))
                };

                Ok(Arc::new(GlobalLimitExec::new(input, limit)))
            }
            LogicalPlan::CreateExternalTable { .. } => {
                // There is no default plan for "CREATE EXTERNAL
                // TABLE" -- it must be handled at a higher level (so
                // that the appropriate table can be registered with
                // the context)
                Err(DataFusionError::Internal(
                    "Unsupported logical plan: CreateExternalTable".to_string(),
                ))
            }
            LogicalPlan::Explain {
                verbose,
                plan,
                stringified_plans,
                schema,
            } => {
                let input = self.create_initial_plan(plan, ctx_state)?;

                let mut stringified_plans = stringified_plans
                    .iter()
                    .filter(|s| s.should_display(*verbose))
                    .cloned()
                    .collect::<Vec<_>>();

                // add in the physical plan if requested
                if *verbose {
                    stringified_plans.push(StringifiedPlan::new(
                        PlanType::PhysicalPlan,
                        displayable(input.as_ref()).indent().to_string(),
                    ));
                }
                Ok(Arc::new(ExplainExec::new(
                    SchemaRef::new(schema.as_ref().to_owned().into()),
                    stringified_plans,
                )))
            }
            LogicalPlan::Extension { node } => {
                let physical_inputs = node
                    .inputs()
                    .into_iter()
                    .map(|input_plan| self.create_initial_plan(input_plan, ctx_state))
                    .collect::<Result<Vec<_>>>()?;

                let maybe_plan = self.extension_planners.iter().try_fold(
                    None,
                    |maybe_plan, planner| {
                        if let Some(plan) = maybe_plan {
                            Ok(Some(plan))
                        } else {
                            planner.plan_extension(
                                self,
                                node.as_ref(),
                                &node.inputs(),
                                &physical_inputs,
                                ctx_state,
                            )
                        }
                    },
                )?;
                let plan = maybe_plan.ok_or_else(|| DataFusionError::Plan(format!(
                    "No installed planner was able to convert the custom node to an execution plan: {:?}", node
                )))?;

                // Ensure the ExecutionPlan's schema matches the
                // declared logical schema to catch and warn about
                // logic errors when creating user defined plans.
                if !node.schema().matches_arrow_schema(&plan.schema()) {
                    Err(DataFusionError::Plan(format!(
                        "Extension planner for {:?} created an ExecutionPlan with mismatched schema. \
                         LogicalPlan schema: {:?}, ExecutionPlan schema: {:?}",
                        node, node.schema(), plan.schema()
                    )))
                } else {
                    Ok(plan)
                }
            }
        }
    }

    /// Create a physical expression from a logical expression
    pub fn create_physical_expr(
        &self,
        e: &Expr,
        input_dfschema: &DFSchema,
        input_schema: &Schema,
        ctx_state: &ExecutionContextState,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        match e {
            Expr::Alias(expr, ..) => Ok(self.create_physical_expr(
                expr,
                input_dfschema,
                input_schema,
                ctx_state,
            )?),
            Expr::Column(c) => {
                let idx = input_dfschema.index_of_column(c)?;
                Ok(Arc::new(Column::new(&c.name, idx)))
            }
            Expr::Literal(value) => Ok(Arc::new(Literal::new(value.clone()))),
            Expr::ScalarVariable(variable_names) => {
                if &variable_names[0][0..2] == "@@" {
                    match ctx_state.var_provider.get(&VarType::System) {
                        Some(provider) => {
                            let scalar_value =
                                provider.get_value(variable_names.clone())?;
                            Ok(Arc::new(Literal::new(scalar_value)))
                        }
                        _ => Err(DataFusionError::Plan(
                            "No system variable provider found".to_string(),
                        )),
                    }
                } else {
                    match ctx_state.var_provider.get(&VarType::UserDefined) {
                        Some(provider) => {
                            let scalar_value =
                                provider.get_value(variable_names.clone())?;
                            Ok(Arc::new(Literal::new(scalar_value)))
                        }
                        _ => Err(DataFusionError::Plan(
                            "No user defined variable provider found".to_string(),
                        )),
                    }
                }
            }
            Expr::BinaryExpr { left, op, right } => {
                let lhs = self.create_physical_expr(
                    left,
                    input_dfschema,
                    input_schema,
                    ctx_state,
                )?;
                let rhs = self.create_physical_expr(
                    right,
                    input_dfschema,
                    input_schema,
                    ctx_state,
                )?;
                binary(lhs, *op, rhs, input_schema)
            }
            Expr::Case {
                expr,
                when_then_expr,
                else_expr,
                ..
            } => {
                let expr: Option<Arc<dyn PhysicalExpr>> = if let Some(e) = expr {
                    Some(self.create_physical_expr(
                        e.as_ref(),
                        input_dfschema,
                        input_schema,
                        ctx_state,
                    )?)
                } else {
                    None
                };
                let when_expr = when_then_expr
                    .iter()
                    .map(|(w, _)| {
                        self.create_physical_expr(
                            w.as_ref(),
                            input_dfschema,
                            input_schema,
                            ctx_state,
                        )
                    })
                    .collect::<Result<Vec<_>>>()?;
                let then_expr = when_then_expr
                    .iter()
                    .map(|(_, t)| {
                        self.create_physical_expr(
                            t.as_ref(),
                            input_dfschema,
                            input_schema,
                            ctx_state,
                        )
                    })
                    .collect::<Result<Vec<_>>>()?;
                let when_then_expr: Vec<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)> =
                    when_expr
                        .iter()
                        .zip(then_expr.iter())
                        .map(|(w, t)| (w.clone(), t.clone()))
                        .collect();
                let else_expr: Option<Arc<dyn PhysicalExpr>> = if let Some(e) = else_expr
                {
                    Some(self.create_physical_expr(
                        e.as_ref(),
                        input_dfschema,
                        input_schema,
                        ctx_state,
                    )?)
                } else {
                    None
                };
                Ok(Arc::new(CaseExpr::try_new(
                    expr,
                    &when_then_expr,
                    else_expr,
                )?))
            }
            Expr::Cast { expr, data_type } => expressions::cast(
                self.create_physical_expr(expr, input_dfschema, input_schema, ctx_state)?,
                input_schema,
                data_type.clone(),
            ),
            Expr::TryCast { expr, data_type } => expressions::try_cast(
                self.create_physical_expr(expr, input_dfschema, input_schema, ctx_state)?,
                input_schema,
                data_type.clone(),
            ),
            Expr::Not(expr) => expressions::not(
                self.create_physical_expr(expr, input_dfschema, input_schema, ctx_state)?,
                input_schema,
            ),
            Expr::Negative(expr) => expressions::negative(
                self.create_physical_expr(expr, input_dfschema, input_schema, ctx_state)?,
                input_schema,
            ),
            Expr::IsNull(expr) => expressions::is_null(self.create_physical_expr(
                expr,
                input_dfschema,
                input_schema,
                ctx_state,
            )?),
            Expr::IsNotNull(expr) => expressions::is_not_null(
                self.create_physical_expr(expr, input_dfschema, input_schema, ctx_state)?,
            ),
            Expr::ScalarFunction { fun, args } => {
                let physical_args = args
                    .iter()
                    .map(|e| {
                        self.create_physical_expr(
                            e,
                            input_dfschema,
                            input_schema,
                            ctx_state,
                        )
                    })
                    .collect::<Result<Vec<_>>>()?;
                functions::create_physical_expr(
                    fun,
                    &physical_args,
                    input_schema,
                    ctx_state,
                )
            }
            Expr::ScalarUDF { fun, args } => {
                let mut physical_args = vec![];
                for e in args {
                    physical_args.push(self.create_physical_expr(
                        e,
                        input_dfschema,
                        input_schema,
                        ctx_state,
                    )?);
                }

                udf::create_physical_expr(
                    fun.clone().as_ref(),
                    &physical_args,
                    input_schema,
                )
            }
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => {
                let value_expr = self.create_physical_expr(
                    expr,
                    input_dfschema,
                    input_schema,
                    ctx_state,
                )?;
                let low_expr = self.create_physical_expr(
                    low,
                    input_dfschema,
                    input_schema,
                    ctx_state,
                )?;
                let high_expr = self.create_physical_expr(
                    high,
                    input_dfschema,
                    input_schema,
                    ctx_state,
                )?;

                // rewrite the between into the two binary operators
                let binary_expr = binary(
                    binary(value_expr.clone(), Operator::GtEq, low_expr, input_schema)?,
                    Operator::And,
                    binary(value_expr.clone(), Operator::LtEq, high_expr, input_schema)?,
                    input_schema,
                );

                if *negated {
                    expressions::not(binary_expr?, input_schema)
                } else {
                    binary_expr
                }
            }
            Expr::InList {
                expr,
                list,
                negated,
            } => match expr.as_ref() {
                Expr::Literal(ScalarValue::Utf8(None)) => {
                    Ok(expressions::lit(ScalarValue::Boolean(None)))
                }
                _ => {
                    let value_expr = self.create_physical_expr(
                        expr,
                        input_dfschema,
                        input_schema,
                        ctx_state,
                    )?;
                    let value_expr_data_type = value_expr.data_type(input_schema)?;

                    let list_exprs = list
                        .iter()
                        .map(|expr| match expr {
                            Expr::Literal(ScalarValue::Utf8(None)) => self
                                .create_physical_expr(
                                    expr,
                                    input_dfschema,
                                    input_schema,
                                    ctx_state,
                                ),
                            _ => {
                                let list_expr = self.create_physical_expr(
                                    expr,
                                    input_dfschema,
                                    input_schema,
                                    ctx_state,
                                )?;
                                let list_expr_data_type =
                                    list_expr.data_type(input_schema)?;

                                if list_expr_data_type == value_expr_data_type {
                                    Ok(list_expr)
                                } else if can_cast_types(
                                    &list_expr_data_type,
                                    &value_expr_data_type,
                                ) {
                                    expressions::cast(
                                        list_expr,
                                        input_schema,
                                        value_expr.data_type(input_schema)?,
                                    )
                                } else {
                                    Err(DataFusionError::Plan(format!(
                                        "Unsupported CAST from {:?} to {:?}",
                                        list_expr_data_type, value_expr_data_type
                                    )))
                                }
                            }
                        })
                        .collect::<Result<Vec<_>>>()?;

                    expressions::in_list(value_expr, list_exprs, negated)
                }
            },
            other => Err(DataFusionError::NotImplemented(format!(
                "Physical plan does not support logical expression {:?}",
                other
            ))),
        }
    }

    /// Create a window expression with a name from a logical expression
    pub fn create_window_expr_with_name(
        &self,
        e: &Expr,
        name: impl Into<String>,
        logical_input_schema: &DFSchema,
        physical_input_schema: &Schema,
        ctx_state: &ExecutionContextState,
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
                        self.create_physical_expr(
                            e,
                            logical_input_schema,
                            physical_input_schema,
                            ctx_state,
                        )
                    })
                    .collect::<Result<Vec<_>>>()?;
                let partition_by = partition_by
                    .iter()
                    .map(|e| {
                        self.create_physical_expr(
                            e,
                            logical_input_schema,
                            physical_input_schema,
                            ctx_state,
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
                        } => self.create_physical_sort_expr(
                            expr,
                            logical_input_schema,
                            physical_input_schema,
                            SortOptions {
                                descending: !*asc,
                                nulls_first: *nulls_first,
                            },
                            ctx_state,
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
        &self,
        e: &Expr,
        logical_input_schema: &DFSchema,
        physical_input_schema: &Schema,
        ctx_state: &ExecutionContextState,
    ) -> Result<Arc<dyn WindowExpr>> {
        // unpack aliased logical expressions, e.g. "sum(col) over () as total"
        let (name, e) = match e {
            Expr::Alias(sub_expr, alias) => (alias.clone(), sub_expr.as_ref()),
            _ => (physical_name(e, logical_input_schema)?, e),
        };
        self.create_window_expr_with_name(
            e,
            name,
            logical_input_schema,
            physical_input_schema,
            ctx_state,
        )
    }

    /// Create an aggregate expression with a name from a logical expression
    pub fn create_aggregate_expr_with_name(
        &self,
        e: &Expr,
        name: impl Into<String>,
        logical_input_schema: &DFSchema,
        physical_input_schema: &Schema,
        ctx_state: &ExecutionContextState,
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
                        self.create_physical_expr(
                            e,
                            logical_input_schema,
                            physical_input_schema,
                            ctx_state,
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
                        self.create_physical_expr(
                            e,
                            logical_input_schema,
                            physical_input_schema,
                            ctx_state,
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
        &self,
        e: &Expr,
        logical_input_schema: &DFSchema,
        physical_input_schema: &Schema,
        ctx_state: &ExecutionContextState,
    ) -> Result<Arc<dyn AggregateExpr>> {
        // unpack aliased logical expressions, e.g. "sum(col) as total"
        let (name, e) = match e {
            Expr::Alias(sub_expr, alias) => (alias.clone(), sub_expr.as_ref()),
            _ => (physical_name(e, logical_input_schema)?, e),
        };

        self.create_aggregate_expr_with_name(
            e,
            name,
            logical_input_schema,
            physical_input_schema,
            ctx_state,
        )
    }

    /// Create a physical sort expression from a logical expression
    pub fn create_physical_sort_expr(
        &self,
        e: &Expr,
        input_dfschema: &DFSchema,
        input_schema: &Schema,
        options: SortOptions,
        ctx_state: &ExecutionContextState,
    ) -> Result<PhysicalSortExpr> {
        Ok(PhysicalSortExpr {
            expr: self.create_physical_expr(
                e,
                input_dfschema,
                input_schema,
                ctx_state,
            )?,
            options,
        })
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
    use crate::logical_plan::{DFField, DFSchema, DFSchemaRef};
    use crate::physical_plan::{csv::CsvReadOptions, expressions, Partitioning};
    use crate::scalar::ScalarValue;
    use crate::{
        logical_plan::{col, lit, sum, LogicalPlanBuilder},
        physical_plan::SendableRecordBatchStream,
    };
    use arrow::datatypes::{DataType, Field, SchemaRef};
    use async_trait::async_trait;
    use fmt::Debug;
    use std::convert::TryFrom;
    use std::{any::Any, fmt};

    fn make_ctx_state() -> ExecutionContextState {
        ExecutionContextState::new()
    }

    fn plan(logical_plan: &LogicalPlan) -> Result<Arc<dyn ExecutionPlan>> {
        let mut ctx_state = make_ctx_state();
        ctx_state.config.concurrency = 4;
        let planner = DefaultPhysicalPlanner::default();
        planner.create_physical_plan(logical_plan, &ctx_state)
    }

    #[test]
    fn test_all_operators() -> Result<()> {
        let testdata = crate::test_util::arrow_test_data();
        let path = format!("{}/csv/aggregate_test_100.csv", testdata);

        let options = CsvReadOptions::new().schema_infer_max_records(100);
        let logical_plan = LogicalPlanBuilder::scan_csv(path, options, None)?
            // filter clause needs the type coercion rule applied
            .filter(col("c7").lt(lit(5_u8)))?
            .project(vec![col("c1"), col("c2")])?
            .aggregate(vec![col("c1")], vec![sum(col("c2"))])?
            .sort(vec![col("c1").sort(true, true)])?
            .limit(10)?
            .build()?;

        let plan = plan(&logical_plan)?;

        // verify that the plan correctly casts u8 to i64
        // the cast here is implicit so has CastOptions with safe=true
        let expected = "BinaryExpr { left: Column { name: \"c7\", index: 6 }, op: Lt, right: TryCastExpr { expr: Literal { value: UInt8(5) }, cast_type: Int64 } }";
        assert!(format!("{:?}", plan).contains(expected));

        Ok(())
    }

    #[test]
    fn test_create_not() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Boolean, true)]);
        let dfschema = DFSchema::try_from(schema.clone())?;

        let planner = DefaultPhysicalPlanner::default();

        let expr = planner.create_physical_expr(
            &col("a").not(),
            &dfschema,
            &schema,
            &make_ctx_state(),
        )?;
        let expected = expressions::not(expressions::col("a", &schema)?, &schema)?;

        assert_eq!(format!("{:?}", expr), format!("{:?}", expected));

        Ok(())
    }

    #[test]
    fn test_with_csv_plan() -> Result<()> {
        let testdata = crate::test_util::arrow_test_data();
        let path = format!("{}/csv/aggregate_test_100.csv", testdata);

        let options = CsvReadOptions::new().schema_infer_max_records(100);
        let logical_plan = LogicalPlanBuilder::scan_csv(path, options, None)?
            .filter(col("c7").lt(col("c12")))?
            .build()?;

        let plan = plan(&logical_plan)?;

        // c12 is f64, c7 is u8 -> cast c7 to f64
        // the cast here is implicit so has CastOptions with safe=true
        let expected = "predicate: BinaryExpr { left: TryCastExpr { expr: Column { name: \"c7\", index: 6 }, cast_type: Float64 }, op: Lt, right: Column { name: \"c12\", index: 11 } }";
        assert!(format!("{:?}", plan).contains(expected));
        Ok(())
    }

    #[test]
    fn errors() -> Result<()> {
        let testdata = crate::test_util::arrow_test_data();
        let path = format!("{}/csv/aggregate_test_100.csv", testdata);
        let options = CsvReadOptions::new().schema_infer_max_records(100);

        let bool_expr = col("c1").eq(col("c1"));
        let cases = vec![
            // utf8 < u32
            col("c1").lt(col("c2")),
            // utf8 AND utf8
            col("c1").and(col("c1")),
            // u8 AND u8
            col("c3").and(col("c3")),
            // utf8 = u32
            col("c1").eq(col("c2")),
            // utf8 = bool
            col("c1").eq(bool_expr.clone()),
            // u32 AND bool
            col("c2").and(bool_expr),
            // utf8 LIKE u32
            col("c1").like(col("c2")),
        ];
        for case in cases {
            let logical_plan = LogicalPlanBuilder::scan_csv(&path, options, None)?
                .project(vec![case.clone()]);
            let message = format!(
                "Expression {:?} expected to error due to impossible coercion",
                case
            );
            assert!(logical_plan.is_err(), "{}", message);
        }
        Ok(())
    }

    #[test]
    fn default_extension_planner() {
        let ctx_state = make_ctx_state();
        let planner = DefaultPhysicalPlanner::default();
        let logical_plan = LogicalPlan::Extension {
            node: Arc::new(NoOpExtensionNode::default()),
        };
        let plan = planner.create_physical_plan(&logical_plan, &ctx_state);

        let expected_error =
            "No installed planner was able to convert the custom node to an execution plan: NoOp";
        match plan {
            Ok(_) => panic!("Expected planning failure"),
            Err(e) => assert!(
                e.to_string().contains(expected_error),
                "Error '{}' did not contain expected error '{}'",
                e.to_string(),
                expected_error
            ),
        }
    }

    #[test]
    fn bad_extension_planner() {
        // Test that creating an execution plan whose schema doesn't
        // match the logical plan's schema generates an error.
        let ctx_state = make_ctx_state();
        let planner = DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(
            BadExtensionPlanner {},
        )]);

        let logical_plan = LogicalPlan::Extension {
            node: Arc::new(NoOpExtensionNode::default()),
        };
        let plan = planner.create_physical_plan(&logical_plan, &ctx_state);

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
        ] }, \
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
                e.to_string(),
                expected_error
            ),
        }
    }

    #[test]
    fn in_list_types() -> Result<()> {
        let testdata = crate::test_util::arrow_test_data();
        let path = format!("{}/csv/aggregate_test_100.csv", testdata);
        let options = CsvReadOptions::new().schema_infer_max_records(100);

        // expression: "a in ('a', 1)"
        let list = vec![
            Expr::Literal(ScalarValue::Utf8(Some("a".to_string()))),
            Expr::Literal(ScalarValue::Int64(Some(1))),
        ];
        let logical_plan = LogicalPlanBuilder::scan_csv(&path, options, None)?
            // filter clause needs the type coercion rule applied
            .filter(col("c12").lt(lit(0.05)))?
            .project(vec![col("c1").in_list(list, false)])?
            .build()?;
        let execution_plan = plan(&logical_plan)?;
        // verify that the plan correctly adds cast from Int64(1) to Utf8
        let expected = "InListExpr { expr: Column { name: \"c1\", index: 0 }, list: [Literal { value: Utf8(\"a\") }, CastExpr { expr: Literal { value: Int64(1) }, cast_type: Utf8, cast_options: CastOptions { safe: false } }], negated: false }";
        assert!(format!("{:?}", execution_plan).contains(expected));

        // expression: "a in (true, 'a')"
        let list = vec![
            Expr::Literal(ScalarValue::Boolean(Some(true))),
            Expr::Literal(ScalarValue::Utf8(Some("a".to_string()))),
        ];
        let logical_plan = LogicalPlanBuilder::scan_csv(path, options, None)?
            // filter clause needs the type coercion rule applied
            .filter(col("c12").lt(lit(0.05)))?
            .project(vec![col("c12").lt_eq(lit(0.025)).in_list(list, false)])?
            .build()?;
        let execution_plan = plan(&logical_plan);

        let expected_error = "Unsupported CAST from Utf8 to Boolean";
        match execution_plan {
            Ok(_) => panic!("Expected planning failure"),
            Err(e) => assert!(
                e.to_string().contains(expected_error),
                "Error '{}' did not contain expected error '{}'",
                e.to_string(),
                expected_error
            ),
        }

        Ok(())
    }

    #[test]
    fn hash_agg_input_schema() -> Result<()> {
        let testdata = crate::test_util::arrow_test_data();
        let path = format!("{}/csv/aggregate_test_100.csv", testdata);

        let options = CsvReadOptions::new().schema_infer_max_records(100);
        let logical_plan = LogicalPlanBuilder::scan_csv(path, options, None)?
            .aggregate(vec![col("c1")], vec![sum(col("c2"))])?
            .build()?;

        let execution_plan = plan(&logical_plan)?;
        let final_hash_agg = execution_plan
            .as_any()
            .downcast_ref::<HashAggregateExec>()
            .expect("hash aggregate");
        assert_eq!("SUM(c2)", final_hash_agg.schema().field(1).name());
        // we need access to the input to the partial aggregate so that other projects can
        // implement serde
        assert_eq!("c2", final_hash_agg.input_schema().field(1).name());

        Ok(())
    }

    #[test]
    fn hash_agg_group_by_partitioned() -> Result<()> {
        let testdata = crate::test_util::arrow_test_data();
        let path = format!("{}/csv/aggregate_test_100.csv", testdata);

        let options = CsvReadOptions::new().schema_infer_max_records(100);
        let logical_plan = LogicalPlanBuilder::scan_csv(path, options, None)?
            .aggregate(vec![col("c1")], vec![sum(col("c2"))])?
            .build()?;

        let execution_plan = plan(&logical_plan)?;
        let formatted = format!("{:?}", execution_plan);

        // Make sure the plan contains a FinalPartitioned, which means it will not use the Final
        // mode in HashAggregate (which is slower)
        assert!(formatted.contains("FinalPartitioned"));

        Ok(())
    }

    /// An example extension node that doesn't do anything
    struct NoOpExtensionNode {
        schema: DFSchemaRef,
    }

    impl Default for NoOpExtensionNode {
        fn default() -> Self {
            Self {
                schema: DFSchemaRef::new(
                    DFSchema::new(vec![DFField::new(None, "a", DataType::Int32, false)])
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
        ) -> Arc<dyn UserDefinedLogicalNode + Send + Sync> {
            unimplemented!("NoOp");
        }
    }

    #[derive(Debug)]
    struct NoOpExecutionPlan {
        schema: SchemaRef,
    }

    #[async_trait]
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

        fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
            vec![]
        }

        fn with_new_children(
            &self,
            _children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            unimplemented!("NoOpExecutionPlan::with_new_children");
        }

        async fn execute(&self, _partition: usize) -> Result<SendableRecordBatchStream> {
            unimplemented!("NoOpExecutionPlan::execute");
        }
    }

    //  Produces an execution plan where the schema is mismatched from
    //  the logical plan node.
    struct BadExtensionPlanner {}

    impl ExtensionPlanner for BadExtensionPlanner {
        /// Create a physical plan for an extension node
        fn plan_extension(
            &self,
            _planner: &dyn PhysicalPlanner,
            _node: &dyn UserDefinedLogicalNode,
            _logical_inputs: &[&LogicalPlan],
            _physical_inputs: &[Arc<dyn ExecutionPlan>],
            _ctx_state: &ExecutionContextState,
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
}
