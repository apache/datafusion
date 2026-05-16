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

//! [`EagerAggregation`] pushes partial aggregations below joins when beneficial.
//!
//! This implements the "Eager Aggregation" optimization from Yan & Larson (VLDB 1995).
//! When a query aggregates columns from one side of a join (the "fact" side),
//! pre-aggregating by the join key before the join can dramatically reduce the
//! number of rows flowing into the join.

use std::sync::Arc;

use crate::PhysicalOptimizerRule;
use crate::optimizer::{ConfigOnlyContext, PhysicalOptimizerContext};

use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::{JoinType, NullEquality, Result};
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::aggregates::{
    AggregateExec, AggregateMode, PhysicalGroupBy,
};
use datafusion_physical_plan::joins::HashJoinExec;
use datafusion_physical_plan::operator_statistics::StatisticsRegistry;

const MIN_REDUCTION_RATIO: f64 = 4.0;
const MIN_DIMENSION_ROWS: usize = 4;

#[derive(Default, Debug)]
pub struct EagerAggregation;

impl EagerAggregation {
    pub fn new() -> Self {
        Self
    }
}

impl PhysicalOptimizerRule for EagerAggregation {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.optimize_with_context(plan, &ConfigOnlyContext::new(config))
    }

    fn optimize_with_context(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        context: &dyn PhysicalOptimizerContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let config = context.config_options();
        if !config.optimizer.eager_aggregation {
            return Ok(plan);
        }

        let mut default_registry = None;
        let registry: Option<&StatisticsRegistry> =
            if config.optimizer.use_statistics_registry {
                Some(context.statistics_registry().unwrap_or_else(|| {
                    default_registry
                        .insert(StatisticsRegistry::default_with_builtin_providers())
                }))
            } else {
                None
            };

        plan.transform_down(|node| try_eager_aggregation(node, registry))
            .data()
    }

    fn name(&self) -> &str {
        "EagerAggregation"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Determines which side of the join the aggregate columns reference.
#[derive(Debug, Clone, Copy, PartialEq)]
enum AggregateSide {
    Left,
    Right,
}

/// Attempt to apply eager aggregation on a plan node.
fn try_eager_aggregation(
    plan: Arc<dyn ExecutionPlan>,
    registry: Option<&StatisticsRegistry>,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    let Some(agg_exec) = plan.downcast_ref::<AggregateExec>() else {
        return Ok(Transformed::no(plan));
    };

    // Only apply to Single or SinglePartitioned mode (pre-distribution enforcement)
    match agg_exec.mode() {
        AggregateMode::Single | AggregateMode::SinglePartitioned => {}
        _ => return Ok(Transformed::no(plan)),
    }

    let Some(hash_join) = agg_exec.input().downcast_ref::<HashJoinExec>() else {
        return Ok(Transformed::no(plan));
    };

    // Only inner joins (avoids COUNT bug with outer joins)
    if *hash_join.join_type() != JoinType::Inner {
        return Ok(Transformed::no(plan));
    }

    // No residual filter (equi-join only)
    if hash_join.filter().is_some() {
        return Ok(Transformed::no(plan));
    }

    // No GROUPING SETS/ROLLUP/CUBE
    if agg_exec.group_expr().has_grouping_set() {
        return Ok(Transformed::no(plan));
    }

    // Must have at least one aggregate function
    if agg_exec.aggr_expr().is_empty() {
        return Ok(Transformed::no(plan));
    }

    // Validate all aggregates are eligible (no DISTINCT, no FILTER, no ORDER BY)
    for aggr in agg_exec.aggr_expr().iter() {
        if aggr.is_distinct() {
            return Ok(Transformed::no(plan));
        }
        if !aggr.order_bys().is_empty() {
            return Ok(Transformed::no(plan));
        }
    }
    for filter in agg_exec.filter_expr().iter() {
        if filter.is_some() {
            return Ok(Transformed::no(plan));
        }
    }

    // Determine the schema boundary between left and right join sides
    let left_schema = hash_join.left().schema();
    let left_col_count = left_schema.fields().len();

    // Determine which side all aggregate input columns reference
    let agg_side = match determine_aggregate_side(agg_exec, left_col_count)? {
        Some(side) => side,
        None => return Ok(Transformed::no(plan)),
    };

    // Verify join keys on the fact side are simple Column expressions
    let join_on = hash_join.on();
    for (left_key, right_key) in join_on.iter() {
        let fact_key = match agg_side {
            AggregateSide::Left => left_key,
            AggregateSide::Right => right_key,
        };
        if fact_key.downcast_ref::<Column>().is_none() {
            return Ok(Transformed::no(plan));
        }
    }

    // Cost-based decision
    if !passes_cost_heuristic(hash_join, agg_side, registry)? {
        return Ok(Transformed::no(plan));
    }

    // All checks pass — construct the eager aggregation plan
    construct_eager_plan(agg_exec, hash_join, agg_side, left_col_count)
}

/// Determine which side of the join all aggregate input columns reference.
/// Returns None if columns reference both sides (cannot push down).
fn determine_aggregate_side(
    agg_exec: &AggregateExec,
    left_col_count: usize,
) -> Result<Option<AggregateSide>> {
    let mut side: Option<AggregateSide> = None;

    // Check aggregate function input columns
    for aggr in agg_exec.aggr_expr().iter() {
        for expr in aggr.expressions() {
            if let Some(col) = expr.downcast_ref::<Column>() {
                let col_side = if col.index() < left_col_count {
                    AggregateSide::Left
                } else {
                    AggregateSide::Right
                };
                match side {
                    None => side = Some(col_side),
                    Some(s) if s != col_side => return Ok(None),
                    _ => {}
                }
            }
        }
    }

    // Check group-by columns: they can reference either side, but we need
    // at least the aggregate inputs to be on one side
    // Group-by cols from the dimension side are fine (they pass through the join)
    // Group-by cols from the fact side get included in the lower aggregate

    // If no aggregate references any column (e.g., COUNT(*)), default to the
    // side with more rows — we'll use the left side as default
    if side.is_none() {
        // For COUNT(*), pick the larger side. We need to check group-by columns
        // to see which side they reference. If group-by cols are from one side,
        // push the aggregate to the OTHER side (the fact side).
        for (expr, _) in agg_exec.group_expr().expr() {
            if let Some(col) = expr.downcast_ref::<Column>() {
                let col_side = if col.index() < left_col_count {
                    AggregateSide::Left
                } else {
                    AggregateSide::Right
                };
                // If group-by cols are on left, fact is right and vice versa
                if side.is_none() {
                    side = Some(match col_side {
                        AggregateSide::Left => AggregateSide::Right,
                        AggregateSide::Right => AggregateSide::Left,
                    });
                }
            }
        }
    }

    Ok(side)
}

/// Check cost heuristic: is the reduction ratio high enough to justify pre-aggregation?
fn passes_cost_heuristic(
    hash_join: &HashJoinExec,
    agg_side: AggregateSide,
    registry: Option<&StatisticsRegistry>,
) -> Result<bool> {
    let (fact_plan, dim_plan): (&dyn ExecutionPlan, &dyn ExecutionPlan) = match agg_side {
        AggregateSide::Left => (hash_join.left().as_ref(), hash_join.right().as_ref()),
        AggregateSide::Right => (hash_join.right().as_ref(), hash_join.left().as_ref()),
    };

    let fact_stats = get_stats(fact_plan, registry)?;
    let dim_stats = get_stats(dim_plan, registry)?;

    let fact_rows = match fact_stats.num_rows.get_value() {
        Some(&rows) if rows > 0 => rows,
        _ => return Ok(false), // Can't decide without stats
    };

    let dim_rows = match dim_stats.num_rows.get_value() {
        Some(&rows) if rows > 0 => rows,
        _ => return Ok(false),
    };

    if dim_rows < MIN_DIMENSION_ROWS {
        return Ok(false);
    }

    // Try NDV-based ratio first (more accurate)
    let join_on = hash_join.on();
    let fact_key_idx = match agg_side {
        AggregateSide::Left => join_on
            .first()
            .and_then(|(k, _)| k.downcast_ref::<Column>())
            .map(|c| c.index()),
        AggregateSide::Right => join_on
            .first()
            .and_then(|(_, k)| k.downcast_ref::<Column>())
            .map(|c| c.index()),
    };

    let ndv = fact_key_idx.and_then(|idx| {
        fact_stats
            .column_statistics
            .get(idx)
            .and_then(|cs| cs.distinct_count.get_value().copied())
    });

    let ratio = if let Some(ndv) = ndv {
        if ndv == 0 {
            return Ok(false);
        }
        fact_rows as f64 / ndv as f64
    } else {
        // Fallback: use dimension row count as proxy for NDV
        fact_rows as f64 / dim_rows as f64
    };

    Ok(ratio >= MIN_REDUCTION_RATIO)
}

fn get_stats(
    plan: &dyn ExecutionPlan,
    registry: Option<&StatisticsRegistry>,
) -> Result<Arc<datafusion_common::Statistics>> {
    if let Some(reg) = registry {
        reg.compute(plan).map(|s| Arc::clone(s.base_arc()))
    } else {
        plan.partition_statistics(None)
    }
}

/// Construct the eager aggregation plan:
/// Original: AggregateExec(mode=Single) -> HashJoinExec(left, right)
/// New: AggregateExec(mode=Final) -> HashJoinExec(lower_agg, dim) -> AggregateExec(mode=Partial, fact)
fn construct_eager_plan(
    agg_exec: &AggregateExec,
    hash_join: &HashJoinExec,
    agg_side: AggregateSide,
    left_col_count: usize,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    let join_on = hash_join.on();
    let fact_child = match agg_side {
        AggregateSide::Left => Arc::clone(hash_join.left()),
        AggregateSide::Right => Arc::clone(hash_join.right()),
    };
    let dim_child = match agg_side {
        AggregateSide::Left => Arc::clone(hash_join.right()),
        AggregateSide::Right => Arc::clone(hash_join.left()),
    };

    let fact_schema = fact_child.schema();
    let fact_col_offset = match agg_side {
        AggregateSide::Left => 0usize,
        AggregateSide::Right => left_col_count,
    };

    // Build group-by keys for the lower aggregate:
    // 1. Join key columns from the fact side
    // 2. Any original GROUP BY columns from the fact side
    let mut lower_group_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = Vec::new();
    let mut fact_join_key_indices: Vec<usize> = Vec::new();

    for (left_key, right_key) in join_on.iter() {
        let fact_key = match agg_side {
            AggregateSide::Left => left_key,
            AggregateSide::Right => right_key,
        };
        if let Some(col) = fact_key.downcast_ref::<Column>() {
            let local_idx = col.index() - fact_col_offset;
            let field_name = fact_schema.field(local_idx).name().clone();
            lower_group_exprs
                .push((Arc::new(Column::new(&field_name, local_idx)), field_name));
            fact_join_key_indices.push(local_idx);
        } else {
            return Ok(Transformed::no(
                Arc::new(agg_exec.clone()) as Arc<dyn ExecutionPlan>
            ));
        }
    }

    // Add original GROUP BY columns from fact side (not already included as join keys)
    let mut fact_group_col_positions: Vec<(usize, usize)> = Vec::new(); // (original_join_output_idx, lower_group_position)
    for (expr, alias) in agg_exec.group_expr().expr() {
        if let Some(col) = expr.downcast_ref::<Column>() {
            let idx = col.index();
            let is_fact_side = match agg_side {
                AggregateSide::Left => idx < left_col_count,
                AggregateSide::Right => idx >= left_col_count,
            };
            if is_fact_side {
                let local_idx = idx - fact_col_offset;
                // Check if already in join keys
                if !fact_join_key_indices.contains(&local_idx) {
                    let pos = lower_group_exprs.len();
                    let field_name = fact_schema.field(local_idx).name().clone();
                    lower_group_exprs.push((
                        Arc::new(Column::new(&field_name, local_idx)),
                        alias.clone(),
                    ));
                    fact_group_col_positions.push((idx, pos));
                }
            }
        }
    }

    // Remap aggregate function expressions to fact-side local schema
    let mut lower_aggr_exprs: Vec<
        Arc<datafusion_physical_expr::aggregate::AggregateFunctionExpr>,
    > = Vec::new();
    for aggr in agg_exec.aggr_expr().iter() {
        // We reuse the same AggregateFunctionExpr — the mode on AggregateExec
        // determines whether update_batch or merge_batch is called
        lower_aggr_exprs.push(Arc::clone(aggr));
    }

    // But we need to remap the column references inside the aggregate expressions
    // to point to the fact-side local schema. We'll need to rebuild them.
    let remapped_aggr_exprs = remap_aggregate_exprs_to_fact_side(
        agg_exec.aggr_expr(),
        fact_col_offset,
        &fact_schema,
    )?;

    // Create the lower (Partial) AggregateExec
    let lower_group_by = PhysicalGroupBy::new_single(lower_group_exprs.clone());
    let lower_filter_exprs = vec![None; remapped_aggr_exprs.len()];

    let lower_agg = AggregateExec::try_new(
        AggregateMode::Partial,
        lower_group_by,
        remapped_aggr_exprs.clone(),
        lower_filter_exprs,
        fact_child,
        fact_schema,
    )?;
    let lower_agg_arc: Arc<dyn ExecutionPlan> = Arc::new(lower_agg);
    let lower_agg_schema = lower_agg_arc.schema();

    // Build the new join with the lower aggregate replacing the fact side
    // Join keys must be remapped to lower aggregate output positions
    let new_join_on: Vec<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)> = join_on
        .iter()
        .enumerate()
        .map(|(i, (left_key, right_key))| {
            // The join key in the lower aggregate is at position i (we added them first)
            let lower_key_name = lower_agg_schema.field(i).name();
            let lower_col: Arc<dyn PhysicalExpr> =
                Arc::new(Column::new(lower_key_name, i));

            match agg_side {
                AggregateSide::Left => {
                    // fact is left, dim is right
                    (lower_col, Arc::clone(right_key))
                }
                AggregateSide::Right => {
                    // fact is right, dim is left
                    (Arc::clone(left_key), lower_col)
                }
            }
        })
        .collect();

    let (new_left, new_right) = match agg_side {
        AggregateSide::Left => (lower_agg_arc, dim_child),
        AggregateSide::Right => (dim_child, lower_agg_arc),
    };

    let new_join = HashJoinExec::try_new(
        Arc::clone(&new_left),
        Arc::clone(&new_right),
        new_join_on,
        None, // no residual filter
        &JoinType::Inner,
        None, // no projection
        *hash_join.partition_mode(),
        NullEquality::NullEqualsNothing,
        false,
    )?;
    let new_join_arc: Arc<dyn ExecutionPlan> = Arc::new(new_join);
    let new_join_schema = new_join_arc.schema();

    // Build the upper (Final) AggregateExec
    // Remap original group-by columns to new join output schema
    let new_left_col_count = new_left.schema().fields().len();
    let upper_group_exprs = build_upper_group_exprs(
        agg_exec,
        agg_side,
        left_col_count,
        new_left_col_count,
        &new_join_schema,
        &lower_group_exprs,
    )?;

    let upper_group_by = PhysicalGroupBy::new_single(upper_group_exprs);

    // For the upper aggregate in Final mode, we need aggregate expressions
    // that reference the state columns in the new join output.
    // The state columns from the lower Partial aggregate are in the lower_agg output
    // after the group-by columns.
    let upper_aggr_exprs = remap_aggregate_exprs_for_final(
        &remapped_aggr_exprs,
        agg_side,
        new_left_col_count,
        lower_group_exprs.len(),
        &new_join_schema,
    )?;

    let upper_filter_exprs = vec![None; upper_aggr_exprs.len()];
    let upper_mode = match agg_exec.mode() {
        AggregateMode::SinglePartitioned => AggregateMode::FinalPartitioned,
        _ => AggregateMode::Final,
    };

    let upper_agg = AggregateExec::try_new(
        upper_mode,
        upper_group_by,
        upper_aggr_exprs,
        upper_filter_exprs,
        new_join_arc,
        new_join_schema,
    )?;

    Ok(Transformed::yes(
        Arc::new(upper_agg) as Arc<dyn ExecutionPlan>
    ))
}

/// Remap aggregate expressions so their Column references point to the fact-side
/// local schema (subtracting the offset for right-side columns).
fn remap_aggregate_exprs_to_fact_side(
    aggr_exprs: &[Arc<datafusion_physical_expr::aggregate::AggregateFunctionExpr>],
    fact_col_offset: usize,
    fact_schema: &arrow::datatypes::SchemaRef,
) -> Result<Vec<Arc<datafusion_physical_expr::aggregate::AggregateFunctionExpr>>> {
    use datafusion_physical_expr::aggregate::AggregateExprBuilder;

    let mut result = Vec::with_capacity(aggr_exprs.len());
    for aggr in aggr_exprs.iter() {
        let new_args: Vec<Arc<dyn PhysicalExpr>> = aggr
            .expressions()
            .into_iter()
            .map(|expr| {
                if let Some(col) = expr.downcast_ref::<Column>() {
                    let new_idx = col.index() - fact_col_offset;
                    let field_name = fact_schema.field(new_idx).name();
                    Arc::new(Column::new(field_name, new_idx)) as Arc<dyn PhysicalExpr>
                } else {
                    expr
                }
            })
            .collect();

        let fun: Arc<datafusion_expr::AggregateUDF> = Arc::new(aggr.fun().clone());
        let builder = AggregateExprBuilder::new(fun, new_args)
            .schema(Arc::clone(fact_schema))
            .alias(aggr.name().to_string());

        result.push(builder.build().map(Arc::new)?);
    }
    Ok(result)
}

/// Build upper group-by expressions remapped to the new join output schema.
fn build_upper_group_exprs(
    agg_exec: &AggregateExec,
    agg_side: AggregateSide,
    original_left_col_count: usize,
    new_left_col_count: usize,
    new_join_schema: &arrow::datatypes::SchemaRef,
    lower_group_exprs: &[(Arc<dyn PhysicalExpr>, String)],
) -> Result<Vec<(Arc<dyn PhysicalExpr>, String)>> {
    let mut upper_exprs = Vec::new();

    for (expr, alias) in agg_exec.group_expr().expr() {
        if let Some(col) = expr.downcast_ref::<Column>() {
            let orig_idx = col.index();
            let is_fact_side = match agg_side {
                AggregateSide::Left => orig_idx < original_left_col_count,
                AggregateSide::Right => orig_idx >= original_left_col_count,
            };

            if is_fact_side {
                // This group column is from the fact side — find it in the lower
                // aggregate's group-by output
                let fact_local_idx = orig_idx
                    - match agg_side {
                        AggregateSide::Left => 0,
                        AggregateSide::Right => original_left_col_count,
                    };

                // Find this column in lower_group_exprs
                let lower_pos = lower_group_exprs.iter().position(|(e, _)| {
                    e.downcast_ref::<Column>()
                        .is_some_and(|c| c.index() == fact_local_idx)
                });

                if let Some(pos) = lower_pos {
                    // In the new join, the lower aggregate is on one side
                    let new_idx = match agg_side {
                        AggregateSide::Left => pos,
                        AggregateSide::Right => new_left_col_count + pos,
                    };
                    let field_name = new_join_schema.field(new_idx).name();
                    upper_exprs.push((
                        Arc::new(Column::new(field_name, new_idx)) as _,
                        alias.clone(),
                    ));
                } else {
                    return Ok(Vec::new()); // shouldn't happen
                }
            } else {
                // This group column is from the dimension side
                let dim_local_idx = match agg_side {
                    AggregateSide::Left => orig_idx - original_left_col_count,
                    AggregateSide::Right => orig_idx,
                };

                let new_idx = match agg_side {
                    AggregateSide::Left => new_left_col_count + dim_local_idx,
                    AggregateSide::Right => dim_local_idx,
                };
                let field_name = new_join_schema.field(new_idx).name();
                upper_exprs.push((
                    Arc::new(Column::new(field_name, new_idx)) as _,
                    alias.clone(),
                ));
            }
        } else {
            // Non-column group-by expression — bail out
            return Ok(Vec::new());
        }
    }

    Ok(upper_exprs)
}

/// Remap aggregate expressions for the Final mode above the join.
/// The Final aggregate reads state columns from the lower Partial aggregate's output,
/// which are now accessible through the join output schema.
fn remap_aggregate_exprs_for_final(
    lower_aggr_exprs: &[Arc<
        datafusion_physical_expr::aggregate::AggregateFunctionExpr,
    >],
    agg_side: AggregateSide,
    new_left_col_count: usize,
    num_lower_group_cols: usize,
    new_join_schema: &arrow::datatypes::SchemaRef,
) -> Result<Vec<Arc<datafusion_physical_expr::aggregate::AggregateFunctionExpr>>> {
    use datafusion_physical_expr::aggregate::AggregateExprBuilder;

    // In the new join output, state columns from the lower aggregate
    // start after the group-by columns on the fact side of the join.
    let fact_side_offset = match agg_side {
        AggregateSide::Left => 0,
        AggregateSide::Right => new_left_col_count,
    };

    let mut state_col_start = fact_side_offset + num_lower_group_cols;
    let mut result = Vec::with_capacity(lower_aggr_exprs.len());

    for aggr in lower_aggr_exprs.iter() {
        // Each aggregate in Partial mode outputs state_fields() columns
        let state_fields = aggr.state_fields()?;
        let num_state_cols = state_fields.len();

        // For the Final aggregate, we need to reference these state columns
        // The aggregate expression itself stays the same — only the column
        // references in its input change based on mode. But in DataFusion,
        // the AggregateFunctionExpr's expressions() return the original input
        // columns (used in Raw mode) and state_fields() defines what Final reads.
        //
        // When AggregateExec is in Final mode, it reads state columns starting
        // after the group-by columns in its input schema. The expressions()
        // on the AggregateFunctionExpr are not used in Final mode — the executor
        // reads state fields by position.
        //
        // So we just need to pass the same AggregateFunctionExpr, and the Final
        // mode executor will read columns at the correct offsets based on
        // the input schema structure.

        // Build a new expression pointing to the state columns in the join output
        let new_args: Vec<Arc<dyn PhysicalExpr>> = (0..num_state_cols)
            .map(|i| {
                let col_idx = state_col_start + i;
                let field_name = new_join_schema.field(col_idx).name();
                Arc::new(Column::new(field_name, col_idx)) as Arc<dyn PhysicalExpr>
            })
            .collect();

        state_col_start += num_state_cols;

        // For the upper (Final) aggregate, we reuse the same UDF but point
        // its input expressions to where the state lives in the join output.
        // However, in Final mode the executor ignores expressions() and uses
        // state_fields() positionally. We just need to pass the same expr.
        let fun: Arc<datafusion_expr::AggregateUDF> = Arc::new(aggr.fun().clone());
        let builder = AggregateExprBuilder::new(fun, new_args)
            .schema(Arc::clone(new_join_schema))
            .alias(aggr.name().to_string());

        result.push(builder.build().map(Arc::new)?);
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_eager_aggregation_rule_name() {
        let rule = EagerAggregation::new();
        assert_eq!(rule.name(), "EagerAggregation");
        assert!(rule.schema_check());
    }
}
