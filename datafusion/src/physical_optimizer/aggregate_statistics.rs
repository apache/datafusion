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

//! Utilizing exact statistics from sources to avoid scanning data
use std::sync::Arc;

use arrow::datatypes::Schema;

use crate::execution::context::ExecutionConfig;
use crate::physical_plan::empty::EmptyExec;
use crate::physical_plan::hash_aggregate::{AggregateMode, HashAggregateExec};
use crate::physical_plan::projection::ProjectionExec;
use crate::physical_plan::{
    expressions, AggregateExpr, ColumnStatistics, ExecutionPlan, Statistics,
};
use crate::scalar::ScalarValue;

use super::optimizer::PhysicalOptimizerRule;
use super::utils::optimize_children;
use crate::error::Result;

/// Optimizer that uses available statistics for aggregate functions
pub struct AggregateStatistics {}

impl AggregateStatistics {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl PhysicalOptimizerRule for AggregateStatistics {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        execution_config: &ExecutionConfig,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if let Some(partial_agg_exec) = take_optimizable(&*plan) {
            let partial_agg_exec = partial_agg_exec
                .as_any()
                .downcast_ref::<HashAggregateExec>()
                .expect("take_optimizable() ensures that this is a HashAggregateExec");
            let stats = partial_agg_exec.input().statistics();
            let mut projections = vec![];
            for expr in partial_agg_exec.aggr_expr() {
                if let Some((num_rows, name)) = take_optimizable_count(&**expr, &stats) {
                    projections.push((expressions::lit(num_rows), name.to_owned()));
                } else if let Some((min, name)) = take_optimizable_min(&**expr, &stats) {
                    projections.push((expressions::lit(min), name.to_owned()));
                } else if let Some((max, name)) = take_optimizable_max(&**expr, &stats) {
                    projections.push((expressions::lit(max), name.to_owned()));
                } else {
                    // TODO: we need all aggr_expr to be resolved (cf TODO fullres)
                    break;
                }
            }

            // TODO fullres: use statistics even if not all aggr_expr could be resolved
            if projections.len() == partial_agg_exec.aggr_expr().len() {
                // input can be entirely removed
                Ok(Arc::new(ProjectionExec::try_new(
                    projections,
                    Arc::new(EmptyExec::new(true, Arc::new(Schema::empty()))),
                )?))
            } else {
                optimize_children(self, plan, execution_config)
            }
        } else {
            optimize_children(self, plan, execution_config)
        }
    }

    fn name(&self) -> &str {
        "aggregate_statistics"
    }
}

/// assert if the node passed as argument is a final `HashAggregateExec` node that can be optimized:
/// - its child (with posssible intermediate layers) is a partial `HashAggregateExec` node
/// - they both have no grouping expression
/// - the statistics are exact
/// If this is the case, return a ref to the partial `HashAggregateExec`, else `None`.
/// We would have prefered to return a casted ref to HashAggregateExec but the recursion requires
/// the `ExecutionPlan.children()` method that returns an owned reference.
fn take_optimizable(node: &dyn ExecutionPlan) -> Option<Arc<dyn ExecutionPlan>> {
    if let Some(final_agg_exec) = node.as_any().downcast_ref::<HashAggregateExec>() {
        if final_agg_exec.mode() == &AggregateMode::Final
            && final_agg_exec.group_expr().is_empty()
        {
            let mut child = Arc::clone(final_agg_exec.input());
            loop {
                if let Some(partial_agg_exec) =
                    child.as_any().downcast_ref::<HashAggregateExec>()
                {
                    if partial_agg_exec.mode() == &AggregateMode::Partial
                        && partial_agg_exec.group_expr().is_empty()
                    {
                        let stats = partial_agg_exec.input().statistics();
                        if stats.is_exact {
                            return Some(child);
                        }
                    }
                }
                if let [ref childrens_child] = child.children().as_slice() {
                    child = Arc::clone(childrens_child);
                } else {
                    break;
                }
            }
        }
    }
    None
}

/// If this agg_expr is a count that is defined in the statistics, return it
fn take_optimizable_count(
    agg_expr: &dyn AggregateExpr,
    stats: &Statistics,
) -> Option<(ScalarValue, &'static str)> {
    if let (Some(num_rows), Some(casted_expr)) = (
        stats.num_rows,
        agg_expr.as_any().downcast_ref::<expressions::Count>(),
    ) {
        // TODO implementing Eq on PhysicalExpr would help a lot here
        if casted_expr.expressions().len() == 1 {
            if let Some(lit_expr) = casted_expr.expressions()[0]
                .as_any()
                .downcast_ref::<expressions::Literal>()
            {
                if lit_expr.value() == &ScalarValue::UInt8(Some(1)) {
                    return Some((
                        ScalarValue::UInt64(Some(num_rows as u64)),
                        "COUNT(Uint8(1))",
                    ));
                }
            }
        }
    }
    None
}

/// If this agg_expr is a min that is defined in the statistics, return it
fn take_optimizable_min(
    agg_expr: &dyn AggregateExpr,
    stats: &Statistics,
) -> Option<(ScalarValue, String)> {
    if let (Some(col_stats), Some(casted_expr)) = (
        &stats.column_statistics,
        agg_expr.as_any().downcast_ref::<expressions::Min>(),
    ) {
        if casted_expr.expressions().len() == 1 {
            // TODO optimize with exprs other than Column
            if let Some(col_expr) = casted_expr.expressions()[0]
                .as_any()
                .downcast_ref::<expressions::Column>()
            {
                if let ColumnStatistics {
                    min_value: Some(val),
                    ..
                } = &col_stats[col_expr.index()]
                {
                    return Some((val.clone(), format!("MIN({})", col_expr.name())));
                }
            }
        }
    }
    None
}

/// If this agg_expr is a max that is defined in the statistics, return it
fn take_optimizable_max(
    agg_expr: &dyn AggregateExpr,
    stats: &Statistics,
) -> Option<(ScalarValue, String)> {
    if let (Some(col_stats), Some(casted_expr)) = (
        &stats.column_statistics,
        agg_expr.as_any().downcast_ref::<expressions::Max>(),
    ) {
        if casted_expr.expressions().len() == 1 {
            // TODO optimize with exprs other than Column
            if let Some(col_expr) = casted_expr.expressions()[0]
                .as_any()
                .downcast_ref::<expressions::Column>()
            {
                if let ColumnStatistics {
                    max_value: Some(val),
                    ..
                } = &col_stats[col_expr.index()]
                {
                    return Some((val.clone(), format!("MAX({})", col_expr.name())));
                }
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow::array::{Int32Array, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use crate::error::Result;
    use crate::logical_plan::Operator;
    use crate::physical_plan::coalesce_partitions::CoalescePartitionsExec;
    use crate::physical_plan::common;
    use crate::physical_plan::expressions::Count;
    use crate::physical_plan::filter::FilterExec;
    use crate::physical_plan::hash_aggregate::HashAggregateExec;
    use crate::physical_plan::memory::MemoryExec;

    /// Mock data using a MemoryExec which has an exact count statistic
    fn mock_data() -> Result<Arc<MemoryExec>> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![4, 5, 6])),
            ],
        )?;

        Ok(Arc::new(MemoryExec::try_new(
            &[vec![batch]],
            Arc::clone(&schema),
            None,
        )?))
    }

    /// Checks that the count optimization was applied and we still get the right result
    async fn assert_count_optim_success(plan: HashAggregateExec) -> Result<()> {
        let conf = ExecutionConfig::new();
        let optimized = AggregateStatistics::new().optimize(Arc::new(plan), &conf)?;

        assert!(optimized.as_any().is::<ProjectionExec>());
        let result = common::collect(optimized.execute(0).await?).await?;
        assert_eq!(
            result[0].schema(),
            Arc::new(Schema::new(vec![Field::new(
                "COUNT(Uint8(1))",
                DataType::UInt64,
                false
            )]))
        );
        assert_eq!(
            result[0]
                .column(0)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .values(),
            &[3]
        );
        Ok(())
    }

    fn count_expr() -> Arc<dyn AggregateExpr> {
        Arc::new(Count::new(
            expressions::lit(ScalarValue::UInt8(Some(1))),
            "my_count_alias",
            DataType::UInt64,
        ))
    }

    #[tokio::test]
    async fn test_count_partial_direct_child() -> Result<()> {
        // basic test case with the aggregation applied on a source with exact statistics
        let source = mock_data()?;
        let schema = source.schema();

        let partial_agg = HashAggregateExec::try_new(
            AggregateMode::Partial,
            vec![],
            vec![count_expr()],
            source,
            Arc::clone(&schema),
        )?;

        let final_agg = HashAggregateExec::try_new(
            AggregateMode::Final,
            vec![],
            vec![count_expr()],
            Arc::new(partial_agg),
            Arc::clone(&schema),
        )?;

        assert_count_optim_success(final_agg).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_count_partial_indirect_child() -> Result<()> {
        let source = mock_data()?;
        let schema = source.schema();

        let partial_agg = HashAggregateExec::try_new(
            AggregateMode::Partial,
            vec![],
            vec![count_expr()],
            source,
            Arc::clone(&schema),
        )?;

        // We introduce an intermediate optimization step between the partial and final aggregtator
        let coalesce = CoalescePartitionsExec::new(Arc::new(partial_agg));

        let final_agg = HashAggregateExec::try_new(
            AggregateMode::Final,
            vec![],
            vec![count_expr()],
            Arc::new(coalesce),
            Arc::clone(&schema),
        )?;

        assert_count_optim_success(final_agg).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_count_inexact_stat() -> Result<()> {
        let source = mock_data()?;
        let schema = source.schema();

        // adding a filter makes the statistics inexact
        let filter = Arc::new(FilterExec::try_new(
            expressions::binary(
                expressions::col("a", &schema)?,
                Operator::Gt,
                expressions::lit(ScalarValue::from(1u32)),
                &schema,
            )?,
            source,
        )?);

        let partial_agg = HashAggregateExec::try_new(
            AggregateMode::Partial,
            vec![],
            vec![count_expr()],
            filter,
            Arc::clone(&schema),
        )?;

        let final_agg = HashAggregateExec::try_new(
            AggregateMode::Final,
            vec![],
            vec![count_expr()],
            Arc::new(partial_agg),
            Arc::clone(&schema),
        )?;

        let conf = ExecutionConfig::new();
        let optimized =
            AggregateStatistics::new().optimize(Arc::new(final_agg), &conf)?;

        // check that the original ExecutionPlan was not replaced
        assert!(optimized.as_any().is::<HashAggregateExec>());

        Ok(())
    }
}
