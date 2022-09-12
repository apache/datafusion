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
use datafusion_expr::utils::COUNT_STAR_EXPANSION;

use crate::execution::context::SessionConfig;
use crate::physical_plan::aggregates::{AggregateExec, AggregateMode};
use crate::physical_plan::empty::EmptyExec;
use crate::physical_plan::projection::ProjectionExec;
use crate::physical_plan::{
    expressions, AggregateExpr, ColumnStatistics, ExecutionPlan, Statistics,
};
use crate::scalar::ScalarValue;

use super::optimizer::PhysicalOptimizerRule;
use super::utils::optimize_children;
use crate::error::Result;

/// Optimizer that uses available statistics for aggregate functions
#[derive(Default)]
pub struct AggregateStatistics {}

/// The name of the column corresponding to [`COUNT_STAR_EXPANSION`]
const COUNT_STAR_NAME: &str = "COUNT(UInt8(1))";

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
        config: &SessionConfig,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if let Some(partial_agg_exec) = take_optimizable(&*plan) {
            let partial_agg_exec = partial_agg_exec
                .as_any()
                .downcast_ref::<AggregateExec>()
                .expect("take_optimizable() ensures that this is a AggregateExec");
            let stats = partial_agg_exec.input().statistics();
            let mut projections = vec![];
            for expr in partial_agg_exec.aggr_expr() {
                if let Some((non_null_rows, name)) =
                    take_optimizable_column_count(&**expr, &stats)
                {
                    projections.push((expressions::lit(non_null_rows), name.to_owned()));
                } else if let Some((num_rows, name)) =
                    take_optimizable_table_count(&**expr, &stats)
                {
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
                optimize_children(self, plan, config)
            }
        } else {
            optimize_children(self, plan, config)
        }
    }

    fn name(&self) -> &str {
        "aggregate_statistics"
    }
}

/// assert if the node passed as argument is a final `AggregateExec` node that can be optimized:
/// - its child (with possible intermediate layers) is a partial `AggregateExec` node
/// - they both have no grouping expression
/// - the statistics are exact
/// If this is the case, return a ref to the partial `AggregateExec`, else `None`.
/// We would have preferred to return a casted ref to AggregateExec but the recursion requires
/// the `ExecutionPlan.children()` method that returns an owned reference.
fn take_optimizable(node: &dyn ExecutionPlan) -> Option<Arc<dyn ExecutionPlan>> {
    if let Some(final_agg_exec) = node.as_any().downcast_ref::<AggregateExec>() {
        if final_agg_exec.mode() == &AggregateMode::Final
            && final_agg_exec.group_expr().is_empty()
        {
            let mut child = Arc::clone(final_agg_exec.input());
            loop {
                if let Some(partial_agg_exec) =
                    child.as_any().downcast_ref::<AggregateExec>()
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
fn take_optimizable_table_count(
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
                if lit_expr.value() == &COUNT_STAR_EXPANSION {
                    return Some((
                        ScalarValue::Int64(Some(num_rows as i64)),
                        COUNT_STAR_NAME,
                    ));
                }
            }
        }
    }
    None
}

/// If this agg_expr is a count that can be derived from the statistics, return it
fn take_optimizable_column_count(
    agg_expr: &dyn AggregateExpr,
    stats: &Statistics,
) -> Option<(ScalarValue, String)> {
    if let (Some(num_rows), Some(col_stats), Some(casted_expr)) = (
        stats.num_rows,
        &stats.column_statistics,
        agg_expr.as_any().downcast_ref::<expressions::Count>(),
    ) {
        if casted_expr.expressions().len() == 1 {
            // TODO optimize with exprs other than Column
            if let Some(col_expr) = casted_expr.expressions()[0]
                .as_any()
                .downcast_ref::<expressions::Column>()
            {
                if let ColumnStatistics {
                    null_count: Some(val),
                    ..
                } = &col_stats[col_expr.index()]
                {
                    let expr = format!("COUNT({})", col_expr.name());
                    return Some((
                        ScalarValue::Int64(Some((num_rows - val) as i64)),
                        expr,
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

    use arrow::array::{Int32Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion_physical_expr::PhysicalExpr;

    use crate::error::Result;
    use crate::logical_plan::Operator;
    use crate::physical_plan::aggregates::{AggregateExec, PhysicalGroupBy};
    use crate::physical_plan::coalesce_partitions::CoalescePartitionsExec;
    use crate::physical_plan::common;
    use crate::physical_plan::expressions::Count;
    use crate::physical_plan::filter::FilterExec;
    use crate::physical_plan::memory::MemoryExec;
    use crate::prelude::SessionContext;

    /// Mock data using a MemoryExec which has an exact count statistic
    fn mock_data() -> Result<Arc<MemoryExec>> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
        ]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![Some(1), Some(2), None])),
                Arc::new(Int32Array::from(vec![Some(4), None, Some(6)])),
            ],
        )?;

        Ok(Arc::new(MemoryExec::try_new(
            &[vec![batch]],
            Arc::clone(&schema),
            None,
        )?))
    }

    /// Checks that the count optimization was applied and we still get the right result
    async fn assert_count_optim_success(
        plan: AggregateExec,
        agg: TestAggregate,
    ) -> Result<()> {
        let session_ctx = SessionContext::new();
        let conf = session_ctx.copied_config();
        let plan = Arc::new(plan) as _;
        let optimized = AggregateStatistics::new().optimize(Arc::clone(&plan), &conf)?;

        // A ProjectionExec is a sign that the count optimization was applied
        assert!(optimized.as_any().is::<ProjectionExec>());

        // run both the optimized and nonoptimized plan
        let optimized_result =
            common::collect(optimized.execute(0, session_ctx.task_ctx())?).await?;
        let nonoptimized_result =
            common::collect(plan.execute(0, session_ctx.task_ctx())?).await?;
        assert_eq!(optimized_result.len(), nonoptimized_result.len());

        //  and validate the results are the same and expected
        assert_eq!(optimized_result.len(), 1);
        check_batch(optimized_result.into_iter().next().unwrap(), &agg);
        // check the non optimized one too to ensure types and names remain the same
        assert_eq!(nonoptimized_result.len(), 1);
        check_batch(nonoptimized_result.into_iter().next().unwrap(), &agg);

        Ok(())
    }

    fn check_batch(batch: RecordBatch, agg: &TestAggregate) {
        let schema = batch.schema();
        let fields = schema.fields();
        assert_eq!(fields.len(), 1);

        let field = &fields[0];
        assert_eq!(field.name(), agg.column_name());
        assert_eq!(field.data_type(), &DataType::Int64);
        // note that nullabiolity differs

        assert_eq!(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &[agg.expected_count()]
        );
    }

    /// Describe the type of aggregate being tested
    enum TestAggregate {
        /// Testing COUNT(*) type aggregates
        CountStar,

        /// Testing for COUNT(column) aggregate
        ColumnA(Arc<Schema>),
    }

    impl TestAggregate {
        fn new_count_star() -> Self {
            Self::CountStar
        }

        fn new_count_column(schema: &Arc<Schema>) -> Self {
            Self::ColumnA(schema.clone())
        }

        /// Return appropriate expr depending if COUNT is for col or table (*)
        fn count_expr(&self) -> Arc<dyn AggregateExpr> {
            Arc::new(Count::new(
                self.column(),
                self.column_name(),
                DataType::Int64,
            ))
        }

        /// what argument would this aggregate need in the plan?
        fn column(&self) -> Arc<dyn PhysicalExpr> {
            match self {
                Self::CountStar => expressions::lit(COUNT_STAR_EXPANSION),
                Self::ColumnA(s) => expressions::col("a", s).unwrap(),
            }
        }

        /// What name would this aggregate produce in a plan?
        fn column_name(&self) -> &'static str {
            match self {
                Self::CountStar => COUNT_STAR_NAME,
                Self::ColumnA(_) => "COUNT(a)",
            }
        }

        /// What is the expected count?
        fn expected_count(&self) -> i64 {
            match self {
                TestAggregate::CountStar => 3,
                TestAggregate::ColumnA(_) => 2,
            }
        }
    }

    #[tokio::test]
    async fn test_count_partial_direct_child() -> Result<()> {
        // basic test case with the aggregation applied on a source with exact statistics
        let source = mock_data()?;
        let schema = source.schema();
        let agg = TestAggregate::new_count_star();

        let partial_agg = AggregateExec::try_new(
            AggregateMode::Partial,
            PhysicalGroupBy::default(),
            vec![agg.count_expr()],
            source,
            Arc::clone(&schema),
        )?;

        let final_agg = AggregateExec::try_new(
            AggregateMode::Final,
            PhysicalGroupBy::default(),
            vec![agg.count_expr()],
            Arc::new(partial_agg),
            Arc::clone(&schema),
        )?;

        assert_count_optim_success(final_agg, agg).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_count_partial_with_nulls_direct_child() -> Result<()> {
        // basic test case with the aggregation applied on a source with exact statistics
        let source = mock_data()?;
        let schema = source.schema();
        let agg = TestAggregate::new_count_column(&schema);

        let partial_agg = AggregateExec::try_new(
            AggregateMode::Partial,
            PhysicalGroupBy::default(),
            vec![agg.count_expr()],
            source,
            Arc::clone(&schema),
        )?;

        let final_agg = AggregateExec::try_new(
            AggregateMode::Final,
            PhysicalGroupBy::default(),
            vec![agg.count_expr()],
            Arc::new(partial_agg),
            Arc::clone(&schema),
        )?;

        assert_count_optim_success(final_agg, agg).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_count_partial_indirect_child() -> Result<()> {
        let source = mock_data()?;
        let schema = source.schema();
        let agg = TestAggregate::new_count_star();

        let partial_agg = AggregateExec::try_new(
            AggregateMode::Partial,
            PhysicalGroupBy::default(),
            vec![agg.count_expr()],
            source,
            Arc::clone(&schema),
        )?;

        // We introduce an intermediate optimization step between the partial and final aggregtator
        let coalesce = CoalescePartitionsExec::new(Arc::new(partial_agg));

        let final_agg = AggregateExec::try_new(
            AggregateMode::Final,
            PhysicalGroupBy::default(),
            vec![agg.count_expr()],
            Arc::new(coalesce),
            Arc::clone(&schema),
        )?;

        assert_count_optim_success(final_agg, agg).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_count_partial_with_nulls_indirect_child() -> Result<()> {
        let source = mock_data()?;
        let schema = source.schema();
        let agg = TestAggregate::new_count_column(&schema);

        let partial_agg = AggregateExec::try_new(
            AggregateMode::Partial,
            PhysicalGroupBy::default(),
            vec![agg.count_expr()],
            source,
            Arc::clone(&schema),
        )?;

        // We introduce an intermediate optimization step between the partial and final aggregtator
        let coalesce = CoalescePartitionsExec::new(Arc::new(partial_agg));

        let final_agg = AggregateExec::try_new(
            AggregateMode::Final,
            PhysicalGroupBy::default(),
            vec![agg.count_expr()],
            Arc::new(coalesce),
            Arc::clone(&schema),
        )?;

        assert_count_optim_success(final_agg, agg).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_count_inexact_stat() -> Result<()> {
        let source = mock_data()?;
        let schema = source.schema();
        let agg = TestAggregate::new_count_star();

        // adding a filter makes the statistics inexact
        let filter = Arc::new(FilterExec::try_new(
            expressions::binary(
                expressions::col("a", &schema)?,
                Operator::Gt,
                expressions::lit(1u32),
                &schema,
            )?,
            source,
        )?);

        let partial_agg = AggregateExec::try_new(
            AggregateMode::Partial,
            PhysicalGroupBy::default(),
            vec![agg.count_expr()],
            filter,
            Arc::clone(&schema),
        )?;

        let final_agg = AggregateExec::try_new(
            AggregateMode::Final,
            PhysicalGroupBy::default(),
            vec![agg.count_expr()],
            Arc::new(partial_agg),
            Arc::clone(&schema),
        )?;

        let conf = SessionConfig::new();
        let optimized =
            AggregateStatistics::new().optimize(Arc::new(final_agg), &conf)?;

        // check that the original ExecutionPlan was not replaced
        assert!(optimized.as_any().is::<AggregateExec>());

        Ok(())
    }

    #[tokio::test]
    async fn test_count_with_nulls_inexact_stat() -> Result<()> {
        let source = mock_data()?;
        let schema = source.schema();
        let agg = TestAggregate::new_count_column(&schema);

        // adding a filter makes the statistics inexact
        let filter = Arc::new(FilterExec::try_new(
            expressions::binary(
                expressions::col("a", &schema)?,
                Operator::Gt,
                expressions::lit(1u32),
                &schema,
            )?,
            source,
        )?);

        let partial_agg = AggregateExec::try_new(
            AggregateMode::Partial,
            PhysicalGroupBy::default(),
            vec![agg.count_expr()],
            filter,
            Arc::clone(&schema),
        )?;

        let final_agg = AggregateExec::try_new(
            AggregateMode::Final,
            PhysicalGroupBy::default(),
            vec![agg.count_expr()],
            Arc::new(partial_agg),
            Arc::clone(&schema),
        )?;

        let conf = SessionConfig::new();
        let optimized =
            AggregateStatistics::new().optimize(Arc::new(final_agg), &conf)?;

        // check that the original ExecutionPlan was not replaced
        assert!(optimized.as_any().is::<AggregateExec>());

        Ok(())
    }
}
