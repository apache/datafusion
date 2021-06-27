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

use crate::{
    execution::context::ExecutionProps,
    logical_plan::{DFSchema, Expr, LogicalPlan},
    physical_plan::aggregates::AggregateFunction,
    scalar::ScalarValue,
};

use super::{optimizer::OptimizerRule, utils};
use crate::error::Result;

/// Optimizer that uses available statistics for aggregate functions
pub struct AggregateStatistics {}

impl AggregateStatistics {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for AggregateStatistics {
    fn optimize(
        &self,
        plan: &LogicalPlan,
        execution_props: &ExecutionProps,
    ) -> crate::error::Result<LogicalPlan> {
        match plan {
            // match only select count(*) from table_scan
            LogicalPlan::Aggregate {
                input,
                group_expr,
                aggr_expr,
                schema,
            } if group_expr.is_empty() && aggr_expr.len() == 1 => {
                if let Some(num_rows) = match input.as_ref() {
                    LogicalPlan::TableScan { source, .. }
                        if source.has_exact_statistics() =>
                    {
                        source.statistics().num_rows
                    }
                    _ => None,
                } {
                    return match &aggr_expr[0] {
                        Expr::AggregateFunction {
                            fun: AggregateFunction::Count,
                            args,
                            distinct: false,
                        } if args == &[Expr::Literal(ScalarValue::UInt8(Some(1)))] => {
                            Ok(LogicalPlan::Projection {
                                expr: vec![Expr::Alias(
                                    Box::new(Expr::Literal(ScalarValue::UInt64(Some(
                                        num_rows as u64,
                                    )))),
                                    "COUNT(Uint8(1))".to_string(),
                                )],
                                input: Arc::new(LogicalPlan::EmptyRelation {
                                    produce_one_row: true,
                                    schema: Arc::new(DFSchema::empty()),
                                }),
                                schema: schema.clone(),
                            })
                        }
                        _ => Ok(plan.clone()),
                    };
                }

                Ok(plan.clone())
            }
            // Rest: recurse and find possible statistics
            _ => {
                let expr = plan.expressions();

                // apply the optimization to all inputs of the plan
                let inputs = plan.inputs();
                let new_inputs = inputs
                    .iter()
                    .map(|plan| self.optimize(plan, execution_props))
                    .collect::<Result<Vec<_>>>()?;

                utils::from_plan(plan, &expr, &new_inputs)
            }
        }
    }

    fn name(&self) -> &str {
        "count_statistics"
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};

    use crate::error::Result;
    use crate::execution::context::ExecutionProps;
    use crate::logical_plan::LogicalPlan;
    use crate::optimizer::aggregate_statistics::AggregateStatistics;
    use crate::optimizer::optimizer::OptimizerRule;
    use crate::{
        datasource::{datasource::Statistics, TableProvider},
        logical_plan::Expr,
    };

    struct TestTableProvider {
        num_rows: usize,
    }

    impl TableProvider for TestTableProvider {
        fn as_any(&self) -> &dyn std::any::Any {
            unimplemented!()
        }
        fn schema(&self) -> arrow::datatypes::SchemaRef {
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]))
        }

        fn scan(
            &self,
            _projection: &Option<Vec<usize>>,
            _batch_size: usize,
            _filters: &[Expr],
            _limit: Option<usize>,
        ) -> Result<std::sync::Arc<dyn crate::physical_plan::ExecutionPlan>> {
            unimplemented!()
        }
        fn statistics(&self) -> crate::datasource::datasource::Statistics {
            Statistics {
                num_rows: Some(self.num_rows),
                total_byte_size: None,
                column_statistics: None,
            }
        }
        fn has_exact_statistics(&self) -> bool {
            true
        }
    }

    #[test]
    fn optimize_count_using_statistics() -> Result<()> {
        use crate::execution::context::ExecutionContext;
        let mut ctx = ExecutionContext::new();
        ctx.register_table("test", Arc::new(TestTableProvider { num_rows: 100 }))
            .unwrap();

        let plan = ctx
            .create_logical_plan("select count(*) from test")
            .unwrap();
        let expected = "\
            Projection: #COUNT(UInt8(1))\
            \n  Projection: UInt64(100) AS COUNT(Uint8(1))\
            \n    EmptyRelation";

        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    fn assert_optimized_plan_eq(plan: &LogicalPlan, expected: &str) {
        let opt = AggregateStatistics::new();
        let optimized_plan = opt.optimize(plan, &ExecutionProps::new()).unwrap();
        let formatted_plan = format!("{:?}", optimized_plan);
        assert_eq!(formatted_plan, expected);
        assert_eq!(plan.schema(), plan.schema());
    }
}
