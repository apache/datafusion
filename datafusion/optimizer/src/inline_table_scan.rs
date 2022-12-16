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

//! Optimizer rule to replace TableScan references
//! such as DataFrames and Views and inlines the LogicalPlan
//! to support further optimization
use crate::{utils, OptimizerConfig, OptimizerRule};
use datafusion_common::Result;
use datafusion_expr::{logical_plan::LogicalPlan, Expr, LogicalPlanBuilder, TableScan};

/// Optimization rule that inlines TableScan that provide a [LogicalPlan]
/// ([DataFrame] / [ViewTable])
#[derive(Default)]
pub struct InlineTableScan;

impl InlineTableScan {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for InlineTableScan {
    fn try_optimize(
        &self,
        plan: &LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Option<LogicalPlan>> {
        match plan {
            // Match only on scans without filter / projection / fetch
            // Views and DataFrames won't have those added
            // during the early stage of planning
            LogicalPlan::TableScan(TableScan {
                source,
                table_name,
                filters,
                ..
            }) if filters.is_empty() => {
                if let Some(sub_plan) = source.get_logical_plan() {
                    // Recursively apply optimization
                    let plan = utils::optimize_children(self, sub_plan, config)?;
                    let plan = LogicalPlanBuilder::from(plan)
                        .project(vec![Expr::Wildcard])?
                        .alias(table_name)?;
                    Ok(Some(plan.build()?))
                } else {
                    // No plan available, return with table scan as is
                    Ok(Some(plan.clone()))
                }
            }

            // Rest: Recurse
            _ => {
                // apply the optimization to all inputs of the plan
                Ok(Some(utils::optimize_children(self, plan, config)?))
            }
        }
    }

    fn name(&self) -> &str {
        "inline_table_scan"
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, vec};

    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_expr::{col, lit, LogicalPlan, LogicalPlanBuilder, TableSource};

    use crate::optimizer::OptimizerContext;
    use crate::{inline_table_scan::InlineTableScan, OptimizerRule};

    pub struct RawTableSource {}

    impl TableSource for RawTableSource {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn schema(&self) -> arrow::datatypes::SchemaRef {
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]))
        }

        fn supports_filter_pushdown(
            &self,
            _filter: &datafusion_expr::Expr,
        ) -> datafusion_common::Result<datafusion_expr::TableProviderFilterPushDown>
        {
            Ok(datafusion_expr::TableProviderFilterPushDown::Inexact)
        }
    }

    pub struct CustomSource {
        plan: LogicalPlan,
    }

    impl CustomSource {
        fn new() -> Self {
            Self {
                plan: LogicalPlanBuilder::scan("y", Arc::new(RawTableSource {}), None)
                    .unwrap()
                    .build()
                    .unwrap(),
            }
        }
    }

    impl TableSource for CustomSource {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn supports_filter_pushdown(
            &self,
            _filter: &datafusion_expr::Expr,
        ) -> datafusion_common::Result<datafusion_expr::TableProviderFilterPushDown>
        {
            Ok(datafusion_expr::TableProviderFilterPushDown::Exact)
        }

        fn schema(&self) -> arrow::datatypes::SchemaRef {
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]))
        }

        fn get_logical_plan(&self) -> Option<&LogicalPlan> {
            Some(&self.plan)
        }
    }

    #[test]
    fn inline_table_scan() {
        let rule = InlineTableScan::new();

        let source = Arc::new(CustomSource::new());

        let scan = LogicalPlanBuilder::scan("x".to_string(), source, None).unwrap();

        let plan = scan.filter(col("x.a").eq(lit(1))).unwrap().build().unwrap();

        let optimized_plan = rule
            .try_optimize(&plan, &OptimizerContext::new())
            .unwrap()
            .expect("failed to optimize plan");
        let formatted_plan = format!("{:?}", optimized_plan);
        let expected = "Filter: x.a = Int32(1)\
        \n  SubqueryAlias: x\
        \n    Projection: y.a\
        \n      TableScan: y";

        assert_eq!(formatted_plan, expected);
        assert_eq!(plan.schema(), optimized_plan.schema());
    }
}
