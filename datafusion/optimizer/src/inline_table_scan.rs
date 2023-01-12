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
use crate::optimizer::ApplyOrder;
use crate::{OptimizerConfig, OptimizerRule};
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
        _config: &dyn OptimizerConfig,
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
                    let plan = LogicalPlanBuilder::from(sub_plan.clone())
                        .project(vec![Expr::Wildcard])?
                        .alias(table_name)?;
                    Ok(Some(plan.build()?))
                } else {
                    Ok(None)
                }
            }
            _ => Ok(None),
        }
    }

    fn name(&self) -> &str {
        "inline_table_scan"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, vec};

    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_expr::{col, lit, LogicalPlan, LogicalPlanBuilder, TableSource};

    use crate::inline_table_scan::InlineTableScan;
    use crate::test::assert_optimized_plan_eq;

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
    fn inline_table_scan() -> datafusion_common::Result<()> {
        let scan = LogicalPlanBuilder::scan(
            "x".to_string(),
            Arc::new(CustomSource::new()),
            None,
        )?;
        let plan = scan.filter(col("x.a").eq(lit(1)))?.build()?;
        let expected = "Filter: x.a = Int32(1)\
        \n  SubqueryAlias: x\
        \n    Projection: y.a\
        \n      TableScan: y";

        assert_optimized_plan_eq(Arc::new(InlineTableScan::new()), &plan, expected)
    }
}
