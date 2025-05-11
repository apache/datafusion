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

//! [`EliminateSelfJoin`] eliminates self joins on unique constraint columns

use crate::{ApplyOrder, OptimizerConfig, OptimizerRule};
use datafusion_common::{tree_node::Transformed, Result};
use datafusion_expr::LogicalPlan;

#[derive(Default, Debug)]
pub struct EliminateSelfJoin;

impl EliminateSelfJoin {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for EliminateSelfJoin {
    fn name(&self) -> &str {
        "self_join"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        None
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        Ok(Transformed::no(plan))
    }
}

#[cfg(test)]
mod tests {
    use crate::assert_optimized_plan_eq_snapshot;
    use crate::OptimizerContext;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::{Column, Result};
    use datafusion_expr::builder::subquery_alias;
    use datafusion_expr::table_scan;
    use datafusion_expr::JoinConstraint;
    use datafusion_expr::{
        col, lit, logical_plan::builder::LogicalPlanBuilder, JoinType, LogicalPlan,
    };
    use datafusion_sql::TableReference;
    use std::sync::Arc;

    macro_rules! assert_optimized_plan_equal {
        (
            $plan:expr,
            @ $expected:literal $(,)?
        ) => {{
            let optimizer_ctx = OptimizerContext::new().with_max_passes(1);
            let rules: Vec<Arc<dyn crate::OptimizerRule + Send + Sync>> = vec![Arc::new(super::EliminateSelfJoin::new())];
            assert_optimized_plan_eq_snapshot!(
                optimizer_ctx,
                rules,
                $plan,
                @ $expected,
            )
        }};
    }

    fn test_table_scan() -> Result<LogicalPlan> {
        let schema = Schema::new(vec![
            Field::new("id", DataType::UInt32, false),
            Field::new("department", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
        ]);
        let left = table_scan(Some("employees"), &schema, None)?.build()?;
        let left = subquery_alias(left, TableReference::from("a"))?;
        let right = table_scan(Some("employees"), &schema, None)?.build()?;
        let right = subquery_alias(right, TableReference::from("b"))?;

        let plan = LogicalPlanBuilder::new(left)
            .join(
                right,
                JoinType::Inner,
                (vec![Column::from_name("id")], vec![Column::from_name("id")]),
                None,
            )?
            .build()?;
        // TODO: double check if this monkey patch is needed. If only `join_keys` is specified, the `join_constraint` should be `Using`
        let join = match plan {
            LogicalPlan::Join(mut join) => {
                join.join_constraint = JoinConstraint::Using;
                join
            }
            _ => panic!("Expected a Join"),
        };
        LogicalPlanBuilder::new(LogicalPlan::Join(join))
            .filter(col("b.department").eq(lit("HR")))?
            .project(vec![col("a.id")])?
            .build()
    }

    #[test]
    fn join_on_unique_key_with_filter() -> Result<()> {
        let plan = test_table_scan()?;

        assert_optimized_plan_equal!(plan, @r#"
        Projection: a.id
          Filter: a.department = Utf8("HR")
            SubqueryAlias: a
              TableScan: employees
        "#)
    }
}
