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

//! [`DelimGetRemove`] converts correlated subqueries to `DependentJoin`

use crate::{ApplyOrder, OptimizerConfig, OptimizerRule};

use arrow::datatypes::{DataType, Field};
use datafusion_common::{
    tree_node::{Transformed, TreeNode, TreeNodeRecursion},
    Result,
};
use datafusion_expr::LogicalPlan;

#[derive(Debug)]
struct DelimGetRemove {}

impl OptimizerRule for DelimGetRemove {
    fn name(&self) -> &str {
        todo!()
    }
    fn apply_order(&self) -> Option<ApplyOrder> {
        None
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        println!("{plan}");
        Ok(Transformed::no(plan))
    }
}
#[cfg(test)]
mod tests {
    use crate::test::test_table_scan_with_name;
    use crate::Optimizer;
    use crate::{
        assert_optimized_plan_eq_display_indent_snapshot, OptimizerConfig,
        OptimizerContext, OptimizerRule,
    };
    use crate::{
        decorrelate_dependent_join::DecorrelateDependentJoin,
        delim_get_remove::DelimGetRemove,
    };
    use arrow::datatypes::DataType as ArrowDataType;
    use datafusion_common::Result;
    use datafusion_expr::{
        exists, expr_fn::col, in_subquery, lit, out_ref_col, scalar_subquery, Expr,
        LogicalPlan, LogicalPlanBuilder,
    };
    use datafusion_functions_aggregate::count::count;
    use std::sync::Arc;

    macro_rules! assert_decorrelate_delim_get_removed {
        (
            $plan:expr,
            @ $expected:literal $(,)?
        ) => {{
            let rule1: Arc<dyn crate::OptimizerRule + Send + Sync> = Arc::new(DecorrelateDependentJoin::new());
            let rule2: Arc<dyn crate::OptimizerRule + Send + Sync> = Arc::new(DelimGetRemove::new());
            let optimizer = $crate::Optimizer::with_rules(vec![rule1,rule2]);
            let optimized_plan = optimizer
                .optimize($plan, &$crate::OptimizerContext::new(), |_, _| {})
                .expect("failed to optimize plan");
            let formatted_plan = optimized_plan.display_indent_schema();
            insta::assert_snapshot!(formatted_plan, @ $expected);

            Ok::<(), datafusion_common::DataFusionError>(())
        }};
    }

    #[test]
    fn todo() -> Result<()> {
        Ok(())
    }
}
