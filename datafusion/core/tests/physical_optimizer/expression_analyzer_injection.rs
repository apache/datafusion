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

//! Tests that ExpressionAnalyzerRegistry is preserved through optimizer rules.
//!
//! Optimizer rules like CombinePartialFinalAggregate, EnforceDistribution, and
//! ProjectionPushdown rebuild exec nodes internally. The post-rule injection loop
//! in the physical planner must re-inject the ExpressionAnalyzer registry into those new nodes.

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow::array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::prelude::SessionContext;
    use datafusion_common::Result;
    use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
    use datafusion_execution::config::SessionConfig;
    use datafusion_physical_plan::ExecutionPlan;

    /// Creates a SessionContext with `use_expression_analyzer = true`.
    fn make_ctx() -> SessionContext {
        let mut config = SessionConfig::new();
        config.options_mut().optimizer.use_expression_analyzer = true;
        SessionContext::new_with_config(config)
    }

    /// Registers an in-memory table with a single int64 column.
    async fn register_table(ctx: &SessionContext, name: &str) -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1i64, 2, 3]))],
        )?;
        let table = datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]])?;
        ctx.register_table(name, Arc::new(table))?;
        Ok(())
    }

    /// Asserts that every node in `plan` that declares
    /// `uses_expression_level_statistics() == true` has the registry set.
    fn assert_expression_analyzer_injected(plan: &Arc<dyn ExecutionPlan>) -> Result<()> {
        plan.clone().apply(|node| {
            if node.uses_expression_level_statistics() {
                assert!(
                    node.expression_analyzer_registry().is_some(),
                    "{} declares uses_expression_level_statistics but has no registry after optimization",
                    node.name()
                );
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        Ok(())
    }

    /// Filter and aggregate go through CombinePartialFinalAggregate which rebuilds
    /// the partial/final AggregateExec pair; both must end up with the ExpressionAnalyzer registry injected.
    #[tokio::test]
    async fn test_expression_analyzer_injected_through_aggregate_rewrite() -> Result<()> {
        let ctx = make_ctx();
        register_table(&ctx, "t").await?;

        let df = ctx
            .sql("SELECT a, COUNT(*) FROM t WHERE a > 1 GROUP BY a")
            .await?;
        let plan = df.create_physical_plan().await?;

        assert_expression_analyzer_injected(&plan)
    }

    /// A simple filter query - FilterExec must have the registry even without complex rewrites.
    #[tokio::test]
    async fn test_expression_analyzer_injected_filter() -> Result<()> {
        let ctx = make_ctx();
        register_table(&ctx, "t").await?;

        let df = ctx.sql("SELECT a FROM t WHERE a > 1").await?;
        let plan = df.create_physical_plan().await?;

        assert_expression_analyzer_injected(&plan)
    }

    /// A join query - HashJoinExec must have the registry after join selection rewrites it.
    #[tokio::test]
    async fn test_expression_analyzer_injected_join() -> Result<()> {
        let ctx = make_ctx();
        register_table(&ctx, "t1").await?;
        register_table(&ctx, "t2").await?;

        let df = ctx
            .sql("SELECT t1.a FROM t1 JOIN t2 ON t1.a = t2.a WHERE t1.a > 0")
            .await?;
        let plan = df.create_physical_plan().await?;

        assert_expression_analyzer_injected(&plan)
    }

    /// Disabled by default: verify that the flag gates the injection walk.
    /// When `use_expression_analyzer = false`, exec nodes must NOT have the registry set.
    #[tokio::test]
    async fn test_expression_analyzer_not_injected_when_disabled() -> Result<()> {
        let ctx = SessionContext::new(); // default config, ExpressionAnalyzer disabled

        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1i64, 2, 3]))],
        )?;
        let table = datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]])?;
        ctx.register_table("t", Arc::new(table))?;

        let df = ctx.sql("SELECT a FROM t WHERE a > 1").await?;
        let plan = df.create_physical_plan().await?;

        plan.clone().apply(|node| {
            if node.uses_expression_level_statistics() {
                assert!(
                    node.expression_analyzer_registry().is_none(),
                    "{} should NOT have the registry when use_expression_analyzer=false",
                    node.name()
                );
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        Ok(())
    }
}
