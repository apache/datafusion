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

use std::fmt::{self, Formatter};
use std::sync::Arc;

use arrow::array::record_batch;
use arrow::datatypes as arrow_schema;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::{Result, ScalarValue};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_expr::Operator;
use datafusion_physical_expr::expressions::{BinaryExpr, NegativeExpr, col, lit};
use datafusion_physical_expr::{LexOrdering, PhysicalExpr, PhysicalSortExpr};
use datafusion_physical_optimizer::PhysicalOptimizerRule;
use datafusion_physical_optimizer::ensure_requirements::EnsureRequirements;
use datafusion_physical_optimizer::output_requirements::OutputRequirements;
use datafusion_physical_optimizer::sanity_checker::SanityCheckPlan;
use datafusion_physical_plan::projection::ProjectionExec;
use datafusion_physical_plan::sorts::sort::SortExec;
use datafusion_physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion_physical_plan::test::TestMemoryExec;
use datafusion_physical_plan::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    ReplaceChildrenOptions, collect,
};

/// A real, executable projection hidden behind a custom operator. It preserves
/// row order, but its output columns need not identify the same input values.
#[derive(Debug)]
struct CustomProjection(ProjectionExec);

impl DisplayAs for CustomProjection {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        write!(f, "CustomProjection")
    }
}

impl ExecutionPlan for CustomProjection {
    fn name(&self) -> &str {
        "CustomProjection"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.0.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.0.children()
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn replace_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        _: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self(ProjectionExec::try_new(
            self.0.expr().to_vec(),
            Arc::clone(&children[0]),
        )?)))
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.replace_children(
            children,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        self.0.apply_expressions(f)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.0.execute(partition, context)
    }
}

#[tokio::test]
async fn custom_sort_pushdown_preserves_output_values() -> Result<()> {
    let first = record_batch!(("a", Int32, [2, 1]), ("b", Int32, [30, 10]))?;
    let second = record_batch!(("a", Int32, [3]), ("b", Int32, [20]))?;
    let schema = first.schema();
    let source = TestMemoryExec::try_new_exec(
        &[vec![first], vec![second]],
        Arc::clone(&schema),
        None,
    )?;
    let a = col("a", &schema)?;
    let b = col("b", &schema)?;
    let sum = Arc::new(BinaryExpr::new(a.clone(), Operator::Plus, b.clone()))
        as Arc<dyn PhysicalExpr>;
    let negative = Arc::new(NegativeExpr::new(a.clone())) as Arc<dyn PhysicalExpr>;
    let flag = Arc::new(BinaryExpr::new(b.clone(), Operator::Gt, lit(10_i32)))
        as Arc<dyn PhysicalExpr>;
    let rows = [("1", "10"), ("2", "30"), ("3", "20")];
    for (label, expressions, boolean_key, expected, pushdown) in [
        (
            "identity",
            [(a.clone(), "a"), (b.clone(), "b")],
            false,
            rows,
            true,
        ),
        (
            "renamed",
            [(a.clone(), "first"), (b.clone(), "second")],
            false,
            rows,
            true,
        ),
        (
            "reordered",
            [(b.clone(), "b"), (a.clone(), "a")],
            false,
            [("10", "1"), ("20", "3"), ("30", "2")],
            false,
        ),
        (
            "generated with identical schema",
            [(sum, "a"), (b.clone(), "b")],
            false,
            [("11", "10"), ("23", "20"), ("32", "30")],
            false,
        ),
        (
            "reversed values with identical schema",
            [(negative, "a"), (b.clone(), "b")],
            false,
            [("-3", "20"), ("-2", "30"), ("-1", "10")],
            false,
        ),
        (
            "incompatible child expression",
            [(flag, "flag"), (a, "a")],
            true,
            [("false", "1"), ("true", "2"), ("true", "3")],
            false,
        ),
    ] {
        let custom: Arc<dyn ExecutionPlan> =
            Arc::new(CustomProjection(ProjectionExec::try_new(
                expressions.map(|(expr, name)| (expr, name.to_owned())),
                source.clone(),
            )?));
        let output = custom.schema();
        let mut first_key = col(output.field(0).name(), &output)?;
        if boolean_key {
            first_key = Arc::new(BinaryExpr::new(first_key, Operator::And, lit(true)));
        }
        let ordering = LexOrdering::new([
            PhysicalSortExpr::new_default(first_key),
            PhysicalSortExpr::new_default(col(output.field(1).name(), &output)?),
        ])
        .unwrap();
        let local_sort = Arc::new(
            SortExec::new(ordering.clone(), custom).with_preserve_partitioning(true),
        );
        let plan = Arc::new(SortPreservingMergeExec::new(ordering, local_sort));
        let mut config = ConfigOptions::new();
        config.execution.target_partitions = 2;
        let plan = OutputRequirements::new_add_mode().optimize(plan, &config)?;
        let optimized = EnsureRequirements::new().optimize(plan, &config)?;
        let optimized =
            OutputRequirements::new_remove_mode().optimize(optimized, &config)?;
        SanityCheckPlan::new().optimize(optimized.clone(), &config)?;
        assert_eq!(
            optimized.children()[0].is::<CustomProjection>(),
            pushdown,
            "{label}: {}",
            datafusion_physical_plan::displayable(optimized.as_ref()).indent(true)
        );
        let batches = collect(optimized, Arc::new(TaskContext::default())).await?;
        let mut actual = Vec::new();
        for batch in batches {
            for row in 0..batch.num_rows() {
                actual.push((
                    ScalarValue::try_from_array(batch.column(0), row)?.to_string(),
                    ScalarValue::try_from_array(batch.column(1), row)?.to_string(),
                ));
            }
        }
        assert_eq!(
            actual,
            expected.map(|(left, right)| (left.to_string(), right.to_string())),
            "{label}"
        );
    }
    Ok(())
}
