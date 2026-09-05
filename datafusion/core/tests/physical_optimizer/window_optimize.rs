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

#[cfg(test)]
mod test {
    use arrow::array::{Int32Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema, SortOptions};
    use datafusion_common::Result;
    use datafusion_common::config::ConfigOptions;
    use datafusion_datasource::memory::MemorySourceConfig;
    use datafusion_datasource::source::DataSourceExec;
    use datafusion_execution::TaskContext;
    use datafusion_expr::{WindowFrame, WindowFunctionDefinition};
    use datafusion_functions_aggregate::count::count_udaf;
    use datafusion_functions_aggregate::first_last::last_value_udaf;
    use datafusion_physical_expr::aggregate::AggregateExprBuilder;
    use datafusion_physical_expr::expressions::{Column, col};
    use datafusion_physical_expr::window::{PlainAggregateWindowExpr, WindowExpr};
    use datafusion_physical_expr::{LexOrdering, PhysicalExpr, PhysicalSortExpr};
    use datafusion_physical_optimizer::PhysicalOptimizerRule;
    use datafusion_physical_optimizer::ensure_requirements::EnsureRequirements;
    use datafusion_physical_plan::projection::{ProjectionExec, ProjectionExpr};
    use datafusion_physical_plan::windows::{BoundedWindowAggExec, create_window_expr};
    use datafusion_physical_plan::{ExecutionPlan, InputOrderMode, common};
    use std::sync::Arc;

    /// Test case for <https://github.com/apache/datafusion/issues/16308>
    #[tokio::test]
    async fn test_window_constant_aggregate() -> Result<()> {
        let source = mock_data()?;
        let schema = source.schema();
        let c = Arc::new(Column::new("b", 1));
        let cnt = AggregateExprBuilder::new(count_udaf(), vec![c])
            .schema(schema.clone())
            .alias("t")
            .build()?;
        let partition = [col("a", &schema)?];
        let frame = WindowFrame::new(None);
        let plain = PlainAggregateWindowExpr::new(
            Arc::new(cnt),
            &partition,
            &[],
            Arc::new(frame),
            None,
        );

        let bounded_agg_exec = BoundedWindowAggExec::try_new(
            vec![Arc::new(plain)],
            source,
            InputOrderMode::Linear,
            true,
        )?;
        let task_ctx = Arc::new(TaskContext::default());
        common::collect(bounded_agg_exec.execute(0, task_ctx)?).await?;

        Ok(())
    }

    /// Test case for <https://github.com/apache/datafusion/issues/24884>
    ///
    /// `EnsureRequirements` reverses the second window expression to reuse the
    /// `t ASC` ordering required by the first one. That reversal must not rename
    /// the window's output field, otherwise the parent projection -- which
    /// references those columns by name -- can no longer be resolved.
    #[tokio::test]
    async fn test_window_reversal_preserves_output_field_names() -> Result<()> {
        // `t` must be non-nullable for the ordering equivalence that triggers
        // the reversal.
        let schema = Arc::new(Schema::new(vec![
            Field::new("t", DataType::Int32, false),
            Field::new("v", DataType::Int32, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(Int32Array::from(vec![None, Some(10)])),
            ],
        )?;
        let ordering =
            LexOrdering::new([PhysicalSortExpr::new_default(col("t", &schema)?)])
                .unwrap();
        let source: Arc<dyn ExecutionPlan> = Arc::new(DataSourceExec::new(Arc::new(
            MemorySourceConfig::try_new(&[vec![batch]], Arc::clone(&schema), None)?
                .try_with_sort_information(vec![ordering])?,
        )));

        // Two `last_value` aggregate-UDAF windows with opposite ORDER BY
        // directions, so the optimizer reverses the second one.
        let window_expr = |ascending: bool| -> Result<Arc<dyn WindowExpr>> {
            let sort_expr = PhysicalSortExpr::new(
                col("t", &schema)?,
                SortOptions::new(!ascending, false),
            );
            create_window_expr(
                &WindowFunctionDefinition::AggregateUDF(last_value_udaf()),
                format!("last_value_{}", if ascending { "asc" } else { "desc" }),
                &[col("v", &schema)?],
                &[],
                std::slice::from_ref(&sort_expr),
                Arc::new(WindowFrame::new(Some(false))),
                Arc::clone(&schema),
                false,
                false,
                None,
            )
        };

        let mut plan: Arc<dyn ExecutionPlan> = source;
        for ascending in [true, false] {
            plan = Arc::new(BoundedWindowAggExec::try_new(
                vec![window_expr(ascending)?],
                plan,
                InputOrderMode::Sorted,
                false,
            )?);
        }

        // A projection that references the window outputs by name, as the
        // physical planner would produce.
        let plan_schema = plan.schema();
        let projection_exprs = plan_schema
            .fields()
            .iter()
            .enumerate()
            .map(|(idx, field)| ProjectionExpr {
                expr: Arc::new(Column::new(field.name(), idx)) as Arc<dyn PhysicalExpr>,
                alias: field.name().clone(),
            })
            .collect::<Vec<_>>();
        let plan: Arc<dyn ExecutionPlan> =
            Arc::new(ProjectionExec::try_new(projection_exprs, plan)?);

        // Used to fail with an internal error from `ProjectionMapping::try_new`.
        let optimized = EnsureRequirements::new()
            .optimize(Arc::clone(&plan), &ConfigOptions::new())?;

        let names = |plan: &Arc<dyn ExecutionPlan>| {
            plan.schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect::<Vec<_>>()
        };
        assert_eq!(names(&optimized), names(&plan));

        // The window exec below the projection must keep its field names too,
        // otherwise the projection's columns would dangle.
        let window = optimized.children()[0];
        assert_eq!(names(window), names(&plan));

        Ok(())
    }

    pub fn mock_data() -> Result<Arc<DataSourceExec>> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
        ]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![
                    Some(1),
                    Some(1),
                    Some(3),
                    Some(2),
                    Some(1),
                ])),
                Arc::new(Int32Array::from(vec![
                    Some(1),
                    Some(6),
                    Some(2),
                    Some(8),
                    Some(9),
                ])),
            ],
        )?;

        MemorySourceConfig::try_new_exec(&[vec![batch]], Arc::clone(&schema), None)
    }
}
