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
    use arrow_schema::{DataType, Field, Schema};
    use datafusion_common::Result;
    use datafusion_datasource::memory::MemorySourceConfig;
    use datafusion_datasource::source::DataSourceExec;
    use datafusion_execution::TaskContext;
    use datafusion_expr::WindowFrame;
    use datafusion_functions_aggregate::count::count_udaf;
    use datafusion_physical_expr::aggregate::AggregateExprBuilder;
    use datafusion_physical_expr::expressions::{Column, col};
    use datafusion_physical_expr::window::PlainAggregateWindowExpr;
    use datafusion_physical_plan::windows::BoundedWindowAggExec;
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
