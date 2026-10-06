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

//! Nested RANGE peers through public physical execution APIs.
//! Construct plans directly to test peer boundaries independently of SQL type admission.

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{ArrayRef, AsArray, Int32Array, Int64Array, ListArray, StructArray};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::compute::concat_batches;
use arrow::datatypes::{DataType, Field, Int64Type, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};
use datafusion::physical_plan::windows::{
    BoundedWindowAggExec, WindowAggExec, create_window_expr,
};
use datafusion::physical_plan::{
    ExecutionPlan, InputOrderMode, SendableRecordBatchStream, collect,
};
use datafusion::prelude::SessionContext;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::{
    WindowFrame, WindowFrameBound, WindowFrameUnits, WindowFunctionDefinition,
};
use datafusion_functions_aggregate::sum::sum_udaf;
use datafusion_physical_expr::PhysicalSortExpr;
use datafusion_physical_expr::expressions::col;
use datafusion_physical_expr_common::sort_expr::LexOrdering;
use futures::{FutureExt, StreamExt};

fn nested_batches() -> Result<Vec<RecordBatch>> {
    // Three nested keys: a NULL container, a nested NULL, and nested 1.
    // Hidden values under the two NULL containers deliberately differ.
    let values = [Some(10), Some(99), None, None, None, Some(1)];
    let values: ArrayRef = Arc::new(Int32Array::from(values.to_vec()));
    let lists: ArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new("item", DataType::Int32, true)),
        OffsetBuffer::from_lengths([1; 6]),
        Arc::clone(&values),
        None,
    ));
    let structs: ArrayRef = Arc::new(StructArray::new(
        vec![Arc::new(Field::new("item", DataType::Int32, true))].into(),
        vec![values],
        None,
    ));
    let nulls = NullBuffer::from(vec![false, false, true, true, true, true]);
    let list_field = Arc::new(Field::new("item", lists.data_type().clone(), true));
    let keys: [ArrayRef; 3] = [
        Arc::new(ListArray::new(
            Arc::clone(&list_field),
            OffsetBuffer::from_lengths([1; 6]),
            Arc::clone(&lists),
            Some(nulls.clone()),
        )),
        Arc::new(ListArray::new(
            Arc::new(Field::new("item", structs.data_type().clone(), true)),
            OffsetBuffer::from_lengths([1; 6]),
            structs,
            Some(nulls.clone()),
        )),
        Arc::new(StructArray::new(
            vec![list_field].into(),
            vec![lists],
            Some(nulls),
        )),
    ];
    keys.into_iter()
        .map(|keys| {
            // The tie separates the third nested NULL from the preceding peers.
            let tie: ArrayRef = Arc::new(Int32Array::from(vec![0, 0, 0, 0, 1, 0]));
            Ok(RecordBatch::try_from_iter(vec![
                ("key", keys),
                ("tie", tie),
                (
                    "value",
                    Arc::new(Int64Array::from(vec![1, 2, 4, 8, 16, 32])) as ArrayRef,
                ),
            ])?)
        })
        .collect()
}

#[tokio::test]
async fn nested_range_current_row_physical_operators() -> Result<()> {
    use WindowFrameBound::{CurrentRow, Following, Preceding};

    for batch in nested_batches()? {
        let schema = batch.schema();
        let order_by = vec![
            PhysicalSortExpr::new_default(col("key", &schema)?),
            PhysicalSortExpr::new_default(col("tie", &schema)?),
        ];
        // Repeated peers cross input batch boundaries.
        let batches = (0..6).map(|row| batch.slice(row, 1)).collect();
        let source = MemorySourceConfig::try_new(&[batches], Arc::clone(&schema), None)?
            .try_with_sort_information(vec![
                LexOrdering::new(order_by.clone()).unwrap(),
            ])?;
        let input: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(source);
        for (start, end, expected) in [
            (
                Preceding(ScalarValue::UInt64(None)),
                CurrentRow,
                [3, 3, 15, 15, 31, 63],
            ),
            (CurrentRow, CurrentRow, [3, 3, 12, 12, 16, 32]),
            (
                CurrentRow,
                Following(ScalarValue::UInt64(None)),
                [63, 63, 60, 60, 48, 32],
            ),
        ] {
            for bounded in [false, true] {
                let expr = create_window_expr(
                    &WindowFunctionDefinition::AggregateUDF(sum_udaf()),
                    "sum".to_string(),
                    &[col("value", &schema)?],
                    &[],
                    &order_by,
                    Arc::new(WindowFrame::new_bounds(
                        WindowFrameUnits::Range,
                        start.clone(),
                        end.clone(),
                    )),
                    Arc::clone(&schema),
                    false,
                    false,
                    None,
                )?;
                let plan: Arc<dyn ExecutionPlan> = if bounded {
                    Arc::new(BoundedWindowAggExec::try_new(
                        vec![expr],
                        Arc::clone(&input),
                        InputOrderMode::Sorted,
                        false,
                    )?)
                } else {
                    Arc::new(WindowAggExec::try_new(
                        vec![expr],
                        Arc::clone(&input),
                        false,
                    )?)
                };
                let output_schema = plan.schema();
                let output = collect(plan, SessionContext::new().task_ctx()).await?;
                let output = concat_batches(&output_schema, &output)?;
                let actual = output.column(3).as_primitive::<Int64Type>();
                assert_eq!(
                    actual.iter().collect::<Vec<_>>(),
                    expected.map(Some),
                    "{}, {start:?} to {end:?}, bounded={bounded}",
                    batch.column(0).data_type(),
                );
            }
        }
    }
    Ok(())
}

#[derive(Debug)]
struct OpenEndedPartition {
    batch: RecordBatch,
}

impl PartitionStream for OpenEndedPartition {
    fn schema(&self) -> &SchemaRef {
        self.batch.schema_ref()
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let batches = (0..self.batch.num_rows())
            .map(|row| Ok(self.batch.slice(row, 1)))
            .collect::<Vec<_>>();
        Box::pin(RecordBatchStreamAdapter::new(
            self.batch.schema(),
            futures::stream::iter(batches).chain(futures::stream::pending()),
        ))
    }
}

#[tokio::test]
async fn sorted_nested_range_emits_completed_peers_before_eof() -> Result<()> {
    let batch = nested_batches()?.remove(0);
    let schema = batch.schema();
    let order_by = vec![
        PhysicalSortExpr::new_default(col("key", &schema)?),
        PhysicalSortExpr::new_default(col("tie", &schema)?),
    ];
    let source = Arc::new(StreamingTableExec::try_new(
        Arc::clone(&schema),
        vec![Arc::new(OpenEndedPartition { batch })],
        None,
        vec![LexOrdering::new(order_by.clone()).unwrap()],
        true,
        None,
    )?);
    let expr = create_window_expr(
        &WindowFunctionDefinition::AggregateUDF(sum_udaf()),
        "sum".to_string(),
        &[col("value", &schema)?],
        &[],
        &order_by,
        Arc::new(WindowFrame::new_bounds(
            WindowFrameUnits::Range,
            WindowFrameBound::Preceding(ScalarValue::UInt64(None)),
            WindowFrameBound::CurrentRow,
        )),
        schema,
        false,
        false,
        None,
    )?;
    let plan =
        BoundedWindowAggExec::try_new(vec![expr], source, InputOrderMode::Sorted, false)?;
    let mut stream = plan.execute(0, SessionContext::new().task_ctx())?;
    let actual = tokio::time::timeout(Duration::from_secs(5), async {
        let mut sums = Vec::new();
        while sums.len() < 5 {
            let batch = stream.next().await.unwrap()?;
            sums.extend(batch.column(3).as_primitive::<Int64Type>().iter());
        }
        Ok::<_, datafusion_common::DataFusionError>(sums)
    })
    .await
    .expect("completed peer groups should emit before EOF")?;
    assert_eq!(actual, [Some(3), Some(3), Some(15), Some(15), Some(31)]);
    // The final peer group cannot finish until another key or EOF arrives.
    assert!(stream.next().now_or_never().is_none());
    Ok(())
}
