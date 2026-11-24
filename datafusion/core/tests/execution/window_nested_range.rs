// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Nested RANGE peers through public physical execution APIs.
//! Construct plans directly to test peer boundaries independently of SQL type admission.

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{ArrayRef, AsArray, Int32Array, Int64Array, ListArray, StructArray};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::compute::concat_batches;
use arrow::datatypes::{DataType, Field, Int32Type, Int64Type, SchemaRef, UInt64Type};
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
    ExecutionPlan, ExecutionPlanProperties, InputOrderMode, SendableRecordBatchStream,
    collect,
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
                // In Linear mode, the final row advances another partition's peers.
                (
                    "p",
                    Arc::new(Int32Array::from(vec![0, 0, 0, 0, 0, 1])) as ArrayRef,
                ),
                (
                    "value",
                    Arc::new(Int64Array::from(vec![1, 2, 4, 8, 16, 32])) as ArrayRef,
                ),
            ])?)
        })
        .collect()
}

fn nested_ordering(schema: &SchemaRef) -> Result<LexOrdering> {
    Ok(LexOrdering::from([
        PhysicalSortExpr::new_default(col("key", schema)?),
        PhysicalSortExpr::new_default(col("tie", schema)?),
    ]))
}

fn window_plan(
    input: &Arc<dyn ExecutionPlan>,
    mode: Option<&InputOrderMode>,
    start: WindowFrameBound,
    end: WindowFrameBound,
) -> Result<Arc<dyn ExecutionPlan>> {
    let schema = input.schema();
    let partition_by = if mode == Some(&InputOrderMode::Linear) {
        vec![col("p", &schema)?]
    } else {
        vec![]
    };
    let expr = create_window_expr(
        &WindowFunctionDefinition::AggregateUDF(sum_udaf()),
        "sum".to_string(),
        &[col("value", &schema)?],
        &partition_by,
        input.output_ordering().unwrap(),
        Arc::new(WindowFrame::new_bounds(WindowFrameUnits::Range, start, end)),
        schema,
        false,
        false,
        None,
    )?;
    let plan: Arc<dyn ExecutionPlan> = match mode {
        Some(mode) => Arc::new(BoundedWindowAggExec::try_new(
            vec![expr],
            Arc::clone(input),
            mode.clone(),
            false,
        )?),
        None => Arc::new(WindowAggExec::try_new(
            vec![expr],
            Arc::clone(input),
            false,
        )?),
    };
    assert!(
        input
            .equivalence_properties()
            .ordering_satisfy_requirement(
                plan.required_input_ordering()[0]
                    .clone()
                    .unwrap()
                    .into_single(),
            )?
    );
    Ok(plan)
}

#[tokio::test]
async fn nested_range_current_row_physical_operators() -> Result<()> {
    use WindowFrameBound::{CurrentRow, Following, Preceding};

    for batch in nested_batches()? {
        let schema = batch.schema();
        // Repeated peers cross input batch boundaries.
        let batches = (0..batch.num_rows())
            .map(|row| batch.slice(row, 1))
            .collect();
        let source = MemorySourceConfig::try_new(&[batches], Arc::clone(&schema), None)?
            .try_with_sort_information(vec![nested_ordering(&schema)?])?;
        let input: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(source);
        // window.slt covers list keys in SQL-planned sorted windows.
        let modes: &[Option<InputOrderMode>] = if batch.column(0).data_type().is_list() {
            &[Some(InputOrderMode::Linear)]
        } else {
            &[
                None,
                Some(InputOrderMode::Sorted),
                Some(InputOrderMode::Linear),
            ]
        };
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
            for mode in modes.iter().map(Option::as_ref) {
                // The planner uses WindowAggExec for UNBOUNDED FOLLOWING.
                if mode.is_some() && end.is_unbounded() {
                    continue;
                }
                let plan = window_plan(&input, mode, start.clone(), end.clone())?;
                let output_schema = plan.schema();
                let output = collect(plan, SessionContext::new().task_ctx()).await?;
                let output = concat_batches(&output_schema, &output)?;
                let actual = output
                    .column(batch.num_columns())
                    .as_primitive::<Int64Type>();
                let mut expected = expected;
                if mode == Some(&InputOrderMode::Linear) {
                    // The final row is in its own PARTITION BY group.
                    expected[5] = 32;
                }
                assert_eq!(
                    actual.iter().collect::<Vec<_>>(),
                    expected.map(Some),
                    "{}, {start:?} to {end:?}, mode={mode:?}",
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

async fn assert_emits_before_eof(
    batch: &RecordBatch,
    order_by: LexOrdering,
    mode: InputOrderMode,
    expected: &[Option<i64>],
) -> Result<()> {
    let source: Arc<dyn ExecutionPlan> = Arc::new(StreamingTableExec::try_new(
        batch.schema(),
        vec![Arc::new(OpenEndedPartition {
            batch: batch.clone(),
        })],
        None,
        vec![order_by],
        true,
        None,
    )?);
    let plan = window_plan(
        &source,
        Some(&mode),
        WindowFrameBound::Preceding(ScalarValue::UInt64(None)),
        WindowFrameBound::CurrentRow,
    )?;
    let mut stream = plan.execute(0, SessionContext::new().task_ctx())?;
    let actual = tokio::time::timeout(Duration::from_secs(5), async {
        let mut sums = Vec::new();
        while sums.len() < expected.len() {
            let output = stream.next().await.unwrap()?;
            sums.extend(
                output
                    .column(batch.num_columns())
                    .as_primitive::<Int64Type>()
                    .iter(),
            );
        }
        Ok::<_, datafusion_common::DataFusionError>(sums)
    })
    .await
    .expect("completed peer groups should emit before EOF")?;
    assert_eq!(
        actual,
        expected,
        "{}, mode={mode:?}",
        batch.column(0).data_type()
    );
    // The final peer group cannot finish until another key or EOF arrives.
    assert!(stream.next().now_or_never().is_none());
    Ok(())
}

#[tokio::test]
async fn nested_range_emits_completed_peers_before_eof() -> Result<()> {
    for batch in nested_batches()? {
        for mode in [InputOrderMode::Sorted, InputOrderMode::Linear] {
            assert_emits_before_eof(
                &batch,
                nested_ordering(batch.schema_ref())?,
                mode,
                &[Some(3), Some(3), Some(15), Some(15), Some(31)],
            )
            .await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn nested_range_desc_nulls_last_emits_before_eof() -> Result<()> {
    // Both [1] and a top-level NULL sort after [2] under DESC NULLS LAST.
    // Each stream stops after that row arrives in a different partition.
    for last_key in [Some([Some(1)]), None] {
        let keys = ListArray::from_iter_primitive::<Int32Type, _, _>([
            Some([Some(2)]),
            Some([Some(2)]),
            last_key,
        ]);
        let batch = RecordBatch::try_from_iter([
            ("key", Arc::new(keys) as ArrayRef),
            ("p", Arc::new(Int32Array::from(vec![0, 0, 1]))),
            ("value", Arc::new(Int64Array::from(vec![1, 2, 4]))),
        ])?;
        let order = PhysicalSortExpr::new_default(col("key", batch.schema_ref())?)
            .desc()
            .nulls_last();
        assert_emits_before_eof(
            &batch,
            [order].into(),
            InputOrderMode::Linear,
            &[Some(3), Some(3)],
        )
        .await?;
    }
    Ok(())
}

#[tokio::test]
async fn dictionary_null_encodings_share_groups_and_rank() -> Result<()> {
    use arrow::array::{DictionaryArray, Int8Array};
    use arrow::datatypes::Int8Type;
    use datafusion::functions_window::rank::{dense_rank_udwf, rank_udwf};
    use datafusion_functions_aggregate::count::count_udaf;
    use datafusion_physical_plan::windows::BoundedWindowAggExec;

    // The two rows both hold `[NULL]`: one via a null dictionary key, one via
    // a key referencing a null dictionary value (#26689).
    let children: ArrayRef = Arc::new(DictionaryArray::<Int8Type>::try_new(
        Int8Array::from(vec![None, Some(0)]),
        Arc::new(Int32Array::from(vec![None])),
    )?);
    let key: ArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new("item", children.data_type().clone(), true)),
        OffsetBuffer::from_lengths([1, 1]),
        children,
        None,
    ));
    let tie: ArrayRef = Arc::new(Int32Array::from(vec![0, 0]));
    let value: ArrayRef = Arc::new(Int32Array::from(vec![1, 1]));
    let batch =
        RecordBatch::try_from_iter([("key", key), ("tie", tie), ("value", value)])?;

    // One row per input batch, so group and rank state crosses batches.
    let schema = batch.schema();
    let batches = (0..batch.num_rows())
        .map(|row| batch.slice(row, 1))
        .collect();
    let source = MemorySourceConfig::try_new(&[batches], Arc::clone(&schema), None)?
        .try_with_sort_information(vec![nested_ordering(&schema)?])?;
    let input: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(source);

    // GROUPS BETWEEN CURRENT ROW AND CURRENT ROW counts both rows per group.
    let count_expr = create_window_expr(
        &WindowFunctionDefinition::AggregateUDF(count_udaf()),
        "count".to_string(),
        &[col("value", &schema)?],
        &[],
        input.output_ordering().unwrap(),
        Arc::new(WindowFrame::new_bounds(
            WindowFrameUnits::Groups,
            WindowFrameBound::CurrentRow,
            WindowFrameBound::CurrentRow,
        )),
        Arc::clone(&schema),
        false,
        false,
        None,
    )?;
    let plan: Arc<dyn ExecutionPlan> = Arc::new(BoundedWindowAggExec::try_new(
        vec![count_expr],
        Arc::clone(&input),
        InputOrderMode::Sorted,
        false,
    )?);
    let output = collect(Arc::clone(&plan), SessionContext::new().task_ctx()).await?;
    let output = concat_batches(&plan.schema(), &output)?;
    let counts = output
        .column(batch.num_columns())
        .as_primitive::<Int64Type>();
    assert_eq!(counts.iter().collect::<Vec<_>>(), vec![Some(2), Some(2)]);

    // rank() and dense_rank() treat the two rows as peers.
    let make_rank = |udwf, name: &str| {
        create_window_expr(
            &WindowFunctionDefinition::WindowUDF(udwf),
            name.to_string(),
            &[],
            &[],
            input.output_ordering().unwrap(),
            Arc::new(WindowFrame::new_bounds(
                WindowFrameUnits::Range,
                WindowFrameBound::Preceding(ScalarValue::UInt64(None)),
                WindowFrameBound::CurrentRow,
            )),
            Arc::clone(&schema),
            false,
            false,
            None,
        )
    };
    let rank_expr = make_rank(rank_udwf(), "rank")?;
    let dense_expr = make_rank(dense_rank_udwf(), "dense_rank")?;
    let plan: Arc<dyn ExecutionPlan> = Arc::new(BoundedWindowAggExec::try_new(
        vec![rank_expr, dense_expr],
        Arc::clone(&input),
        InputOrderMode::Sorted,
        false,
    )?);
    let output = collect(Arc::clone(&plan), SessionContext::new().task_ctx()).await?;
    let output = concat_batches(&plan.schema(), &output)?;
    let ranks = output
        .column(batch.num_columns())
        .as_primitive::<UInt64Type>();
    assert_eq!(ranks.iter().collect::<Vec<_>>(), vec![Some(1), Some(1)]);
    let dense = output
        .column(batch.num_columns() + 1)
        .as_primitive::<UInt64Type>();
    assert_eq!(dense.iter().collect::<Vec<_>>(), vec![Some(1), Some(1)]);

    Ok(())
}
