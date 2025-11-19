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

//! Integration tests for BatchedTableFunction

use std::any::Any;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::array::{Array, ArrayRef, AsArray, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::prelude::*;
use datafusion_catalog::batched_function::helpers::materialized_batch_stream;
use datafusion_catalog::{BatchedTableFunction, BatchedTableFunctionImpl};
use datafusion_common::{exec_err, Result};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_expr::{Expr, Signature, Volatility};
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion_physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, RecordBatchStream,
};
use futures::stream::{Stream, StreamExt};

/// Simple test execution plan that produces a single batch of data
#[derive(Debug, Clone)]
struct TestExec {
    schema: SchemaRef,
    data: Vec<RecordBatch>,
    cache: PlanProperties,
}

impl TestExec {
    fn new(data: Vec<RecordBatch>) -> Self {
        let schema = data[0].schema();
        let cache = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            datafusion_physical_plan::Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            schema,
            data,
            cache,
        }
    }
}

impl DisplayAs for TestExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "TestExec")
    }
}

impl ExecutionPlan for TestExec {
    fn name(&self) -> &str {
        "TestExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(TestStream {
            schema: Arc::clone(&self.schema),
            data: self.data.clone(),
            index: 0,
        }))
    }
}

struct TestStream {
    schema: SchemaRef,
    data: Vec<RecordBatch>,
    index: usize,
}

impl Stream for TestStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if self.index < self.data.len() {
            let batch = self.data[self.index].clone();
            self.index += 1;
            Poll::Ready(Some(Ok(batch)))
        } else {
            Poll::Ready(None)
        }
    }
}

impl RecordBatchStream for TestStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

/// Simple batched table function that generates a range of numbers
#[derive(Debug)]
struct GenerateRangeFunction {
    signature: Signature,
}

impl GenerateRangeFunction {
    fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Int64, DataType::Int64],
                Volatility::Immutable,
            ),
        }
    }
}

#[async_trait::async_trait]
impl BatchedTableFunctionImpl for GenerateRangeFunction {
    fn name(&self) -> &str {
        "generate_range"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<Schema> {
        Ok(Schema::new(vec![Field::new("n", DataType::Int64, false)]))
    }

    async fn invoke_batch(
        &self,
        args: &[ArrayRef],
        _projection: Option<&[usize]>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<datafusion_catalog::BatchResultStream> {
        if args.len() != 2 {
            return exec_err!("generate_range expects 2 arguments");
        }

        let start_array = args[0].as_primitive::<arrow::datatypes::Int64Type>();
        let end_array = args[1].as_primitive::<arrow::datatypes::Int64Type>();

        if start_array.len() != end_array.len() {
            return exec_err!("start and end arrays must have same length");
        }

        let mut output_values = Vec::new();
        let mut input_row_indices = Vec::new();

        for row_idx in 0..start_array.len() {
            if start_array.is_null(row_idx) || end_array.is_null(row_idx) {
                continue;
            }

            let start = start_array.value(row_idx);
            let end = end_array.value(row_idx);

            // [start, end)
            for value in start..end {
                output_values.push(value);
                input_row_indices.push(row_idx as u32);
            }
        }

        let schema = self.return_type(&[DataType::Int64, DataType::Int64])?;
        let output_array = Int64Array::from(output_values);
        let output_batch =
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(output_array)])?;

        Ok(materialized_batch_stream(output_batch, input_row_indices))
    }
}

#[tokio::test]
async fn test_batched_table_function_basic() -> Result<()> {
    let func = Arc::new(GenerateRangeFunction::new());
    let batched_tf = BatchedTableFunction::new(func);

    // Single row: generate_range(10, 13) -> [10, 11, 12]
    let input_schema = Arc::new(Schema::new(vec![
        Field::new("start", DataType::Int64, false),
        Field::new("end", DataType::Int64, false),
    ]));

    let input_batch = RecordBatch::try_new(
        Arc::clone(&input_schema),
        vec![
            Arc::new(Int64Array::from(vec![10])),
            Arc::new(Int64Array::from(vec![13])),
        ],
    )?;

    let input_exec = Arc::new(TestExec::new(vec![input_batch]));

    let args = vec![
        Arc::new(Column::new("start", 0)) as Arc<dyn PhysicalExpr>,
        Arc::new(Column::new("end", 1)) as Arc<dyn PhysicalExpr>,
    ];

    let output_schema =
        Arc::new(batched_tf.return_type(&[DataType::Int64, DataType::Int64])?);
    let exec_plan =
        batched_tf.create_plan(args, input_exec, Arc::clone(&output_schema))?;

    let task_ctx = Arc::new(TaskContext::default());
    let mut stream = exec_plan.execute(0, task_ctx)?;

    let mut results = Vec::new();
    while let Some(batch) = stream.next().await {
        results.push(batch?);
    }

    assert!(!results.is_empty(), "Should have at least one batch");

    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3, "Should generate 3 values: 10, 11, 12");

    let first_batch = &results[0];
    assert_eq!(first_batch.num_columns(), 1);

    let array = first_batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    assert_eq!(array.value(0), 10);
    assert_eq!(array.value(1), 11);
    assert_eq!(array.value(2), 12);

    Ok(())
}

#[tokio::test]
async fn test_batched_table_function_multiple_input_rows() -> Result<()> {
    let func = Arc::new(GenerateRangeFunction::new());
    let batched_tf = BatchedTableFunction::new(func);

    // Two rows:
    // Row 0: generate_range(1, 3) -> [1, 2]
    // Row 1: generate_range(5, 7) -> [5, 6]
    let input_schema = Arc::new(Schema::new(vec![
        Field::new("start", DataType::Int64, false),
        Field::new("end", DataType::Int64, false),
    ]));

    let input_batch = RecordBatch::try_new(
        Arc::clone(&input_schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 5])),
            Arc::new(Int64Array::from(vec![3, 7])),
        ],
    )?;

    let input_exec = Arc::new(TestExec::new(vec![input_batch]));

    let args = vec![
        Arc::new(Column::new("start", 0)) as Arc<dyn PhysicalExpr>,
        Arc::new(Column::new("end", 1)) as Arc<dyn PhysicalExpr>,
    ];

    let output_schema =
        Arc::new(batched_tf.return_type(&[DataType::Int64, DataType::Int64])?);
    let exec_plan =
        batched_tf.create_plan(args, input_exec, Arc::clone(&output_schema))?;

    let task_ctx = Arc::new(TaskContext::default());
    let mut stream = exec_plan.execute(0, task_ctx)?;

    let mut results = Vec::new();
    while let Some(batch) = stream.next().await {
        results.push(batch?);
    }

    // [1, 2] from row 0, [5, 6] from row 1
    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 4);

    let first_batch = &results[0];
    let array = first_batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    // Values in order: 1, 2, 5, 6
    assert_eq!(array.value(0), 1);
    assert_eq!(array.value(1), 2);
    assert_eq!(array.value(2), 5);
    assert_eq!(array.value(3), 6);

    Ok(())
}

#[tokio::test]
async fn test_batched_table_function_registration() -> Result<()> {
    let ctx = SessionContext::new();

    {
        let state_ref = ctx.state_ref();
        let mut state = state_ref.write();
        state.register_batched_table_function(
            "my_range",
            Arc::new(GenerateRangeFunction::new()),
        );
    }

    {
        let state_ref = ctx.state_ref();
        let state = state_ref.read();
        let registered_func = state.batched_table_function("my_range");
        assert!(registered_func.is_some(), "Function should be registered");

        let func = registered_func.unwrap();
        assert_eq!(func.name(), "generate_range");
    }

    {
        let state_ref = ctx.state_ref();
        let mut state = state_ref.write();
        state.deregister_batched_table_function("my_range")?;
    }

    {
        let state_ref = ctx.state_ref();
        let state = state_ref.read();
        let after_dereg = state.batched_table_function("my_range");
        assert!(after_dereg.is_none(), "Function should be deregistered");
    }

    Ok(())
}

/// Simple test function for LATERAL testing
#[derive(Debug)]
struct DoubleFn {
    signature: Signature,
}

impl DoubleFn {
    fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Int64], Volatility::Immutable),
        }
    }
}

#[async_trait::async_trait]
impl BatchedTableFunctionImpl for DoubleFn {
    fn name(&self) -> &str {
        "double"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<Schema> {
        Ok(Schema::new(vec![Field::new(
            "doubled",
            DataType::Int64,
            true,
        )]))
    }

    async fn invoke_batch(
        &self,
        args: &[ArrayRef],
        _projection: Option<&[usize]>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<datafusion_catalog::BatchResultStream> {
        if args.is_empty() {
            return exec_err!("Expected at least one argument");
        }

        let input_array = args[0].as_primitive::<arrow::datatypes::Int64Type>();

        let output_array: Int64Array =
            input_array.iter().map(|v| v.map(|val| val * 2)).collect();

        let schema = self.return_type(&[DataType::Int64])?;
        let output_batch =
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(output_array)])?;

        // 1:1 mapping
        let input_row_indices: Vec<u32> = (0..input_array.len() as u32).collect();

        Ok(materialized_batch_stream(output_batch, input_row_indices))
    }
}

#[tokio::test]
async fn test_lateral_wildcard() -> Result<()> {
    let ctx = SessionContext::new();

    {
        let state_ref = ctx.state_ref();
        let mut state = state_ref.write();
        state.register_batched_table_function("double", Arc::new(DoubleFn::new()));
    }

    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
    )?;

    ctx.register_batch("numbers", batch)?;

    let df = ctx
        .sql("SELECT * FROM numbers CROSS JOIN LATERAL double(numbers.value)")
        .await?;

    let results = df.collect().await?;

    assert!(!results.is_empty());
    assert_eq!(results[0].num_columns(), 2);

    Ok(())
}

#[tokio::test]
async fn test_lateral_explicit_columns() -> Result<()> {
    let ctx = SessionContext::new();

    {
        let state_ref = ctx.state_ref();
        let mut state = state_ref.write();
        state.register_batched_table_function("double", Arc::new(DoubleFn::new()));
    }

    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
    )?;

    ctx.register_batch("numbers", batch)?;

    let df = ctx
        .sql(
            "SELECT value, doubled FROM numbers CROSS JOIN LATERAL double(numbers.value)",
        )
        .await?;

    let results = df.collect().await?;

    assert!(!results.is_empty());
    assert_eq!(results[0].num_columns(), 2);

    let value_array = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let doubled_array = results[0]
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    assert_eq!(value_array.len(), 3);
    assert_eq!(value_array.value(0), 1);
    assert_eq!(doubled_array.value(0), 2);

    Ok(())
}
