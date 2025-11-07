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

//! ExecutionPlan for BatchedTableFunction
//!
//! This provides the generic wrapper that evaluates arguments and calls invoke_batch()

use std::any::Any;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::{exec_err, Result};
use datafusion_execution::SendableRecordBatchStream;
use datafusion_expr::Expr;
use datafusion_physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    RecordBatchStream,
};
use futures::stream::{Stream, StreamExt};

use crate::batched_function::helpers::combine_lateral_result;
use crate::{BatchResultChunk, BatchResultStream, BatchedTableFunctionImpl};

/// Execution mode for batched table functions
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchedTableFunctionMode {
    /// Standalone mode: function executed once, output is only function columns
    Standalone,
    /// Lateral mode: function executed per input row, output is input + function columns
    Lateral,
}

/// ExecutionPlan that wraps a BatchedTableFunctionImpl
///
/// This evaluates the argument expressions once per input batch,
/// calls invoke_batch() to get a stream of output chunks, and produces output batches.
///
/// In Lateral mode, this combines each input row with the function's output rows
/// using the input_row_indices mapping from BatchResultChunk.
#[derive(Debug)]
pub struct BatchedTableFunctionExec {
    func: Arc<dyn BatchedTableFunctionImpl>,
    args: Vec<Arc<dyn PhysicalExpr>>,
    input: Arc<dyn ExecutionPlan>,
    /// The final output schema of this exec
    projected_schema: SchemaRef,
    /// Table function output schema (function columns only, no input columns)
    table_function_schema: SchemaRef,
    mode: BatchedTableFunctionMode,
    /// Optional column projection (indices into function output schema)
    projection: Option<Vec<usize>>,
    /// Logical filter expressions to pass to invoke_batch
    filters: Vec<Expr>,
    /// Optional limit pushed down from optimizer
    limit: Option<usize>,
    properties: PlanProperties,
}

impl BatchedTableFunctionExec {
    pub fn new(
        func: Arc<dyn BatchedTableFunctionImpl>,
        args: Vec<Arc<dyn PhysicalExpr>>,
        input: Arc<dyn ExecutionPlan>,
        projected_schema: SchemaRef,
        table_function_schema: SchemaRef,
        mode: BatchedTableFunctionMode,
        projection: Option<Vec<usize>>,
        filters: Vec<Expr>,
        limit: Option<usize>,
    ) -> Result<Self> {
        let eq_properties = EquivalenceProperties::new(Arc::clone(&projected_schema));

        let properties = PlanProperties::new(
            eq_properties,
            input.output_partitioning().clone(),
            input.pipeline_behavior(),
            input.boundedness(),
        );

        Ok(Self {
            func,
            args,
            input,
            projected_schema,
            table_function_schema,
            mode,
            projection,
            filters,
            limit,
            properties,
        })
    }
}

impl DisplayAs for BatchedTableFunctionExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(f, "BatchedTableFunctionExec")?;
                write!(f, ": function={}", self.func.name())?;
                if !self.args.is_empty() {
                    write!(f, ", args=[")?;
                    for (i, arg) in self.args.iter().enumerate() {
                        if i > 0 {
                            write!(f, ", ")?;
                        }
                        write!(f, "{arg}")?;
                    }
                    write!(f, "]")?;
                }
                if let Some(proj) = &self.projection {
                    write!(f, ", projection={proj:?}")?;
                }
                if !self.filters.is_empty() {
                    write!(f, ", filters=[")?;
                    for (i, filter) in self.filters.iter().enumerate() {
                        if i > 0 {
                            write!(f, ", ")?;
                        }
                        write!(f, "{filter}")?;
                    }
                    write!(f, "]")?;
                }
                Ok(())
            }
            DisplayFormatType::Verbose => {
                write!(f, "BatchedTableFunctionExec: function={}", self.func.name())?;
                if !self.args.is_empty() {
                    write!(f, ", args=[")?;
                    for (i, arg) in self.args.iter().enumerate() {
                        if i > 0 {
                            write!(f, ", ")?;
                        }
                        write!(f, "{arg}")?;
                    }
                    write!(f, "]")?;
                }
                if let Some(proj) = &self.projection {
                    write!(f, ", projection={proj:?}")?;
                }
                if !self.filters.is_empty() {
                    write!(f, ", filters=[")?;
                    for (i, filter) in self.filters.iter().enumerate() {
                        if i > 0 {
                            write!(f, ", ")?;
                        }
                        write!(f, "{filter}")?;
                    }
                    write!(f, "]")?;
                }
                Ok(())
            }
            DisplayFormatType::TreeRender => {
                // TreeRender is used for the box diagram display
                // Only write details, not the node name (that's automatic)
                write!(f, "function: {}", self.func.name())?;
                if !self.args.is_empty() {
                    writeln!(f)?;
                    write!(f, "args: [")?;
                    for (i, arg) in self.args.iter().enumerate() {
                        if i > 0 {
                            write!(f, ", ")?;
                        }
                        write!(f, "{arg}")?;
                    }
                    write!(f, "]")?;
                }
                if let Some(proj) = &self.projection {
                    writeln!(f)?;
                    write!(f, "projection: {proj:?}")?;
                }
                if !self.filters.is_empty() {
                    writeln!(f)?;
                    write!(f, "filters: [")?;
                    for (i, filter) in self.filters.iter().enumerate() {
                        if i > 0 {
                            write!(f, ", ")?;
                        }
                        write!(f, "{filter}")?;
                    }
                    write!(f, "]")?;
                }
                Ok(())
            }
        }
    }
}

impl ExecutionPlan for BatchedTableFunctionExec {
    fn name(&self) -> &str {
        "BatchedTableFunctionExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return exec_err!("BatchedTableFunctionExec expects exactly 1 child");
        }

        Ok(Arc::new(Self::new(
            Arc::clone(&self.func),
            self.args.clone(),
            Arc::clone(&children[0]),
            Arc::clone(&self.projected_schema),
            Arc::clone(&self.table_function_schema),
            self.mode,
            self.projection.clone(),
            self.filters.clone(),
            self.limit,
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion_execution::TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input_stream = self.input.execute(partition, context)?;

        Ok(Box::pin(BatchedTableFunctionStream {
            func: Arc::clone(&self.func),
            args: self.args.clone(),
            input_stream,
            schema: Arc::clone(&self.projected_schema),
            mode: self.mode,
            projection: self.projection.clone(),
            filters: self.filters.clone(),
            limit: self.limit,
            state: StreamState::AwaitingInput,
        }))
    }
}

enum StreamState {
    /// Waiting for the next input batch from input_stream
    AwaitingInput,
    /// Polling the invoke_batch future
    Invoking {
        future: Pin<Box<dyn Future<Output = Result<BatchResultStream>> + Send>>,
        input_batch: RecordBatch,
    },
    /// Polling the result stream from invoke_batch
    Streaming {
        stream: BatchResultStream,
        input_batch: RecordBatch,
    },
}

struct BatchedTableFunctionStream {
    func: Arc<dyn BatchedTableFunctionImpl>,
    args: Vec<Arc<dyn PhysicalExpr>>,
    input_stream: SendableRecordBatchStream,
    schema: SchemaRef,
    mode: BatchedTableFunctionMode,
    projection: Option<Vec<usize>>,
    filters: Vec<Expr>,
    limit: Option<usize>,
    state: StreamState,
}

impl BatchedTableFunctionStream {
    /// Process a chunk from the inner stream
    fn process_chunk(
        &self,
        chunk: BatchResultChunk,
        input_batch: &RecordBatch,
    ) -> Result<RecordBatch> {
        match self.mode {
            BatchedTableFunctionMode::Lateral => combine_lateral_result(
                input_batch,
                chunk.output,
                &chunk.input_row_indices,
            ),
            BatchedTableFunctionMode::Standalone => Ok(chunk.output),
        }
    }
}

impl Stream for BatchedTableFunctionStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        loop {
            match std::mem::replace(&mut self.state, StreamState::AwaitingInput) {
                StreamState::AwaitingInput => {
                    // Get next input batch from input_stream
                    match self.input_stream.poll_next_unpin(cx) {
                        Poll::Ready(Some(Ok(batch))) => {
                            // Evaluate arguments
                            let arg_arrays: Vec<arrow::array::ArrayRef> = match self
                                .args
                                .iter()
                                .map(|expr| {
                                    expr.evaluate(&batch)?.into_array(batch.num_rows())
                                })
                                .collect::<Result<Vec<_>>>()
                            {
                                Ok(arrays) => arrays,
                                Err(e) => return Poll::Ready(Some(Err(e))),
                            };

                            // Create future for invoke_batch
                            let func = Arc::clone(&self.func);
                            let projection = self.projection.clone();
                            let filters = self.filters.clone();
                            let limit = self.limit;

                            let future = Box::pin(async move {
                                func.invoke_batch(
                                    &arg_arrays,
                                    projection.as_deref(),
                                    &filters,
                                    limit,
                                )
                                .await
                            });

                            self.state = StreamState::Invoking {
                                future,
                                input_batch: batch,
                            };
                            continue;
                        }
                        Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                        Poll::Ready(None) => return Poll::Ready(None),
                        Poll::Pending => {
                            self.state = StreamState::AwaitingInput;
                            return Poll::Pending;
                        }
                    }
                }
                StreamState::Invoking {
                    mut future,
                    input_batch,
                } => {
                    // Poll the invoke_batch future
                    match future.as_mut().poll(cx) {
                        Poll::Ready(Ok(stream)) => {
                            self.state = StreamState::Streaming {
                                stream,
                                input_batch,
                            };
                            continue;
                        }
                        Poll::Ready(Err(e)) => return Poll::Ready(Some(Err(e))),
                        Poll::Pending => {
                            self.state = StreamState::Invoking {
                                future,
                                input_batch,
                            };
                            return Poll::Pending;
                        }
                    }
                }
                StreamState::Streaming {
                    mut stream,
                    input_batch,
                } => {
                    // Poll the result stream
                    match stream.poll_next_unpin(cx) {
                        Poll::Ready(Some(Ok(chunk))) => {
                            match self.process_chunk(chunk, &input_batch) {
                                Ok(batch) => {
                                    self.state = StreamState::Streaming {
                                        stream,
                                        input_batch,
                                    };
                                    return Poll::Ready(Some(Ok(batch)));
                                }
                                Err(e) => return Poll::Ready(Some(Err(e))),
                            }
                        }
                        Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                        Poll::Ready(None) => {
                            // Stream finished, go back to awaiting next input
                            self.state = StreamState::AwaitingInput;
                            continue;
                        }
                        Poll::Pending => {
                            self.state = StreamState::Streaming {
                                stream,
                                input_batch,
                            };
                            return Poll::Pending;
                        }
                    }
                }
            }
        }
    }
}

impl RecordBatchStream for BatchedTableFunctionStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl fmt::Debug for BatchedTableFunctionStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BatchedTableFunctionStream")
            .field("func", &self.func.name())
            .field("schema", &self.schema)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::batched_function::helpers::materialized_batch_stream;
    use crate::BatchResultStream;
    use arrow::array::{Array, AsArray, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::Result;
    use datafusion_expr::{Expr, Signature, Volatility};
    use futures::StreamExt;

    /// Simple test function: doubles the input value
    #[derive(Debug)]
    struct DoubleFunction {
        signature: Signature,
    }

    impl DoubleFunction {
        fn new() -> Self {
            Self {
                signature: Signature::exact(vec![DataType::Int64], Volatility::Immutable),
            }
        }
    }

    #[async_trait::async_trait]
    impl BatchedTableFunctionImpl for DoubleFunction {
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
                true, // nullable
            )]))
        }

        async fn invoke_batch(
            &self,
            args: &[arrow::array::ArrayRef],
            _projection: Option<&[usize]>,
            _filters: &[Expr],
            _limit: Option<usize>,
        ) -> Result<BatchResultStream> {
            if args.len() != 1 {
                return exec_err!("double expects 1 argument");
            }

            let input_array = args[0].as_primitive::<arrow::datatypes::Int64Type>();

            let output_array: Int64Array =
                input_array.iter().map(|v| v.map(|val| val * 2)).collect();

            let schema = self.return_type(&[DataType::Int64])?;
            let output_batch =
                RecordBatch::try_new(Arc::new(schema), vec![Arc::new(output_array)])?;

            let input_row_indices: Vec<u32> = (0..input_array.len() as u32).collect();

            Ok(materialized_batch_stream(output_batch, input_row_indices))
        }
    }

    #[tokio::test]
    async fn test_double_function() -> Result<()> {
        let func = DoubleFunction::new();

        let input_array = Int64Array::from(vec![5]);
        let args: Vec<arrow::array::ArrayRef> = vec![Arc::new(input_array)];

        let mut stream = func.invoke_batch(&args, None, &[], None).await?;
        let chunk = stream.next().await.unwrap()?;

        assert_eq!(chunk.output.num_rows(), 1);
        assert_eq!(chunk.input_row_indices, vec![0]);

        let output_array = chunk
            .output
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(output_array.value(0), 10);

        Ok(())
    }

    #[tokio::test]
    async fn test_double_function_null() -> Result<()> {
        let func = DoubleFunction::new();

        let input_array = Int64Array::from(vec![None]);
        let args: Vec<arrow::array::ArrayRef> = vec![Arc::new(input_array)];

        let mut stream = func.invoke_batch(&args, None, &[], None).await?;
        let chunk = stream.next().await.unwrap()?;

        assert_eq!(chunk.output.num_rows(), 1);
        let output_array = chunk
            .output
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert!(output_array.is_null(0));

        Ok(())
    }

    /// Function that generates N copies of the input
    #[derive(Debug)]
    struct ReplicateFunction {
        signature: Signature,
    }

    impl ReplicateFunction {
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
    impl BatchedTableFunctionImpl for ReplicateFunction {
        fn name(&self) -> &str {
            "replicate"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, _arg_types: &[DataType]) -> Result<Schema> {
            Ok(Schema::new(vec![Field::new(
                "value",
                DataType::Int64,
                false,
            )]))
        }

        async fn invoke_batch(
            &self,
            args: &[arrow::array::ArrayRef],
            _projection: Option<&[usize]>,
            _filters: &[Expr],
            _limit: Option<usize>,
        ) -> Result<BatchResultStream> {
            if args.len() != 2 {
                return exec_err!("replicate expects 2 arguments");
            }

            let value_array = args[0].as_primitive::<arrow::datatypes::Int64Type>();
            let count_array = args[1].as_primitive::<arrow::datatypes::Int64Type>();

            if value_array.len() != count_array.len() {
                return exec_err!("value and count arrays must have same length");
            }

            let mut output_values = Vec::new();
            let mut input_row_indices = Vec::new();

            for row_idx in 0..value_array.len() {
                if value_array.is_null(row_idx) || count_array.is_null(row_idx) {
                    continue;
                }

                let value = value_array.value(row_idx);
                let count = count_array.value(row_idx) as usize;

                for _ in 0..count {
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
    async fn test_replicate_function() -> Result<()> {
        let func = ReplicateFunction::new();

        let value_array = Int64Array::from(vec![5]);
        let count_array = Int64Array::from(vec![3]);
        let args: Vec<arrow::array::ArrayRef> =
            vec![Arc::new(value_array), Arc::new(count_array)];

        let mut stream = func.invoke_batch(&args, None, &[], None).await?;
        let chunk = stream.next().await.unwrap()?;

        assert_eq!(chunk.output.num_rows(), 3);
        assert_eq!(chunk.input_row_indices, vec![0, 0, 0]); // All from input row 0

        let output_array = chunk
            .output
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        for i in 0..3 {
            assert_eq!(output_array.value(i), 5);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_replicate_function_zero_count() -> Result<()> {
        let func = ReplicateFunction::new();

        let value_array = Int64Array::from(vec![5]);
        let count_array = Int64Array::from(vec![0]);
        let args: Vec<arrow::array::ArrayRef> =
            vec![Arc::new(value_array), Arc::new(count_array)];

        let mut stream = func.invoke_batch(&args, None, &[], None).await?;
        let chunk = stream.next().await.unwrap()?;

        assert_eq!(chunk.output.num_rows(), 0);
        assert_eq!(chunk.input_row_indices.len(), 0);

        Ok(())
    }
}
