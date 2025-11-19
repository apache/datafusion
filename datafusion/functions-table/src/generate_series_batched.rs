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

//! Batched implementation of generate_series table function
//!
//! This validates the BatchedTableFunctionImpl trait design.
//! The function receives already-evaluated arguments and just needs to
//! generate output rows.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion_catalog::{BatchResultChunk, BatchedTableFunctionImpl};
use datafusion_common::{exec_err, Result};
use datafusion_expr::Signature;

/// Batched generate_series table function
///
/// Generates a series of Int64 values from start to end (inclusive).
///
/// # Example
/// ```text
/// invoke([ScalarValue::Int64(1), ScalarValue::Int64(5)])
/// Returns: [[1], [2], [3], [4], [5]]
/// ```
#[derive(Debug)]
pub struct GenerateSeriesFunction {
    signature: Signature,
}

impl GenerateSeriesFunction {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Int64, DataType::Int64],
                datafusion_expr::Volatility::Immutable,
            ),
        }
    }
}

impl Default for GenerateSeriesFunction {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl BatchedTableFunctionImpl for GenerateSeriesFunction {
    fn name(&self) -> &str {
        "generate_series"
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
        args: &[ArrayRef],
        _projection: Option<&[usize]>,
        _filters: &[datafusion_expr::Expr],
        limit: Option<usize>,
    ) -> Result<datafusion_catalog::BatchResultStream> {
        if args.len() != 2 {
            return exec_err!("generate_series expects 2 arguments, got {}", args.len());
        }

        let start_array = args[0].as_primitive::<arrow::datatypes::Int64Type>();
        let end_array = args[1].as_primitive::<arrow::datatypes::Int64Type>();

        if start_array.len() != end_array.len() {
            return exec_err!("start and end arrays must have same length");
        }

        let mut ranges = Vec::new();
        for row_idx in 0..start_array.len() {
            if start_array.is_null(row_idx) {
                return exec_err!("generate_series start argument cannot be NULL");
            }
            if end_array.is_null(row_idx) {
                return exec_err!("generate_series end argument cannot be NULL");
            }

            let start = start_array.value(row_idx);
            let end = end_array.value(row_idx);

            ranges.push((start, end, row_idx as u32));
        }

        let schema = Arc::new(self.return_type(&[DataType::Int64, DataType::Int64])?);
        let stream = GenerateSeriesStream {
            schema,
            ranges,
            current_range_idx: 0,
            current_value: None,
            batch_size: 8192,
            limit,
            total_emitted: 0,
        };

        Ok(Box::pin(stream))
    }
}

/// Batched generator that yields batches of generated series values
struct GenerateSeriesStream {
    schema: Arc<Schema>,
    ranges: Vec<(i64, i64, u32)>, // (start, end, input_row_idx)
    current_range_idx: usize,
    current_value: Option<i64>,
    batch_size: usize,
    limit: Option<usize>,
    total_emitted: usize,
}

impl futures::stream::Stream for GenerateSeriesStream {
    type Item = Result<BatchResultChunk>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        // Check if we've reached the limit
        if let Some(limit) = self.limit {
            if self.total_emitted >= limit {
                return std::task::Poll::Ready(None);
            }
        }

        if self.current_range_idx >= self.ranges.len() {
            return std::task::Poll::Ready(None);
        }

        let mut output_values = Vec::with_capacity(self.batch_size);
        let mut input_row_indices = Vec::with_capacity(self.batch_size);

        // Calculate effective batch size considering limit
        let batch_size = match self.limit {
            Some(limit) => self.batch_size.min(limit - self.total_emitted),
            None => self.batch_size,
        };

        while output_values.len() < batch_size
            && self.current_range_idx < self.ranges.len()
        {
            let (start, end, input_row_idx) = self.ranges[self.current_range_idx];

            if self.current_value.is_none() {
                self.current_value = Some(start);
            }
            let current = self.current_value.as_mut().unwrap();

            while output_values.len() < batch_size && *current <= end {
                output_values.push(*current);
                input_row_indices.push(input_row_idx);
                *current += 1;
            }

            if *current > end {
                self.current_range_idx += 1;
                self.current_value = None;
            }
        }

        if output_values.is_empty() {
            return std::task::Poll::Ready(None);
        }

        // Update total emitted count
        self.total_emitted += output_values.len();

        let output_array = Int64Array::from(output_values);
        let output_batch = match RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![Arc::new(output_array)],
        ) {
            Ok(batch) => batch,
            Err(e) => {
                return std::task::Poll::Ready(Some(Err(
                    datafusion_common::DataFusionError::ArrowError(Box::new(e), None),
                )))
            }
        };

        std::task::Poll::Ready(Some(Ok(BatchResultChunk {
            output: output_batch,
            input_row_indices,
        })))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    #[test]
    fn test_function_metadata() {
        let function = GenerateSeriesFunction::new();

        assert_eq!(function.name(), "generate_series");

        let sig = function.signature();
        assert_eq!(sig.volatility, datafusion_expr::Volatility::Immutable);

        let schema = function
            .return_type(&[DataType::Int64, DataType::Int64])
            .unwrap();
        assert_eq!(schema.fields().len(), 1);
        assert_eq!(schema.field(0).name(), "value");
        assert_eq!(schema.field(0).data_type(), &DataType::Int64);
    }

    #[tokio::test]
    async fn test_invoke_basic() {
        let function = GenerateSeriesFunction::new();

        // generate_series(1, 5)
        let start_array = Int64Array::from(vec![1]);
        let end_array = Int64Array::from(vec![5]);
        let args: Vec<ArrayRef> = vec![Arc::new(start_array), Arc::new(end_array)];

        let mut stream = function.invoke_batch(&args, None, &[], None).await.unwrap();
        let chunk = stream.next().await.unwrap().unwrap();

        assert_eq!(chunk.output.num_rows(), 5);
        assert_eq!(chunk.input_row_indices, vec![0, 0, 0, 0, 0]);

        let output_array = chunk
            .output
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        for (i, expected) in (1..=5).enumerate() {
            assert_eq!(output_array.value(i), expected);
        }
    }

    #[tokio::test]
    async fn test_invoke_empty_range() {
        let function = GenerateSeriesFunction::new();

        // start > end
        let start_array = Int64Array::from(vec![5]);
        let end_array = Int64Array::from(vec![1]);
        let args: Vec<ArrayRef> = vec![Arc::new(start_array), Arc::new(end_array)];

        let mut stream = function.invoke_batch(&args, None, &[], None).await.unwrap();
        let chunk = stream.next().await;

        assert!(chunk.is_none());
    }

    #[tokio::test]
    async fn test_invoke_single_value() {
        let function = GenerateSeriesFunction::new();

        let start_array = Int64Array::from(vec![3]);
        let end_array = Int64Array::from(vec![3]);
        let args: Vec<ArrayRef> = vec![Arc::new(start_array), Arc::new(end_array)];

        let mut stream = function.invoke_batch(&args, None, &[], None).await.unwrap();
        let chunk = stream.next().await.unwrap().unwrap();

        assert_eq!(chunk.output.num_rows(), 1);
        assert_eq!(chunk.input_row_indices, vec![0]);

        let output_array = chunk
            .output
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(output_array.value(0), 3);
    }

    #[tokio::test]
    async fn test_invoke_null_argument() {
        let function = GenerateSeriesFunction::new();

        let start_array = Int64Array::from(vec![None]);
        let end_array = Int64Array::from(vec![Some(5)]);
        let args: Vec<ArrayRef> = vec![Arc::new(start_array), Arc::new(end_array)];

        let result = function.invoke_batch(&args, None, &[], None).await;
        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(err.to_string().contains("cannot be NULL"));
    }

    #[tokio::test]
    async fn test_invoke_wrong_arg_count() {
        let function = GenerateSeriesFunction::new();

        let start_array = Int64Array::from(vec![1]);
        let args: Vec<ArrayRef> = vec![Arc::new(start_array)];

        let result = function.invoke_batch(&args, None, &[], None).await;
        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(err.to_string().contains("expects 2 arguments"));
    }

    #[tokio::test]
    async fn test_limit_pushdown() {
        use futures::StreamExt;

        let function = GenerateSeriesFunction::new();

        // Generate series 1 to 10000, but with limit 10
        let start_array = Int64Array::from(vec![1]);
        let end_array = Int64Array::from(vec![10000]);
        let args: Vec<ArrayRef> = vec![Arc::new(start_array), Arc::new(end_array)];

        let mut stream = function
            .invoke_batch(&args, None, &[], Some(10))
            .await
            .unwrap();

        let mut total_rows = 0;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.unwrap();
            total_rows += chunk.output.num_rows();
        }

        // With limit pushdown, should only generate 10 rows instead of 10000
        assert_eq!(
            total_rows, 10,
            "Limit pushdown should generate exactly 10 rows"
        );
    }

    #[tokio::test]
    async fn test_projection_parameter() {
        use futures::StreamExt;

        let function = GenerateSeriesFunction::new();

        // Generate series with projection (column 0)
        let start_array = Int64Array::from(vec![1]);
        let end_array = Int64Array::from(vec![5]);
        let args: Vec<ArrayRef> = vec![Arc::new(start_array), Arc::new(end_array)];

        // Pass projection - even though we only have 1 column, verify it's accepted
        let projection = Some(&[0usize][..]);
        let mut stream = function
            .invoke_batch(&args, projection, &[], None)
            .await
            .unwrap();

        let mut total_rows = 0;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.unwrap();
            total_rows += chunk.output.num_rows();

            // Verify we still get the expected output
            let output_array = chunk
                .output
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();

            for i in 0..chunk.output.num_rows() {
                assert!(output_array.value(i) >= 1 && output_array.value(i) <= 5);
            }
        }

        assert_eq!(total_rows, 5, "Should generate 5 rows with projection");
    }

    #[tokio::test]
    async fn test_empty_projection() {
        use futures::StreamExt;

        let function = GenerateSeriesFunction::new();

        let start_array = Int64Array::from(vec![1]);
        let end_array = Int64Array::from(vec![3]);
        let args: Vec<ArrayRef> = vec![Arc::new(start_array), Arc::new(end_array)];

        // Pass None for projection (all columns)
        let mut stream = function.invoke_batch(&args, None, &[], None).await.unwrap();

        let chunk = stream.next().await.unwrap().unwrap();
        assert_eq!(chunk.output.num_rows(), 3);
        assert_eq!(chunk.output.num_columns(), 1);
    }
}
