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

//! Helper functions for table function implementations

use arrow::array::RecordBatch;
use datafusion_common::{exec_err, Result};
use std::sync::Arc;

/// Create a stream that yields a single batch result chunk
///
/// This is a convenience helper for simple table functions that materialize
/// all results upfront and want to return them as a stream.
///
/// # Arguments
/// * `output` - The generated rows as a RecordBatch
/// * `input_row_indices` - Maps each output row to its input row
///
/// # Example
/// ```
/// use datafusion_catalog::batched_function::helpers::materialized_batch_stream;
/// use datafusion_catalog::BatchResultChunk;
/// use arrow::array::Int64Array;
/// use arrow::array::RecordBatch;
/// use arrow::datatypes::{Schema, Field, DataType};
/// use std::sync::Arc;
///
/// # async fn example() {
/// let output = RecordBatch::try_new(
///     Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)])),
///     vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
/// ).unwrap();
/// let indices = vec![0, 0, 0]; // All from input row 0
///
/// let stream = materialized_batch_stream(output, indices);
/// // Stream yields one chunk and then completes
/// # }
/// ```
pub fn materialized_batch_stream(
    output: RecordBatch,
    input_row_indices: Vec<u32>,
) -> crate::BatchResultStream {
    use crate::BatchResultChunk;
    use futures::stream;
    Box::pin(stream::once(async move {
        Ok(BatchResultChunk {
            output,
            input_row_indices,
        })
    }))
}

/// Combine input batch with output batch for LATERAL join
///
/// For each output row, duplicates the corresponding input row (based on
/// input_row_indices) and concatenates it with the output row.
///
/// # Arguments
/// * `input_batch` - The input rows (from the outer table)
/// * `output_batch` - The generated rows (from the table function)
/// * `input_row_indices` - Maps each output row to its source input row
///
/// # Returns
/// A RecordBatch with schema [input_columns..., output_columns...]
///
/// # Example
/// ```text
/// Input (2 rows):
///   id | value
///   1  | 10
///   2  | 20
///
/// Output (4 rows):
///   n
///   1
///   2
///   3
///   4
///
/// input_row_indices = [0, 0, 1, 1]
///
/// Result (4 rows):
///   id | value | n
///   1  | 10    | 1
///   1  | 10    | 2
///   2  | 20    | 3
///   2  | 20    | 4
/// ```
pub fn combine_lateral_result(
    input_batch: &RecordBatch,
    output_batch: RecordBatch,
    input_row_indices: &[u32],
) -> Result<RecordBatch> {
    if input_row_indices.len() != output_batch.num_rows() {
        return exec_err!(
            "input_row_indices length ({}) != output batch rows ({})",
            input_row_indices.len(),
            output_batch.num_rows()
        );
    }

    let take_indices = arrow::array::UInt32Array::from(input_row_indices.to_vec());

    let mut combined_columns =
        Vec::with_capacity(input_batch.num_columns() + output_batch.num_columns());

    for col in input_batch.columns() {
        let taken = arrow::compute::take(col.as_ref(), &take_indices, None)?;
        combined_columns.push(taken);
    }

    for col in output_batch.columns() {
        combined_columns.push(Arc::clone(col));
    }

    let mut combined_fields = input_batch.schema().fields().to_vec();
    combined_fields.extend_from_slice(output_batch.schema().fields());
    let combined_schema = Arc::new(arrow::datatypes::Schema::new(combined_fields));

    Ok(RecordBatch::try_new(combined_schema, combined_columns)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};

    #[test]
    fn test_combine_lateral_result() {
        // Input batch: 2 rows with columns (id, value)
        let input_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
        ]));
        let input_batch = RecordBatch::try_new(
            input_schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(Int64Array::from(vec![10, 20])),
            ],
        )
        .unwrap();

        // Output batch: 4 rows with column (n)
        let output_schema =
            Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
        let output_batch = RecordBatch::try_new(
            output_schema,
            vec![Arc::new(Int64Array::from(vec![100, 101, 200, 201]))],
        )
        .unwrap();

        let input_row_indices = vec![0, 0, 1, 1];

        let result =
            combine_lateral_result(&input_batch, output_batch, &input_row_indices)
                .unwrap();

        assert_eq!(result.num_columns(), 3);
        assert_eq!(result.num_rows(), 4);

        assert_eq!(result.schema().field(0).name(), "id");
        assert_eq!(result.schema().field(1).name(), "value");
        assert_eq!(result.schema().field(2).name(), "n");

        let id_col = result
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let value_col = result
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let n_col = result
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        // Row 0: id=1, value=10, n=100
        assert_eq!(id_col.value(0), 1);
        assert_eq!(value_col.value(0), 10);
        assert_eq!(n_col.value(0), 100);

        // Row 1: id=1, value=10, n=101
        assert_eq!(id_col.value(1), 1);
        assert_eq!(value_col.value(1), 10);
        assert_eq!(n_col.value(1), 101);

        // Row 2: id=2, value=20, n=200
        assert_eq!(id_col.value(2), 2);
        assert_eq!(value_col.value(2), 20);
        assert_eq!(n_col.value(2), 200);

        // Row 3: id=2, value=20, n=201
        assert_eq!(id_col.value(3), 2);
        assert_eq!(value_col.value(3), 20);
        assert_eq!(n_col.value(3), 201);
    }

    #[test]
    fn test_combine_lateral_result_mismatched_lengths() {
        let input_schema =
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let input_batch =
            RecordBatch::try_new(input_schema, vec![Arc::new(Int64Array::from(vec![1]))])
                .unwrap();

        let output_schema =
            Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
        let output_batch = RecordBatch::try_new(
            output_schema,
            vec![Arc::new(Int64Array::from(vec![100, 101]))],
        )
        .unwrap();

        // Wrong: indices has length 3 but output has 2 rows
        let input_row_indices = vec![0, 0, 0];

        let result =
            combine_lateral_result(&input_batch, output_batch, &input_row_indices);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("input_row_indices length"));
    }
}
