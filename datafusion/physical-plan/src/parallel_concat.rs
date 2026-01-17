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

//! Defines a parallel version of `concat_batches`.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::Result;
use futures::future::try_join_all;

/// Concatenates `RecordBatch`es by concatenating each column in parallel.
pub async fn parallel_concat_batches<'a>(
    schema: &SchemaRef,
    batches: &[&'a RecordBatch],
) -> Result<RecordBatch> {
    if batches.is_empty() {
        return Ok(RecordBatch::new_empty(Arc::clone(schema)));
    }

    let num_columns = schema.fields().len();
    let mut tasks = Vec::with_capacity(num_columns);

    for i in 0..num_columns {
        let column_arrays: Vec<ArrayRef> =
            batches.iter().map(|batch| batch.column(i).clone()).collect();

        let task = tokio::spawn(async move {
            let arrays_to_concat: Vec<&dyn Array> =
                column_arrays.iter().map(|a| a.as_ref()).collect();
            arrow::compute::concat(&arrays_to_concat)
        });
        tasks.push(task);
    }

    let task_outputs = try_join_all(tasks)
        .await
        .map_err(|e| {
            datafusion_common::DataFusionError::Execution(format!(
                "Tokio join error during parallel concatenation: {e}"
            ))
        })?;

    let columns = task_outputs
        .into_iter()
        .collect::<std::result::Result<Vec<_>, arrow::error::ArrowError>>()?;

    RecordBatch::try_new(Arc::clone(schema), columns).map_err(Into::into)
}
