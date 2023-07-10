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

//! Defines the unnest column plan for unnesting values in a column that contains a list
//! type, conceptually is like joining each row with all the values in the list column.
use arrow::array::{
    new_null_array, Array, ArrayAccessor, ArrayRef, FixedSizeListArray, LargeListArray,
    ListArray,
};
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion_execution::TaskContext;
use futures::Stream;
use futures::StreamExt;
use log::trace;
use std::time::Instant;
use std::{any::Any, sync::Arc};

use crate::physical_plan::{
    coalesce_batches::concat_batches, expressions::Column, DisplayFormatType,
    Distribution, EquivalenceProperties, ExecutionPlan, Partitioning, PhysicalExpr,
    PhysicalSortExpr, RecordBatchStream, SendableRecordBatchStream, Statistics,
};
use datafusion_common::{DataFusionError, Result, ScalarValue};

use super::DisplayAs;

/// Unnest the given column by joining the row with each value in the nested type.
#[derive(Debug)]
pub struct UnnestExec {
    /// Input execution plan
    input: Arc<dyn ExecutionPlan>,
    /// The schema once the unnest is applied
    schema: SchemaRef,
    /// The unnest column
    column: Column,
}

impl UnnestExec {
    /// Create a new [UnnestExec].
    pub fn new(input: Arc<dyn ExecutionPlan>, column: Column, schema: SchemaRef) -> Self {
        UnnestExec {
            input,
            schema,
            column,
        }
    }
}

impl DisplayAs for UnnestExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "UnnestExec")
            }
        }
    }
}

impl ExecutionPlan for UnnestExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    /// Specifies whether this plan generates an infinite stream of records.
    /// If the plan does not support pipelining, but its input(s) are
    /// infinite, returns an error to indicate this.
    fn unbounded_output(&self, children: &[bool]) -> Result<bool> {
        Ok(children[0])
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(UnnestExec::new(
            children[0].clone(),
            self.column.clone(),
            self.schema.clone(),
        )))
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::UnspecifiedDistribution]
    }

    fn output_partitioning(&self) -> Partitioning {
        self.input.output_partitioning()
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn equivalence_properties(&self) -> EquivalenceProperties {
        self.input.equivalence_properties()
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;

        Ok(Box::pin(UnnestStream {
            input,
            schema: self.schema.clone(),
            column: self.column.clone(),
            num_input_batches: 0,
            num_input_rows: 0,
            num_output_batches: 0,
            num_output_rows: 0,
            unnest_time: 0,
        }))
    }

    fn statistics(&self) -> Statistics {
        Default::default()
    }
}

/// A stream that issues [RecordBatch]es with unnested column data.
struct UnnestStream {
    /// Input stream
    input: SendableRecordBatchStream,
    /// Unnested schema
    schema: Arc<Schema>,
    /// The unnest column
    column: Column,
    /// number of input batches
    num_input_batches: usize,
    /// number of input rows
    num_input_rows: usize,
    /// number of batches produced
    num_output_batches: usize,
    /// number of rows produced
    num_output_rows: usize,
    /// total time for column unnesting, in ms
    unnest_time: usize,
}

impl RecordBatchStream for UnnestStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[async_trait]
impl Stream for UnnestStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.poll_next_impl(cx)
    }
}

impl UnnestStream {
    /// Separate implementation function that unpins the [`UnnestStream`] so
    /// that partial borrows work correctly
    fn poll_next_impl(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<RecordBatch>>> {
        self.input
            .poll_next_unpin(cx)
            .map(|maybe_batch| match maybe_batch {
                Some(Ok(batch)) => {
                    let start = Instant::now();
                    let result = build_batch(&batch, &self.schema, &self.column);
                    self.num_input_batches += 1;
                    self.num_input_rows += batch.num_rows();
                    if let Ok(ref batch) = result {
                        self.unnest_time += start.elapsed().as_millis() as usize;
                        self.num_output_batches += 1;
                        self.num_output_rows += batch.num_rows();
                    }

                    Some(result)
                }
                other => {
                    trace!(
                        "Processed {} probe-side input batches containing {} rows and \
                        produced {} output batches containing {} rows in {} ms",
                        self.num_input_batches,
                        self.num_input_rows,
                        self.num_output_batches,
                        self.num_output_rows,
                        self.unnest_time,
                    );
                    other
                }
            })
    }
}

fn build_batch(
    batch: &RecordBatch,
    schema: &SchemaRef,
    column: &Column,
) -> Result<RecordBatch> {
    let list_array = column.evaluate(batch)?.into_array(batch.num_rows());
    match list_array.data_type() {
        arrow::datatypes::DataType::List(_) => {
            let list_array = list_array.as_any().downcast_ref::<ListArray>().unwrap();
            unnest_batch(batch, schema, column, &list_array)
        }
        arrow::datatypes::DataType::LargeList(_) => {
            let list_array = list_array
                .as_any()
                .downcast_ref::<LargeListArray>()
                .unwrap();
            unnest_batch(batch, schema, column, &list_array)
        }
        arrow::datatypes::DataType::FixedSizeList(_, _) => {
            let list_array = list_array
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .unwrap();
            unnest_batch(batch, schema, column, list_array)
        }
        _ => Err(DataFusionError::Execution(format!(
            "Invalid unnest column {column}"
        ))),
    }
}

fn unnest_batch<T>(
    batch: &RecordBatch,
    schema: &SchemaRef,
    column: &Column,
    list_array: &T,
) -> Result<RecordBatch>
where
    T: ArrayAccessor<Item = ArrayRef>,
{
    let mut batches = Vec::new();
    let mut num_rows = 0;

    for row in 0..batch.num_rows() {
        let arrays = batch
            .columns()
            .iter()
            .enumerate()
            .map(|(col_idx, arr)| {
                if col_idx == column.index() {
                    // Unnest the value at the given row.
                    if list_array.value(row).is_empty() {
                        // If nested array is empty add an array with 1 null.
                        Ok(new_null_array(list_array.value(row).data_type(), 1))
                    } else {
                        Ok(list_array.value(row))
                    }
                } else {
                    // Number of elements to duplicate, use max(1) to handle null.
                    let nested_len = list_array.value(row).len().max(1);
                    // Duplicate rows for each value in the nested array.
                    if arr.is_null(row) {
                        Ok(new_null_array(arr.data_type(), nested_len))
                    } else {
                        let scalar = ScalarValue::try_from_array(arr, row)?;
                        Ok(scalar.to_array_of_size(nested_len))
                    }
                }
            })
            .collect::<Result<Vec<_>>>()?;

        let rb = RecordBatch::try_new(schema.clone(), arrays.to_vec())?;
        num_rows += rb.num_rows();
        batches.push(rb);
    }

    concat_batches(schema, &batches, num_rows).map_err(Into::into)
}
