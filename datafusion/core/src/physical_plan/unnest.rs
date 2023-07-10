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
    new_null_array, Array, ArrayAccessor, ArrayRef, ArrowPrimitiveType,
    FixedSizeListArray, LargeListArray, ListArray, PrimitiveArray,
};
use arrow::compute::kernels;
use arrow::datatypes::{
    ArrowNativeType, ArrowNativeTypeOp, DataType, Int32Type, Int64Type, Schema, SchemaRef,
};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion_common::{cast::as_primitive_array, DataFusionError, Result};
use datafusion_execution::TaskContext;
use futures::Stream;
use futures::StreamExt;
use log::trace;
use std::time::Instant;
use std::{any::Any, sync::Arc};

use crate::physical_plan::{
    expressions::Column, DisplayFormatType, Distribution, EquivalenceProperties,
    ExecutionPlan, Partitioning, PhysicalExpr, PhysicalSortExpr, RecordBatchStream,
    SendableRecordBatchStream, Statistics,
};

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
        DataType::List(_) => {
            let list_array = list_array.as_any().downcast_ref::<ListArray>().unwrap();
            unnest_batch(batch, schema, column, &list_array)
        }
        DataType::LargeList(_) => {
            let list_array = list_array
                .as_any()
                .downcast_ref::<LargeListArray>()
                .unwrap();
            unnest_batch(batch, schema, column, &list_array)
        }
        DataType::FixedSizeList(_, _) => {
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
    // Create an array with the unnested values of the list array, given the list
    // array:
    //
    //   [1], null, [2, 3, 4], null, [5, 6]
    //
    // the result array is:
    //
    //   1, null, 2, 3, 4, null, 5, 6
    //
    let unnested_array = unnest_array(list_array)?;

    // Create an array with the lengths of each list value in the nested array.
    // Given the nested array:
    //
    //   [1], null, [2, 3, 4], null, [5, 6]
    //
    // the result array is:
    //
    //   1, null, 3, null, 2
    //
    // Depending on the list type the result may be Int32Array or Int64Array.
    let list_lengths = kernels::length::length(list_array)?;

    // Create the indices for the take kernel and then use those indices to create
    // the unnested record batch.
    match list_lengths.data_type() {
        DataType::Int32 => {
            let list_lengths = as_primitive_array::<Int32Type>(&list_lengths)?;
            let indices = create_take_indices(list_lengths, unnested_array.len());
            batch_from_indices(batch, schema, column.index(), &unnested_array, &indices)
        }
        DataType::Int64 => {
            let list_lengths = as_primitive_array::<Int64Type>(&list_lengths)?;
            let indices = create_take_indices(list_lengths, unnested_array.len());
            batch_from_indices(batch, schema, column.index(), &unnested_array, &indices)
        }
        dt => Err(DataFusionError::Execution(format!(
            "Unnest: unsupported indices type {dt}"
        ))),
    }
}

/// Create the indices for the take kernel given an array of list values lengths.
///
/// The indices are used to duplicate column elements so that all columns have as
/// many rows as the unnested array.
///
/// Given the nested array:
///
/// ```ignore
/// [1], null, [2, 3, 4], null, [5, 6]
/// ```
///
/// the `list_lengths` array contains the length of each list value:
///
/// ```ignore
/// 1, null,  3, null, 2
/// ```
///
/// the result indices array is:
///
/// ```ignore
/// 0, 1, 2, 2, 2, 3, 4, 4
/// ```
///
/// where a null value count as one element.
fn create_take_indices<T>(
    list_lengths: &PrimitiveArray<T>,
    capacity: usize,
) -> PrimitiveArray<T>
where
    T: ArrowPrimitiveType,
{
    let mut builder = PrimitiveArray::<T>::builder(capacity);
    for row in 0..list_lengths.len() {
        let repeat = if list_lengths.is_null(row) {
            T::Native::ONE
        } else {
            list_lengths.value(row)
        };

        // Both `repeat` and `index` are positive intergers.
        let repeat = repeat.to_usize().unwrap();
        let index = T::Native::from_usize(row).unwrap();
        (0..repeat).for_each(|_| builder.append_value(index));
    }

    builder.finish()
}

fn batch_from_indices<T>(
    batch: &RecordBatch,
    schema: &SchemaRef,
    unnest_column_idx: usize,
    unnested_array: &ArrayRef,
    indices: &PrimitiveArray<T>,
) -> Result<RecordBatch>
where
    T: ArrowPrimitiveType,
{
    let arrays = batch
        .columns()
        .iter()
        .enumerate()
        .map(|(col_idx, arr)| {
            if col_idx == unnest_column_idx {
                Ok(unnested_array.clone())
            } else {
                Ok(kernels::take::take(&arr, indices, None)?)
            }
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(RecordBatch::try_new(schema.clone(), arrays.to_vec())?)
}

fn unnest_array<T>(list_array: &T) -> Result<Arc<dyn Array + 'static>>
where
    T: ArrayAccessor<Item = ArrayRef>,
{
    let elem_type = match list_array.data_type() {
        DataType::List(f) | DataType::FixedSizeList(f, _) | DataType::LargeList(f) => {
            f.data_type()
        }
        dt => {
            return Err(DataFusionError::Execution(format!(
                "Cannot flatten array of type {dt}"
            )))
        }
    };

    let null_row = new_null_array(elem_type, 1);

    // Create a vec of ArrayRef from the list elements.
    let arrays = (0..list_array.len())
        .map(|row| {
            if list_array.value(row).is_empty() {
                null_row.clone()
            } else {
                list_array.value(row)
            }
        })
        .collect::<Vec<_>>();

    // Create Vec<&dyn Array> from Vec<Arc<dyn Array>> for `concat`. Calling
    // `as_ref()` in the `map` above causes the borrow checker to complain.
    let arrays = arrays.iter().map(|a| a.as_ref()).collect::<Vec<_>>();

    Ok(kernels::concat::concat(&arrays)?)
}
