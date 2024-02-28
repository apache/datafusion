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
use std::{any::Any, sync::Arc};

use super::metrics::{self, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet};
use super::{DisplayAs, ExecutionPlanProperties, PlanProperties};
use crate::{
    expressions::Column, DisplayFormatType, Distribution, ExecutionPlan, PhysicalExpr,
    RecordBatchStream, SendableRecordBatchStream,
};

use arrow::array::{
    Array, ArrayRef, ArrowPrimitiveType, FixedSizeListArray, GenericListArray,
    LargeListArray, ListArray, OffsetSizeTrait, PrimitiveArray,
};
use arrow::compute::kernels;
use arrow::datatypes::{
    ArrowNativeType, DataType, Int32Type, Int64Type, Schema, SchemaRef,
};
use arrow::record_batch::RecordBatch;
use datafusion_common::{exec_err, Result, UnnestOptions};
use datafusion_execution::TaskContext;
use datafusion_physical_expr::EquivalenceProperties;

use async_trait::async_trait;
use futures::{Stream, StreamExt};
use log::trace;

/// Unnest the given column by joining the row with each value in the
/// nested type.
///
/// See [`UnnestOptions`] for more details and an example.
#[derive(Debug)]
pub struct UnnestExec {
    /// Input execution plan
    input: Arc<dyn ExecutionPlan>,
    /// The schema once the unnest is applied
    schema: SchemaRef,
    /// The unnest column
    column: Column,
    /// Options
    options: UnnestOptions,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// Cache holding plan properties like equivalences, output partitioning etc.
    cache: PlanProperties,
}

impl UnnestExec {
    /// Create a new [UnnestExec].
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        column: Column,
        schema: SchemaRef,
        options: UnnestOptions,
    ) -> Self {
        let cache = Self::compute_properties(&input, schema.clone());
        UnnestExec {
            input,
            schema,
            column,
            options,
            metrics: Default::default(),
            cache,
        }
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(
        input: &Arc<dyn ExecutionPlan>,
        schema: SchemaRef,
    ) -> PlanProperties {
        let eq_properties = EquivalenceProperties::new(schema);

        PlanProperties::new(
            eq_properties,
            input.output_partitioning().clone(),
            input.execution_mode(),
        )
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

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(UnnestExec::new(
            children[0].clone(),
            self.column.clone(),
            self.schema.clone(),
            self.options.clone(),
        )))
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::UnspecifiedDistribution]
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;
        let metrics = UnnestMetrics::new(partition, &self.metrics);

        Ok(Box::pin(UnnestStream {
            input,
            schema: self.schema.clone(),
            column: self.column.clone(),
            options: self.options.clone(),
            metrics,
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

#[derive(Clone, Debug)]
struct UnnestMetrics {
    /// total time for column unnesting
    elapsed_compute: metrics::Time,
    /// Number of batches consumed
    input_batches: metrics::Count,
    /// Number of rows consumed
    input_rows: metrics::Count,
    /// Number of batches produced
    output_batches: metrics::Count,
    /// Number of rows produced by this operator
    output_rows: metrics::Count,
}

impl UnnestMetrics {
    fn new(partition: usize, metrics: &ExecutionPlanMetricsSet) -> Self {
        let elapsed_compute = MetricBuilder::new(metrics).elapsed_compute(partition);

        let input_batches =
            MetricBuilder::new(metrics).counter("input_batches", partition);

        let input_rows = MetricBuilder::new(metrics).counter("input_rows", partition);

        let output_batches =
            MetricBuilder::new(metrics).counter("output_batches", partition);

        let output_rows = MetricBuilder::new(metrics).output_rows(partition);

        Self {
            input_batches,
            input_rows,
            output_batches,
            output_rows,
            elapsed_compute,
        }
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
    /// Options
    options: UnnestOptions,
    /// Metrics
    metrics: UnnestMetrics,
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
                    let timer = self.metrics.elapsed_compute.timer();
                    let result =
                        build_batch(&batch, &self.schema, &self.column, &self.options);
                    self.metrics.input_batches.add(1);
                    self.metrics.input_rows.add(batch.num_rows());
                    if let Ok(ref batch) = result {
                        timer.done();
                        self.metrics.output_batches.add(1);
                        self.metrics.output_rows.add(batch.num_rows());
                    }

                    Some(result)
                }
                other => {
                    trace!(
                        "Processed {} probe-side input batches containing {} rows and \
                        produced {} output batches containing {} rows in {}",
                        self.metrics.input_batches,
                        self.metrics.input_rows,
                        self.metrics.output_batches,
                        self.metrics.output_rows,
                        self.metrics.elapsed_compute,
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
    options: &UnnestOptions,
) -> Result<RecordBatch> {
    let list_array = column.evaluate(batch)?.into_array(batch.num_rows())?;
    match list_array.data_type() {
        DataType::List(_) => {
            let list_array = list_array.as_any().downcast_ref::<ListArray>().unwrap();
            build_batch_generic_list::<i32, Int32Type>(
                batch,
                schema,
                column.index(),
                list_array,
                options,
            )
        }
        DataType::LargeList(_) => {
            let list_array = list_array
                .as_any()
                .downcast_ref::<LargeListArray>()
                .unwrap();
            build_batch_generic_list::<i64, Int64Type>(
                batch,
                schema,
                column.index(),
                list_array,
                options,
            )
        }
        DataType::FixedSizeList(_, _) => {
            let list_array = list_array
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .unwrap();
            build_batch_fixedsize_list(batch, schema, column.index(), list_array, options)
        }
        _ => exec_err!("Invalid unnest column {column}"),
    }
}

fn build_batch_generic_list<T: OffsetSizeTrait, P: ArrowPrimitiveType<Native = T>>(
    batch: &RecordBatch,
    schema: &SchemaRef,
    unnest_column_idx: usize,
    list_array: &GenericListArray<T>,
    options: &UnnestOptions,
) -> Result<RecordBatch> {
    let unnested_array = unnest_generic_list::<T, P>(list_array, options)?;

    let take_indicies =
        create_take_indicies_generic::<T, P>(list_array, unnested_array.len(), options);

    batch_from_indices(
        batch,
        schema,
        unnest_column_idx,
        &unnested_array,
        &take_indicies,
    )
}

/// Given this `GenericList` list_array:
///
/// ```ignore
/// [1], null, [2, 3, 4], null, [5, 6]
/// ```
/// Its values array is represented like this:
///
/// ```ignore
/// [1, 2, 3, 4, 5, 6]
/// ```
///
/// So if there are no null values or `UnnestOptions.preserve_nulls` is false
/// we can return the values array without any copying.
///
/// Otherwise we'll transfrom the values array using the take kernel and the following take indicies:
///
/// ```ignore
/// 0, null, 1, 2, 3, null, 4, 5
/// ```
///
fn unnest_generic_list<T: OffsetSizeTrait, P: ArrowPrimitiveType<Native = T>>(
    list_array: &GenericListArray<T>,
    options: &UnnestOptions,
) -> Result<Arc<dyn Array + 'static>> {
    let values = list_array.values();
    if list_array.null_count() == 0 || !options.preserve_nulls {
        Ok(values.clone())
    } else {
        let mut take_indicies_builder =
            PrimitiveArray::<P>::builder(values.len() + list_array.null_count());
        let mut take_offset = 0;

        list_array.iter().for_each(|elem| match elem {
            Some(array) => {
                for i in 0..array.len() {
                    // take_offset + i is always positive
                    let take_index = P::Native::from_usize(take_offset + i).unwrap();
                    take_indicies_builder.append_value(take_index);
                }
                take_offset += array.len();
            }
            None => {
                take_indicies_builder.append_null();
            }
        });
        Ok(kernels::take::take(
            &values,
            &take_indicies_builder.finish(),
            None,
        )?)
    }
}

fn build_batch_fixedsize_list(
    batch: &RecordBatch,
    schema: &SchemaRef,
    unnest_column_idx: usize,
    list_array: &FixedSizeListArray,
    options: &UnnestOptions,
) -> Result<RecordBatch> {
    let unnested_array = unnest_fixed_list(list_array, options)?;

    let take_indicies =
        create_take_indicies_fixed(list_array, unnested_array.len(), options);

    batch_from_indices(
        batch,
        schema,
        unnest_column_idx,
        &unnested_array,
        &take_indicies,
    )
}

/// Given this `FixedSizeListArray` list_array:
///
/// ```ignore
/// [1, 2], null, [3, 4], null, [5, 6]
/// ```
/// Its values array is represented like this:
///
/// ```ignore
/// [1, 2, null, null 3, 4, null, null, 5, 6]
/// ```
///
/// So if there are no null values
/// we can return the values array without any copying.
///
/// Otherwise we'll transfrom the values array using the take kernel.
///
/// If `UnnestOptions.preserve_nulls` is true the take indicies will look like this:
///
/// ```ignore
/// 0, 1, null, 4, 5, null, 8, 9
/// ```
/// Otherwise we drop the nulls and take indicies will look like this:
///
/// ```ignore
/// 0, 1, 4, 5, 8, 9
/// ```
///
fn unnest_fixed_list(
    list_array: &FixedSizeListArray,
    options: &UnnestOptions,
) -> Result<Arc<dyn Array + 'static>> {
    let values = list_array.values();

    if list_array.null_count() == 0 {
        Ok(values.clone())
    } else {
        let len_without_nulls =
            values.len() - list_array.null_count() * list_array.value_length() as usize;
        let null_count = if options.preserve_nulls {
            list_array.null_count()
        } else {
            0
        };
        let mut builder =
            PrimitiveArray::<Int32Type>::builder(len_without_nulls + null_count);
        let mut take_offset = 0;
        let fixed_value_length = list_array.value_length() as usize;
        list_array.iter().for_each(|elem| match elem {
            Some(_) => {
                for i in 0..fixed_value_length {
                    //take_offset + i is always positive
                    let take_index = take_offset + i;
                    builder.append_value(take_index as i32);
                }
                take_offset += fixed_value_length;
            }
            None => {
                if options.preserve_nulls {
                    builder.append_null();
                }
                take_offset += fixed_value_length;
            }
        });
        Ok(kernels::take::take(&values, &builder.finish(), None)?)
    }
}

/// Creates take indicies to be used to expand all other column's data.
/// Every column value needs to be repeated as many times as many elements there is in each corresponding array value.
///
/// If the column being unnested looks like this:
///
/// ```ignore
/// [1], null, [2, 3, 4], null, [5, 6]
/// ```
/// Then `create_take_indicies_generic` will return an array like this
///
/// ```ignore
/// [1, null, 2, 2, 2, null, 4, 4]
/// ```
///
fn create_take_indicies_generic<T: OffsetSizeTrait, P: ArrowPrimitiveType<Native = T>>(
    list_array: &GenericListArray<T>,
    capacity: usize,
    options: &UnnestOptions,
) -> PrimitiveArray<P> {
    let mut builder = PrimitiveArray::<P>::builder(capacity);
    let null_repeat: usize = if options.preserve_nulls { 1 } else { 0 };

    for row in 0..list_array.len() {
        let repeat = if list_array.is_null(row) {
            null_repeat
        } else {
            list_array.value(row).len()
        };

        // `index` is a positive interger.
        let index = P::Native::from_usize(row).unwrap();
        (0..repeat).for_each(|_| builder.append_value(index));
    }

    builder.finish()
}

fn create_take_indicies_fixed(
    list_array: &FixedSizeListArray,
    capacity: usize,
    options: &UnnestOptions,
) -> PrimitiveArray<Int32Type> {
    let mut builder = PrimitiveArray::<Int32Type>::builder(capacity);
    let null_repeat: usize = if options.preserve_nulls { 1 } else { 0 };

    for row in 0..list_array.len() {
        let repeat = if list_array.is_null(row) {
            null_repeat
        } else {
            list_array.value_length() as usize
        };

        // `index` is a positive interger.
        let index = <Int32Type as ArrowPrimitiveType>::Native::from_usize(row).unwrap();
        (0..repeat).for_each(|_| builder.append_value(index));
    }

    builder.finish()
}

/// Create the final batch given the unnested column array and a `indices` array
/// that is used by the take kernel to copy values.
///
/// For example if we have the following `RecordBatch`:
///
/// ```ignore
/// c1: [1], null, [2, 3, 4], null, [5, 6]
/// c2: 'a', 'b',  'c', null, 'd'
/// ```
///
/// then the `unnested_array` contains the unnest column that will replace `c1` in
/// the final batch:
///
/// ```ignore
/// c1: 1, null, 2, 3, 4, null, 5, 6
/// ```
///
/// And the `indices` array contains the indices that are used by `take` kernel to
/// repeat the values in `c2`:
///
/// ```ignore
/// 0, 1, 2, 2, 2, 3, 4, 4
/// ```
///
/// so that the final batch will look like:
///
/// ```ignore
/// c1: 1, null, 2, 3, 4, null, 5, 6
/// c2: 'a', 'b', 'c', 'c', 'c', null, 'd', 'd'
/// ```
///
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
