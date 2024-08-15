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

//! Define a plan for unnesting values in columns that contain a list type.

use std::cmp::{self, Ordering};
use std::collections::HashMap;
use std::{any::Any, sync::Arc};

use super::metrics::{self, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet};
use super::{DisplayAs, ExecutionPlanProperties, PlanProperties};
use crate::{
    DisplayFormatType, Distribution, ExecutionPlan, RecordBatchStream,
    SendableRecordBatchStream,
};

use arrow::array::{
    Array, ArrayRef, AsArray, FixedSizeListArray, LargeListArray, ListArray,
    PrimitiveArray,
};
use arrow::compute::kernels::length::length;
use arrow::compute::kernels::zip::zip;
use arrow::compute::{cast, is_not_null, kernels, sum};
use arrow::datatypes::{DataType, Int32Type, Int64Type, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use arrow_array::{Int32Array, Int64Array, Scalar, StructArray};
use arrow_ord::cmp::lt;
use datafusion_common::{
    exec_datafusion_err, exec_err, internal_err, Result, UnnestOptions,
};
use datafusion_execution::TaskContext;
use datafusion_physical_expr::EquivalenceProperties;

use async_trait::async_trait;
use futures::{Stream, StreamExt};
use hashbrown::HashSet;
use log::trace;

/// Unnest the given columns (either with type struct or list)
/// For list unnesting, each rows is vertically transformed into multiple rows
/// For struct unnesting, each columns is horizontally transformed into multiple columns,
/// Thus the original RecordBatch with dimension (n x m) may have new dimension (n' x m')
///
/// See [`UnnestOptions`] for more details and an example.
#[derive(Debug)]
pub struct UnnestExec {
    /// Input execution plan
    input: Arc<dyn ExecutionPlan>,
    /// The schema once the unnest is applied
    schema: SchemaRef,
    /// indices of the list-typed columns in the input schema
    list_column_indices: Vec<ListUnnest>,
    /// indices of the struct-typed columns in the input schema
    struct_column_indices: Vec<usize>,
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
        list_column_indices: Vec<ListUnnest>,
        struct_column_indices: Vec<usize>,
        schema: SchemaRef,
        options: UnnestOptions,
    ) -> Self {
        let cache = Self::compute_properties(&input, Arc::clone(&schema));

        UnnestExec {
            input,
            schema,
            list_column_indices,
            struct_column_indices,
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
    fn name(&self) -> &'static str {
        "UnnestExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(UnnestExec::new(
            Arc::clone(&children[0]),
            self.list_column_indices.clone(),
            self.struct_column_indices.clone(),
            Arc::clone(&self.schema),
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
            schema: Arc::clone(&self.schema),
            list_type_columns: self.list_column_indices.clone(),
            struct_column_indices: self.struct_column_indices.iter().copied().collect(),
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
    /// represents all unnest operations to be applied to the input (input index, depth)
    /// e.g unnest(col1),unnest(unnest(col1)) where col1 has index 1 in original input schema
    /// then list_type_columns = [(1,1),(1,2)]
    list_type_columns: Vec<ListUnnest>,
    struct_column_indices: HashSet<usize>,
    /// Options
    options: UnnestOptions,
    /// Metrics
    metrics: UnnestMetrics,
}

impl RecordBatchStream for UnnestStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
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
                    let result = build_batch_v2(
                        &batch,
                        &self.schema,
                        &self.list_type_columns,
                        &self.struct_column_indices,
                        &self.options,
                    );
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

/// Given a set of struct column indices to flatten
/// try converting the column in input into multiple subfield columns
/// For example
/// struct_col: [a: struct(item: int, name: string), b: int]
/// with a batch
/// {a: {item: 1, name: "a"}, b: 2},
/// {a: {item: 3, name: "b"}, b: 4]
/// will be converted into
/// {a.item: 1, a.name: "a", b: 2},
/// {a.item: 3, a.name: "b", b: 4}
fn flatten_struct_cols(
    input_batch: &[Arc<dyn Array>],
    schema: &SchemaRef,
    struct_column_indices: &HashSet<usize>,
) -> Result<RecordBatch> {
    // horizontal expansion because of struct unnest
    let columns_expanded = input_batch
        .iter()
        .enumerate()
        .map(|(idx, column_data)| match struct_column_indices.get(&idx) {
            Some(_) => match column_data.data_type() {
                DataType::Struct(_) => {
                    let struct_arr =
                        column_data.as_any().downcast_ref::<StructArray>().unwrap();
                    Ok(struct_arr.columns().to_vec())
                }
                data_type => internal_err!(
                    "expecting column {} from input plan to be a struct, got {:?}",
                    idx,
                    data_type
                ),
            },
            None => Ok(vec![Arc::clone(column_data)]),
        })
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        .collect();
    Ok(RecordBatch::try_new(Arc::clone(schema), columns_expanded)?)
}

#[derive(Debug, Clone)]
pub struct ListUnnest {
    pub index_in_input_schema: usize,
    pub depth: usize,
}

// a map of original index (track which column in the batch to be interested in)
// input schema:
// col1_unnest_placeholder: list[list[int]], col1: list[list[int]], col2 list[int]
// with unnest on unnest(col1,depth=2), unnest(col1,depth=1) and unnest(col2,depth=1)
// output schema:
// unnest_col1_depth_2: int, unnest_col1_depth1: list[int], col1: list[list[int]], unnest_col2_depth_1: int
// Meaning the placeholder column will be replaced by its unnested variation(s), note
// the plural.

/*
    Note: unnest has a big difference in behavior between Postgres and DuckDB
    Take this example
    1.Postgres
    ```ignored
    create table temp (
        i integer[][][], j integer[]
    )
    insert into temp values ('{{{1,2},{3,4}},{{5,6},{7,8}}}', '{1,2}');
    select unnest(i), unnest(j) from temp;
    ```

    Result
        1	1
        2	2
        3
        4
        5
        6
        7
        8
    2. DuckDB
    ```ignore
        create table temp (i integer[][][], j integer[]);
        insert into temp values ([[[1,2],[3,4]],[[5,6],[7,8]]], [1,2]);
        select unnest(i,recursive:=true), unnest(j,recursive:=true) from temp;
    ```
    Result:
        ┌────────────────────────────────────────────────┬────────────────────────────────────────────────┐
        │ unnest(i, "recursive" := CAST('t' AS BOOLEAN)) │ unnest(j, "recursive" := CAST('t' AS BOOLEAN)) │
        │                     int32                      │                     int32                      │
        ├────────────────────────────────────────────────┼────────────────────────────────────────────────┤
        │                                              1 │                                              1 │
        │                                              2 │                                              2 │
        │                                              3 │                                              1 │
        │                                              4 │                                              2 │
        │                                              5 │                                              1 │
        │                                              6 │                                              2 │
        │                                              7 │                                              1 │
        │                                              8 │                                              2 │
        └────────────────────────────────────────────────┴────────────────────────────────────────────────┘
    The following implementation refer to Postgres's implementation
    For DuckDB's result to be similar, the above query can be written as



*/

fn unnest_at_level(
    batch: &[ArrayRef],
    list_type_columns: &[ListUnnest],
    temp_batch: &mut HashMap<(usize, usize), ArrayRef>,
    level_to_unnest: usize,
    options: &UnnestOptions,
) -> Result<Vec<ArrayRef>> {
    // unnested columns at this depth level
    // now do some kind of projection-like
    // This query:
    // select unnest(col1, max_depth:=3), unnest(col1,max_depth:=2), unnest(col1, max_depth:=1)  from temp;
    // is equivalent to
    //
    // unnest(depth3) , unnest(depth2), unnest(depth1)
    // select (unnest)
    // batch comes in as [a,b]
    // in this example list_type_columns as value
    // [(a,1), (a,2)] and [(b,1)]
    // 1.microwork(level=2)
    // new batch as [unnest_a,b]
    // new list as [(a,1)] and [(b,1)]
    // temp_batch_for_projection: [(a,depth=1,unnested(a))]
    // 2.microwork(level=1)
    // new batch as [unnest(unnest_a),b]
    // new list as []
    // temp_batch: [(a,depth=1,unnested(a)),(a,depth=2,unnested(unnested(a))),(b,depth=1,unnested(b))]
    // this is final, now combine the mainbatch and temp_batch

    let (temp_unnest_cols, unnested_locations): (Vec<Arc<dyn Array>>, Vec<_>) =
        list_type_columns
            .iter()
            .filter_map(
                |ListUnnest {
                     depth,
                     index_in_input_schema,
                 }| {
                    if *depth == level_to_unnest {
                        return Some((
                            Arc::clone(&batch[*index_in_input_schema]),
                            (*index_in_input_schema, *depth),
                        ));
                    }
                    // this means the depth to unnest is still on going, keep on unnesting
                    // at current level
                    if *depth > level_to_unnest {
                        return Some((
                            Arc::clone(
                                temp_batch
                                    .get(&(*index_in_input_schema, *depth))
                                    .unwrap(),
                            ),
                            (*index_in_input_schema, *depth),
                        ));
                    }
                    None
                },
            )
            .unzip();

    // filter out so that list_arrays only contain column with the highest depth
    // at the same time, during iteration remove this depth so next time we don't have to unnest them again
    let longest_length = find_longest_length(&temp_unnest_cols, options)?;
    let unnested_length = longest_length.as_primitive::<Int64Type>();
    let total_length = if unnested_length.is_empty() {
        0
    } else {
        sum(unnested_length).ok_or_else(|| {
            exec_datafusion_err!("Failed to calculate the total unnested length")
        })? as usize
    };
    if total_length == 0 {
        return Ok(vec![]);
    }

    // Unnest all the list arrays
    let unnested_temp_arrays =
        unnest_list_arrays(temp_unnest_cols.as_ref(), unnested_length, total_length)?;
    unnested_temp_arrays.iter().for_each(|arr| {
        println!("ret {}", arr.len());
    });

    // Create the take indices array for other columns
    let take_indices = create_take_indicies(unnested_length, total_length);
    println!("take indices {}", take_indices.len());

    // vertical expansion because of list unnest
    let ret = flatten_batch_from_indices(batch, &take_indices)?;
    ret.iter().for_each(|arr| {
        println!("ret {}", arr.len());
    });
    unnested_temp_arrays
        .into_iter()
        .zip(unnested_locations.iter())
        .for_each(|(flatten_arr, (index_in_input_schema, depth))| {
            println!(
                "inserting into temp value {} {}",
                index_in_input_schema, depth
            );
            temp_batch.insert((*index_in_input_schema, *depth), flatten_arr);
        });
    Ok(ret)
}

fn build_batch_v2(
    batch: &RecordBatch,
    schema: &SchemaRef,
    list_type_columns: &[ListUnnest],
    struct_column_indices: &HashSet<usize>,
    options: &UnnestOptions,
) -> Result<RecordBatch> {
    let transformed = match list_type_columns.len() {
        0 => flatten_struct_cols(batch.columns(), schema, struct_column_indices),
        _ => {
            let mut temp_batch = HashMap::new();
            let highest_depth = list_type_columns
                .iter()
                .fold(0, |highest_depth, ListUnnest { depth, .. }| {
                    cmp::max(highest_depth, *depth)
                });
            let mut unnested_original_columns = vec![];
            for depth in (1..=highest_depth).rev() {
                let input = match depth == highest_depth {
                    true => batch.columns(),
                    false => &unnested_original_columns,
                };
                println!("unnesting at level {}", depth);
                let temp = unnest_at_level(
                    input,
                    list_type_columns,
                    &mut temp_batch,
                    depth,
                    options,
                )?;
                unnested_original_columns = temp;
            }
            println!("{} temp batch", temp_batch.len());
            // TODO: combine temp with the batch
            let unnested_array_map: HashMap<usize, Vec<(Arc<dyn Array>, usize)>> =
                temp_batch.into_iter().fold(
                    HashMap::new(),
                    |mut acc, ((index_in_input_schema, depth), flattened_array)| {
                        acc.entry(index_in_input_schema)
                            .or_default()
                            .push((flattened_array, depth));
                        acc
                    },
                );
            let output_order: HashMap<(usize, usize), usize> = list_type_columns
                .iter()
                .enumerate()
                .map(|(order, unnest_def)| {
                    let ListUnnest {
                        depth,
                        index_in_input_schema,
                    } = unnest_def;
                    ((*index_in_input_schema, *depth), order)
                })
                .collect();

            println!("unnested array map {}", unnested_array_map.len());
            let mut ordered_unnested_array_map = unnested_array_map
                .into_iter()
                .map(
                    // each item in unnested_columns is the result of unnesting the same input column
                    // we need to sort them to conform with the unnest definition
                    // e.g unnest(unnest(col)) must goes before unnest(col)
                    |(original_index, mut unnested_columns)| {
                        unnested_columns.sort_by(|(_, depth), (_, depth2)| -> Ordering {
                            output_order.get(&(original_index, *depth)).unwrap().cmp(
                                output_order.get(&(original_index, *depth2)).unwrap(),
                            )
                        });
                        (
                            original_index,
                            unnested_columns
                                .into_iter()
                                .map(|(arr, _)| arr)
                                .collect::<Vec<_>>(),
                        )
                    },
                )
                .collect::<HashMap<_, _>>();
            // let highest_depth = list_type_columns
            //     .iter()
            //     .fold(0, |highest_depth, ListUnnest { depth, .. }| {
            //         cmp::max(highest_depth, *depth)
            //     });
            println!("len of map {}", ordered_unnested_array_map.len());
            let ret = unnested_original_columns
                .into_iter()
                .enumerate()
                .flat_map(|(col_idx, arr)| {
                    match ordered_unnested_array_map.remove(&col_idx) {
                        Some(unnested_arrays) => unnested_arrays,
                        None => vec![arr],
                    }
                })
                .collect::<Vec<_>>();

            flatten_struct_cols(&ret, schema, struct_column_indices)
        }
    };
    transformed
}

/// For each row in a `RecordBatch`, some list/struct columns need to be unnested.
/// - For list columns: We will expand the values in each list into multiple rows,
/// taking the longest length among these lists, and shorter lists are padded with NULLs.
/// - For struct columns: We will expand the struct columns into multiple subfield columns.
/// For columns that don't need to be unnested, repeat their values until reaching the longest length.
// fn build_batch(
//     batch: &RecordBatch,
//     schema: &SchemaRef,
//     list_type_columns: &[ListUnnest],
//     struct_column_indices: &HashSet<usize>,
//     options: &UnnestOptions,
// ) -> Result<RecordBatch> {
//     let transformed = match list_type_columns.len() {
//         0 => flatten_struct_cols(batch.columns(), schema, struct_column_indices),
//         _ => {
//             // maintain a map of temp result
//             // <usize,Vec<Result>>
//             // given a map of unnesting
//             // col1: <depth=2,depth=1>
//             // col2: <depth=1>
//             // 1.run for each to unnest all the top level
//             // col1: <depth=1> => to temp
//             // 2.run unnest on remaining level
//             // col1: <depth=2> => to temp
//             // col2: <depth=1> => to temp
//             let list_arrays: Vec<(ArrayRef, usize)> = list_type_columns
//                 .iter()
//                 .map(
//                     |ListUnnest {
//                          depth,
//                          index_in_input_schema,
//                      }| {
//                         Ok((
//                             ColumnarValue::Array(Arc::clone(
//                                 batch.column(*index_in_input_schema),
//                             ))
//                             .into_array(batch.num_rows())?,
//                             *depth,
//                         ))
//                     },
//                 )
//                 .collect::<Result<_>>()?;

//             let longest_length = find_longest_length(&list_arrays, options)?;
//             let unnested_length = longest_length.as_primitive::<Int64Type>();
//             let total_length = if unnested_length.is_empty() {
//                 0
//             } else {
//                 sum(unnested_length).ok_or_else(|| {
//                     exec_datafusion_err!("Failed to calculate the total unnested length")
//                 })? as usize
//             };
//             if total_length == 0 {
//                 return Ok(RecordBatch::new_empty(Arc::clone(schema)));
//             }

//             // Unnest all the list arrays
//             let unnested_arrays =
//                 unnest_list_arrays(&list_arrays, unnested_length, total_length)?;
//             let unnested_array_map: HashMap<usize, Vec<Arc<dyn Array>>> = unnested_arrays
//                 .into_iter()
//                 .zip(list_type_columns.iter())
//                 .fold(
//                     HashMap::new(),
//                     |mut acc,
//                      (
//                         flattened_array,
//                         ListUnnest {
//                             index_in_input_schema,
//                             depth,
//                         },
//                     )| {
//                         acc.entry(*index_in_input_schema)
//                             .or_insert(vec![])
//                             .push(flattened_array);
//                         acc
//                     },
//                 );

//             // Create the take indices array for other columns
//             let take_indicies = create_take_indicies(unnested_length, total_length);

//             // vertical expansion because of list unnest
//             let ret = flatten_list_cols_from_indices(
//                 batch,
//                 unnested_array_map,
//                 &take_indicies,
//             )?;
//             flatten_struct_cols(&ret, schema, struct_column_indices)
//         }
//     };
//     transformed
// }

/// TODO: only prototype
/// This function does not handle NULL yet
pub fn length_recursive(
    array: &dyn Array,
    depth: usize,
    preserve_nulls: bool,
    is_top_level: bool, // null values at top level recursion is ignored, regardless of preserve_null
) -> Result<ArrayRef> {
    let list = array.as_list::<i32>();
    let null_length = if preserve_nulls {
        Scalar::new(Int32Array::from_value(1, 1))
    } else {
        Scalar::new(Int32Array::from_value(0, 1))
    };
    // preserve null only concern nullability of the element in array, not the value of the array itself
    // e.g unnest(null) => 0 rows
    // while unnest([null]) => 1 row with null value
    let null_length_for_value = match preserve_nulls && !is_top_level {
        true => 1,
        false => 0,
    };
    if depth == 1 {
        if array.is_empty() {
            return Ok(Arc::new(PrimitiveArray::<Int32Type>::from(vec![
                null_length_for_value,
            ])));
        }

        // null elements will have null length
        // respect preserve_nulls here
        let mut ret = length(array)?;
        ret = cast(&ret, &DataType::Int32)?;
        return Ok(zip(&is_not_null(&ret)?, &ret, &null_length)?);
    }
    let a: Vec<i32> = list
        .iter()
        .map(|x| {
            if x.is_none() {
                return null_length_for_value;
            }
            // [[1,2,3],null,[4,5]]
            // [[7,8,9,10],]
            // length([1,2]) + length(null) + length([4,5])
            let ret =
                length_recursive(&x.unwrap(), depth - 1, preserve_nulls, false).unwrap();
            let a = ret.as_primitive::<Int32Type>();
            if a.is_empty() {
                // TODO respect is_preserve_nulls
                1
            } else {
                sum(a).unwrap()
            }
        })
        .collect();
    Ok(Arc::new(PrimitiveArray::<Int32Type>::from(a)))
}

/// Find the longest list length among the given list arrays for each row.
///
/// For example if we have the following two list arrays:
///
/// ```ignore
/// l1: [1, 2, 3], null, [], [3]
/// l2: [4,5], [], null, [6, 7]
/// ```
///
/// If `preserve_nulls` is false, the longest length array will be:
///
/// ```ignore
/// longest_length: [3, 0, 0, 2]
/// ```
///
/// whereas if `preserve_nulls` is true, the longest length array will be:
///
///
/// ```ignore
/// longest_length: [3, 1, 1, 2]
/// ```
///
fn find_longest_length(
    list_arrays: &[ArrayRef],
    options: &UnnestOptions,
) -> Result<ArrayRef> {
    // The length of a NULL list
    let null_length = if options.preserve_nulls {
        Scalar::new(Int64Array::from_value(1, 1))
    } else {
        Scalar::new(Int64Array::from_value(0, 1))
    };
    // col1: [[1,2]]|[[2,3]]
    // unnest(col1): [1,2]|[2,3] => length = 2
    // unnest(unnest(col1)): 1|2|2|3 => length = 4
    // col2: [3]|[4] => length =2
    // unnest(col2): 3|4 => length = 2
    // unnest(col1), unnest(unnest(col1)), unnest(col2)
    let list_lengths: Vec<ArrayRef> = list_arrays
        .iter()
        .map(|list_array| {
            // Perhaps we don't need recursive here
            // let mut length_array =
            //     length_recursive(list_array, *depth, options.preserve_nulls, true)?;
            let mut length_array = length(list_array)?;
            // Make sure length arrays have the same type. Int64 is the most general one.
            // Respect the depth of unnest( current func only get the length of 1 level of unnest)
            length_array = cast(&length_array, &DataType::Int64)?;
            length_array =
                zip(&is_not_null(&length_array)?, &length_array, &null_length)?;
            Ok(length_array)
        })
        .collect::<Result<_>>()?;

    let longest_length = list_lengths.iter().skip(1).try_fold(
        Arc::clone(&list_lengths[0]),
        |longest, current| {
            let is_lt = lt(&longest, &current)?;
            zip(&is_lt, &current, &longest)
        },
    )?;
    Ok(longest_length)
}

/// Trait defining common methods used for unnesting, implemented by list array types.
trait ListArrayType: Array {
    /// Returns a reference to the values of this list.
    fn values(&self) -> &ArrayRef;

    /// Returns the start and end offset of the values for the given row.
    fn value_offsets(&self, row: usize) -> (i64, i64);
}

impl ListArrayType for ListArray {
    fn values(&self) -> &ArrayRef {
        self.values()
    }

    fn value_offsets(&self, row: usize) -> (i64, i64) {
        let offsets = self.value_offsets();
        (offsets[row].into(), offsets[row + 1].into())
    }
}

impl ListArrayType for LargeListArray {
    fn values(&self) -> &ArrayRef {
        self.values()
    }

    fn value_offsets(&self, row: usize) -> (i64, i64) {
        let offsets = self.value_offsets();
        (offsets[row], offsets[row + 1])
    }
}

impl ListArrayType for FixedSizeListArray {
    fn values(&self) -> &ArrayRef {
        self.values()
    }

    fn value_offsets(&self, row: usize) -> (i64, i64) {
        let start = self.value_offset(row) as i64;
        (start, start + self.value_length() as i64)
    }
}

/// Unnest multiple list arrays according to the length array.
fn unnest_list_arrays(
    list_arrays: &[ArrayRef],
    length_array: &PrimitiveArray<Int64Type>,
    capacity: usize,
) -> Result<Vec<ArrayRef>> {
    let typed_arrays = list_arrays
        .iter()
        .map(|list_array| match list_array.data_type() {
            DataType::List(_) => Ok(list_array.as_list::<i32>() as &dyn ListArrayType),
            DataType::LargeList(_) => {
                Ok(list_array.as_list::<i64>() as &dyn ListArrayType)
            }
            DataType::FixedSizeList(_, _) => {
                Ok(list_array.as_fixed_size_list() as &dyn ListArrayType)
            }
            other => exec_err!("Invalid unnest datatype {other }"),
        })
        .collect::<Result<Vec<_>>>()?;

    typed_arrays
        .iter()
        .map(|list_array| unnest_list_array(*list_array, length_array, capacity))
        .collect::<Result<_>>()
}

/// Unnest a list array according the target length array.
///
/// Consider a list array like this:
///
/// ```ignore
/// [1], [2, 3, 4], null, [5], [],
/// ```
///
/// and the length array is:
///
/// ```ignore
/// [2, 3, 2, 1, 2]
/// ```
///
/// If the length of a certain list is less than the target length, pad with NULLs.
/// So the unnested array will look like this:
///
/// ```ignore
/// [1, null, 2, 3, 4, null, null, 5, null, null]
/// ```
///
fn unnest_list_array(
    list_array: &dyn ListArrayType,
    length_array: &PrimitiveArray<Int64Type>,
    capacity: usize,
) -> Result<ArrayRef> {
    let values = list_array.values();
    // TODO: handle me recursively
    let mut take_indicies_builder = PrimitiveArray::<Int64Type>::builder(capacity);
    for row in 0..list_array.len() {
        let mut value_length = 0;
        if !list_array.is_null(row) {
            let (start, end) = list_array.value_offsets(row);
            value_length = end - start;
            for i in start..end {
                take_indicies_builder.append_value(i)
            }
        }
        let target_length = length_array.value(row);
        debug_assert!(
            value_length <= target_length,
            "value length is beyond the longest length"
        );
        // Pad with NULL values
        for _ in value_length..target_length {
            take_indicies_builder.append_null();
        }
    }
    Ok(kernels::take::take(
        &values,
        &take_indicies_builder.finish(),
        None,
    )?)
}

/// Creates take indicies that will be used to expand all columns except for the list type
/// [`columns`](UnnestExec::list_column_indices) that is being unnested.
/// Every column value needs to be repeated multiple times according to the length array.
///
/// If the length array looks like this:
///
/// ```ignore
/// [2, 3, 1]
/// ```
/// Then `create_take_indicies` will return an array like this
///
/// ```ignore
/// [0, 0, 1, 1, 1, 2]
/// ```
///
fn create_take_indicies(
    length_array: &PrimitiveArray<Int64Type>,
    capacity: usize,
) -> PrimitiveArray<Int64Type> {
    // `find_longest_length()` guarantees this.
    debug_assert!(
        length_array.null_count() == 0,
        "length array should not contain nulls"
    );
    let mut builder = PrimitiveArray::<Int64Type>::builder(capacity);
    for (index, repeat) in length_array.iter().enumerate() {
        // The length array should not contain nulls, so unwrap is safe
        let repeat = repeat.unwrap();
        (0..repeat).for_each(|_| builder.append_value(index as i64));
    }
    builder.finish()
}

fn flatten_batch_from_indices(
    batch: &[ArrayRef],
    // temp: &[(usize, Arc<dyn Array>)],
    // mut unnested_list_arrays: HashMap<usize, Vec<ArrayRef>>,
    indices: &PrimitiveArray<Int64Type>,
) -> Result<Vec<Arc<dyn Array>>> {
    return batch
        .into_iter()
        .map(|arr| Ok(kernels::take::take(arr, indices, None)?))
        .collect::<Result<_>>();
    // return Ok(new_arrays);
    // Ok(new_arrays)
    // let arrays = batch
    //     .columns()
    //     .iter()
    //     .enumerate()
    //     .map(
    //         |(col_idx, arr)| match unnested_list_arrays.remove(&col_idx) {
    //             Some(unnested_arrays) => Ok(unnested_arrays),
    //             None => Ok(vec![kernels::take::take(arr, indices, None)?]),
    //         },
    //     )
    //     .collect::<Result<Vec<_>>>()?
    //     .into_iter()
    //     .flatten()
    //     .collect::<Vec<_>>();
    // Ok(arrays)
}

/// Create the final batch given the unnested column arrays and a `indices` array
/// that is used by the take kernel to copy values.
///
/// For example if we have the following `RecordBatch`:
///
/// ```ignore
/// c1: [1], null, [2, 3, 4], null, [5, 6]
/// c2: 'a', 'b',  'c', null, 'd'
/// ```
///
/// then the `unnested_list_arrays` contains the unnest column that will replace `c1` in
/// the final batch if `preserve_nulls` is true:
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
fn flatten_list_cols_from_indices(
    batch: &RecordBatch,
    mut unnested_list_arrays: HashMap<usize, Vec<ArrayRef>>,
    indices: &PrimitiveArray<Int64Type>,
) -> Result<Vec<Arc<dyn Array>>> {
    let arrays = batch
        .columns()
        .iter()
        .enumerate()
        .map(
            |(col_idx, arr)| match unnested_list_arrays.remove(&col_idx) {
                Some(unnested_arrays) => Ok(unnested_arrays),
                None => Ok(vec![kernels::take::take(arr, indices, None)?]),
            },
        )
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    Ok(arrays)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{datatypes::Field, util::pretty::pretty_format_batches};
    use arrow_array::{GenericListArray, OffsetSizeTrait, StringArray};
    use arrow_buffer::{BooleanBufferBuilder, NullBuffer, OffsetBuffer};

    // Create a GenericListArray with the following list values:
    //  [A, B, C], [], NULL, [D], NULL, [NULL, F]
    fn make_generic_array<OffsetSize>() -> GenericListArray<OffsetSize>
    where
        OffsetSize: OffsetSizeTrait,
    {
        let mut values = vec![];
        let mut offsets: Vec<OffsetSize> = vec![OffsetSize::zero()];
        let mut valid = BooleanBufferBuilder::new(6);

        // [A, B, C]
        values.extend_from_slice(&[Some("A"), Some("B"), Some("C")]);
        offsets.push(OffsetSize::from_usize(values.len()).unwrap());
        valid.append(true);

        // []
        offsets.push(OffsetSize::from_usize(values.len()).unwrap());
        valid.append(true);

        // NULL with non-zero value length
        // Issue https://github.com/apache/datafusion/issues/9932
        values.push(Some("?"));
        offsets.push(OffsetSize::from_usize(values.len()).unwrap());
        valid.append(false);

        // [D]
        values.push(Some("D"));
        offsets.push(OffsetSize::from_usize(values.len()).unwrap());
        valid.append(true);

        // Another NULL with zero value length
        offsets.push(OffsetSize::from_usize(values.len()).unwrap());
        valid.append(false);

        // [NULL, F]
        values.extend_from_slice(&[None, Some("F")]);
        offsets.push(OffsetSize::from_usize(values.len()).unwrap());
        valid.append(true);

        let field = Arc::new(Field::new("item", DataType::Utf8, true));
        GenericListArray::<OffsetSize>::new(
            field,
            OffsetBuffer::new(offsets.into()),
            Arc::new(StringArray::from(values)),
            Some(NullBuffer::new(valid.finish())),
        )
    }

    // Create a FixedSizeListArray with the following list values:
    //  [A, B], NULL, [C, D], NULL, [NULL, F], [NULL, NULL]
    fn make_fixed_list() -> FixedSizeListArray {
        let values = Arc::new(StringArray::from_iter([
            Some("A"),
            Some("B"),
            None,
            None,
            Some("C"),
            Some("D"),
            None,
            None,
            None,
            Some("F"),
            None,
            None,
        ]));
        let field = Arc::new(Field::new("item", DataType::Utf8, true));
        let valid = NullBuffer::from(vec![true, false, true, false, true, true]);
        FixedSizeListArray::new(field, 2, values, Some(valid))
    }

    fn verify_unnest_list_array(
        list_array: &dyn ListArrayType,
        lengths: Vec<i64>,
        expected: Vec<Option<&str>>,
    ) -> datafusion_common::Result<()> {
        let length_array = Int64Array::from(lengths);
        let unnested_array = unnest_list_array(list_array, &length_array, 3 * 6)?;
        let strs = unnested_array.as_string::<i32>().iter().collect::<Vec<_>>();
        assert_eq!(strs, expected);
        Ok(())
    }
    #[test]
    fn test_build_batch() -> datafusion_common::Result<()> {
        // col1                             | col2
        // [[1,2,3],null,[4,5]]             | ['a','b']
        // [[7,8,9,10], null, [11,12,13]]   | ['c','d']
        // null                             | ['e']
        let list_arr1 = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(1), Some(2), Some(3)]),
            None,
            Some(vec![Some(4), Some(5)]),
            Some(vec![Some(7), Some(8), Some(9), Some(10)]),
            None,
            Some(vec![Some(11), Some(12), Some(13)]),
        ]);

        let list_arr1_ref = Arc::new(list_arr1) as ArrayRef;
        let offsets = OffsetBuffer::from_lengths([3, 3, 0]);
        let mut nulls = BooleanBufferBuilder::new(3);
        nulls.append(true);
        nulls.append(true);
        nulls.append(false);
        // list<list<int32>>
        let col1_field = Field::new_list_field(
            DataType::List(Arc::new(Field::new_list_field(
                list_arr1_ref.data_type().to_owned(),
                true,
            ))),
            true,
        );
        let col1 = ListArray::new(
            Arc::new(Field::new_list_field(
                list_arr1_ref.data_type().to_owned(),
                true,
            )),
            offsets,
            list_arr1_ref,
            Some(NullBuffer::new(nulls.finish())),
        );

        let list_arr2 = StringArray::from(vec![
            Some("a"),
            Some("b"),
            Some("c"),
            Some("d"),
            Some("e"),
        ]);

        let offsets = OffsetBuffer::from_lengths([2, 2, 1]);
        let mut nulls = BooleanBufferBuilder::new(3);
        nulls.append_n(3, true);
        let col2_field = Field::new(
            "col2",
            DataType::List(Arc::new(Field::new_list_field(DataType::Utf8, true))),
            true,
        );
        let col2 = GenericListArray::<i32>::new(
            Arc::new(Field::new_list_field(DataType::Utf8, true)),
            OffsetBuffer::new(offsets.into()),
            Arc::new(list_arr2),
            Some(NullBuffer::new(nulls.finish())),
        );
        // convert col1 and col2 to a record batch
        let schema = Arc::new(Schema::new(vec![col1_field, col2_field]));
        let out_schema = Arc::new(Schema::new(vec![
            Field::new(
                "col1_unnest_placeholder_depth_1",
                DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                true,
            ),
            Field::new("col1_unnest_placeholder_depth_2", DataType::Int32, true),
            Field::new("col2_unnest_placeholder_depth_1", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(col1) as ArrayRef, Arc::new(col2) as ArrayRef],
        )
        .unwrap();
        let list_type_columns = vec![
            ListUnnest {
                index_in_input_schema: 0,
                depth: 1,
            },
            ListUnnest {
                index_in_input_schema: 0,
                depth: 2,
            },
            ListUnnest {
                index_in_input_schema: 1,
                depth: 1,
            },
        ];
        let ret = build_batch_v2(
            &batch,
            &out_schema,
            list_type_columns.as_ref(),
            &HashSet::default(),
            &UnnestOptions {
                preserve_nulls: true,
            },
        )?;
        let actual =
            format!("{}", pretty_format_batches(vec![ret].as_ref())?).to_lowercase();
        let expected_asc = r#"
+----------------------------------+--------------------------+
| trace_id                         | max(traces.timestamp_ms) |
+----------------------------------+--------------------------+
| 5868861a23ed31355efc5200eb80fe74 | 16909009999999           |
| 4040e64656804c3d77320d7a0e7eb1f0 | 16909009999998           |
| 02801bbe533190a9f8713d75222f445d | 16909009999997           |
| 9e31b3b5a620de32b68fefa5aeea57f1 | 16909009999996           |
| 2d88a860e9bd1cfaa632d8e7caeaa934 | 16909009999995           |
| a47edcef8364ab6f191dd9103e51c171 | 16909009999994           |
| 36a3fa2ccfbf8e00337f0b1254384db6 | 16909009999993           |
| 0756be84f57369012e10de18b57d8a2f | 16909009999992           |
| d4d6bf9845fa5897710e3a8db81d5907 | 16909009999991           |
| 3c2cc1abe728a66b61e14880b53482a0 | 16909009999990           |
+----------------------------------+--------------------------+
        "#
        .trim();

        println!("{}", actual);
        // format pretty record batch
        Ok(())
    }

    #[test]
    fn test_unnest_list_array() -> datafusion_common::Result<()> {
        // [A, B, C], [], NULL, [D], NULL, [NULL, F]
        let list_array = make_generic_array::<i32>();
        verify_unnest_list_array(
            &list_array,
            vec![3, 2, 1, 2, 0, 3],
            vec![
                Some("A"),
                Some("B"),
                Some("C"),
                None,
                None,
                None,
                Some("D"),
                None,
                None,
                Some("F"),
                None,
            ],
        )?;

        // [A, B], NULL, [C, D], NULL, [NULL, F], [NULL, NULL]
        let list_array = make_fixed_list();
        verify_unnest_list_array(
            &list_array,
            vec![3, 1, 2, 0, 2, 3],
            vec![
                Some("A"),
                Some("B"),
                None,
                None,
                Some("C"),
                Some("D"),
                None,
                Some("F"),
                None,
                None,
                None,
            ],
        )?;

        Ok(())
    }

    fn verify_longest_length(
        list_arrays: &[ArrayRef],
        preserve_nulls: bool,
        expected: Vec<i64>,
    ) -> datafusion_common::Result<()> {
        let options = UnnestOptions { preserve_nulls };
        let longest_length = find_longest_length(list_arrays, &options)?;
        let expected_array = Int64Array::from(expected);
        assert_eq!(
            longest_length
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap(),
            &expected_array
        );
        Ok(())
    }

    #[test]
    fn test_longest_list_length() -> datafusion_common::Result<()> {
        // Test with single ListArray
        //  [A, B, C], [], NULL, [D], NULL, [NULL, F]
        let list_array = Arc::new(make_generic_array::<i32>()) as ArrayRef;
        verify_longest_length(&[Arc::clone(&list_array)], false, vec![3, 0, 0, 1, 0, 2])?;
        verify_longest_length(&[Arc::clone(&list_array)], true, vec![3, 0, 1, 1, 1, 2])?;

        // Test with single LargeListArray
        //  [A, B, C], [], NULL, [D], NULL, [NULL, F]
        let list_array = Arc::new(make_generic_array::<i64>()) as ArrayRef;
        verify_longest_length(&[Arc::clone(&list_array)], false, vec![3, 0, 0, 1, 0, 2])?;
        verify_longest_length(&[Arc::clone(&list_array)], true, vec![3, 0, 1, 1, 1, 2])?;

        // Test with single FixedSizeListArray
        //  [A, B], NULL, [C, D], NULL, [NULL, F], [NULL, NULL]
        let list_array = Arc::new(make_fixed_list()) as ArrayRef;
        verify_longest_length(&[Arc::clone(&list_array)], false, vec![2, 0, 2, 0, 2, 2])?;
        verify_longest_length(&[Arc::clone(&list_array)], true, vec![2, 1, 2, 1, 2, 2])?;

        // Test with multiple list arrays
        //  [A, B, C], [], NULL, [D], NULL, [NULL, F]
        //  [A, B], NULL, [C, D], NULL, [NULL, F], [NULL, NULL]
        let list1 = Arc::new(make_generic_array::<i32>()) as ArrayRef;
        let list2 = Arc::new(make_fixed_list()) as ArrayRef;
        let list_arrays = vec![Arc::clone(&list1), Arc::clone(&list2)];
        verify_longest_length(&list_arrays, false, vec![3, 0, 2, 1, 2, 2])?;
        verify_longest_length(&list_arrays, true, vec![3, 1, 2, 1, 2, 2])?;

        Ok(())
    }

    #[test]
    fn test_create_take_indicies() -> datafusion_common::Result<()> {
        let length_array = Int64Array::from(vec![2, 3, 1]);
        let take_indicies = create_take_indicies(&length_array, 6);
        let expected = Int64Array::from(vec![0, 0, 1, 1, 1, 2]);
        assert_eq!(take_indicies, expected);
        Ok(())
    }
}
