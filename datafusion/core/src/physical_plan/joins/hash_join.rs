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

//! Defines the join plan for executing partitions in parallel and then joining the results
//! into a set of partitions.

use ahash::RandomState;

use arrow::{
    array::{
        ArrayData, ArrayRef, BooleanArray, Date32Array, Date64Array, Decimal128Array,
        DictionaryArray, LargeStringArray, PrimitiveArray, Time32MillisecondArray,
        Time32SecondArray, Time64MicrosecondArray, Time64NanosecondArray,
        TimestampMicrosecondArray, TimestampMillisecondArray, TimestampSecondArray,
        UInt32BufferBuilder, UInt32Builder, UInt64BufferBuilder,
    },
    compute,
    datatypes::{
        Int16Type, Int32Type, Int64Type, Int8Type, UInt16Type, UInt32Type, UInt64Type,
        UInt8Type,
    },
};
use smallvec::{smallvec, SmallVec};
use std::sync::Arc;
use std::{any::Any, usize};
use std::{time::Instant, vec};

use futures::{ready, Stream, StreamExt, TryStreamExt};

use arrow::array::{new_null_array, Array};
use arrow::datatypes::{ArrowNativeType, DataType};
use arrow::datatypes::{Schema, SchemaRef};
use arrow::error::{ArrowError, Result as ArrowResult};
use arrow::record_batch::RecordBatch;

use arrow::array::{
    Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array,
    StringArray, TimestampNanosecondArray, UInt16Array, UInt32Array, UInt64Array,
    UInt8Array,
};

use datafusion_common::cast::{as_boolean_array, as_dictionary_array, as_string_array};

use hashbrown::raw::RawTable;

use crate::physical_plan::{
    coalesce_batches::concat_batches,
    coalesce_partitions::CoalescePartitionsExec,
    expressions::Column,
    expressions::PhysicalSortExpr,
    hash_utils::create_hashes,
    joins::utils::{
        adjust_right_output_partitioning, build_join_schema, check_join_is_valid,
        combine_join_equivalence_properties, estimate_join_statistics,
        partitioned_join_output_partitioning, ColumnIndex, JoinFilter, JoinOn, JoinSide,
    },
    metrics::{self, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet},
    DisplayFormatType, Distribution, EquivalenceProperties, ExecutionPlan, Partitioning,
    PhysicalExpr, RecordBatchStream, SendableRecordBatchStream, Statistics,
};

use crate::error::{DataFusionError, Result};
use crate::logical_expr::JoinType;

use crate::arrow::array::BooleanBufferBuilder;
use crate::arrow::datatypes::TimeUnit;
use crate::execution::context::TaskContext;

use super::{
    utils::{OnceAsync, OnceFut},
    PartitionMode,
};
use log::debug;
use std::fmt;
use std::task::Poll;

// Maps a `u64` hash value based on the left ["on" values] to a list of indices with this key's value.
//
// Note that the `u64` keys are not stored in the hashmap (hence the `()` as key), but are only used
// to put the indices in a certain bucket.
// By allocating a `HashMap` with capacity for *at least* the number of rows for entries at the left side,
// we make sure that we don't have to re-hash the hashmap, which needs access to the key (the hash in this case) value.
// E.g. 1 -> [3, 6, 8] indicates that the column values map to rows 3, 6 and 8 for hash value 1
// As the key is a hash value, we need to check possible hash collisions in the probe stage
// During this stage it might be the case that a row is contained the same hashmap value,
// but the values don't match. Those are checked in the [equal_rows] macro
// TODO: speed up collision check and move away from using a hashbrown HashMap
// https://github.com/apache/arrow-datafusion/issues/50
struct JoinHashMap(RawTable<(u64, SmallVec<[u64; 1]>)>);

impl fmt::Debug for JoinHashMap {
    fn fmt(&self, _f: &mut fmt::Formatter) -> fmt::Result {
        Ok(())
    }
}

type JoinLeftData = (JoinHashMap, RecordBatch);

/// Join execution plan executes partitions in parallel and combines them into a set of
/// partitions.
///
/// Filter expression expected to contain non-equality predicates that can not be pushed
/// down to any of join inputs.
/// In case of outer join, filter applied to only matched rows.
#[derive(Debug)]
pub struct HashJoinExec {
    /// left (build) side which gets hashed
    pub(crate) left: Arc<dyn ExecutionPlan>,
    /// right (probe) side which are filtered by the hash table
    pub(crate) right: Arc<dyn ExecutionPlan>,
    /// Set of common columns used to join on
    pub(crate) on: Vec<(Column, Column)>,
    /// Filters which are applied while finding matching rows
    pub(crate) filter: Option<JoinFilter>,
    /// How the join is performed
    pub(crate) join_type: JoinType,
    /// The schema once the join is applied
    schema: SchemaRef,
    /// Build-side data
    left_fut: OnceAsync<JoinLeftData>,
    /// Shares the `RandomState` for the hashing algorithm
    random_state: RandomState,
    /// Partitioning mode to use
    pub(crate) mode: PartitionMode,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// Information of index and left / right placement of columns
    column_indices: Vec<ColumnIndex>,
    /// If null_equals_null is true, null == null else null != null
    pub(crate) null_equals_null: bool,
}

/// Metrics for HashJoinExec
#[derive(Debug)]
struct HashJoinMetrics {
    /// Total time for joining probe-side batches to the build-side batches
    probe_time: metrics::Time,
    /// Total time for building hashmap
    build_time: metrics::Time,
    /// Number of batches consumed by this operator
    input_batches: metrics::Count,
    /// Number of rows consumed by this operator
    input_rows: metrics::Count,
    /// Number of batches produced by this operator
    output_batches: metrics::Count,
    /// Number of rows produced by this operator
    output_rows: metrics::Count,
}

impl HashJoinMetrics {
    pub fn new(partition: usize, metrics: &ExecutionPlanMetricsSet) -> Self {
        let probe_time = MetricBuilder::new(metrics).subset_time("probe_time", partition);

        let build_time = MetricBuilder::new(metrics).subset_time("build_time", partition);

        let input_batches =
            MetricBuilder::new(metrics).counter("input_batches", partition);

        let input_rows = MetricBuilder::new(metrics).counter("input_rows", partition);

        let output_batches =
            MetricBuilder::new(metrics).counter("output_batches", partition);

        let output_rows = MetricBuilder::new(metrics).output_rows(partition);

        Self {
            probe_time,
            build_time,
            input_batches,
            input_rows,
            output_batches,
            output_rows,
        }
    }
}

impl HashJoinExec {
    /// Tries to create a new [HashJoinExec].
    /// # Error
    /// This function errors when it is not possible to join the left and right sides on keys `on`.
    pub fn try_new(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: JoinOn,
        filter: Option<JoinFilter>,
        join_type: &JoinType,
        partition_mode: PartitionMode,
        null_equals_null: &bool,
    ) -> Result<Self> {
        let left_schema = left.schema();
        let right_schema = right.schema();
        if on.is_empty() {
            return Err(DataFusionError::Plan(
                "On constraints in HashJoinExec should be non-empty".to_string(),
            ));
        }

        check_join_is_valid(&left_schema, &right_schema, &on)?;

        let (schema, column_indices) =
            build_join_schema(&left_schema, &right_schema, join_type);

        let random_state = RandomState::with_seeds(0, 0, 0, 0);

        Ok(HashJoinExec {
            left,
            right,
            on,
            filter,
            join_type: *join_type,
            schema: Arc::new(schema),
            left_fut: Default::default(),
            random_state,
            mode: partition_mode,
            metrics: ExecutionPlanMetricsSet::new(),
            column_indices,
            null_equals_null: *null_equals_null,
        })
    }

    /// left (build) side which gets hashed
    pub fn left(&self) -> &Arc<dyn ExecutionPlan> {
        &self.left
    }

    /// right (probe) side which are filtered by the hash table
    pub fn right(&self) -> &Arc<dyn ExecutionPlan> {
        &self.right
    }

    /// Set of common columns used to join on
    pub fn on(&self) -> &[(Column, Column)] {
        &self.on
    }

    /// Filters applied before join output
    pub fn filter(&self) -> Option<&JoinFilter> {
        self.filter.as_ref()
    }

    /// How the join is performed
    pub fn join_type(&self) -> &JoinType {
        &self.join_type
    }

    /// The partitioning mode of this hash join
    pub fn partition_mode(&self) -> &PartitionMode {
        &self.mode
    }

    /// Get null_equals_null
    pub fn null_equals_null(&self) -> &bool {
        &self.null_equals_null
    }
}

impl ExecutionPlan for HashJoinExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        match self.mode {
            PartitionMode::CollectLeft => vec![
                Distribution::SinglePartition,
                Distribution::UnspecifiedDistribution,
            ],
            PartitionMode::Partitioned => {
                let (left_expr, right_expr) = self
                    .on
                    .iter()
                    .map(|(l, r)| {
                        (
                            Arc::new(l.clone()) as Arc<dyn PhysicalExpr>,
                            Arc::new(r.clone()) as Arc<dyn PhysicalExpr>,
                        )
                    })
                    .unzip();
                vec![
                    Distribution::HashPartitioned(left_expr),
                    Distribution::HashPartitioned(right_expr),
                ]
            }
            PartitionMode::Auto => vec![
                Distribution::UnspecifiedDistribution,
                Distribution::UnspecifiedDistribution,
            ],
        }
    }

    fn output_partitioning(&self) -> Partitioning {
        let left_columns_len = self.left.schema().fields.len();
        match self.mode {
            PartitionMode::CollectLeft => match self.join_type {
                JoinType::Inner | JoinType::Right => adjust_right_output_partitioning(
                    self.right.output_partitioning(),
                    left_columns_len,
                ),
                JoinType::RightSemi | JoinType::RightAnti => {
                    self.right.output_partitioning()
                }
                JoinType::Left
                | JoinType::LeftSemi
                | JoinType::LeftAnti
                | JoinType::Full => Partitioning::UnknownPartitioning(
                    self.right.output_partitioning().partition_count(),
                ),
            },
            PartitionMode::Partitioned => partitioned_join_output_partitioning(
                self.join_type,
                self.left.output_partitioning(),
                self.right.output_partitioning(),
                left_columns_len,
            ),
            PartitionMode::Auto => Partitioning::UnknownPartitioning(
                self.right.output_partitioning().partition_count(),
            ),
        }
    }

    // TODO Output ordering might be kept for some cases.
    // For example if it is inner join then the stream side order can be kept
    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn equivalence_properties(&self) -> EquivalenceProperties {
        let left_columns_len = self.left.schema().fields.len();
        combine_join_equivalence_properties(
            self.join_type,
            self.left.equivalence_properties(),
            self.right.equivalence_properties(),
            left_columns_len,
            self.on(),
            self.schema(),
        )
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.left.clone(), self.right.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(HashJoinExec::try_new(
            children[0].clone(),
            children[1].clone(),
            self.on.clone(),
            self.filter.clone(),
            &self.join_type,
            self.mode,
            &self.null_equals_null,
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let on_left = self.on.iter().map(|on| on.0.clone()).collect::<Vec<_>>();
        let on_right = self.on.iter().map(|on| on.1.clone()).collect::<Vec<_>>();

        let left_fut = match self.mode {
            PartitionMode::CollectLeft => self.left_fut.once(|| {
                collect_left_input(
                    self.random_state.clone(),
                    self.left.clone(),
                    on_left.clone(),
                    context.clone(),
                )
            }),
            PartitionMode::Partitioned => OnceFut::new(partitioned_left_input(
                partition,
                self.random_state.clone(),
                self.left.clone(),
                on_left.clone(),
                context.clone(),
            )),
            PartitionMode::Auto => {
                return Err(DataFusionError::Plan(format!(
                    "Invalid HashJoinExec, unsupported PartitionMode {:?} in execute()",
                    PartitionMode::Auto
                )));
            }
        };

        // we have the batches and the hash map with their keys. We can how create a stream
        // over the right that uses this information to issue new batches.
        let right_stream = self.right.execute(partition, context)?;

        Ok(Box::pin(HashJoinStream {
            schema: self.schema(),
            on_left,
            on_right,
            filter: self.filter.clone(),
            join_type: self.join_type,
            left_fut,
            visited_left_side: None,
            right: right_stream,
            column_indices: self.column_indices.clone(),
            random_state: self.random_state.clone(),
            join_metrics: HashJoinMetrics::new(partition, &self.metrics),
            null_equals_null: self.null_equals_null,
            is_exhausted: false,
        }))
    }

    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default => {
                let display_filter = self.filter.as_ref().map_or_else(
                    || "".to_string(),
                    |f| format!(", filter={:?}", f.expression()),
                );
                write!(
                    f,
                    "HashJoinExec: mode={:?}, join_type={:?}, on={:?}{}",
                    self.mode, self.join_type, self.on, display_filter
                )
            }
        }
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Statistics {
        // TODO stats: it is not possible in general to know the output size of joins
        // There are some special cases though, for example:
        // - `A LEFT JOIN B ON A.col=B.col` with `COUNT_DISTINCT(B.col)=COUNT(B.col)`
        estimate_join_statistics(
            self.left.clone(),
            self.right.clone(),
            self.on.clone(),
            &self.join_type,
        )
    }
}

async fn collect_left_input(
    random_state: RandomState,
    left: Arc<dyn ExecutionPlan>,
    on_left: Vec<Column>,
    context: Arc<TaskContext>,
) -> Result<JoinLeftData> {
    let schema = left.schema();
    let start = Instant::now();
    // merge all left parts into a single stream
    let merge = {
        if left.output_partitioning().partition_count() != 1 {
            Arc::new(CoalescePartitionsExec::new(left))
        } else {
            left
        }
    };
    let stream = merge.execute(0, context)?;

    // This operation performs 2 steps at once:
    // 1. creates a [JoinHashMap] of all batches from the stream
    // 2. stores the batches in a vector.
    let initial = (0, Vec::new());
    let (num_rows, batches) = stream
        .try_fold(initial, |mut acc, batch| async {
            acc.0 += batch.num_rows();
            acc.1.push(batch);
            Ok(acc)
        })
        .await?;

    let mut hashmap = JoinHashMap(RawTable::with_capacity(num_rows));
    let mut hashes_buffer = Vec::new();
    let mut offset = 0;
    for batch in batches.iter() {
        hashes_buffer.clear();
        hashes_buffer.resize(batch.num_rows(), 0);
        update_hash(
            &on_left,
            batch,
            &mut hashmap,
            offset,
            &random_state,
            &mut hashes_buffer,
        )?;
        offset += batch.num_rows();
    }
    // Merge all batches into a single batch, so we
    // can directly index into the arrays
    let single_batch = concat_batches(&schema, &batches, num_rows)?;

    debug!(
        "Built build-side of hash join containing {} rows in {} ms",
        num_rows,
        start.elapsed().as_millis()
    );

    Ok((hashmap, single_batch))
}

async fn partitioned_left_input(
    partition: usize,
    random_state: RandomState,
    left: Arc<dyn ExecutionPlan>,
    on_left: Vec<Column>,
    context: Arc<TaskContext>,
) -> Result<JoinLeftData> {
    let schema = left.schema();

    let start = Instant::now();

    // Load 1 partition of left side in memory
    let stream = left.execute(partition, context.clone())?;

    // This operation performs 2 steps at once:
    // 1. creates a [JoinHashMap] of all batches from the stream
    // 2. stores the batches in a vector.
    let initial = (0, Vec::new());
    let (num_rows, batches) = stream
        .try_fold(initial, |mut acc, batch| async {
            acc.0 += batch.num_rows();
            acc.1.push(batch);
            Ok(acc)
        })
        .await?;

    let mut hashmap = JoinHashMap(RawTable::with_capacity(num_rows));
    let mut hashes_buffer = Vec::new();
    let mut offset = 0;
    for batch in batches.iter() {
        hashes_buffer.clear();
        hashes_buffer.resize(batch.num_rows(), 0);
        update_hash(
            &on_left,
            batch,
            &mut hashmap,
            offset,
            &random_state,
            &mut hashes_buffer,
        )?;
        offset += batch.num_rows();
    }
    // Merge all batches into a single batch, so we
    // can directly index into the arrays
    let single_batch = concat_batches(&schema, &batches, num_rows)?;

    debug!(
        "Built build-side {} of hash join containing {} rows in {} ms",
        partition,
        num_rows,
        start.elapsed().as_millis()
    );

    Ok((hashmap, single_batch))
}

/// Updates `hash` with new entries from [RecordBatch] evaluated against the expressions `on`,
/// assuming that the [RecordBatch] corresponds to the `index`th
fn update_hash(
    on: &[Column],
    batch: &RecordBatch,
    hash_map: &mut JoinHashMap,
    offset: usize,
    random_state: &RandomState,
    hashes_buffer: &mut Vec<u64>,
) -> Result<()> {
    // evaluate the keys
    let keys_values = on
        .iter()
        .map(|c| Ok(c.evaluate(batch)?.into_array(batch.num_rows())))
        .collect::<Result<Vec<_>>>()?;

    // calculate the hash values
    let hash_values = create_hashes(&keys_values, random_state, hashes_buffer)?;

    // insert hashes to key of the hashmap
    for (row, hash_value) in hash_values.iter().enumerate() {
        let item = hash_map
            .0
            .get_mut(*hash_value, |(hash, _)| *hash_value == *hash);
        if let Some((_, indices)) = item {
            indices.push((row + offset) as u64);
        } else {
            hash_map.0.insert(
                *hash_value,
                (*hash_value, smallvec![(row + offset) as u64]),
                |(hash, _)| *hash,
            );
        }
    }
    Ok(())
}

/// A stream that issues [RecordBatch]es as they arrive from the right  of the join.
struct HashJoinStream {
    /// Input schema
    schema: Arc<Schema>,
    /// columns from the left
    on_left: Vec<Column>,
    /// columns from the right used to compute the hash
    on_right: Vec<Column>,
    /// join filter
    filter: Option<JoinFilter>,
    /// type of the join
    join_type: JoinType,
    /// future for data from left side
    left_fut: OnceFut<JoinLeftData>,
    /// Keeps track of the left side rows whether they are visited
    visited_left_side: Option<BooleanBufferBuilder>,
    /// right
    right: SendableRecordBatchStream,
    /// Random state used for hashing initialization
    random_state: RandomState,
    /// There is nothing to process anymore and left side is processed in case of left join
    is_exhausted: bool,
    /// Metrics
    join_metrics: HashJoinMetrics,
    /// Information of index and left / right placement of columns
    column_indices: Vec<ColumnIndex>,
    /// If null_equals_null is true, null == null else null != null
    null_equals_null: bool,
}

impl RecordBatchStream for HashJoinStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// Returns a new [RecordBatch] by combining the `left` and `right` according to `indices`.
/// The resulting batch has [Schema] `schema`.
fn build_batch_from_indices(
    schema: &Schema,
    left: &RecordBatch,
    right: &RecordBatch,
    left_indices: UInt64Array,
    right_indices: UInt32Array,
    column_indices: &[ColumnIndex],
) -> ArrowResult<RecordBatch> {
    // build the columns of the new [RecordBatch]:
    // 1. pick whether the column is from the left or right
    // 2. based on the pick, `take` items from the different RecordBatches
    let mut columns: Vec<Arc<dyn Array>> = Vec::with_capacity(schema.fields().len());

    for column_index in column_indices {
        let array = match column_index.side {
            JoinSide::Left => {
                let array = left.column(column_index.index);
                if array.is_empty() || left_indices.null_count() == left_indices.len() {
                    // Outer join would generate a null index when finding no match at our side.
                    // Therefore, it's possible we are empty but need to populate an n-length null array,
                    // where n is the length of the index array.
                    assert_eq!(left_indices.null_count(), left_indices.len());
                    new_null_array(array.data_type(), left_indices.len())
                } else {
                    compute::take(array.as_ref(), &left_indices, None)?
                }
            }
            JoinSide::Right => {
                let array = right.column(column_index.index);
                if array.is_empty() || right_indices.null_count() == right_indices.len() {
                    assert_eq!(right_indices.null_count(), right_indices.len());
                    new_null_array(array.data_type(), right_indices.len())
                } else {
                    compute::take(array.as_ref(), &right_indices, None)?
                }
            }
        };
        columns.push(array);
    }
    RecordBatch::try_new(Arc::new(schema.clone()), columns)
}

// Get left and right indices which is satisfies the on condition (include equal_conditon and filter_in_join) in the Join
#[allow(clippy::too_many_arguments)]
fn build_join_indices(
    batch: &RecordBatch,
    left_data: &JoinLeftData,
    on_left: &[Column],
    on_right: &[Column],
    filter: Option<&JoinFilter>,
    random_state: &RandomState,
    null_equals_null: &bool,
) -> Result<(UInt64Array, UInt32Array)> {
    // Get the indices which is satisfies the equal join condition, like `left.a1 = right.a2`
    let (left_indices, right_indices) = build_equal_condition_join_indices(
        left_data,
        batch,
        on_left,
        on_right,
        random_state,
        null_equals_null,
    )?;
    if let Some(filter) = filter {
        // Filter the indices which is satisfies the non-equal join condition, like `left.b1 = 10`
        apply_join_filter_to_indices(
            &left_data.1,
            batch,
            left_indices,
            right_indices,
            filter,
        )
    } else {
        Ok((left_indices, right_indices))
    }
}

// Returns the index of equal condition join result: left_indices and right_indices
// On LEFT.b1 = RIGHT.b2
// LEFT Table:
//  a1  b1  c1
//  1   1   10
//  3   3   30
//  5   5   50
//  7   7   70
//  9   8   90
//  11  8   110
// 13   10  130
// RIGHT Table:
//  a2   b2  c2
//  2    2   20
//  4    4   40
//  6    6   60
//  8    8   80
// 10   10  100
// 12   10  120
// The result is
// "+----+----+-----+----+----+-----+",
// "| a1 | b1 | c1  | a2 | b2 | c2  |",
// "+----+----+-----+----+----+-----+",
// "| 11 | 8  | 110 | 8  | 8  | 80  |",
// "| 13 | 10 | 130 | 10 | 10 | 100 |",
// "| 13 | 10 | 130 | 12 | 10 | 120 |",
// "| 9  | 8  | 90  | 8  | 8  | 80  |",
// "+----+----+-----+----+----+-----+"
// And the result of left and right indices
// left indices:  5, 6, 6, 4
// right indices: 3, 4, 5, 3
fn build_equal_condition_join_indices(
    left_data: &JoinLeftData,
    right: &RecordBatch,
    left_on: &[Column],
    right_on: &[Column],
    random_state: &RandomState,
    null_equals_null: &bool,
) -> Result<(UInt64Array, UInt32Array)> {
    let keys_values = right_on
        .iter()
        .map(|c| Ok(c.evaluate(right)?.into_array(right.num_rows())))
        .collect::<Result<Vec<_>>>()?;
    let left_join_values = left_on
        .iter()
        .map(|c| Ok(c.evaluate(&left_data.1)?.into_array(left_data.1.num_rows())))
        .collect::<Result<Vec<_>>>()?;
    let hashes_buffer = &mut vec![0; keys_values[0].len()];
    let hash_values = create_hashes(&keys_values, random_state, hashes_buffer)?;
    let left = &left_data.0;
    // Using a buffer builder to avoid slower normal builder
    let mut left_indices = UInt64BufferBuilder::new(0);
    let mut right_indices = UInt32BufferBuilder::new(0);

    // Visit all of the right rows
    for (row, hash_value) in hash_values.iter().enumerate() {
        // Get the hash and find it in the build index

        // For every item on the left and right we check if it matches
        // This possibly contains rows with hash collisions,
        // So we have to check here whether rows are equal or not
        if let Some((_, indices)) =
            left.0.get(*hash_value, |(hash, _)| *hash_value == *hash)
        {
            for &i in indices {
                // Check hash collisions
                if equal_rows(
                    i as usize,
                    row,
                    &left_join_values,
                    &keys_values,
                    *null_equals_null,
                )? {
                    left_indices.append(i);
                    right_indices.append(row as u32);
                }
            }
        }
    }
    let left = ArrayData::builder(DataType::UInt64)
        .len(left_indices.len())
        .add_buffer(left_indices.finish())
        .build()
        .unwrap();
    let right = ArrayData::builder(DataType::UInt32)
        .len(right_indices.len())
        .add_buffer(right_indices.finish())
        .build()
        .unwrap();

    Ok((
        PrimitiveArray::<UInt64Type>::from(left),
        PrimitiveArray::<UInt32Type>::from(right),
    ))
}

fn apply_join_filter_to_indices(
    left: &RecordBatch,
    right: &RecordBatch,
    left_indices: UInt64Array,
    right_indices: UInt32Array,
    filter: &JoinFilter,
) -> Result<(UInt64Array, UInt32Array)> {
    if left_indices.is_empty() && right_indices.is_empty() {
        return Ok((left_indices, right_indices));
    };

    let intermediate_batch = build_batch_from_indices(
        filter.schema(),
        left,
        right,
        PrimitiveArray::from(left_indices.data().clone()),
        PrimitiveArray::from(right_indices.data().clone()),
        filter.column_indices(),
    )?;
    let filter_result = filter
        .expression()
        .evaluate(&intermediate_batch)?
        .into_array(intermediate_batch.num_rows());
    let mask = as_boolean_array(&filter_result)?;

    let left_filtered = PrimitiveArray::<UInt64Type>::from(
        compute::filter(&left_indices, mask)?.data().clone(),
    );
    let right_filtered = PrimitiveArray::<UInt32Type>::from(
        compute::filter(&right_indices, mask)?.data().clone(),
    );

    Ok((left_filtered, right_filtered))
}

macro_rules! equal_rows_elem {
    ($array_type:ident, $l: ident, $r: ident, $left: ident, $right: ident, $null_equals_null: ident) => {{
        let left_array = $l.as_any().downcast_ref::<$array_type>().unwrap();
        let right_array = $r.as_any().downcast_ref::<$array_type>().unwrap();

        match (left_array.is_null($left), right_array.is_null($right)) {
            (false, false) => left_array.value($left) == right_array.value($right),
            (true, true) => $null_equals_null,
            _ => false,
        }
    }};
}

macro_rules! equal_rows_elem_with_string_dict {
    ($key_array_type:ident, $l: ident, $r: ident, $left: ident, $right: ident, $null_equals_null: ident) => {{
        let left_array: &DictionaryArray<$key_array_type> =
            as_dictionary_array::<$key_array_type>($l).unwrap();
        let right_array: &DictionaryArray<$key_array_type> =
            as_dictionary_array::<$key_array_type>($r).unwrap();

        let (left_values, left_values_index) = {
            let keys_col = left_array.keys();
            if keys_col.is_valid($left) {
                let values_index = keys_col
                    .value($left)
                    .to_usize()
                    .expect("Can not convert index to usize in dictionary");

                (
                    as_string_array(left_array.values()).unwrap(),
                    Some(values_index),
                )
            } else {
                (as_string_array(left_array.values()).unwrap(), None)
            }
        };
        let (right_values, right_values_index) = {
            let keys_col = right_array.keys();
            if keys_col.is_valid($right) {
                let values_index = keys_col
                    .value($right)
                    .to_usize()
                    .expect("Can not convert index to usize in dictionary");

                (
                    as_string_array(right_array.values()).unwrap(),
                    Some(values_index),
                )
            } else {
                (as_string_array(right_array.values()).unwrap(), None)
            }
        };

        match (left_values_index, right_values_index) {
            (Some(left_values_index), Some(right_values_index)) => {
                left_values.value(left_values_index)
                    == right_values.value(right_values_index)
            }
            (None, None) => $null_equals_null,
            _ => false,
        }
    }};
}

/// Left and right row have equal values
/// If more data types are supported here, please also add the data types in can_hash function
/// to generate hash join logical plan.
fn equal_rows(
    left: usize,
    right: usize,
    left_arrays: &[ArrayRef],
    right_arrays: &[ArrayRef],
    null_equals_null: bool,
) -> Result<bool> {
    let mut err = None;
    let res = left_arrays
        .iter()
        .zip(right_arrays)
        .all(|(l, r)| match l.data_type() {
            DataType::Null => {
                // lhs and rhs are both `DataType::Null`, so the equal result
                // is dependent on `null_equals_null`
                null_equals_null
            }
            DataType::Boolean => {
                equal_rows_elem!(BooleanArray, l, r, left, right, null_equals_null)
            }
            DataType::Int8 => {
                equal_rows_elem!(Int8Array, l, r, left, right, null_equals_null)
            }
            DataType::Int16 => {
                equal_rows_elem!(Int16Array, l, r, left, right, null_equals_null)
            }
            DataType::Int32 => {
                equal_rows_elem!(Int32Array, l, r, left, right, null_equals_null)
            }
            DataType::Int64 => {
                equal_rows_elem!(Int64Array, l, r, left, right, null_equals_null)
            }
            DataType::UInt8 => {
                equal_rows_elem!(UInt8Array, l, r, left, right, null_equals_null)
            }
            DataType::UInt16 => {
                equal_rows_elem!(UInt16Array, l, r, left, right, null_equals_null)
            }
            DataType::UInt32 => {
                equal_rows_elem!(UInt32Array, l, r, left, right, null_equals_null)
            }
            DataType::UInt64 => {
                equal_rows_elem!(UInt64Array, l, r, left, right, null_equals_null)
            }
            DataType::Float32 => {
                equal_rows_elem!(Float32Array, l, r, left, right, null_equals_null)
            }
            DataType::Float64 => {
                equal_rows_elem!(Float64Array, l, r, left, right, null_equals_null)
            }
            DataType::Date32 => {
                equal_rows_elem!(Date32Array, l, r, left, right, null_equals_null)
            }
            DataType::Date64 => {
                equal_rows_elem!(Date64Array, l, r, left, right, null_equals_null)
            }
            DataType::Time32(time_unit) => match time_unit {
                TimeUnit::Second => {
                    equal_rows_elem!(Time32SecondArray, l, r, left, right, null_equals_null)
                }
                TimeUnit::Millisecond => {
                    equal_rows_elem!(Time32MillisecondArray, l, r, left, right, null_equals_null)
                }
                _ => {
                    err = Some(Err(DataFusionError::Internal(
                        "Unsupported data type in hasher".to_string(),
                    )));
                    false
                }
            }
            DataType::Time64(time_unit) => match time_unit {
                TimeUnit::Microsecond => {
                    equal_rows_elem!(Time64MicrosecondArray, l, r, left, right, null_equals_null)
                }
                TimeUnit::Nanosecond => {
                    equal_rows_elem!(Time64NanosecondArray, l, r, left, right, null_equals_null)
                }
                _ => {
                    err = Some(Err(DataFusionError::Internal(
                        "Unsupported data type in hasher".to_string(),
                    )));
                    false
                }
            }
            DataType::Timestamp(time_unit, None) => match time_unit {
                TimeUnit::Second => {
                    equal_rows_elem!(
                        TimestampSecondArray,
                        l,
                        r,
                        left,
                        right,
                        null_equals_null
                    )
                }
                TimeUnit::Millisecond => {
                    equal_rows_elem!(
                        TimestampMillisecondArray,
                        l,
                        r,
                        left,
                        right,
                        null_equals_null
                    )
                }
                TimeUnit::Microsecond => {
                    equal_rows_elem!(
                        TimestampMicrosecondArray,
                        l,
                        r,
                        left,
                        right,
                        null_equals_null
                    )
                }
                TimeUnit::Nanosecond => {
                    equal_rows_elem!(
                        TimestampNanosecondArray,
                        l,
                        r,
                        left,
                        right,
                        null_equals_null
                    )
                }
            },
            DataType::Utf8 => {
                equal_rows_elem!(StringArray, l, r, left, right, null_equals_null)
            }
            DataType::LargeUtf8 => {
                equal_rows_elem!(LargeStringArray, l, r, left, right, null_equals_null)
            }
            DataType::Decimal128(_, lscale) => match r.data_type() {
                DataType::Decimal128(_, rscale) => {
                    if lscale == rscale {
                        equal_rows_elem!(
                            Decimal128Array,
                            l,
                            r,
                            left,
                            right,
                            null_equals_null
                        )
                    } else {
                        err = Some(Err(DataFusionError::Internal(
                            "Inconsistent Decimal data type in hasher, the scale should be same".to_string(),
                        )));
                        false
                    }
                }
                _ => {
                    err = Some(Err(DataFusionError::Internal(
                        "Unsupported data type in hasher".to_string(),
                    )));
                    false
                }
            },
            DataType::Dictionary(key_type, value_type)
            if *value_type.as_ref() == DataType::Utf8 =>
                {
                    match key_type.as_ref() {
                        DataType::Int8 => {
                            equal_rows_elem_with_string_dict!(
                            Int8Type,
                            l,
                            r,
                            left,
                            right,
                            null_equals_null
                        )
                        }
                        DataType::Int16 => {
                            equal_rows_elem_with_string_dict!(
                            Int16Type,
                            l,
                            r,
                            left,
                            right,
                            null_equals_null
                        )
                        }
                        DataType::Int32 => {
                            equal_rows_elem_with_string_dict!(
                            Int32Type,
                            l,
                            r,
                            left,
                            right,
                            null_equals_null
                        )
                        }
                        DataType::Int64 => {
                            equal_rows_elem_with_string_dict!(
                            Int64Type,
                            l,
                            r,
                            left,
                            right,
                            null_equals_null
                        )
                        }
                        DataType::UInt8 => {
                            equal_rows_elem_with_string_dict!(
                            UInt8Type,
                            l,
                            r,
                            left,
                            right,
                            null_equals_null
                        )
                        }
                        DataType::UInt16 => {
                            equal_rows_elem_with_string_dict!(
                            UInt16Type,
                            l,
                            r,
                            left,
                            right,
                            null_equals_null
                        )
                        }
                        DataType::UInt32 => {
                            equal_rows_elem_with_string_dict!(
                            UInt32Type,
                            l,
                            r,
                            left,
                            right,
                            null_equals_null
                        )
                        }
                        DataType::UInt64 => {
                            equal_rows_elem_with_string_dict!(
                            UInt64Type,
                            l,
                            r,
                            left,
                            right,
                            null_equals_null
                        )
                        }
                        _ => {
                            // should not happen
                            err = Some(Err(DataFusionError::Internal(
                                "Unsupported data type in hasher".to_string(),
                            )));
                            false
                        }
                    }
                }
            other => {
                // This is internal because we should have caught this before.
                err = Some(Err(DataFusionError::Internal(format!(
                    "Unsupported data type in hasher: {}",
                    other
                ))));
                false
            }
        });

    err.unwrap_or(Ok(res))
}

// The input is the matched indices for left and right.
// Adjust the indices according to the join type
fn adjust_indices_by_join_type(
    left_indices: UInt64Array,
    right_indices: UInt32Array,
    count_right_batch: usize,
    join_type: JoinType,
) -> (UInt64Array, UInt32Array) {
    match join_type {
        JoinType::Inner => {
            // matched
            (left_indices, right_indices)
        }
        JoinType::Left => {
            // matched
            (left_indices, right_indices)
            // unmatched left row will be produced in the end of loop, and it has been set in the left visited bitmap
        }
        JoinType::Right | JoinType::Full => {
            // matched
            // unmatched right row will be produced in this batch
            let right_unmatched_indices =
                get_anti_indices(count_right_batch, &right_indices);
            // combine the matched and unmatched right result together
            append_right_indices(left_indices, right_indices, right_unmatched_indices)
        }
        JoinType::RightSemi => {
            // need to remove the duplicated record in the right side
            let right_indices = get_semi_indices(count_right_batch, &right_indices);
            // the left_indices will not be used later for the `right semi` join
            (left_indices, right_indices)
        }
        JoinType::RightAnti => {
            // need to remove the duplicated record in the right side
            // get the anti index for the right side
            let right_indices = get_anti_indices(count_right_batch, &right_indices);
            // the left_indices will not be used later for the `right anti` join
            (left_indices, right_indices)
        }
        JoinType::LeftSemi | JoinType::LeftAnti => {
            // matched or unmatched left row will be produced in the end of loop
            // TODO: left semi can be optimized.
            // When visit the right batch, we can output the matched left row and don't need to wait the end of loop
            (
                UInt64Array::from_iter_values(vec![]),
                UInt32Array::from_iter_values(vec![]),
            )
        }
    }
}

fn append_right_indices(
    left_indices: UInt64Array,
    right_indices: UInt32Array,
    appended_right_indices: UInt32Array,
) -> (UInt64Array, UInt32Array) {
    // left_indices, right_indices and appended_right_indices must not contain the null value
    if appended_right_indices.is_empty() {
        (left_indices, right_indices)
    } else {
        let unmatched_size = appended_right_indices.len();
        // the new left indices: left_indices + null array
        // the new right indices: right_indices + appended_right_indices
        let new_left_indices = left_indices
            .iter()
            .chain(std::iter::repeat(None).take(unmatched_size))
            .collect::<UInt64Array>();
        let new_right_indices = right_indices
            .iter()
            .chain(appended_right_indices.iter())
            .collect::<UInt32Array>();
        (new_left_indices, new_right_indices)
    }
}

fn get_anti_indices(row_count: usize, input_indices: &UInt32Array) -> UInt32Array {
    let mut bitmap = BooleanBufferBuilder::new(row_count);
    bitmap.append_n(row_count, false);
    input_indices.iter().flatten().for_each(|v| {
        bitmap.set_bit(v as usize, true);
    });

    // get the anti index
    (0..row_count)
        .filter_map(|idx| (!bitmap.get_bit(idx)).then_some(idx as u32))
        .collect::<UInt32Array>()
}

fn get_semi_indices(row_count: usize, input_indices: &UInt32Array) -> UInt32Array {
    let mut bitmap = BooleanBufferBuilder::new(row_count);
    bitmap.append_n(row_count, false);
    input_indices.iter().flatten().for_each(|v| {
        bitmap.set_bit(v as usize, true);
    });

    // get the semi index
    (0..row_count)
        .filter_map(|idx| (bitmap.get_bit(idx)).then_some(idx as u32))
        .collect::<UInt32Array>()
}

fn need_produce_result_in_final(join_type: JoinType) -> bool {
    matches!(
        join_type,
        JoinType::Left | JoinType::LeftAnti | JoinType::LeftSemi | JoinType::Full
    )
}

fn get_final_indices(
    left_bit_map: &BooleanBufferBuilder,
    join_type: JoinType,
) -> (UInt64Array, UInt32Array) {
    let left_size = left_bit_map.len();
    let left_indices = if join_type == JoinType::LeftSemi {
        (0..left_size)
            .filter_map(|idx| (left_bit_map.get_bit(idx)).then_some(idx as u64))
            .collect::<UInt64Array>()
    } else {
        // just for `Left`, `LeftAnti` and `Full` join
        // `LeftAnti`, `Left` and `Full` will produce the unmatched left row finally
        (0..left_size)
            .filter_map(|idx| (!left_bit_map.get_bit(idx)).then_some(idx as u64))
            .collect::<UInt64Array>()
    };
    // right_indices
    // all the element in the right side is None
    let mut builder = UInt32Builder::with_capacity(left_indices.len());
    builder.append_nulls(left_indices.len());
    let right_indices = builder.finish();
    (left_indices, right_indices)
}

impl HashJoinStream {
    /// Separate implementation function that unpins the [`HashJoinStream`] so
    /// that partial borrows work correctly
    fn poll_next_impl(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<ArrowResult<RecordBatch>>> {
        let build_timer = self.join_metrics.build_time.timer();
        let left_data = match ready!(self.left_fut.get(cx)) {
            Ok(left_data) => left_data,
            Err(e) => return Poll::Ready(Some(Err(e))),
        };
        build_timer.done();

        let visited_left_side = self.visited_left_side.get_or_insert_with(|| {
            let num_rows = left_data.1.num_rows();
            if need_produce_result_in_final(self.join_type) {
                // these join type need the bitmap to identify which row has be matched or unmatched.
                // For the `left semi` join, need to use the bitmap to produce the matched row in the left side
                // For the `left` join, need to use the bitmap to produce the unmatched row in the left side with null
                // For the `left anti` join, need to use the bitmap to produce the unmatched row in the left side
                // For the `full` join, need to use the bitmap to produce the unmatched row in the left side with null
                let mut buffer = BooleanBufferBuilder::new(num_rows);
                buffer.append_n(num_rows, false);
                buffer
            } else {
                BooleanBufferBuilder::new(0)
            }
        });

        self.right
            .poll_next_unpin(cx)
            .map(|maybe_batch| match maybe_batch {
                // one right batch in the join loop
                Some(Ok(batch)) => {
                    self.join_metrics.input_batches.add(1);
                    self.join_metrics.input_rows.add(batch.num_rows());
                    let timer = self.join_metrics.probe_time.timer();

                    // get the matched two indices for the on condition
                    let left_right_indices = build_join_indices(
                        &batch,
                        left_data,
                        &self.on_left,
                        &self.on_right,
                        self.filter.as_ref(),
                        &self.random_state,
                        &self.null_equals_null,
                    );

                    let result = match left_right_indices {
                        Ok((left_side, right_side)) => {
                            // set the left bitmap
                            // and only left, full, left semi, left anti need the left bitmap
                            if need_produce_result_in_final(self.join_type) {
                                left_side.iter().flatten().for_each(|x| {
                                    visited_left_side.set_bit(x as usize, true);
                                });
                            }

                            // adjust the two side indices base on the join type
                            let (left_side, right_side) = adjust_indices_by_join_type(
                                left_side,
                                right_side,
                                batch.num_rows(),
                                self.join_type,
                            );

                            let result = build_batch_from_indices(
                                &self.schema,
                                &left_data.1,
                                &batch,
                                left_side,
                                right_side,
                                &self.column_indices,
                            );
                            self.join_metrics.output_batches.add(1);
                            self.join_metrics.output_rows.add(batch.num_rows());
                            Some(result)
                        }
                        Err(_) => {
                            // TODO why the type of result stream is `Result<T, ArrowError>`, and not the `DataFusionError`
                            Some(Err(ArrowError::ComputeError(
                                "Build left right indices error".to_string(),
                            )))
                        }
                    };
                    timer.done();
                    result
                }
                None => {
                    let timer = self.join_metrics.probe_time.timer();
                    if need_produce_result_in_final(self.join_type) && !self.is_exhausted
                    {
                        // use the global left bitmap to produce the left indices and right indices
                        let (left_side, right_side) =
                            get_final_indices(visited_left_side, self.join_type);
                        let empty_right_batch =
                            RecordBatch::new_empty(self.right.schema());
                        // use the left and right indices to produce the batch result
                        let result = build_batch_from_indices(
                            &self.schema,
                            &left_data.1,
                            &empty_right_batch,
                            left_side,
                            right_side,
                            &self.column_indices,
                        );

                        if let Ok(ref batch) = result {
                            self.join_metrics.input_batches.add(1);
                            self.join_metrics.input_rows.add(batch.num_rows());

                            self.join_metrics.output_batches.add(1);
                            self.join_metrics.output_rows.add(batch.num_rows());
                        }
                        timer.done();
                        self.is_exhausted = true;
                        Some(result)
                    } else {
                        // end of the join loop
                        None
                    }
                }
                Some(err) => Some(err),
            })
    }
}

impl Stream for HashJoinStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.poll_next_impl(cx)
    }
}

#[cfg(test)]
mod tests {
    use crate::physical_expr::expressions::BinaryExpr;
    use crate::{
        assert_batches_sorted_eq,
        physical_plan::{
            common, expressions::Column, memory::MemoryExec, repartition::RepartitionExec,
        },
        test::exec::MockExec,
        test::{build_table_i32, columns},
    };
    use arrow::array::UInt64Builder;
    use arrow::datatypes::Field;
    use arrow::error::ArrowError;
    use datafusion_expr::Operator;

    use super::*;
    use crate::prelude::SessionContext;
    use datafusion_common::ScalarValue;
    use datafusion_physical_expr::expressions::Literal;
    use std::sync::Arc;

    fn build_table(
        a: (&str, &Vec<i32>),
        b: (&str, &Vec<i32>),
        c: (&str, &Vec<i32>),
    ) -> Arc<dyn ExecutionPlan> {
        let batch = build_table_i32(a, b, c);
        let schema = batch.schema();
        Arc::new(MemoryExec::try_new(&[vec![batch]], schema, None).unwrap())
    }

    fn join(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: JoinOn,
        join_type: &JoinType,
        null_equals_null: bool,
    ) -> Result<HashJoinExec> {
        HashJoinExec::try_new(
            left,
            right,
            on,
            None,
            join_type,
            PartitionMode::CollectLeft,
            &null_equals_null,
        )
    }

    fn join_with_filter(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: JoinOn,
        filter: JoinFilter,
        join_type: &JoinType,
        null_equals_null: bool,
    ) -> Result<HashJoinExec> {
        HashJoinExec::try_new(
            left,
            right,
            on,
            Some(filter),
            join_type,
            PartitionMode::CollectLeft,
            &null_equals_null,
        )
    }

    async fn join_collect(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: JoinOn,
        join_type: &JoinType,
        null_equals_null: bool,
        context: Arc<TaskContext>,
    ) -> Result<(Vec<String>, Vec<RecordBatch>)> {
        let join = join(left, right, on, join_type, null_equals_null)?;
        let columns_header = columns(&join.schema());

        let stream = join.execute(0, context)?;
        let batches = common::collect(stream).await?;

        Ok((columns_header, batches))
    }

    async fn partitioned_join_collect(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: JoinOn,
        join_type: &JoinType,
        null_equals_null: bool,
        context: Arc<TaskContext>,
    ) -> Result<(Vec<String>, Vec<RecordBatch>)> {
        let partition_count = 4;

        let (left_expr, right_expr) = on
            .iter()
            .map(|(l, r)| {
                (
                    Arc::new(l.clone()) as Arc<dyn PhysicalExpr>,
                    Arc::new(r.clone()) as Arc<dyn PhysicalExpr>,
                )
            })
            .unzip();

        let join = HashJoinExec::try_new(
            Arc::new(RepartitionExec::try_new(
                left,
                Partitioning::Hash(left_expr, partition_count),
            )?),
            Arc::new(RepartitionExec::try_new(
                right,
                Partitioning::Hash(right_expr, partition_count),
            )?),
            on,
            None,
            join_type,
            PartitionMode::Partitioned,
            &null_equals_null,
        )?;

        let columns = columns(&join.schema());

        let mut batches = vec![];
        for i in 0..partition_count {
            let stream = join.execute(i, context.clone())?;
            let more_batches = common::collect(stream).await?;
            batches.extend(
                more_batches
                    .into_iter()
                    .filter(|b| b.num_rows() > 0)
                    .collect::<Vec<_>>(),
            );
        }

        Ok((columns, batches))
    }

    #[tokio::test]
    async fn join_inner_one() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![4, 5, 5]), // this has a repetition
            ("c1", &vec![7, 8, 9]),
        );
        let right = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![4, 5, 6]),
            ("c2", &vec![70, 80, 90]),
        );

        let on = vec![(
            Column::new_with_schema("b1", &left.schema())?,
            Column::new_with_schema("b1", &right.schema())?,
        )];

        let (columns, batches) = join_collect(
            left.clone(),
            right.clone(),
            on.clone(),
            &JoinType::Inner,
            false,
            task_ctx,
        )
        .await?;

        assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b1", "c2"]);

        let expected = vec![
            "+----+----+----+----+----+----+",
            "| a1 | b1 | c1 | a2 | b1 | c2 |",
            "+----+----+----+----+----+----+",
            "| 1  | 4  | 7  | 10 | 4  | 70 |",
            "| 2  | 5  | 8  | 20 | 5  | 80 |",
            "| 3  | 5  | 9  | 20 | 5  | 80 |",
            "+----+----+----+----+----+----+",
        ];
        assert_batches_sorted_eq!(expected, &batches);

        Ok(())
    }

    #[tokio::test]
    async fn partitioned_join_inner_one() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![4, 5, 5]), // this has a repetition
            ("c1", &vec![7, 8, 9]),
        );
        let right = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![4, 5, 6]),
            ("c2", &vec![70, 80, 90]),
        );
        let on = vec![(
            Column::new_with_schema("b1", &left.schema())?,
            Column::new_with_schema("b1", &right.schema())?,
        )];

        let (columns, batches) = partitioned_join_collect(
            left.clone(),
            right.clone(),
            on.clone(),
            &JoinType::Inner,
            false,
            task_ctx,
        )
        .await?;

        assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b1", "c2"]);

        let expected = vec![
            "+----+----+----+----+----+----+",
            "| a1 | b1 | c1 | a2 | b1 | c2 |",
            "+----+----+----+----+----+----+",
            "| 1  | 4  | 7  | 10 | 4  | 70 |",
            "| 2  | 5  | 8  | 20 | 5  | 80 |",
            "| 3  | 5  | 9  | 20 | 5  | 80 |",
            "+----+----+----+----+----+----+",
        ];
        assert_batches_sorted_eq!(expected, &batches);

        Ok(())
    }

    #[tokio::test]
    async fn join_inner_one_no_shared_column_names() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![4, 5, 5]), // this has a repetition
            ("c1", &vec![7, 8, 9]),
        );
        let right = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b2", &vec![4, 5, 6]),
            ("c2", &vec![70, 80, 90]),
        );
        let on = vec![(
            Column::new_with_schema("b1", &left.schema())?,
            Column::new_with_schema("b2", &right.schema())?,
        )];

        let (columns, batches) =
            join_collect(left, right, on, &JoinType::Inner, false, task_ctx).await?;

        assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b2", "c2"]);

        let expected = vec![
            "+----+----+----+----+----+----+",
            "| a1 | b1 | c1 | a2 | b2 | c2 |",
            "+----+----+----+----+----+----+",
            "| 1  | 4  | 7  | 10 | 4  | 70 |",
            "| 2  | 5  | 8  | 20 | 5  | 80 |",
            "| 3  | 5  | 9  | 20 | 5  | 80 |",
            "+----+----+----+----+----+----+",
        ];

        assert_batches_sorted_eq!(expected, &batches);

        Ok(())
    }

    #[tokio::test]
    async fn join_inner_two() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let left = build_table(
            ("a1", &vec![1, 2, 2]),
            ("b2", &vec![1, 2, 2]),
            ("c1", &vec![7, 8, 9]),
        );
        let right = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b2", &vec![1, 2, 2]),
            ("c2", &vec![70, 80, 90]),
        );
        let on = vec![
            (
                Column::new_with_schema("a1", &left.schema())?,
                Column::new_with_schema("a1", &right.schema())?,
            ),
            (
                Column::new_with_schema("b2", &left.schema())?,
                Column::new_with_schema("b2", &right.schema())?,
            ),
        ];

        let (columns, batches) =
            join_collect(left, right, on, &JoinType::Inner, false, task_ctx).await?;

        assert_eq!(columns, vec!["a1", "b2", "c1", "a1", "b2", "c2"]);

        assert_eq!(batches.len(), 1);

        let expected = vec![
            "+----+----+----+----+----+----+",
            "| a1 | b2 | c1 | a1 | b2 | c2 |",
            "+----+----+----+----+----+----+",
            "| 1  | 1  | 7  | 1  | 1  | 70 |",
            "| 2  | 2  | 8  | 2  | 2  | 80 |",
            "| 2  | 2  | 9  | 2  | 2  | 80 |",
            "+----+----+----+----+----+----+",
        ];

        assert_batches_sorted_eq!(expected, &batches);

        Ok(())
    }

    /// Test where the left has 2 parts, the right with 1 part => 1 part
    #[tokio::test]
    async fn join_inner_one_two_parts_left() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let batch1 = build_table_i32(
            ("a1", &vec![1, 2]),
            ("b2", &vec![1, 2]),
            ("c1", &vec![7, 8]),
        );
        let batch2 =
            build_table_i32(("a1", &vec![2]), ("b2", &vec![2]), ("c1", &vec![9]));
        let schema = batch1.schema();
        let left = Arc::new(
            MemoryExec::try_new(&[vec![batch1], vec![batch2]], schema, None).unwrap(),
        );

        let right = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b2", &vec![1, 2, 2]),
            ("c2", &vec![70, 80, 90]),
        );
        let on = vec![
            (
                Column::new_with_schema("a1", &left.schema())?,
                Column::new_with_schema("a1", &right.schema())?,
            ),
            (
                Column::new_with_schema("b2", &left.schema())?,
                Column::new_with_schema("b2", &right.schema())?,
            ),
        ];

        let (columns, batches) =
            join_collect(left, right, on, &JoinType::Inner, false, task_ctx).await?;

        assert_eq!(columns, vec!["a1", "b2", "c1", "a1", "b2", "c2"]);

        assert_eq!(batches.len(), 1);

        let expected = vec![
            "+----+----+----+----+----+----+",
            "| a1 | b2 | c1 | a1 | b2 | c2 |",
            "+----+----+----+----+----+----+",
            "| 1  | 1  | 7  | 1  | 1  | 70 |",
            "| 2  | 2  | 8  | 2  | 2  | 80 |",
            "| 2  | 2  | 9  | 2  | 2  | 80 |",
            "+----+----+----+----+----+----+",
        ];

        assert_batches_sorted_eq!(expected, &batches);

        Ok(())
    }

    /// Test where the left has 1 part, the right has 2 parts => 2 parts
    #[tokio::test]
    async fn join_inner_one_two_parts_right() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![4, 5, 5]), // this has a repetition
            ("c1", &vec![7, 8, 9]),
        );

        let batch1 = build_table_i32(
            ("a2", &vec![10, 20]),
            ("b1", &vec![4, 6]),
            ("c2", &vec![70, 80]),
        );
        let batch2 =
            build_table_i32(("a2", &vec![30]), ("b1", &vec![5]), ("c2", &vec![90]));
        let schema = batch1.schema();
        let right = Arc::new(
            MemoryExec::try_new(&[vec![batch1], vec![batch2]], schema, None).unwrap(),
        );

        let on = vec![(
            Column::new_with_schema("b1", &left.schema())?,
            Column::new_with_schema("b1", &right.schema())?,
        )];

        let join = join(left, right, on, &JoinType::Inner, false)?;

        let columns = columns(&join.schema());
        assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b1", "c2"]);

        // first part
        let stream = join.execute(0, task_ctx.clone())?;
        let batches = common::collect(stream).await?;
        assert_eq!(batches.len(), 1);

        let expected = vec![
            "+----+----+----+----+----+----+",
            "| a1 | b1 | c1 | a2 | b1 | c2 |",
            "+----+----+----+----+----+----+",
            "| 1  | 4  | 7  | 10 | 4  | 70 |",
            "+----+----+----+----+----+----+",
        ];
        assert_batches_sorted_eq!(expected, &batches);

        // second part
        let stream = join.execute(1, task_ctx.clone())?;
        let batches = common::collect(stream).await?;
        assert_eq!(batches.len(), 1);
        let expected = vec![
            "+----+----+----+----+----+----+",
            "| a1 | b1 | c1 | a2 | b1 | c2 |",
            "+----+----+----+----+----+----+",
            "| 2  | 5  | 8  | 30 | 5  | 90 |",
            "| 3  | 5  | 9  | 30 | 5  | 90 |",
            "+----+----+----+----+----+----+",
        ];

        assert_batches_sorted_eq!(expected, &batches);

        Ok(())
    }

    fn build_table_two_batches(
        a: (&str, &Vec<i32>),
        b: (&str, &Vec<i32>),
        c: (&str, &Vec<i32>),
    ) -> Arc<dyn ExecutionPlan> {
        let batch = build_table_i32(a, b, c);
        let schema = batch.schema();
        Arc::new(
            MemoryExec::try_new(&[vec![batch.clone(), batch]], schema, None).unwrap(),
        )
    }

    #[tokio::test]
    async fn join_left_multi_batch() {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![4, 5, 7]), // 7 does not exist on the right
            ("c1", &vec![7, 8, 9]),
        );
        let right = build_table_two_batches(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![4, 5, 6]),
            ("c2", &vec![70, 80, 90]),
        );
        let on = vec![(
            Column::new_with_schema("b1", &left.schema()).unwrap(),
            Column::new_with_schema("b1", &right.schema()).unwrap(),
        )];

        let join = join(left, right, on, &JoinType::Left, false).unwrap();

        let columns = columns(&join.schema());
        assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b1", "c2"]);

        let stream = join.execute(0, task_ctx).unwrap();
        let batches = common::collect(stream).await.unwrap();

        let expected = vec![
            "+----+----+----+----+----+----+",
            "| a1 | b1 | c1 | a2 | b1 | c2 |",
            "+----+----+----+----+----+----+",
            "| 1  | 4  | 7  | 10 | 4  | 70 |",
            "| 1  | 4  | 7  | 10 | 4  | 70 |",
            "| 2  | 5  | 8  | 20 | 5  | 80 |",
            "| 2  | 5  | 8  | 20 | 5  | 80 |",
            "| 3  | 7  | 9  |    |    |    |",
            "+----+----+----+----+----+----+",
        ];

        assert_batches_sorted_eq!(expected, &batches);
    }

    #[tokio::test]
    async fn join_full_multi_batch() {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![4, 5, 7]), // 7 does not exist on the right
            ("c1", &vec![7, 8, 9]),
        );
        // create two identical batches for the right side
        let right = build_table_two_batches(
            ("a2", &vec![10, 20, 30]),
            ("b2", &vec![4, 5, 6]),
            ("c2", &vec![70, 80, 90]),
        );
        let on = vec![(
            Column::new_with_schema("b1", &left.schema()).unwrap(),
            Column::new_with_schema("b2", &right.schema()).unwrap(),
        )];

        let join = join(left, right, on, &JoinType::Full, false).unwrap();

        let columns = columns(&join.schema());
        assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b2", "c2"]);

        let stream = join.execute(0, task_ctx).unwrap();
        let batches = common::collect(stream).await.unwrap();

        let expected = vec![
            "+----+----+----+----+----+----+",
            "| a1 | b1 | c1 | a2 | b2 | c2 |",
            "+----+----+----+----+----+----+",
            "|    |    |    | 30 | 6  | 90 |",
            "|    |    |    | 30 | 6  | 90 |",
            "| 1  | 4  | 7  | 10 | 4  | 70 |",
            "| 1  | 4  | 7  | 10 | 4  | 70 |",
            "| 2  | 5  | 8  | 20 | 5  | 80 |",
            "| 2  | 5  | 8  | 20 | 5  | 80 |",
            "| 3  | 7  | 9  |    |    |    |",
            "+----+----+----+----+----+----+",
        ];

        assert_batches_sorted_eq!(expected, &batches);
    }

    #[tokio::test]
    async fn join_left_empty_right() {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![4, 5, 7]),
            ("c1", &vec![7, 8, 9]),
        );
        let right = build_table_i32(("a2", &vec![]), ("b1", &vec![]), ("c2", &vec![]));
        let on = vec![(
            Column::new_with_schema("b1", &left.schema()).unwrap(),
            Column::new_with_schema("b1", &right.schema()).unwrap(),
        )];
        let schema = right.schema();
        let right = Arc::new(MemoryExec::try_new(&[vec![right]], schema, None).unwrap());
        let join = join(left, right, on, &JoinType::Left, false).unwrap();

        let columns = columns(&join.schema());
        assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b1", "c2"]);

        let stream = join.execute(0, task_ctx).unwrap();
        let batches = common::collect(stream).await.unwrap();

        let expected = vec![
            "+----+----+----+----+----+----+",
            "| a1 | b1 | c1 | a2 | b1 | c2 |",
            "+----+----+----+----+----+----+",
            "| 1  | 4  | 7  |    |    |    |",
            "| 2  | 5  | 8  |    |    |    |",
            "| 3  | 7  | 9  |    |    |    |",
            "+----+----+----+----+----+----+",
        ];

        assert_batches_sorted_eq!(expected, &batches);
    }

    #[tokio::test]
    async fn join_full_empty_right() {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![4, 5, 7]),
            ("c1", &vec![7, 8, 9]),
        );
        let right = build_table_i32(("a2", &vec![]), ("b2", &vec![]), ("c2", &vec![]));
        let on = vec![(
            Column::new_with_schema("b1", &left.schema()).unwrap(),
            Column::new_with_schema("b2", &right.schema()).unwrap(),
        )];
        let schema = right.schema();
        let right = Arc::new(MemoryExec::try_new(&[vec![right]], schema, None).unwrap());
        let join = join(left, right, on, &JoinType::Full, false).unwrap();

        let columns = columns(&join.schema());
        assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b2", "c2"]);

        let stream = join.execute(0, task_ctx).unwrap();
        let batches = common::collect(stream).await.unwrap();

        let expected = vec![
            "+----+----+----+----+----+----+",
            "| a1 | b1 | c1 | a2 | b2 | c2 |",
            "+----+----+----+----+----+----+",
            "| 1  | 4  | 7  |    |    |    |",
            "| 2  | 5  | 8  |    |    |    |",
            "| 3  | 7  | 9  |    |    |    |",
            "+----+----+----+----+----+----+",
        ];

        assert_batches_sorted_eq!(expected, &batches);
    }

    #[tokio::test]
    async fn join_left_one() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![4, 5, 7]), // 7 does not exist on the right
            ("c1", &vec![7, 8, 9]),
        );
        let right = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![4, 5, 6]),
            ("c2", &vec![70, 80, 90]),
        );
        let on = vec![(
            Column::new_with_schema("b1", &left.schema())?,
            Column::new_with_schema("b1", &right.schema())?,
        )];

        let (columns, batches) = join_collect(
            left.clone(),
            right.clone(),
            on.clone(),
            &JoinType::Left,
            false,
            task_ctx,
        )
        .await?;
        assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b1", "c2"]);

        let expected = vec![
            "+----+----+----+----+----+----+",
            "| a1 | b1 | c1 | a2 | b1 | c2 |",
            "+----+----+----+----+----+----+",
            "| 1  | 4  | 7  | 10 | 4  | 70 |",
            "| 2  | 5  | 8  | 20 | 5  | 80 |",
            "| 3  | 7  | 9  |    |    |    |",
            "+----+----+----+----+----+----+",
        ];
        assert_batches_sorted_eq!(expected, &batches);

        Ok(())
    }

    #[tokio::test]
    async fn partitioned_join_left_one() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![4, 5, 7]), // 7 does not exist on the right
            ("c1", &vec![7, 8, 9]),
        );
        let right = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![4, 5, 6]),
            ("c2", &vec![70, 80, 90]),
        );
        let on = vec![(
            Column::new_with_schema("b1", &left.schema())?,
            Column::new_with_schema("b1", &right.schema())?,
        )];

        let (columns, batches) = partitioned_join_collect(
            left.clone(),
            right.clone(),
            on.clone(),
            &JoinType::Left,
            false,
            task_ctx,
        )
        .await?;
        assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b1", "c2"]);

        let expected = vec![
            "+----+----+----+----+----+----+",
            "| a1 | b1 | c1 | a2 | b1 | c2 |",
            "+----+----+----+----+----+----+",
            "| 1  | 4  | 7  | 10 | 4  | 70 |",
            "| 2  | 5  | 8  | 20 | 5  | 80 |",
            "| 3  | 7  | 9  |    |    |    |",
            "+----+----+----+----+----+----+",
        ];
        assert_batches_sorted_eq!(expected, &batches);

        Ok(())
    }

    fn build_semi_anti_left_table() -> Arc<dyn ExecutionPlan> {
        // just two line match
        // b1 = 10
        build_table(
            ("a1", &vec![1, 3, 5, 7, 9, 11, 13]),
            ("b1", &vec![1, 3, 5, 7, 8, 8, 10]),
            ("c1", &vec![10, 30, 50, 70, 90, 110, 130]),
        )
    }

    fn build_semi_anti_right_table() -> Arc<dyn ExecutionPlan> {
        // just two line match
        // b2 = 10
        build_table(
            ("a2", &vec![8, 12, 6, 2, 10, 4]),
            ("b2", &vec![8, 10, 6, 2, 10, 4]),
            ("c2", &vec![20, 40, 60, 80, 100, 120]),
        )
    }

    #[tokio::test]
    async fn join_left_semi() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let left = build_semi_anti_left_table();
        let right = build_semi_anti_right_table();
        // left_table left semi join right_table on left_table.b1 = right_table.b2
        let on = vec![(
            Column::new_with_schema("b1", &left.schema())?,
            Column::new_with_schema("b2", &right.schema())?,
        )];

        let join = join(left, right, on, &JoinType::LeftSemi, false)?;

        let columns = columns(&join.schema());
        assert_eq!(columns, vec!["a1", "b1", "c1"]);

        let stream = join.execute(0, task_ctx)?;
        let batches = common::collect(stream).await?;

        // ignore the order
        let expected = vec![
            "+----+----+-----+",
            "| a1 | b1 | c1  |",
            "+----+----+-----+",
            "| 11 | 8  | 110 |",
            "| 13 | 10 | 130 |",
            "| 9  | 8  | 90  |",
            "+----+----+-----+",
        ];
        assert_batches_sorted_eq!(expected, &batches);

        Ok(())
    }

    #[tokio::test]
    async fn join_left_semi_with_filter() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let left = build_semi_anti_left_table();
        let right = build_semi_anti_right_table();

        // left_table left semi join right_table on left_table.b1 = right_table.b2 and right_table.a2 != 10
        let on = vec![(
            Column::new_with_schema("b1", &left.schema())?,
            Column::new_with_schema("b2", &right.schema())?,
        )];

        let column_indices = vec![ColumnIndex {
            index: 0,
            side: JoinSide::Right,
        }];
        let intermediate_schema =
            Schema::new(vec![Field::new("x", DataType::Int32, true)]);

        let filter_expression = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("x", 0)),
            Operator::NotEq,
            Arc::new(Literal::new(ScalarValue::Int32(Some(10)))),
        )) as Arc<dyn PhysicalExpr>;

        let filter = JoinFilter::new(
            filter_expression,
            column_indices.clone(),
            intermediate_schema.clone(),
        );

        let join = join_with_filter(
            left.clone(),
            right.clone(),
            on.clone(),
            filter,
            &JoinType::LeftSemi,
            false,
        )?;

        let columns_header = columns(&join.schema());
        assert_eq!(columns_header.clone(), vec!["a1", "b1", "c1"]);

        let stream = join.execute(0, task_ctx.clone())?;
        let batches = common::collect(stream).await?;

        let expected = vec![
            "+----+----+-----+",
            "| a1 | b1 | c1  |",
            "+----+----+-----+",
            "| 11 | 8  | 110 |",
            "| 13 | 10 | 130 |",
            "| 9  | 8  | 90  |",
            "+----+----+-----+",
        ];
        assert_batches_sorted_eq!(expected, &batches);

        // left_table left semi join right_table on left_table.b1 = right_table.b2 and right_table.a2 > 10
        let filter_expression = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("x", 0)),
            Operator::Gt,
            Arc::new(Literal::new(ScalarValue::Int32(Some(10)))),
        )) as Arc<dyn PhysicalExpr>;
        let filter =
            JoinFilter::new(filter_expression, column_indices, intermediate_schema);

        let join = join_with_filter(left, right, on, filter, &JoinType::LeftSemi, false)?;

        let columns_header = columns(&join.schema());
        assert_eq!(columns_header, vec!["a1", "b1", "c1"]);

        let stream = join.execute(0, task_ctx)?;
        let batches = common::collect(stream).await?;

        let expected = vec![
            "+----+----+-----+",
            "| a1 | b1 | c1  |",
            "+----+----+-----+",
            "| 13 | 10 | 130 |",
            "+----+----+-----+",
        ];
        assert_batches_sorted_eq!(expected, &batches);

        Ok(())
    }

    #[tokio::test]
    async fn join_right_semi() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let left = build_semi_anti_left_table();
        let right = build_semi_anti_right_table();

        // left_table right semi join right_table on left_table.b1 = right_table.b2
        let on = vec![(
            Column::new_with_schema("b1", &left.schema())?,
            Column::new_with_schema("b2", &right.schema())?,
        )];

        let join = join(left, right, on, &JoinType::RightSemi, false)?;

        let columns = columns(&join.schema());
        assert_eq!(columns, vec!["a2", "b2", "c2"]);

        let stream = join.execute(0, task_ctx)?;
        let batches = common::collect(stream).await?;

        let expected = vec![
            "+----+----+-----+",
            "| a2 | b2 | c2  |",
            "+----+----+-----+",
            "| 10 | 10 | 100 |",
            "| 12 | 10 | 40  |",
            "| 8  | 8  | 20  |",
            "+----+----+-----+",
        ];
        assert_batches_sorted_eq!(expected, &batches);

        Ok(())
    }

    #[tokio::test]
    async fn join_right_semi_with_filter() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let left = build_semi_anti_left_table();
        let right = build_semi_anti_right_table();

        // left_table right semi join right_table on left_table.b1 = right_table.b2 on left_table.a1!=9
        let on = vec![(
            Column::new_with_schema("b1", &left.schema())?,
            Column::new_with_schema("b2", &right.schema())?,
        )];

        let column_indices = vec![ColumnIndex {
            index: 0,
            side: JoinSide::Left,
        }];
        let intermediate_schema =
            Schema::new(vec![Field::new("x", DataType::Int32, true)]);

        let filter_expression = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("x", 0)),
            Operator::NotEq,
            Arc::new(Literal::new(ScalarValue::Int32(Some(9)))),
        )) as Arc<dyn PhysicalExpr>;

        let filter = JoinFilter::new(
            filter_expression,
            column_indices.clone(),
            intermediate_schema.clone(),
        );

        let join = join_with_filter(
            left.clone(),
            right.clone(),
            on.clone(),
            filter,
            &JoinType::RightSemi,
            false,
        )?;

        let columns = columns(&join.schema());
        assert_eq!(columns, vec!["a2", "b2", "c2"]);

        let stream = join.execute(0, task_ctx.clone())?;
        let batches = common::collect(stream).await?;

        let expected = vec![
            "+----+----+-----+",
            "| a2 | b2 | c2  |",
            "+----+----+-----+",
            "| 10 | 10 | 100 |",
            "| 12 | 10 | 40  |",
            "| 8  | 8  | 20  |",
            "+----+----+-----+",
        ];
        assert_batches_sorted_eq!(expected, &batches);

        // left_table right semi join right_table on left_table.b1 = right_table.b2 on left_table.a1!=9
        let filter_expression = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("x", 0)),
            Operator::Gt,
            Arc::new(Literal::new(ScalarValue::Int32(Some(11)))),
        )) as Arc<dyn PhysicalExpr>;

        let filter =
            JoinFilter::new(filter_expression, column_indices, intermediate_schema);

        let join =
            join_with_filter(left, right, on, filter, &JoinType::RightSemi, false)?;
        let stream = join.execute(0, task_ctx)?;
        let batches = common::collect(stream).await?;

        let expected = vec![
            "+----+----+-----+",
            "| a2 | b2 | c2  |",
            "+----+----+-----+",
            "| 10 | 10 | 100 |",
            "| 12 | 10 | 40  |",
            "+----+----+-----+",
        ];
        assert_batches_sorted_eq!(expected, &batches);

        Ok(())
    }

    #[tokio::test]
    async fn join_left_anti() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let left = build_semi_anti_left_table();
        let right = build_semi_anti_right_table();
        // left_table left anti join right_table on left_table.b1 = right_table.b2
        let on = vec![(
            Column::new_with_schema("b1", &left.schema())?,
            Column::new_with_schema("b2", &right.schema())?,
        )];

        let join = join(left, right, on, &JoinType::LeftAnti, false)?;

        let columns = columns(&join.schema());
        assert_eq!(columns, vec!["a1", "b1", "c1"]);

        let stream = join.execute(0, task_ctx)?;
        let batches = common::collect(stream).await?;

        let expected = vec![
            "+----+----+----+",
            "| a1 | b1 | c1 |",
            "+----+----+----+",
            "| 1  | 1  | 10 |",
            "| 3  | 3  | 30 |",
            "| 5  | 5  | 50 |",
            "| 7  | 7  | 70 |",
            "+----+----+----+",
        ];
        assert_batches_sorted_eq!(expected, &batches);
        Ok(())
    }

    #[tokio::test]
    async fn join_left_anti_with_filter() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let left = build_semi_anti_left_table();
        let right = build_semi_anti_right_table();
        // left_table left anti join right_table on left_table.b1 = right_table.b2 and right_table.a2!=8
        let on = vec![(
            Column::new_with_schema("b1", &left.schema())?,
            Column::new_with_schema("b2", &right.schema())?,
        )];

        let column_indices = vec![ColumnIndex {
            index: 0,
            side: JoinSide::Right,
        }];
        let intermediate_schema =
            Schema::new(vec![Field::new("x", DataType::Int32, true)]);
        let filter_expression = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("x", 0)),
            Operator::NotEq,
            Arc::new(Literal::new(ScalarValue::Int32(Some(8)))),
        )) as Arc<dyn PhysicalExpr>;

        let filter = JoinFilter::new(
            filter_expression,
            column_indices.clone(),
            intermediate_schema.clone(),
        );

        let join = join_with_filter(
            left.clone(),
            right.clone(),
            on.clone(),
            filter,
            &JoinType::LeftAnti,
            false,
        )?;

        let columns_header = columns(&join.schema());
        assert_eq!(columns_header, vec!["a1", "b1", "c1"]);

        let stream = join.execute(0, task_ctx.clone())?;
        let batches = common::collect(stream).await?;

        let expected = vec![
            "+----+----+-----+",
            "| a1 | b1 | c1  |",
            "+----+----+-----+",
            "| 1  | 1  | 10  |",
            "| 11 | 8  | 110 |",
            "| 3  | 3  | 30  |",
            "| 5  | 5  | 50  |",
            "| 7  | 7  | 70  |",
            "| 9  | 8  | 90  |",
            "+----+----+-----+",
        ];
        assert_batches_sorted_eq!(expected, &batches);

        // left_table left anti join right_table on left_table.b1 = right_table.b2 and right_table.a2 != 13
        let filter_expression = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("x", 0)),
            Operator::NotEq,
            Arc::new(Literal::new(ScalarValue::Int32(Some(8)))),
        )) as Arc<dyn PhysicalExpr>;

        let filter =
            JoinFilter::new(filter_expression, column_indices, intermediate_schema);

        let join = join_with_filter(left, right, on, filter, &JoinType::LeftAnti, false)?;

        let columns_header = columns(&join.schema());
        assert_eq!(columns_header, vec!["a1", "b1", "c1"]);

        let stream = join.execute(0, task_ctx)?;
        let batches = common::collect(stream).await?;

        let expected = vec![
            "+----+----+-----+",
            "| a1 | b1 | c1  |",
            "+----+----+-----+",
            "| 1  | 1  | 10  |",
            "| 11 | 8  | 110 |",
            "| 3  | 3  | 30  |",
            "| 5  | 5  | 50  |",
            "| 7  | 7  | 70  |",
            "| 9  | 8  | 90  |",
            "+----+----+-----+",
        ];
        assert_batches_sorted_eq!(expected, &batches);

        Ok(())
    }

    #[tokio::test]
    async fn join_right_anti() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let left = build_semi_anti_left_table();
        let right = build_semi_anti_right_table();
        let on = vec![(
            Column::new_with_schema("b1", &left.schema())?,
            Column::new_with_schema("b2", &right.schema())?,
        )];

        let join = join(left, right, on, &JoinType::RightAnti, false)?;

        let columns = columns(&join.schema());
        assert_eq!(columns, vec!["a2", "b2", "c2"]);

        let stream = join.execute(0, task_ctx)?;
        let batches = common::collect(stream).await?;

        let expected = vec![
            "+----+----+-----+",
            "| a2 | b2 | c2  |",
            "+----+----+-----+",
            "| 2  | 2  | 80  |",
            "| 4  | 4  | 120 |",
            "| 6  | 6  | 60  |",
            "+----+----+-----+",
        ];
        assert_batches_sorted_eq!(expected, &batches);
        Ok(())
    }

    #[tokio::test]
    async fn join_right_anti_with_filter() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let left = build_semi_anti_left_table();
        let right = build_semi_anti_right_table();
        // left_table right anti join right_table on left_table.b1 = right_table.b2 and left_table.a1!=13
        let on = vec![(
            Column::new_with_schema("b1", &left.schema())?,
            Column::new_with_schema("b2", &right.schema())?,
        )];

        let column_indices = vec![ColumnIndex {
            index: 0,
            side: JoinSide::Left,
        }];
        let intermediate_schema =
            Schema::new(vec![Field::new("x", DataType::Int32, true)]);

        let filter_expression = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("x", 0)),
            Operator::NotEq,
            Arc::new(Literal::new(ScalarValue::Int32(Some(13)))),
        )) as Arc<dyn PhysicalExpr>;

        let filter = JoinFilter::new(
            filter_expression,
            column_indices,
            intermediate_schema.clone(),
        );

        let join = join_with_filter(
            left.clone(),
            right.clone(),
            on.clone(),
            filter,
            &JoinType::RightAnti,
            false,
        )?;

        let columns_header = columns(&join.schema());
        assert_eq!(columns_header, vec!["a2", "b2", "c2"]);

        let stream = join.execute(0, task_ctx.clone())?;
        let batches = common::collect(stream).await?;

        let expected = vec![
            "+----+----+-----+",
            "| a2 | b2 | c2  |",
            "+----+----+-----+",
            "| 10 | 10 | 100 |",
            "| 12 | 10 | 40  |",
            "| 2  | 2  | 80  |",
            "| 4  | 4  | 120 |",
            "| 6  | 6  | 60  |",
            "+----+----+-----+",
        ];
        assert_batches_sorted_eq!(expected, &batches);

        // left_table right anti join right_table on left_table.b1 = right_table.b2 and right_table.b2!=8
        let column_indices = vec![ColumnIndex {
            index: 1,
            side: JoinSide::Right,
        }];
        let filter_expression = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("x", 0)),
            Operator::NotEq,
            Arc::new(Literal::new(ScalarValue::Int32(Some(8)))),
        )) as Arc<dyn PhysicalExpr>;

        let filter =
            JoinFilter::new(filter_expression, column_indices, intermediate_schema);

        let join =
            join_with_filter(left, right, on, filter, &JoinType::RightAnti, false)?;

        let columns_header = columns(&join.schema());
        assert_eq!(columns_header, vec!["a2", "b2", "c2"]);

        let stream = join.execute(0, task_ctx)?;
        let batches = common::collect(stream).await?;

        let expected = vec![
            "+----+----+-----+",
            "| a2 | b2 | c2  |",
            "+----+----+-----+",
            "| 2  | 2  | 80  |",
            "| 4  | 4  | 120 |",
            "| 6  | 6  | 60  |",
            "| 8  | 8  | 20  |",
            "+----+----+-----+",
        ];
        assert_batches_sorted_eq!(expected, &batches);

        Ok(())
    }

    #[tokio::test]
    async fn join_right_one() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![4, 5, 7]),
            ("c1", &vec![7, 8, 9]),
        );
        let right = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![4, 5, 6]), // 6 does not exist on the left
            ("c2", &vec![70, 80, 90]),
        );
        let on = vec![(
            Column::new_with_schema("b1", &left.schema())?,
            Column::new_with_schema("b1", &right.schema())?,
        )];

        let (columns, batches) =
            join_collect(left, right, on, &JoinType::Right, false, task_ctx).await?;

        assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b1", "c2"]);

        let expected = vec![
            "+----+----+----+----+----+----+",
            "| a1 | b1 | c1 | a2 | b1 | c2 |",
            "+----+----+----+----+----+----+",
            "|    |    |    | 30 | 6  | 90 |",
            "| 1  | 4  | 7  | 10 | 4  | 70 |",
            "| 2  | 5  | 8  | 20 | 5  | 80 |",
            "+----+----+----+----+----+----+",
        ];

        assert_batches_sorted_eq!(expected, &batches);

        Ok(())
    }

    #[tokio::test]
    async fn partitioned_join_right_one() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![4, 5, 7]),
            ("c1", &vec![7, 8, 9]),
        );
        let right = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![4, 5, 6]), // 6 does not exist on the left
            ("c2", &vec![70, 80, 90]),
        );
        let on = vec![(
            Column::new_with_schema("b1", &left.schema())?,
            Column::new_with_schema("b1", &right.schema())?,
        )];

        let (columns, batches) =
            partitioned_join_collect(left, right, on, &JoinType::Right, false, task_ctx)
                .await?;

        assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b1", "c2"]);

        let expected = vec![
            "+----+----+----+----+----+----+",
            "| a1 | b1 | c1 | a2 | b1 | c2 |",
            "+----+----+----+----+----+----+",
            "|    |    |    | 30 | 6  | 90 |",
            "| 1  | 4  | 7  | 10 | 4  | 70 |",
            "| 2  | 5  | 8  | 20 | 5  | 80 |",
            "+----+----+----+----+----+----+",
        ];

        assert_batches_sorted_eq!(expected, &batches);

        Ok(())
    }

    #[tokio::test]
    async fn join_full_one() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![4, 5, 7]), // 7 does not exist on the right
            ("c1", &vec![7, 8, 9]),
        );
        let right = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b2", &vec![4, 5, 6]),
            ("c2", &vec![70, 80, 90]),
        );
        let on = vec![(
            Column::new_with_schema("b1", &left.schema()).unwrap(),
            Column::new_with_schema("b2", &right.schema()).unwrap(),
        )];

        let join = join(left, right, on, &JoinType::Full, false)?;

        let columns = columns(&join.schema());
        assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b2", "c2"]);

        let stream = join.execute(0, task_ctx)?;
        let batches = common::collect(stream).await?;

        let expected = vec![
            "+----+----+----+----+----+----+",
            "| a1 | b1 | c1 | a2 | b2 | c2 |",
            "+----+----+----+----+----+----+",
            "|    |    |    | 30 | 6  | 90 |",
            "| 1  | 4  | 7  | 10 | 4  | 70 |",
            "| 2  | 5  | 8  | 20 | 5  | 80 |",
            "| 3  | 7  | 9  |    |    |    |",
            "+----+----+----+----+----+----+",
        ];
        assert_batches_sorted_eq!(expected, &batches);

        Ok(())
    }

    #[test]
    fn join_with_hash_collision() -> Result<()> {
        let mut hashmap_left = RawTable::with_capacity(2);
        let left = build_table_i32(
            ("a", &vec![10, 20]),
            ("x", &vec![100, 200]),
            ("y", &vec![200, 300]),
        );

        let random_state = RandomState::with_seeds(0, 0, 0, 0);
        let hashes_buff = &mut vec![0; left.num_rows()];
        let hashes =
            create_hashes(&[left.columns()[0].clone()], &random_state, hashes_buff)?;

        // Create hash collisions (same hashes)
        hashmap_left.insert(hashes[0], (hashes[0], smallvec![0, 1]), |(h, _)| *h);
        hashmap_left.insert(hashes[1], (hashes[1], smallvec![0, 1]), |(h, _)| *h);

        let right = build_table_i32(
            ("a", &vec![10, 20]),
            ("b", &vec![0, 0]),
            ("c", &vec![30, 40]),
        );

        let left_data = (JoinHashMap(hashmap_left), left);
        let (l, r) = build_equal_condition_join_indices(
            &left_data,
            &right,
            &[Column::new("a", 0)],
            &[Column::new("a", 0)],
            &random_state,
            &false,
        )?;

        let mut left_ids = UInt64Builder::with_capacity(0);
        left_ids.append_value(0);
        left_ids.append_value(1);

        let mut right_ids = UInt32Builder::with_capacity(0);
        right_ids.append_value(0);
        right_ids.append_value(1);

        assert_eq!(left_ids.finish(), l);

        assert_eq!(right_ids.finish(), r);

        Ok(())
    }

    #[tokio::test]
    async fn join_with_duplicated_column_names() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let left = build_table(
            ("a", &vec![1, 2, 3]),
            ("b", &vec![4, 5, 7]),
            ("c", &vec![7, 8, 9]),
        );
        let right = build_table(
            ("a", &vec![10, 20, 30]),
            ("b", &vec![1, 2, 7]),
            ("c", &vec![70, 80, 90]),
        );
        let on = vec![(
            // join on a=b so there are duplicate column names on unjoined columns
            Column::new_with_schema("a", &left.schema()).unwrap(),
            Column::new_with_schema("b", &right.schema()).unwrap(),
        )];

        let join = join(left, right, on, &JoinType::Inner, false)?;

        let columns = columns(&join.schema());
        assert_eq!(columns, vec!["a", "b", "c", "a", "b", "c"]);

        let stream = join.execute(0, task_ctx)?;
        let batches = common::collect(stream).await?;

        let expected = vec![
            "+---+---+---+----+---+----+",
            "| a | b | c | a  | b | c  |",
            "+---+---+---+----+---+----+",
            "| 1 | 4 | 7 | 10 | 1 | 70 |",
            "| 2 | 5 | 8 | 20 | 2 | 80 |",
            "+---+---+---+----+---+----+",
        ];
        assert_batches_sorted_eq!(expected, &batches);

        Ok(())
    }

    fn prepare_join_filter() -> JoinFilter {
        let column_indices = vec![
            ColumnIndex {
                index: 2,
                side: JoinSide::Left,
            },
            ColumnIndex {
                index: 2,
                side: JoinSide::Right,
            },
        ];
        let intermediate_schema = Schema::new(vec![
            Field::new("c", DataType::Int32, true),
            Field::new("c", DataType::Int32, true),
        ]);
        let filter_expression = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("c", 0)),
            Operator::Gt,
            Arc::new(Column::new("c", 1)),
        )) as Arc<dyn PhysicalExpr>;

        JoinFilter::new(filter_expression, column_indices, intermediate_schema)
    }

    #[tokio::test]
    async fn join_inner_with_filter() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let left = build_table(
            ("a", &vec![0, 1, 2, 2]),
            ("b", &vec![4, 5, 7, 8]),
            ("c", &vec![7, 8, 9, 1]),
        );
        let right = build_table(
            ("a", &vec![10, 20, 30, 40]),
            ("b", &vec![2, 2, 3, 4]),
            ("c", &vec![7, 5, 6, 4]),
        );
        let on = vec![(
            Column::new_with_schema("a", &left.schema()).unwrap(),
            Column::new_with_schema("b", &right.schema()).unwrap(),
        )];
        let filter = prepare_join_filter();

        let join = join_with_filter(left, right, on, filter, &JoinType::Inner, false)?;

        let columns = columns(&join.schema());
        assert_eq!(columns, vec!["a", "b", "c", "a", "b", "c"]);

        let stream = join.execute(0, task_ctx)?;
        let batches = common::collect(stream).await?;

        let expected = vec![
            "+---+---+---+----+---+---+",
            "| a | b | c | a  | b | c |",
            "+---+---+---+----+---+---+",
            "| 2 | 7 | 9 | 10 | 2 | 7 |",
            "| 2 | 7 | 9 | 20 | 2 | 5 |",
            "+---+---+---+----+---+---+",
        ];
        assert_batches_sorted_eq!(expected, &batches);

        Ok(())
    }

    #[tokio::test]
    async fn join_left_with_filter() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let left = build_table(
            ("a", &vec![0, 1, 2, 2]),
            ("b", &vec![4, 5, 7, 8]),
            ("c", &vec![7, 8, 9, 1]),
        );
        let right = build_table(
            ("a", &vec![10, 20, 30, 40]),
            ("b", &vec![2, 2, 3, 4]),
            ("c", &vec![7, 5, 6, 4]),
        );
        let on = vec![(
            Column::new_with_schema("a", &left.schema()).unwrap(),
            Column::new_with_schema("b", &right.schema()).unwrap(),
        )];
        let filter = prepare_join_filter();

        let join = join_with_filter(left, right, on, filter, &JoinType::Left, false)?;

        let columns = columns(&join.schema());
        assert_eq!(columns, vec!["a", "b", "c", "a", "b", "c"]);

        let stream = join.execute(0, task_ctx)?;
        let batches = common::collect(stream).await?;

        let expected = vec![
            "+---+---+---+----+---+---+",
            "| a | b | c | a  | b | c |",
            "+---+---+---+----+---+---+",
            "| 0 | 4 | 7 |    |   |   |",
            "| 1 | 5 | 8 |    |   |   |",
            "| 2 | 7 | 9 | 10 | 2 | 7 |",
            "| 2 | 7 | 9 | 20 | 2 | 5 |",
            "| 2 | 8 | 1 |    |   |   |",
            "+---+---+---+----+---+---+",
        ];
        assert_batches_sorted_eq!(expected, &batches);

        Ok(())
    }

    #[tokio::test]
    async fn join_right_with_filter() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let left = build_table(
            ("a", &vec![0, 1, 2, 2]),
            ("b", &vec![4, 5, 7, 8]),
            ("c", &vec![7, 8, 9, 1]),
        );
        let right = build_table(
            ("a", &vec![10, 20, 30, 40]),
            ("b", &vec![2, 2, 3, 4]),
            ("c", &vec![7, 5, 6, 4]),
        );
        let on = vec![(
            Column::new_with_schema("a", &left.schema()).unwrap(),
            Column::new_with_schema("b", &right.schema()).unwrap(),
        )];
        let filter = prepare_join_filter();

        let join = join_with_filter(left, right, on, filter, &JoinType::Right, false)?;

        let columns = columns(&join.schema());
        assert_eq!(columns, vec!["a", "b", "c", "a", "b", "c"]);

        let stream = join.execute(0, task_ctx)?;
        let batches = common::collect(stream).await?;

        let expected = vec![
            "+---+---+---+----+---+---+",
            "| a | b | c | a  | b | c |",
            "+---+---+---+----+---+---+",
            "|   |   |   | 30 | 3 | 6 |",
            "|   |   |   | 40 | 4 | 4 |",
            "| 2 | 7 | 9 | 10 | 2 | 7 |",
            "| 2 | 7 | 9 | 20 | 2 | 5 |",
            "+---+---+---+----+---+---+",
        ];
        assert_batches_sorted_eq!(expected, &batches);

        Ok(())
    }

    #[tokio::test]
    async fn join_full_with_filter() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let left = build_table(
            ("a", &vec![0, 1, 2, 2]),
            ("b", &vec![4, 5, 7, 8]),
            ("c", &vec![7, 8, 9, 1]),
        );
        let right = build_table(
            ("a", &vec![10, 20, 30, 40]),
            ("b", &vec![2, 2, 3, 4]),
            ("c", &vec![7, 5, 6, 4]),
        );
        let on = vec![(
            Column::new_with_schema("a", &left.schema()).unwrap(),
            Column::new_with_schema("b", &right.schema()).unwrap(),
        )];
        let filter = prepare_join_filter();

        let join = join_with_filter(left, right, on, filter, &JoinType::Full, false)?;

        let columns = columns(&join.schema());
        assert_eq!(columns, vec!["a", "b", "c", "a", "b", "c"]);

        let stream = join.execute(0, task_ctx)?;
        let batches = common::collect(stream).await?;

        let expected = vec![
            "+---+---+---+----+---+---+",
            "| a | b | c | a  | b | c |",
            "+---+---+---+----+---+---+",
            "|   |   |   | 30 | 3 | 6 |",
            "|   |   |   | 40 | 4 | 4 |",
            "| 2 | 7 | 9 | 10 | 2 | 7 |",
            "| 2 | 7 | 9 | 20 | 2 | 5 |",
            "| 0 | 4 | 7 |    |   |   |",
            "| 1 | 5 | 8 |    |   |   |",
            "| 2 | 8 | 1 |    |   |   |",
            "+---+---+---+----+---+---+",
        ];
        assert_batches_sorted_eq!(expected, &batches);

        Ok(())
    }

    #[tokio::test]
    async fn join_date32() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("date", DataType::Date32, false),
            Field::new("n", DataType::Int32, false),
        ]));

        let dates: ArrayRef = Arc::new(Date32Array::from(vec![19107, 19108, 19109]));
        let n: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let batch = RecordBatch::try_new(schema.clone(), vec![dates, n])?;
        let left =
            Arc::new(MemoryExec::try_new(&[vec![batch]], schema.clone(), None).unwrap());

        let dates: ArrayRef = Arc::new(Date32Array::from(vec![19108, 19108, 19109]));
        let n: ArrayRef = Arc::new(Int32Array::from(vec![4, 5, 6]));
        let batch = RecordBatch::try_new(schema.clone(), vec![dates, n])?;
        let right = Arc::new(MemoryExec::try_new(&[vec![batch]], schema, None).unwrap());

        let on = vec![(
            Column::new_with_schema("date", &left.schema()).unwrap(),
            Column::new_with_schema("date", &right.schema()).unwrap(),
        )];

        let join = join(left, right, on, &JoinType::Inner, false)?;

        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let stream = join.execute(0, task_ctx)?;
        let batches = common::collect(stream).await?;

        let expected = vec![
            "+------------+---+------------+---+",
            "| date       | n | date       | n |",
            "+------------+---+------------+---+",
            "| 2022-04-26 | 2 | 2022-04-26 | 4 |",
            "| 2022-04-26 | 2 | 2022-04-26 | 5 |",
            "| 2022-04-27 | 3 | 2022-04-27 | 6 |",
            "+------------+---+------------+---+",
        ];
        assert_batches_sorted_eq!(expected, &batches);

        Ok(())
    }

    #[tokio::test]
    async fn join_with_error_right() {
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![4, 5, 7]),
            ("c1", &vec![7, 8, 9]),
        );

        // right input stream returns one good batch and then one error.
        // The error should be returned.
        let err = Err(ArrowError::ComputeError("bad data error".to_string()));
        let right = build_table_i32(("a2", &vec![]), ("b1", &vec![]), ("c2", &vec![]));

        let on = vec![(
            Column::new_with_schema("b1", &left.schema()).unwrap(),
            Column::new_with_schema("b1", &right.schema()).unwrap(),
        )];
        let schema = right.schema();
        let right = build_table_i32(("a2", &vec![]), ("b1", &vec![]), ("c2", &vec![]));
        let right_input = Arc::new(MockExec::new(vec![Ok(right), err], schema));

        let join_types = vec![
            JoinType::Inner,
            JoinType::Left,
            JoinType::Right,
            JoinType::Full,
            JoinType::LeftSemi,
            JoinType::LeftAnti,
            JoinType::RightSemi,
            JoinType::RightAnti,
        ];

        for join_type in join_types {
            let join = join(
                left.clone(),
                right_input.clone(),
                on.clone(),
                &join_type,
                false,
            )
            .unwrap();
            let session_ctx = SessionContext::new();
            let task_ctx = session_ctx.task_ctx();

            let stream = join.execute(0, task_ctx).unwrap();

            // Expect that an error is returned
            let result_string = crate::physical_plan::common::collect(stream)
                .await
                .unwrap_err()
                .to_string();
            assert!(
                result_string.contains("bad data error"),
                "actual: {}",
                result_string
            );
        }
    }
}
