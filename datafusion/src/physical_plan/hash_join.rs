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

use ahash::CallHasher;
use ahash::RandomState;

use arrow::{
    array::{
        ArrayData, ArrayRef, BooleanArray, Date32Array, Date64Array, Float32Array,
        Float64Array, LargeStringArray, PrimitiveArray, TimestampMicrosecondArray,
        TimestampMillisecondArray, TimestampNanosecondArray, UInt32BufferBuilder,
        UInt32Builder, UInt64BufferBuilder, UInt64Builder,
    },
    compute,
    datatypes::{TimeUnit, UInt32Type, UInt64Type},
};
use smallvec::{smallvec, SmallVec};
use std::{any::Any, usize};
use std::{hash::Hasher, sync::Arc};
use std::{time::Instant, vec};

use async_trait::async_trait;
use futures::{Stream, StreamExt, TryStreamExt};
use hashbrown::HashMap;
use tokio::sync::Mutex;

use arrow::array::Array;
use arrow::datatypes::DataType;
use arrow::datatypes::{Schema, SchemaRef};
use arrow::error::Result as ArrowResult;
use arrow::record_batch::RecordBatch;

use arrow::array::{
    Int16Array, Int32Array, Int64Array, Int8Array, StringArray, UInt16Array, UInt32Array,
    UInt64Array, UInt8Array,
};

use super::expressions::Column;
use super::{
    coalesce_partitions::CoalescePartitionsExec,
    hash_utils::{build_join_schema, check_join_is_valid, JoinOn},
};
use crate::error::{DataFusionError, Result};
use crate::logical_plan::JoinType;

use super::{
    DisplayFormatType, ExecutionPlan, Partitioning, RecordBatchStream,
    SendableRecordBatchStream,
};
use crate::physical_plan::coalesce_batches::concat_batches;
use crate::physical_plan::{PhysicalExpr, SQLMetric};
use log::debug;

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
// TODO: speed up collission check and move away from using a hashbrown HashMap
// https://github.com/apache/arrow-datafusion/issues/50
type JoinHashMap = HashMap<(), SmallVec<[u64; 1]>, IdHashBuilder>;
type JoinLeftData = Arc<(JoinHashMap, RecordBatch)>;

/// join execution plan executes partitions in parallel and combines them into a set of
/// partitions.
#[derive(Debug)]
pub struct HashJoinExec {
    /// left (build) side which gets hashed
    left: Arc<dyn ExecutionPlan>,
    /// right (probe) side which are filtered by the hash table
    right: Arc<dyn ExecutionPlan>,
    /// Set of common columns used to join on
    on: Vec<(Column, Column)>,
    /// How the join is performed
    join_type: JoinType,
    /// The schema once the join is applied
    schema: SchemaRef,
    /// Build-side
    build_side: Arc<Mutex<Option<JoinLeftData>>>,
    /// Shares the `RandomState` for the hashing algorithm
    random_state: RandomState,
    /// Partitioning mode to use
    mode: PartitionMode,
    /// Metrics
    metrics: Arc<HashJoinMetrics>,
}

/// Metrics for HashJoinExec
#[derive(Debug)]
struct HashJoinMetrics {
    /// Total time for joining probe-side batches to the build-side batches
    join_time: Arc<SQLMetric>,
    /// Number of batches consumed by this operator
    input_batches: Arc<SQLMetric>,
    /// Number of rows consumed by this operator
    input_rows: Arc<SQLMetric>,
    /// Number of batches produced by this operator
    output_batches: Arc<SQLMetric>,
    /// Number of rows produced by this operator
    output_rows: Arc<SQLMetric>,
}

impl HashJoinMetrics {
    fn new() -> Self {
        Self {
            join_time: SQLMetric::time_nanos(),
            input_batches: SQLMetric::counter(),
            input_rows: SQLMetric::counter(),
            output_batches: SQLMetric::counter(),
            output_rows: SQLMetric::counter(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
/// Partitioning mode to use for hash join
pub enum PartitionMode {
    /// Left/right children are partitioned using the left and right keys
    Partitioned,
    /// Left side will collected into one partition
    CollectLeft,
}

/// Information about the index and placement (left or right) of the columns
struct ColumnIndex {
    /// Index of the column
    index: usize,
    /// Whether the column is at the left or right side
    is_left: bool,
}

impl HashJoinExec {
    /// Tries to create a new [HashJoinExec].
    /// # Error
    /// This function errors when it is not possible to join the left and right sides on keys `on`.
    pub fn try_new(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: JoinOn,
        join_type: &JoinType,
        partition_mode: PartitionMode,
    ) -> Result<Self> {
        let left_schema = left.schema();
        let right_schema = right.schema();
        check_join_is_valid(&left_schema, &right_schema, &on)?;

        let schema = Arc::new(build_join_schema(&left_schema, &right_schema, join_type));

        let random_state = RandomState::with_seeds(0, 0, 0, 0);

        Ok(HashJoinExec {
            left,
            right,
            on,
            join_type: *join_type,
            schema,
            build_side: Arc::new(Mutex::new(None)),
            random_state,
            mode: partition_mode,
            metrics: Arc::new(HashJoinMetrics::new()),
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

    /// How the join is performed
    pub fn join_type(&self) -> &JoinType {
        &self.join_type
    }

    /// The partitioning mode of this hash join
    pub fn partition_mode(&self) -> &PartitionMode {
        &self.mode
    }

    /// Calculates column indices and left/right placement on input / output schemas and jointype
    fn column_indices_from_schema(&self) -> ArrowResult<Vec<ColumnIndex>> {
        let (primary_is_left, primary_schema, secondary_schema) = match self.join_type {
            JoinType::Inner
            | JoinType::Left
            | JoinType::Full
            | JoinType::Semi
            | JoinType::Anti => (true, self.left.schema(), self.right.schema()),
            JoinType::Right => (false, self.right.schema(), self.left.schema()),
        };
        let mut column_indices = Vec::with_capacity(self.schema.fields().len());
        for field in self.schema.fields() {
            let (is_primary, index) = match primary_schema.index_of(field.name()) {
                    Ok(i) => Ok((true, i)),
                    Err(_) => {
                        match secondary_schema.index_of(field.name()) {
                            Ok(i) => Ok((false, i)),
                            _ => Err(DataFusionError::Internal(
                                format!("During execution, the column {} was not found in neither the left or right side of the join", field.name()).to_string()
                            ))
                        }
                    }
                }.map_err(DataFusionError::into_arrow_external_error)?;

            let is_left =
                is_primary && primary_is_left || !is_primary && !primary_is_left;
            column_indices.push(ColumnIndex { index, is_left });
        }

        Ok(column_indices)
    }
}

#[async_trait]
impl ExecutionPlan for HashJoinExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.left.clone(), self.right.clone()]
    }

    fn with_new_children(
        &self,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match children.len() {
            2 => Ok(Arc::new(HashJoinExec::try_new(
                children[0].clone(),
                children[1].clone(),
                self.on.clone(),
                &self.join_type,
                self.mode,
            )?)),
            _ => Err(DataFusionError::Internal(
                "HashJoinExec wrong number of children".to_string(),
            )),
        }
    }

    fn output_partitioning(&self) -> Partitioning {
        self.right.output_partitioning()
    }

    async fn execute(&self, partition: usize) -> Result<SendableRecordBatchStream> {
        let on_left = self.on.iter().map(|on| on.0.clone()).collect::<Vec<_>>();
        // we only want to compute the build side once for PartitionMode::CollectLeft
        let left_data = {
            match self.mode {
                PartitionMode::CollectLeft => {
                    let mut build_side = self.build_side.lock().await;

                    match build_side.as_ref() {
                        Some(stream) => stream.clone(),
                        None => {
                            let start = Instant::now();

                            // merge all left parts into a single stream
                            let merge = CoalescePartitionsExec::new(self.left.clone());
                            let stream = merge.execute(0).await?;

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
                            let mut hashmap = JoinHashMap::with_capacity_and_hasher(
                                num_rows,
                                IdHashBuilder {},
                            );
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
                                    &self.random_state,
                                    &mut hashes_buffer,
                                )?;
                                offset += batch.num_rows();
                            }
                            // Merge all batches into a single batch, so we
                            // can directly index into the arrays
                            let single_batch =
                                concat_batches(&self.left.schema(), &batches, num_rows)?;

                            let left_side = Arc::new((hashmap, single_batch));

                            *build_side = Some(left_side.clone());

                            debug!(
                                "Built build-side of hash join containing {} rows in {} ms",
                                num_rows,
                                start.elapsed().as_millis()
                            );

                            left_side
                        }
                    }
                }
                PartitionMode::Partitioned => {
                    let start = Instant::now();

                    // Load 1 partition of left side in memory
                    let stream = self.left.execute(partition).await?;

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
                    let mut hashmap =
                        JoinHashMap::with_capacity_and_hasher(num_rows, IdHashBuilder {});
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
                            &self.random_state,
                            &mut hashes_buffer,
                        )?;
                        offset += batch.num_rows();
                    }
                    // Merge all batches into a single batch, so we
                    // can directly index into the arrays
                    let single_batch =
                        concat_batches(&self.left.schema(), &batches, num_rows)?;

                    let left_side = Arc::new((hashmap, single_batch));

                    debug!(
                        "Built build-side {} of hash join containing {} rows in {} ms",
                        partition,
                        num_rows,
                        start.elapsed().as_millis()
                    );

                    left_side
                }
            }
        };

        // we have the batches and the hash map with their keys. We can how create a stream
        // over the right that uses this information to issue new batches.

        let right_stream = self.right.execute(partition).await?;
        let on_right = self.on.iter().map(|on| on.1.clone()).collect::<Vec<_>>();

        let column_indices = self.column_indices_from_schema()?;
        let num_rows = left_data.1.num_rows();
        let visited_left_side = match self.join_type {
            JoinType::Left | JoinType::Full | JoinType::Semi | JoinType::Anti => {
                vec![false; num_rows]
            }
            JoinType::Inner | JoinType::Right => vec![],
        };
        Ok(Box::pin(HashJoinStream::new(
            self.schema.clone(),
            on_left,
            on_right,
            self.join_type,
            left_data,
            right_stream,
            column_indices,
            self.random_state.clone(),
            visited_left_side,
            self.metrics.clone(),
        )))
    }

    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(
                    f,
                    "HashJoinExec: mode={:?}, join_type={:?}, on={:?}",
                    self.mode, self.join_type, self.on
                )
            }
        }
    }

    fn metrics(&self) -> HashMap<String, SQLMetric> {
        let mut metrics = HashMap::new();
        metrics.insert("joinTime".to_owned(), (*self.metrics.join_time).clone());
        metrics.insert(
            "inputBatches".to_owned(),
            (*self.metrics.input_batches).clone(),
        );
        metrics.insert("inputRows".to_owned(), (*self.metrics.input_rows).clone());
        metrics.insert(
            "outputBatches".to_owned(),
            (*self.metrics.output_batches).clone(),
        );
        metrics.insert("outputRows".to_owned(), (*self.metrics.output_rows).clone());
        metrics
    }
}

/// Updates `hash` with new entries from [RecordBatch] evaluated against the expressions `on`,
/// assuming that the [RecordBatch] corresponds to the `index`th
fn update_hash(
    on: &[Column],
    batch: &RecordBatch,
    hash: &mut JoinHashMap,
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
        match hash.raw_entry_mut().from_hash(*hash_value, |_| true) {
            hashbrown::hash_map::RawEntryMut::Occupied(mut entry) => {
                entry.get_mut().push((row + offset) as u64);
            }
            hashbrown::hash_map::RawEntryMut::Vacant(entry) => {
                entry.insert_hashed_nocheck(
                    *hash_value,
                    (),
                    smallvec![(row + offset) as u64],
                );
            }
        };
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
    /// type of the join
    join_type: JoinType,
    /// information from the left
    left_data: JoinLeftData,
    /// right
    right: SendableRecordBatchStream,
    /// Information of index and left / right placement of columns
    column_indices: Vec<ColumnIndex>,
    /// Random state used for hashing initialization
    random_state: RandomState,
    /// Keeps track of the left side rows whether they are visited
    visited_left_side: Vec<bool>, // TODO: use a more memory efficient data structure, https://github.com/apache/arrow-datafusion/issues/240
    /// There is nothing to process anymore and left side is processed in case of left join
    is_exhausted: bool,
    /// Metrics
    metrics: Arc<HashJoinMetrics>,
}

#[allow(clippy::too_many_arguments)]
impl HashJoinStream {
    fn new(
        schema: Arc<Schema>,
        on_left: Vec<Column>,
        on_right: Vec<Column>,
        join_type: JoinType,
        left_data: JoinLeftData,
        right: SendableRecordBatchStream,
        column_indices: Vec<ColumnIndex>,
        random_state: RandomState,
        visited_left_side: Vec<bool>,
        metrics: Arc<HashJoinMetrics>,
    ) -> Self {
        HashJoinStream {
            schema,
            on_left,
            on_right,
            join_type,
            left_data,
            right,
            column_indices,
            random_state,
            visited_left_side,
            is_exhausted: false,
            metrics,
        }
    }
}

impl RecordBatchStream for HashJoinStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// Returns a new [RecordBatch] by combining the `left` and `right` according to `indices`.
/// The resulting batch has [Schema] `schema`.
/// # Error
/// This function errors when:
/// *
fn build_batch_from_indices(
    schema: &Schema,
    left: &RecordBatch,
    right: &RecordBatch,
    left_indices: UInt64Array,
    right_indices: UInt32Array,
    column_indices: &[ColumnIndex],
) -> ArrowResult<(RecordBatch, UInt64Array)> {
    // build the columns of the new [RecordBatch]:
    // 1. pick whether the column is from the left or right
    // 2. based on the pick, `take` items from the different RecordBatches
    let mut columns: Vec<Arc<dyn Array>> = Vec::with_capacity(schema.fields().len());

    for column_index in column_indices {
        let array = if column_index.is_left {
            let array = left.column(column_index.index);
            compute::take(array.as_ref(), &left_indices, None)?
        } else {
            let array = right.column(column_index.index);
            compute::take(array.as_ref(), &right_indices, None)?
        };
        columns.push(array);
    }
    RecordBatch::try_new(Arc::new(schema.clone()), columns).map(|x| (x, left_indices))
}

#[allow(clippy::too_many_arguments)]
fn build_batch(
    batch: &RecordBatch,
    left_data: &JoinLeftData,
    on_left: &[Column],
    on_right: &[Column],
    join_type: JoinType,
    schema: &Schema,
    column_indices: &[ColumnIndex],
    random_state: &RandomState,
) -> ArrowResult<(RecordBatch, UInt64Array)> {
    let (left_indices, right_indices) =
        build_join_indexes(left_data, batch, join_type, on_left, on_right, random_state)
            .unwrap();

    if matches!(join_type, JoinType::Semi | JoinType::Anti) {
        return Ok((
            RecordBatch::new_empty(Arc::new(schema.clone())),
            left_indices,
        ));
    }

    build_batch_from_indices(
        schema,
        &left_data.1,
        batch,
        left_indices,
        right_indices,
        column_indices,
    )
}

/// returns a vector with (index from left, index from right).
/// The size of this vector corresponds to the total size of a joined batch
// For a join on column A:
// left       right
//     batch 1
// A B         A D
// ---------------
// 1 a         3 6
// 2 b         1 2
// 3 c         2 4
//     batch 2
// A B         A D
// ---------------
// 1 a         5 10
// 2 b         2 2
// 4 d         1 1
// indices (batch, batch_row)
// left       right
// (0, 2)     (0, 0)
// (0, 0)     (0, 1)
// (0, 1)     (0, 2)
// (1, 0)     (0, 1)
// (1, 1)     (0, 2)
// (0, 1)     (1, 1)
// (0, 0)     (1, 2)
// (1, 1)     (1, 1)
// (1, 0)     (1, 2)
fn build_join_indexes(
    left_data: &JoinLeftData,
    right: &RecordBatch,
    join_type: JoinType,
    left_on: &[Column],
    right_on: &[Column],
    random_state: &RandomState,
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

    match join_type {
        JoinType::Inner | JoinType::Semi | JoinType::Anti => {
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
                    left.raw_entry().from_hash(*hash_value, |_| true)
                {
                    for &i in indices {
                        // Check hash collisions
                        if equal_rows(i as usize, row, &left_join_values, &keys_values)? {
                            left_indices.append(i);
                            right_indices.append(row as u32);
                        }
                    }
                }
            }
            let left = ArrayData::builder(DataType::UInt64)
                .len(left_indices.len())
                .add_buffer(left_indices.finish())
                .build();
            let right = ArrayData::builder(DataType::UInt32)
                .len(right_indices.len())
                .add_buffer(right_indices.finish())
                .build();

            Ok((
                PrimitiveArray::<UInt64Type>::from(left),
                PrimitiveArray::<UInt32Type>::from(right),
            ))
        }
        JoinType::Left => {
            let mut left_indices = UInt64Builder::new(0);
            let mut right_indices = UInt32Builder::new(0);

            // First visit all of the rows
            for (row, hash_value) in hash_values.iter().enumerate() {
                if let Some((_, indices)) =
                    left.raw_entry().from_hash(*hash_value, |_| true)
                {
                    for &i in indices {
                        // Collision check
                        if equal_rows(i as usize, row, &left_join_values, &keys_values)? {
                            left_indices.append_value(i)?;
                            right_indices.append_value(row as u32)?;
                        }
                    }
                };
            }
            Ok((left_indices.finish(), right_indices.finish()))
        }
        JoinType::Right | JoinType::Full => {
            let mut left_indices = UInt64Builder::new(0);
            let mut right_indices = UInt32Builder::new(0);

            for (row, hash_value) in hash_values.iter().enumerate() {
                match left.raw_entry().from_hash(*hash_value, |_| true) {
                    Some((_, indices)) => {
                        for &i in indices {
                            if equal_rows(
                                i as usize,
                                row,
                                &left_join_values,
                                &keys_values,
                            )? {
                                left_indices.append_value(i)?;
                            } else {
                                left_indices.append_null()?;
                            }
                            right_indices.append_value(row as u32)?;
                        }
                    }
                    None => {
                        // when no match, add the row with None for the left side
                        left_indices.append_null()?;
                        right_indices.append_value(row as u32)?;
                    }
                }
            }
            Ok((left_indices.finish(), right_indices.finish()))
        }
    }
}
use core::hash::BuildHasher;

/// `Hasher` that returns the same `u64` value as a hash, to avoid re-hashing
/// it when inserting/indexing or regrowing the `HashMap`
struct IdHasher {
    hash: u64,
}

impl Hasher for IdHasher {
    fn finish(&self) -> u64 {
        self.hash
    }

    fn write_u64(&mut self, i: u64) {
        self.hash = i;
    }

    fn write(&mut self, _bytes: &[u8]) {
        unreachable!("IdHasher should only be used for u64 keys")
    }
}

#[derive(Debug)]
struct IdHashBuilder {}

impl BuildHasher for IdHashBuilder {
    type Hasher = IdHasher;

    fn build_hasher(&self) -> Self::Hasher {
        IdHasher { hash: 0 }
    }
}

// Combines two hashes into one hash
#[inline]
fn combine_hashes(l: u64, r: u64) -> u64 {
    let hash = (17 * 37u64).wrapping_add(l);
    hash.wrapping_mul(37).wrapping_add(r)
}

macro_rules! equal_rows_elem {
    ($array_type:ident, $l: ident, $r: ident, $left: ident, $right: ident) => {{
        let left_array = $l.as_any().downcast_ref::<$array_type>().unwrap();
        let right_array = $r.as_any().downcast_ref::<$array_type>().unwrap();

        match (left_array.is_null($left), left_array.is_null($right)) {
            (false, false) => left_array.value($left) == right_array.value($right),
            _ => false,
        }
    }};
}

/// Left and right row have equal values
fn equal_rows(
    left: usize,
    right: usize,
    left_arrays: &[ArrayRef],
    right_arrays: &[ArrayRef],
) -> Result<bool> {
    let mut err = None;
    let res = left_arrays
        .iter()
        .zip(right_arrays)
        .all(|(l, r)| match l.data_type() {
            DataType::Null => true,
            DataType::Boolean => equal_rows_elem!(BooleanArray, l, r, left, right),
            DataType::Int8 => equal_rows_elem!(Int8Array, l, r, left, right),
            DataType::Int16 => equal_rows_elem!(Int16Array, l, r, left, right),
            DataType::Int32 => equal_rows_elem!(Int32Array, l, r, left, right),
            DataType::Int64 => equal_rows_elem!(Int64Array, l, r, left, right),
            DataType::UInt8 => equal_rows_elem!(UInt8Array, l, r, left, right),
            DataType::UInt16 => equal_rows_elem!(UInt16Array, l, r, left, right),
            DataType::UInt32 => equal_rows_elem!(UInt32Array, l, r, left, right),
            DataType::UInt64 => equal_rows_elem!(UInt64Array, l, r, left, right),
            DataType::Timestamp(_, None) => {
                equal_rows_elem!(Int64Array, l, r, left, right)
            }
            DataType::Utf8 => equal_rows_elem!(StringArray, l, r, left, right),
            DataType::LargeUtf8 => equal_rows_elem!(LargeStringArray, l, r, left, right),
            _ => {
                // This is internal because we should have caught this before.
                err = Some(Err(DataFusionError::Internal(
                    "Unsupported data type in hasher".to_string(),
                )));
                false
            }
        });

    err.unwrap_or(Ok(res))
}

macro_rules! hash_array {
    ($array_type:ident, $column: ident, $ty: ident, $hashes: ident, $random_state: ident, $multi_col: ident) => {
        let array = $column.as_any().downcast_ref::<$array_type>().unwrap();
        if array.null_count() == 0 {
            if $multi_col {
                for (i, hash) in $hashes.iter_mut().enumerate() {
                    *hash = combine_hashes(
                        $ty::get_hash(&array.value(i), $random_state),
                        *hash,
                    );
                }
            } else {
                for (i, hash) in $hashes.iter_mut().enumerate() {
                    *hash = $ty::get_hash(&array.value(i), $random_state);
                }
            }
        } else {
            if $multi_col {
                for (i, hash) in $hashes.iter_mut().enumerate() {
                    if !array.is_null(i) {
                        *hash = combine_hashes(
                            $ty::get_hash(&array.value(i), $random_state),
                            *hash,
                        );
                    }
                }
            } else {
                for (i, hash) in $hashes.iter_mut().enumerate() {
                    if !array.is_null(i) {
                        *hash = $ty::get_hash(&array.value(i), $random_state);
                    }
                }
            }
        }
    };
}

macro_rules! hash_array_primitive {
    ($array_type:ident, $column: ident, $ty: ident, $hashes: ident, $random_state: ident, $multi_col: ident) => {
        let array = $column.as_any().downcast_ref::<$array_type>().unwrap();
        let values = array.values();

        if array.null_count() == 0 {
            if $multi_col {
                for (hash, value) in $hashes.iter_mut().zip(values.iter()) {
                    *hash = combine_hashes($ty::get_hash(value, $random_state), *hash);
                }
            } else {
                for (hash, value) in $hashes.iter_mut().zip(values.iter()) {
                    *hash = $ty::get_hash(value, $random_state)
                }
            }
        } else {
            if $multi_col {
                for (i, (hash, value)) in
                    $hashes.iter_mut().zip(values.iter()).enumerate()
                {
                    if !array.is_null(i) {
                        *hash =
                            combine_hashes($ty::get_hash(value, $random_state), *hash);
                    }
                }
            } else {
                for (i, (hash, value)) in
                    $hashes.iter_mut().zip(values.iter()).enumerate()
                {
                    if !array.is_null(i) {
                        *hash = $ty::get_hash(value, $random_state);
                    }
                }
            }
        }
    };
}

macro_rules! hash_array_float {
    ($array_type:ident, $column: ident, $ty: ident, $hashes: ident, $random_state: ident, $multi_col: ident) => {
        let array = $column.as_any().downcast_ref::<$array_type>().unwrap();
        let values = array.values();

        if array.null_count() == 0 {
            if $multi_col {
                for (hash, value) in $hashes.iter_mut().zip(values.iter()) {
                    *hash = combine_hashes(
                        $ty::get_hash(
                            &$ty::from_le_bytes(value.to_le_bytes()),
                            $random_state,
                        ),
                        *hash,
                    );
                }
            } else {
                for (hash, value) in $hashes.iter_mut().zip(values.iter()) {
                    *hash = $ty::get_hash(
                        &$ty::from_le_bytes(value.to_le_bytes()),
                        $random_state,
                    )
                }
            }
        } else {
            if $multi_col {
                for (i, (hash, value)) in
                    $hashes.iter_mut().zip(values.iter()).enumerate()
                {
                    if !array.is_null(i) {
                        *hash = combine_hashes(
                            $ty::get_hash(
                                &$ty::from_le_bytes(value.to_le_bytes()),
                                $random_state,
                            ),
                            *hash,
                        );
                    }
                }
            } else {
                for (i, (hash, value)) in
                    $hashes.iter_mut().zip(values.iter()).enumerate()
                {
                    if !array.is_null(i) {
                        *hash = $ty::get_hash(
                            &$ty::from_le_bytes(value.to_le_bytes()),
                            $random_state,
                        );
                    }
                }
            }
        }
    };
}

/// Creates hash values for every element in the row based on the values in the columns
pub fn create_hashes<'a>(
    arrays: &[ArrayRef],
    random_state: &RandomState,
    hashes_buffer: &'a mut Vec<u64>,
) -> Result<&'a mut Vec<u64>> {
    // combine hashes with `combine_hashes` if we have more than 1 column
    let multi_col = arrays.len() > 1;

    for col in arrays {
        match col.data_type() {
            DataType::UInt8 => {
                hash_array_primitive!(
                    UInt8Array,
                    col,
                    u8,
                    hashes_buffer,
                    random_state,
                    multi_col
                );
            }
            DataType::UInt16 => {
                hash_array_primitive!(
                    UInt16Array,
                    col,
                    u16,
                    hashes_buffer,
                    random_state,
                    multi_col
                );
            }
            DataType::UInt32 => {
                hash_array_primitive!(
                    UInt32Array,
                    col,
                    u32,
                    hashes_buffer,
                    random_state,
                    multi_col
                );
            }
            DataType::UInt64 => {
                hash_array_primitive!(
                    UInt64Array,
                    col,
                    u64,
                    hashes_buffer,
                    random_state,
                    multi_col
                );
            }
            DataType::Int8 => {
                hash_array_primitive!(
                    Int8Array,
                    col,
                    i8,
                    hashes_buffer,
                    random_state,
                    multi_col
                );
            }
            DataType::Int16 => {
                hash_array_primitive!(
                    Int16Array,
                    col,
                    i16,
                    hashes_buffer,
                    random_state,
                    multi_col
                );
            }
            DataType::Int32 => {
                hash_array_primitive!(
                    Int32Array,
                    col,
                    i32,
                    hashes_buffer,
                    random_state,
                    multi_col
                );
            }
            DataType::Int64 => {
                hash_array_primitive!(
                    Int64Array,
                    col,
                    i64,
                    hashes_buffer,
                    random_state,
                    multi_col
                );
            }
            DataType::Float32 => {
                hash_array_float!(
                    Float32Array,
                    col,
                    u32,
                    hashes_buffer,
                    random_state,
                    multi_col
                );
            }
            DataType::Float64 => {
                hash_array_float!(
                    Float64Array,
                    col,
                    u64,
                    hashes_buffer,
                    random_state,
                    multi_col
                );
            }
            DataType::Timestamp(TimeUnit::Millisecond, None) => {
                hash_array_primitive!(
                    TimestampMillisecondArray,
                    col,
                    i64,
                    hashes_buffer,
                    random_state,
                    multi_col
                );
            }
            DataType::Timestamp(TimeUnit::Microsecond, None) => {
                hash_array_primitive!(
                    TimestampMicrosecondArray,
                    col,
                    i64,
                    hashes_buffer,
                    random_state,
                    multi_col
                );
            }
            DataType::Timestamp(TimeUnit::Nanosecond, None) => {
                hash_array_primitive!(
                    TimestampNanosecondArray,
                    col,
                    i64,
                    hashes_buffer,
                    random_state,
                    multi_col
                );
            }
            DataType::Date32 => {
                hash_array_primitive!(
                    Date32Array,
                    col,
                    i32,
                    hashes_buffer,
                    random_state,
                    multi_col
                );
            }
            DataType::Date64 => {
                hash_array_primitive!(
                    Date64Array,
                    col,
                    i64,
                    hashes_buffer,
                    random_state,
                    multi_col
                );
            }
            DataType::Boolean => {
                hash_array!(
                    BooleanArray,
                    col,
                    u8,
                    hashes_buffer,
                    random_state,
                    multi_col
                );
            }
            DataType::Utf8 => {
                hash_array!(
                    StringArray,
                    col,
                    str,
                    hashes_buffer,
                    random_state,
                    multi_col
                );
            }
            DataType::LargeUtf8 => {
                hash_array!(
                    LargeStringArray,
                    col,
                    str,
                    hashes_buffer,
                    random_state,
                    multi_col
                );
            }
            _ => {
                // This is internal because we should have caught this before.
                return Err(DataFusionError::Internal(
                    "Unsupported data type in hasher".to_string(),
                ));
            }
        }
    }
    Ok(hashes_buffer)
}

// Produces a batch for left-side rows that have/have not been matched during the whole join
fn produce_from_matched(
    visited_left_side: &[bool],
    schema: &SchemaRef,
    column_indices: &[ColumnIndex],
    left_data: &JoinLeftData,
    unmatched: bool,
) -> ArrowResult<RecordBatch> {
    // Find indices which didn't match any right row (are false)
    let indices = if unmatched {
        UInt64Array::from_iter_values(
            visited_left_side
                .iter()
                .enumerate()
                .filter(|&(_, &value)| !value)
                .map(|(index, _)| index as u64),
        )
    } else {
        // produce those that did match
        UInt64Array::from_iter_values(
            visited_left_side
                .iter()
                .enumerate()
                .filter(|&(_, &value)| value)
                .map(|(index, _)| index as u64),
        )
    };

    // generate batches by taking values from the left side and generating columns filled with null on the right side
    let num_rows = indices.len();
    let mut columns: Vec<Arc<dyn Array>> = Vec::with_capacity(schema.fields().len());
    for (idx, column_index) in column_indices.iter().enumerate() {
        let array = if column_index.is_left {
            let array = left_data.1.column(column_index.index);
            compute::take(array.as_ref(), &indices, None).unwrap()
        } else {
            let datatype = schema.field(idx).data_type();
            arrow::array::new_null_array(datatype, num_rows)
        };

        columns.push(array);
    }
    RecordBatch::try_new(schema.clone(), columns)
}

impl Stream for HashJoinStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.right
            .poll_next_unpin(cx)
            .map(|maybe_batch| match maybe_batch {
                Some(Ok(batch)) => {
                    let start = Instant::now();
                    let result = build_batch(
                        &batch,
                        &self.left_data,
                        &self.on_left,
                        &self.on_right,
                        self.join_type,
                        &self.schema,
                        &self.column_indices,
                        &self.random_state,
                    );
                    self.metrics.input_batches.add(1);
                    self.metrics.input_rows.add(batch.num_rows());
                    if let Ok((ref batch, ref left_side)) = result {
                        self.metrics
                            .join_time
                            .add(start.elapsed().as_millis() as usize);
                        self.metrics.output_batches.add(1);
                        self.metrics.output_rows.add(batch.num_rows());

                        match self.join_type {
                            JoinType::Left
                            | JoinType::Full
                            | JoinType::Semi
                            | JoinType::Anti => {
                                left_side.iter().flatten().for_each(|x| {
                                    self.visited_left_side[x as usize] = true;
                                });
                            }
                            JoinType::Inner | JoinType::Right => {}
                        }
                    }
                    Some(result.map(|x| x.0))
                }
                other => {
                    let start = Instant::now();
                    // For the left join, produce rows for unmatched rows
                    match self.join_type {
                        JoinType::Left
                        | JoinType::Full
                        | JoinType::Semi
                        | JoinType::Anti
                            if !self.is_exhausted =>
                        {
                            let result = produce_from_matched(
                                &self.visited_left_side,
                                &self.schema,
                                &self.column_indices,
                                &self.left_data,
                                self.join_type != JoinType::Semi,
                            );
                            if let Ok(ref batch) = result {
                                self.metrics.input_batches.add(1);
                                self.metrics.input_rows.add(batch.num_rows());
                                if let Ok(ref batch) = result {
                                    self.metrics
                                        .join_time
                                        .add(start.elapsed().as_millis() as usize);
                                    self.metrics.output_batches.add(1);
                                    self.metrics.output_rows.add(batch.num_rows());
                                }
                            }
                            self.is_exhausted = true;
                            return Some(result);
                        }
                        JoinType::Left
                        | JoinType::Full
                        | JoinType::Semi
                        | JoinType::Anti
                        | JoinType::Inner
                        | JoinType::Right => {}
                    }

                    other
                }
            })
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        assert_batches_sorted_eq,
        physical_plan::{
            common, expressions::Column, memory::MemoryExec, repartition::RepartitionExec,
        },
        test::{build_table_i32, columns},
    };

    use super::*;
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
    ) -> Result<HashJoinExec> {
        HashJoinExec::try_new(left, right, on, join_type, PartitionMode::CollectLeft)
    }

    async fn join_collect(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: JoinOn,
        join_type: &JoinType,
    ) -> Result<(Vec<String>, Vec<RecordBatch>)> {
        let join = join(left, right, on, join_type)?;
        let columns = columns(&join.schema());

        let stream = join.execute(0).await?;
        let batches = common::collect(stream).await?;

        Ok((columns, batches))
    }

    async fn partitioned_join_collect(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: JoinOn,
        join_type: &JoinType,
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
            join_type,
            PartitionMode::Partitioned,
        )?;

        let columns = columns(&join.schema());

        let mut batches = vec![];
        for i in 0..partition_count {
            let stream = join.execute(i).await?;
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

        let (columns, batches) =
            join_collect(left.clone(), right.clone(), on.clone(), &JoinType::Inner)
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

        let (columns, batches) = join_collect(left, right, on, &JoinType::Inner).await?;

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

        let (columns, batches) = join_collect(left, right, on, &JoinType::Inner).await?;

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

        let (columns, batches) = join_collect(left, right, on, &JoinType::Inner).await?;

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

        let join = join(left, right, on, &JoinType::Inner)?;

        let columns = columns(&join.schema());
        assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b1", "c2"]);

        // first part
        let stream = join.execute(0).await?;
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
        let stream = join.execute(1).await?;
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

        let join = join(left, right, on, &JoinType::Left).unwrap();

        let columns = columns(&join.schema());
        assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b1", "c2"]);

        let stream = join.execute(0).await.unwrap();
        let batches = common::collect(stream).await.unwrap();

        let expected = vec![
            "+----+----+----+----+----+----+",
            "| a1 | b1 | c1 | a2 | b1 | c2 |",
            "+----+----+----+----+----+----+",
            "| 1  | 4  | 7  | 10 | 4  | 70 |",
            "| 1  | 4  | 7  | 10 | 4  | 70 |",
            "| 2  | 5  | 8  | 20 | 5  | 80 |",
            "| 2  | 5  | 8  | 20 | 5  | 80 |",
            "| 3  | 7  | 9  |    | 7  |    |",
            "+----+----+----+----+----+----+",
        ];

        assert_batches_sorted_eq!(expected, &batches);
    }

    #[tokio::test]
    async fn join_full_multi_batch() {
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

        let join = join(left, right, on, &JoinType::Full).unwrap();

        let columns = columns(&join.schema());
        assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b2", "c2"]);

        let stream = join.execute(0).await.unwrap();
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
        let join = join(left, right, on, &JoinType::Left).unwrap();

        let columns = columns(&join.schema());
        assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b1", "c2"]);

        let stream = join.execute(0).await.unwrap();
        let batches = common::collect(stream).await.unwrap();

        let expected = vec![
            "+----+----+----+----+----+----+",
            "| a1 | b1 | c1 | a2 | b1 | c2 |",
            "+----+----+----+----+----+----+",
            "| 1  | 4  | 7  |    | 4  |    |",
            "| 2  | 5  | 8  |    | 5  |    |",
            "| 3  | 7  | 9  |    | 7  |    |",
            "+----+----+----+----+----+----+",
        ];

        assert_batches_sorted_eq!(expected, &batches);
    }

    #[tokio::test]
    async fn join_full_empty_right() {
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
        let join = join(left, right, on, &JoinType::Full).unwrap();

        let columns = columns(&join.schema());
        assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b2", "c2"]);

        let stream = join.execute(0).await.unwrap();
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

        let (columns, batches) =
            join_collect(left.clone(), right.clone(), on.clone(), &JoinType::Left)
                .await?;
        assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b1", "c2"]);

        let expected = vec![
            "+----+----+----+----+----+----+",
            "| a1 | b1 | c1 | a2 | b1 | c2 |",
            "+----+----+----+----+----+----+",
            "| 1  | 4  | 7  | 10 | 4  | 70 |",
            "| 2  | 5  | 8  | 20 | 5  | 80 |",
            "| 3  | 7  | 9  |    | 7  |    |",
            "+----+----+----+----+----+----+",
        ];
        assert_batches_sorted_eq!(expected, &batches);

        Ok(())
    }

    #[tokio::test]
    async fn partitioned_join_left_one() -> Result<()> {
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
        )
        .await?;
        assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b1", "c2"]);

        let expected = vec![
            "+----+----+----+----+----+----+",
            "| a1 | b1 | c1 | a2 | b1 | c2 |",
            "+----+----+----+----+----+----+",
            "| 1  | 4  | 7  | 10 | 4  | 70 |",
            "| 2  | 5  | 8  | 20 | 5  | 80 |",
            "| 3  | 7  | 9  |    | 7  |    |",
            "+----+----+----+----+----+----+",
        ];
        assert_batches_sorted_eq!(expected, &batches);

        Ok(())
    }

    #[tokio::test]
    async fn join_semi() -> Result<()> {
        let left = build_table(
            ("a1", &vec![1, 2, 2, 3]),
            ("b1", &vec![4, 5, 5, 7]), // 7 does not exist on the right
            ("c1", &vec![7, 8, 8, 9]),
        );
        let right = build_table(
            ("a2", &vec![10, 20, 30, 40]),
            ("b1", &vec![4, 5, 6, 5]), // 5 is double on the right
            ("c2", &vec![70, 80, 90, 100]),
        );
        let on = vec![(
            Column::new_with_schema("b1", &left.schema())?,
            Column::new_with_schema("b1", &right.schema())?,
        )];

        let join = join(left, right, on, &JoinType::Semi)?;

        let columns = columns(&join.schema());
        assert_eq!(columns, vec!["a1", "b1", "c1"]);

        let stream = join.execute(0).await?;
        let batches = common::collect(stream).await?;

        let expected = vec![
            "+----+----+----+",
            "| a1 | b1 | c1 |",
            "+----+----+----+",
            "| 1  | 4  | 7  |",
            "| 2  | 5  | 8  |",
            "| 2  | 5  | 8  |",
            "+----+----+----+",
        ];
        assert_batches_sorted_eq!(expected, &batches);

        Ok(())
    }

    #[tokio::test]
    async fn join_anti() -> Result<()> {
        let left = build_table(
            ("a1", &vec![1, 2, 2, 3, 5]),
            ("b1", &vec![4, 5, 5, 7, 7]), // 7 does not exist on the right
            ("c1", &vec![7, 8, 8, 9, 11]),
        );
        let right = build_table(
            ("a2", &vec![10, 20, 30, 40]),
            ("b1", &vec![4, 5, 6, 5]), // 5 is double on the right
            ("c2", &vec![70, 80, 90, 100]),
        );
        let on = vec![(
            Column::new_with_schema("b1", &left.schema())?,
            Column::new_with_schema("b1", &right.schema())?,
        )];

        let join = join(left, right, on, &JoinType::Anti)?;

        let columns = columns(&join.schema());
        assert_eq!(columns, vec!["a1", "b1", "c1"]);

        let stream = join.execute(0).await?;
        let batches = common::collect(stream).await?;

        let expected = vec![
            "+----+----+----+",
            "| a1 | b1 | c1 |",
            "+----+----+----+",
            "| 3  | 7  | 9  |",
            "| 5  | 7  | 11 |",
            "+----+----+----+",
        ];
        assert_batches_sorted_eq!(expected, &batches);
        Ok(())
    }

    #[tokio::test]
    async fn join_right_one() -> Result<()> {
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

        let (columns, batches) = join_collect(left, right, on, &JoinType::Right).await?;

        assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b1", "c2"]);

        let expected = vec![
            "+----+----+----+----+----+----+",
            "| a1 | b1 | c1 | a2 | b1 | c2 |",
            "+----+----+----+----+----+----+",
            "|    | 6  |    | 30 | 6  | 90 |",
            "| 1  | 4  | 7  | 10 | 4  | 70 |",
            "| 2  | 5  | 8  | 20 | 5  | 80 |",
            "+----+----+----+----+----+----+",
        ];

        assert_batches_sorted_eq!(expected, &batches);

        Ok(())
    }

    #[tokio::test]
    async fn partitioned_join_right_one() -> Result<()> {
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
            partitioned_join_collect(left, right, on, &JoinType::Right).await?;

        assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b1", "c2"]);

        let expected = vec![
            "+----+----+----+----+----+----+",
            "| a1 | b1 | c1 | a2 | b1 | c2 |",
            "+----+----+----+----+----+----+",
            "|    | 6  |    | 30 | 6  | 90 |",
            "| 1  | 4  | 7  | 10 | 4  | 70 |",
            "| 2  | 5  | 8  | 20 | 5  | 80 |",
            "+----+----+----+----+----+----+",
        ];

        assert_batches_sorted_eq!(expected, &batches);

        Ok(())
    }

    #[tokio::test]
    async fn join_full_one() -> Result<()> {
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

        let join = join(left, right, on, &JoinType::Full)?;

        let columns = columns(&join.schema());
        assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b2", "c2"]);

        let stream = join.execute(0).await?;
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
    fn create_hashes_for_float_arrays() -> Result<()> {
        let f32_arr = Arc::new(Float32Array::from(vec![0.12, 0.5, 1f32, 444.7]));
        let f64_arr = Arc::new(Float64Array::from(vec![0.12, 0.5, 1f64, 444.7]));

        let random_state = RandomState::with_seeds(0, 0, 0, 0);
        let hashes_buff = &mut vec![0; f32_arr.len()];
        let hashes = create_hashes(&[f32_arr], &random_state, hashes_buff)?;
        assert_eq!(hashes.len(), 4,);

        let hashes = create_hashes(&[f64_arr], &random_state, hashes_buff)?;
        assert_eq!(hashes.len(), 4,);

        Ok(())
    }

    #[test]
    fn join_with_hash_collision() -> Result<()> {
        let mut hashmap_left = HashMap::with_capacity_and_hasher(2, IdHashBuilder {});
        let left = build_table_i32(
            ("a", &vec![10, 20]),
            ("x", &vec![100, 200]),
            ("y", &vec![200, 300]),
        );

        let random_state = RandomState::with_seeds(0, 0, 0, 0);
        let hashes_buff = &mut vec![0; left.num_rows()];
        let hashes =
            create_hashes(&[left.columns()[0].clone()], &random_state, hashes_buff)?;

        // Create hash collisions
        match hashmap_left.raw_entry_mut().from_hash(hashes[0], |_| true) {
            hashbrown::hash_map::RawEntryMut::Vacant(entry) => {
                entry.insert_hashed_nocheck(hashes[0], (), smallvec![0, 1])
            }
            _ => unreachable!("Hash should not be vacant"),
        };
        match hashmap_left.raw_entry_mut().from_hash(hashes[1], |_| true) {
            hashbrown::hash_map::RawEntryMut::Vacant(entry) => {
                entry.insert_hashed_nocheck(hashes[1], (), smallvec![0, 1])
            }
            _ => unreachable!("Hash should not be vacant"),
        };

        let right = build_table_i32(
            ("a", &vec![10, 20]),
            ("b", &vec![0, 0]),
            ("c", &vec![30, 40]),
        );

        let left_data = JoinLeftData::new((hashmap_left, left));
        let (l, r) = build_join_indexes(
            &left_data,
            &right,
            JoinType::Inner,
            &[Column::new("a", 0)],
            &[Column::new("a", 0)],
            &random_state,
        )?;

        let mut left_ids = UInt64Builder::new(0);
        left_ids.append_value(0)?;
        left_ids.append_value(1)?;

        let mut right_ids = UInt32Builder::new(0);
        right_ids.append_value(0)?;
        right_ids.append_value(1)?;

        assert_eq!(left_ids.finish(), l);

        assert_eq!(right_ids.finish(), r);

        Ok(())
    }
}
