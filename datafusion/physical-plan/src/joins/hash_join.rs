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

//! [`HashJoinExec`] Partitioned Hash Join Operator

use std::fmt;
use std::mem::size_of;
use std::sync::Arc;
use std::task::Poll;
use std::{any::Any, usize, vec};

use crate::joins::utils::{
    adjust_indices_by_join_type, apply_join_filter_to_indices, build_batch_from_indices,
    calculate_join_output_ordering, get_final_indices_from_bit_map,
    need_produce_result_in_final,
};
use crate::DisplayAs;
use crate::{
    coalesce_batches::concat_batches,
    coalesce_partitions::CoalescePartitionsExec,
    expressions::Column,
    expressions::PhysicalSortExpr,
    hash_utils::create_hashes,
    joins::hash_join_utils::{JoinHashMap, JoinHashMapType},
    joins::utils::{
        adjust_right_output_partitioning, build_join_schema, check_join_is_valid,
        estimate_join_statistics, partitioned_join_output_partitioning,
        BuildProbeJoinMetrics, ColumnIndex, JoinFilter, JoinOn,
    },
    metrics::{ExecutionPlanMetricsSet, MetricsSet},
    DisplayFormatType, Distribution, ExecutionPlan, Partitioning, PhysicalExpr,
    RecordBatchStream, SendableRecordBatchStream, Statistics,
};

use super::{
    utils::{OnceAsync, OnceFut},
    PartitionMode,
};

use arrow::array::{
    Array, ArrayRef, BooleanArray, BooleanBufferBuilder, PrimitiveArray, UInt32Array,
    UInt32BufferBuilder, UInt64Array, UInt64BufferBuilder,
};
use arrow::compute::kernels::cmp::{eq, not_distinct};
use arrow::compute::{and, take, FilterBuilder};
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use arrow::util::bit_util;
use arrow_array::cast::downcast_array;
use arrow_schema::ArrowError;
use datafusion_common::{
    exec_err, internal_err, plan_err, DataFusionError, JoinSide, JoinType, Result,
};
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion_execution::TaskContext;
use datafusion_physical_expr::equivalence::join_equivalence_properties;
use datafusion_physical_expr::EquivalenceProperties;

use ahash::RandomState;
use futures::{ready, Stream, StreamExt, TryStreamExt};

type JoinLeftData = (JoinHashMap, RecordBatch, MemoryReservation);

/// Join execution plan: Evaluates eqijoin predicates in parallel on multiple
/// partitions using a hash table and an optional filter list to apply post
/// join.
///
/// # Join Expressions
///
/// This implementation is optimized for evaluating eqijoin predicates  (
/// `<col1> = <col2>`) expressions, which are represented as a list of `Columns`
/// in [`Self::on`].
///
/// Non-equality predicates, which can not pushed down to a join inputs (e.g.
/// `<col1> != <col2>`) are known as "filter expressions" and are evaluated
/// after the equijoin predicates.
///
/// # "Build Side" vs "Probe Side"
///
/// HashJoin takes two inputs, which are referred to as the "build" and the
/// "probe". The build side is the first child, and the probe side is the second
/// child.
///
/// The two inputs are treated differently and it is VERY important that the
/// *smaller* input is placed on the build side to minimize the work of creating
/// the hash table.
///
/// ```text
///          ┌───────────┐
///          │ HashJoin  │
///          │           │
///          └───────────┘
///              │   │
///        ┌─────┘   └─────┐
///        ▼               ▼
/// ┌────────────┐  ┌─────────────┐
/// │   Input    │  │    Input    │
/// │    [0]     │  │     [1]     │
/// └────────────┘  └─────────────┘
///
///  "build side"    "probe side"
/// ```
///
/// Execution proceeds in 2 stages:
///
/// 1. the **build phase** where a hash table is created from the tuples of the
/// build side.
///
/// 2. the **probe phase** where the tuples of the probe side are streamed
/// through, checking for matches of the join keys in the hash table.
///
/// ```text
///                 ┌────────────────┐          ┌────────────────┐
///                 │ ┌─────────┐    │          │ ┌─────────┐    │
///                 │ │  Hash   │    │          │ │  Hash   │    │
///                 │ │  Table  │    │          │ │  Table  │    │
///                 │ │(keys are│    │          │ │(keys are│    │
///                 │ │equi join│    │          │ │equi join│    │  Stage 2: batches from
///  Stage 1: the   │ │columns) │    │          │ │columns) │    │    the probe side are
/// *entire* build  │ │         │    │          │ │         │    │  streamed through, and
///  side is read   │ └─────────┘    │          │ └─────────┘    │   checked against the
/// into the hash   │      ▲         │          │          ▲     │   contents of the hash
///     table       │       HashJoin │          │  HashJoin      │          table
///                 └──────┼─────────┘          └──────────┼─────┘
///             ─ ─ ─ ─ ─ ─                                 ─ ─ ─ ─ ─ ─ ─
///            │                                                         │
///
///            │                                                         │
///     ┌────────────┐                                            ┌────────────┐
///     │RecordBatch │                                            │RecordBatch │
///     └────────────┘                                            └────────────┘
///     ┌────────────┐                                            ┌────────────┐
///     │RecordBatch │                                            │RecordBatch │
///     └────────────┘                                            └────────────┘
///           ...                                                       ...
///     ┌────────────┐                                            ┌────────────┐
///     │RecordBatch │                                            │RecordBatch │
///     └────────────┘                                            └────────────┘
///
///        build side                                                probe side
///
/// ```
///
/// # Example "Optimal" Plans
///
/// The differences in the inputs means that for classic "Star Schema Query",
/// the optimal plan will be a **"Right Deep Tree"** . A Star Schema Query is
/// one where there is one large table and several smaller "dimension" tables,
/// joined on `Foreign Key = Primary Key` predicates.
///
/// A "Right Deep Tree" looks like this large table as the probe side on the
/// lowest join:
///
/// ```text
///             ┌───────────┐
///             │ HashJoin  │
///             │           │
///             └───────────┘
///                 │   │
///         ┌───────┘   └──────────┐
///         ▼                      ▼
/// ┌───────────────┐        ┌───────────┐
/// │ small table 1 │        │ HashJoin  │
/// │  "dimension"  │        │           │
/// └───────────────┘        └───┬───┬───┘
///                   ┌──────────┘   └───────┐
///                   │                      │
///                   ▼                      ▼
///           ┌───────────────┐        ┌───────────┐
///           │ small table 2 │        │ HashJoin  │
///           │  "dimension"  │        │           │
///           └───────────────┘        └───┬───┬───┘
///                               ┌────────┘   └────────┐
///                               │                     │
///                               ▼                     ▼
///                       ┌───────────────┐     ┌───────────────┐
///                       │ small table 3 │     │  large table  │
///                       │  "dimension"  │     │    "fact"     │
///                       └───────────────┘     └───────────────┘
/// ```
#[derive(Debug)]
pub struct HashJoinExec {
    /// left (build) side which gets hashed
    pub left: Arc<dyn ExecutionPlan>,
    /// right (probe) side which are filtered by the hash table
    pub right: Arc<dyn ExecutionPlan>,
    /// Set of equijoin columns from the relations: `(left_col, right_col)`
    pub on: Vec<(Column, Column)>,
    /// Filters which are applied while finding matching rows
    pub filter: Option<JoinFilter>,
    /// How the join is performed (`OUTER`, `INNER`, etc)
    pub join_type: JoinType,
    /// The output schema for the join
    schema: SchemaRef,
    /// Future that consumes left input and builds the hash table
    left_fut: OnceAsync<JoinLeftData>,
    /// Shared the `RandomState` for the hashing algorithm
    random_state: RandomState,
    /// Output order
    output_order: Option<Vec<PhysicalSortExpr>>,
    /// Partitioning mode to use
    pub mode: PartitionMode,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// Information of index and left / right placement of columns
    column_indices: Vec<ColumnIndex>,
    /// Null matching behavior: If `null_equals_null` is true, rows that have
    /// `null`s in both left and right equijoin columns will be matched.
    /// Otherwise, rows that have `null`s in the join columns will not be
    /// matched and thus will not appear in the output.
    pub null_equals_null: bool,
}

impl HashJoinExec {
    /// Tries to create a new [HashJoinExec].
    ///
    /// # Error
    /// This function errors when it is not possible to join the left and right sides on keys `on`.
    pub fn try_new(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: JoinOn,
        filter: Option<JoinFilter>,
        join_type: &JoinType,
        partition_mode: PartitionMode,
        null_equals_null: bool,
    ) -> Result<Self> {
        let left_schema = left.schema();
        let right_schema = right.schema();
        if on.is_empty() {
            return plan_err!("On constraints in HashJoinExec should be non-empty");
        }

        check_join_is_valid(&left_schema, &right_schema, &on)?;

        let (schema, column_indices) =
            build_join_schema(&left_schema, &right_schema, join_type);

        let random_state = RandomState::with_seeds(0, 0, 0, 0);

        let output_order = calculate_join_output_ordering(
            left.output_ordering().unwrap_or(&[]),
            right.output_ordering().unwrap_or(&[]),
            *join_type,
            &on,
            left_schema.fields.len(),
            &Self::maintains_input_order(*join_type),
            Some(Self::probe_side()),
        );

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
            null_equals_null,
            output_order,
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
    pub fn null_equals_null(&self) -> bool {
        self.null_equals_null
    }

    /// Calculate order preservation flags for this hash join.
    fn maintains_input_order(join_type: JoinType) -> Vec<bool> {
        vec![
            false,
            matches!(
                join_type,
                JoinType::Inner | JoinType::RightAnti | JoinType::RightSemi
            ),
        ]
    }

    /// Get probe side information for the hash join.
    pub fn probe_side() -> JoinSide {
        // In current implementation right side is always probe side.
        JoinSide::Right
    }
}

impl DisplayAs for HashJoinExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                let display_filter = self.filter.as_ref().map_or_else(
                    || "".to_string(),
                    |f| format!(", filter={}", f.expression()),
                );
                let on = self
                    .on
                    .iter()
                    .map(|(c1, c2)| format!("({}, {})", c1, c2))
                    .collect::<Vec<String>>()
                    .join(", ");
                write!(
                    f,
                    "HashJoinExec: mode={:?}, join_type={:?}, on=[{}]{}",
                    self.mode, self.join_type, on, display_filter
                )
            }
        }
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

    /// Specifies whether this plan generates an infinite stream of records.
    /// If the plan does not support pipelining, but its input(s) are
    /// infinite, returns an error to indicate this.
    fn unbounded_output(&self, children: &[bool]) -> Result<bool> {
        let (left, right) = (children[0], children[1]);
        // If left is unbounded, or right is unbounded with JoinType::Right,
        // JoinType::Full, JoinType::RightAnti types.
        let breaking = left
            || (right
                && matches!(
                    self.join_type,
                    JoinType::Left
                        | JoinType::Full
                        | JoinType::LeftAnti
                        | JoinType::LeftSemi
                ));

        if breaking {
            plan_err!(
                "Join Error: The join with cannot be executed with unbounded inputs. {}",
                if left && right {
                    "Currently, we do not support unbounded inputs on both sides."
                } else {
                    "Please consider a different type of join or sources."
                }
            )
        } else {
            Ok(left || right)
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

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        self.output_order.as_deref()
    }

    // For [JoinType::Inner] and [JoinType::RightSemi] in hash joins, the probe phase initiates by
    // applying the hash function to convert the join key(s) in each row into a hash value from the
    // probe side table in the order they're arranged. The hash value is used to look up corresponding
    // entries in the hash table that was constructed from the build side table during the build phase.
    //
    // Because of the immediate generation of result rows once a match is found,
    // the output of the join tends to follow the order in which the rows were read from
    // the probe side table. This is simply due to the sequence in which the rows were processed.
    // Hence, it appears that the hash join is preserving the order of the probe side.
    //
    // Meanwhile, in the case of a [JoinType::RightAnti] hash join,
    // the unmatched rows from the probe side are also kept in order.
    // This is because the **`RightAnti`** join is designed to return rows from the right
    // (probe side) table that have no match in the left (build side) table. Because the rows
    // are processed sequentially in the probe phase, and unmatched rows are directly output
    // as results, these results tend to retain the order of the probe side table.
    fn maintains_input_order(&self) -> Vec<bool> {
        Self::maintains_input_order(self.join_type)
    }

    fn equivalence_properties(&self) -> EquivalenceProperties {
        join_equivalence_properties(
            self.left.equivalence_properties(),
            self.right.equivalence_properties(),
            &self.join_type,
            self.schema(),
            &self.maintains_input_order(),
            Some(Self::probe_side()),
            self.on(),
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
            self.null_equals_null,
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let on_left = self.on.iter().map(|on| on.0.clone()).collect::<Vec<_>>();
        let on_right = self.on.iter().map(|on| on.1.clone()).collect::<Vec<_>>();
        let left_partitions = self.left.output_partitioning().partition_count();
        let right_partitions = self.right.output_partitioning().partition_count();

        if self.mode == PartitionMode::Partitioned && left_partitions != right_partitions
        {
            return internal_err!(
                "Invalid HashJoinExec, partition count mismatch {left_partitions}!={right_partitions},\
                 consider using RepartitionExec"
            );
        }

        let join_metrics = BuildProbeJoinMetrics::new(partition, &self.metrics);
        let left_fut = match self.mode {
            PartitionMode::CollectLeft => self.left_fut.once(|| {
                let reservation =
                    MemoryConsumer::new("HashJoinInput").register(context.memory_pool());
                collect_left_input(
                    None,
                    self.random_state.clone(),
                    self.left.clone(),
                    on_left.clone(),
                    context.clone(),
                    join_metrics.clone(),
                    reservation,
                )
            }),
            PartitionMode::Partitioned => {
                let reservation =
                    MemoryConsumer::new(format!("HashJoinInput[{partition}]"))
                        .register(context.memory_pool());

                OnceFut::new(collect_left_input(
                    Some(partition),
                    self.random_state.clone(),
                    self.left.clone(),
                    on_left.clone(),
                    context.clone(),
                    join_metrics.clone(),
                    reservation,
                ))
            }
            PartitionMode::Auto => {
                return plan_err!(
                    "Invalid HashJoinExec, unsupported PartitionMode {:?} in execute()",
                    PartitionMode::Auto
                );
            }
        };

        let reservation = MemoryConsumer::new(format!("HashJoinStream[{partition}]"))
            .register(context.memory_pool());

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
            join_metrics,
            null_equals_null: self.null_equals_null,
            is_exhausted: false,
            reservation,
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        // TODO stats: it is not possible in general to know the output size of joins
        // There are some special cases though, for example:
        // - `A LEFT JOIN B ON A.col=B.col` with `COUNT_DISTINCT(B.col)=COUNT(B.col)`
        estimate_join_statistics(
            self.left.clone(),
            self.right.clone(),
            self.on.clone(),
            &self.join_type,
            &self.schema,
        )
    }
}

async fn collect_left_input(
    partition: Option<usize>,
    random_state: RandomState,
    left: Arc<dyn ExecutionPlan>,
    on_left: Vec<Column>,
    context: Arc<TaskContext>,
    metrics: BuildProbeJoinMetrics,
    reservation: MemoryReservation,
) -> Result<JoinLeftData> {
    let schema = left.schema();

    let (left_input, left_input_partition) = if let Some(partition) = partition {
        (left, partition)
    } else if left.output_partitioning().partition_count() != 1 {
        (Arc::new(CoalescePartitionsExec::new(left)) as _, 0)
    } else {
        (left, 0)
    };

    // Depending on partition argument load single partition or whole left side in memory
    let stream = left_input.execute(left_input_partition, context.clone())?;

    // This operation performs 2 steps at once:
    // 1. creates a [JoinHashMap] of all batches from the stream
    // 2. stores the batches in a vector.
    let initial = (Vec::new(), 0, metrics, reservation);
    let (batches, num_rows, metrics, mut reservation) = stream
        .try_fold(initial, |mut acc, batch| async {
            let batch_size = batch.get_array_memory_size();
            // Reserve memory for incoming batch
            acc.3.try_grow(batch_size)?;
            // Update metrics
            acc.2.build_mem_used.add(batch_size);
            acc.2.build_input_batches.add(1);
            acc.2.build_input_rows.add(batch.num_rows());
            // Update rowcount
            acc.1 += batch.num_rows();
            // Push batch to output
            acc.0.push(batch);
            Ok(acc)
        })
        .await?;

    // Estimation of memory size, required for hashtable, prior to allocation.
    // Final result can be verified using `RawTable.allocation_info()`
    //
    // For majority of cases hashbrown overestimates buckets qty to keep ~1/8 of them empty.
    // This formula leads to overallocation for small tables (< 8 elements) but fine overall.
    let estimated_buckets = (num_rows.checked_mul(8).ok_or_else(|| {
        DataFusionError::Execution(
            "usize overflow while estimating number of hasmap buckets".to_string(),
        )
    })? / 7)
        .next_power_of_two();
    // 16 bytes per `(u64, u64)`
    // + 1 byte for each bucket
    // + fixed size of JoinHashMap (RawTable + Vec)
    let estimated_hastable_size =
        16 * estimated_buckets + estimated_buckets + size_of::<JoinHashMap>();

    reservation.try_grow(estimated_hastable_size)?;
    metrics.build_mem_used.add(estimated_hastable_size);

    let mut hashmap = JoinHashMap::with_capacity(num_rows);
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
            0,
        )?;
        offset += batch.num_rows();
    }
    // Merge all batches into a single batch, so we
    // can directly index into the arrays
    let single_batch = concat_batches(&schema, &batches, num_rows)?;

    Ok((hashmap, single_batch, reservation))
}

/// Updates `hash` with new entries from [RecordBatch] evaluated against the expressions `on`,
/// assuming that the [RecordBatch] corresponds to the `index`th
pub fn update_hash<T>(
    on: &[Column],
    batch: &RecordBatch,
    hash_map: &mut T,
    offset: usize,
    random_state: &RandomState,
    hashes_buffer: &mut Vec<u64>,
    deleted_offset: usize,
) -> Result<()>
where
    T: JoinHashMapType,
{
    // evaluate the keys
    let keys_values = on
        .iter()
        .map(|c| c.evaluate(batch)?.into_array(batch.num_rows()))
        .collect::<Result<Vec<_>>>()?;

    // calculate the hash values
    let hash_values = create_hashes(&keys_values, random_state, hashes_buffer)?;

    // For usual JoinHashmap, the implementation is void.
    hash_map.extend_zero(batch.num_rows());

    // insert hashes to key of the hashmap
    let (mut_map, mut_list) = hash_map.get_mut();
    for (row, hash_value) in hash_values.iter().enumerate() {
        let item = mut_map.get_mut(*hash_value, |(hash, _)| *hash_value == *hash);
        if let Some((_, index)) = item {
            // Already exists: add index to next array
            let prev_index = *index;
            // Store new value inside hashmap
            *index = (row + offset + 1) as u64;
            // Update chained Vec at row + offset with previous value
            mut_list[row + offset - deleted_offset] = prev_index;
        } else {
            mut_map.insert(
                *hash_value,
                // store the value + 1 as 0 value reserved for end of list
                (*hash_value, (row + offset + 1) as u64),
                |(hash, _)| *hash,
            );
            // chained list at (row + offset) is already initialized with 0
            // meaning end of list
        }
    }
    Ok(())
}

/// [`Stream`] for [`HashJoinExec`] that does the actual join.
///
/// This stream:
///
/// 1. Reads the entire left input (build) and constructs a hash table
///
/// 2. Streams [RecordBatch]es as they arrive from the right input (probe) and joins
/// them with the contents of the hash table
struct HashJoinStream {
    /// Input schema
    schema: Arc<Schema>,
    /// equijoin columns from the left (build side)
    on_left: Vec<Column>,
    /// equijoin columns from the right (probe side)
    on_right: Vec<Column>,
    /// optional join filter
    filter: Option<JoinFilter>,
    /// type of the join (left, right, semi, etc)
    join_type: JoinType,
    /// future which builds hash table from left side
    left_fut: OnceFut<JoinLeftData>,
    /// Which left (probe) side rows have been matches while creating output.
    /// For some OUTER joins, we need to know which rows have not been matched
    /// to produce the correct.
    visited_left_side: Option<BooleanBufferBuilder>,
    /// right (probe) input
    right: SendableRecordBatchStream,
    /// Random state used for hashing initialization
    random_state: RandomState,
    /// The join output is complete. For outer joins, this is used to
    /// distinguish when the input stream is exhausted and when any unmatched
    /// rows are output.
    is_exhausted: bool,
    /// Metrics
    join_metrics: BuildProbeJoinMetrics,
    /// Information of index and left / right placement of columns
    column_indices: Vec<ColumnIndex>,
    /// If null_equals_null is true, null == null else null != null
    null_equals_null: bool,
    /// Memory reservation
    reservation: MemoryReservation,
}

impl RecordBatchStream for HashJoinStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// Returns build/probe indices satisfying the equality condition.
///
/// # Example
///
/// For `LEFT.b1 = RIGHT.b2`:
/// LEFT Table:
/// ```text
///  a1  b1  c1
///  1   1   10
///  3   3   30
///  5   5   50
///  7   7   70
///  9   8   90
///  11  8   110
///  13   10  130
/// ```
///
/// RIGHT Table:
/// ```text
///  a2   b2  c2
///  2    2   20
///  4    4   40
///  6    6   60
///  8    8   80
/// 10   10  100
/// 12   10  120
/// ```
///
/// The result is
/// ```text
/// "+----+----+-----+----+----+-----+",
/// "| a1 | b1 | c1  | a2 | b2 | c2  |",
/// "+----+----+-----+----+----+-----+",
/// "| 9  | 8  | 90  | 8  | 8  | 80  |",
/// "| 11 | 8  | 110 | 8  | 8  | 80  |",
/// "| 13 | 10 | 130 | 10 | 10 | 100 |",
/// "| 13 | 10 | 130 | 12 | 10 | 120 |",
/// "+----+----+-----+----+----+-----+"
/// ```
///
/// And the result of build and probe indices are:
/// ```text
/// Build indices: 4, 5, 6, 6
/// Probe indices: 3, 3, 4, 5
/// ```
#[allow(clippy::too_many_arguments)]
pub fn build_equal_condition_join_indices<T: JoinHashMapType>(
    build_hashmap: &T,
    build_input_buffer: &RecordBatch,
    probe_batch: &RecordBatch,
    build_on: &[Column],
    probe_on: &[Column],
    random_state: &RandomState,
    null_equals_null: bool,
    hashes_buffer: &mut Vec<u64>,
    filter: Option<&JoinFilter>,
    build_side: JoinSide,
    deleted_offset: Option<usize>,
) -> Result<(UInt64Array, UInt32Array)> {
    let keys_values = probe_on
        .iter()
        .map(|c| c.evaluate(probe_batch)?.into_array(probe_batch.num_rows()))
        .collect::<Result<Vec<_>>>()?;
    let build_join_values = build_on
        .iter()
        .map(|c| {
            c.evaluate(build_input_buffer)?
                .into_array(build_input_buffer.num_rows())
        })
        .collect::<Result<Vec<_>>>()?;
    hashes_buffer.clear();
    hashes_buffer.resize(probe_batch.num_rows(), 0);
    let hash_values = create_hashes(&keys_values, random_state, hashes_buffer)?;
    // Using a buffer builder to avoid slower normal builder
    let mut build_indices = UInt64BufferBuilder::new(0);
    let mut probe_indices = UInt32BufferBuilder::new(0);
    // The chained list algorithm generates build indices for each probe row in a reversed sequence as such:
    // Build Indices: [5, 4, 3]
    // Probe Indices: [1, 1, 1]
    //
    // This affects the output sequence. Hypothetically, it's possible to preserve the lexicographic order on the build side.
    // Let's consider probe rows [0,1] as an example:
    //
    // When the probe iteration sequence is reversed, the following pairings can be derived:
    //
    // For probe row 1:
    //     (5, 1)
    //     (4, 1)
    //     (3, 1)
    //
    // For probe row 0:
    //     (5, 0)
    //     (4, 0)
    //     (3, 0)
    //
    // After reversing both sets of indices, we obtain reversed indices:
    //
    //     (3,0)
    //     (4,0)
    //     (5,0)
    //     (3,1)
    //     (4,1)
    //     (5,1)
    //
    // With this approach, the lexicographic order on both the probe side and the build side is preserved.
    let hash_map = build_hashmap.get_map();
    let next_chain = build_hashmap.get_list();
    for (row, hash_value) in hash_values.iter().enumerate().rev() {
        // Get the hash and find it in the build index

        // For every item on the build and probe we check if it matches
        // This possibly contains rows with hash collisions,
        // So we have to check here whether rows are equal or not
        if let Some((_, index)) =
            hash_map.get(*hash_value, |(hash, _)| *hash_value == *hash)
        {
            let mut i = *index - 1;
            loop {
                let build_row_value = if let Some(offset) = deleted_offset {
                    // This arguments means that we prune the next index way before here.
                    if i < offset as u64 {
                        // End of the list due to pruning
                        break;
                    }
                    i - offset as u64
                } else {
                    i
                };
                build_indices.append(build_row_value);
                probe_indices.append(row as u32);
                // Follow the chain to get the next index value
                let next = next_chain[build_row_value as usize];
                if next == 0 {
                    // end of list
                    break;
                }
                i = next - 1;
            }
        }
    }
    // Reversing both sets of indices
    build_indices.as_slice_mut().reverse();
    probe_indices.as_slice_mut().reverse();

    let left: UInt64Array = PrimitiveArray::new(build_indices.finish().into(), None);
    let right: UInt32Array = PrimitiveArray::new(probe_indices.finish().into(), None);

    let (left, right) = if let Some(filter) = filter {
        // Filter the indices which satisfy the non-equal join condition, like `left.b1 = 10`
        apply_join_filter_to_indices(
            build_input_buffer,
            probe_batch,
            left,
            right,
            filter,
            build_side,
        )?
    } else {
        (left, right)
    };

    equal_rows_arr(
        &left,
        &right,
        &build_join_values,
        &keys_values,
        null_equals_null,
    )
}

// version of eq_dyn supporting equality on null arrays
fn eq_dyn_null(
    left: &dyn Array,
    right: &dyn Array,
    null_equals_null: bool,
) -> Result<BooleanArray, ArrowError> {
    match (left.data_type(), right.data_type()) {
        _ if null_equals_null => not_distinct(&left, &right),
        _ => eq(&left, &right),
    }
}

pub fn equal_rows_arr(
    indices_left: &UInt64Array,
    indices_right: &UInt32Array,
    left_arrays: &[ArrayRef],
    right_arrays: &[ArrayRef],
    null_equals_null: bool,
) -> Result<(UInt64Array, UInt32Array)> {
    let mut iter = left_arrays.iter().zip(right_arrays.iter());

    let (first_left, first_right) = iter.next().ok_or_else(|| {
        DataFusionError::Internal(
            "At least one array should be provided for both left and right".to_string(),
        )
    })?;

    let arr_left = take(first_left.as_ref(), indices_left, None)?;
    let arr_right = take(first_right.as_ref(), indices_right, None)?;

    let mut equal: BooleanArray = eq_dyn_null(&arr_left, &arr_right, null_equals_null)?;

    // Use map and try_fold to iterate over the remaining pairs of arrays.
    // In each iteration, take is used on the pair of arrays and their equality is determined.
    // The results are then folded (combined) using the and function to get a final equality result.
    equal = iter
        .map(|(left, right)| {
            let arr_left = take(left.as_ref(), indices_left, None)?;
            let arr_right = take(right.as_ref(), indices_right, None)?;
            eq_dyn_null(arr_left.as_ref(), arr_right.as_ref(), null_equals_null)
        })
        .try_fold(equal, |acc, equal2| and(&acc, &equal2?))?;

    let filter_builder = FilterBuilder::new(&equal).optimize().build();

    let left_filtered = filter_builder.filter(indices_left)?;
    let right_filtered = filter_builder.filter(indices_right)?;

    Ok((
        downcast_array(left_filtered.as_ref()),
        downcast_array(right_filtered.as_ref()),
    ))
}

impl HashJoinStream {
    /// Separate implementation function that unpins the [`HashJoinStream`] so
    /// that partial borrows work correctly
    fn poll_next_impl(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        let build_timer = self.join_metrics.build_time.timer();
        // build hash table from left (build) side, if not yet done
        let left_data = match ready!(self.left_fut.get(cx)) {
            Ok(left_data) => left_data,
            Err(e) => return Poll::Ready(Some(Err(e))),
        };
        build_timer.done();

        // Reserving memory for visited_left_side bitmap in case it hasn't been initialized yet
        // and join_type requires to store it
        if self.visited_left_side.is_none()
            && need_produce_result_in_final(self.join_type)
        {
            // TODO: Replace `ceil` wrapper with stable `div_cell` after
            // https://github.com/rust-lang/rust/issues/88581
            let visited_bitmap_size = bit_util::ceil(left_data.1.num_rows(), 8);
            self.reservation.try_grow(visited_bitmap_size)?;
            self.join_metrics.build_mem_used.add(visited_bitmap_size);
        }

        let visited_left_side = self.visited_left_side.get_or_insert_with(|| {
            let num_rows = left_data.1.num_rows();
            if need_produce_result_in_final(self.join_type) {
                // Some join types need to track which row has be matched or unmatched:
                // `left semi` join:  need to use the bitmap to produce the matched row in the left side
                // `left` join:       need to use the bitmap to produce the unmatched row in the left side with null
                // `left anti` join:  need to use the bitmap to produce the unmatched row in the left side
                // `full` join:       need to use the bitmap to produce the unmatched row in the left side with null
                let mut buffer = BooleanBufferBuilder::new(num_rows);
                buffer.append_n(num_rows, false);
                buffer
            } else {
                BooleanBufferBuilder::new(0)
            }
        });
        let mut hashes_buffer = vec![];
        // get next right (probe) input batch
        self.right
            .poll_next_unpin(cx)
            .map(|maybe_batch| match maybe_batch {
                // one right batch in the join loop
                Some(Ok(batch)) => {
                    self.join_metrics.input_batches.add(1);
                    self.join_metrics.input_rows.add(batch.num_rows());
                    let timer = self.join_metrics.join_time.timer();

                    // get the matched two indices for the on condition
                    let left_right_indices = build_equal_condition_join_indices(
                        &left_data.0,
                        &left_data.1,
                        &batch,
                        &self.on_left,
                        &self.on_right,
                        &self.random_state,
                        self.null_equals_null,
                        &mut hashes_buffer,
                        self.filter.as_ref(),
                        JoinSide::Left,
                        None,
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
                                &left_side,
                                &right_side,
                                &self.column_indices,
                                JoinSide::Left,
                            );
                            self.join_metrics.output_batches.add(1);
                            self.join_metrics.output_rows.add(batch.num_rows());
                            Some(result)
                        }
                        Err(err) => Some(exec_err!(
                            "Fail to build join indices in HashJoinExec, error:{err}"
                        )),
                    };
                    timer.done();
                    result
                }
                None => {
                    let timer = self.join_metrics.join_time.timer();
                    if need_produce_result_in_final(self.join_type) && !self.is_exhausted
                    {
                        // use the global left bitmap to produce the left indices and right indices
                        let (left_side, right_side) = get_final_indices_from_bit_map(
                            visited_left_side,
                            self.join_type,
                        );
                        let empty_right_batch =
                            RecordBatch::new_empty(self.right.schema());
                        // use the left and right indices to produce the batch result
                        let result = build_batch_from_indices(
                            &self.schema,
                            &left_data.1,
                            &empty_right_batch,
                            &left_side,
                            &right_side,
                            &self.column_indices,
                            JoinSide::Left,
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
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.poll_next_impl(cx)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::{
        common, expressions::Column, hash_utils::create_hashes,
        joins::hash_join::build_equal_condition_join_indices, memory::MemoryExec,
        repartition::RepartitionExec, test::build_table_i32, test::exec::MockExec,
    };

    use arrow::array::{ArrayRef, Date32Array, Int32Array, UInt32Builder, UInt64Builder};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::{assert_batches_sorted_eq, assert_contains, ScalarValue};
    use datafusion_execution::config::SessionConfig;
    use datafusion_execution::runtime_env::{RuntimeConfig, RuntimeEnv};
    use datafusion_expr::Operator;
    use datafusion_physical_expr::expressions::{BinaryExpr, Literal};

    use hashbrown::raw::RawTable;

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
            null_equals_null,
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
            null_equals_null,
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
            null_equals_null,
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
        let task_ctx = Arc::new(TaskContext::default());
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

        let expected = [
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
        let task_ctx = Arc::new(TaskContext::default());
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

        let expected = [
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
        let task_ctx = Arc::new(TaskContext::default());
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

        let expected = [
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
        let task_ctx = Arc::new(TaskContext::default());
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

        let expected = [
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
        let task_ctx = Arc::new(TaskContext::default());
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

        let expected = [
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
        let task_ctx = Arc::new(TaskContext::default());
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

        let expected = [
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
        let expected = [
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
        let task_ctx = Arc::new(TaskContext::default());
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

        let expected = [
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
        let task_ctx = Arc::new(TaskContext::default());
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

        let expected = [
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
        let task_ctx = Arc::new(TaskContext::default());
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

        let expected = [
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
        let task_ctx = Arc::new(TaskContext::default());
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

        let expected = [
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
        let task_ctx = Arc::new(TaskContext::default());
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

        let expected = [
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
        let task_ctx = Arc::new(TaskContext::default());
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

        let expected = [
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
        let task_ctx = Arc::new(TaskContext::default());
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
        let expected = [
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
        let task_ctx = Arc::new(TaskContext::default());
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

        let expected = [
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

        let expected = [
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
        let task_ctx = Arc::new(TaskContext::default());
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

        let expected = [
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
        let task_ctx = Arc::new(TaskContext::default());
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

        let expected = [
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

        let expected = [
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
        let task_ctx = Arc::new(TaskContext::default());
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

        let expected = [
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
        let task_ctx = Arc::new(TaskContext::default());
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

        let expected = [
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

        let expected = [
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
        let task_ctx = Arc::new(TaskContext::default());
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

        let expected = [
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
        let task_ctx = Arc::new(TaskContext::default());
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

        let expected = [
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

        let expected = [
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
        let task_ctx = Arc::new(TaskContext::default());
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

        let expected = [
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
        let task_ctx = Arc::new(TaskContext::default());
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

        let expected = [
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
        let task_ctx = Arc::new(TaskContext::default());
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

        let expected = [
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
        hashmap_left.insert(hashes[0], (hashes[0], 1), |(h, _)| *h);
        hashmap_left.insert(hashes[1], (hashes[1], 1), |(h, _)| *h);

        let next = vec![2, 0];

        let right = build_table_i32(
            ("a", &vec![10, 20]),
            ("b", &vec![0, 0]),
            ("c", &vec![30, 40]),
        );

        let left_data = (
            JoinHashMap {
                map: hashmap_left,
                next,
            },
            left,
        );
        let (l, r) = build_equal_condition_join_indices(
            &left_data.0,
            &left_data.1,
            &right,
            &[Column::new("a", 0)],
            &[Column::new("a", 0)],
            &random_state,
            false,
            &mut vec![0; right.num_rows()],
            None,
            JoinSide::Left,
            None,
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
        let task_ctx = Arc::new(TaskContext::default());
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

        let expected = [
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
        let task_ctx = Arc::new(TaskContext::default());
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

        let expected = [
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
        let task_ctx = Arc::new(TaskContext::default());
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

        let expected = [
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
        let task_ctx = Arc::new(TaskContext::default());
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

        let expected = [
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
        let task_ctx = Arc::new(TaskContext::default());
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

        let expected = [
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

        let task_ctx = Arc::new(TaskContext::default());
        let stream = join.execute(0, task_ctx)?;
        let batches = common::collect(stream).await?;

        let expected = [
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
        let err = exec_err!("bad data error");
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
            let task_ctx = Arc::new(TaskContext::default());

            let stream = join.execute(0, task_ctx).unwrap();

            // Expect that an error is returned
            let result_string = crate::common::collect(stream)
                .await
                .unwrap_err()
                .to_string();
            assert!(
                result_string.contains("bad data error"),
                "actual: {result_string}"
            );
        }
    }

    #[tokio::test]
    async fn single_partition_join_overallocation() -> Result<()> {
        let left = build_table(
            ("a1", &vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 0]),
            ("b1", &vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 0]),
            ("c1", &vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 0]),
        );
        let right = build_table(
            ("a2", &vec![10, 11]),
            ("b2", &vec![12, 13]),
            ("c2", &vec![14, 15]),
        );
        let on = vec![(
            Column::new_with_schema("a1", &left.schema()).unwrap(),
            Column::new_with_schema("b2", &right.schema()).unwrap(),
        )];

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
            let runtime_config = RuntimeConfig::new().with_memory_limit(100, 1.0);
            let runtime = Arc::new(RuntimeEnv::new(runtime_config)?);
            let task_ctx = TaskContext::default().with_runtime(runtime);
            let task_ctx = Arc::new(task_ctx);

            let join = join(left.clone(), right.clone(), on.clone(), &join_type, false)?;

            let stream = join.execute(0, task_ctx)?;
            let err = common::collect(stream).await.unwrap_err();

            assert_contains!(
                err.to_string(),
                "External error: Resources exhausted: Failed to allocate additional"
            );

            // Asserting that operator-level reservation attempting to overallocate
            assert_contains!(err.to_string(), "HashJoinInput");
        }

        Ok(())
    }

    #[tokio::test]
    async fn partitioned_join_overallocation() -> Result<()> {
        // Prepare partitioned inputs for HashJoinExec
        // No need to adjust partitioning, as execution should fail with `Resources exhausted` error
        let left_batch = build_table_i32(
            ("a1", &vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 0]),
            ("b1", &vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 0]),
            ("c1", &vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 0]),
        );
        let left = Arc::new(
            MemoryExec::try_new(
                &[vec![left_batch.clone()], vec![left_batch.clone()]],
                left_batch.schema(),
                None,
            )
            .unwrap(),
        );
        let right_batch = build_table_i32(
            ("a2", &vec![10, 11]),
            ("b2", &vec![12, 13]),
            ("c2", &vec![14, 15]),
        );
        let right = Arc::new(
            MemoryExec::try_new(
                &[vec![right_batch.clone()], vec![right_batch.clone()]],
                right_batch.schema(),
                None,
            )
            .unwrap(),
        );
        let on = vec![(
            Column::new_with_schema("b1", &left_batch.schema())?,
            Column::new_with_schema("b2", &right_batch.schema())?,
        )];

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
            let runtime_config = RuntimeConfig::new().with_memory_limit(100, 1.0);
            let runtime = Arc::new(RuntimeEnv::new(runtime_config)?);
            let session_config = SessionConfig::default().with_batch_size(50);
            let task_ctx = TaskContext::default()
                .with_session_config(session_config)
                .with_runtime(runtime);
            let task_ctx = Arc::new(task_ctx);

            let join = HashJoinExec::try_new(
                left.clone(),
                right.clone(),
                on.clone(),
                None,
                &join_type,
                PartitionMode::Partitioned,
                false,
            )?;

            let stream = join.execute(1, task_ctx)?;
            let err = common::collect(stream).await.unwrap_err();

            assert_contains!(
                err.to_string(),
                "External error: Resources exhausted: Failed to allocate additional"
            );

            // Asserting that stream-level reservation attempting to overallocate
            assert_contains!(err.to_string(), "HashJoinInput[1]");
        }

        Ok(())
    }

    /// Returns the column names on the schema
    fn columns(schema: &Schema) -> Vec<String> {
        schema.fields().iter().map(|f| f.name().clone()).collect()
    }
}
