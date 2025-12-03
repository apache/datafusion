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

use std::fmt;
use std::mem::size_of;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::{any::Any, vec};

use crate::execution_plan::{boundedness_from_children, EmissionType};
use crate::filter_pushdown::{
    ChildPushdownResult, FilterDescription, FilterPushdownPhase,
    FilterPushdownPropagation,
};
use crate::joins::hash_join::shared_bounds::{
    ColumnBounds, PartitionBounds, SharedBuildAccumulator,
};
use crate::joins::hash_join::stream::{
    BuildSide, BuildSideInitialState, HashJoinStream, HashJoinStreamState,
};
use crate::joins::join_hash_map::{JoinHashMapU32, JoinHashMapU64};
use crate::joins::utils::{
    asymmetric_join_output_partitioning, reorder_output_after_swap, swap_join_projection,
    update_hash, OnceAsync, OnceFut,
};
use crate::joins::{JoinOn, JoinOnRef, PartitionMode, SharedBitmapBuilder};
use crate::projection::{
    try_embed_projection, try_pushdown_through_join, EmbeddedProjection, JoinData,
    ProjectionExec,
};
use crate::repartition::REPARTITION_RANDOM_STATE;
use crate::spill::get_record_batch_memory_size;
use crate::ExecutionPlanProperties;
use crate::{
    common::can_project,
    joins::utils::{
        build_join_schema, check_join_is_valid, estimate_join_statistics,
        need_produce_result_in_final, symmetric_join_output_partitioning,
        BuildProbeJoinMetrics, ColumnIndex, JoinFilter, JoinHashMapType,
    },
    metrics::{ExecutionPlanMetricsSet, MetricsSet},
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, Partitioning,
    PlanProperties, SendableRecordBatchStream, Statistics,
};

use arrow::array::{ArrayRef, BooleanBufferBuilder};
use arrow::compute::concat_batches;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use arrow::util::bit_util;
use arrow_schema::DataType;
use datafusion_common::config::ConfigOptions;
use datafusion_common::utils::memory::estimate_memory_size;
use datafusion_common::{
    assert_or_internal_err, plan_err, project_schema, JoinSide, JoinType, NullEquality,
    Result,
};
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion_execution::TaskContext;
use datafusion_expr::Accumulator;
use datafusion_functions_aggregate_common::min_max::{MaxAccumulator, MinAccumulator};
use datafusion_physical_expr::equivalence::{
    join_equivalence_properties, ProjectionMapping,
};
use datafusion_physical_expr::expressions::{lit, DynamicFilterPhysicalExpr};
use datafusion_physical_expr::{PhysicalExpr, PhysicalExprRef};

use ahash::RandomState;
use datafusion_physical_expr_common::physical_expr::fmt_sql;
use datafusion_physical_expr_common::utils::evaluate_expressions_to_arrays;
use futures::TryStreamExt;
use parking_lot::Mutex;

/// Hard-coded seed to ensure hash values from the hash join differ from `RepartitionExec`, avoiding collisions.
const HASH_JOIN_SEED: RandomState =
    RandomState::with_seeds('J' as u64, 'O' as u64, 'I' as u64, 'N' as u64);

/// HashTable and input data for the left (build side) of a join
pub(super) struct JoinLeftData {
    /// The hash table with indices into `batch`
    /// Arc is used to allow sharing with SharedBuildAccumulator for hash map pushdown
    pub(super) hash_map: Arc<dyn JoinHashMapType>,
    /// The input rows for the build side
    batch: RecordBatch,
    /// The build side on expressions values
    values: Vec<ArrayRef>,
    /// Shared bitmap builder for visited left indices
    visited_indices_bitmap: SharedBitmapBuilder,
    /// Counter of running probe-threads, potentially
    /// able to update `visited_indices_bitmap`
    probe_threads_counter: AtomicUsize,
    /// We need to keep this field to maintain accurate memory accounting, even though we don't directly use it.
    /// Without holding onto this reservation, the recorded memory usage would become inconsistent with actual usage.
    /// This could hide potential out-of-memory issues, especially when upstream operators increase their memory consumption.
    /// The MemoryReservation ensures proper tracking of memory resources throughout the join operation's lifecycle.
    _reservation: MemoryReservation,
    /// Bounds computed from the build side for dynamic filter pushdown.
    /// If the partition is empty (no rows) this will be None.
    /// If the partition has some rows this will be Some with the bounds for each join key column.
    pub(super) bounds: Option<PartitionBounds>,
}

impl JoinLeftData {
    /// return a reference to the hash map
    pub(super) fn hash_map(&self) -> &dyn JoinHashMapType {
        &*self.hash_map
    }

    /// returns a reference to the build side batch
    pub(super) fn batch(&self) -> &RecordBatch {
        &self.batch
    }

    /// returns a reference to the build side expressions values
    pub(super) fn values(&self) -> &[ArrayRef] {
        &self.values
    }

    /// returns a reference to the visited indices bitmap
    pub(super) fn visited_indices_bitmap(&self) -> &SharedBitmapBuilder {
        &self.visited_indices_bitmap
    }

    /// Decrements the counter of running threads, and returns `true`
    /// if caller is the last running thread
    pub(super) fn report_probe_completed(&self) -> bool {
        self.probe_threads_counter.fetch_sub(1, Ordering::Relaxed) == 1
    }
}

#[expect(rustdoc::private_intra_doc_links)]
/// Join execution plan: Evaluates equijoin predicates in parallel on multiple
/// partitions using a hash table and an optional filter list to apply post
/// join.
///
/// # Join Expressions
///
/// This implementation is optimized for evaluating equijoin predicates  (
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
/// 1. the **build phase** creates a hash table from the tuples of the build side,
///    and single concatenated batch containing data from all fetched record batches.
///    Resulting hash table stores hashed join-key fields for each row as a key, and
///    indices of corresponding rows in concatenated batch.
///
/// Hash join uses LIFO data structure as a hash table, and in order to retain
/// original build-side input order while obtaining data during probe phase, hash
/// table is updated by iterating batch sequence in reverse order -- it allows to
/// keep rows with smaller indices "on the top" of hash table, and still maintain
/// correct indexing for concatenated build-side data batch.
///
/// Example of build phase for 3 record batches:
///
///
/// ```text
///
///  Original build-side data   Inserting build-side values into hashmap    Concatenated build-side batch
///                                                                         ┌───────────────────────────┐
///                             hashmap.insert(row-hash, row-idx + offset)  │                      idx  │
///            ┌───────┐                                                    │          ┌───────┐        │
///            │ Row 1 │        1) update_hash for batch 3 with offset 0    │          │ Row 6 │    0   │
///   Batch 1  │       │           - hashmap.insert(Row 7, idx 1)           │ Batch 3  │       │        │
///            │ Row 2 │           - hashmap.insert(Row 6, idx 0)           │          │ Row 7 │    1   │
///            └───────┘                                                    │          └───────┘        │
///                                                                         │                           │
///            ┌───────┐                                                    │          ┌───────┐        │
///            │ Row 3 │        2) update_hash for batch 2 with offset 2    │          │ Row 3 │    2   │
///            │       │           - hashmap.insert(Row 5, idx 4)           │          │       │        │
///   Batch 2  │ Row 4 │           - hashmap.insert(Row 4, idx 3)           │ Batch 2  │ Row 4 │    3   │
///            │       │           - hashmap.insert(Row 3, idx 2)           │          │       │        │
///            │ Row 5 │                                                    │          │ Row 5 │    4   │
///            └───────┘                                                    │          └───────┘        │
///                                                                         │                           │
///            ┌───────┐                                                    │          ┌───────┐        │
///            │ Row 6 │        3) update_hash for batch 1 with offset 5    │          │ Row 1 │    5   │
///   Batch 3  │       │           - hashmap.insert(Row 2, idx 6)           │ Batch 1  │       │        │
///            │ Row 7 │           - hashmap.insert(Row 1, idx 5)           │          │ Row 2 │    6   │
///            └───────┘                                                    │          └───────┘        │
///                                                                         │                           │
///                                                                         └───────────────────────────┘
/// ```
///
/// 2. the **probe phase** where the tuples of the probe side are streamed
///    through, checking for matches of the join keys in the hash table.
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
///
/// # Clone / Shared State
///
/// Note this structure includes a [`OnceAsync`] that is used to coordinate the
/// loading of the left side with the processing in each output stream.
/// Therefore it can not be [`Clone`]
pub struct HashJoinExec {
    /// left (build) side which gets hashed
    pub left: Arc<dyn ExecutionPlan>,
    /// right (probe) side which are filtered by the hash table
    pub right: Arc<dyn ExecutionPlan>,
    /// Set of equijoin columns from the relations: `(left_col, right_col)`
    pub on: Vec<(PhysicalExprRef, PhysicalExprRef)>,
    /// Filters which are applied while finding matching rows
    pub filter: Option<JoinFilter>,
    /// How the join is performed (`OUTER`, `INNER`, etc)
    pub join_type: JoinType,
    /// The schema after join. Please be careful when using this schema,
    /// if there is a projection, the schema isn't the same as the output schema.
    join_schema: SchemaRef,
    /// Future that consumes left input and builds the hash table
    ///
    /// For CollectLeft partition mode, this structure is *shared* across all output streams.
    ///
    /// Each output stream waits on the `OnceAsync` to signal the completion of
    /// the hash table creation.
    left_fut: Arc<OnceAsync<JoinLeftData>>,
    /// Shared the `RandomState` for the hashing algorithm
    random_state: RandomState,
    /// Partitioning mode to use
    pub mode: PartitionMode,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// The projection indices of the columns in the output schema of join
    pub projection: Option<Vec<usize>>,
    /// Information of index and left / right placement of columns
    column_indices: Vec<ColumnIndex>,
    /// The equality null-handling behavior of the join algorithm.
    pub null_equality: NullEquality,
    /// Cache holding plan properties like equivalences, output partitioning etc.
    cache: PlanProperties,
    /// Dynamic filter for pushing down to the probe side
    /// Set when dynamic filter pushdown is detected in handle_child_pushdown_result.
    /// HashJoinExec also needs to keep a shared bounds accumulator for coordinating updates.
    dynamic_filter: Option<HashJoinExecDynamicFilter>,
}

#[derive(Clone)]
struct HashJoinExecDynamicFilter {
    /// Dynamic filter that we'll update with the results of the build side once that is done.
    filter: Arc<DynamicFilterPhysicalExpr>,
    /// Build accumulator to collect build-side information (hash maps and/or bounds) from each partition.
    /// It is lazily initialized during execution to make sure we use the actual execution time partition counts.
    build_accumulator: OnceLock<Arc<SharedBuildAccumulator>>,
}

impl fmt::Debug for HashJoinExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HashJoinExec")
            .field("left", &self.left)
            .field("right", &self.right)
            .field("on", &self.on)
            .field("filter", &self.filter)
            .field("join_type", &self.join_type)
            .field("join_schema", &self.join_schema)
            .field("left_fut", &self.left_fut)
            .field("random_state", &self.random_state)
            .field("mode", &self.mode)
            .field("metrics", &self.metrics)
            .field("projection", &self.projection)
            .field("column_indices", &self.column_indices)
            .field("null_equality", &self.null_equality)
            .field("cache", &self.cache)
            // Explicitly exclude dynamic_filter to avoid runtime state differences in tests
            .finish()
    }
}

impl EmbeddedProjection for HashJoinExec {
    fn with_projection(&self, projection: Option<Vec<usize>>) -> Result<Self> {
        self.with_projection(projection)
    }
}

impl HashJoinExec {
    /// Tries to create a new [HashJoinExec].
    ///
    /// # Error
    /// This function errors when it is not possible to join the left and right sides on keys `on`.
    #[expect(clippy::too_many_arguments)]
    pub fn try_new(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: JoinOn,
        filter: Option<JoinFilter>,
        join_type: &JoinType,
        projection: Option<Vec<usize>>,
        partition_mode: PartitionMode,
        null_equality: NullEquality,
    ) -> Result<Self> {
        let left_schema = left.schema();
        let right_schema = right.schema();
        if on.is_empty() {
            return plan_err!("On constraints in HashJoinExec should be non-empty");
        }

        check_join_is_valid(&left_schema, &right_schema, &on)?;

        let (join_schema, column_indices) =
            build_join_schema(&left_schema, &right_schema, join_type);

        let random_state = HASH_JOIN_SEED;

        let join_schema = Arc::new(join_schema);

        //  check if the projection is valid
        can_project(&join_schema, projection.as_ref())?;

        let cache = Self::compute_properties(
            &left,
            &right,
            &join_schema,
            *join_type,
            &on,
            partition_mode,
            projection.as_ref(),
        )?;

        // Initialize both dynamic filter and bounds accumulator to None
        // They will be set later if dynamic filtering is enabled

        Ok(HashJoinExec {
            left,
            right,
            on,
            filter,
            join_type: *join_type,
            join_schema,
            left_fut: Default::default(),
            random_state,
            mode: partition_mode,
            metrics: ExecutionPlanMetricsSet::new(),
            projection,
            column_indices,
            null_equality,
            cache,
            dynamic_filter: None,
        })
    }

    fn create_dynamic_filter(on: &JoinOn) -> Arc<DynamicFilterPhysicalExpr> {
        // Extract the right-side keys (probe side keys) from the `on` clauses
        // Dynamic filter will be created from build side values (left side) and applied to probe side (right side)
        let right_keys: Vec<_> = on.iter().map(|(_, r)| Arc::clone(r)).collect();
        // Initialize with a placeholder expression (true) that will be updated when the hash table is built
        Arc::new(DynamicFilterPhysicalExpr::new(right_keys, lit(true)))
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
    pub fn on(&self) -> &[(PhysicalExprRef, PhysicalExprRef)] {
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

    /// The schema after join. Please be careful when using this schema,
    /// if there is a projection, the schema isn't the same as the output schema.
    pub fn join_schema(&self) -> &SchemaRef {
        &self.join_schema
    }

    /// The partitioning mode of this hash join
    pub fn partition_mode(&self) -> &PartitionMode {
        &self.mode
    }

    /// Get null_equality
    pub fn null_equality(&self) -> NullEquality {
        self.null_equality
    }

    /// Calculate order preservation flags for this hash join.
    fn maintains_input_order(join_type: JoinType) -> Vec<bool> {
        vec![
            false,
            matches!(
                join_type,
                JoinType::Inner
                    | JoinType::Right
                    | JoinType::RightAnti
                    | JoinType::RightSemi
                    | JoinType::RightMark
            ),
        ]
    }

    /// Get probe side information for the hash join.
    pub fn probe_side() -> JoinSide {
        // In current implementation right side is always probe side.
        JoinSide::Right
    }

    /// Return whether the join contains a projection
    pub fn contains_projection(&self) -> bool {
        self.projection.is_some()
    }

    /// Return new instance of [HashJoinExec] with the given projection.
    pub fn with_projection(&self, projection: Option<Vec<usize>>) -> Result<Self> {
        //  check if the projection is valid
        can_project(&self.schema(), projection.as_ref())?;
        let projection = match projection {
            Some(projection) => match &self.projection {
                Some(p) => Some(projection.iter().map(|i| p[*i]).collect()),
                None => Some(projection),
            },
            None => None,
        };
        Self::try_new(
            Arc::clone(&self.left),
            Arc::clone(&self.right),
            self.on.clone(),
            self.filter.clone(),
            &self.join_type,
            projection,
            self.mode,
            self.null_equality,
        )
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(
        left: &Arc<dyn ExecutionPlan>,
        right: &Arc<dyn ExecutionPlan>,
        schema: &SchemaRef,
        join_type: JoinType,
        on: JoinOnRef,
        mode: PartitionMode,
        projection: Option<&Vec<usize>>,
    ) -> Result<PlanProperties> {
        // Calculate equivalence properties:
        let mut eq_properties = join_equivalence_properties(
            left.equivalence_properties().clone(),
            right.equivalence_properties().clone(),
            &join_type,
            Arc::clone(schema),
            &Self::maintains_input_order(join_type),
            Some(Self::probe_side()),
            on,
        )?;

        let mut output_partitioning = match mode {
            PartitionMode::CollectLeft => {
                asymmetric_join_output_partitioning(left, right, &join_type)?
            }
            PartitionMode::Auto => Partitioning::UnknownPartitioning(
                right.output_partitioning().partition_count(),
            ),
            PartitionMode::Partitioned => {
                symmetric_join_output_partitioning(left, right, &join_type)?
            }
        };

        let emission_type = if left.boundedness().is_unbounded() {
            EmissionType::Final
        } else if right.pipeline_behavior() == EmissionType::Incremental {
            match join_type {
                // If we only need to generate matched rows from the probe side,
                // we can emit rows incrementally.
                JoinType::Inner
                | JoinType::LeftSemi
                | JoinType::RightSemi
                | JoinType::Right
                | JoinType::RightAnti
                | JoinType::RightMark => EmissionType::Incremental,
                // If we need to generate unmatched rows from the *build side*,
                // we need to emit them at the end.
                JoinType::Left
                | JoinType::LeftAnti
                | JoinType::LeftMark
                | JoinType::Full => EmissionType::Both,
            }
        } else {
            right.pipeline_behavior()
        };

        // If contains projection, update the PlanProperties.
        if let Some(projection) = projection {
            // construct a map from the input expressions to the output expression of the Projection
            let projection_mapping = ProjectionMapping::from_indices(projection, schema)?;
            let out_schema = project_schema(schema, Some(projection))?;
            output_partitioning =
                output_partitioning.project(&projection_mapping, &eq_properties);
            eq_properties = eq_properties.project(&projection_mapping, out_schema);
        }

        Ok(PlanProperties::new(
            eq_properties,
            output_partitioning,
            emission_type,
            boundedness_from_children([left, right]),
        ))
    }

    /// Returns a new `ExecutionPlan` that computes the same join as this one,
    /// with the left and right inputs swapped using the  specified
    /// `partition_mode`.
    ///
    /// # Notes:
    ///
    /// This function is public so other downstream projects can use it to
    /// construct `HashJoinExec` with right side as the build side.
    ///
    /// For using this interface directly, please refer to below:
    ///
    /// Hash join execution may require specific input partitioning (for example,
    /// the left child may have a single partition while the right child has multiple).
    ///
    /// Calling this function on join nodes whose children have already been repartitioned
    /// (e.g., after a `RepartitionExec` has been inserted) may break the partitioning
    /// requirements of the hash join. Therefore, ensure you call this function
    /// before inserting any repartitioning operators on the join's children.
    ///
    /// In DataFusion's default SQL interface, this function is used by the `JoinSelection`
    /// physical optimizer rule to determine a good join order, which is
    /// executed before the `EnforceDistribution` rule (the rule that may
    /// insert `RepartitionExec` operators).
    pub fn swap_inputs(
        &self,
        partition_mode: PartitionMode,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let left = self.left();
        let right = self.right();
        let new_join = HashJoinExec::try_new(
            Arc::clone(right),
            Arc::clone(left),
            self.on()
                .iter()
                .map(|(l, r)| (Arc::clone(r), Arc::clone(l)))
                .collect(),
            self.filter().map(JoinFilter::swap),
            &self.join_type().swap(),
            swap_join_projection(
                left.schema().fields().len(),
                right.schema().fields().len(),
                self.projection.as_ref(),
                self.join_type(),
            ),
            partition_mode,
            self.null_equality(),
        )?;
        // In case of anti / semi joins or if there is embedded projection in HashJoinExec, output column order is preserved, no need to add projection again
        if matches!(
            self.join_type(),
            JoinType::LeftSemi
                | JoinType::RightSemi
                | JoinType::LeftAnti
                | JoinType::RightAnti
                | JoinType::LeftMark
                | JoinType::RightMark
        ) || self.projection.is_some()
        {
            Ok(Arc::new(new_join))
        } else {
            reorder_output_after_swap(Arc::new(new_join), &left.schema(), &right.schema())
        }
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
                let display_projections = if self.contains_projection() {
                    format!(
                        ", projection=[{}]",
                        self.projection
                            .as_ref()
                            .unwrap()
                            .iter()
                            .map(|index| format!(
                                "{}@{}",
                                self.join_schema.fields().get(*index).unwrap().name(),
                                index
                            ))
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                } else {
                    "".to_string()
                };
                let display_null_equality =
                    if matches!(self.null_equality(), NullEquality::NullEqualsNull) {
                        ", NullsEqual: true"
                    } else {
                        ""
                    };
                let on = self
                    .on
                    .iter()
                    .map(|(c1, c2)| format!("({c1}, {c2})"))
                    .collect::<Vec<String>>()
                    .join(", ");
                write!(
                    f,
                    "HashJoinExec: mode={:?}, join_type={:?}, on=[{}]{}{}{}",
                    self.mode,
                    self.join_type,
                    on,
                    display_filter,
                    display_projections,
                    display_null_equality,
                )
            }
            DisplayFormatType::TreeRender => {
                let on = self
                    .on
                    .iter()
                    .map(|(c1, c2)| {
                        format!("({} = {})", fmt_sql(c1.as_ref()), fmt_sql(c2.as_ref()))
                    })
                    .collect::<Vec<String>>()
                    .join(", ");

                if *self.join_type() != JoinType::Inner {
                    writeln!(f, "join_type={:?}", self.join_type)?;
                }

                writeln!(f, "on={on}")?;

                if matches!(self.null_equality(), NullEquality::NullEqualsNull) {
                    writeln!(f, "NullsEqual: true")?;
                }

                if let Some(filter) = self.filter.as_ref() {
                    writeln!(f, "filter={filter}")?;
                }

                Ok(())
            }
        }
    }
}

impl ExecutionPlan for HashJoinExec {
    fn name(&self) -> &'static str {
        "HashJoinExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
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
                    .map(|(l, r)| (Arc::clone(l), Arc::clone(r)))
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

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.left, &self.right]
    }

    /// Creates a new HashJoinExec with different children while preserving configuration.
    ///
    /// This method is called during query optimization when the optimizer creates new
    /// plan nodes. Importantly, it creates a fresh bounds_accumulator via `try_new`
    /// rather than cloning the existing one because partitioning may have changed.
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(HashJoinExec {
            left: Arc::clone(&children[0]),
            right: Arc::clone(&children[1]),
            on: self.on.clone(),
            filter: self.filter.clone(),
            join_type: self.join_type,
            join_schema: Arc::clone(&self.join_schema),
            left_fut: Arc::clone(&self.left_fut),
            random_state: self.random_state.clone(),
            mode: self.mode,
            metrics: ExecutionPlanMetricsSet::new(),
            projection: self.projection.clone(),
            column_indices: self.column_indices.clone(),
            null_equality: self.null_equality,
            cache: Self::compute_properties(
                &children[0],
                &children[1],
                &self.join_schema,
                self.join_type,
                &self.on,
                self.mode,
                self.projection.as_ref(),
            )?,
            // Keep the dynamic filter, bounds accumulator will be reset
            dynamic_filter: self.dynamic_filter.clone(),
        }))
    }

    fn reset_state(self: Arc<Self>) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(HashJoinExec {
            left: Arc::clone(&self.left),
            right: Arc::clone(&self.right),
            on: self.on.clone(),
            filter: self.filter.clone(),
            join_type: self.join_type,
            join_schema: Arc::clone(&self.join_schema),
            // Reset the left_fut to allow re-execution
            left_fut: Arc::new(OnceAsync::default()),
            random_state: self.random_state.clone(),
            mode: self.mode,
            metrics: ExecutionPlanMetricsSet::new(),
            projection: self.projection.clone(),
            column_indices: self.column_indices.clone(),
            null_equality: self.null_equality,
            cache: self.cache.clone(),
            // Reset dynamic filter and bounds accumulator to initial state
            dynamic_filter: None,
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let on_left = self
            .on
            .iter()
            .map(|on| Arc::clone(&on.0))
            .collect::<Vec<_>>();
        let left_partitions = self.left.output_partitioning().partition_count();
        let right_partitions = self.right.output_partitioning().partition_count();

        assert_or_internal_err!(
            self.mode != PartitionMode::Partitioned
                || left_partitions == right_partitions,
            "Invalid HashJoinExec, partition count mismatch {left_partitions}!={right_partitions},\
             consider using RepartitionExec"
        );

        assert_or_internal_err!(
            self.mode != PartitionMode::CollectLeft || left_partitions == 1,
            "Invalid HashJoinExec, the output partition count of the left child must be 1 in CollectLeft mode,\
             consider using CoalescePartitionsExec or the EnforceDistribution rule"
        );

        let enable_dynamic_filter_pushdown = self.dynamic_filter.is_some();

        let join_metrics = BuildProbeJoinMetrics::new(partition, &self.metrics);
        let left_fut = match self.mode {
            PartitionMode::CollectLeft => self.left_fut.try_once(|| {
                let left_stream = self.left.execute(0, Arc::clone(&context))?;

                let reservation =
                    MemoryConsumer::new("HashJoinInput").register(context.memory_pool());

                Ok(collect_left_input(
                    self.random_state.clone(),
                    left_stream,
                    on_left.clone(),
                    join_metrics.clone(),
                    reservation,
                    need_produce_result_in_final(self.join_type),
                    self.right().output_partitioning().partition_count(),
                    enable_dynamic_filter_pushdown,
                ))
            })?,
            PartitionMode::Partitioned => {
                let left_stream = self.left.execute(partition, Arc::clone(&context))?;

                let reservation =
                    MemoryConsumer::new(format!("HashJoinInput[{partition}]"))
                        .register(context.memory_pool());

                OnceFut::new(collect_left_input(
                    self.random_state.clone(),
                    left_stream,
                    on_left.clone(),
                    join_metrics.clone(),
                    reservation,
                    need_produce_result_in_final(self.join_type),
                    1,
                    enable_dynamic_filter_pushdown,
                ))
            }
            PartitionMode::Auto => {
                return plan_err!(
                    "Invalid HashJoinExec, unsupported PartitionMode {:?} in execute()",
                    PartitionMode::Auto
                );
            }
        };

        let batch_size = context.session_config().batch_size();

        // Initialize build_accumulator lazily with runtime partition counts (only if enabled)
        // Use RepartitionExec's random state (seeds: 0,0,0,0) for partition routing
        let repartition_random_state = REPARTITION_RANDOM_STATE;
        let build_accumulator = enable_dynamic_filter_pushdown
            .then(|| {
                self.dynamic_filter.as_ref().map(|df| {
                    let filter = Arc::clone(&df.filter);
                    let on_right = self
                        .on
                        .iter()
                        .map(|(_, right_expr)| Arc::clone(right_expr))
                        .collect::<Vec<_>>();
                    Some(Arc::clone(df.build_accumulator.get_or_init(|| {
                        Arc::new(SharedBuildAccumulator::new_from_partition_mode(
                            self.mode,
                            self.left.as_ref(),
                            self.right.as_ref(),
                            filter,
                            on_right,
                            repartition_random_state,
                        ))
                    })))
                })
            })
            .flatten()
            .flatten();

        // we have the batches and the hash map with their keys. We can how create a stream
        // over the right that uses this information to issue new batches.
        let right_stream = self.right.execute(partition, context)?;

        // update column indices to reflect the projection
        let column_indices_after_projection = match &self.projection {
            Some(projection) => projection
                .iter()
                .map(|i| self.column_indices[*i].clone())
                .collect(),
            None => self.column_indices.clone(),
        };

        let on_right = self
            .on
            .iter()
            .map(|(_, right_expr)| Arc::clone(right_expr))
            .collect::<Vec<_>>();

        Ok(Box::pin(HashJoinStream::new(
            partition,
            self.schema(),
            on_right,
            self.filter.clone(),
            self.join_type,
            right_stream,
            self.random_state.clone(),
            join_metrics,
            column_indices_after_projection,
            self.null_equality,
            HashJoinStreamState::WaitBuildSide,
            BuildSide::Initial(BuildSideInitialState { left_fut }),
            batch_size,
            vec![],
            self.right.output_ordering().is_some(),
            build_accumulator,
            self.mode,
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        self.partition_statistics(None)
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
        if partition.is_some() {
            return Ok(Statistics::new_unknown(&self.schema()));
        }
        // TODO stats: it is not possible in general to know the output size of joins
        // There are some special cases though, for example:
        // - `A LEFT JOIN B ON A.col=B.col` with `COUNT_DISTINCT(B.col)=COUNT(B.col)`
        let stats = estimate_join_statistics(
            self.left.partition_statistics(None)?,
            self.right.partition_statistics(None)?,
            &self.on,
            &self.join_type,
            &self.join_schema,
        )?;
        // Project statistics if there is a projection
        Ok(stats.project(self.projection.as_ref()))
    }

    /// Tries to push `projection` down through `hash_join`. If possible, performs the
    /// pushdown and returns a new [`HashJoinExec`] as the top plan which has projections
    /// as its children. Otherwise, returns `None`.
    fn try_swapping_with_projection(
        &self,
        projection: &ProjectionExec,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        // TODO: currently if there is projection in HashJoinExec, we can't push down projection to left or right input. Maybe we can pushdown the mixed projection later.
        if self.contains_projection() {
            return Ok(None);
        }

        let schema = self.schema();
        if let Some(JoinData {
            projected_left_child,
            projected_right_child,
            join_filter,
            join_on,
        }) = try_pushdown_through_join(
            projection,
            self.left(),
            self.right(),
            self.on(),
            &schema,
            self.filter(),
        )? {
            Ok(Some(Arc::new(HashJoinExec::try_new(
                Arc::new(projected_left_child),
                Arc::new(projected_right_child),
                join_on,
                join_filter,
                self.join_type(),
                // Returned early if projection is not None
                None,
                *self.partition_mode(),
                self.null_equality,
            )?)))
        } else {
            try_embed_projection(projection, self)
        }
    }

    fn gather_filters_for_pushdown(
        &self,
        phase: FilterPushdownPhase,
        parent_filters: Vec<Arc<dyn PhysicalExpr>>,
        config: &ConfigOptions,
    ) -> Result<FilterDescription> {
        // Other types of joins can support *some* filters, but restrictions are complex and error prone.
        // For now we don't support them.
        // See the logical optimizer rules for more details: datafusion/optimizer/src/push_down_filter.rs
        // See https://github.com/apache/datafusion/issues/16973 for tracking.
        if self.join_type != JoinType::Inner {
            return Ok(FilterDescription::all_unsupported(
                &parent_filters,
                &self.children(),
            ));
        }

        // Get basic filter descriptions for both children
        let left_child = crate::filter_pushdown::ChildFilterDescription::from_child(
            &parent_filters,
            self.left(),
        )?;
        let mut right_child = crate::filter_pushdown::ChildFilterDescription::from_child(
            &parent_filters,
            self.right(),
        )?;

        // Add dynamic filters in Post phase if enabled
        if matches!(phase, FilterPushdownPhase::Post)
            && config.optimizer.enable_join_dynamic_filter_pushdown
        {
            // Add actual dynamic filter to right side (probe side)
            let dynamic_filter = Self::create_dynamic_filter(&self.on);
            right_child = right_child.with_self_filter(dynamic_filter);
        }

        Ok(FilterDescription::new()
            .with_child(left_child)
            .with_child(right_child))
    }

    fn handle_child_pushdown_result(
        &self,
        _phase: FilterPushdownPhase,
        child_pushdown_result: ChildPushdownResult,
        _config: &ConfigOptions,
    ) -> Result<FilterPushdownPropagation<Arc<dyn ExecutionPlan>>> {
        // Note: this check shouldn't be necessary because we already marked all parent filters as unsupported for
        // non-inner joins in `gather_filters_for_pushdown`.
        // However it's a cheap check and serves to inform future devs touching this function that they need to be really
        // careful pushing down filters through non-inner joins.
        if self.join_type != JoinType::Inner {
            // Other types of joins can support *some* filters, but restrictions are complex and error prone.
            // For now we don't support them.
            // See the logical optimizer rules for more details: datafusion/optimizer/src/push_down_filter.rs
            return Ok(FilterPushdownPropagation::all_unsupported(
                child_pushdown_result,
            ));
        }

        let mut result = FilterPushdownPropagation::if_any(child_pushdown_result.clone());
        assert_eq!(child_pushdown_result.self_filters.len(), 2); // Should always be 2, we have 2 children
        let right_child_self_filters = &child_pushdown_result.self_filters[1]; // We only push down filters to the right child
                                                                               // We expect 0 or 1 self filters
        if let Some(filter) = right_child_self_filters.first() {
            // Note that we don't check PushdDownPredicate::discrimnant because even if nothing said
            // "yes, I can fully evaluate this filter" things might still use it for statistics -> it's worth updating
            let predicate = Arc::clone(&filter.predicate);
            if let Ok(dynamic_filter) =
                Arc::downcast::<DynamicFilterPhysicalExpr>(predicate)
            {
                // We successfully pushed down our self filter - we need to make a new node with the dynamic filter
                let new_node = Arc::new(HashJoinExec {
                    left: Arc::clone(&self.left),
                    right: Arc::clone(&self.right),
                    on: self.on.clone(),
                    filter: self.filter.clone(),
                    join_type: self.join_type,
                    join_schema: Arc::clone(&self.join_schema),
                    left_fut: Arc::clone(&self.left_fut),
                    random_state: self.random_state.clone(),
                    mode: self.mode,
                    metrics: ExecutionPlanMetricsSet::new(),
                    projection: self.projection.clone(),
                    column_indices: self.column_indices.clone(),
                    null_equality: self.null_equality,
                    cache: self.cache.clone(),
                    dynamic_filter: Some(HashJoinExecDynamicFilter {
                        filter: dynamic_filter,
                        build_accumulator: OnceLock::new(),
                    }),
                });
                result = result.with_updated_node(new_node as Arc<dyn ExecutionPlan>);
            }
        }
        Ok(result)
    }
}

/// Accumulator for collecting min/max bounds from build-side data during hash join.
///
/// This struct encapsulates the logic for progressively computing column bounds
/// (minimum and maximum values) for a specific join key expression as batches
/// are processed during the build phase of a hash join.
///
/// The bounds are used for dynamic filter pushdown optimization, where filters
/// based on the actual data ranges can be pushed down to the probe side to
/// eliminate unnecessary data early.
struct CollectLeftAccumulator {
    /// The physical expression to evaluate for each batch
    expr: Arc<dyn PhysicalExpr>,
    /// Accumulator for tracking the minimum value across all batches
    min: MinAccumulator,
    /// Accumulator for tracking the maximum value across all batches
    max: MaxAccumulator,
}

impl CollectLeftAccumulator {
    /// Creates a new accumulator for tracking bounds of a join key expression.
    ///
    /// # Arguments
    /// * `expr` - The physical expression to track bounds for
    /// * `schema` - The schema of the input data
    ///
    /// # Returns
    /// A new `CollectLeftAccumulator` instance configured for the expression's data type
    fn try_new(expr: Arc<dyn PhysicalExpr>, schema: &SchemaRef) -> Result<Self> {
        /// Recursively unwraps dictionary types to get the underlying value type.
        fn dictionary_value_type(data_type: &DataType) -> DataType {
            match data_type {
                DataType::Dictionary(_, value_type) => {
                    dictionary_value_type(value_type.as_ref())
                }
                _ => data_type.clone(),
            }
        }

        let data_type = expr
            .data_type(schema)
            // Min/Max can operate on dictionary data but expect to be initialized with the underlying value type
            .map(|dt| dictionary_value_type(&dt))?;
        Ok(Self {
            expr,
            min: MinAccumulator::try_new(&data_type)?,
            max: MaxAccumulator::try_new(&data_type)?,
        })
    }

    /// Updates the accumulators with values from a new batch.
    ///
    /// Evaluates the expression on the batch and updates both min and max
    /// accumulators with the resulting values.
    ///
    /// # Arguments
    /// * `batch` - The record batch to process
    ///
    /// # Returns
    /// Ok(()) if the update succeeds, or an error if expression evaluation fails
    fn update_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        let array = self.expr.evaluate(batch)?.into_array(batch.num_rows())?;
        self.min.update_batch(std::slice::from_ref(&array))?;
        self.max.update_batch(std::slice::from_ref(&array))?;
        Ok(())
    }

    /// Finalizes the accumulation and returns the computed bounds.
    ///
    /// Consumes self to extract the final min and max values from the accumulators.
    ///
    /// # Returns
    /// The `ColumnBounds` containing the minimum and maximum values observed
    fn evaluate(mut self) -> Result<ColumnBounds> {
        Ok(ColumnBounds::new(
            self.min.evaluate()?,
            self.max.evaluate()?,
        ))
    }
}

/// State for collecting the build-side data during hash join
struct BuildSideState {
    batches: Vec<RecordBatch>,
    num_rows: usize,
    metrics: BuildProbeJoinMetrics,
    reservation: MemoryReservation,
    bounds_accumulators: Option<Vec<CollectLeftAccumulator>>,
}

impl BuildSideState {
    /// Create a new BuildSideState with optional accumulators for bounds computation
    fn try_new(
        metrics: BuildProbeJoinMetrics,
        reservation: MemoryReservation,
        on_left: Vec<Arc<dyn PhysicalExpr>>,
        schema: &SchemaRef,
        should_compute_dynamic_filters: bool,
    ) -> Result<Self> {
        Ok(Self {
            batches: Vec::new(),
            num_rows: 0,
            metrics,
            reservation,
            bounds_accumulators: should_compute_dynamic_filters
                .then(|| {
                    on_left
                        .into_iter()
                        .map(|expr| CollectLeftAccumulator::try_new(expr, schema))
                        .collect::<Result<Vec<_>>>()
                })
                .transpose()?,
        })
    }
}

/// Collects all batches from the left (build) side stream and creates a hash map for joining.
///
/// This function is responsible for:
/// 1. Consuming the entire left stream and collecting all batches into memory
/// 2. Building a hash map from the join key columns for efficient probe operations
/// 3. Computing bounds for dynamic filter pushdown (if enabled)
/// 4. Preparing visited indices bitmap for certain join types
///
/// # Parameters
/// * `random_state` - Random state for consistent hashing across partitions
/// * `left_stream` - Stream of record batches from the build side
/// * `on_left` - Physical expressions for the left side join keys
/// * `metrics` - Metrics collector for tracking memory usage and row counts
/// * `reservation` - Memory reservation tracker for the hash table and data
/// * `with_visited_indices_bitmap` - Whether to track visited indices (for outer joins)
/// * `probe_threads_count` - Number of threads that will probe this hash table
/// * `should_compute_dynamic_filters` - Whether to compute min/max bounds for dynamic filtering
///
/// # Dynamic Filter Coordination
/// When `should_compute_dynamic_filters` is true, this function computes the min/max bounds
/// for each join key column but does NOT update the dynamic filter. Instead, the
/// bounds are stored in the returned `JoinLeftData` and later coordinated by
/// `SharedBuildAccumulator` to ensure all partitions contribute their bounds
/// before updating the filter exactly once.
///
/// # Returns
/// `JoinLeftData` containing the hash map, consolidated batch, join key values,
/// visited indices bitmap, and computed bounds (if requested).
#[expect(clippy::too_many_arguments)]
async fn collect_left_input(
    random_state: RandomState,
    left_stream: SendableRecordBatchStream,
    on_left: Vec<PhysicalExprRef>,
    metrics: BuildProbeJoinMetrics,
    reservation: MemoryReservation,
    with_visited_indices_bitmap: bool,
    probe_threads_count: usize,
    should_compute_dynamic_filters: bool,
) -> Result<JoinLeftData> {
    let schema = left_stream.schema();

    // This operation performs 2 steps at once:
    // 1. creates a [JoinHashMap] of all batches from the stream
    // 2. stores the batches in a vector.
    let initial = BuildSideState::try_new(
        metrics,
        reservation,
        on_left.clone(),
        &schema,
        should_compute_dynamic_filters,
    )?;

    let state = left_stream
        .try_fold(initial, |mut state, batch| async move {
            // Update accumulators if computing bounds
            if let Some(ref mut accumulators) = state.bounds_accumulators {
                for accumulator in accumulators {
                    accumulator.update_batch(&batch)?;
                }
            }

            // Decide if we spill or not
            let batch_size = get_record_batch_memory_size(&batch);
            // Reserve memory for incoming batch
            state.reservation.try_grow(batch_size)?;
            // Update metrics
            state.metrics.build_mem_used.add(batch_size);
            state.metrics.build_input_batches.add(1);
            state.metrics.build_input_rows.add(batch.num_rows());
            // Update row count
            state.num_rows += batch.num_rows();
            // Push batch to output
            state.batches.push(batch);
            Ok(state)
        })
        .await?;

    // Extract fields from state
    let BuildSideState {
        batches,
        num_rows,
        metrics,
        mut reservation,
        bounds_accumulators,
    } = state;

    // Estimation of memory size, required for hashtable, prior to allocation.
    // Final result can be verified using `RawTable.allocation_info()`
    let fixed_size_u32 = size_of::<JoinHashMapU32>();
    let fixed_size_u64 = size_of::<JoinHashMapU64>();

    // Use `u32` indices for the JoinHashMap when num_rows ≤ u32::MAX, otherwise use the
    // `u64` indice variant
    // Arc is used instead of Box to allow sharing with SharedBuildAccumulator for hash map pushdown
    let mut hashmap: Box<dyn JoinHashMapType> = if num_rows > u32::MAX as usize {
        let estimated_hashtable_size =
            estimate_memory_size::<(u64, u64)>(num_rows, fixed_size_u64)?;
        reservation.try_grow(estimated_hashtable_size)?;
        metrics.build_mem_used.add(estimated_hashtable_size);
        Box::new(JoinHashMapU64::with_capacity(num_rows))
    } else {
        let estimated_hashtable_size =
            estimate_memory_size::<(u32, u64)>(num_rows, fixed_size_u32)?;
        reservation.try_grow(estimated_hashtable_size)?;
        metrics.build_mem_used.add(estimated_hashtable_size);
        Box::new(JoinHashMapU32::with_capacity(num_rows))
    };

    let mut hashes_buffer = Vec::new();
    let mut offset = 0;

    // Updating hashmap starting from the last batch
    let batches_iter = batches.iter().rev();
    for batch in batches_iter.clone() {
        hashes_buffer.clear();
        hashes_buffer.resize(batch.num_rows(), 0);
        update_hash(
            &on_left,
            batch,
            &mut *hashmap,
            offset,
            &random_state,
            &mut hashes_buffer,
            0,
            true,
        )?;
        offset += batch.num_rows();
    }
    // Merge all batches into a single batch, so we can directly index into the arrays
    let batch = concat_batches(&schema, batches_iter)?;

    // Reserve additional memory for visited indices bitmap and create shared builder
    let visited_indices_bitmap = if with_visited_indices_bitmap {
        let bitmap_size = bit_util::ceil(batch.num_rows(), 8);
        reservation.try_grow(bitmap_size)?;
        metrics.build_mem_used.add(bitmap_size);

        let mut bitmap_buffer = BooleanBufferBuilder::new(batch.num_rows());
        bitmap_buffer.append_n(num_rows, false);
        bitmap_buffer
    } else {
        BooleanBufferBuilder::new(0)
    };

    let left_values = evaluate_expressions_to_arrays(&on_left, &batch)?;

    // Compute bounds for dynamic filter if enabled
    let bounds = match bounds_accumulators {
        Some(accumulators) if num_rows > 0 => {
            let bounds = accumulators
                .into_iter()
                .map(CollectLeftAccumulator::evaluate)
                .collect::<Result<Vec<_>>>()?;
            Some(PartitionBounds::new(bounds))
        }
        _ => None,
    };

    // Convert Box to Arc for sharing with SharedBuildAccumulator
    let hash_map: Arc<dyn JoinHashMapType> = hashmap.into();

    let data = JoinLeftData {
        hash_map,
        batch,
        values: left_values,
        visited_indices_bitmap: Mutex::new(visited_indices_bitmap),
        probe_threads_counter: AtomicUsize::new(probe_threads_count),
        _reservation: reservation,
        bounds,
    };

    Ok(data)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coalesce_partitions::CoalescePartitionsExec;
    use crate::joins::hash_join::stream::lookup_join_hashmap;
    use crate::test::{assert_join_metrics, TestMemoryExec};
    use crate::{
        common, expressions::Column, repartition::RepartitionExec, test::build_table_i32,
        test::exec::MockExec,
    };

    use arrow::array::{Date32Array, Int32Array, StructArray, UInt32Array, UInt64Array};
    use arrow::buffer::NullBuffer;
    use arrow::datatypes::{DataType, Field};
    use arrow_schema::Schema;
    use datafusion_common::hash_utils::create_hashes;
    use datafusion_common::test_util::{batches_to_sort_string, batches_to_string};
    use datafusion_common::{
        assert_batches_eq, assert_batches_sorted_eq, assert_contains, exec_err,
        internal_err, ScalarValue,
    };
    use datafusion_execution::config::SessionConfig;
    use datafusion_execution::runtime_env::RuntimeEnvBuilder;
    use datafusion_expr::Operator;
    use datafusion_physical_expr::expressions::{BinaryExpr, Literal};
    use datafusion_physical_expr::PhysicalExpr;
    use hashbrown::HashTable;
    use insta::{allow_duplicates, assert_snapshot};
    use rstest::*;
    use rstest_reuse::*;

    fn div_ceil(a: usize, b: usize) -> usize {
        a.div_ceil(b)
    }

    #[template]
    #[rstest]
    fn batch_sizes(#[values(8192, 10, 5, 2, 1)] batch_size: usize) {}

    fn prepare_task_ctx(batch_size: usize) -> Arc<TaskContext> {
        let session_config = SessionConfig::default().with_batch_size(batch_size);
        Arc::new(TaskContext::default().with_session_config(session_config))
    }

    fn build_table(
        a: (&str, &Vec<i32>),
        b: (&str, &Vec<i32>),
        c: (&str, &Vec<i32>),
    ) -> Arc<dyn ExecutionPlan> {
        let batch = build_table_i32(a, b, c);
        let schema = batch.schema();
        TestMemoryExec::try_new_exec(&[vec![batch]], schema, None).unwrap()
    }

    fn join(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: JoinOn,
        join_type: &JoinType,
        null_equality: NullEquality,
    ) -> Result<HashJoinExec> {
        HashJoinExec::try_new(
            left,
            right,
            on,
            None,
            join_type,
            None,
            PartitionMode::CollectLeft,
            null_equality,
        )
    }

    fn join_with_filter(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: JoinOn,
        filter: JoinFilter,
        join_type: &JoinType,
        null_equality: NullEquality,
    ) -> Result<HashJoinExec> {
        HashJoinExec::try_new(
            left,
            right,
            on,
            Some(filter),
            join_type,
            None,
            PartitionMode::CollectLeft,
            null_equality,
        )
    }

    async fn join_collect(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: JoinOn,
        join_type: &JoinType,
        null_equality: NullEquality,
        context: Arc<TaskContext>,
    ) -> Result<(Vec<String>, Vec<RecordBatch>, MetricsSet)> {
        let join = join(left, right, on, join_type, null_equality)?;
        let columns_header = columns(&join.schema());

        let stream = join.execute(0, context)?;
        let batches = common::collect(stream).await?;
        let metrics = join.metrics().unwrap();

        Ok((columns_header, batches, metrics))
    }

    async fn partitioned_join_collect(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: JoinOn,
        join_type: &JoinType,
        null_equality: NullEquality,
        context: Arc<TaskContext>,
    ) -> Result<(Vec<String>, Vec<RecordBatch>, MetricsSet)> {
        join_collect_with_partition_mode(
            left,
            right,
            on,
            join_type,
            PartitionMode::Partitioned,
            null_equality,
            context,
        )
        .await
    }

    async fn join_collect_with_partition_mode(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: JoinOn,
        join_type: &JoinType,
        partition_mode: PartitionMode,
        null_equality: NullEquality,
        context: Arc<TaskContext>,
    ) -> Result<(Vec<String>, Vec<RecordBatch>, MetricsSet)> {
        let partition_count = 4;

        let (left_expr, right_expr) = on
            .iter()
            .map(|(l, r)| (Arc::clone(l), Arc::clone(r)))
            .unzip();

        let left_repartitioned: Arc<dyn ExecutionPlan> = match partition_mode {
            PartitionMode::CollectLeft => Arc::new(CoalescePartitionsExec::new(left)),
            PartitionMode::Partitioned => Arc::new(RepartitionExec::try_new(
                left,
                Partitioning::Hash(left_expr, partition_count),
            )?),
            PartitionMode::Auto => {
                return internal_err!("Unexpected PartitionMode::Auto in join tests")
            }
        };

        let right_repartitioned: Arc<dyn ExecutionPlan> = match partition_mode {
            PartitionMode::CollectLeft => {
                let partition_column_name = right.schema().field(0).name().clone();
                let partition_expr = vec![Arc::new(Column::new_with_schema(
                    &partition_column_name,
                    &right.schema(),
                )?) as _];
                Arc::new(RepartitionExec::try_new(
                    right,
                    Partitioning::Hash(partition_expr, partition_count),
                )?) as _
            }
            PartitionMode::Partitioned => Arc::new(RepartitionExec::try_new(
                right,
                Partitioning::Hash(right_expr, partition_count),
            )?),
            PartitionMode::Auto => {
                return internal_err!("Unexpected PartitionMode::Auto in join tests")
            }
        };

        let join = HashJoinExec::try_new(
            left_repartitioned,
            right_repartitioned,
            on,
            None,
            join_type,
            None,
            partition_mode,
            null_equality,
        )?;

        let columns = columns(&join.schema());

        let mut batches = vec![];
        for i in 0..partition_count {
            let stream = join.execute(i, Arc::clone(&context))?;
            let more_batches = common::collect(stream).await?;
            batches.extend(
                more_batches
                    .into_iter()
                    .filter(|b| b.num_rows() > 0)
                    .collect::<Vec<_>>(),
            );
        }
        let metrics = join.metrics().unwrap();

        Ok((columns, batches, metrics))
    }

    #[apply(batch_sizes)]
    #[tokio::test]
    async fn join_inner_one(batch_size: usize) -> Result<()> {
        let task_ctx = prepare_task_ctx(batch_size);
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
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        )];

        let (columns, batches, metrics) = join_collect(
            Arc::clone(&left),
            Arc::clone(&right),
            on.clone(),
            &JoinType::Inner,
            NullEquality::NullEqualsNothing,
            task_ctx,
        )
        .await?;

        assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b1", "c2"]);

        allow_duplicates! {
            // Inner join output is expected to preserve both inputs order
            assert_snapshot!(batches_to_string(&batches), @r#"
                +----+----+----+----+----+----+
                | a1 | b1 | c1 | a2 | b1 | c2 |
                +----+----+----+----+----+----+
                | 1  | 4  | 7  | 10 | 4  | 70 |
                | 2  | 5  | 8  | 20 | 5  | 80 |
                | 3  | 5  | 9  | 20 | 5  | 80 |
                +----+----+----+----+----+----+
                "#);
        }

        assert_join_metrics!(metrics, 3);

        Ok(())
    }

    #[apply(batch_sizes)]
    #[tokio::test]
    async fn partitioned_join_inner_one(batch_size: usize) -> Result<()> {
        let task_ctx = prepare_task_ctx(batch_size);
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
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        )];

        let (columns, batches, metrics) = partitioned_join_collect(
            Arc::clone(&left),
            Arc::clone(&right),
            on.clone(),
            &JoinType::Inner,
            NullEquality::NullEqualsNothing,
            task_ctx,
        )
        .await?;

        assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b1", "c2"]);

        allow_duplicates! {
            assert_snapshot!(batches_to_sort_string(&batches), @r#"
                +----+----+----+----+----+----+
                | a1 | b1 | c1 | a2 | b1 | c2 |
                +----+----+----+----+----+----+
                | 1  | 4  | 7  | 10 | 4  | 70 |
                | 2  | 5  | 8  | 20 | 5  | 80 |
                | 3  | 5  | 9  | 20 | 5  | 80 |
                +----+----+----+----+----+----+
                "#);
        }

        assert_join_metrics!(metrics, 3);

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
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b2", &right.schema())?) as _,
        )];

        let (columns, batches, metrics) = join_collect(
            left,
            right,
            on,
            &JoinType::Inner,
            NullEquality::NullEqualsNothing,
            task_ctx,
        )
        .await?;

        assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b2", "c2"]);

        // Inner join output is expected to preserve both inputs order
        allow_duplicates! {
            assert_snapshot!(batches_to_string(&batches), @r#"
            +----+----+----+----+----+----+
            | a1 | b1 | c1 | a2 | b2 | c2 |
            +----+----+----+----+----+----+
            | 1  | 4  | 7  | 10 | 4  | 70 |
            | 2  | 5  | 8  | 20 | 5  | 80 |
            | 3  | 5  | 9  | 20 | 5  | 80 |
            +----+----+----+----+----+----+
                "#);
        }

        assert_join_metrics!(metrics, 3);

        Ok(())
    }

    #[tokio::test]
    async fn join_inner_one_randomly_ordered() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let left = build_table(
            ("a1", &vec![0, 3, 2, 1]),
            ("b1", &vec![4, 5, 5, 4]),
            ("c1", &vec![6, 9, 8, 7]),
        );
        let right = build_table(
            ("a2", &vec![20, 30, 10]),
            ("b2", &vec![5, 6, 4]),
            ("c2", &vec![80, 90, 70]),
        );
        let on = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b2", &right.schema())?) as _,
        )];

        let (columns, batches, metrics) = join_collect(
            left,
            right,
            on,
            &JoinType::Inner,
            NullEquality::NullEqualsNothing,
            task_ctx,
        )
        .await?;

        assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b2", "c2"]);

        // Inner join output is expected to preserve both inputs order
        allow_duplicates! {
            assert_snapshot!(batches_to_string(&batches), @r#"
            +----+----+----+----+----+----+
            | a1 | b1 | c1 | a2 | b2 | c2 |
            +----+----+----+----+----+----+
            | 3  | 5  | 9  | 20 | 5  | 80 |
            | 2  | 5  | 8  | 20 | 5  | 80 |
            | 0  | 4  | 6  | 10 | 4  | 70 |
            | 1  | 4  | 7  | 10 | 4  | 70 |
            +----+----+----+----+----+----+
                "#);
        }

        assert_join_metrics!(metrics, 4);

        Ok(())
    }

    #[apply(batch_sizes)]
    #[tokio::test]
    async fn join_inner_two(batch_size: usize) -> Result<()> {
        let task_ctx = prepare_task_ctx(batch_size);
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
                Arc::new(Column::new_with_schema("a1", &left.schema())?) as _,
                Arc::new(Column::new_with_schema("a1", &right.schema())?) as _,
            ),
            (
                Arc::new(Column::new_with_schema("b2", &left.schema())?) as _,
                Arc::new(Column::new_with_schema("b2", &right.schema())?) as _,
            ),
        ];

        let (columns, batches, metrics) = join_collect(
            left,
            right,
            on,
            &JoinType::Inner,
            NullEquality::NullEqualsNothing,
            task_ctx,
        )
        .await?;

        assert_eq!(columns, vec!["a1", "b2", "c1", "a1", "b2", "c2"]);

        let expected_batch_count = if cfg!(not(feature = "force_hash_collisions")) {
            // Expected number of hash table matches = 3
            // in case batch_size is 1 - additional empty batch for remaining 3-2 row
            let mut expected_batch_count = div_ceil(3, batch_size);
            if batch_size == 1 {
                expected_batch_count += 1;
            }
            expected_batch_count
        } else {
            // With hash collisions enabled, all records will match each other
            // and filtered later.
            div_ceil(9, batch_size)
        };

        assert_eq!(batches.len(), expected_batch_count);

        // Inner join output is expected to preserve both inputs order
        allow_duplicates! {
            assert_snapshot!(batches_to_string(&batches), @r#"
            +----+----+----+----+----+----+
            | a1 | b2 | c1 | a1 | b2 | c2 |
            +----+----+----+----+----+----+
            | 1  | 1  | 7  | 1  | 1  | 70 |
            | 2  | 2  | 8  | 2  | 2  | 80 |
            | 2  | 2  | 9  | 2  | 2  | 80 |
            +----+----+----+----+----+----+
                "#);
        }

        assert_join_metrics!(metrics, 3);

        Ok(())
    }

    /// Test where the left has 2 parts, the right with 1 part => 1 part
    #[apply(batch_sizes)]
    #[tokio::test]
    async fn join_inner_one_two_parts_left(batch_size: usize) -> Result<()> {
        let task_ctx = prepare_task_ctx(batch_size);
        let batch1 = build_table_i32(
            ("a1", &vec![1, 2]),
            ("b2", &vec![1, 2]),
            ("c1", &vec![7, 8]),
        );
        let batch2 =
            build_table_i32(("a1", &vec![2]), ("b2", &vec![2]), ("c1", &vec![9]));
        let schema = batch1.schema();
        let left =
            TestMemoryExec::try_new_exec(&[vec![batch1], vec![batch2]], schema, None)
                .unwrap();
        let left = Arc::new(CoalescePartitionsExec::new(left));

        let right = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b2", &vec![1, 2, 2]),
            ("c2", &vec![70, 80, 90]),
        );
        let on = vec![
            (
                Arc::new(Column::new_with_schema("a1", &left.schema())?) as _,
                Arc::new(Column::new_with_schema("a1", &right.schema())?) as _,
            ),
            (
                Arc::new(Column::new_with_schema("b2", &left.schema())?) as _,
                Arc::new(Column::new_with_schema("b2", &right.schema())?) as _,
            ),
        ];

        let (columns, batches, metrics) = join_collect(
            left,
            right,
            on,
            &JoinType::Inner,
            NullEquality::NullEqualsNothing,
            task_ctx,
        )
        .await?;

        assert_eq!(columns, vec!["a1", "b2", "c1", "a1", "b2", "c2"]);

        let expected_batch_count = if cfg!(not(feature = "force_hash_collisions")) {
            // Expected number of hash table matches = 3
            // in case batch_size is 1 - additional empty batch for remaining 3-2 row
            let mut expected_batch_count = div_ceil(3, batch_size);
            if batch_size == 1 {
                expected_batch_count += 1;
            }
            expected_batch_count
        } else {
            // With hash collisions enabled, all records will match each other
            // and filtered later.
            div_ceil(9, batch_size)
        };

        assert_eq!(batches.len(), expected_batch_count);

        // Inner join output is expected to preserve both inputs order
        allow_duplicates! {
            assert_snapshot!(batches_to_string(&batches), @r#"
            +----+----+----+----+----+----+
            | a1 | b2 | c1 | a1 | b2 | c2 |
            +----+----+----+----+----+----+
            | 1  | 1  | 7  | 1  | 1  | 70 |
            | 2  | 2  | 8  | 2  | 2  | 80 |
            | 2  | 2  | 9  | 2  | 2  | 80 |
            +----+----+----+----+----+----+
                "#);
        }

        assert_join_metrics!(metrics, 3);

        Ok(())
    }

    #[tokio::test]
    async fn join_inner_one_two_parts_left_randomly_ordered() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let batch1 = build_table_i32(
            ("a1", &vec![0, 3]),
            ("b1", &vec![4, 5]),
            ("c1", &vec![6, 9]),
        );
        let batch2 = build_table_i32(
            ("a1", &vec![2, 1]),
            ("b1", &vec![5, 4]),
            ("c1", &vec![8, 7]),
        );
        let schema = batch1.schema();

        let left =
            TestMemoryExec::try_new_exec(&[vec![batch1], vec![batch2]], schema, None)
                .unwrap();
        let left = Arc::new(CoalescePartitionsExec::new(left));
        let right = build_table(
            ("a2", &vec![20, 30, 10]),
            ("b2", &vec![5, 6, 4]),
            ("c2", &vec![80, 90, 70]),
        );
        let on = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b2", &right.schema())?) as _,
        )];

        let (columns, batches, metrics) = join_collect(
            left,
            right,
            on,
            &JoinType::Inner,
            NullEquality::NullEqualsNothing,
            task_ctx,
        )
        .await?;

        assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b2", "c2"]);

        // Inner join output is expected to preserve both inputs order
        allow_duplicates! {
            assert_snapshot!(batches_to_string(&batches), @r#"
            +----+----+----+----+----+----+
            | a1 | b1 | c1 | a2 | b2 | c2 |
            +----+----+----+----+----+----+
            | 3  | 5  | 9  | 20 | 5  | 80 |
            | 2  | 5  | 8  | 20 | 5  | 80 |
            | 0  | 4  | 6  | 10 | 4  | 70 |
            | 1  | 4  | 7  | 10 | 4  | 70 |
            +----+----+----+----+----+----+
                "#);
        }

        assert_join_metrics!(metrics, 4);

        Ok(())
    }

    /// Test where the left has 1 part, the right has 2 parts => 2 parts
    #[apply(batch_sizes)]
    #[tokio::test]
    async fn join_inner_one_two_parts_right(batch_size: usize) -> Result<()> {
        let task_ctx = prepare_task_ctx(batch_size);
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
        let right =
            TestMemoryExec::try_new_exec(&[vec![batch1], vec![batch2]], schema, None)
                .unwrap();

        let on = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        )];

        let join = join(
            left,
            right,
            on,
            &JoinType::Inner,
            NullEquality::NullEqualsNothing,
        )?;

        let columns = columns(&join.schema());
        assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b1", "c2"]);

        // first part
        let stream = join.execute(0, Arc::clone(&task_ctx))?;
        let batches = common::collect(stream).await?;

        let expected_batch_count = if cfg!(not(feature = "force_hash_collisions")) {
            // Expected number of hash table matches for first right batch = 1
            // and additional empty batch for non-joined 20-6-80
            let mut expected_batch_count = div_ceil(1, batch_size);
            if batch_size == 1 {
                expected_batch_count += 1;
            }
            expected_batch_count
        } else {
            // With hash collisions enabled, all records will match each other
            // and filtered later.
            div_ceil(6, batch_size)
        };
        assert_eq!(batches.len(), expected_batch_count);

        // Inner join output is expected to preserve both inputs order
        allow_duplicates! {
            assert_snapshot!(batches_to_string(&batches), @r#"
            +----+----+----+----+----+----+
            | a1 | b1 | c1 | a2 | b1 | c2 |
            +----+----+----+----+----+----+
            | 1  | 4  | 7  | 10 | 4  | 70 |
            +----+----+----+----+----+----+
                "#);
        }

        // second part
        let stream = join.execute(1, Arc::clone(&task_ctx))?;
        let batches = common::collect(stream).await?;

        let expected_batch_count = if cfg!(not(feature = "force_hash_collisions")) {
            // Expected number of hash table matches for second right batch = 2
            div_ceil(2, batch_size)
        } else {
            // With hash collisions enabled, all records will match each other
            // and filtered later.
            div_ceil(3, batch_size)
        };
        assert_eq!(batches.len(), expected_batch_count);

        // Inner join output is expected to preserve both inputs order
        allow_duplicates! {
            assert_snapshot!(batches_to_string(&batches), @r#"
            +----+----+----+----+----+----+
            | a1 | b1 | c1 | a2 | b1 | c2 |
            +----+----+----+----+----+----+
            | 2  | 5  | 8  | 30 | 5  | 90 |
            | 3  | 5  | 9  | 30 | 5  | 90 |
            +----+----+----+----+----+----+
                "#);
        }

        Ok(())
    }

    fn build_table_two_batches(
        a: (&str, &Vec<i32>),
        b: (&str, &Vec<i32>),
        c: (&str, &Vec<i32>),
    ) -> Arc<dyn ExecutionPlan> {
        let batch = build_table_i32(a, b, c);
        let schema = batch.schema();
        TestMemoryExec::try_new_exec(&[vec![batch.clone(), batch]], schema, None).unwrap()
    }

    #[apply(batch_sizes)]
    #[tokio::test]
    async fn join_left_multi_batch(batch_size: usize) {
        let task_ctx = prepare_task_ctx(batch_size);
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
            Arc::new(Column::new_with_schema("b1", &left.schema()).unwrap()) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema()).unwrap()) as _,
        )];

        let join = join(
            left,
            right,
            on,
            &JoinType::Left,
            NullEquality::NullEqualsNothing,
        )
        .unwrap();

        let columns = columns(&join.schema());
        assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b1", "c2"]);

        let stream = join.execute(0, task_ctx).unwrap();
        let batches = common::collect(stream).await.unwrap();

        allow_duplicates! {
            assert_snapshot!(batches_to_sort_string(&batches), @r#"
            +----+----+----+----+----+----+
            | a1 | b1 | c1 | a2 | b1 | c2 |
            +----+----+----+----+----+----+
            | 1  | 4  | 7  | 10 | 4  | 70 |
            | 1  | 4  | 7  | 10 | 4  | 70 |
            | 2  | 5  | 8  | 20 | 5  | 80 |
            | 2  | 5  | 8  | 20 | 5  | 80 |
            | 3  | 7  | 9  |    |    |    |
            +----+----+----+----+----+----+
                "#);
        }
    }

    #[apply(batch_sizes)]
    #[tokio::test]
    async fn join_full_multi_batch(batch_size: usize) {
        let task_ctx = prepare_task_ctx(batch_size);
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
            Arc::new(Column::new_with_schema("b1", &left.schema()).unwrap()) as _,
            Arc::new(Column::new_with_schema("b2", &right.schema()).unwrap()) as _,
        )];

        let join = join(
            left,
            right,
            on,
            &JoinType::Full,
            NullEquality::NullEqualsNothing,
        )
        .unwrap();

        let columns = columns(&join.schema());
        assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b2", "c2"]);

        let stream = join.execute(0, task_ctx).unwrap();
        let batches = common::collect(stream).await.unwrap();

        allow_duplicates! {
            assert_snapshot!(batches_to_sort_string(&batches), @r#"
            +----+----+----+----+----+----+
            | a1 | b1 | c1 | a2 | b2 | c2 |
            +----+----+----+----+----+----+
            |    |    |    | 30 | 6  | 90 |
            |    |    |    | 30 | 6  | 90 |
            | 1  | 4  | 7  | 10 | 4  | 70 |
            | 1  | 4  | 7  | 10 | 4  | 70 |
            | 2  | 5  | 8  | 20 | 5  | 80 |
            | 2  | 5  | 8  | 20 | 5  | 80 |
            | 3  | 7  | 9  |    |    |    |
            +----+----+----+----+----+----+
                "#);
        }
    }

    #[apply(batch_sizes)]
    #[tokio::test]
    async fn join_left_empty_right(batch_size: usize) {
        let task_ctx = prepare_task_ctx(batch_size);
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![4, 5, 7]),
            ("c1", &vec![7, 8, 9]),
        );
        let right = build_table_i32(("a2", &vec![]), ("b1", &vec![]), ("c2", &vec![]));
        let on = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema()).unwrap()) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema()).unwrap()) as _,
        )];
        let schema = right.schema();
        let right = TestMemoryExec::try_new_exec(&[vec![right]], schema, None).unwrap();
        let join = join(
            left,
            right,
            on,
            &JoinType::Left,
            NullEquality::NullEqualsNothing,
        )
        .unwrap();

        let columns = columns(&join.schema());
        assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b1", "c2"]);

        let stream = join.execute(0, task_ctx).unwrap();
        let batches = common::collect(stream).await.unwrap();

        allow_duplicates! {
            assert_snapshot!(batches_to_sort_string(&batches), @r#"
            +----+----+----+----+----+----+
            | a1 | b1 | c1 | a2 | b1 | c2 |
            +----+----+----+----+----+----+
            | 1  | 4  | 7  |    |    |    |
            | 2  | 5  | 8  |    |    |    |
            | 3  | 7  | 9  |    |    |    |
            +----+----+----+----+----+----+
                "#);
        }
    }

    #[apply(batch_sizes)]
    #[tokio::test]
    async fn join_full_empty_right(batch_size: usize) {
        let task_ctx = prepare_task_ctx(batch_size);
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![4, 5, 7]),
            ("c1", &vec![7, 8, 9]),
        );
        let right = build_table_i32(("a2", &vec![]), ("b2", &vec![]), ("c2", &vec![]));
        let on = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema()).unwrap()) as _,
            Arc::new(Column::new_with_schema("b2", &right.schema()).unwrap()) as _,
        )];
        let schema = right.schema();
        let right = TestMemoryExec::try_new_exec(&[vec![right]], schema, None).unwrap();
        let join = join(
            left,
            right,
            on,
            &JoinType::Full,
            NullEquality::NullEqualsNothing,
        )
        .unwrap();

        let columns = columns(&join.schema());
        assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b2", "c2"]);

        let stream = join.execute(0, task_ctx).unwrap();
        let batches = common::collect(stream).await.unwrap();

        allow_duplicates! {
            assert_snapshot!(batches_to_sort_string(&batches), @r#"
            +----+----+----+----+----+----+
            | a1 | b1 | c1 | a2 | b2 | c2 |
            +----+----+----+----+----+----+
            | 1  | 4  | 7  |    |    |    |
            | 2  | 5  | 8  |    |    |    |
            | 3  | 7  | 9  |    |    |    |
            +----+----+----+----+----+----+
                "#);
        }
    }

    #[apply(batch_sizes)]
    #[tokio::test]
    async fn join_left_one(batch_size: usize) -> Result<()> {
        let task_ctx = prepare_task_ctx(batch_size);
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
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        )];

        let (columns, batches, metrics) = join_collect(
            Arc::clone(&left),
            Arc::clone(&right),
            on.clone(),
            &JoinType::Left,
            NullEquality::NullEqualsNothing,
            task_ctx,
        )
        .await?;

        assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b1", "c2"]);

        allow_duplicates! {
            assert_snapshot!(batches_to_sort_string(&batches), @r#"
            +----+----+----+----+----+----+
            | a1 | b1 | c1 | a2 | b1 | c2 |
            +----+----+----+----+----+----+
            | 1  | 4  | 7  | 10 | 4  | 70 |
            | 2  | 5  | 8  | 20 | 5  | 80 |
            | 3  | 7  | 9  |    |    |    |
            +----+----+----+----+----+----+
                "#);
        }

        assert_join_metrics!(metrics, 3);

        Ok(())
    }

    #[apply(batch_sizes)]
    #[tokio::test]
    async fn partitioned_join_left_one(batch_size: usize) -> Result<()> {
        let task_ctx = prepare_task_ctx(batch_size);
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
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        )];

        let (columns, batches, metrics) = partitioned_join_collect(
            Arc::clone(&left),
            Arc::clone(&right),
            on.clone(),
            &JoinType::Left,
            NullEquality::NullEqualsNothing,
            task_ctx,
        )
        .await?;

        assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b1", "c2"]);

        allow_duplicates! {
            assert_snapshot!(batches_to_sort_string(&batches), @r#"
            +----+----+----+----+----+----+
            | a1 | b1 | c1 | a2 | b1 | c2 |
            +----+----+----+----+----+----+
            | 1  | 4  | 7  | 10 | 4  | 70 |
            | 2  | 5  | 8  | 20 | 5  | 80 |
            | 3  | 7  | 9  |    |    |    |
            +----+----+----+----+----+----+
                "#);
        }

        assert_join_metrics!(metrics, 3);

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

    #[apply(batch_sizes)]
    #[tokio::test]
    async fn join_left_semi(batch_size: usize) -> Result<()> {
        let task_ctx = prepare_task_ctx(batch_size);
        let left = build_semi_anti_left_table();
        let right = build_semi_anti_right_table();
        // left_table left semi join right_table on left_table.b1 = right_table.b2
        let on = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b2", &right.schema())?) as _,
        )];

        let join = join(
            left,
            right,
            on,
            &JoinType::LeftSemi,
            NullEquality::NullEqualsNothing,
        )?;

        let columns = columns(&join.schema());
        assert_eq!(columns, vec!["a1", "b1", "c1"]);

        let stream = join.execute(0, task_ctx)?;
        let batches = common::collect(stream).await?;

        // ignore the order
        allow_duplicates! {
            assert_snapshot!(batches_to_sort_string(&batches), @r#"
            +----+----+-----+
            | a1 | b1 | c1  |
            +----+----+-----+
            | 11 | 8  | 110 |
            | 13 | 10 | 130 |
            | 9  | 8  | 90  |
            +----+----+-----+
                "#);
        }

        Ok(())
    }

    #[apply(batch_sizes)]
    #[tokio::test]
    async fn join_left_semi_with_filter(batch_size: usize) -> Result<()> {
        let task_ctx = prepare_task_ctx(batch_size);
        let left = build_semi_anti_left_table();
        let right = build_semi_anti_right_table();

        // left_table left semi join right_table on left_table.b1 = right_table.b2 and right_table.a2 != 10
        let on = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b2", &right.schema())?) as _,
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
            Arc::new(intermediate_schema.clone()),
        );

        let join = join_with_filter(
            Arc::clone(&left),
            Arc::clone(&right),
            on.clone(),
            filter,
            &JoinType::LeftSemi,
            NullEquality::NullEqualsNothing,
        )?;

        let columns_header = columns(&join.schema());
        assert_eq!(columns_header.clone(), vec!["a1", "b1", "c1"]);

        let stream = join.execute(0, Arc::clone(&task_ctx))?;
        let batches = common::collect(stream).await?;

        allow_duplicates! {
            assert_snapshot!(batches_to_sort_string(&batches), @r"
            +----+----+-----+
            | a1 | b1 | c1  |
            +----+----+-----+
            | 11 | 8  | 110 |
            | 13 | 10 | 130 |
            | 9  | 8  | 90  |
            +----+----+-----+
            ");
        }

        // left_table left semi join right_table on left_table.b1 = right_table.b2 and right_table.a2 > 10
        let filter_expression = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("x", 0)),
            Operator::Gt,
            Arc::new(Literal::new(ScalarValue::Int32(Some(10)))),
        )) as Arc<dyn PhysicalExpr>;
        let filter = JoinFilter::new(
            filter_expression,
            column_indices,
            Arc::new(intermediate_schema),
        );

        let join = join_with_filter(
            left,
            right,
            on,
            filter,
            &JoinType::LeftSemi,
            NullEquality::NullEqualsNothing,
        )?;

        let columns_header = columns(&join.schema());
        assert_eq!(columns_header, vec!["a1", "b1", "c1"]);

        let stream = join.execute(0, task_ctx)?;
        let batches = common::collect(stream).await?;

        allow_duplicates! {
            assert_snapshot!(batches_to_sort_string(&batches), @r#"
            +----+----+-----+
            | a1 | b1 | c1  |
            +----+----+-----+
            | 13 | 10 | 130 |
            +----+----+-----+
                "#);
        }

        Ok(())
    }

    #[apply(batch_sizes)]
    #[tokio::test]
    async fn join_right_semi(batch_size: usize) -> Result<()> {
        let task_ctx = prepare_task_ctx(batch_size);
        let left = build_semi_anti_left_table();
        let right = build_semi_anti_right_table();

        // left_table right semi join right_table on left_table.b1 = right_table.b2
        let on = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b2", &right.schema())?) as _,
        )];

        let join = join(
            left,
            right,
            on,
            &JoinType::RightSemi,
            NullEquality::NullEqualsNothing,
        )?;

        let columns = columns(&join.schema());
        assert_eq!(columns, vec!["a2", "b2", "c2"]);

        let stream = join.execute(0, task_ctx)?;
        let batches = common::collect(stream).await?;

        // RightSemi join output is expected to preserve right input order
        allow_duplicates! {
            assert_snapshot!(batches_to_string(&batches), @r#"
            +----+----+-----+
            | a2 | b2 | c2  |
            +----+----+-----+
            | 8  | 8  | 20  |
            | 12 | 10 | 40  |
            | 10 | 10 | 100 |
            +----+----+-----+
                "#);
        }

        Ok(())
    }

    #[apply(batch_sizes)]
    #[tokio::test]
    async fn join_right_semi_with_filter(batch_size: usize) -> Result<()> {
        let task_ctx = prepare_task_ctx(batch_size);
        let left = build_semi_anti_left_table();
        let right = build_semi_anti_right_table();

        // left_table right semi join right_table on left_table.b1 = right_table.b2 on left_table.a1!=9
        let on = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b2", &right.schema())?) as _,
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
            Arc::new(intermediate_schema.clone()),
        );

        let join = join_with_filter(
            Arc::clone(&left),
            Arc::clone(&right),
            on.clone(),
            filter,
            &JoinType::RightSemi,
            NullEquality::NullEqualsNothing,
        )?;

        let columns = columns(&join.schema());
        assert_eq!(columns, vec!["a2", "b2", "c2"]);

        let stream = join.execute(0, Arc::clone(&task_ctx))?;
        let batches = common::collect(stream).await?;

        // RightSemi join output is expected to preserve right input order
        allow_duplicates! {
            assert_snapshot!(batches_to_string(&batches), @r#"
            +----+----+-----+
            | a2 | b2 | c2  |
            +----+----+-----+
            | 8  | 8  | 20  |
            | 12 | 10 | 40  |
            | 10 | 10 | 100 |
            +----+----+-----+
                "#);
        }

        // left_table right semi join right_table on left_table.b1 = right_table.b2 on left_table.a1!=9
        let filter_expression = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("x", 0)),
            Operator::Gt,
            Arc::new(Literal::new(ScalarValue::Int32(Some(11)))),
        )) as Arc<dyn PhysicalExpr>;

        let filter = JoinFilter::new(
            filter_expression,
            column_indices,
            Arc::new(intermediate_schema.clone()),
        );

        let join = join_with_filter(
            left,
            right,
            on,
            filter,
            &JoinType::RightSemi,
            NullEquality::NullEqualsNothing,
        )?;
        let stream = join.execute(0, task_ctx)?;
        let batches = common::collect(stream).await?;

        // RightSemi join output is expected to preserve right input order
        allow_duplicates! {
            assert_snapshot!(batches_to_string(&batches), @r#"
            +----+----+-----+
            | a2 | b2 | c2  |
            +----+----+-----+
            | 12 | 10 | 40  |
            | 10 | 10 | 100 |
            +----+----+-----+
                "#);
        }

        Ok(())
    }

    #[apply(batch_sizes)]
    #[tokio::test]
    async fn join_left_anti(batch_size: usize) -> Result<()> {
        let task_ctx = prepare_task_ctx(batch_size);
        let left = build_semi_anti_left_table();
        let right = build_semi_anti_right_table();
        // left_table left anti join right_table on left_table.b1 = right_table.b2
        let on = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b2", &right.schema())?) as _,
        )];

        let join = join(
            left,
            right,
            on,
            &JoinType::LeftAnti,
            NullEquality::NullEqualsNothing,
        )?;

        let columns = columns(&join.schema());
        assert_eq!(columns, vec!["a1", "b1", "c1"]);

        let stream = join.execute(0, task_ctx)?;
        let batches = common::collect(stream).await?;

        allow_duplicates! {
            assert_snapshot!(batches_to_sort_string(&batches), @r#"
            +----+----+----+
            | a1 | b1 | c1 |
            +----+----+----+
            | 1  | 1  | 10 |
            | 3  | 3  | 30 |
            | 5  | 5  | 50 |
            | 7  | 7  | 70 |
            +----+----+----+
                "#);
        }
        Ok(())
    }

    #[apply(batch_sizes)]
    #[tokio::test]
    async fn join_left_anti_with_filter(batch_size: usize) -> Result<()> {
        let task_ctx = prepare_task_ctx(batch_size);
        let left = build_semi_anti_left_table();
        let right = build_semi_anti_right_table();
        // left_table left anti join right_table on left_table.b1 = right_table.b2 and right_table.a2!=8
        let on = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b2", &right.schema())?) as _,
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
            Arc::new(intermediate_schema.clone()),
        );

        let join = join_with_filter(
            Arc::clone(&left),
            Arc::clone(&right),
            on.clone(),
            filter,
            &JoinType::LeftAnti,
            NullEquality::NullEqualsNothing,
        )?;

        let columns_header = columns(&join.schema());
        assert_eq!(columns_header, vec!["a1", "b1", "c1"]);

        let stream = join.execute(0, Arc::clone(&task_ctx))?;
        let batches = common::collect(stream).await?;

        allow_duplicates! {
            assert_snapshot!(batches_to_sort_string(&batches), @r#"
            +----+----+-----+
            | a1 | b1 | c1  |
            +----+----+-----+
            | 1  | 1  | 10  |
            | 11 | 8  | 110 |
            | 3  | 3  | 30  |
            | 5  | 5  | 50  |
            | 7  | 7  | 70  |
            | 9  | 8  | 90  |
            +----+----+-----+
                "#);
        }

        // left_table left anti join right_table on left_table.b1 = right_table.b2 and right_table.a2 != 13
        let filter_expression = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("x", 0)),
            Operator::NotEq,
            Arc::new(Literal::new(ScalarValue::Int32(Some(8)))),
        )) as Arc<dyn PhysicalExpr>;

        let filter = JoinFilter::new(
            filter_expression,
            column_indices,
            Arc::new(intermediate_schema),
        );

        let join = join_with_filter(
            left,
            right,
            on,
            filter,
            &JoinType::LeftAnti,
            NullEquality::NullEqualsNothing,
        )?;

        let columns_header = columns(&join.schema());
        assert_eq!(columns_header, vec!["a1", "b1", "c1"]);

        let stream = join.execute(0, task_ctx)?;
        let batches = common::collect(stream).await?;

        allow_duplicates! {
            assert_snapshot!(batches_to_sort_string(&batches), @r#"
            +----+----+-----+
            | a1 | b1 | c1  |
            +----+----+-----+
            | 1  | 1  | 10  |
            | 11 | 8  | 110 |
            | 3  | 3  | 30  |
            | 5  | 5  | 50  |
            | 7  | 7  | 70  |
            | 9  | 8  | 90  |
            +----+----+-----+
                "#);
        }

        Ok(())
    }

    #[apply(batch_sizes)]
    #[tokio::test]
    async fn join_right_anti(batch_size: usize) -> Result<()> {
        let task_ctx = prepare_task_ctx(batch_size);
        let left = build_semi_anti_left_table();
        let right = build_semi_anti_right_table();
        let on = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b2", &right.schema())?) as _,
        )];

        let join = join(
            left,
            right,
            on,
            &JoinType::RightAnti,
            NullEquality::NullEqualsNothing,
        )?;

        let columns = columns(&join.schema());
        assert_eq!(columns, vec!["a2", "b2", "c2"]);

        let stream = join.execute(0, task_ctx)?;
        let batches = common::collect(stream).await?;

        // RightAnti join output is expected to preserve right input order
        allow_duplicates! {
            assert_snapshot!(batches_to_string(&batches), @r#"
            +----+----+-----+
            | a2 | b2 | c2  |
            +----+----+-----+
            | 6  | 6  | 60  |
            | 2  | 2  | 80  |
            | 4  | 4  | 120 |
            +----+----+-----+
                "#);
        }
        Ok(())
    }

    #[apply(batch_sizes)]
    #[tokio::test]
    async fn join_right_anti_with_filter(batch_size: usize) -> Result<()> {
        let task_ctx = prepare_task_ctx(batch_size);
        let left = build_semi_anti_left_table();
        let right = build_semi_anti_right_table();
        // left_table right anti join right_table on left_table.b1 = right_table.b2 and left_table.a1!=13
        let on = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b2", &right.schema())?) as _,
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
            Arc::new(intermediate_schema.clone()),
        );

        let join = join_with_filter(
            Arc::clone(&left),
            Arc::clone(&right),
            on.clone(),
            filter,
            &JoinType::RightAnti,
            NullEquality::NullEqualsNothing,
        )?;

        let columns_header = columns(&join.schema());
        assert_eq!(columns_header, vec!["a2", "b2", "c2"]);

        let stream = join.execute(0, Arc::clone(&task_ctx))?;
        let batches = common::collect(stream).await?;

        // RightAnti join output is expected to preserve right input order
        allow_duplicates! {
            assert_snapshot!(batches_to_string(&batches), @r#"
            +----+----+-----+
            | a2 | b2 | c2  |
            +----+----+-----+
            | 12 | 10 | 40  |
            | 6  | 6  | 60  |
            | 2  | 2  | 80  |
            | 10 | 10 | 100 |
            | 4  | 4  | 120 |
            +----+----+-----+
                "#);
        }

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

        let filter = JoinFilter::new(
            filter_expression,
            column_indices,
            Arc::new(intermediate_schema),
        );

        let join = join_with_filter(
            left,
            right,
            on,
            filter,
            &JoinType::RightAnti,
            NullEquality::NullEqualsNothing,
        )?;

        let columns_header = columns(&join.schema());
        assert_eq!(columns_header, vec!["a2", "b2", "c2"]);

        let stream = join.execute(0, task_ctx)?;
        let batches = common::collect(stream).await?;

        // RightAnti join output is expected to preserve right input order
        allow_duplicates! {
            assert_snapshot!(batches_to_string(&batches), @r#"
            +----+----+-----+
            | a2 | b2 | c2  |
            +----+----+-----+
            | 8  | 8  | 20  |
            | 6  | 6  | 60  |
            | 2  | 2  | 80  |
            | 4  | 4  | 120 |
            +----+----+-----+
                "#);
        }

        Ok(())
    }

    #[apply(batch_sizes)]
    #[tokio::test]
    async fn join_right_one(batch_size: usize) -> Result<()> {
        let task_ctx = prepare_task_ctx(batch_size);
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
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        )];

        let (columns, batches, metrics) = join_collect(
            left,
            right,
            on,
            &JoinType::Right,
            NullEquality::NullEqualsNothing,
            task_ctx,
        )
        .await?;

        assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b1", "c2"]);

        allow_duplicates! {
            assert_snapshot!(batches_to_sort_string(&batches), @r#"
            +----+----+----+----+----+----+
            | a1 | b1 | c1 | a2 | b1 | c2 |
            +----+----+----+----+----+----+
            |    |    |    | 30 | 6  | 90 |
            | 1  | 4  | 7  | 10 | 4  | 70 |
            | 2  | 5  | 8  | 20 | 5  | 80 |
            +----+----+----+----+----+----+
                "#);
        }

        assert_join_metrics!(metrics, 3);

        Ok(())
    }

    #[apply(batch_sizes)]
    #[tokio::test]
    async fn partitioned_join_right_one(batch_size: usize) -> Result<()> {
        let task_ctx = prepare_task_ctx(batch_size);
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
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        )];

        let (columns, batches, metrics) = partitioned_join_collect(
            left,
            right,
            on,
            &JoinType::Right,
            NullEquality::NullEqualsNothing,
            task_ctx,
        )
        .await?;

        assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b1", "c2"]);

        allow_duplicates! {
            assert_snapshot!(batches_to_sort_string(&batches), @r#"
            +----+----+----+----+----+----+
            | a1 | b1 | c1 | a2 | b1 | c2 |
            +----+----+----+----+----+----+
            |    |    |    | 30 | 6  | 90 |
            | 1  | 4  | 7  | 10 | 4  | 70 |
            | 2  | 5  | 8  | 20 | 5  | 80 |
            +----+----+----+----+----+----+
                "#);
        }

        assert_join_metrics!(metrics, 3);

        Ok(())
    }

    #[apply(batch_sizes)]
    #[tokio::test]
    async fn join_full_one(batch_size: usize) -> Result<()> {
        let task_ctx = prepare_task_ctx(batch_size);
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
            Arc::new(Column::new_with_schema("b1", &left.schema()).unwrap()) as _,
            Arc::new(Column::new_with_schema("b2", &right.schema()).unwrap()) as _,
        )];

        let join = join(
            left,
            right,
            on,
            &JoinType::Full,
            NullEquality::NullEqualsNothing,
        )?;

        let columns = columns(&join.schema());
        assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b2", "c2"]);

        let stream = join.execute(0, task_ctx)?;
        let batches = common::collect(stream).await?;

        allow_duplicates! {
            assert_snapshot!(batches_to_sort_string(&batches), @r#"
            +----+----+----+----+----+----+
            | a1 | b1 | c1 | a2 | b2 | c2 |
            +----+----+----+----+----+----+
            |    |    |    | 30 | 6  | 90 |
            | 1  | 4  | 7  | 10 | 4  | 70 |
            | 2  | 5  | 8  | 20 | 5  | 80 |
            | 3  | 7  | 9  |    |    |    |
            +----+----+----+----+----+----+
                "#);
        }

        Ok(())
    }

    #[apply(batch_sizes)]
    #[tokio::test]
    async fn join_left_mark(batch_size: usize) -> Result<()> {
        let task_ctx = prepare_task_ctx(batch_size);
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
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        )];

        let (columns, batches, metrics) = join_collect(
            Arc::clone(&left),
            Arc::clone(&right),
            on.clone(),
            &JoinType::LeftMark,
            NullEquality::NullEqualsNothing,
            task_ctx,
        )
        .await?;

        assert_eq!(columns, vec!["a1", "b1", "c1", "mark"]);

        allow_duplicates! {
            assert_snapshot!(batches_to_sort_string(&batches), @r#"
            +----+----+----+-------+
            | a1 | b1 | c1 | mark  |
            +----+----+----+-------+
            | 1  | 4  | 7  | true  |
            | 2  | 5  | 8  | true  |
            | 3  | 7  | 9  | false |
            +----+----+----+-------+
                "#);
        }

        assert_join_metrics!(metrics, 3);

        Ok(())
    }

    #[apply(batch_sizes)]
    #[tokio::test]
    async fn partitioned_join_left_mark(batch_size: usize) -> Result<()> {
        let task_ctx = prepare_task_ctx(batch_size);
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![4, 5, 7]), // 7 does not exist on the right
            ("c1", &vec![7, 8, 9]),
        );
        let right = build_table(
            ("a2", &vec![10, 20, 30, 40]),
            ("b1", &vec![4, 4, 5, 6]),
            ("c2", &vec![60, 70, 80, 90]),
        );
        let on = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        )];

        let (columns, batches, metrics) = partitioned_join_collect(
            Arc::clone(&left),
            Arc::clone(&right),
            on.clone(),
            &JoinType::LeftMark,
            NullEquality::NullEqualsNothing,
            task_ctx,
        )
        .await?;

        assert_eq!(columns, vec!["a1", "b1", "c1", "mark"]);

        allow_duplicates! {
            assert_snapshot!(batches_to_sort_string(&batches), @r#"
            +----+----+----+-------+
            | a1 | b1 | c1 | mark  |
            +----+----+----+-------+
            | 1  | 4  | 7  | true  |
            | 2  | 5  | 8  | true  |
            | 3  | 7  | 9  | false |
            +----+----+----+-------+
                "#);
        }

        assert_join_metrics!(metrics, 3);

        Ok(())
    }

    #[apply(batch_sizes)]
    #[tokio::test]
    async fn join_right_mark(batch_size: usize) -> Result<()> {
        let task_ctx = prepare_task_ctx(batch_size);
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![4, 5, 7]), // 7 does not exist on the right
            ("c1", &vec![7, 8, 9]),
        );
        let right = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![4, 5, 6]), // 6 does not exist on the left
            ("c2", &vec![70, 80, 90]),
        );
        let on = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        )];

        let (columns, batches, metrics) = join_collect(
            Arc::clone(&left),
            Arc::clone(&right),
            on.clone(),
            &JoinType::RightMark,
            NullEquality::NullEqualsNothing,
            task_ctx,
        )
        .await?;

        assert_eq!(columns, vec!["a2", "b1", "c2", "mark"]);

        let expected = [
            "+----+----+----+-------+",
            "| a2 | b1 | c2 | mark  |",
            "+----+----+----+-------+",
            "| 10 | 4  | 70 | true  |",
            "| 20 | 5  | 80 | true  |",
            "| 30 | 6  | 90 | false |",
            "+----+----+----+-------+",
        ];
        assert_batches_sorted_eq!(expected, &batches);

        assert_join_metrics!(metrics, 3);

        Ok(())
    }

    #[apply(batch_sizes)]
    #[tokio::test]
    async fn partitioned_join_right_mark(batch_size: usize) -> Result<()> {
        let task_ctx = prepare_task_ctx(batch_size);
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![4, 5, 7]), // 7 does not exist on the right
            ("c1", &vec![7, 8, 9]),
        );
        let right = build_table(
            ("a2", &vec![10, 20, 30, 40]),
            ("b1", &vec![4, 4, 5, 6]), // 6 does not exist on the left
            ("c2", &vec![60, 70, 80, 90]),
        );
        let on = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        )];

        let (columns, batches, metrics) = partitioned_join_collect(
            Arc::clone(&left),
            Arc::clone(&right),
            on.clone(),
            &JoinType::RightMark,
            NullEquality::NullEqualsNothing,
            task_ctx,
        )
        .await?;

        assert_eq!(columns, vec!["a2", "b1", "c2", "mark"]);

        let expected = [
            "+----+----+----+-------+",
            "| a2 | b1 | c2 | mark  |",
            "+----+----+----+-------+",
            "| 10 | 4  | 60 | true  |",
            "| 20 | 4  | 70 | true  |",
            "| 30 | 5  | 80 | true  |",
            "| 40 | 6  | 90 | false |",
            "+----+----+----+-------+",
        ];
        assert_batches_sorted_eq!(expected, &batches);

        assert_join_metrics!(metrics, 4);

        Ok(())
    }

    #[test]
    fn join_with_hash_collisions_64() -> Result<()> {
        let mut hashmap_left = HashTable::with_capacity(4);
        let left = build_table_i32(
            ("a", &vec![10, 20]),
            ("x", &vec![100, 200]),
            ("y", &vec![200, 300]),
        );

        let random_state = RandomState::with_seeds(0, 0, 0, 0);
        let hashes_buff = &mut vec![0; left.num_rows()];
        let hashes = create_hashes([&left.columns()[0]], &random_state, hashes_buff)?;

        // Maps both values to both indices (1 and 2, representing input 0 and 1)
        // 0 -> (0, 1)
        // 1 -> (0, 2)
        // The equality check will make sure only hashes[0] maps to 0 and hashes[1] maps to 1
        hashmap_left.insert_unique(hashes[0], (hashes[0], 1), |(h, _)| *h);
        hashmap_left.insert_unique(hashes[0], (hashes[0], 2), |(h, _)| *h);

        hashmap_left.insert_unique(hashes[1], (hashes[1], 1), |(h, _)| *h);
        hashmap_left.insert_unique(hashes[1], (hashes[1], 2), |(h, _)| *h);

        let next = vec![2, 0];

        let right = build_table_i32(
            ("a", &vec![10, 20]),
            ("b", &vec![0, 0]),
            ("c", &vec![30, 40]),
        );

        // Join key column for both join sides
        let key_column: PhysicalExprRef = Arc::new(Column::new("a", 0)) as _;

        let join_hash_map = JoinHashMapU64::new(hashmap_left, next);

        let left_keys_values = key_column.evaluate(&left)?.into_array(left.num_rows())?;
        let right_keys_values =
            key_column.evaluate(&right)?.into_array(right.num_rows())?;
        let mut hashes_buffer = vec![0; right.num_rows()];
        create_hashes([&right_keys_values], &random_state, &mut hashes_buffer)?;

        let (l, r, _) = lookup_join_hashmap(
            &join_hash_map,
            &[left_keys_values],
            &[right_keys_values],
            NullEquality::NullEqualsNothing,
            &hashes_buffer,
            8192,
            (0, None),
        )?;

        let left_ids: UInt64Array = vec![0, 1].into();

        let right_ids: UInt32Array = vec![0, 1].into();

        assert_eq!(left_ids, l);

        assert_eq!(right_ids, r);

        Ok(())
    }

    #[test]
    fn join_with_hash_collisions_u32() -> Result<()> {
        let mut hashmap_left = HashTable::with_capacity(4);
        let left = build_table_i32(
            ("a", &vec![10, 20]),
            ("x", &vec![100, 200]),
            ("y", &vec![200, 300]),
        );

        let random_state = RandomState::with_seeds(0, 0, 0, 0);
        let hashes_buff = &mut vec![0; left.num_rows()];
        let hashes = create_hashes([&left.columns()[0]], &random_state, hashes_buff)?;

        hashmap_left.insert_unique(hashes[0], (hashes[0], 1u32), |(h, _)| *h);
        hashmap_left.insert_unique(hashes[0], (hashes[0], 2u32), |(h, _)| *h);
        hashmap_left.insert_unique(hashes[1], (hashes[1], 1u32), |(h, _)| *h);
        hashmap_left.insert_unique(hashes[1], (hashes[1], 2u32), |(h, _)| *h);

        let next: Vec<u32> = vec![2, 0];

        let right = build_table_i32(
            ("a", &vec![10, 20]),
            ("b", &vec![0, 0]),
            ("c", &vec![30, 40]),
        );

        let key_column: PhysicalExprRef = Arc::new(Column::new("a", 0)) as _;

        let join_hash_map = JoinHashMapU32::new(hashmap_left, next);

        let left_keys_values = key_column.evaluate(&left)?.into_array(left.num_rows())?;
        let right_keys_values =
            key_column.evaluate(&right)?.into_array(right.num_rows())?;
        let mut hashes_buffer = vec![0; right.num_rows()];
        create_hashes([&right_keys_values], &random_state, &mut hashes_buffer)?;

        let (l, r, _) = lookup_join_hashmap(
            &join_hash_map,
            &[left_keys_values],
            &[right_keys_values],
            NullEquality::NullEqualsNothing,
            &hashes_buffer,
            8192,
            (0, None),
        )?;

        // We still expect to match rows 0 and 1 on both sides
        let left_ids: UInt64Array = vec![0, 1].into();
        let right_ids: UInt32Array = vec![0, 1].into();

        assert_eq!(left_ids, l);
        assert_eq!(right_ids, r);

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
            Arc::new(Column::new_with_schema("a", &left.schema()).unwrap()) as _,
            Arc::new(Column::new_with_schema("b", &right.schema()).unwrap()) as _,
        )];

        let join = join(
            left,
            right,
            on,
            &JoinType::Inner,
            NullEquality::NullEqualsNothing,
        )?;

        let columns = columns(&join.schema());
        assert_eq!(columns, vec!["a", "b", "c", "a", "b", "c"]);

        let stream = join.execute(0, task_ctx)?;
        let batches = common::collect(stream).await?;

        allow_duplicates! {
            assert_snapshot!(batches_to_sort_string(&batches), @r#"
            +---+---+---+----+---+----+
            | a | b | c | a  | b | c  |
            +---+---+---+----+---+----+
            | 1 | 4 | 7 | 10 | 1 | 70 |
            | 2 | 5 | 8 | 20 | 2 | 80 |
            +---+---+---+----+---+----+
                "#);
        }

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

        JoinFilter::new(
            filter_expression,
            column_indices,
            Arc::new(intermediate_schema),
        )
    }

    #[apply(batch_sizes)]
    #[tokio::test]
    async fn join_inner_with_filter(batch_size: usize) -> Result<()> {
        let task_ctx = prepare_task_ctx(batch_size);
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
            Arc::new(Column::new_with_schema("a", &left.schema()).unwrap()) as _,
            Arc::new(Column::new_with_schema("b", &right.schema()).unwrap()) as _,
        )];
        let filter = prepare_join_filter();

        let join = join_with_filter(
            left,
            right,
            on,
            filter,
            &JoinType::Inner,
            NullEquality::NullEqualsNothing,
        )?;

        let columns = columns(&join.schema());
        assert_eq!(columns, vec!["a", "b", "c", "a", "b", "c"]);

        let stream = join.execute(0, task_ctx)?;
        let batches = common::collect(stream).await?;

        allow_duplicates! {
            assert_snapshot!(batches_to_sort_string(&batches), @r#"
            +---+---+---+----+---+---+
            | a | b | c | a  | b | c |
            +---+---+---+----+---+---+
            | 2 | 7 | 9 | 10 | 2 | 7 |
            | 2 | 7 | 9 | 20 | 2 | 5 |
            +---+---+---+----+---+---+
                "#);
        }

        Ok(())
    }

    #[apply(batch_sizes)]
    #[tokio::test]
    async fn join_left_with_filter(batch_size: usize) -> Result<()> {
        let task_ctx = prepare_task_ctx(batch_size);
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
            Arc::new(Column::new_with_schema("a", &left.schema()).unwrap()) as _,
            Arc::new(Column::new_with_schema("b", &right.schema()).unwrap()) as _,
        )];
        let filter = prepare_join_filter();

        let join = join_with_filter(
            left,
            right,
            on,
            filter,
            &JoinType::Left,
            NullEquality::NullEqualsNothing,
        )?;

        let columns = columns(&join.schema());
        assert_eq!(columns, vec!["a", "b", "c", "a", "b", "c"]);

        let stream = join.execute(0, task_ctx)?;
        let batches = common::collect(stream).await?;

        allow_duplicates! {
            assert_snapshot!(batches_to_sort_string(&batches), @r#"
            +---+---+---+----+---+---+
            | a | b | c | a  | b | c |
            +---+---+---+----+---+---+
            | 0 | 4 | 7 |    |   |   |
            | 1 | 5 | 8 |    |   |   |
            | 2 | 7 | 9 | 10 | 2 | 7 |
            | 2 | 7 | 9 | 20 | 2 | 5 |
            | 2 | 8 | 1 |    |   |   |
            +---+---+---+----+---+---+
                "#);
        }

        Ok(())
    }

    #[apply(batch_sizes)]
    #[tokio::test]
    async fn join_right_with_filter(batch_size: usize) -> Result<()> {
        let task_ctx = prepare_task_ctx(batch_size);
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
            Arc::new(Column::new_with_schema("a", &left.schema()).unwrap()) as _,
            Arc::new(Column::new_with_schema("b", &right.schema()).unwrap()) as _,
        )];
        let filter = prepare_join_filter();

        let join = join_with_filter(
            left,
            right,
            on,
            filter,
            &JoinType::Right,
            NullEquality::NullEqualsNothing,
        )?;

        let columns = columns(&join.schema());
        assert_eq!(columns, vec!["a", "b", "c", "a", "b", "c"]);

        let stream = join.execute(0, task_ctx)?;
        let batches = common::collect(stream).await?;

        allow_duplicates! {
            assert_snapshot!(batches_to_sort_string(&batches), @r#"
            +---+---+---+----+---+---+
            | a | b | c | a  | b | c |
            +---+---+---+----+---+---+
            |   |   |   | 30 | 3 | 6 |
            |   |   |   | 40 | 4 | 4 |
            | 2 | 7 | 9 | 10 | 2 | 7 |
            | 2 | 7 | 9 | 20 | 2 | 5 |
            +---+---+---+----+---+---+
                "#);
        }

        Ok(())
    }

    #[apply(batch_sizes)]
    #[tokio::test]
    async fn join_full_with_filter(batch_size: usize) -> Result<()> {
        let task_ctx = prepare_task_ctx(batch_size);
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
            Arc::new(Column::new_with_schema("a", &left.schema()).unwrap()) as _,
            Arc::new(Column::new_with_schema("b", &right.schema()).unwrap()) as _,
        )];
        let filter = prepare_join_filter();

        let join = join_with_filter(
            left,
            right,
            on,
            filter,
            &JoinType::Full,
            NullEquality::NullEqualsNothing,
        )?;

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

        // THIS MIGRATION HALTED DUE TO ISSUE #15312
        //allow_duplicates! {
        //    assert_snapshot!(batches_to_sort_string(&batches), @r#"
        //    +---+---+---+----+---+---+
        //    | a | b | c | a  | b | c |
        //    +---+---+---+----+---+---+
        //    |   |   |   | 30 | 3 | 6 |
        //    |   |   |   | 40 | 4 | 4 |
        //    | 2 | 7 | 9 | 10 | 2 | 7 |
        //    | 2 | 7 | 9 | 20 | 2 | 5 |
        //    | 0 | 4 | 7 |    |   |   |
        //    | 1 | 5 | 8 |    |   |   |
        //    | 2 | 8 | 1 |    |   |   |
        //    +---+---+---+----+---+---+
        //        "#)
        //}

        Ok(())
    }

    /// Test for parallelized HashJoinExec with PartitionMode::CollectLeft
    #[tokio::test]
    async fn test_collect_left_multiple_partitions_join() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![4, 5, 7]),
            ("c1", &vec![7, 8, 9]),
        );
        let right = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b2", &vec![4, 5, 6]),
            ("c2", &vec![70, 80, 90]),
        );
        let on = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema()).unwrap()) as _,
            Arc::new(Column::new_with_schema("b2", &right.schema()).unwrap()) as _,
        )];

        let expected_inner = vec![
            "+----+----+----+----+----+----+",
            "| a1 | b1 | c1 | a2 | b2 | c2 |",
            "+----+----+----+----+----+----+",
            "| 1  | 4  | 7  | 10 | 4  | 70 |",
            "| 2  | 5  | 8  | 20 | 5  | 80 |",
            "+----+----+----+----+----+----+",
        ];
        let expected_left = vec![
            "+----+----+----+----+----+----+",
            "| a1 | b1 | c1 | a2 | b2 | c2 |",
            "+----+----+----+----+----+----+",
            "| 1  | 4  | 7  | 10 | 4  | 70 |",
            "| 2  | 5  | 8  | 20 | 5  | 80 |",
            "| 3  | 7  | 9  |    |    |    |",
            "+----+----+----+----+----+----+",
        ];
        let expected_right = vec![
            "+----+----+----+----+----+----+",
            "| a1 | b1 | c1 | a2 | b2 | c2 |",
            "+----+----+----+----+----+----+",
            "|    |    |    | 30 | 6  | 90 |",
            "| 1  | 4  | 7  | 10 | 4  | 70 |",
            "| 2  | 5  | 8  | 20 | 5  | 80 |",
            "+----+----+----+----+----+----+",
        ];
        let expected_full = vec![
            "+----+----+----+----+----+----+",
            "| a1 | b1 | c1 | a2 | b2 | c2 |",
            "+----+----+----+----+----+----+",
            "|    |    |    | 30 | 6  | 90 |",
            "| 1  | 4  | 7  | 10 | 4  | 70 |",
            "| 2  | 5  | 8  | 20 | 5  | 80 |",
            "| 3  | 7  | 9  |    |    |    |",
            "+----+----+----+----+----+----+",
        ];
        let expected_left_semi = vec![
            "+----+----+----+",
            "| a1 | b1 | c1 |",
            "+----+----+----+",
            "| 1  | 4  | 7  |",
            "| 2  | 5  | 8  |",
            "+----+----+----+",
        ];
        let expected_left_anti = vec![
            "+----+----+----+",
            "| a1 | b1 | c1 |",
            "+----+----+----+",
            "| 3  | 7  | 9  |",
            "+----+----+----+",
        ];
        let expected_right_semi = vec![
            "+----+----+----+",
            "| a2 | b2 | c2 |",
            "+----+----+----+",
            "| 10 | 4  | 70 |",
            "| 20 | 5  | 80 |",
            "+----+----+----+",
        ];
        let expected_right_anti = vec![
            "+----+----+----+",
            "| a2 | b2 | c2 |",
            "+----+----+----+",
            "| 30 | 6  | 90 |",
            "+----+----+----+",
        ];
        let expected_left_mark = vec![
            "+----+----+----+-------+",
            "| a1 | b1 | c1 | mark  |",
            "+----+----+----+-------+",
            "| 1  | 4  | 7  | true  |",
            "| 2  | 5  | 8  | true  |",
            "| 3  | 7  | 9  | false |",
            "+----+----+----+-------+",
        ];
        let expected_right_mark = vec![
            "+----+----+----+-------+",
            "| a2 | b2 | c2 | mark  |",
            "+----+----+----+-------+",
            "| 10 | 4  | 70 | true  |",
            "| 20 | 5  | 80 | true  |",
            "| 30 | 6  | 90 | false |",
            "+----+----+----+-------+",
        ];

        let test_cases = vec![
            (JoinType::Inner, expected_inner),
            (JoinType::Left, expected_left),
            (JoinType::Right, expected_right),
            (JoinType::Full, expected_full),
            (JoinType::LeftSemi, expected_left_semi),
            (JoinType::LeftAnti, expected_left_anti),
            (JoinType::RightSemi, expected_right_semi),
            (JoinType::RightAnti, expected_right_anti),
            (JoinType::LeftMark, expected_left_mark),
            (JoinType::RightMark, expected_right_mark),
        ];

        for (join_type, expected) in test_cases {
            let (_, batches, metrics) = join_collect_with_partition_mode(
                Arc::clone(&left),
                Arc::clone(&right),
                on.clone(),
                &join_type,
                PartitionMode::CollectLeft,
                NullEquality::NullEqualsNothing,
                Arc::clone(&task_ctx),
            )
            .await?;
            assert_batches_sorted_eq!(expected, &batches);
            assert_join_metrics!(metrics, expected.len() - 4);
        }

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
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![dates, n])?;
        let left =
            TestMemoryExec::try_new_exec(&[vec![batch]], Arc::clone(&schema), None)
                .unwrap();
        let dates: ArrayRef = Arc::new(Date32Array::from(vec![19108, 19108, 19109]));
        let n: ArrayRef = Arc::new(Int32Array::from(vec![4, 5, 6]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![dates, n])?;
        let right = TestMemoryExec::try_new_exec(&[vec![batch]], schema, None).unwrap();
        let on = vec![(
            Arc::new(Column::new_with_schema("date", &left.schema()).unwrap()) as _,
            Arc::new(Column::new_with_schema("date", &right.schema()).unwrap()) as _,
        )];

        let join = join(
            left,
            right,
            on,
            &JoinType::Inner,
            NullEquality::NullEqualsNothing,
        )?;

        let task_ctx = Arc::new(TaskContext::default());
        let stream = join.execute(0, task_ctx)?;
        let batches = common::collect(stream).await?;

        allow_duplicates! {
            assert_snapshot!(batches_to_sort_string(&batches), @r#"
            +------------+---+------------+---+
            | date       | n | date       | n |
            +------------+---+------------+---+
            | 2022-04-26 | 2 | 2022-04-26 | 4 |
            | 2022-04-26 | 2 | 2022-04-26 | 5 |
            | 2022-04-27 | 3 | 2022-04-27 | 6 |
            +------------+---+------------+---+
                "#);
        }

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
            Arc::new(Column::new_with_schema("b1", &left.schema()).unwrap()) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema()).unwrap()) as _,
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
                Arc::clone(&left),
                Arc::clone(&right_input) as Arc<dyn ExecutionPlan>,
                on.clone(),
                &join_type,
                NullEquality::NullEqualsNothing,
            )
            .unwrap();
            let task_ctx = Arc::new(TaskContext::default());

            let stream = join.execute(0, task_ctx).unwrap();

            // Expect that an error is returned
            let result_string = common::collect(stream).await.unwrap_err().to_string();
            assert!(
                result_string.contains("bad data error"),
                "actual: {result_string}"
            );
        }
    }

    #[tokio::test]
    async fn join_split_batch() {
        let left = build_table(
            ("a1", &vec![1, 2, 3, 4]),
            ("b1", &vec![1, 1, 1, 1]),
            ("c1", &vec![0, 0, 0, 0]),
        );
        let right = build_table(
            ("a2", &vec![10, 20, 30, 40, 50]),
            ("b2", &vec![1, 1, 1, 1, 1]),
            ("c2", &vec![0, 0, 0, 0, 0]),
        );
        let on = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema()).unwrap()) as _,
            Arc::new(Column::new_with_schema("b2", &right.schema()).unwrap()) as _,
        )];

        let join_types = vec![
            JoinType::Inner,
            JoinType::Left,
            JoinType::Right,
            JoinType::Full,
            JoinType::RightSemi,
            JoinType::RightAnti,
            JoinType::LeftSemi,
            JoinType::LeftAnti,
        ];
        let expected_resultset_records = 20;
        let common_result = [
            "+----+----+----+----+----+----+",
            "| a1 | b1 | c1 | a2 | b2 | c2 |",
            "+----+----+----+----+----+----+",
            "| 1  | 1  | 0  | 10 | 1  | 0  |",
            "| 2  | 1  | 0  | 10 | 1  | 0  |",
            "| 3  | 1  | 0  | 10 | 1  | 0  |",
            "| 4  | 1  | 0  | 10 | 1  | 0  |",
            "| 1  | 1  | 0  | 20 | 1  | 0  |",
            "| 2  | 1  | 0  | 20 | 1  | 0  |",
            "| 3  | 1  | 0  | 20 | 1  | 0  |",
            "| 4  | 1  | 0  | 20 | 1  | 0  |",
            "| 1  | 1  | 0  | 30 | 1  | 0  |",
            "| 2  | 1  | 0  | 30 | 1  | 0  |",
            "| 3  | 1  | 0  | 30 | 1  | 0  |",
            "| 4  | 1  | 0  | 30 | 1  | 0  |",
            "| 1  | 1  | 0  | 40 | 1  | 0  |",
            "| 2  | 1  | 0  | 40 | 1  | 0  |",
            "| 3  | 1  | 0  | 40 | 1  | 0  |",
            "| 4  | 1  | 0  | 40 | 1  | 0  |",
            "| 1  | 1  | 0  | 50 | 1  | 0  |",
            "| 2  | 1  | 0  | 50 | 1  | 0  |",
            "| 3  | 1  | 0  | 50 | 1  | 0  |",
            "| 4  | 1  | 0  | 50 | 1  | 0  |",
            "+----+----+----+----+----+----+",
        ];
        let left_batch = [
            "+----+----+----+",
            "| a1 | b1 | c1 |",
            "+----+----+----+",
            "| 1  | 1  | 0  |",
            "| 2  | 1  | 0  |",
            "| 3  | 1  | 0  |",
            "| 4  | 1  | 0  |",
            "+----+----+----+",
        ];
        let right_batch = [
            "+----+----+----+",
            "| a2 | b2 | c2 |",
            "+----+----+----+",
            "| 10 | 1  | 0  |",
            "| 20 | 1  | 0  |",
            "| 30 | 1  | 0  |",
            "| 40 | 1  | 0  |",
            "| 50 | 1  | 0  |",
            "+----+----+----+",
        ];
        let right_empty = [
            "+----+----+----+",
            "| a2 | b2 | c2 |",
            "+----+----+----+",
            "+----+----+----+",
        ];
        let left_empty = [
            "+----+----+----+",
            "| a1 | b1 | c1 |",
            "+----+----+----+",
            "+----+----+----+",
        ];

        // validation of partial join results output for different batch_size setting
        for join_type in join_types {
            for batch_size in (1..21).rev() {
                let task_ctx = prepare_task_ctx(batch_size);

                let join = join(
                    Arc::clone(&left),
                    Arc::clone(&right),
                    on.clone(),
                    &join_type,
                    NullEquality::NullEqualsNothing,
                )
                .unwrap();

                let stream = join.execute(0, task_ctx).unwrap();
                let batches = common::collect(stream).await.unwrap();

                // For inner/right join expected batch count equals dev_ceil result,
                // as there is no need to append non-joined build side data.
                // For other join types it'll be div_ceil + 1 -- for additional batch
                // containing not visited build side rows (empty in this test case).
                let expected_batch_count = match join_type {
                    JoinType::Inner
                    | JoinType::Right
                    | JoinType::RightSemi
                    | JoinType::RightAnti => {
                        div_ceil(expected_resultset_records, batch_size)
                    }
                    _ => div_ceil(expected_resultset_records, batch_size) + 1,
                };
                assert_eq!(
                    batches.len(),
                    expected_batch_count,
                    "expected {expected_batch_count} output batches for {join_type} join with batch_size = {batch_size}"
                );

                let expected = match join_type {
                    JoinType::RightSemi => right_batch.to_vec(),
                    JoinType::RightAnti => right_empty.to_vec(),
                    JoinType::LeftSemi => left_batch.to_vec(),
                    JoinType::LeftAnti => left_empty.to_vec(),
                    _ => common_result.to_vec(),
                };
                assert_batches_eq!(expected, &batches);
            }
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
            Arc::new(Column::new_with_schema("a1", &left.schema()).unwrap()) as _,
            Arc::new(Column::new_with_schema("b2", &right.schema()).unwrap()) as _,
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
            JoinType::LeftMark,
            JoinType::RightMark,
        ];

        for join_type in join_types {
            let runtime = RuntimeEnvBuilder::new()
                .with_memory_limit(100, 1.0)
                .build_arc()?;
            let task_ctx = TaskContext::default().with_runtime(runtime);
            let task_ctx = Arc::new(task_ctx);

            let join = join(
                Arc::clone(&left),
                Arc::clone(&right),
                on.clone(),
                &join_type,
                NullEquality::NullEqualsNothing,
            )?;

            let stream = join.execute(0, task_ctx)?;
            let err = common::collect(stream).await.unwrap_err();

            // Asserting that operator-level reservation attempting to overallocate
            assert_contains!(
                err.to_string(),
                "Resources exhausted: Additional allocation failed for HashJoinInput with top memory consumers (across reservations) as:\n  HashJoinInput"
            );

            assert_contains!(
                err.to_string(),
                "Failed to allocate additional 120.0 B for HashJoinInput"
            );
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
        let left = TestMemoryExec::try_new_exec(
            &[vec![left_batch.clone()], vec![left_batch.clone()]],
            left_batch.schema(),
            None,
        )
        .unwrap();
        let right_batch = build_table_i32(
            ("a2", &vec![10, 11]),
            ("b2", &vec![12, 13]),
            ("c2", &vec![14, 15]),
        );
        let right = TestMemoryExec::try_new_exec(
            &[vec![right_batch.clone()], vec![right_batch.clone()]],
            right_batch.schema(),
            None,
        )
        .unwrap();
        let on = vec![(
            Arc::new(Column::new_with_schema("b1", &left_batch.schema())?) as _,
            Arc::new(Column::new_with_schema("b2", &right_batch.schema())?) as _,
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
            let runtime = RuntimeEnvBuilder::new()
                .with_memory_limit(100, 1.0)
                .build_arc()?;
            let session_config = SessionConfig::default().with_batch_size(50);
            let task_ctx = TaskContext::default()
                .with_session_config(session_config)
                .with_runtime(runtime);
            let task_ctx = Arc::new(task_ctx);

            let join = HashJoinExec::try_new(
                Arc::clone(&left) as Arc<dyn ExecutionPlan>,
                Arc::clone(&right) as Arc<dyn ExecutionPlan>,
                on.clone(),
                None,
                &join_type,
                None,
                PartitionMode::Partitioned,
                NullEquality::NullEqualsNothing,
            )?;

            let stream = join.execute(1, task_ctx)?;
            let err = common::collect(stream).await.unwrap_err();

            // Asserting that stream-level reservation attempting to overallocate
            assert_contains!(
                err.to_string(),
                "Resources exhausted: Additional allocation failed for HashJoinInput[1] with top memory consumers (across reservations) as:\n  HashJoinInput[1]"

            );

            assert_contains!(
                err.to_string(),
                "Failed to allocate additional 120.0 B for HashJoinInput[1]"
            );
        }

        Ok(())
    }

    fn build_table_struct(
        struct_name: &str,
        field_name_and_values: (&str, &Vec<Option<i32>>),
        nulls: Option<NullBuffer>,
    ) -> Arc<dyn ExecutionPlan> {
        let (field_name, values) = field_name_and_values;
        let inner_fields = vec![Field::new(field_name, DataType::Int32, true)];
        let schema = Schema::new(vec![Field::new(
            struct_name,
            DataType::Struct(inner_fields.clone().into()),
            nulls.is_some(),
        )]);

        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(StructArray::new(
                inner_fields.into(),
                vec![Arc::new(Int32Array::from(values.clone()))],
                nulls,
            ))],
        )
        .unwrap();
        let schema_ref = batch.schema();
        TestMemoryExec::try_new_exec(&[vec![batch]], schema_ref, None).unwrap()
    }

    #[tokio::test]
    async fn join_on_struct() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let left =
            build_table_struct("n1", ("a", &vec![None, Some(1), Some(2), Some(3)]), None);
        let right =
            build_table_struct("n2", ("a", &vec![None, Some(1), Some(2), Some(4)]), None);
        let on = vec![(
            Arc::new(Column::new_with_schema("n1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("n2", &right.schema())?) as _,
        )];

        let (columns, batches, metrics) = join_collect(
            left,
            right,
            on,
            &JoinType::Inner,
            NullEquality::NullEqualsNothing,
            task_ctx,
        )
        .await?;

        assert_eq!(columns, vec!["n1", "n2"]);

        allow_duplicates! {
            assert_snapshot!(batches_to_string(&batches), @r#"
            +--------+--------+
            | n1     | n2     |
            +--------+--------+
            | {a: }  | {a: }  |
            | {a: 1} | {a: 1} |
            | {a: 2} | {a: 2} |
            +--------+--------+
                "#);
        }

        assert_join_metrics!(metrics, 3);

        Ok(())
    }

    #[tokio::test]
    async fn join_on_struct_with_nulls() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let left =
            build_table_struct("n1", ("a", &vec![None]), Some(NullBuffer::new_null(1)));
        let right =
            build_table_struct("n2", ("a", &vec![None]), Some(NullBuffer::new_null(1)));
        let on = vec![(
            Arc::new(Column::new_with_schema("n1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("n2", &right.schema())?) as _,
        )];

        let (_, batches_null_eq, metrics) = join_collect(
            Arc::clone(&left),
            Arc::clone(&right),
            on.clone(),
            &JoinType::Inner,
            NullEquality::NullEqualsNull,
            Arc::clone(&task_ctx),
        )
        .await?;

        allow_duplicates! {
            assert_snapshot!(batches_to_sort_string(&batches_null_eq), @r#"
            +----+----+
            | n1 | n2 |
            +----+----+
            |    |    |
            +----+----+
                "#);
        }

        assert_join_metrics!(metrics, 1);

        let (_, batches_null_neq, metrics) = join_collect(
            left,
            right,
            on,
            &JoinType::Inner,
            NullEquality::NullEqualsNothing,
            task_ctx,
        )
        .await?;

        assert_join_metrics!(metrics, 0);

        let expected_null_neq =
            ["+----+----+", "| n1 | n2 |", "+----+----+", "+----+----+"];
        assert_batches_eq!(expected_null_neq, &batches_null_neq);

        Ok(())
    }

    /// Returns the column names on the schema
    fn columns(schema: &Schema) -> Vec<String> {
        schema.fields().iter().map(|f| f.name().clone()).collect()
    }

    /// This test verifies that the dynamic filter is marked as complete after HashJoinExec finishes building the hash table.
    #[tokio::test]
    async fn test_hash_join_marks_filter_complete() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![4, 5, 6]),
            ("c1", &vec![7, 8, 9]),
        );
        let right = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![4, 5, 6]),
            ("c2", &vec![70, 80, 90]),
        );

        let on = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        )];

        // Create a dynamic filter manually
        let dynamic_filter = HashJoinExec::create_dynamic_filter(&on);
        let dynamic_filter_clone = Arc::clone(&dynamic_filter);

        // Create HashJoinExec with the dynamic filter
        let mut join = HashJoinExec::try_new(
            left,
            right,
            on,
            None,
            &JoinType::Inner,
            None,
            PartitionMode::CollectLeft,
            NullEquality::NullEqualsNothing,
        )?;
        join.dynamic_filter = Some(HashJoinExecDynamicFilter {
            filter: dynamic_filter,
            build_accumulator: OnceLock::new(),
        });

        // Execute the join
        let stream = join.execute(0, task_ctx)?;
        let _batches = common::collect(stream).await?;

        // After the join completes, the dynamic filter should be marked as complete
        // wait_complete() should return immediately
        dynamic_filter_clone.wait_complete().await;

        Ok(())
    }

    /// This test verifies that the dynamic filter is marked as complete even when the build side is empty.
    #[tokio::test]
    async fn test_hash_join_marks_filter_complete_empty_build_side() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        // Empty left side (build side)
        let left = build_table(("a1", &vec![]), ("b1", &vec![]), ("c1", &vec![]));
        let right = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![4, 5, 6]),
            ("c2", &vec![70, 80, 90]),
        );

        let on = vec![(
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        )];

        // Create a dynamic filter manually
        let dynamic_filter = HashJoinExec::create_dynamic_filter(&on);
        let dynamic_filter_clone = Arc::clone(&dynamic_filter);

        // Create HashJoinExec with the dynamic filter
        let mut join = HashJoinExec::try_new(
            left,
            right,
            on,
            None,
            &JoinType::Inner,
            None,
            PartitionMode::CollectLeft,
            NullEquality::NullEqualsNothing,
        )?;
        join.dynamic_filter = Some(HashJoinExecDynamicFilter {
            filter: dynamic_filter,
            build_accumulator: OnceLock::new(),
        });

        // Execute the join
        let stream = join.execute(0, task_ctx)?;
        let _batches = common::collect(stream).await?;

        // Even with empty build side, the dynamic filter should be marked as complete
        // wait_complete() should return immediately
        dynamic_filter_clone.wait_complete().await;

        Ok(())
    }
}
