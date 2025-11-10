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

use crate::execution_plan::{boundedness_from_children, EmissionType};
use crate::filter_pushdown::{
    ChildPushdownResult, FilterDescription, FilterPushdownPhase,
    FilterPushdownPropagation,
};
use crate::joins::utils::{
    reorder_output_after_swap, swap_join_projection, OnceFut,
};
use crate::joins::{JoinOn, JoinOnRef, PartitionMode};
use crate::projection::{
    try_embed_projection, try_pushdown_through_join, EmbeddedProjection, JoinData,
    ProjectionExec,
};
use crate::spill::get_record_batch_memory_size;
use crate::{
    common::can_project,
    joins::utils::{
        build_join_schema, check_join_is_valid, estimate_join_statistics,
        symmetric_join_output_partitioning,
        BuildProbeJoinMetrics, ColumnIndex, JoinFilter,
    },
    metrics::{ExecutionPlanMetricsSet, MetricsSet},
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlan,
    PlanProperties, SendableRecordBatchStream, Statistics,
};
use crate::{ExecutionPlanProperties, SpillManager};
use std::fmt;
use std::fmt::Formatter;
use std::sync::{Arc, OnceLock};
use std::{any::Any, vec};

use arrow::array::UInt32Array;
use arrow::compute::{concat_batches, take};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::config::ConfigOptions;
use datafusion_common::{
    internal_err, plan_err, project_schema, JoinSide, JoinType,
    NullEquality, Result,
};
use datafusion_execution::TaskContext;
use datafusion_functions_aggregate_common::min_max::{MaxAccumulator, MinAccumulator};
use datafusion_physical_expr::equivalence::{
    join_equivalence_properties, ProjectionMapping,
};
use datafusion_physical_expr::expressions::{lit, DynamicFilterPhysicalExpr};
use datafusion_physical_expr::{PhysicalExpr, PhysicalExprRef};

use crate::joins::grace_hash_join::stream::{
    GraceAccumulator, GraceHashJoinStream, SpillFut,
};
use crate::joins::hash_join::shared_bounds::SharedBoundsAccumulator;
use crate::metrics::SpillMetrics;
use crate::spill::spill_manager::SpillLocation;
use ahash::RandomState;
use datafusion_common::hash_utils::create_hashes;
use datafusion_physical_expr_common::physical_expr::fmt_sql;
use futures::StreamExt;

/// Hard-coded seed to ensure hash values from the hash join differ from `RepartitionExec`, avoiding collisions.
const HASH_JOIN_SEED: RandomState =
    RandomState::with_seeds('J' as u64, 'O' as u64, 'I' as u64, 'N' as u64);

pub struct GraceHashJoinExec {
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
    /// Shared the `RandomState` for the hashing algorithm
    random_state: RandomState,
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
    accumulator: Arc<GraceAccumulator>,
}

#[derive(Clone)]
struct HashJoinExecDynamicFilter {
    /// Dynamic filter that we'll update with the results of the build side once that is done.
    filter: Arc<DynamicFilterPhysicalExpr>,
    /// Bounds accumulator to keep track of the min/max bounds on the join keys for each partition.
    /// It is lazily initialized during execution to make sure we use the actual execution time partition counts.
    bounds_accumulator: OnceLock<Arc<SharedBoundsAccumulator>>,
}

impl fmt::Debug for GraceHashJoinExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("HashJoinExec")
            .field("left", &self.left)
            .field("right", &self.right)
            .field("on", &self.on)
            .field("filter", &self.filter)
            .field("join_type", &self.join_type)
            .field("join_schema", &self.join_schema)
            .field("random_state", &self.random_state)
            .field("metrics", &self.metrics)
            .field("projection", &self.projection)
            .field("column_indices", &self.column_indices)
            .field("null_equality", &self.null_equality)
            .field("cache", &self.cache)
            // Explicitly exclude dynamic_filter to avoid runtime state differences in tests
            .finish()
    }
}

impl EmbeddedProjection for GraceHashJoinExec {
    fn with_projection(&self, projection: Option<Vec<usize>>) -> Result<Self> {
        self.with_projection(projection)
    }
}

impl GraceHashJoinExec {
    /// Tries to create a new [GraceHashJoinExec].
    ///
    /// # Error
    /// This function errors when it is not possible to join the left and right sides on keys `on`.
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: JoinOn,
        filter: Option<JoinFilter>,
        join_type: &JoinType,
        projection: Option<Vec<usize>>,
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
            Arc::clone(&join_schema),
            *join_type,
            &on,
            projection.as_ref(),
        )?;
        let partitions = left.output_partitioning().partition_count();
        let accumulator = GraceAccumulator::new(partitions);

        let metrics = ExecutionPlanMetricsSet::new();
        // Initialize both dynamic filter and bounds accumulator to None
        // They will be set later if dynamic filtering is enabled
        Ok(GraceHashJoinExec {
            left,
            right,
            on,
            filter,
            join_type: *join_type,
            join_schema,
            random_state,
            metrics,
            projection,
            column_indices,
            null_equality,
            cache,
            dynamic_filter: None,
            accumulator,
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
            self.null_equality,
        )
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(
        left: &Arc<dyn ExecutionPlan>,
        right: &Arc<dyn ExecutionPlan>,
        schema: SchemaRef,
        join_type: JoinType,
        on: JoinOnRef,
        projection: Option<&Vec<usize>>,
    ) -> Result<PlanProperties> {
        // Calculate equivalence properties:
        let mut eq_properties = join_equivalence_properties(
            left.equivalence_properties().clone(),
            right.equivalence_properties().clone(),
            &join_type,
            Arc::clone(&schema),
            &Self::maintains_input_order(join_type),
            Some(Self::probe_side()),
            on,
        )?;

        let mut output_partitioning =
            symmetric_join_output_partitioning(left, right, &join_type)?;
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
            let projection_mapping =
                ProjectionMapping::from_indices(projection, &schema)?;
            let out_schema = project_schema(&schema, Some(projection))?;
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
        _partition_mode: PartitionMode,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let left = self.left();
        let right = self.right();
        let new_join = GraceHashJoinExec::try_new(
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
            self.null_equality(),
        )?;
        // In case of anti / semi joins or if there is embedded projection in HashJoinExec, output column order is preserved, no need to add projection again
        if matches!(
            self.join_type(),
            JoinType::LeftSemi
                | JoinType::RightSemi
                | JoinType::LeftAnti
                | JoinType::RightAnti
        ) || self.projection.is_some()
        {
            Ok(Arc::new(new_join))
        } else {
            reorder_output_after_swap(Arc::new(new_join), &left.schema(), &right.schema())
        }
    }
}

impl DisplayAs for GraceHashJoinExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
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
                let on = self
                    .on
                    .iter()
                    .map(|(c1, c2)| format!("({c1}, {c2})"))
                    .collect::<Vec<String>>()
                    .join(", ");
                write!(
                    f,
                    "GraceHashJoinExec: join_type={:?}, on=[{}]{}{}",
                    self.join_type, on, display_filter, display_projections,
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

                if let Some(filter) = self.filter.as_ref() {
                    writeln!(f, "filter={filter}")?;
                }

                Ok(())
            }
        }
    }
}

impl ExecutionPlan for GraceHashJoinExec {
    fn name(&self) -> &'static str {
        "GraceHashJoinExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
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
        Ok(Arc::new(GraceHashJoinExec {
            left: Arc::clone(&children[0]),
            right: Arc::clone(&children[1]),
            on: self.on.clone(),
            filter: self.filter.clone(),
            join_type: self.join_type,
            join_schema: Arc::clone(&self.join_schema),
            random_state: self.random_state.clone(),
            metrics: ExecutionPlanMetricsSet::new(),
            projection: self.projection.clone(),
            column_indices: self.column_indices.clone(),
            null_equality: self.null_equality,
            cache: Self::compute_properties(
                &children[0],
                &children[1],
                Arc::clone(&self.join_schema),
                self.join_type,
                &self.on,
                self.projection.as_ref(),
            )?,
            // Keep the dynamic filter, bounds accumulator will be reset
            dynamic_filter: self.dynamic_filter.clone(),
            accumulator: Arc::clone(&self.accumulator),
        }))
    }

    fn reset_state(self: Arc<Self>) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(GraceHashJoinExec {
            left: Arc::clone(&self.left),
            right: Arc::clone(&self.right),
            on: self.on.clone(),
            filter: self.filter.clone(),
            join_type: self.join_type,
            join_schema: Arc::clone(&self.join_schema),
            random_state: self.random_state.clone(),
            metrics: ExecutionPlanMetricsSet::new(),
            projection: self.projection.clone(),
            column_indices: self.column_indices.clone(),
            null_equality: self.null_equality,
            cache: self.cache.clone(),
            // Reset dynamic filter and bounds accumulator to initial state
            dynamic_filter: None,
            accumulator: Arc::clone(&self.accumulator),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let left_partitions = self.left.output_partitioning().partition_count();
        let right_partitions = self.right.output_partitioning().partition_count();

        if left_partitions != right_partitions {
            return internal_err!(
                "Invalid GraceHashJoinExec, partition count mismatch {left_partitions}!={right_partitions},\
                 consider using RepartitionExec"
            );
        }

        let enable_dynamic_filter_pushdown = self.dynamic_filter.is_some();

        let join_metrics = Arc::new(BuildProbeJoinMetrics::new(partition, &self.metrics));

        let left = self.left.execute(partition, Arc::clone(&context))?;
        let left_schema = Arc::clone(&self.left.schema());
        let on_left = self
            .on
            .iter()
            .map(|(left_expr, _)| Arc::clone(left_expr))
            .collect::<Vec<_>>();

        let right = self.right.execute(partition, Arc::clone(&context))?;
        let right_schema = Arc::clone(&self.right.schema());
        let on_right = self
            .on
            .iter()
            .map(|(_, right_expr)| Arc::clone(right_expr))
            .collect::<Vec<_>>();

        let spill_left = Arc::new(SpillManager::new(
            Arc::clone(&context.runtime_env()),
            SpillMetrics::new(&self.metrics, partition),
            Arc::clone(&left_schema),
        ));
        let spill_right = Arc::new(SpillManager::new(
            Arc::clone(&context.runtime_env()),
            SpillMetrics::new(&self.metrics, partition),
            Arc::clone(&right_schema),
        ));

        // update column indices to reflect the projection
        let column_indices_after_projection = match &self.projection {
            Some(projection) => projection
                .iter()
                .map(|i| self.column_indices[*i].clone())
                .collect(),
            None => self.column_indices.clone(),
        };

        let random_state = self.random_state.clone();
        let on = self.on.clone();
        let spill_left_clone = Arc::clone(&spill_left);
        let spill_right_clone = Arc::clone(&spill_right);
        let accumulator_clone = Arc::clone(&self.accumulator);
        let join_metrics_clone = Arc::clone(&join_metrics);
        let spill_fut = OnceFut::new(async move {
            let (left_idx, right_idx) = partition_and_spill(
                random_state,
                on,
                left,
                right,
                join_metrics_clone,
                enable_dynamic_filter_pushdown,
                left_partitions,
                spill_left_clone,
                spill_right_clone,
                partition,
            )
                .await?;
            accumulator_clone
                .report_partition(partition, left_idx.clone(), right_idx.clone())
                .await;
            Ok(SpillFut::new(partition, left_idx, right_idx))
        });

        Ok(Box::pin(GraceHashJoinStream::new(
            self.schema(),
            spill_fut,
            spill_left,
            spill_right,
            on_left,
            on_right,
            self.projection.clone(),
            self.filter.clone(),
            self.join_type,
            column_indices_after_projection,
            join_metrics,
            context,
            Arc::clone(&self.accumulator),
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
        let stats = estimate_join_statistics(
            self.left.partition_statistics(None)?,
            self.right.partition_statistics(None)?,
            self.on.clone(),
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
        // TODO: currently if there is projection in GraceHashJoinExec, we can't push down projection to left or right input. Maybe we can pushdown the mixed projection later.
        if self.contains_projection() {
            return Ok(None);
        }

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
            self.schema(),
            self.filter(),
        )? {
            Ok(Some(Arc::new(GraceHashJoinExec::try_new(
                Arc::new(projected_left_child),
                Arc::new(projected_right_child),
                join_on,
                join_filter,
                self.join_type(),
                // Returned early if projection is not None
                None,
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
            && config.optimizer.enable_dynamic_filter_pushdown
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
                let new_node = Arc::new(GraceHashJoinExec {
                    left: Arc::clone(&self.left),
                    right: Arc::clone(&self.right),
                    on: self.on.clone(),
                    filter: self.filter.clone(),
                    join_type: self.join_type,
                    join_schema: Arc::clone(&self.join_schema),
                    random_state: self.random_state.clone(),
                    metrics: ExecutionPlanMetricsSet::new(),
                    projection: self.projection.clone(),
                    column_indices: self.column_indices.clone(),
                    null_equality: self.null_equality,
                    cache: self.cache.clone(),
                    dynamic_filter: Some(HashJoinExecDynamicFilter {
                        filter: dynamic_filter,
                        bounds_accumulator: OnceLock::new(),
                    }),
                    accumulator: Arc::clone(&self.accumulator),
                });
                result = result.with_updated_node(new_node as Arc<dyn ExecutionPlan>);
            }
        }
        Ok(result)
    }
}


#[allow(clippy::too_many_arguments)]
pub async fn partition_and_spill(
    random_state: RandomState,
    on: Vec<(PhysicalExprRef, PhysicalExprRef)>,
    mut left_stream: SendableRecordBatchStream,
    mut right_stream: SendableRecordBatchStream,
    join_metrics: Arc<BuildProbeJoinMetrics>,
    enable_dynamic_filter_pushdown: bool,
    partition_count: usize,
    spill_left: Arc<SpillManager>,
    spill_right: Arc<SpillManager>,
    partition: usize,
) -> Result<(Vec<PartitionIndex>, Vec<PartitionIndex>)> {
    let on_left: Vec<_> = on.iter().map(|(l, _)| Arc::clone(l)).collect();
    let on_right: Vec<_> = on.iter().map(|(_, r)| Arc::clone(r)).collect();

    // LEFT side partitioning
    let left_index = partition_and_spill_one_side(
        &mut left_stream,
        &on_left,
        &random_state,
        partition_count,
        spill_left,
        &format!("left_{partition}"),
        &join_metrics,
        enable_dynamic_filter_pushdown,
    )
        .await?;

    // RIGHT side partitioning
    let right_index = partition_and_spill_one_side(
        &mut right_stream,
        &on_right,
        &random_state,
        partition_count,
        spill_right,
        &format!("right_{partition}"),
        &join_metrics,
        enable_dynamic_filter_pushdown,
    )
        .await?;
    Ok((left_index, right_index))
}

#[allow(clippy::too_many_arguments)]
async fn partition_and_spill_one_side(
    input: &mut SendableRecordBatchStream,
    on_exprs: &[PhysicalExprRef],
    random_state: &RandomState,
    partition_count: usize,
    spill_manager: Arc<SpillManager>,
    spilling_request_msg: &str,
    join_metrics: &BuildProbeJoinMetrics,
    _enable_dynamic_filter_pushdown: bool,
) -> Result<Vec<PartitionIndex>> {
    let mut partitions: Vec<PartitionWriter> = (0..partition_count)
        .map(|_| PartitionWriter::new(Arc::clone(&spill_manager)))
        .collect();

    let mut buffered_batches = Vec::new();

    let schema = input.schema();
    while let Some(batch) = input.next().await {
        let batch = batch?;
        if batch.num_rows() == 0 {
            continue;
        }
        join_metrics.build_input_batches.add(1);
        join_metrics.build_input_rows.add(batch.num_rows());
        buffered_batches.push(batch);
    }
    if buffered_batches.is_empty() {
        return Ok(Vec::new());
    }
    // Create single batch to reduce number of spilled files
    let single_batch = concat_batches(&schema, &buffered_batches)?;
    let num_rows = single_batch.num_rows();
    if num_rows == 0 {
        return Ok(Vec::new());
    }

    // Calculate hashes
    let keys = on_exprs
        .iter()
        .map(|c| c.evaluate(&single_batch)?.into_array(num_rows))
        .collect::<Result<Vec<_>>>()?;

    let mut hashes = vec![0u64; num_rows];
    create_hashes(&keys, random_state, &mut hashes)?;

    // Spread to partitions
    let mut indices: Vec<Vec<u32>> = vec![Vec::new(); partition_count];
    for (row, h) in hashes.iter().enumerate() {
        let bucket = (*h as usize) % partition_count;
        indices[bucket].push(row as u32);
    }

    // Collect and spill
    for (i, idxs) in indices.into_iter().enumerate() {
        if idxs.is_empty() {
            continue;
        }

        let idx_array = UInt32Array::from(idxs);
        let taken = single_batch
            .columns()
            .iter()
            .map(|c| take(c.as_ref(), &idx_array, None))
            .collect::<arrow::error::Result<Vec<_>>>()?;

        let part_batch = RecordBatch::try_new(single_batch.schema(), taken)?;
        // We need unique name for spilling
        let request_msg = format!("grace_partition_{spilling_request_msg}_{i}");
        partitions[i].spill_batch_auto(&part_batch, &request_msg)?;
    }

    // Prepare indexes
    let mut result = Vec::with_capacity(partitions.len());
    for (i, writer) in partitions.into_iter().enumerate() {
        result.push(writer.finish(i)?);
    }
    // println!("spill_manager {:?}", spill_manager.metrics);
    Ok(result)
}

#[derive(Debug)]
pub struct PartitionWriter {
    spill_manager: Arc<SpillManager>,
    total_rows: usize,
    total_bytes: usize,
    chunks: Vec<SpillLocation>,
}

impl PartitionWriter {
    pub fn new(spill_manager: Arc<SpillManager>) -> Self {
        Self {
            spill_manager,
            total_rows: 0,
            total_bytes: 0,
            chunks: vec![],
        }
    }

    pub fn spill_batch_auto(
        &mut self,
        batch: &RecordBatch,
        request_msg: &str,
    ) -> Result<()> {
        let loc = self.spill_manager.spill_batch_auto(batch, request_msg)?;
        self.total_rows += batch.num_rows();
        self.total_bytes += get_record_batch_memory_size(batch);
        self.chunks.push(loc);
        Ok(())
    }

    pub fn finish(self, part_id: usize) -> Result<PartitionIndex> {
        Ok(PartitionIndex {
            part_id,
            chunks: self.chunks,
            total_rows: self.total_rows,
            total_bytes: self.total_bytes,
        })
    }
}

/// Describes a single partition of spilled data (used in GraceHashJoin).
///
/// Each partition can consist of one or multiple chunks (batches)
/// that were spilled either to memory or to disk.
/// These chunks are later reloaded during the join phase.
///
/// Example:
/// Partition 3 -> [ spill_chunk_3_0.arrow, spill_chunk_3_1.arrow ]
#[derive(Debug, Clone)]
pub struct PartitionIndex {
    /// Unique partition identifier (0..N-1)
    pub part_id: usize,

    /// Total number of rows in this partition
    pub total_rows: usize,

    /// Total size in bytes of all batches in this partition
    pub total_bytes: usize,

    /// Collection of spill locations (each corresponds to one batch written
    /// by [`PartitionWriter::spill_batch_auto`])
    pub chunks: Vec<SpillLocation>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::TestMemoryExec;
    use crate::{
        common, expressions::Column, repartition::RepartitionExec, test::build_table_i32,
    };

    use crate::joins::HashJoinExec;
    use arrow::array::{ArrayRef, Int32Array};
    use datafusion_execution::runtime_env::RuntimeEnvBuilder;
    use datafusion_physical_expr::Partitioning;
    use futures::future;

    fn build_large_table(
        a_name: &str,
        b_name: &str,
        c_name: &str,
        n: usize,
    ) -> Arc<dyn ExecutionPlan> {
        let a: ArrayRef = Arc::new(Int32Array::from_iter_values(1..=n as i32));
        let b: ArrayRef =
            Arc::new(Int32Array::from_iter_values((1..=n as i32).map(|x| x * 2)));
        let c: ArrayRef =
            Arc::new(Int32Array::from_iter_values((1..=n as i32).map(|x| x * 10)));

        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new(
                a_name,
                arrow::datatypes::DataType::Int32,
                false,
            ),
            arrow::datatypes::Field::new(
                b_name,
                arrow::datatypes::DataType::Int32,
                false,
            ),
            arrow::datatypes::Field::new(
                c_name,
                arrow::datatypes::DataType::Int32,
                false,
            ),
        ]));

        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![a, b, c]).unwrap();
        Arc::new(TestMemoryExec::try_new(&[vec![batch]], schema, None).unwrap())
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

    #[tokio::test]
    async fn simple_grace_hash_join() -> Result<()> {
        // let left = build_table(
        //     ("a1", &vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 0]),
        //     ("b1", &vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 0]),
        //     ("c1", &vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 0]),
        // );
        // let right = build_table(
        //     ("a2", &vec![1, 2]),
        //     ("b2", &vec![1, 2]),
        //     ("c2", &vec![14, 15]),
        // );
        let left = build_large_table("a1", "b1", "c1", 2000000);
        let right = build_large_table("a2", "b2", "c2", 5000000);
        let on = vec![(
            Arc::new(Column::new_with_schema("a1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b2", &right.schema())?) as _,
        )];
        let (left_expr, right_expr): (
            Vec<Arc<dyn PhysicalExpr>>,
            Vec<Arc<dyn PhysicalExpr>>,
        ) = on
            .iter()
            .map(|(l, r)| (Arc::clone(l), Arc::clone(r)))
            .unzip();
        let left_repartitioned: Arc<dyn ExecutionPlan> = Arc::new(
            RepartitionExec::try_new(left, Partitioning::Hash(left_expr, 32))?,
        );
        let right_repartitioned: Arc<dyn ExecutionPlan> = Arc::new(
            RepartitionExec::try_new(right, Partitioning::Hash(right_expr, 32))?,
        );

        let runtime = RuntimeEnvBuilder::new()
            .with_memory_limit(500_000_000, 1.0)
            .build_arc()?;
        let task_ctx = TaskContext::default().with_runtime(runtime);
        let task_ctx = Arc::new(task_ctx);

        let join = GraceHashJoinExec::try_new(
            Arc::clone(&left_repartitioned),
            Arc::clone(&right_repartitioned),
            on.clone(),
            None,
            &JoinType::Inner,
            None,
            NullEquality::NullEqualsNothing,
        )?;

        let partition_count = right_repartitioned.output_partitioning().partition_count();
        let tasks: Vec<_> = (0..partition_count)
            .map(|i| {
                let ctx = Arc::clone(&task_ctx);
                let s = join.execute(i, ctx).unwrap();
                async move { common::collect(s).await }
            })
            .collect();

        let results = future::join_all(tasks).await;
        let mut batches = Vec::new();
        for r in results {
            let mut v = r?;
            v.retain(|b| b.num_rows() > 0);
            batches.extend(v);
        }
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        println!("TOTAL ROWS = {}", total_rows);

        // print_batches(&*batches).unwrap();
        // Asserting that operator-level reservation attempting to overallocate
        // assert_contains!(
        //     err.to_string(),
        //     "Resources exhausted: Additional allocation failed with top memory consumers (across reservations) as:\n  HashJoinInput"
        // );
        //
        // assert_contains!(
        //     err.to_string(),
        //     "Failed to allocate additional 120.0 B for HashJoinInput"
        // );
        Ok(())
    }

    #[tokio::test]
    async fn simple_hash_join() -> Result<()> {
        let left = build_large_table("a1", "b1", "c1", 2000000);
        let right = build_large_table("a2", "b2", "c2", 5000000);
        let on = vec![(
            Arc::new(Column::new_with_schema("a1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b2", &right.schema())?) as _,
        )];
        let (left_expr, right_expr): (
            Vec<Arc<dyn PhysicalExpr>>,
            Vec<Arc<dyn PhysicalExpr>>,
        ) = on
            .iter()
            .map(|(l, r)| (Arc::clone(l), Arc::clone(r)))
            .unzip();
        let left_repartitioned: Arc<dyn ExecutionPlan> = Arc::new(
            RepartitionExec::try_new(left, Partitioning::Hash(left_expr, 32))?,
        );
        let right_repartitioned: Arc<dyn ExecutionPlan> = Arc::new(
            RepartitionExec::try_new(right, Partitioning::Hash(right_expr, 32))?,
        );
        let partition_count = left_repartitioned.output_partitioning().partition_count();

        let join = HashJoinExec::try_new(
            left_repartitioned,
            right_repartitioned,
            on,
            None,
            &JoinType::Inner,
            None,
            PartitionMode::Partitioned,
            NullEquality::NullEqualsNothing,
        )?;

        let task_ctx = Arc::new(TaskContext::default());
        let mut batches = vec![];
        for i in 0..partition_count {
            let stream = join.execute(i, Arc::clone(&task_ctx))?;
            let more_batches = common::collect(stream).await?;
            batches.extend(
                more_batches
                    .into_iter()
                    .filter(|b| b.num_rows() > 0)
                    .collect::<Vec<_>>(),
            );
        }
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        println!("TOTAL ROWS = {}", total_rows);

        // print_batches(&*batches).unwrap();
        Ok(())
    }
}
