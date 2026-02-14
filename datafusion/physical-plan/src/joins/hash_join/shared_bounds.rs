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

//! Utilities for shared build-side information. Used in dynamic filter pushdown in Hash Joins.
// TODO: include the link to the Dynamic Filter blog post.

use std::fmt;
use std::sync::Arc;

use crate::joins::Map;
use crate::joins::PartitionMode;
use crate::joins::hash_join::exec::HASH_JOIN_SEED;
use crate::joins::hash_join::inlist_builder::build_struct_fields;
use crate::joins::hash_join::partitioned_hash_eval::{
    HashTableLookupExpr, SeededRandomState,
};
use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Field, Schema};
use datafusion_common::config::ConfigOptions;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::Operator;
use datafusion_functions::core::r#struct as struct_func;
use datafusion_physical_expr::expressions::{
    BinaryExpr, DynamicFilterPhysicalExpr, InListExpr, lit,
};
use datafusion_physical_expr::{PhysicalExpr, PhysicalExprRef, ScalarFunctionExpr};

use parking_lot::Mutex;

/// Represents the minimum and maximum values for a specific column.
/// Used in dynamic filter pushdown to establish value boundaries.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ColumnBounds {
    /// The minimum value observed for this column
    pub(crate) min: ScalarValue,
    /// The maximum value observed for this column
    pub(crate) max: ScalarValue,
}

impl ColumnBounds {
    pub(crate) fn new(min: ScalarValue, max: ScalarValue) -> Self {
        Self { min, max }
    }
}

/// Represents the bounds for all join key columns from a single partition.
/// This contains the min/max values computed from one partition's build-side data.
#[derive(Debug, Clone)]
pub(crate) struct PartitionBounds {
    /// Min/max bounds for each join key column in this partition.
    /// Index corresponds to the join key expression index.
    column_bounds: Vec<ColumnBounds>,
}

impl PartitionBounds {
    pub(crate) fn new(column_bounds: Vec<ColumnBounds>) -> Self {
        Self { column_bounds }
    }

    pub(crate) fn get_column_bounds(&self, index: usize) -> Option<&ColumnBounds> {
        self.column_bounds.get(index)
    }
}

/// Creates a membership predicate for filter pushdown.
///
/// If `inlist_values` is provided (for small build sides), creates an InList expression.
/// Otherwise, creates a HashTableLookup expression (for large build sides).
///
/// Supports both single-column and multi-column joins using struct expressions.
fn create_membership_predicate(
    on_right: &[PhysicalExprRef],
    pushdown: PushdownStrategy,
    random_state: &SeededRandomState,
    schema: &Schema,
) -> Result<Option<Arc<dyn PhysicalExpr>>> {
    match pushdown {
        // Use InList expression for small build sides
        PushdownStrategy::InList(in_list_array) => {
            // Build the expression to compare against
            let expr = if on_right.len() == 1 {
                // Single column: col IN (val1, val2, ...)
                Arc::clone(&on_right[0])
            } else {
                let fields = build_struct_fields(
                    on_right
                        .iter()
                        .map(|r| r.data_type(schema))
                        .collect::<Result<Vec<_>>>()?
                        .as_ref(),
                )?;

                // The return field name and the function field name don't really matter here.
                let return_field =
                    Arc::new(Field::new("struct", DataType::Struct(fields), true));

                Arc::new(ScalarFunctionExpr::new(
                    "struct",
                    struct_func(),
                    on_right.to_vec(),
                    return_field,
                    Arc::new(ConfigOptions::default()),
                )) as Arc<dyn PhysicalExpr>
            };

            // Use in_list_from_array() helper to create InList with static_filter optimization (hash-based lookup)
            Ok(Some(Arc::new(InListExpr::try_new_from_array(
                expr,
                in_list_array,
                false,
            )?)))
        }
        // Use hash table lookup for large build sides
        PushdownStrategy::Map(hash_map) => Ok(Some(Arc::new(HashTableLookupExpr::new(
            on_right.to_vec(),
            random_state.clone(),
            hash_map,
            "hash_lookup".to_string(),
        )) as Arc<dyn PhysicalExpr>)),
        // Empty partition - should not create a filter for this
        PushdownStrategy::Empty => Ok(None),
    }
}

/// Creates a bounds predicate from partition bounds.
///
/// Returns `None` if no column bounds are available.
/// Returns a combined predicate (col >= min AND col <= max) for all columns with bounds.
fn create_bounds_predicate(
    on_right: &[PhysicalExprRef],
    bounds: &PartitionBounds,
) -> Option<Arc<dyn PhysicalExpr>> {
    let mut column_predicates = Vec::new();

    for (col_idx, right_expr) in on_right.iter().enumerate() {
        if let Some(column_bounds) = bounds.get_column_bounds(col_idx) {
            // Create predicate: col >= min AND col <= max
            let min_expr = Arc::new(BinaryExpr::new(
                Arc::clone(right_expr),
                Operator::GtEq,
                lit(column_bounds.min.clone()),
            )) as Arc<dyn PhysicalExpr>;
            let max_expr = Arc::new(BinaryExpr::new(
                Arc::clone(right_expr),
                Operator::LtEq,
                lit(column_bounds.max.clone()),
            )) as Arc<dyn PhysicalExpr>;
            let range_expr = Arc::new(BinaryExpr::new(min_expr, Operator::And, max_expr))
                as Arc<dyn PhysicalExpr>;
            column_predicates.push(range_expr);
        }
    }

    if column_predicates.is_empty() {
        None
    } else {
        Some(
            column_predicates
                .into_iter()
                .reduce(|acc, pred| {
                    Arc::new(BinaryExpr::new(acc, Operator::And, pred))
                        as Arc<dyn PhysicalExpr>
                })
                .unwrap(),
        )
    }
}

/// Combines membership and bounds expressions into a single filter expression.
///
/// Returns `None` if neither membership nor bounds are available (e.g., empty build side).
fn combine_membership_and_bounds(
    membership_expr: Option<Arc<dyn PhysicalExpr>>,
    bounds_expr: Option<Arc<dyn PhysicalExpr>>,
) -> Option<Arc<dyn PhysicalExpr>> {
    match (membership_expr, bounds_expr) {
        (Some(membership), Some(bounds)) => {
            // Both available: combine with AND
            Some(Arc::new(BinaryExpr::new(bounds, Operator::And, membership))
                as Arc<dyn PhysicalExpr>)
        }
        (Some(membership), None) => Some(membership),
        (None, Some(bounds)) => Some(bounds),
        (None, None) => None,
    }
}

/// Coordinates build-side information collection across multiple partitions.
///
/// This structure collects information from the build side (hash tables and/or bounds) and
/// ensures that dynamic filters are updated correctly as partitions complete.
///
/// ## Synchronization Strategy
///
/// **Partitioned mode:** Each partition independently updates its own DynamicFilter.
/// No shared mutable state per partition — zero contention.
///
/// **CollectLeft mode:** Uses a mutex for deduplication:
/// 1. The first partition to report updates `partition_filters[0]` and marks it complete
/// 2. Subsequent partitions skip the update (dedup)
///
/// ## Architecture
///
/// The per-partition DynamicFilters are pushed directly to the scan (via a CASE
/// expression or a single DynamicFilter). There is no top-level DynamicFilter
/// wrapper — the expression tree is directly traversable, which allows the
/// `snapshot_generation()` function to detect per-partition updates for file pruning.
///
/// ## Thread Safety
///
/// **Partitioned mode:** Each partition updates its own DynamicFilter — no shared state.
///
/// **CollectLeft mode:** Mutex dedup ensures only the first report updates the filter.
pub(crate) struct SharedBuildAccumulator {
    /// Build-side data protected by a single mutex (used only for CollectLeft dedup)
    inner: Mutex<AccumulatedBuildData>,
    /// Right side join expressions needed for creating filter expressions
    on_right: Vec<PhysicalExprRef>,
    /// Schema of the probe (right) side for evaluating filter expressions
    probe_schema: Arc<Schema>,
    /// Per-partition DynamicFilters that are pushed down to the scan.
    /// CollectLeft: single filter. Partitioned: N filters.
    partition_filters: Vec<Arc<DynamicFilterPhysicalExpr>>,
}

/// Strategy for filter pushdown (decided at collection time)
#[derive(Clone)]
pub(crate) enum PushdownStrategy {
    /// Use InList for small build sides (< 128MB)
    InList(ArrayRef),
    /// Use map lookup for large build sides
    Map(Arc<Map>),
    /// There was no data in this partition, do not build a dynamic filter for it
    Empty,
}

/// Build-side data reported by a single partition
pub(crate) enum PartitionBuildData {
    Partitioned {
        partition_id: usize,
        pushdown: PushdownStrategy,
        bounds: PartitionBounds,
    },
    CollectLeft {
        pushdown: PushdownStrategy,
        bounds: PartitionBounds,
    },
}

/// Build-side data organized by partition mode (used for CollectLeft dedup)
enum AccumulatedBuildData {
    Partitioned,
    CollectLeft {
        /// Whether the first report has already been processed
        reported: bool,
    },
}

impl SharedBuildAccumulator {
    /// Creates a new SharedBuildAccumulator.
    ///
    /// The `partition_filters` are the per-partition DynamicFilter instances that
    /// have already been pushed down to the scan (either directly or via a CASE expression).
    /// This constructor simply stores them for later updates during execution.
    pub(crate) fn new(
        mode: PartitionMode,
        partition_filters: Vec<Arc<DynamicFilterPhysicalExpr>>,
        on_right: Vec<PhysicalExprRef>,
        probe_schema: Arc<Schema>,
    ) -> Self {
        let mode_data = match mode {
            PartitionMode::Partitioned => AccumulatedBuildData::Partitioned,
            PartitionMode::CollectLeft => {
                AccumulatedBuildData::CollectLeft { reported: false }
            }
            PartitionMode::Auto => unreachable!(
                "PartitionMode::Auto should not be present at execution time. This is a bug in DataFusion, please report it!"
            ),
        };

        Self {
            inner: Mutex::new(mode_data),
            on_right,
            probe_schema,
            partition_filters,
        }
    }

    /// Report build-side data from a partition (synchronous).
    ///
    /// **Partitioned mode:** Each partition independently builds its filter expression
    /// and updates its own per-partition DynamicFilter. No mutex contention.
    ///
    /// **CollectLeft mode:** The first partition to report updates `partition_filters[0]`.
    /// Subsequent reports are no-ops (dedup via mutex).
    ///
    /// Each filter is marked complete independently — no cross-partition coordination needed.
    pub(crate) fn report_build_data(&self, data: PartitionBuildData) -> Result<()> {
        match data {
            PartitionBuildData::Partitioned {
                partition_id,
                pushdown,
                bounds,
            } => {
                match &pushdown {
                    PushdownStrategy::Empty => {
                        // Empty partition: update per-partition filter to lit(false)
                        self.partition_filters[partition_id].update(lit(false))?;
                    }
                    _ => {
                        // Build the bounds AND membership expression for this partition
                        let membership_expr = create_membership_predicate(
                            &self.on_right,
                            pushdown,
                            &HASH_JOIN_SEED,
                            self.probe_schema.as_ref(),
                        )?;
                        let bounds_expr =
                            create_bounds_predicate(&self.on_right, &bounds);

                        if let Some(filter_expr) =
                            combine_membership_and_bounds(membership_expr, bounds_expr)
                        {
                            self.partition_filters[partition_id].update(filter_expr)?;
                        }
                    }
                }

                // Mark this partition's filter as complete
                self.partition_filters[partition_id].mark_complete();
            }
            PartitionBuildData::CollectLeft { pushdown, bounds } => {
                // Use mutex for deduplication: only the first report updates the filter
                {
                    let mut guard = self.inner.lock();
                    match &mut *guard {
                        AccumulatedBuildData::CollectLeft { reported } => {
                            if *reported {
                                // Already reported by another partition, skip
                                return Ok(());
                            }
                            *reported = true;
                        }
                        AccumulatedBuildData::Partitioned => {
                            return datafusion_common::internal_err!(
                                "Build data mode mismatch: expected CollectLeft, got Partitioned"
                            );
                        }
                    }
                }
                // Lock released; build the filter expression outside the lock

                let membership_expr = create_membership_predicate(
                    &self.on_right,
                    pushdown,
                    &HASH_JOIN_SEED,
                    self.probe_schema.as_ref(),
                )?;
                let bounds_expr = create_bounds_predicate(&self.on_right, &bounds);

                if let Some(filter_expr) =
                    combine_membership_and_bounds(membership_expr, bounds_expr)
                {
                    self.partition_filters[0].update(filter_expr)?;
                }

                // Mark the single filter as complete
                self.partition_filters[0].mark_complete();
            }
        }

        Ok(())
    }
}

impl fmt::Debug for SharedBuildAccumulator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SharedBuildAccumulator")
    }
}
