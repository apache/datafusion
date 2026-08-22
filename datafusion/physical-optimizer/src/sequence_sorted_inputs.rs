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

use crate::PhysicalOptimizerRule;
use datafusion_common::config::ConfigOptions;
use datafusion_common::stats::Statistics;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::{Result, ScalarValue};
use datafusion_physical_expr::LexOrdering;
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_plan::sorts::{
    progressive_eval::ProgressiveEvalExec, reorder_partitions::ReorderPartitionsExec,
    sort_preserving_merge::SortPreservingMergeExec,
};
use datafusion_physical_plan::statistics::{StatisticsArgs, StatisticsContext};
use datafusion_physical_plan::{ExecutionPlan, ExecutionPlanProperties as _};

use std::cmp::Ordering;
use std::sync::Arc;

/// Optimization that replaces [`SortPreservingMergeExec`] with [`ProgressiveEvalExec`]
/// when its input partitions are non-overlapping with respect to the merge
/// ordering. The partitions are arranged into the required order (via
/// [`ReorderPartitionsExec`] when they are not already laid out that way) so
/// that concatenating them yields globally ordered output.
#[derive(Debug, Default)]
pub struct SequenceSortedInputs;

impl SequenceSortedInputs {
    pub fn new() -> Self {
        Self
    }
}

impl PhysicalOptimizerRule for SequenceSortedInputs {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !config.optimizer.sequence_sorted_inputs {
            return Ok(plan);
        }
        plan.transform_down(|plan| {
            let Some(merge) = (plan.as_ref() as &dyn ExecutionPlan)
                .downcast_ref::<SortPreservingMergeExec>()
            else {
                return Ok(Transformed::no(plan));
            };
            let input = merge.input();
            let Some(permutation) = ordered_partition_permutation(input, merge.expr())
            else {
                return Ok(Transformed::no(plan));
            };
            let input = preserve_input_order(Arc::clone(input))?;
            let ordered_input =
                if permutation.iter().enumerate().all(|(idx, &src)| idx == src) {
                    input
                } else {
                    Arc::new(ReorderPartitionsExec::new(input, permutation))
                        as Arc<dyn ExecutionPlan>
                };
            let replacement = ProgressiveEvalExec::new(ordered_input, merge.fetch());
            Ok(Transformed::yes(Arc::new(replacement) as _))
        })
        .data()
    }

    fn name(&self) -> &str {
        "SequenceSortedInputs"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Mark every node in `plan` as order-sensitive, so data sources keep their
/// partition-to-data mapping.
/// Otherwise, work stealing can modify which files end up being read in a given partition.
fn preserve_input_order(plan: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
    plan.transform_down(|plan| match plan.with_preserve_order(true) {
        Some(pinned) => Ok(Transformed::yes(pinned)),
        None => Ok(Transformed::no(plan)),
    })
    .data()
}

/// Per-partition ordering statistics: the (start, end) value of each sort
/// column (oriented to the sort direction) plus each sort column's null count.
struct PartitionOrderStats {
    starts: Vec<ScalarValue>,
    ends: Vec<ScalarValue>,
    null_counts: Vec<usize>,
}

/// Find an arrangement of `plan`'s partitions whose concatenation is globally
/// ordered by `ordering`. Returns None if no such arrangement exists.
///
/// Each partition is assumed to be internally ordered by `ordering` (the caller
/// only applies this to a merge whose input already satisfies the ordering).
fn ordered_partition_permutation(
    plan: &Arc<dyn ExecutionPlan>,
    ordering: &LexOrdering,
) -> Option<Vec<usize>> {
    let partition_count = plan.output_partitioning().partition_count();
    let mut stats: Vec<PartitionOrderStats> = Vec::with_capacity(partition_count);
    let stats_ctx = StatisticsContext::new();
    for partition_idx in 0..partition_count {
        let partition_stats = stats_ctx
            .compute(
                plan.as_ref(),
                &StatisticsArgs::new().with_partition(Some(partition_idx)),
            )
            .ok()?;
        stats.push(get_ordering_stats(&partition_stats, ordering)?);
    }

    // Order the partitions by the ordering
    let mut perm: Vec<usize> = (0..partition_count).collect();
    perm.sort_by(|&a, &b| compare_partitions(&stats[a], &stats[b], ordering));

    // Check for overlap in the ordering columns
    for pair in perm.windows(2) {
        if !boundary_ordered(&stats[pair[0]], &stats[pair[1]], ordering) {
            return None;
        }
    }

    Some(perm)
}

/// Compare two partitions by their boundary values corresponding to `ordering`.
fn compare_partitions(
    a: &PartitionOrderStats,
    b: &PartitionOrderStats,
    ordering: &LexOrdering,
) -> Ordering {
    compare_boundary_values(&a.starts, &b.starts, ordering)
        .then_with(|| compare_boundary_values(&a.ends, &b.ends, ordering))
}

/// Compare two partition boundaries based on the ordering columns and sort directions.
/// Incomparable values are treated as equal.
fn compare_boundary_values(
    a: &[ScalarValue],
    b: &[ScalarValue],
    ordering: &LexOrdering,
) -> Ordering {
    for (i, sort_expr) in ordering.iter().enumerate() {
        let Some(cmp) = a[i].partial_cmp(&b[i]) else {
            return Ordering::Equal;
        };
        let cmp = if sort_expr.options.descending {
            cmp.reverse()
        } else {
            cmp
        };
        if cmp != Ordering::Equal {
            return cmp;
        }
    }
    Ordering::Equal
}

/// Whether we can prove that `cur` is ordered after `prev` under `ordering`.
fn boundary_ordered(
    prev: &PartitionOrderStats,
    cur: &PartitionOrderStats,
    ordering: &LexOrdering,
) -> bool {
    for (i, sort_expr) in ordering.iter().enumerate() {
        // Reject nulls that could sort onto the wrong side of this boundary.
        let boundary_null_count = if sort_expr.options.nulls_first {
            cur.null_counts[i]
        } else {
            prev.null_counts[i]
        };
        if boundary_null_count != 0 {
            return false;
        }
        // Incomparable values (partial_cmp is None) are rejected.
        let Some(cmp) = cur.starts[i].partial_cmp(&prev.ends[i]) else {
            return false;
        };
        let cmp = if sort_expr.options.descending {
            cmp.reverse()
        } else {
            cmp
        };
        match cmp {
            Ordering::Greater => return true,
            Ordering::Less => return false,
            Ordering::Equal => continue, // Continue to the next sort column
        }
    }
    // If we reach here, all sort columns are equal
    true
}

fn get_ordering_stats(
    stats: &Arc<Statistics>,
    ordering: &LexOrdering,
) -> Option<PartitionOrderStats> {
    let mut starts = Vec::with_capacity(ordering.len());
    let mut ends = Vec::with_capacity(ordering.len());
    let mut null_counts = Vec::with_capacity(ordering.len());

    for sort_expr in ordering.iter() {
        let column = sort_expr.expr.downcast_ref::<Column>()?;
        let col_stats = stats.column_statistics.get(column.index())?;
        // We require exact stats to guarantee no overlap in partition ranges.
        if !(col_stats.null_count.is_exact()?
            && col_stats.min_value.is_exact()?
            && col_stats.max_value.is_exact()?)
        {
            return None;
        }
        // Note that for secondary sort columns, the start and end values are bounds
        // on the actual start and end, as we only have access to the min/max stats,
        // and not the sort-column values from the first and last rows.
        // This means we may reject some orderings that are actually valid because we
        // can't prove they're non-overlapping.
        let (start, end) = if sort_expr.options.descending {
            (
                col_stats.max_value.get_value()?,
                col_stats.min_value.get_value()?,
            )
        } else {
            (
                col_stats.min_value.get_value()?,
                col_stats.max_value.get_value()?,
            )
        };
        // Stats may be null for all-null or empty partitions.
        // For now, don't try to optimize this case:
        if start.is_null() || end.is_null() {
            return None;
        }
        starts.push(start.clone());
        ends.push(end.clone());
        null_counts.push(*col_stats.null_count.get_value()?);
    }

    Some(PartitionOrderStats {
        starts,
        ends,
        null_counts,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::compute::SortOptions;
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion_common::stats::{ColumnStatistics, Precision};
    use datafusion_common::tree_node::TreeNodeRecursion;
    use datafusion_execution::{SendableRecordBatchStream, TaskContext};
    use datafusion_physical_expr::{
        EquivalenceProperties, Partitioning, PhysicalSortExpr,
    };
    use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
    use datafusion_physical_plan::execution_plan::{Boundedness, EmissionType};
    use datafusion_physical_plan::{DisplayAs, DisplayFormatType, PlanProperties};

    /// Test plan with fixed per-partition statistics
    #[derive(Debug)]
    struct StatsTestExec {
        stats: Vec<Statistics>,
        cache: Arc<PlanProperties>,
    }

    impl StatsTestExec {
        fn new(stats: Vec<Statistics>) -> Self {
            let cache = Arc::new(PlanProperties::new(
                EquivalenceProperties::new(test_schema()),
                Partitioning::UnknownPartitioning(stats.len()),
                EmissionType::Incremental,
                Boundedness::Bounded,
            ));
            Self { stats, cache }
        }
    }

    impl DisplayAs for StatsTestExec {
        fn fmt_as(
            &self,
            _t: DisplayFormatType,
            f: &mut std::fmt::Formatter<'_>,
        ) -> std::fmt::Result {
            write!(f, "StatsTestExec")
        }
    }

    impl ExecutionPlan for StatsTestExec {
        fn name(&self) -> &'static str {
            "StatsTestExec"
        }

        fn properties(&self) -> &Arc<PlanProperties> {
            &self.cache
        }

        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![]
        }

        fn apply_expressions(
            &self,
            _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
        ) -> Result<TreeNodeRecursion> {
            Ok(TreeNodeRecursion::Continue)
        }

        fn with_new_children(
            self: Arc<Self>,
            _children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            Ok(self)
        }

        fn execute(
            &self,
            _partition: usize,
            _context: Arc<TaskContext>,
        ) -> Result<SendableRecordBatchStream> {
            unimplemented!("StatsTestExec is only used for planning")
        }

        fn statistics_from_inputs(
            &self,
            _input_stats: &[Arc<Statistics>],
            args: &StatisticsArgs,
        ) -> Result<Arc<Statistics>> {
            match args.partition() {
                Some(idx) => Ok(Arc::new(self.stats[idx].clone())),
                None => Ok(Arc::new(Statistics::new_unknown(&self.schema()))),
            }
        }
    }

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("t", DataType::Int64, true),
            Field::new("id", DataType::Int64, true),
        ]))
    }

    fn sort_expr(index: usize, name: &str, options: SortOptions) -> PhysicalSortExpr {
        PhysicalSortExpr::new(Arc::new(Column::new(name, index)), options)
    }

    /// Ascending order on the specified column, with nulls last
    fn asc(index: usize, name: &str) -> PhysicalSortExpr {
        sort_expr(
            index,
            name,
            SortOptions {
                descending: false,
                nulls_first: false,
            },
        )
    }

    /// Descending order on the specified column, with nulls first
    fn desc(index: usize, name: &str) -> PhysicalSortExpr {
        sort_expr(
            index,
            name,
            SortOptions {
                descending: true,
                nulls_first: true,
            },
        )
    }

    fn exact_i64(min: i64, max: i64, null_count: usize) -> ColumnStatistics {
        ColumnStatistics {
            null_count: Precision::Exact(null_count),
            min_value: Precision::Exact(ScalarValue::Int64(Some(min))),
            max_value: Precision::Exact(ScalarValue::Int64(Some(max))),
            ..Default::default()
        }
    }

    fn partition(column_statistics: Vec<ColumnStatistics>) -> Statistics {
        Statistics {
            num_rows: Precision::Exact(10),
            total_byte_size: Precision::Exact(100),
            column_statistics,
        }
    }

    #[test]
    fn equal_first_column_boundary_ordered_by_second_column() {
        // The partitions share t = 100 on the boundary; the disjoint id
        // ranges disambiguate, so the check falls through to the second
        // sort column and accepts.
        let plan: Arc<dyn ExecutionPlan> = Arc::new(StatsTestExec::new(vec![
            partition(vec![exact_i64(0, 100, 0), exact_i64(0, 10, 0)]),
            partition(vec![exact_i64(100, 200, 0), exact_i64(11, 20, 0)]),
        ]));
        let ordering = LexOrdering::new(vec![asc(0, "t"), asc(1, "id")]).unwrap();

        let perm = ordered_partition_permutation(&plan, &ordering)
            .expect("expected an ordered permutation");
        assert_eq!(perm, vec![0, 1]);
    }

    #[test]
    fn equal_first_column_boundary_overlapping_second_column() {
        let plan: Arc<dyn ExecutionPlan> = Arc::new(StatsTestExec::new(vec![
            partition(vec![exact_i64(0, 100, 0), exact_i64(0, 10, 0)]),
            partition(vec![exact_i64(100, 200, 0), exact_i64(5, 20, 0)]),
        ]));
        let ordering = LexOrdering::new(vec![asc(0, "t"), asc(1, "id")]).unwrap();

        assert!(ordered_partition_permutation(&plan, &ordering).is_none());
    }

    #[test]
    fn boundary_equal_on_all_sort_columns_is_accepted() {
        // min == prev max on every sort column is considered ordered.
        let plan: Arc<dyn ExecutionPlan> = Arc::new(StatsTestExec::new(vec![
            partition(vec![exact_i64(0, 100, 0), exact_i64(0, 10, 0)]),
            partition(vec![exact_i64(100, 200, 0), exact_i64(10, 20, 0)]),
        ]));
        let ordering = LexOrdering::new(vec![asc(0, "t"), asc(1, "id")]).unwrap();

        assert!(ordered_partition_permutation(&plan, &ordering).is_some());
    }

    #[test]
    fn nulls_only_allowed_in_last_partition_for_nulls_last() {
        let ordering = LexOrdering::new(vec![asc(0, "t")]).unwrap();

        // Nulls in the last partition sort after all values: accepted.
        let plan: Arc<dyn ExecutionPlan> = Arc::new(StatsTestExec::new(vec![
            partition(vec![exact_i64(0, 99, 0), exact_i64(0, 10, 0)]),
            partition(vec![exact_i64(100, 200, 2), exact_i64(0, 10, 0)]),
        ]));
        assert!(ordered_partition_permutation(&plan, &ordering).is_some());

        // Nulls in the first partition would surface mid-stream: rejected.
        let plan: Arc<dyn ExecutionPlan> = Arc::new(StatsTestExec::new(vec![
            partition(vec![exact_i64(0, 99, 2), exact_i64(0, 10, 0)]),
            partition(vec![exact_i64(100, 200, 0), exact_i64(0, 10, 0)]),
        ]));
        assert!(ordered_partition_permutation(&plan, &ordering).is_none());
    }

    #[test]
    fn nulls_only_allowed_in_first_partition_for_nulls_first() {
        let ordering = LexOrdering::new(vec![desc(0, "t")]).unwrap();

        // Descending partitions with nulls leading in the first: accepted.
        let plan: Arc<dyn ExecutionPlan> = Arc::new(StatsTestExec::new(vec![
            partition(vec![exact_i64(100, 200, 2), exact_i64(0, 10, 0)]),
            partition(vec![exact_i64(0, 99, 0), exact_i64(0, 10, 0)]),
        ]));
        assert!(ordered_partition_permutation(&plan, &ordering).is_some());

        // Nulls in the last partition sort before its values: rejected.
        let plan: Arc<dyn ExecutionPlan> = Arc::new(StatsTestExec::new(vec![
            partition(vec![exact_i64(100, 200, 0), exact_i64(0, 10, 0)]),
            partition(vec![exact_i64(0, 99, 2), exact_i64(0, 10, 0)]),
        ]));
        assert!(ordered_partition_permutation(&plan, &ordering).is_none());
    }

    #[test]
    fn nulls_in_deeper_sort_column_hidden_by_first_column_break() {
        // The middle partition carries a null in the second sort column. Its
        // boundary with the first partition is strict on the first column, so
        // the second column is never inspected for it. The boundary with the
        // last partition is equal on the first column and falls through to
        // the second: the middle partition's non-null max (4) < the last
        // partition's min (5) looks ordered, but the middle partition's null
        // rows sort after every non-null value (nulls last), so a row like
        // (20, NULL) would precede (20, 5) in the concatenation.
        let plan: Arc<dyn ExecutionPlan> = Arc::new(StatsTestExec::new(vec![
            partition(vec![exact_i64(0, 9, 0), exact_i64(0, 9, 0)]),
            partition(vec![exact_i64(10, 20, 0), exact_i64(0, 4, 1)]),
            partition(vec![exact_i64(20, 30, 0), exact_i64(5, 8, 0)]),
        ]));
        let ordering = LexOrdering::new(vec![asc(0, "t"), asc(1, "id")]).unwrap();

        assert!(ordered_partition_permutation(&plan, &ordering).is_none());
    }

    #[test]
    fn nulls_in_deeper_sort_column_harmless_when_boundaries_strict_on_first() {
        // Both of the middle partition's boundaries are strict on the first
        // column, so the second sort column is never relied on and its nulls
        // cannot surface out of order.
        let plan: Arc<dyn ExecutionPlan> = Arc::new(StatsTestExec::new(vec![
            partition(vec![exact_i64(0, 9, 0), exact_i64(0, 9, 0)]),
            partition(vec![exact_i64(10, 19, 0), exact_i64(0, 4, 1)]),
            partition(vec![exact_i64(20, 30, 0), exact_i64(5, 8, 0)]),
        ]));
        let ordering = LexOrdering::new(vec![asc(0, "t"), asc(1, "id")]).unwrap();

        assert!(ordered_partition_permutation(&plan, &ordering).is_some());
    }

    #[test]
    fn nulls_in_first_partition_with_all_equal_values_rejected() {
        // Every partition shares the same value on both sort columns, so no
        // boundary breaks early and every column is inspected. The first
        // partition's nulls (sorting last) would surface before the later
        // partitions' rows; the first boundary must reject them.
        let plan: Arc<dyn ExecutionPlan> = Arc::new(StatsTestExec::new(vec![
            partition(vec![exact_i64(5, 5, 0), exact_i64(7, 7, 1)]),
            partition(vec![exact_i64(5, 5, 0), exact_i64(7, 7, 0)]),
            partition(vec![exact_i64(5, 5, 0), exact_i64(7, 7, 0)]),
        ]));
        let ordering = LexOrdering::new(vec![asc(0, "t"), asc(1, "id")]).unwrap();

        assert!(ordered_partition_permutation(&plan, &ordering).is_none());
    }

    #[test]
    fn nulls_in_middle_partition_with_all_equal_values_rejected() {
        // As above, but the nulls sit in the middle partition: its boundary
        // with the *next* partition is the one that must reject them.
        let plan: Arc<dyn ExecutionPlan> = Arc::new(StatsTestExec::new(vec![
            partition(vec![exact_i64(5, 5, 0), exact_i64(7, 7, 0)]),
            partition(vec![exact_i64(5, 5, 0), exact_i64(7, 7, 1)]),
            partition(vec![exact_i64(5, 5, 0), exact_i64(7, 7, 0)]),
        ]));
        let ordering = LexOrdering::new(vec![asc(0, "t"), asc(1, "id")]).unwrap();

        assert!(ordered_partition_permutation(&plan, &ordering).is_none());
    }

    #[test]
    fn nulls_in_last_partition_with_all_equal_values_accepted() {
        // Nulls sorting last in the last partition stream at the very end of
        // the concatenation: correct, and there is no later boundary to
        // invalidate.
        let plan: Arc<dyn ExecutionPlan> = Arc::new(StatsTestExec::new(vec![
            partition(vec![exact_i64(5, 5, 0), exact_i64(7, 7, 0)]),
            partition(vec![exact_i64(5, 5, 0), exact_i64(7, 7, 0)]),
            partition(vec![exact_i64(5, 5, 0), exact_i64(7, 7, 1)]),
        ]));
        let ordering = LexOrdering::new(vec![asc(0, "t"), asc(1, "id")]).unwrap();

        assert!(ordered_partition_permutation(&plan, &ordering).is_some());
    }

    #[test]
    fn equal_starts_ordered_by_end_values() {
        // Both partitions start at 0, but the [0, 0] partition must come
        // before [0, 1]: placing the partition with the smaller end first is
        // the only arrangement whose concatenation stays ordered.
        let plan: Arc<dyn ExecutionPlan> = Arc::new(StatsTestExec::new(vec![
            partition(vec![exact_i64(0, 1, 0), exact_i64(0, 10, 0)]),
            partition(vec![exact_i64(0, 0, 0), exact_i64(0, 10, 0)]),
        ]));
        let ordering = LexOrdering::new(vec![asc(0, "t")]).unwrap();

        let perm = ordered_partition_permutation(&plan, &ordering)
            .expect("expected a permutation");
        assert_eq!(perm, vec![1, 0]);
    }

    #[test]
    fn descending_ordering_reorders_ascending_layout() {
        // Ascending partition layout under a descending ordering: the
        // partitions are non-overlapping, so a permutation reverses them into
        // descending order rather than bailing.
        let plan: Arc<dyn ExecutionPlan> = Arc::new(StatsTestExec::new(vec![
            partition(vec![exact_i64(0, 99, 0), exact_i64(0, 10, 0)]),
            partition(vec![exact_i64(100, 200, 0), exact_i64(0, 10, 0)]),
        ]));
        let ordering = LexOrdering::new(vec![desc(0, "t")]).unwrap();

        let perm = ordered_partition_permutation(&plan, &ordering)
            .expect("expected a permutation");
        assert_eq!(perm, vec![1, 0]);
    }

    #[test]
    fn incomparable_statistics_types_bail_out() {
        // Mismatched stat types across partitions are incomparable; they
        // must not be treated as an equal boundary.
        let utf8 = |value: &str| ScalarValue::Utf8(Some(value.to_string()));
        let plan: Arc<dyn ExecutionPlan> = Arc::new(StatsTestExec::new(vec![
            partition(vec![exact_i64(0, 100, 0), exact_i64(0, 10, 0)]),
            partition(vec![
                ColumnStatistics {
                    null_count: Precision::Exact(0),
                    min_value: Precision::Exact(utf8("a")),
                    max_value: Precision::Exact(utf8("b")),
                    ..Default::default()
                },
                exact_i64(0, 10, 0),
            ]),
        ]));
        let ordering = LexOrdering::new(vec![asc(0, "t")]).unwrap();

        assert!(ordered_partition_permutation(&plan, &ordering).is_none());
    }

    #[test]
    fn inexact_statistics_bail_out() {
        let inexact = ColumnStatistics {
            null_count: Precision::Exact(0),
            min_value: Precision::Inexact(ScalarValue::Int64(Some(100))),
            max_value: Precision::Exact(ScalarValue::Int64(Some(200))),
            ..Default::default()
        };
        let plan: Arc<dyn ExecutionPlan> = Arc::new(StatsTestExec::new(vec![
            partition(vec![exact_i64(0, 99, 0), exact_i64(0, 10, 0)]),
            partition(vec![inexact, exact_i64(0, 10, 0)]),
        ]));
        let ordering = LexOrdering::new(vec![asc(0, "t")]).unwrap();

        assert!(ordered_partition_permutation(&plan, &ordering).is_none());
    }

    #[test]
    fn null_statistics_values_bail_out() {
        // All-null or empty partitions report exact but null min/max values;
        // they prove nothing about the partition's range. An all-null first
        // partition under an ascending nulls-first ordering is the dangerous
        // layout: the null guard passes (the later partition has no nulls)
        // and a null scalar compares before any value, so without the
        // explicit bail-out the boundary would look ordered.
        let all_null = ColumnStatistics {
            null_count: Precision::Exact(10),
            min_value: Precision::Exact(ScalarValue::Int64(None)),
            max_value: Precision::Exact(ScalarValue::Int64(None)),
            ..Default::default()
        };
        let plan: Arc<dyn ExecutionPlan> = Arc::new(StatsTestExec::new(vec![
            partition(vec![all_null, exact_i64(0, 10, 0)]),
            partition(vec![exact_i64(0, 99, 0), exact_i64(0, 10, 0)]),
        ]));
        let ordering = LexOrdering::new(vec![sort_expr(
            0,
            "t",
            SortOptions {
                descending: false,
                nulls_first: true,
            },
        )])
        .unwrap();

        assert!(ordered_partition_permutation(&plan, &ordering).is_none());
    }

    #[test]
    fn missing_column_statistics_bail_out() {
        // The ordering references a column index beyond the available
        // statistics; the lookup must bail out rather than panic.
        let plan: Arc<dyn ExecutionPlan> = Arc::new(StatsTestExec::new(vec![
            partition(vec![exact_i64(0, 99, 0)]),
            partition(vec![exact_i64(100, 200, 0)]),
        ]));
        let ordering = LexOrdering::new(vec![asc(1, "id")]).unwrap();

        assert!(ordered_partition_permutation(&plan, &ordering).is_none());
    }
}
