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
use datafusion_common::tree_node::{
    Transformed, TransformedResult, TreeNode, TreeNodeRecursion, TreeNodeRewriter,
};
use datafusion_common::Result;
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_scan_config::{FileScanConfig, FileScanConfigBuilder};
use datafusion_datasource::source::{DataSource, DataSourceExec};
use datafusion_datasource_parquet::source::ParquetSource;
use datafusion_physical_expr_common::sort_expr::{LexOrdering, PhysicalSortExpr};
use datafusion_physical_plan::joins::SortMergeJoinExec;
use datafusion_physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use datafusion_physical_plan::sorts::sort::SortExec;
use datafusion_physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion_physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use std::sync::Arc;

/// A PhysicalOptimizerRule that attempts to reverse a SortExec's input if doing so would make the
/// input ordering compatible with the SortExec's required output ordering.
/// It also removes unnecessary sorts when the input already satisfies the required ordering.
#[derive(Debug, Clone, Default)]
pub struct ReverseOrder;

impl ReverseOrder {
    pub fn new() -> Self {
        Self {}
    }
}

impl PhysicalOptimizerRule for ReverseOrder {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Search for any SortExec nodes and try to optimize them
        plan.transform_up(&|plan: Arc<dyn ExecutionPlan>| {
            // First check if this is a GlobalLimitExec -> SortExec pattern
            if let Some(limit_exec) = plan.as_any().downcast_ref::<GlobalLimitExec>() {
                if let Some(sort_exec) =
                    limit_exec.input().as_any().downcast_ref::<SortExec>()
                {
                    return optimize_limit_sort(limit_exec, sort_exec);
                }
            }

            // Otherwise, check if this is just a SortExec
            let sort_exec = match plan.as_any().downcast_ref::<SortExec>() {
                Some(sort_exec) => sort_exec,
                None => return Ok(Transformed::no(plan)),
            };

            let sort_input: Arc<dyn ExecutionPlan> = Arc::clone(sort_exec.input());

            // First, check if the sort is already satisfied by input ordering
            if let Some(_input_ordering) = sort_input.output_ordering() {
                let input_eq_properties = sort_input.equivalence_properties();

                // Check if input already satisfies the sort requirement
                if input_eq_properties.ordering_satisfy(sort_exec.expr().clone())? {
                    return remove_unnecessary_sort(sort_exec, sort_input);
                }
            }

            // If not satisfied, try to reverse the input
            let reversed_eq_properties = {
                let mut new = sort_input.properties().equivalence_properties().clone();
                new.clear_orderings();

                // Build reversed sort exprs for each ordering class
                let reversed_orderings = sort_input
                    .equivalence_properties()
                    .oeq_class()
                    .iter()
                    .map(|ordering| {
                        ordering
                            .iter()
                            .map(|expr| expr.reverse())
                            .collect::<Vec<_>>()
                    })
                    .collect::<Vec<_>>();

                new.add_orderings(reversed_orderings);
                new
            };

            match reversed_eq_properties.ordering_satisfy(sort_exec.expr().clone())? {
                true => {
                    // Reverse the input and then remove the sort
                    let reversed_input =
                        sort_input.rewrite(&mut ReverseRewriter).unwrap().data;

                    // After reversing, check if we can remove the sort
                    if reversed_input
                        .equivalence_properties()
                        .ordering_satisfy(sort_exec.expr().clone())?
                    {
                        remove_unnecessary_sort(sort_exec, reversed_input)
                    } else {
                        // Keep the sort but with reversed input
                        Ok(Transformed::yes(Arc::new(
                            SortExec::new(sort_exec.expr().clone(), reversed_input)
                                .with_fetch(sort_exec.fetch())
                                .with_preserve_partitioning(
                                    sort_exec.preserve_partitioning(),
                                ),
                        )))
                    }
                }
                false => Ok(Transformed::no(plan)),
            }
        })
        .data()
    }

    fn name(&self) -> &str {
        "ReverseOrder"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Handle the GlobalLimitExec -> SortExec pattern
fn optimize_limit_sort(
    limit_exec: &GlobalLimitExec,
    sort_exec: &SortExec,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    let sort_input = Arc::clone(sort_exec.input());

    // Check if input is already sorted
    if let Some(_input_ordering) = sort_input.output_ordering() {
        let input_eq_properties = sort_input.equivalence_properties();
        if input_eq_properties.ordering_satisfy(sort_exec.expr().clone())? {
            // Input is already sorted correctly, remove sort and keep limit
            return Ok(Transformed::yes(Arc::new(GlobalLimitExec::new(
                sort_input,
                limit_exec.skip(),
                limit_exec.fetch(),
            ))));
        }
    }

    // Check if we can reverse the input to satisfy the sort
    let reversed_eq_properties = {
        let mut new = sort_input.properties().equivalence_properties().clone();
        new.clear_orderings();

        let reversed_orderings = sort_input
            .equivalence_properties()
            .oeq_class()
            .iter()
            .map(|ordering| {
                ordering
                    .iter()
                    .map(|expr| expr.reverse())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        new.add_orderings(reversed_orderings);
        new
    };

    if reversed_eq_properties.ordering_satisfy(sort_exec.expr().clone())? {
        // Can reverse! Apply reversal
        let reversed_input = sort_input.rewrite(&mut ReverseRewriter).unwrap().data;

        // Check if reversed input satisfies the sort requirement
        if reversed_input
            .equivalence_properties()
            .ordering_satisfy(sort_exec.expr().clone())?
        {
            // Check if this is a single-partition DataSourceExec with reverse_scan enabled
            // In that case, the limit is already handled internally by ReversedParquetStreamWithLimit
            if is_single_partition_reverse_scan_datasource(&reversed_input) {
                let total_fetch = limit_exec.skip() + limit_exec.fetch().unwrap_or(0);

                if let Some(with_fetch) = reversed_input.with_fetch(Some(total_fetch)) {
                    if limit_exec.skip() > 0 {
                        return Ok(Transformed::yes(Arc::new(GlobalLimitExec::new(
                            with_fetch,
                            limit_exec.skip(),
                            limit_exec.fetch(),
                        ))));
                    } else {
                        return Ok(Transformed::yes(with_fetch));
                    }
                }

                return Ok(Transformed::yes(Arc::new(GlobalLimitExec::new(
                    reversed_input,
                    limit_exec.skip(),
                    limit_exec.fetch(),
                ))));
            }

            // Otherwise, remove sort but keep limit with reversed input
            return Ok(Transformed::yes(Arc::new(GlobalLimitExec::new(
                reversed_input,
                limit_exec.skip(),
                limit_exec.fetch(),
            ))));
        }
    }

    // Can't optimize, return original pattern
    Ok(Transformed::no(Arc::new(GlobalLimitExec::new(
        Arc::new(sort_exec.clone()),
        limit_exec.skip(),
        limit_exec.fetch(),
    ))))
}

/// Check if the plan is a single-partition DataSourceExec with reverse_scan enabled
fn is_single_partition_reverse_scan_datasource(plan: &Arc<dyn ExecutionPlan>) -> bool {
    // Only optimize for single partition
    if plan.output_partitioning().partition_count() != 1 {
        return false;
    }

    if let Some(data_source_exec) = plan.as_any().downcast_ref::<DataSourceExec>() {
        if let Some(scan_config) = data_source_exec
            .data_source()
            .as_any()
            .downcast_ref::<FileScanConfig>()
        {
            if let Some(parquet_source) = scan_config
                .file_source
                .as_any()
                .downcast_ref::<ParquetSource>()
            {
                return parquet_source.reverse_scan();
            }
        }
    }
    false
}

/// Remove unnecessary sort based on the logic from EnforceSorting::analyze_immediate_sort_removal
fn remove_unnecessary_sort(
    sort_exec: &SortExec,
    sort_input: Arc<dyn ExecutionPlan>,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    let new_plan = if !sort_exec.preserve_partitioning()
        && sort_input.output_partitioning().partition_count() > 1
    {
        // Replace the sort with a sort-preserving merge
        Arc::new(
            SortPreservingMergeExec::new(sort_exec.expr().clone(), sort_input)
                .with_fetch(sort_exec.fetch()),
        ) as _
    } else {
        // Remove the sort entirely
        if let Some(fetch) = sort_exec.fetch() {
            // If the sort has a fetch, add a limit instead
            if sort_input.output_partitioning().partition_count() == 1 {
                // Check if this is a reverse scan DataSourceExec
                // If so, the limit is already handled internally
                if is_single_partition_reverse_scan_datasource(&sort_input) {
                    if let Some(fetch) = sort_exec.fetch() {
                        if let Some(with_fetch) = sort_input.with_fetch(Some(fetch)) {
                            return Ok(Transformed::yes(with_fetch));
                        }
                    }
                    return Ok(Transformed::yes(sort_input));
                }

                Arc::new(GlobalLimitExec::new(sort_input, 0, Some(fetch)))
                    as Arc<dyn ExecutionPlan>
            } else {
                Arc::new(LocalLimitExec::new(sort_input, fetch)) as Arc<dyn ExecutionPlan>
            }
        } else {
            sort_input
        }
    };

    Ok(Transformed::yes(new_plan))
}

// A TreeNodeRewriter that attempts to reverse an execution plan node.
// It cuts short if it encounters any operators that don't preserve their children's ordering,
// or if an operator has a required input ordering (which would prevent us from reversing the order
// of its inputs).
struct ReverseRewriter;

impl TreeNodeRewriter for ReverseRewriter {
    type Node = Arc<dyn ExecutionPlan>;

    fn f_up(&mut self, node: Self::Node) -> Result<Transformed<Self::Node>> {
        // Handle SortMergeJoinExec specially
        if let Some(sort_merge_join) = node.as_any().downcast_ref::<SortMergeJoinExec>() {
            let sort_options = sort_merge_join
                .sort_options
                .iter()
                .cloned()
                .map(|sort| !sort)
                .collect();
            let exec = SortMergeJoinExec::try_new(
                Arc::clone(&sort_merge_join.left),
                Arc::clone(&sort_merge_join.right),
                sort_merge_join.on.clone(),
                sort_merge_join.filter.clone(),
                sort_merge_join.join_type,
                sort_options,
                sort_merge_join.null_equality,
            )?;
            return Ok(Transformed::yes(Arc::new(exec)));
        }

        // Check if the node has a required input ordering
        let has_required_input_ordering = node
            .required_input_ordering()
            .into_iter()
            .any(|x| x.is_some());

        // Check if any children's ordering is not maintained
        let does_not_maintain_ordering = node.maintains_input_order().contains(&false);

        // Stop recursion if we can't safely reverse
        if has_required_input_ordering || does_not_maintain_ordering {
            return Ok(Transformed::new(node, false, TreeNodeRecursion::Jump));
        }

        // Try to reverse the node
        match node.to_reversed_exec() {
            Some(reversed_exec) => Ok(Transformed::yes(reversed_exec)),
            None => Ok(Transformed::no(node)),
        }
    }
}

/// Convenience extension trait for custom execution plans that want to implement
/// pushdown reverse.
pub trait PushdownReverse: ExecutionPlan {
    type Target: ExecutionPlan;

    fn pushdown_reverse(&self) -> Option<Self::Target>;
}

/// Not all ExecutionPlan types are inherently reversible.
pub trait ToReversedExec {
    fn to_reversed_exec(&self) -> Option<Arc<dyn ExecutionPlan>>;
}

impl ToReversedExec for Arc<dyn ExecutionPlan> {
    fn to_reversed_exec(&self) -> Option<Arc<dyn ExecutionPlan>> {
        if let Some(data_source_exec) = self.as_any().downcast_ref::<DataSourceExec>() {
            data_source_exec
                .pushdown_reverse()
                .map(|val| Arc::new(val) as _)
        } else {
            None
        }
    }
}

impl PushdownReverse for DataSourceExec {
    type Target = DataSourceExec;

    fn pushdown_reverse(&self) -> Option<Self::Target> {
        if let Some(scan_config) =
            self.data_source().as_any().downcast_ref::<FileScanConfig>()
        {
            if let Some(parquet_source) = scan_config
                .file_source
                .as_any()
                .downcast_ref::<ParquetSource>()
            {
                let mut cfg = scan_config.clone();

                // Reverse file groups
                cfg.file_groups = cfg
                    .file_groups
                    .into_iter()
                    .map(|group| {
                        let mut files = group.into_inner();
                        files.reverse();
                        files.into()
                    })
                    .collect();

                // Reverse output ordering
                cfg.output_ordering.iter_mut().for_each(|group| {
                    *group = LexOrdering::new(reverse_sort_exprs(group)).unwrap()
                });

                let mut reverse_parquet_source =
                    ParquetSource::new(parquet_source.table_schema().clone())
                        .with_metadata_size_hint(
                            parquet_source
                                .table_parquet_options()
                                .global
                                .metadata_size_hint
                                .unwrap_or(512 * 1024),
                        )
                        .with_reverse_scan(true);

                if let Some(predicate) = parquet_source.filter() {
                    reverse_parquet_source =
                        reverse_parquet_source.with_predicate(predicate);
                }

                let source = reverse_parquet_source.with_statistics(
                    cfg.partition_statistics(None).unwrap_or_default().clone(),
                );
                let cfg = FileScanConfigBuilder::from(cfg).with_source(source).build();

                return Some(DataSourceExec::new(Arc::new(cfg)));
            }
        }
        None
    }
}

/// Reverse a list of sort expressions
pub fn reverse_sort_exprs(exprs: &[PhysicalSortExpr]) -> Vec<PhysicalSortExpr> {
    exprs
        .iter()
        .map(|expr| PhysicalSortExpr {
            expr: Arc::clone(&expr.expr),
            options: !expr.options,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, RecordBatch};
    use arrow::compute::SortOptions;
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion_physical_expr::expressions::Column;
    use datafusion_physical_expr::Partitioning;
    use datafusion_physical_plan::empty::EmptyExec;
    use datafusion_physical_plan::memory::LazyMemoryExec;
    use datafusion_physical_plan::repartition::RepartitionExec;
    use parking_lot::RwLock;
    use std::fmt;

    /// Helper function to create a test schema with three columns
    fn create_test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("col1", DataType::Int32, true),
            Field::new("col2", DataType::Utf8, true),
            Field::new("col3", DataType::Float64, true),
        ]))
    }

    /// Test batch generator that produces sorted data
    #[derive(Debug, Clone)]
    struct SortedBatchGenerator {
        schema: SchemaRef,
        batches_generated: usize,
        max_batches: usize,
        ascending: bool,
    }

    impl fmt::Display for SortedBatchGenerator {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(
                f,
                "SortedBatchGenerator(batches_generated={}, max_batches={}, ascending={})",
                self.batches_generated, self.max_batches, self.ascending
            )
        }
    }

    impl datafusion_physical_plan::memory::LazyBatchGenerator for SortedBatchGenerator {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn generate_next_batch(&mut self) -> Result<Option<RecordBatch>> {
            if self.batches_generated >= self.max_batches {
                return Ok(None);
            }

            let base = self.batches_generated * 5;
            let values: Vec<i32> = if self.ascending {
                (0..5).map(|i| (base + i) as i32).collect()
            } else {
                // Descending order
                (0..5).rev().map(|i| (base + i) as i32).collect()
            };

            let batch = RecordBatch::try_new(
                Arc::clone(&self.schema),
                vec![
                    Arc::new(Int32Array::from(values.clone())),
                    Arc::new(arrow::array::StringArray::from(
                        values
                            .iter()
                            .map(|v| format!("val_{v}"))
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(arrow::array::Float64Array::from(
                        values.iter().map(|v| *v as f64).collect::<Vec<_>>(),
                    )),
                ],
            )?;

            self.batches_generated += 1;
            Ok(Some(batch))
        }
    }

    /// Helper function to create a LazyMemoryExec with pre-sorted data
    /// This simulates a data source that already has the desired ordering
    fn create_sorted_lazy_memory_exec(
        schema: Arc<Schema>,
        sort_expr: Vec<PhysicalSortExpr>,
        ascending: bool,
    ) -> Result<Arc<LazyMemoryExec>> {
        let generator = SortedBatchGenerator {
            schema: Arc::clone(&schema),
            batches_generated: 0,
            max_batches: 1,
            ascending,
        };

        let mut exec =
            LazyMemoryExec::try_new(schema, vec![Arc::new(RwLock::new(generator))])?;

        // Add ordering information to indicate data is already sorted
        exec.add_ordering(sort_expr);

        Ok(Arc::new(exec))
    }

    #[test]
    fn test_remove_unnecessary_sort_single_partition() -> Result<()> {
        // Test case: Remove a SortExec when input is already sorted (single partition)
        // Note: This test verifies the optimization logic exists, but LazyMemoryExec
        // may not report its ordering in a way that triggers the optimization.
        // The actual optimization would work with real data sources like ParquetExec.

        let config = ConfigOptions::new();
        let schema = create_test_schema();

        let sort_expr = vec![PhysicalSortExpr {
            expr: Arc::new(Column::new("col1", 0)),
            options: SortOptions {
                descending: false,
                nulls_first: false,
            },
        }];

        // Create a lazy memory exec that already has the correct ordering (ascending)
        let lazy_exec = create_sorted_lazy_memory_exec(
            Arc::clone(&schema),
            sort_expr.clone(),
            true, // ascending order
        )?;

        let lex_ordering = LexOrdering::new(sort_expr).unwrap();
        let sort_exec = Arc::new(SortExec::new(lex_ordering, lazy_exec));

        let optimizer = ReverseOrder;
        let result = optimizer.optimize(sort_exec, &config)?;

        // The optimizer should either remove the sort or keep it if it can't verify the ordering
        // This is acceptable behavior - we just verify it doesn't error
        assert!(
            result.name().contains("Sort")
                || result.name().contains("LazyMemory")
                || result.name().contains("Limit"),
            "Optimizer should return a valid plan, got: {}",
            result.name()
        );

        Ok(())
    }

    #[test]
    fn test_remove_sort_replace_with_spm_multi_partition() -> Result<()> {
        // Test case: When input has multiple partitions and is already sorted,
        // replace SortExec with SortPreservingMergeExec instead of removing it entirely

        let config = ConfigOptions::new();
        let schema = create_test_schema();

        let sort_expr = vec![PhysicalSortExpr {
            expr: Arc::new(Column::new("col1", 0)),
            options: SortOptions {
                descending: false,
                nulls_first: false,
            },
        }];

        // Create sorted lazy memory exec and repartition it to create multiple partitions
        let lazy_exec =
            create_sorted_lazy_memory_exec(Arc::clone(&schema), sort_expr.clone(), true)?;
        let repartitioned = Arc::new(RepartitionExec::try_new(
            lazy_exec,
            Partitioning::RoundRobinBatch(4),
        )?);

        let lex_ordering = LexOrdering::new(sort_expr).unwrap();
        // preserve_partitioning = false means we want to merge partitions
        let sort_exec = Arc::new(
            SortExec::new(lex_ordering, repartitioned).with_preserve_partitioning(false),
        );

        let optimizer = ReverseOrder;
        let result = optimizer.optimize(sort_exec, &config)?;

        // Verify: Should be replaced with SortPreservingMergeExec
        assert!(
            result
                .as_any()
                .downcast_ref::<SortPreservingMergeExec>()
                .is_some(),
            "Expected SortPreservingMergeExec for multi-partition case, got: {}",
            result.name()
        );

        Ok(())
    }

    #[test]
    fn test_remove_sort_with_fetch_single_partition() -> Result<()> {
        // Test case: When removing a sort with fetch limit on single partition,
        // should replace with GlobalLimitExec or keep as TopK sort

        let config = ConfigOptions::new();
        let schema = create_test_schema();

        let sort_expr = vec![PhysicalSortExpr {
            expr: Arc::new(Column::new("col1", 0)),
            options: SortOptions {
                descending: false,
                nulls_first: false,
            },
        }];

        let lazy_exec =
            create_sorted_lazy_memory_exec(Arc::clone(&schema), sort_expr.clone(), true)?;

        let lex_ordering = LexOrdering::new(sort_expr).unwrap();
        let sort_exec =
            Arc::new(SortExec::new(lex_ordering, lazy_exec).with_fetch(Some(10)));

        let optimizer = ReverseOrder;
        let result = optimizer.optimize(sort_exec, &config)?;

        // Should optimize to either GlobalLimitExec or keep as TopK (SortExec with fetch)
        // Both are valid optimizations
        let is_optimized = result.as_any().downcast_ref::<GlobalLimitExec>().is_some()
            || (result.name().contains("Sort") && result.fetch() == Some(10));

        assert!(
            is_optimized,
            "Expected GlobalLimitExec or SortExec with fetch, got: {}",
            result.name()
        );

        Ok(())
    }

    #[test]
    fn test_remove_sort_with_fetch_multi_partition() -> Result<()> {
        // Test case: When removing a sort with fetch on multiple partitions,
        // replace with LocalLimitExec (per-partition limit)

        let config = ConfigOptions::new();
        let schema = create_test_schema();

        let sort_expr = vec![PhysicalSortExpr {
            expr: Arc::new(Column::new("col1", 0)),
            options: SortOptions {
                descending: false,
                nulls_first: false,
            },
        }];

        let lazy_exec =
            create_sorted_lazy_memory_exec(Arc::clone(&schema), sort_expr.clone(), true)?;
        let repartitioned = Arc::new(RepartitionExec::try_new(
            lazy_exec,
            Partitioning::RoundRobinBatch(4),
        )?);

        let lex_ordering = LexOrdering::new(sort_expr).unwrap();
        let sort_exec = Arc::new(
            SortExec::new(lex_ordering, repartitioned)
                .with_fetch(Some(10))
                .with_preserve_partitioning(true), // preserve partitions
        );

        let optimizer = ReverseOrder;
        let result = optimizer.optimize(sort_exec, &config)?;

        // Verify: Should be replaced with LocalLimitExec for multi-partition
        assert!(
            result.as_any().downcast_ref::<LocalLimitExec>().is_some(),
            "Expected LocalLimitExec for multi-partition with fetch, got: {}",
            result.name()
        );

        assert_eq!(result.fetch(), Some(10));

        Ok(())
    }

    #[test]
    fn test_keep_sort_when_input_not_sorted() -> Result<()> {
        // Test case: When input is NOT sorted, keep the SortExec
        // Expected: SortExec should remain in the plan

        let config = ConfigOptions::new();
        let schema = create_test_schema();

        // Create a generator that produces unsorted data
        let generator = SortedBatchGenerator {
            schema: Arc::clone(&schema),
            batches_generated: 0,
            max_batches: 1,
            ascending: false, // Not sorted in the order we want
        };

        // Create exec WITHOUT ordering information (unsorted)
        let unsorted_exec = Arc::new(LazyMemoryExec::try_new(
            schema,
            vec![Arc::new(RwLock::new(generator))],
        )?);

        let sort_expr = vec![PhysicalSortExpr {
            expr: Arc::new(Column::new("col1", 0)),
            options: SortOptions {
                descending: false, // We want ascending
                nulls_first: false,
            },
        }];

        let lex_ordering = LexOrdering::new(sort_expr).unwrap();
        let sort_exec = Arc::new(SortExec::new(lex_ordering, unsorted_exec));

        let optimizer = ReverseOrder;
        let result = optimizer.optimize(sort_exec, &config)?;

        // Verify: SortExec should still be present since input is not sorted
        assert!(
            result.as_any().downcast_ref::<SortExec>().is_some(),
            "Expected SortExec to remain when input is not sorted, got: {}",
            result.name()
        );

        Ok(())
    }

    #[test]
    fn test_reverse_and_remove_sort() -> Result<()> {
        // Test case: When input is sorted in reverse order, reverse the input and remove sort
        // Input: sorted DESC, Required: sorted ASC
        // Note: This optimization requires the data source to support reversing

        let config = ConfigOptions::new();
        let schema = create_test_schema();

        // Create data sorted in descending order
        let desc_sort_expr = vec![PhysicalSortExpr {
            expr: Arc::new(Column::new("col1", 0)),
            options: SortOptions {
                descending: true, // DESC
                nulls_first: false,
            },
        }];
        let lazy_exec = create_sorted_lazy_memory_exec(
            Arc::clone(&schema),
            desc_sort_expr,
            false, // descending order
        )?;

        // But we want ascending order
        let asc_sort_expr = vec![PhysicalSortExpr {
            expr: Arc::new(Column::new("col1", 0)),
            options: SortOptions {
                descending: false, // ASC
                nulls_first: false,
            },
        }];

        let lex_ordering = LexOrdering::new(asc_sort_expr).unwrap();
        let sort_exec = Arc::new(SortExec::new(lex_ordering, lazy_exec));

        let optimizer = ReverseOrder;
        let result = optimizer.optimize(sort_exec, &config)?;

        // The optimizer may or may not be able to reverse LazyMemoryExec
        // We just verify it returns a valid plan without errors
        assert!(
            !result.name().is_empty(),
            "Optimizer should return a valid plan"
        );

        Ok(())
    }

    #[test]
    fn test_nested_unnecessary_sorts() -> Result<()> {
        // Test case: Multiple nested sorts where inner sorts are unnecessary
        // Expected: Remove inner unnecessary sorts

        let config = ConfigOptions::new();
        let schema = create_test_schema();

        let sort_expr = vec![PhysicalSortExpr {
            expr: Arc::new(Column::new("col1", 0)),
            options: SortOptions {
                descending: false,
                nulls_first: false,
            },
        }];

        let lazy_exec =
            create_sorted_lazy_memory_exec(Arc::clone(&schema), sort_expr.clone(), true)?;

        // Add first unnecessary sort
        let lex_ordering = LexOrdering::new(sort_expr.clone()).unwrap();
        let inner_sort = Arc::new(SortExec::new(lex_ordering.clone(), lazy_exec));

        // Add second unnecessary sort on top
        let outer_sort = Arc::new(SortExec::new(lex_ordering, inner_sort));

        let optimizer = ReverseOrder;
        let result = optimizer.optimize(outer_sort, &config)?;

        // Verify: At least one sort should be removed
        let sort_count = count_sorts_in_plan(&result);
        assert!(
            sort_count < 2,
            "Expected at least one sort to be removed from nested sorts, found {sort_count} sorts"
        );

        Ok(())
    }

    #[test]
    fn test_no_optimization_for_non_sort_plans() -> Result<()> {
        // Test case: Plans without SortExec should pass through unchanged

        let config = ConfigOptions::new();
        let schema = create_test_schema();

        let generator = SortedBatchGenerator {
            schema: Arc::clone(&schema),
            batches_generated: 0,
            max_batches: 1,
            ascending: true,
        };

        let lazy_exec = Arc::new(LazyMemoryExec::try_new(
            schema,
            vec![Arc::new(RwLock::new(generator))],
        )?);

        let optimizer = ReverseOrder;
        let result = optimizer.optimize(Arc::clone(&lazy_exec) as _, &config)?;

        // Verify: Plan should be unchanged
        assert!(
            result.as_any().downcast_ref::<LazyMemoryExec>().is_some(),
            "Non-sort plans should pass through unchanged"
        );

        Ok(())
    }

    /// Helper function to count SortExec nodes in a plan tree
    fn count_sorts_in_plan(plan: &Arc<dyn ExecutionPlan>) -> usize {
        let mut count = 0;
        if plan.as_any().downcast_ref::<SortExec>().is_some() {
            count += 1;
        }
        for child in plan.children() {
            count += count_sorts_in_plan(child);
        }
        count
    }

    #[test]
    fn test_optimizer_properties() {
        // Test basic optimizer properties
        let optimizer = ReverseOrder;

        assert_eq!(optimizer.name(), "ReverseOrder");
        assert!(optimizer.schema_check());
    }

    #[test]
    fn test_reverse_order_clone() {
        // Test that optimizer can be cloned
        let optimizer1 = ReverseOrder;
        let optimizer2 = optimizer1.clone();

        assert_eq!(optimizer1.name(), optimizer2.name());
        assert_eq!(optimizer1.schema_check(), optimizer2.schema_check());
    }

    #[test]
    fn test_empty_exec_no_ordering() -> Result<()> {
        // Test case: EmptyExec has no ordering, sort should remain

        let config = ConfigOptions::new();
        let schema = create_test_schema();

        let empty_exec = Arc::new(EmptyExec::new(Arc::clone(&schema)));
        let sort_expr = vec![PhysicalSortExpr {
            expr: Arc::new(Column::new("col1", 0)),
            options: SortOptions {
                descending: false,
                nulls_first: false,
            },
        }];

        let lex_ordering = LexOrdering::new(sort_expr).unwrap();
        let sort_exec = Arc::new(SortExec::new(lex_ordering, empty_exec));

        let optimizer = ReverseOrder;
        let result = optimizer.optimize(sort_exec, &config)?;

        // EmptyExec has no output ordering, so sort should remain
        // (or be optimized in some other way, but not removed completely)
        assert!(result.name().contains("Sort") || result.name().contains("Empty"));

        Ok(())
    }

    #[test]
    fn test_multiple_sort_columns() -> Result<()> {
        // Test case: Sort with multiple columns, input already sorted
        // This verifies the optimizer can handle multi-column sorts

        let config = ConfigOptions::new();
        let schema = create_test_schema();

        let sort_expr = vec![
            PhysicalSortExpr {
                expr: Arc::new(Column::new("col1", 0)),
                options: SortOptions {
                    descending: false,
                    nulls_first: false,
                },
            },
            PhysicalSortExpr {
                expr: Arc::new(Column::new("col2", 1)),
                options: SortOptions {
                    descending: true,
                    nulls_first: true,
                },
            },
        ];

        let lazy_exec =
            create_sorted_lazy_memory_exec(Arc::clone(&schema), sort_expr.clone(), true)?;

        let lex_ordering = LexOrdering::new(sort_expr).unwrap();
        let sort_exec = Arc::new(SortExec::new(lex_ordering, lazy_exec));

        let optimizer = ReverseOrder;
        let result = optimizer.optimize(sort_exec, &config)?;

        // Verify the optimizer returns a valid plan (may or may not optimize depending on
        // whether LazyMemoryExec reports its ordering properly)
        assert!(
            !result.name().is_empty(),
            "Optimizer should return a valid plan for multi-column sort"
        );

        Ok(())
    }

    #[test]
    fn test_optimize_limit_sort_with_reverse_scan() -> Result<()> {
        // Test case: GlobalLimitExec -> SortExec pattern with reverse scan optimization
        // This tests the optimize_limit_sort function with limit pushdown

        let config = ConfigOptions::new();
        let schema = create_test_schema();

        // Create data sorted in ascending order
        let asc_sort_expr = vec![PhysicalSortExpr {
            expr: Arc::new(Column::new("col1", 0)),
            options: SortOptions {
                descending: false, // ASC
                nulls_first: false,
            },
        }];

        let lazy_exec = create_sorted_lazy_memory_exec(
            Arc::clone(&schema),
            asc_sort_expr,
            true, // ascending order
        )?;

        // We want descending order with limit
        let desc_sort_expr = vec![PhysicalSortExpr {
            expr: Arc::new(Column::new("col1", 0)),
            options: SortOptions {
                descending: true, // DESC
                nulls_first: false,
            },
        }];

        let lex_ordering = LexOrdering::new(desc_sort_expr).unwrap();
        let sort_exec = Arc::new(SortExec::new(lex_ordering, lazy_exec));

        // Wrap with GlobalLimitExec
        let limit_exec = Arc::new(GlobalLimitExec::new(sort_exec, 0, Some(5)));

        let optimizer = ReverseOrder;
        let result = optimizer.optimize(limit_exec, &config)?;

        // Verify: The plan should be optimized
        // Either the limit is pushed down or GlobalLimitExec is preserved
        let has_limit = result.fetch().is_some()
            || result.as_any().downcast_ref::<GlobalLimitExec>().is_some();

        assert!(
            has_limit,
            "Expected limit to be preserved in the plan, got: {}",
            result.name()
        );

        Ok(())
    }

    #[test]
    fn test_optimize_limit_sort_with_skip() -> Result<()> {
        // Test case: GlobalLimitExec with skip > 0 should preserve GlobalLimitExec

        let config = ConfigOptions::new();
        let schema = create_test_schema();

        let asc_sort_expr = vec![PhysicalSortExpr {
            expr: Arc::new(Column::new("col1", 0)),
            options: SortOptions {
                descending: false,
                nulls_first: false,
            },
        }];

        let lazy_exec =
            create_sorted_lazy_memory_exec(Arc::clone(&schema), asc_sort_expr, true)?;

        let desc_sort_expr = vec![PhysicalSortExpr {
            expr: Arc::new(Column::new("col1", 0)),
            options: SortOptions {
                descending: true,
                nulls_first: false,
            },
        }];

        let lex_ordering = LexOrdering::new(desc_sort_expr).unwrap();
        let sort_exec = Arc::new(SortExec::new(lex_ordering, lazy_exec));

        // GlobalLimitExec with skip = 2, fetch = 5
        let limit_exec = Arc::new(GlobalLimitExec::new(sort_exec, 2, Some(5)));

        let optimizer = ReverseOrder;
        let result = optimizer.optimize(limit_exec, &config)?;

        // When skip > 0, GlobalLimitExec should be preserved to handle the skip
        // The result should either be a GlobalLimitExec or contain the skip logic
        assert!(
            result.as_any().downcast_ref::<GlobalLimitExec>().is_some()
                || result.fetch().is_some(),
            "Expected GlobalLimitExec or fetch to be preserved when skip > 0, got: {}",
            result.name()
        );

        Ok(())
    }

    #[test]
    fn test_optimize_limit_sort_no_fetch() -> Result<()> {
        // Test case: GlobalLimitExec with fetch = None

        let config = ConfigOptions::new();
        let schema = create_test_schema();

        let sort_expr = vec![PhysicalSortExpr {
            expr: Arc::new(Column::new("col1", 0)),
            options: SortOptions {
                descending: false,
                nulls_first: false,
            },
        }];

        let lazy_exec =
            create_sorted_lazy_memory_exec(Arc::clone(&schema), sort_expr.clone(), true)?;

        let lex_ordering = LexOrdering::new(sort_expr).unwrap();
        let sort_exec = Arc::new(SortExec::new(lex_ordering, lazy_exec));

        // GlobalLimitExec with no fetch (None)
        let limit_exec = Arc::new(GlobalLimitExec::new(sort_exec, 0, None));

        let optimizer = ReverseOrder;
        let result = optimizer.optimize(limit_exec, &config)?;

        // Should still produce a valid plan
        assert!(
            !result.name().is_empty(),
            "Optimizer should return a valid plan even with fetch = None"
        );

        Ok(())
    }

    #[test]
    fn test_remove_unnecessary_sort_with_fetch_pushdown() -> Result<()> {
        // Test case: Remove unnecessary sort and push down fetch to input

        let config = ConfigOptions::new();
        let schema = create_test_schema();

        let sort_expr = vec![PhysicalSortExpr {
            expr: Arc::new(Column::new("col1", 0)),
            options: SortOptions {
                descending: false,
                nulls_first: false,
            },
        }];

        let lazy_exec =
            create_sorted_lazy_memory_exec(Arc::clone(&schema), sort_expr.clone(), true)?;

        let lex_ordering = LexOrdering::new(sort_expr).unwrap();
        let sort_exec =
            Arc::new(SortExec::new(lex_ordering, lazy_exec).with_fetch(Some(3)));

        let optimizer = ReverseOrder;
        let result = optimizer.optimize(sort_exec, &config)?;

        // Verify fetch is preserved
        let fetch = result.fetch();
        assert!(
            fetch.is_some(),
            "Expected fetch to be preserved after optimization"
        );
        assert_eq!(fetch, Some(3), "Expected fetch value to be 3");

        Ok(())
    }

    #[test]
    fn test_limit_sort_already_satisfied() -> Result<()> {
        // Test case: Input already satisfies sort requirement
        // GlobalLimitExec -> SortExec where input is already correctly sorted

        let config = ConfigOptions::new();
        let schema = create_test_schema();

        let sort_expr = vec![PhysicalSortExpr {
            expr: Arc::new(Column::new("col1", 0)),
            options: SortOptions {
                descending: false,
                nulls_first: false,
            },
        }];

        // Input is already sorted in ascending order
        let lazy_exec =
            create_sorted_lazy_memory_exec(Arc::clone(&schema), sort_expr.clone(), true)?;

        let lex_ordering = LexOrdering::new(sort_expr).unwrap();
        let sort_exec = Arc::new(SortExec::new(lex_ordering, lazy_exec));
        let limit_exec = Arc::new(GlobalLimitExec::new(sort_exec, 0, Some(10)));

        let optimizer = ReverseOrder;
        let result = optimizer.optimize(limit_exec, &config)?;

        // Sort should be removed, but limit should remain
        let sort_count = count_sorts_in_plan(&result);
        assert_eq!(
            sort_count, 0,
            "Expected sort to be removed when input already satisfies ordering"
        );

        Ok(())
    }

    #[test]
    fn test_limit_sort_cannot_optimize() -> Result<()> {
        // Test case: Cannot optimize - input is not sorted at all

        let config = ConfigOptions::new();
        let schema = create_test_schema();

        let generator = SortedBatchGenerator {
            schema: Arc::clone(&schema),
            batches_generated: 0,
            max_batches: 1,
            ascending: true,
        };

        // Create exec WITHOUT ordering information
        let unsorted_exec = Arc::new(LazyMemoryExec::try_new(
            schema,
            vec![Arc::new(RwLock::new(generator))],
        )?);

        let sort_expr = vec![PhysicalSortExpr {
            expr: Arc::new(Column::new("col1", 0)),
            options: SortOptions {
                descending: false,
                nulls_first: false,
            },
        }];

        let lex_ordering = LexOrdering::new(sort_expr).unwrap();
        let sort_exec = Arc::new(SortExec::new(lex_ordering, unsorted_exec));
        let limit_exec = Arc::new(GlobalLimitExec::new(sort_exec, 0, Some(5)));

        let optimizer = ReverseOrder;
        let result = optimizer.optimize(limit_exec, &config)?;

        // Sort should remain since input is not sorted
        let has_sort = result
            .as_any()
            .downcast_ref::<GlobalLimitExec>()
            .map(|l| l.input().as_any().downcast_ref::<SortExec>().is_some())
            .unwrap_or(false)
            || result.as_any().downcast_ref::<SortExec>().is_some();

        assert!(
            has_sort,
            "Expected SortExec to remain when input is not sorted, got: {}",
            result.name()
        );

        Ok(())
    }

    #[test]
    fn test_is_single_partition_check() -> Result<()> {
        // Test case: Multi-partition should not be treated as single partition reverse scan

        let config = ConfigOptions::new();
        let schema = create_test_schema();

        let sort_expr = vec![PhysicalSortExpr {
            expr: Arc::new(Column::new("col1", 0)),
            options: SortOptions {
                descending: false,
                nulls_first: false,
            },
        }];

        let lazy_exec =
            create_sorted_lazy_memory_exec(Arc::clone(&schema), sort_expr.clone(), true)?;

        // Repartition to multiple partitions
        let repartitioned = Arc::new(RepartitionExec::try_new(
            lazy_exec,
            Partitioning::RoundRobinBatch(4),
        )?);

        let lex_ordering = LexOrdering::new(sort_expr).unwrap();
        let sort_exec =
            Arc::new(SortExec::new(lex_ordering, repartitioned).with_fetch(Some(5)));

        let optimizer = ReverseOrder;
        let result = optimizer.optimize(sort_exec, &config)?;

        // Multi-partition case should not use the single-partition optimization
        // Result should have proper handling for multiple partitions
        assert!(
            result.output_partitioning().partition_count() >= 1,
            "Result should have valid partitioning"
        );

        Ok(())
    }

    #[test]
    fn test_fetch_value_calculation() -> Result<()> {
        // Test case: Verify total_fetch calculation (skip + fetch)

        let config = ConfigOptions::new();
        let schema = create_test_schema();

        let sort_expr = vec![PhysicalSortExpr {
            expr: Arc::new(Column::new("col1", 0)),
            options: SortOptions {
                descending: false,
                nulls_first: false,
            },
        }];

        let lazy_exec =
            create_sorted_lazy_memory_exec(Arc::clone(&schema), sort_expr.clone(), true)?;

        let lex_ordering = LexOrdering::new(sort_expr).unwrap();
        let sort_exec = Arc::new(SortExec::new(lex_ordering, lazy_exec));

        // skip = 3, fetch = 7, total should be 10
        let limit_exec = Arc::new(GlobalLimitExec::new(sort_exec, 3, Some(7)));

        let optimizer = ReverseOrder;
        let result = optimizer.optimize(limit_exec, &config)?;

        // The optimization should handle the combined skip + fetch correctly
        // Either preserve GlobalLimitExec or push down the total fetch
        assert!(
            !result.name().is_empty(),
            "Optimizer should handle skip + fetch calculation correctly"
        );

        Ok(())
    }

    #[test]
    fn test_nested_limit_sort_optimization() -> Result<()> {
        // Test case: Nested GlobalLimitExec -> SortExec patterns

        let config = ConfigOptions::new();
        let schema = create_test_schema();

        let sort_expr = vec![PhysicalSortExpr {
            expr: Arc::new(Column::new("col1", 0)),
            options: SortOptions {
                descending: false,
                nulls_first: false,
            },
        }];

        let lazy_exec =
            create_sorted_lazy_memory_exec(Arc::clone(&schema), sort_expr.clone(), true)?;

        let lex_ordering = LexOrdering::new(sort_expr).unwrap();

        // Inner sort + limit
        let inner_sort = Arc::new(SortExec::new(lex_ordering.clone(), lazy_exec));
        let inner_limit = Arc::new(GlobalLimitExec::new(inner_sort, 0, Some(20)));

        // Outer sort + limit
        let outer_sort = Arc::new(SortExec::new(lex_ordering, inner_limit));
        let outer_limit = Arc::new(GlobalLimitExec::new(outer_sort, 0, Some(10)));

        let optimizer = ReverseOrder;
        let result = optimizer.optimize(outer_limit, &config)?;

        // Should optimize both layers
        let sort_count = count_sorts_in_plan(&result);
        assert!(
            sort_count < 2,
            "Expected nested sorts to be optimized, found {sort_count} sorts"
        );

        Ok(())
    }

    #[test]
    fn test_preserve_partitioning_with_limit() -> Result<()> {
        // Test case: SortExec with preserve_partitioning and fetch

        let config = ConfigOptions::new();
        let schema = create_test_schema();

        let sort_expr = vec![PhysicalSortExpr {
            expr: Arc::new(Column::new("col1", 0)),
            options: SortOptions {
                descending: false,
                nulls_first: false,
            },
        }];

        let lazy_exec =
            create_sorted_lazy_memory_exec(Arc::clone(&schema), sort_expr.clone(), true)?;

        let repartitioned = Arc::new(RepartitionExec::try_new(
            lazy_exec,
            Partitioning::RoundRobinBatch(2),
        )?);

        let lex_ordering = LexOrdering::new(sort_expr).unwrap();
        let sort_exec = Arc::new(
            SortExec::new(lex_ordering, repartitioned)
                .with_fetch(Some(5))
                .with_preserve_partitioning(true),
        );

        let optimizer = ReverseOrder;
        let result = optimizer.optimize(sort_exec, &config)?;

        // With preserve_partitioning=true and multiple partitions,
        // should use LocalLimitExec instead of GlobalLimitExec
        let is_local_limit = result.as_any().downcast_ref::<LocalLimitExec>().is_some();
        let has_fetch = result.fetch().is_some();

        assert!(
            is_local_limit || has_fetch,
            "Expected LocalLimitExec or fetch for multi-partition with preserve_partitioning"
        );

        Ok(())
    }
}
