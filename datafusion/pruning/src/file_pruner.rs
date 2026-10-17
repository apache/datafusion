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

//! File-level pruning based on partition values and file-level statistics

use std::sync::Arc;

use arrow::datatypes::{FieldRef, SchemaRef};
use datafusion_common::{Result, internal_datafusion_err, pruning::PrunableStatistics};
use datafusion_datasource::PartitionedFile;
use datafusion_physical_expr::DynamicFilterTracking;
use datafusion_physical_expr_common::physical_expr::{PhysicalExpr, snapshot_generation};
use datafusion_physical_plan::metrics::Count;
use log::debug;
use parking_lot::Mutex;

use crate::{PruningPredicate, build_pruning_predicate};

/// Prune based on file-level statistics.
///
/// Note: Partition column pruning is handled earlier via `replace_columns_with_literals`
/// which substitutes partition column references with their literal values before
/// the predicate reaches this pruner.
pub struct FilePruner {
    predicate: Arc<dyn PhysicalExpr>,
    /// Tracks the dynamic filters inside `predicate` so we only rebuild the
    /// pruning predicate when one of them has actually moved.
    tracking: DynamicFilterTracking,
    /// Whether [`Self::should_prune`] has built+evaluated the pruning predicate
    /// at least once. The first check always runs; subsequent checks only run
    /// when a watched dynamic filter changed.
    checked_once: bool,
    /// Schema used for pruning (the logical file schema).
    file_schema: SchemaRef,
    file_stats_pruning: PrunableStatistics,
    predicate_creation_errors: Count,
    /// The last pruning predicate built, which [`Self::with_cache`] shares
    /// with other files' pruners.
    cache: PruningPredicateCache,
}

/// Shares the [`PruningPredicate`]s that [`FilePruner`]s build between the
/// files of a scan.
///
/// Building a pruning predicate (snapshotting the dynamic filters in the
/// predicate, simplifying it and rewriting it in terms of statistics) costs
/// much more than evaluating it against one file's statistics. Files with the
/// same partition values have the same predicate once those are replaced with
/// literals, and so the same pruning predicate for as long as the dynamic
/// filters in it don't change. Without sharing, a scan of many files under a
/// `TopK`, which checks each file when it opens it and again whenever the
/// `TopK`'s filter tightens, builds it again for every file.
///
/// This keeps the last pruning predicate built, with the predicate it was built
/// from and the generation of that predicate's dynamic filters at the time. A
/// [`FilePruner`] given the cache (see [`FilePruner::with_cache`]) reuses it
/// while both match. One is enough when files with the same partition values
/// are next to each other in the scan, as they usually are, and keeps the
/// cache's size bounded. Clones share the cache.
#[derive(Debug, Clone, Default)]
pub struct PruningPredicateCache {
    last: Arc<Mutex<Option<CachedPruningPredicate>>>,
}

#[derive(Debug)]
struct CachedPruningPredicate {
    predicate: Arc<dyn PhysicalExpr>,
    file_schema: SchemaRef,
    generation: u64,
    pruning_predicate: Option<Arc<PruningPredicate>>,
}

impl PruningPredicateCache {
    /// The pruning predicate of `predicate` for `file_schema`, at the current
    /// generation of the predicate's dynamic filters: the cached one if it was
    /// built from the same, else the one `build` returns, which replaces it.
    fn get_or_build(
        &self,
        predicate: &Arc<dyn PhysicalExpr>,
        file_schema: &SchemaRef,
        build: impl FnOnce() -> Option<Arc<PruningPredicate>>,
    ) -> Option<Arc<PruningPredicate>> {
        // Read before `build` snapshots the dynamic filters, so a predicate
        // kept for this generation was built from it or a later one. A later
        // generation of a dynamic filter is as correct to prune with.
        let generation = snapshot_generation(predicate);
        let mut last = self.last.lock();
        if let Some(last) = last.as_ref()
            && last.generation == generation
            && last.file_schema == *file_schema
            && last.predicate.as_ref() == predicate.as_ref()
        {
            return last.pruning_predicate.clone();
        }
        let pruning_predicate = build();
        *last = Some(CachedPruningPredicate {
            predicate: Arc::clone(predicate),
            file_schema: Arc::clone(file_schema),
            generation,
            pruning_predicate: pruning_predicate.clone(),
        });
        pruning_predicate
    }
}

impl FilePruner {
    #[deprecated(
        since = "52.0.0",
        note = "Use `try_new` instead which returns None if no statistics are available"
    )]
    #[expect(clippy::needless_pass_by_value)]
    pub fn new(
        predicate: Arc<dyn PhysicalExpr>,
        logical_file_schema: &SchemaRef,
        _partition_fields: Vec<FieldRef>,
        partitioned_file: PartitionedFile,
        predicate_creation_errors: Count,
    ) -> Result<Self> {
        Self::try_new(
            predicate,
            logical_file_schema,
            &partitioned_file,
            predicate_creation_errors,
        )
        .ok_or_else(|| {
            internal_datafusion_err!(
                "FilePruner::new called on a file without statistics: {:?}",
                partitioned_file
            )
        })
    }

    /// Create a file pruner for this file, or `None` when pruning it cannot
    /// help.
    ///
    /// Returns `None` when the file has no statistics struct to evaluate a
    /// pruning predicate against, or when the predicate is purely static and the
    /// file has no usable column statistics — in that case planning already did
    /// everything such a pruner could. A predicate carrying a dynamic filter is
    /// always accepted (given a statistics struct), since it may prune via
    /// partition-value folding even without column statistics.
    pub fn try_new(
        predicate: Arc<dyn PhysicalExpr>,
        file_schema: &SchemaRef,
        partitioned_file: &PartitionedFile,
        predicate_creation_errors: Count,
    ) -> Option<Self> {
        // A pruning predicate is evaluated against a statistics struct, so one
        // must exist (its columns may all be `Absent`).
        let file_stats = partitioned_file.statistics.as_ref()?;
        let tracking = DynamicFilterTracking::classify(&predicate);
        // Only build a pruner when it could prune something planning didn't
        // already: the file has real column statistics, or the predicate carries
        // a dynamic filter (whose value, or folded partition columns, can prune
        // even without column statistics). For a purely static predicate with no
        // usable stats there is nothing to gain.
        if !partitioned_file.has_statistics() && !tracking.contains_dynamic_filter() {
            return None;
        }
        let file_stats_pruning =
            PrunableStatistics::new(vec![file_stats.clone()], Arc::clone(file_schema));
        Some(Self {
            predicate,
            tracking,
            checked_once: false,
            file_schema: Arc::clone(file_schema),
            file_stats_pruning,
            predicate_creation_errors,
            cache: PruningPredicateCache::default(),
        })
    }

    /// Share the pruning predicates this pruner builds with the other pruners
    /// given `cache`, such as those of the other files of the same scan.
    pub fn with_cache(mut self, cache: PruningPredicateCache) -> Self {
        self.cache = cache;
        self
    }

    /// Returns `true` if this pruner watches a dynamic filter that can still
    /// change, meaning [`Self::should_prune`] is worth re-checking as the scan
    /// progresses. When `false`, the predicate is effectively static for the
    /// remainder of the scan and the caller can avoid wrapping the stream in a
    /// per-batch re-pruning adapter.
    pub fn is_watching(&self) -> bool {
        matches!(self.tracking, DynamicFilterTracking::Watching(_))
    }

    pub fn should_prune(&mut self) -> Result<bool> {
        // Building the pruning predicate is expensive (it involves expression
        // analysis), so we only do it on the first check and whenever a dynamic
        // filter inside the predicate has actually moved, and then only if the
        // cache doesn't already hold it.
        //
        // Dynamic filter expressions can change their values during query
        // execution; `DynamicFilterTracking` watches the still-incomplete
        // filters and reports a change at most once per update. A purely static
        // predicate (or one whose dynamic filters have all completed) is checked
        // exactly once.
        let should_build = if self.checked_once {
            self.tracking.watcher().is_some_and(|w| w.changed())
        } else {
            self.checked_once = true;
            true
        };
        if !should_build {
            return Ok(false);
        }
        let pruning_predicate =
            self.cache
                .get_or_build(&self.predicate, &self.file_schema, || {
                    build_pruning_predicate(
                        Arc::clone(&self.predicate),
                        &self.file_schema,
                        &self.predicate_creation_errors,
                    )
                });
        let Some(pruning_predicate) = pruning_predicate else {
            return Ok(false);
        };
        match pruning_predicate.prune(&self.file_stats_pruning) {
            Ok(values) => {
                assert_eq!(values.len(), 1);
                // We expect a single container -> if all containers are false skip this file
                if values.into_iter().all(|v| !v) {
                    return Ok(true);
                }
            }
            // Stats filter array could not be built, so we can't prune
            Err(e) => {
                debug!("Ignoring error building pruning predicate for file: {e}");
                self.predicate_creation_errors.add(1);
            }
        }

        Ok(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::{
        ColumnStatistics, ScalarValue, Statistics, stats::Precision,
    };
    use datafusion_expr_common::operator::Operator;
    use datafusion_physical_expr::expressions::{
        BinaryExpr, DynamicFilterPhysicalExpr, col, lit,
    };

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]))
    }

    /// A file whose column `a` holds values from `min` to `max`.
    fn file(min: i32, max: i32) -> PartitionedFile {
        let statistics = Statistics {
            num_rows: Precision::Absent,
            total_byte_size: Precision::Absent,
            column_statistics: vec![
                ColumnStatistics::new_unknown()
                    .with_min_value(Precision::Exact(ScalarValue::Int32(Some(min))))
                    .with_max_value(Precision::Exact(ScalarValue::Int32(Some(max))))
                    .with_null_count(Precision::Exact(0)),
            ],
        };
        PartitionedFile::new(format!("{min}-{max}.parquet"), 1_000)
            .with_statistics(Arc::new(statistics))
    }

    /// `a > threshold`
    fn greater_than(schema: &Schema, threshold: i32) -> Arc<dyn PhysicalExpr> {
        Arc::new(BinaryExpr::new(
            col("a", schema).unwrap(),
            Operator::Gt,
            lit(threshold),
        ))
    }

    fn pruner(
        predicate: &Arc<dyn PhysicalExpr>,
        schema: &SchemaRef,
        file: &PartitionedFile,
        cache: &PruningPredicateCache,
    ) -> FilePruner {
        FilePruner::try_new(Arc::clone(predicate), schema, file, Count::new())
            .expect("the file has statistics")
            .with_cache(cache.clone())
    }

    /// The pruning predicate `cache` holds.
    fn cached(cache: &PruningPredicateCache) -> Arc<PruningPredicate> {
        let last = cache.last.lock();
        let last = last.as_ref().expect("a pruning predicate was built");
        Arc::clone(last.pruning_predicate.as_ref().expect("it can prune"))
    }

    #[test]
    fn files_with_the_same_predicate_share_its_pruning_predicate() {
        let schema = schema();
        // As a `TopK` keeping the largest values of `a` publishes them.
        let filter = Arc::new(DynamicFilterPhysicalExpr::new(
            vec![col("a", &schema).unwrap()],
            lit(true),
        ));
        filter.update(greater_than(&schema, 10)).unwrap();
        let predicate = Arc::clone(&filter) as Arc<dyn PhysicalExpr>;
        let cache = PruningPredicateCache::default();

        let mut low = pruner(&predicate, &schema, &file(1, 9), &cache);
        assert!(low.should_prune().unwrap());
        let built = cached(&cache);
        let mut high = pruner(&predicate, &schema, &file(20, 30), &cache);
        assert!(!high.should_prune().unwrap());
        assert!(
            Arc::ptr_eq(&built, &cached(&cache)),
            "a file with the same predicate at the same generation builds nothing"
        );

        // The filter tightens: the open file's next check builds the
        // pruning predicate again, and the files after it use that one.
        filter.update(greater_than(&schema, 25)).unwrap();
        assert!(!high.should_prune().unwrap());
        let rebuilt = cached(&cache);
        assert!(!Arc::ptr_eq(&built, &rebuilt));
        let mut middle = pruner(&predicate, &schema, &file(20, 24), &cache);
        assert!(middle.should_prune().unwrap());
        assert!(Arc::ptr_eq(&rebuilt, &cached(&cache)));
    }

    #[test]
    fn files_with_different_predicates_build_their_own() {
        let schema = schema();
        let cache = PruningPredicateCache::default();
        let file = file(1, 9);
        // As the predicates of files with different partition values are,
        // once those replace the partition columns.
        let mut kept = pruner(&greater_than(&schema, 5), &schema, &file, &cache);
        assert!(!kept.should_prune().unwrap());
        let mut pruned = pruner(&greater_than(&schema, 15), &schema, &file, &cache);
        assert!(pruned.should_prune().unwrap());
        let mut kept = pruner(&greater_than(&schema, 5), &schema, &file, &cache);
        assert!(!kept.should_prune().unwrap());
    }
}
