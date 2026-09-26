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

//! Per-conjunct pruning statistics for [`PruningPredicate`].
//!
//! See [`PruningPredicateBuilder::with_conjunct_stats`] and
//! [`PruningPredicate::prune_with_conjunct_stats`].

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use datafusion_common::Result;
use datafusion_common::pruning::PruningStatistics;
use datafusion_physical_expr::simplifier::PhysicalExprSimplifier;
use datafusion_physical_expr::utils::LiteralGuarantee;
use datafusion_physical_plan::PhysicalExpr;

#[cfg(doc)]
use super::PruningPredicateBuilder;
use super::{
    BoolVecBuilder, PruningExpressionProperties, PruningPredicate, RequiredColumns,
    UnhandledPredicateHook, build_predicate_expression, build_statistics_record_batch,
    is_always_true,
};

/// How many containers one conjunct of a [`PruningPredicate`] prunes when it
/// is evaluated alone.
///
/// Returned by [`PruningPredicate::prune_with_conjunct_stats`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ConjunctPruningStats {
    /// Number of containers that this conjunct alone proves can not contain
    /// matching rows.
    pub containers_pruned: usize,
    /// Number of containers that this conjunct alone can not rule out.
    pub containers_kept: usize,
}

/// The pruning data of each top-level conjunct of a [`PruningPredicate`].
#[derive(Debug, Clone)]
pub(super) struct PruningConjuncts {
    /// The statistics that the whole predicate and all conjuncts need.
    ///
    /// The first entries are the `required_columns` of the whole predicate,
    /// in the same order. Thus the `predicate_expr` of the whole predicate
    /// evaluates against a statistics batch built from this list.
    required_columns: RequiredColumns,
    conjuncts: Vec<PruningConjunct>,
}

/// A conjunct rewritten in terms of statistics.
#[derive(Debug, Clone)]
struct PruningConjunct {
    /// Refers to columns of `PruningConjuncts::required_columns`.
    predicate_expr: Arc<dyn PhysicalExpr>,
    literal_guarantees: Vec<LiteralGuarantee>,
}

impl PruningConjuncts {
    /// Rewrites each (already snapshotted) conjunct in terms of statistics.
    ///
    /// `required_columns` are the statistics of the whole predicate.
    pub(super) fn try_new(
        conjuncts: &[Arc<dyn PhysicalExpr>],
        file_schema: &arrow::datatypes::SchemaRef,
        required_columns: &RequiredColumns,
        unhandled_hook: &Arc<dyn UnhandledPredicateHook>,
        max_in_list_size: usize,
    ) -> Result<Self> {
        // Start from the columns of the whole predicate, so that one statistics
        // batch serves the whole predicate and every conjunct.
        let mut required_columns = required_columns.clone();
        let rewritten = conjuncts
            .iter()
            .map(|conjunct| {
                let predicate_expr = build_predicate_expression(
                    conjunct,
                    file_schema,
                    &mut required_columns,
                    unhandled_hook,
                    max_in_list_size,
                    &mut PruningExpressionProperties::default(),
                );
                (predicate_expr, LiteralGuarantee::analyze(conjunct))
            })
            .collect::<Vec<_>>();

        // Simplify after all statistics columns are known.
        let predicate_schema = required_columns.schema();
        let simplifier = PhysicalExprSimplifier::new(&predicate_schema);
        let conjuncts = rewritten
            .into_iter()
            .map(|(predicate_expr, literal_guarantees)| {
                Ok(PruningConjunct {
                    predicate_expr: simplifier.simplify(predicate_expr)?,
                    literal_guarantees,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Self {
            required_columns,
            conjuncts,
        })
    }

    /// See [`PruningPredicate::prune_with_conjunct_stats`].
    pub(super) fn prune<S: PruningStatistics + ?Sized>(
        &self,
        predicate: &PruningPredicate,
        statistics: &S,
    ) -> Result<(Vec<bool>, Vec<ConjunctPruningStats>)> {
        let num_containers = statistics.num_containers();
        let mut batch = LazyStatisticsBatch {
            statistics,
            required_columns: &self.required_columns,
            batch: None,
        };

        // The result of the whole predicate. This follows the same steps as
        // `PruningPredicate::prune`, so the result is the same.
        let mut builder = BoolVecBuilder::new(num_containers);
        if !builder
            .combine_literal_guarantees(&predicate.literal_guarantees, statistics)?
        {
            builder.combine_value(predicate.predicate_expr.evaluate(batch.get()?)?);
        }
        let result = builder.build();

        // Evaluate each conjunct on all containers. There is no short circuit,
        // so the stats of a conjunct do not depend on the other conjuncts or
        // on their order.
        let stats = self
            .conjuncts
            .iter()
            .map(|conjunct| {
                let mut builder = BoolVecBuilder::new(num_containers);
                if !builder.combine_literal_guarantees(
                    &conjunct.literal_guarantees,
                    statistics,
                )? && !is_always_true(&conjunct.predicate_expr)
                {
                    builder
                        .combine_value(conjunct.predicate_expr.evaluate(batch.get()?)?);
                }
                let containers_kept = builder.build().into_iter().filter(|k| *k).count();
                Ok(ConjunctPruningStats {
                    containers_pruned: num_containers - containers_kept,
                    containers_kept,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        Ok((result, stats))
    }
}

/// A statistics [`RecordBatch`] that is built on first use.
struct LazyStatisticsBatch<'a, S: ?Sized> {
    statistics: &'a S,
    required_columns: &'a RequiredColumns,
    batch: Option<RecordBatch>,
}

impl<S: PruningStatistics + ?Sized> LazyStatisticsBatch<'_, S> {
    fn get(&mut self) -> Result<&RecordBatch> {
        Ok(match &mut self.batch {
            Some(batch) => batch,
            slot @ None => slot.insert(build_statistics_record_batch(
                self.statistics,
                self.required_columns,
            )?),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use arrow::array::{ArrayRef, BooleanArray, Int32Array};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion_common::{Column, ScalarValue};
    use datafusion_expr::{Expr, col, lit};
    use datafusion_physical_expr::expressions::DynamicFilterPhysicalExpr;
    use datafusion_physical_expr::planner::logical2physical;
    use datafusion_physical_expr::utils::collect_columns;

    use super::*;
    use crate::PruningPredicateBuilder;

    /// Int32 min/max statistics and `contained` results per column.
    #[derive(Default)]
    struct TestStats {
        num_containers: usize,
        min_max: HashMap<String, (ArrayRef, ArrayRef)>,
        /// Returned by `contained` for any set of values
        contained: HashMap<String, BooleanArray>,
    }

    impl TestStats {
        fn new(num_containers: usize) -> Self {
            Self {
                num_containers,
                ..Default::default()
            }
        }

        fn with_min_max(mut self, column: &str, min: &[i32], max: &[i32]) -> Self {
            assert_eq!(min.len(), self.num_containers);
            assert_eq!(max.len(), self.num_containers);
            let min = Arc::new(Int32Array::from(min.to_vec())) as ArrayRef;
            let max = Arc::new(Int32Array::from(max.to_vec())) as ArrayRef;
            self.min_max.insert(column.to_string(), (min, max));
            self
        }

        fn with_contained(mut self, column: &str, contained: &[bool]) -> Self {
            assert_eq!(contained.len(), self.num_containers);
            self.contained
                .insert(column.to_string(), BooleanArray::from(contained.to_vec()));
            self
        }
    }

    impl PruningStatistics for TestStats {
        fn min_values(&self, column: &Column) -> Option<ArrayRef> {
            self.min_max
                .get(column.name())
                .map(|(min, _)| Arc::clone(min))
        }

        fn max_values(&self, column: &Column) -> Option<ArrayRef> {
            self.min_max
                .get(column.name())
                .map(|(_, max)| Arc::clone(max))
        }

        fn num_containers(&self) -> usize {
            self.num_containers
        }

        fn null_counts(&self, _column: &Column) -> Option<ArrayRef> {
            None
        }

        fn row_counts(&self) -> Option<ArrayRef> {
            None
        }

        fn contained(
            &self,
            column: &Column,
            _values: &HashSet<ScalarValue>,
        ) -> Option<BooleanArray> {
            self.contained.get(column.name()).cloned()
        }
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
        ]))
    }

    /// Three containers:
    ///
    /// | container | a          | b        |
    /// |-----------|------------|----------|
    /// | 0         | 0..=3      | 0..=1    |
    /// | 1         | 6..=10     | 20..=30  |
    /// | 2         | 200..=300  | 0..=1    |
    fn stats() -> TestStats {
        TestStats::new(3)
            .with_min_max("a", &[0, 6, 200], &[3, 10, 300])
            .with_min_max("b", &[0, 20, 0], &[1, 30, 1])
    }

    fn stats_of(pruned: usize, kept: usize) -> ConjunctPruningStats {
        ConjunctPruningStats {
            containers_pruned: pruned,
            containers_kept: kept,
        }
    }

    fn physical(expr: &Expr) -> Arc<dyn PhysicalExpr> {
        logical2physical(expr, &schema())
    }

    /// Prunes with per-conjunct stats, and checks that the pruning result is
    /// the same as the result of a predicate built without conjunct stats.
    fn prune_with_conjunct_stats(
        predicate: Arc<dyn PhysicalExpr>,
        statistics: &TestStats,
    ) -> (Vec<bool>, Vec<ConjunctPruningStats>) {
        let plain = PruningPredicateBuilder::new()
            .with_file_schema(schema())
            .try_build(Arc::clone(&predicate))
            .unwrap();
        let with_stats = PruningPredicateBuilder::new()
            .with_file_schema(schema())
            .with_conjunct_stats(true)
            .try_build(predicate)
            .unwrap();

        let (result, conjunct_stats) =
            with_stats.prune_with_conjunct_stats(statistics).unwrap();
        let expected = plain.prune(statistics).unwrap();
        assert_eq!(
            result, expected,
            "result differs from PruningPredicate::prune"
        );
        assert_eq!(with_stats.prune(statistics).unwrap(), expected);
        for stats in &conjunct_stats {
            assert_eq!(
                stats.containers_pruned + stats.containers_kept,
                statistics.num_containers()
            );
        }
        (result, conjunct_stats)
    }

    #[test]
    fn and_of_conjuncts() {
        // Each conjunct prunes a different container.
        let predicate = col("a")
            .gt(lit(5))
            .and(col("b").lt(lit(10)))
            .and(col("a").lt(lit(100)));
        let (result, conjunct_stats) =
            prune_with_conjunct_stats(physical(&predicate), &stats());
        assert_eq!(result, vec![false, false, false]);
        assert_eq!(
            conjunct_stats,
            vec![stats_of(1, 2), stats_of(1, 2), stats_of(1, 2)]
        );
    }

    #[test]
    fn stats_do_not_depend_on_conjunct_order() {
        // `a > 1000` prunes all containers. The other conjuncts are still
        // evaluated on all containers.
        let predicate = col("a")
            .gt(lit(1000))
            .and(col("b").lt(lit(10)))
            .and(col("a").gt(lit(5)));
        let (result, conjunct_stats) =
            prune_with_conjunct_stats(physical(&predicate), &stats());
        assert_eq!(result, vec![false, false, false]);
        assert_eq!(
            conjunct_stats,
            vec![stats_of(3, 0), stats_of(1, 2), stats_of(1, 2)]
        );
    }

    #[test]
    fn single_conjunct() {
        let (result, conjunct_stats) =
            prune_with_conjunct_stats(physical(&col("a").gt(lit(5))), &stats());
        assert_eq!(result, vec![false, true, true]);
        assert_eq!(conjunct_stats, vec![stats_of(1, 2)]);
    }

    #[test]
    fn conjunct_that_can_not_be_rewritten_keeps_all_containers() {
        // `a + b = 3` refers to two columns, so statistics can not prune it.
        let predicate = (col("a") + col("b")).eq(lit(3)).and(col("a").gt(lit(5)));
        let (result, conjunct_stats) =
            prune_with_conjunct_stats(physical(&predicate), &stats());
        assert_eq!(result, vec![false, true, true]);
        assert_eq!(conjunct_stats, vec![stats_of(0, 3), stats_of(1, 2)]);
    }

    #[test]
    fn or_and_not_are_one_conjunct() {
        // The `OR` is not split, and the `AND` inside the `NOT` is not split.
        let predicate =
            col("a")
                .lt(lit(3))
                .or(col("a").gt(lit(250)))
                .and(Expr::Not(Box::new(
                    col("a").eq(lit(1)).and(col("b").eq(lit(1))),
                )));
        let (result, conjunct_stats) =
            prune_with_conjunct_stats(physical(&predicate), &stats());
        assert_eq!(result, vec![true, false, true]);
        assert_eq!(conjunct_stats, vec![stats_of(1, 2), stats_of(0, 3)]);
    }

    #[test]
    fn dynamic_filter_is_one_conjunct() {
        // The dynamic filter contains an `AND`, but it is one conjunct.
        let inner = physical(&col("a").gt(lit(5)).and(col("b").lt(lit(10))));
        let children = collect_columns(&inner)
            .into_iter()
            .map(|c| Arc::new(c) as Arc<dyn PhysicalExpr>)
            .collect();
        let dynamic_filter = Arc::new(DynamicFilterPhysicalExpr::new(children, inner))
            as Arc<dyn PhysicalExpr>;
        let predicate = datafusion_physical_expr::conjunction([
            physical(&col("a").lt(lit(100))),
            dynamic_filter,
        ]);

        let (result, conjunct_stats) = prune_with_conjunct_stats(predicate, &stats());
        assert_eq!(result, vec![false, false, false]);
        // Container 0 fails `a > 5` and container 1 fails `b < 10`.
        assert_eq!(conjunct_stats, vec![stats_of(1, 2), stats_of(2, 1)]);
    }

    #[test]
    fn literal_guarantees_are_attributed_per_conjunct() {
        // No min/max statistics, only `contained` (for example Bloom filters).
        let statistics = TestStats::new(3)
            .with_contained("a", &[false, true, true])
            .with_contained("b", &[true, true, false]);
        let predicate = col("a")
            .in_list(vec![lit(7), lit(8)], false)
            .and(col("b").eq(lit(1)));
        let (result, conjunct_stats) =
            prune_with_conjunct_stats(physical(&predicate), &statistics);
        assert_eq!(result, vec![false, true, false]);
        assert_eq!(conjunct_stats, vec![stats_of(1, 2), stats_of(1, 2)]);
    }

    #[test]
    fn error_without_conjunct_stats() {
        let predicate = PruningPredicateBuilder::new()
            .with_file_schema(schema())
            .try_build(physical(&col("a").gt(lit(5))))
            .unwrap();
        let err = predicate.prune_with_conjunct_stats(&stats()).unwrap_err();
        assert!(
            err.to_string().contains("with_conjunct_stats"),
            "unexpected error: {err}"
        );
    }
}
