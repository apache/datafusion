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
