// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Optimizer hooks for rewriting and specializing [`AggregateExec`].

use std::sync::Arc;

use arrow::compute::SortOptions;
use arrow::datatypes::DataType;
use arrow_schema::FieldRef;
use datafusion_common::Result;
use datafusion_common::tree_node::Transformed;
use datafusion_physical_expr::aggregate::AggregateFunctionExpr;
use datafusion_physical_expr::physical_exprs_equal;
use datafusion_physical_expr_common::sort_expr::OrderingRequirements;
use itertools::Itertools;

use super::topk::hash_table::is_supported_hash_key_type;
use super::topk::heap::is_supported_heap_type;
use super::{
    AggregateExec, AggregateKind, AggregateMode, GroupClusteringMode,
    aggregate_expressions,
};
use crate::execution_plan::EmissionType;
use crate::metrics::ExecutionPlanMetricsSet;
use crate::{ExecutionPlan, ExecutionPlanProperties};

// Hooks are grouped by the optimizer rules that use them.
impl AggregateExec {
    // ========================================================================
    // LimitedDistinctAggregation
    // ========================================================================
    /// Try to stop after enough distinct grouping keys have been accumulated.
    ///
    /// The caller must establish that discarding other groups is legal, for
    /// example beneath `SELECT DISTINCT k FROM t LIMIT 10`. The parent limit
    /// remains responsible for enforcing the exact number of rows.
    ///
    /// # Trigger conditions
    ///
    /// - Grouping with no aggregate expressions or aggregate filters.
    /// - Ordering eligible under [`Self::is_unordered_unfiltered_group_by_distinct`],
    ///   including no existing Top-K direction.
    /// - A positive limit that is tighter than any existing limit.
    ///
    /// # Consistency
    ///
    /// This is a safe, atomic optimization: it preserves the grouping keys and
    /// leaves the aggregate in a consistent state. Ineligible aggregates return
    /// `None`. Eligible aggregates are returned unchanged when the limit is zero
    /// or no tighter than the existing limit.
    ///
    /// # Public Only for Internal Use:
    ///
    /// This is not a public API and is for internal use only; see [API policy] for details.
    ///
    /// [API policy]: https://datafusion.apache.org/contributor-guide/api-health.html#internal-public-apis
    #[doc(hidden)]
    pub fn try_optimize_distinct_soft_limit(
        mut self,
        limit: usize,
    ) -> Option<Transformed<Self>> {
        if !self.is_unordered_unfiltered_group_by_distinct() {
            return None;
        }
        if limit == 0
            || self
                .limit_options()
                .is_some_and(|existing| existing.limit <= limit)
        {
            return Some(Transformed::no(self));
        }
        self.kind = AggregateKind::DistinctLimit {
            group_by: Arc::clone(self.group_by()),
            limit,
        };
        Some(Transformed::yes(self))
    }

    /// Returns whether this is a compatible partial/final aggregation pair.
    ///
    /// # Public Only for Internal Use:
    ///
    /// This is not a public API and is for internal use only; see [API policy] for details.
    ///
    /// [API policy]: https://datafusion.apache.org/contributor-guide/api-health.html#internal-public-apis
    #[doc(hidden)]
    pub fn matches_partial(&self, partial: &Self) -> bool {
        matches!(
            (self.mode, partial.mode),
            (
                AggregateMode::Final | AggregateMode::FinalPartitioned,
                AggregateMode::Partial,
            )
        ) && self.group_expr() == &partial.group_expr().as_final()
    }

    /// true, if this Aggregate has a group-by with no required or explicit ordering,
    /// no filtering and no aggregate expressions
    /// This method qualifies the use of the LimitedDistinctAggregation rewrite rule
    /// on an AggregateExec.
    ///
    /// # Public Only for Internal Use:
    ///
    /// This is not a public API and is for internal use only; see [API policy] for details.
    ///
    /// [API policy]: https://datafusion.apache.org/contributor-guide/api-health.html#internal-public-apis
    #[doc(hidden)]
    pub fn is_unordered_unfiltered_group_by_distinct(&self) -> bool {
        if self
            .limit_options()
            .and_then(|config| config.descending)
            .is_some()
        {
            return false;
        }
        // ensure there is a group by
        if self.group_expr().is_empty() && !self.group_expr().has_grouping_set() {
            return false;
        }
        // ensure there are no aggregate expressions
        if !self.aggr_expr().is_empty() {
            return false;
        }
        // ensure there are no filters on aggregate expressions; the above check
        // may preclude this case
        if self.filter_expr().iter().any(|e| e.is_some()) {
            return false;
        }
        // ensure there are no order by expressions
        if !self.aggr_expr().iter().all(|e| e.order_bys().is_empty()) {
            return false;
        }
        // ensure there is no output ordering; can this rule be relaxed?
        if self.properties().output_ordering().is_some() {
            return false;
        }
        // ensure no ordering is required on the input
        if let Some(requirement) = self.required_input_ordering().swap_remove(0) {
            return matches!(requirement, OrderingRequirements::Hard(_));
        }
        true
    }

    // ========================================================================
    // EnsureRequirements / SanityCheckPlan
    // ========================================================================
    /// Whether a candidate input keeps each group in one partition.
    ///
    /// Unlike co-partitioning, aggregation only needs equality groups to be
    /// local. A final stage can inherit that guarantee from a partial stage
    /// even when projecting the grouping expressions loses range metadata.
    /// This check does not require the two stages to be combined.
    ///
    /// # Public Only for Internal Use:
    ///
    /// This is not a public API and is for internal use only; see [API policy] for details.
    ///
    /// [API policy]: https://datafusion.apache.org/contributor-guide/api-health.html#internal-public-apis
    #[doc(hidden)]
    pub fn input_keeps_groups_local(
        &self,
        input: &dyn ExecutionPlan,
        allow_subset: bool,
    ) -> bool {
        if !matches!(
            self.mode,
            AggregateMode::Partial
                | AggregateMode::SinglePartitioned
                | AggregateMode::FinalPartitioned
        ) || self.group_by().is_empty()
            || self.group_by().has_grouping_set()
        {
            return false;
        }

        if input.output_partitioning().keeps_keys_local(
            &self.group_by().input_exprs(),
            input.equivalence_properties(),
            allow_subset,
        ) {
            return true;
        }

        self.mode == AggregateMode::FinalPartitioned
            && input
                .downcast_ref::<AggregateExec>()
                .is_some_and(|partial| {
                    partial.mode == AggregateMode::Partial
                        && physical_exprs_equal(
                            &partial.output_group_expr(),
                            &self.group_by().input_exprs(),
                        )
                        && partial.input_keeps_groups_local(
                            partial.input.as_ref(),
                            allow_subset,
                        )
                })
    }

    // ========================================================================
    // CombinePartialFinalAggregate
    // ========================================================================
    /// Combines adjacent partial and final aggregates in the given plan if they are
    /// compatible. See [`CombinePartialFinalAggregate`] for the invariants required
    /// by this transformation.
    ///
    /// If input plan is not aggregate, this function will short-circuit with
    /// `Transformed::No`.
    ///
    /// ```txt
    /// Before Plan:
    /// AggregateExec(mode=Final)          <-- current plan
    ///   AggregateExec(mode=Partial)
    ///     DataSourceExec
    ///
    /// After Plan:
    /// AggregateExec(mode=Single)         <-- current plan
    ///   DataSourceExec
    /// ```
    ///
    /// # Public Only for Internal Use:
    ///
    /// This is not a public API and is for internal use only; see [API policy] for details.
    ///
    /// [`CombinePartialFinalAggregate`]: https://docs.rs/datafusion/latest/datafusion/physical_optimizer/combine_partial_final_agg/struct.CombinePartialFinalAggregate.html
    /// [API policy]: https://datafusion.apache.org/contributor-guide/api-health.html#internal-public-apis
    #[doc(hidden)]
    pub fn try_combine_partial_final(
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
        // Step 1: Ensure the below plan shape
        //
        // AggregateExec(mode=final) <-- current plan
        //   AggregateExec(mode=partial)

        let Some(final_agg) = plan.downcast_ref::<Self>() else {
            return Ok(Transformed::no(plan));
        };
        let mode = match final_agg.mode {
            AggregateMode::Final => AggregateMode::Single,
            AggregateMode::FinalPartitioned => AggregateMode::SinglePartitioned,
            _ => return Ok(Transformed::no(plan)),
        };
        let Some(partial) = final_agg.input.downcast_ref::<Self>() else {
            return Ok(Transformed::no(plan));
        };
        if partial.mode != AggregateMode::Partial {
            return Ok(Transformed::no(plan));
        }

        // Step 2: Check compatibility between partial and final `AggregateExec`

        let final_group_by = final_agg.group_expr();
        let input_group_by = partial.group_expr();
        let final_aggr_expr = final_agg.aggr_expr();
        let input_aggr_expr = partial.aggr_expr();
        let final_filter_expr = final_agg.filter_expr();
        let input_filter_expr = partial.filter_expr();

        // Compare the partial's output expressions with the final's input expressions.
        let can_combine = physical_exprs_equal(
            &input_group_by.output_exprs(),
            &final_group_by.input_exprs(),
        ) && input_group_by.groups() == final_group_by.groups()
            && input_group_by.null_expr().len() == final_group_by.null_expr().len()
            && input_group_by
                .null_expr()
                .iter()
                .zip(final_group_by.null_expr().iter())
                .all(|((lhs_expr, lhs_str), (rhs_expr, rhs_str))| {
                    lhs_expr.eq(rhs_expr) && lhs_str == rhs_str
                })
            && final_aggr_expr.len() == input_aggr_expr.len()
            && final_aggr_expr
                .iter()
                .zip(input_aggr_expr.iter())
                .all(|(final_expr, partial_expr)| final_expr.eq(partial_expr))
            && final_filter_expr.len() == input_filter_expr.len()
            && final_filter_expr.iter().zip(input_filter_expr.iter()).all(
                |(final_expr, partial_expr)| match (final_expr, partial_expr) {
                    (Some(l), Some(r)) => l.eq(r),
                    (None, None) => true,
                    _ => false,
                },
            );
        if !can_combine {
            return Ok(Transformed::no(plan));
        }

        let Ok(mut combined) = Self::try_new(
            mode,
            input_group_by.clone(),
            input_aggr_expr.to_vec(),
            input_filter_expr.to_vec(),
            Arc::clone(partial.input()),
            partial.input_schema(),
        ) else {
            return Ok(Transformed::no(plan));
        };

        // LimitedDistinctAggregation runs before this optimization. Preserve its
        // limit while using the partial's grouping expressions over raw input.
        //
        // The following logic recognizes all possible patterns produced by `LimitedDistinctAggregation`
        // See its comment for details.
        combined.kind = match (&final_agg.kind, &partial.kind) {
            (AggregateKind::General { .. }, AggregateKind::General { .. }) => {
                combined.kind
            }
            (
                AggregateKind::DistinctLimit { limit, .. },
                AggregateKind::DistinctLimit {
                    group_by,
                    limit: partial_limit,
                },
            ) if limit == partial_limit => AggregateKind::DistinctLimit {
                group_by: Arc::clone(group_by),
                limit: *limit,
            },
            // Not possible in the default optimizer pipeline
            _ => return Ok(Transformed::no(plan)),
        };
        Ok(Transformed::yes(Arc::new(combined)))
    }

    // ========================================================================
    // OptimizeAggregateOrder
    // ========================================================================
    /// Clone with replacement aggregate expressions.
    ///
    /// The caller must preserve the schema, filters, and ordering requirements.
    /// Changing expressions in a TopK aggregate drops its specialization.
    ///
    /// # Public Only for Internal Use:
    ///
    /// This is not a public API and is for internal use only; see [API policy] for details.
    ///
    /// [API policy]: https://datafusion.apache.org/contributor-guide/api-health.html#internal-public-apis
    #[doc(hidden)]
    pub fn with_new_aggr_exprs(
        &self,
        aggr_expr: impl Into<Arc<[Arc<AggregateFunctionExpr>]>>,
    ) -> Self {
        let aggr_expr = aggr_expr.into();
        let mut new = self.clone();
        match &mut new.kind {
            AggregateKind::General { aggr_expr: old, .. } => *old = aggr_expr,
            AggregateKind::DistinctLimit { .. } | AggregateKind::TopKDistinct { .. }
                if aggr_expr.is_empty() => {}
            // Ordering optimization can revisit an existing TopK with the
            // same expressions. Preserve its specialization in that case.
            AggregateKind::TopKMinMax {
                aggr_expr: existing,
                ..
            } if matches!(aggr_expr.as_ref(), [new] if Arc::ptr_eq(existing, new)) => {}
            AggregateKind::DistinctLimit { group_by, .. }
            | AggregateKind::TopKMinMax { group_by, .. }
            | AggregateKind::TopKDistinct { group_by, .. } => {
                // A replacement expression cannot inherit a specialization
                // validated for the previous aggregate.
                new.kind = AggregateKind::General {
                    group_by: Arc::clone(group_by),
                    filter_expr: vec![None; aggr_expr.len()].into(),
                    aggr_expr,
                };
            }
        }
        new.metrics = ExecutionPlanMetricsSet::new();
        new
    }

    // ========================================================================
    // TopKAggregation
    // ========================================================================
    /// Try to use TopK (min/max heap) optimization in AggregateExec.
    ///
    /// The optimizer must keep the limit operator above because the aggregate may
    /// still produce more than K rows.
    /// Returns `None` if not applicable.
    ///
    /// # Public Only for Internal Use:
    ///
    /// This is not a public API and is for internal use only; see [API policy] for details.
    ///
    /// [API policy]: https://datafusion.apache.org/contributor-guide/api-health.html#internal-public-apis
    ///
    /// # TopK Optimization Overview
    ///
    /// Retain candidate groups for a parent ORDER BY / LIMIT. There are two applicable
    /// cases:
    ///
    /// ## TopK MIN/MAX
    ///
    /// ```txt
    /// SELECT k, MAX(v) AS score FROM t GROUP BY k
    /// ORDER BY score DESC NULLS LAST LIMIT 10;
    ///
    /// Before:
    /// Sort: score DESC, fetch=10
    ///   Aggregate: k, MAX(v)
    ///     input
    ///
    /// After (Added info in `AggregateExec` to trigger heap optimization):
    /// Sort: score DESC, fetch=10
    ///   Aggregate: k, MAX(v), TopKMinMax(10)
    ///     input
    /// ```
    ///
    /// For an aggregate output named `score`, the example uses:
    /// ```txt
    /// try_optimize_topk(10, "score", SortOptions {
    ///     descending: true,   // DESC
    ///     nulls_first: false, // NULLS LAST
    /// })
    /// ```
    ///
    /// A min-heap keeps the 10 largest non-NULL scores.
    /// MIN with ASC uses a max-heap.
    ///
    /// ## TopK DISTINCT
    ///
    /// ```txt
    /// SELECT DISTINCT k FROM t ORDER BY k ASC NULLS LAST LIMIT 10;
    ///
    /// Before:
    /// Sort: k ASC, fetch=10
    ///   Aggregate: k, no aggregates
    ///     input
    ///
    /// After (Added info in `AggregateExec` to trigger heap optimization):
    /// Sort: k ASC, fetch=10
    ///   Aggregate: k, TopKDistinct(10)
    ///     input
    /// ```
    ///
    /// Here the grouping key is the output used for ordering:
    /// ```txt
    /// try_optimize_topk(10, "k", SortOptions {
    ///     descending: false,  // ASC
    ///     nulls_first: false, // NULLS LAST
    /// })
    /// ```
    ///
    /// A max-heap keeps the 10 smallest non-NULL keys.
    /// DESC uses a min-heap.
    #[doc(hidden)]
    pub fn try_optimize_topk(
        mut self,
        limit: usize,
        order_by: &str,
        options: SortOptions,
    ) -> Option<Transformed<Self>> {
        if limit == 0
            || matches!(self.kind, AggregateKind::DistinctLimit { .. })
            || self.group_by().has_grouping_set()
            || self.filter_expr().iter().any(Option::is_some)
        {
            return None;
        }
        // The priority map needs one supported key and value type.
        let (group_key, group_key_alias) =
            self.group_expr().expr().iter().exactly_one().ok()?;
        let input_schema = self.input.schema();
        let key_type = group_key.data_type(&input_schema).ok()?;
        let minmax = self.get_minmax_desc();
        let value_type = minmax
            .as_ref()
            .map_or(&key_type, |(field, _)| field.data_type());
        if !topk_types_supported(&key_type, value_type)
            || self.schema.fields().len() != 1 + self.aggr_expr().len()
        {
            return None;
        }

        let kind = if let Some((field, descending)) = minmax {
            let aggregate = self.aggr_expr().iter().exactly_one().ok()?;
            if field.name() != order_by
                || descending != options.descending
                || !aggregate.order_bys().is_empty()
            {
                return None;
            }
            let argument = aggregate.expressions().into_iter().exactly_one().ok()?;
            // NULLS FIRST can worsen a group's rank when its first value arrives.
            if options.nulls_first && argument.nullable(&self.input_schema).ok()? {
                return None;
            }

            // Raw input, partial state, and output must fit the same heap value.
            let state_field = aggregate
                .state_fields()
                .ok()?
                .into_iter()
                .exactly_one()
                .ok()?;
            let input_argument = aggregate_expressions(self.aggr_expr(), &self.mode, 1)
                .ok()?
                .into_iter()
                .flatten()
                .exactly_one()
                .ok()?;
            let value_type = field.data_type();
            if state_field.data_type() != value_type
                || argument.data_type(&self.input_schema).ok()? != *value_type
                || input_argument.data_type(&input_schema).ok()? != *value_type
            {
                return None;
            }
            AggregateKind::TopKMinMax {
                group_by: Arc::clone(self.group_by()),
                aggr_expr: Arc::clone(aggregate),
                limit,
                descending,
                nulls_first: options.nulls_first,
            }
        } else if self.aggr_expr().is_empty() && group_key_alias == order_by {
            AggregateKind::TopKDistinct {
                group_by: Arc::clone(self.group_by()),
                limit,
                descending: options.descending,
            }
        } else {
            return None;
        };

        // Preserve compatible tighter bounds; never switch an existing order.
        match &self.kind {
            AggregateKind::TopKMinMax { nulls_first, .. }
                if *nulls_first != options.nulls_first =>
            {
                return None;
            }
            AggregateKind::TopKMinMax {
                limit: existing,
                descending,
                ..
            }
            | AggregateKind::TopKDistinct {
                limit: existing,
                descending,
                ..
            } => {
                if *descending != options.descending {
                    return None;
                }
                if *existing <= limit {
                    return Some(Transformed::no(self));
                }
            }
            _ => {}
        }

        // Commit the kind and properties together: heap output is unordered and final.
        self.kind = kind;
        self.group_clustering_mode = GroupClusteringMode::None;
        self.required_input_ordering = None;
        // Keep unchanged properties so parent aggregates do not need rebuilding.
        if !self.cache.eq_properties.oeq_class().is_empty()
            || self.cache.emission_type != EmissionType::Final
        {
            let mut equivalence = self.cache.eq_properties.clone();
            equivalence.clear_orderings();
            let cache = Arc::make_mut(&mut self.cache);
            cache.set_eq_properties(equivalence);
            cache.emission_type = EmissionType::Final;
        }
        self.metrics = ExecutionPlanMetricsSet::new();
        Some(Transformed::yes(self))
    }

    /// Finds the DataType and SortDirection for this Aggregate, if there is one
    ///
    /// # Public Only for Internal Use:
    ///
    /// This is not a public API and is for internal use only; see [API policy] for details.
    ///
    /// [API policy]: https://datafusion.apache.org/contributor-guide/api-health.html#internal-public-apis
    #[doc(hidden)]
    pub fn get_minmax_desc(&self) -> Option<(FieldRef, bool)> {
        let agg_expr = self.aggr_expr().iter().exactly_one().ok()?;
        agg_expr.get_minmax_desc()
    }
}

/// Returns true if TopK aggregation data structures support the provided key and value types.
///
/// This function checks whether both the key type (used for grouping) and value type
/// (used in min/max aggregation) can be handled by the TopK aggregation heap and hash table.
/// Supported types include Arrow primitives (integers, floats, decimals, intervals) and
/// UTF-8 strings (`Utf8`, `LargeUtf8`, `Utf8View`).
///
/// # Public Only for Internal Use:
///
/// This is not a public API and is for internal use only; see [API policy] for details.
///
/// [API policy]: https://datafusion.apache.org/contributor-guide/api-health.html#internal-public-apis
#[doc(hidden)]
pub fn topk_types_supported(key_type: &DataType, value_type: &DataType) -> bool {
    is_supported_hash_key_type(key_type) && is_supported_heap_type(value_type)
}
