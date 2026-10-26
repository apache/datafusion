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

//! [`PhysicalOptimizer`] for optimizing [`ExecutionPlan`]s
//!
//! [`ExecutionPlan`]: datafusion_physical_plan::ExecutionPlan

use std::fmt::Debug;
use std::sync::Arc;

use crate::aggregate_statistics::AggregateStatistics;
use crate::combine_partial_final_agg::CombinePartialFinalAggregate;
use crate::eliminate_constant_join::EliminateConstantJoin;
use crate::ensure_coop::EnsureCooperative;
use crate::ensure_requirements::EnsureRequirements;
use crate::filter_pushdown::FilterPushdown;
use crate::join_selection::JoinSelection;
use crate::limit_pushdown::LimitPushdown;
use crate::limited_distinct_aggregation::LimitedDistinctAggregation;
use crate::output_requirements::OutputRequirements;
use crate::projection_pushdown::ProjectionPushdown;
use crate::sanity_checker::SanityCheckPlan;
use crate::topk_aggregation::TopKAggregation;
use crate::topk_repartition::TopKRepartition;
use crate::update_aggr_exprs::OptimizeAggregateOrder;
use arrow::datatypes::{DataType, SchemaRef};

use crate::hash_join_buffering::HashJoinBuffering;
use crate::limit_pushdown_past_window::LimitPushPastWindows;
use crate::pushdown_sort::PushdownSort;
use crate::window_topn::WindowTopN;
use datafusion_common::config::ConfigOptions;
use datafusion_common::stats::Precision;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::{Result, Statistics};
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::expressions::{CastExpr, Column};
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::aggregates::{
    AggregateExec, AggregateMode, PhysicalGroupBy,
};
use datafusion_physical_plan::projection::ProjectionExec;
use datafusion_physical_plan::statistics::{StatisticsArgs, StatisticsContext};

// Re-export from this module for backwards compatibility.
pub use datafusion_session::{PhysicalOptimizerContext, PhysicalOptimizerRule};

pub use datafusion_session::with_statistics_context;

/// Simple context wrapping [`ConfigOptions`] for backward compatibility.
///
/// This struct provides a minimal implementation of [`PhysicalOptimizerContext`]
/// that supplies configuration options and a [`StatisticsContext`] without a
/// statistics registry. Used when no statistics registry is available or
/// needed.
pub struct ConfigOnlyContext<'a> {
    config: &'a ConfigOptions,
    statistics_context: StatisticsContext,
}

impl<'a> ConfigOnlyContext<'a> {
    /// Create a new context wrapping the given config options.
    pub fn new(config: &'a ConfigOptions) -> Self {
        Self {
            config,
            // No providers, matching `statistics_registry()`, which is `None`
            statistics_context: StatisticsContext::new(),
        }
    }
}

impl PhysicalOptimizerContext for ConfigOnlyContext<'_> {
    fn config_options(&self) -> &ConfigOptions {
        self.config
    }

    fn statistics_context(&self) -> Option<&StatisticsContext> {
        Some(&self.statistics_context)
    }
}

const MIN_ROWS_PER_DISTINCT_VALUE: usize = 64;
const MIN_AVERAGE_VALUE_BYTES: usize = 16;
// Parquet reports uncompressed encoded bytes for variable-width columns. A
// compact or tiny source representation is evidence that decoding cannot
// amortize its fixed conversion cost.
const MAX_ENCODED_BYTES_PER_ROW: usize = 16;
const SMALL_ENCODED_SOURCE_BYTES: usize =
    MIN_ROWS_PER_DISTINCT_VALUE * MAX_ENCODED_BYTES_PER_ROW;

/// Decodes dictionary group keys when their input statistics do not prove that
/// keeping the dictionary will amortize its downstream processing cost.
#[derive(Debug, Default)]
struct DictionaryEncoding;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DictionaryDecision {
    Keep,
    KeepGroup,
    Decode,
}

impl DictionaryEncoding {
    fn try_rewrite(
        plan: &Arc<dyn ExecutionPlan>,
        statistics_context: &StatisticsContext,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let Some(final_aggregate) = plan.downcast_ref::<AggregateExec>() else {
            return Ok(None);
        };

        match final_aggregate.mode() {
            AggregateMode::Single | AggregateMode::SinglePartitioned => {
                Self::rewrite_aggregate_chain(final_aggregate, None, statistics_context)
            }
            AggregateMode::Final | AggregateMode::FinalPartitioned => {
                let Some(partial_aggregate) =
                    final_aggregate.input().downcast_ref::<AggregateExec>()
                else {
                    return Ok(None);
                };
                if partial_aggregate.mode() != &AggregateMode::Partial {
                    return Ok(None);
                }
                Self::rewrite_aggregate_chain(
                    final_aggregate,
                    Some(partial_aggregate),
                    statistics_context,
                )
            }
            AggregateMode::Partial | AggregateMode::PartialReduce => Ok(None),
        }
    }

    #[expect(deprecated)]
    fn rewrite_aggregate_chain(
        final_aggregate: &AggregateExec,
        partial_aggregate: Option<&AggregateExec>,
        statistics_context: &StatisticsContext,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let raw_aggregate = partial_aggregate.unwrap_or(final_aggregate);
        let group_by = raw_aggregate.group_expr();
        if group_by.is_empty()
            || group_by.has_grouping_set()
            || !group_by.null_expr().is_empty()
        {
            return Ok(None);
        }

        let statistics = statistics_context
            .compute_arc(raw_aggregate.input(), &StatisticsArgs::new())?;
        let mut candidates = Vec::with_capacity(group_by.expr().len());
        for (expression, name) in group_by.expr() {
            let Some(column) = expression.downcast_ref::<Column>() else {
                candidates.push((Arc::clone(expression), name.clone(), None));
                continue;
            };
            let input_type = raw_aggregate
                .input()
                .schema()
                .field(column.index())
                .data_type()
                .clone();
            let Some((target_type, offset_width)) = Self::dictionary_layout(&input_type)
            else {
                candidates.push((Arc::clone(expression), name.clone(), None));
                continue;
            };
            let decision = Self::dictionary_is_profitable(
                statistics.as_ref(),
                column.index(),
                offset_width,
            )
            .map(|profitable| {
                if profitable {
                    DictionaryDecision::Keep
                } else {
                    DictionaryDecision::Decode
                }
            })
            .or(Self::source_dictionary_decision_from_plan(
                raw_aggregate.input(),
                column.name(),
                &input_type,
                statistics_context,
            )?)
            .unwrap_or(DictionaryDecision::Decode);
            candidates.push((
                Arc::clone(expression),
                name.clone(),
                Some((target_type, decision)),
            ));
        }

        if candidates.iter().any(|(_, _, candidate)| {
            candidate
                .as_ref()
                .is_some_and(|(_, decision)| *decision == DictionaryDecision::KeepGroup)
        }) {
            return Ok(None);
        }
        let mut rewritten = false;
        let expressions = candidates
            .into_iter()
            .map(|(expression, name, candidate)| {
                let Some((target_type, decision)) = candidate else {
                    return (expression, name);
                };
                if decision == DictionaryDecision::Keep {
                    return (expression, name);
                }

                rewritten = true;
                (
                    Arc::new(CastExpr::new(expression, target_type, None))
                        as Arc<dyn PhysicalExpr>,
                    name,
                )
            })
            .collect::<Vec<_>>();
        if !rewritten {
            return Ok(None);
        }

        let raw_input_schema = Arc::clone(&raw_aggregate.input_schema);
        let new_raw_aggregate: Arc<dyn ExecutionPlan> = Arc::new(
            AggregateExec::try_new(
                *raw_aggregate.mode(),
                PhysicalGroupBy::new_single(expressions),
                raw_aggregate.aggr_expr().to_vec(),
                raw_aggregate.filter_expr().to_vec(),
                Arc::clone(raw_aggregate.input()),
                Arc::clone(&raw_input_schema),
            )?
            .with_limit_options(raw_aggregate.limit_options()),
        );

        let new_final_aggregate: Arc<dyn ExecutionPlan> = if partial_aggregate.is_some() {
            Arc::new(
                AggregateExec::try_new(
                    *final_aggregate.mode(),
                    final_aggregate.group_expr().clone(),
                    final_aggregate.aggr_expr().to_vec(),
                    final_aggregate.filter_expr().to_vec(),
                    new_raw_aggregate,
                    raw_input_schema,
                )?
                .with_limit_options(final_aggregate.limit_options()),
            )
        } else {
            new_raw_aggregate
        };

        Self::restore_output_schema(&final_aggregate.schema(), new_final_aggregate)
            .map(Some)
    }

    fn dictionary_layout(data_type: &DataType) -> Option<(DataType, usize)> {
        let DataType::Dictionary(key_type, value_type) = data_type else {
            return None;
        };
        if key_type.as_ref() != &DataType::Int32 {
            return None;
        }
        let offset_width = match value_type.as_ref() {
            DataType::Utf8 | DataType::Binary => size_of::<i32>(),
            DataType::LargeUtf8 | DataType::LargeBinary => size_of::<i64>(),
            _ => return None,
        };
        Some((value_type.as_ref().clone(), offset_width))
    }

    fn dictionary_is_profitable(
        statistics: &Statistics,
        column_index: usize,
        offset_width: usize,
    ) -> Option<bool> {
        let (Precision::Exact(row_count), Some(column_statistics)) = (
            &statistics.num_rows,
            statistics.column_statistics.get(column_index),
        ) else {
            return None;
        };
        let (
            Precision::Exact(null_count),
            Precision::Exact(distinct_count),
            Precision::Exact(byte_size),
        ) = (
            &column_statistics.null_count,
            &column_statistics.distinct_count,
            &column_statistics.byte_size,
        )
        else {
            return None;
        };
        let non_null_rows = row_count.saturating_sub(*null_count);
        if *distinct_count == 0
            || non_null_rows / distinct_count < MIN_ROWS_PER_DISTINCT_VALUE
        {
            return Some(false);
        }

        // Dictionary statistics describe the Arrow output size. Subtract the
        // Int32 keys and Utf8 offsets to conservatively estimate value width.
        let key_bytes = row_count.saturating_mul(size_of::<i32>());
        let offset_bytes = distinct_count
            .saturating_add(1)
            .saturating_mul(offset_width);
        Some(
            byte_size
                .saturating_sub(key_bytes)
                .saturating_sub(offset_bytes)
                / distinct_count
                >= MIN_AVERAGE_VALUE_BYTES,
        )
    }

    fn source_dictionary_decision_from_plan(
        plan: &Arc<dyn ExecutionPlan>,
        column_name: &str,
        data_type: &DataType,
        statistics_context: &StatisticsContext,
    ) -> Result<Option<DictionaryDecision>> {
        let mut decisions = vec![];
        Self::collect_source_dictionary_decisions(
            plan,
            column_name,
            data_type,
            statistics_context,
            &mut decisions,
        )?;
        let Some(first) = decisions.first() else {
            return Ok(None);
        };
        Ok(decisions
            .iter()
            .all(|decision| decision == first)
            .then_some(*first))
    }

    fn collect_source_dictionary_decisions(
        plan: &Arc<dyn ExecutionPlan>,
        column_name: &str,
        data_type: &DataType,
        statistics_context: &StatisticsContext,
        decisions: &mut Vec<DictionaryDecision>,
    ) -> Result<()> {
        let children = plan.children();
        if children.is_empty() {
            let schema = plan.schema();
            let statistics =
                statistics_context.compute_arc(plan, &StatisticsArgs::new())?;
            for (index, field) in schema.fields().iter().enumerate() {
                if field.name() != column_name || field.data_type() != data_type {
                    continue;
                }
                let (Precision::Exact(row_count), Some(byte_size)) = (
                    statistics.num_rows,
                    statistics
                        .column_statistics
                        .get(index)
                        .and_then(|column| column.byte_size.get_value()),
                ) else {
                    continue;
                };
                if row_count > 0 {
                    decisions
                        .push(Self::source_dictionary_decision(row_count, *byte_size));
                }
            }
            return Ok(());
        }

        for child in children {
            Self::collect_source_dictionary_decisions(
                child,
                column_name,
                data_type,
                statistics_context,
                decisions,
            )?;
        }
        Ok(())
    }

    fn source_dictionary_decision(
        source_rows: usize,
        encoded_bytes: usize,
    ) -> DictionaryDecision {
        if encoded_bytes < source_rows.saturating_mul(MAX_ENCODED_BYTES_PER_ROW) {
            DictionaryDecision::Keep
        } else if encoded_bytes < SMALL_ENCODED_SOURCE_BYTES {
            // Mixing decoded and dictionary keys is costly for groups that include
            // a tiny dimension dictionary, so leave the whole group unchanged.
            DictionaryDecision::KeepGroup
        } else {
            DictionaryDecision::Decode
        }
    }

    fn restore_output_schema(
        output_schema: &SchemaRef,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if input.schema() == *output_schema {
            return Ok(input);
        }
        let input_schema = input.schema();
        let expressions =
            output_schema
                .fields()
                .iter()
                .enumerate()
                .map(|(index, field)| {
                    let column = Arc::new(Column::new(field.name(), index))
                        as Arc<dyn PhysicalExpr>;
                    let expression =
                        if input_schema.field(index).data_type() != field.data_type() {
                            Arc::new(CastExpr::new_with_target_field(
                                column,
                                Arc::clone(field),
                                None,
                            )) as Arc<dyn PhysicalExpr>
                        } else {
                            column
                        };
                    (expression, field.name().clone())
                });
        Ok(Arc::new(ProjectionExec::try_new_with_schema_metadata(
            expressions,
            input,
            output_schema.as_ref(),
        )?))
    }
}

impl PhysicalOptimizerRule for DictionaryEncoding {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.optimize_with_context(plan, &ConfigOnlyContext::new(config))
    }

    fn optimize_with_context(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        context: &dyn PhysicalOptimizerContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        with_statistics_context(context, |statistics_context| {
            plan.transform_down(|plan| {
                Ok(match Self::try_rewrite(&plan, statistics_context)? {
                    Some(rewritten) => Transformed::yes(rewritten),
                    None => Transformed::no(plan),
                })
            })
            .data()
        })
    }

    fn name(&self) -> &str {
        "DictionaryEncoding"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// A rule-based physical optimizer.
///
/// Applies a *Pipeline* (an ordered list) of [`PhysicalOptimizerRule`]s in a single pass.
///
/// # Physical Optimizer Contract
///
/// [`PhysicalOptimizer::new`] defines the default sequence of
/// [`PhysicalOptimizerRule`]s. The optimizer applies them in order, in a single
/// pass. This section describes the contract between the optimizer rules, both
/// for projects that extend or reorder the default rule list and for
/// contributors adding new built-in rules.
///
/// ## Invariants
///
/// The following properties hold for all rules (built-in and custom):
///
/// 1. **Correctness.** The returned plan must a) be valid (it can be executed
///    without internal errors) and b) produce the same results as the input
///    plan. A rule may change the operators in the plan, such as changing
///    which operator implements a join, but it may never change the query's
///    semantics.
///
/// 2. **Reorderable.** Because each rule preserves correctness, running the
///    rules in a different order must still yield a valid plan that produces
///    the same results.
///
/// ### Exceptions
///
/// Some built-in rules do not yet fully satisfy the invariants above because
/// they are responsible for making an initial plan *executable* rather than for
/// optimizing an existing plan:
///
/// - [`EnsureRequirements`] inserts the repartitioning and sorting needed to
///   satisfy the distribution and ordering requirements.
/// - [`OutputRequirements`] records output requirements.
/// - [`SanityCheckPlan`] rejects plans whose requirements are not satisfied.
///   It should run last.
///
/// Custom pipelines must keep these rules in a compatible relative order. We
/// are working to remove these exceptions, so that all rules can be reordered
/// freely.
///
/// ## Optimization Coverage Is Not Guaranteed
///
/// The invariants above guarantee *correctness*, not that every optimization is
/// applied. Built-in rules often match specific plan shapes that may only occur
/// at a certain point in the default pipeline. Adding, removing, or reordering
/// rules can change plan shapes, so a built-in rule may silently miss an
/// optimization that would apply in the default order. To keep their
/// implementations simple, built-in rules typically only recognize shapes that
/// the default pipeline produces.
///
/// For example, [`CombinePartialFinalAggregate`] runs after
/// [`EnsureRequirements`] and only merges a `Partial` aggregate that is the
/// direct child of a `Final` aggregate.
///
/// ```text
/// -- Canonical shape (default pipeline):
/// -- built-in rules recognize and optimize this
/// Limit
///   Aggregate(Final)
///     Aggregate(Partial)
///
/// -- Non-canonical shape (repartitioning inserted first):
/// -- built-in rules skip the optimization
/// Limit
///   Aggregate(Final)
///     Repartition
///       Aggregate(Partial)
/// ```
///
/// ## Guidance for Custom Pipelines
///
/// We recommend keeping the default rule order and using the supported
/// configuration options to disable individual optimizations. For example, `SET
/// datafusion.optimizer.enable_distinct_aggregation_soft_limit = false`
/// disables the distinct aggregation soft-limit optimization. These variations
/// are tested and preserve both correctness and optimization coverage.
/// See <https://github.com/apache/datafusion/issues/26481> for more details.
///
/// If you do build a custom pipeline:
///
/// - **Reordering must still yield a valid plan.** Downstream projects are
///   encouraged to add tests that run their pipeline end to end and check
///   results, as DataFusion's own tests only cover the default order.
/// - **Test for reduced optimization coverage,** as described above, and verify
///   the plans you care about with `EXPLAIN`.
/// - **Prefer adding a rule over extending a built-in one.** We discourage
///   extending a built-in rule to match plan shapes it will not encounter in
///   the default pipeline, because such extensions are difficult to test and
///   maintain. Instead, add a custom rule that rewrites your plan into the
///   canonical shape recognized by the built-in rules. If extending a built-in
///   rule still seems necessary, please open a DataFusion issue to discuss it.
#[derive(Clone, Debug)]
pub struct PhysicalOptimizer {
    /// All rules to apply
    pub rules: Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>>,
}

impl Default for PhysicalOptimizer {
    fn default() -> Self {
        Self::new()
    }
}

impl PhysicalOptimizer {
    /// Create a new optimizer using the recommended list of rules
    pub fn new() -> Self {
        // NOTEs:
        // - The order of rules in this list is important, as it determines the
        //   order in which they are applied.
        // - Adding a new rule here is expensive as it will be applied to all
        //   queries, and will likely increase the optimization time. Please extend
        //   existing rules when possible, rather than adding a new rule.
        let rules: Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>> = vec![
            // If there is a output requirement of the query, make sure that
            // this information is not lost across different rules during optimization.
            Arc::new(OutputRequirements::new_add_mode()),
            Arc::new(AggregateStatistics::new()),
            // Runs after AggregateStatistics, which can fold a scalar subquery into a
            // row of literals, and before EnsureRequirements, so that the filter
            // replacing a join against such a row keeps the other input's ordering.
            Arc::new(EliminateConstantJoin::new()),
            // Statistics-based join selection will change the Auto mode to a real join implementation,
            // like collect left, or hash join, or future sort merge join, which will influence the
            // EnsureRequirements rule as it decides whether to add additional repartitioning and
            // local sorting steps to meet distribution and ordering requirements. Therefore, it
            // should run before EnsureRequirements.
            Arc::new(JoinSelection::new()),
            // The LimitedDistinctAggregation rule should be applied before EnsureRequirements,
            // as that rule may inject other operations in between the different AggregateExecs.
            // Applying the rule early means only directly-connected AggregateExecs must be examined.
            Arc::new(LimitedDistinctAggregation::new()),
            // Avoid carrying unprofitable dictionaries through partial and final
            // aggregation before repartitioning is inserted.
            Arc::new(DictionaryEncoding),
            // The FilterPushdown rule tries to push down filters as far as it can.
            // For example, it will push down filtering from a `FilterExec` to `DataSourceExec`.
            // Note that this does not push down dynamic filters (such as those created by a `SortExec` operator in TopK mode),
            // those are handled by the later `FilterPushdown` rule.
            // See `FilterPushdownPhase` for more details.
            Arc::new(FilterPushdown::new()),
            // WindowTopN: replaces Filter(rn<=K) → Window(ROW_NUMBER) with
            // PartitionedTopKExec(fetch=K), which emits `rn` itself; the
            // window node is kept above it only when it has other window
            // expressions to compute.
            // Must run before EnsureRequirements (so it can rewrite against the
            // window's declared ordering without pattern-matching a SortExec)
            // and before ProjectionPushdown (which embeds projections into FilterExec).
            Arc::new(WindowTopN::new()),
            // Ensures each input plan satisfies the distribution and ordering
            // requirements declared by `ExecutionPlan::required_input_distribution`
            // and `ExecutionPlan::required_input_ordering`.
            //
            // If the requirements are already satisfied, this rule leaves the plan
            // unchanged. For example, it does not add sorting when the input is a
            // file scan whose existing order already satisfies the required ordering.
            // Otherwise, this rule inserts the necessary repartitioning and sorting
            // operators.
            //
            // This used to be implemented as two separate rules: `EnforceDistribution`
            // and `EnforceSorting`. It is now a single idempotent rule that decides
            // distribution and sorting together in one bottom-up pass, so the
            // `pushdown_sorts` step no longer breaks distribution invariants set
            // earlier in the pipeline. See the module-level doc on
            // [`EnsureRequirements`](crate::ensure_requirements) for the per-phase
            // breakdown, and <https://github.com/apache/datafusion/issues/21973>
            // for the original failure mode.
            Arc::new(EnsureRequirements::new()),
            // The CombinePartialFinalAggregate rule should be applied after distribution enforcement
            Arc::new(CombinePartialFinalAggregate::new()),
            // Run once after the local sorting requirement is changed
            Arc::new(OptimizeAggregateOrder::new()),
            // TODO: `try_embed_to_hash_join` in the ProjectionPushdown rule would be block by the CoalesceBatches, so add it before CoalesceBatches. Maybe optimize it in the future.
            Arc::new(ProjectionPushdown::new()),
            // Remove the ancillary output requirement operator since we are done with the planning
            // phase.
            Arc::new(OutputRequirements::new_remove_mode()),
            // The aggregation limiter will try to find situations where the accumulator count
            // is not tied to the cardinality, i.e. when the output of the aggregation is passed
            // into an `order by max(x) limit y`. In this case it will copy the limit value down
            // to the aggregation, allowing it to use only y number of accumulators.
            Arc::new(TopKAggregation::new()),
            // Tries to push limits down through window functions, growing as appropriate
            // This can possibly be combined with [LimitPushdown]
            // It needs to come after [EnsureRequirements] (which handles sort enforcement)
            Arc::new(LimitPushPastWindows::new()),
            // The HashJoinBuffering rule adds a BufferExec node with the configured capacity
            // in the prob side of hash joins. That way, the probe side gets eagerly polled before
            // the build side is completely finished.
            Arc::new(HashJoinBuffering::new()),
            // The LimitPushdown rule tries to push limits down as far as possible,
            // replacing operators with fetching variants, or adding limits
            // past operators that support limit pushdown.
            Arc::new(LimitPushdown::new()),
            // TopKRepartition pushes TopK (Sort with fetch) below Hash
            // repartition when the partition key is a prefix of the sort key.
            // This reduces data volume before a hash shuffle. It must run
            // after LimitPushdown so that the TopK already exists on the SortExec.
            Arc::new(TopKRepartition::new()),
            // The ProjectionPushdown rule tries to push projections towards
            // the sources in the execution plan. As a result of this process,
            // a projection can disappear if it reaches the source providers, and
            // sequential projections can merge into one. Even if these two cases
            // are not present, the load of executors such as join or union will be
            // reduced by narrowing their input tables.
            Arc::new(ProjectionPushdown::new()),
            // PushdownSort: Detect sorts that can be pushed down to data sources.
            Arc::new(PushdownSort::new()),
            Arc::new(EnsureCooperative::new()),
            // This FilterPushdown handles dynamic filters that may have references to the source ExecutionPlan.
            // Therefore, it should be run at the end of the optimization process since any changes to the plan may break the dynamic filter's references.
            // See `FilterPushdownPhase` for more details.
            Arc::new(FilterPushdown::new_post_optimization()),
            // The SanityCheckPlan rule checks whether the order and
            // distribution requirements of each node in the plan
            // is satisfied. It will also reject non-runnable query
            // plans that use pipeline-breaking operators on infinite
            // input(s). The rule generates a diagnostic error
            // message for invalid plans. It makes no changes to the
            // given query plan; i.e. it only acts as a final
            // gatekeeping rule.
            Arc::new(SanityCheckPlan::new()),
        ];

        Self::with_rules(rules)
    }

    /// Create a new optimizer with the given rules
    ///
    /// See the [`PhysicalOptimizer`](Self) documentation for the contract
    /// governing non-default optimizer pipelines.
    pub fn with_rules(rules: Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>>) -> Self {
        Self { rules }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, StringDictionaryBuilder};
    use arrow::datatypes::{Field, Int32Type, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion_common::ColumnStatistics;
    use datafusion_execution::TaskContext;
    use datafusion_physical_plan::collect;
    use datafusion_physical_plan::test::TestMemoryExec;
    use datafusion_physical_plan::test::exec::StatisticsExec;

    fn dictionary_statistics(
        row_count: Precision<usize>,
        null_count: Precision<usize>,
        distinct_count: Precision<usize>,
        byte_size: Precision<usize>,
    ) -> Statistics {
        Statistics {
            num_rows: row_count,
            total_byte_size: byte_size,
            column_statistics: vec![ColumnStatistics {
                null_count,
                max_value: Precision::Absent,
                min_value: Precision::Absent,
                sum_value: Precision::Absent,
                distinct_count,
                byte_size,
            }],
        }
    }

    fn dictionary_bytes(row_count: usize, distinct_count: usize, width: usize) -> usize {
        row_count * size_of::<i32>()
            + (distinct_count + 1) * size_of::<i32>()
            + distinct_count * width
    }

    #[test]
    fn dictionary_cost_requires_reuse_width_and_exact_statistics() {
        let profitable = dictionary_statistics(
            Precision::Exact(6_400),
            Precision::Exact(0),
            Precision::Exact(100),
            Precision::Exact(dictionary_bytes(6_400, 100, 16)),
        );
        assert_eq!(
            DictionaryEncoding::dictionary_is_profitable(
                &profitable,
                0,
                size_of::<i32>()
            ),
            Some(true)
        );

        let insufficient_reuse = dictionary_statistics(
            Precision::Exact(6_399),
            Precision::Exact(0),
            Precision::Exact(100),
            Precision::Exact(dictionary_bytes(6_399, 100, 16)),
        );
        assert_eq!(
            DictionaryEncoding::dictionary_is_profitable(
                &insufficient_reuse,
                0,
                size_of::<i32>()
            ),
            Some(false)
        );

        let narrow_values = dictionary_statistics(
            Precision::Exact(6_400),
            Precision::Exact(0),
            Precision::Exact(100),
            Precision::Exact(dictionary_bytes(6_400, 100, 15)),
        );
        assert_eq!(
            DictionaryEncoding::dictionary_is_profitable(
                &narrow_values,
                0,
                size_of::<i32>()
            ),
            Some(false)
        );

        let unknown_ndv = dictionary_statistics(
            Precision::Exact(6_400),
            Precision::Exact(0),
            Precision::Absent,
            Precision::Exact(dictionary_bytes(6_400, 100, 64)),
        );
        assert_eq!(
            DictionaryEncoding::dictionary_is_profitable(
                &unknown_ndv,
                0,
                size_of::<i32>()
            ),
            None
        );
    }

    #[test]
    fn source_dictionary_cost_uses_total_and_per_row_bytes() {
        assert_eq!(
            DictionaryEncoding::source_dictionary_decision(5, 1_000),
            DictionaryDecision::KeepGroup
        );
        assert_eq!(
            DictionaryEncoding::source_dictionary_decision(100, 6_400),
            DictionaryDecision::Decode
        );
        assert_eq!(
            DictionaryEncoding::source_dictionary_decision(100, 1_500),
            DictionaryDecision::Keep
        );
    }

    #[test]
    fn keeps_compact_dictionary_source_when_ndv_is_unknown() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "key",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        )]));
        let statistics = dictionary_statistics(
            Precision::Exact(10_000),
            Precision::Exact(0),
            Precision::Absent,
            Precision::Inexact(10_000),
        );
        let input: Arc<dyn ExecutionPlan> =
            Arc::new(StatisticsExec::new(statistics, schema.as_ref().clone()));
        let aggregate: Arc<dyn ExecutionPlan> = Arc::new(AggregateExec::try_new(
            AggregateMode::Single,
            PhysicalGroupBy::new_single(vec![(
                Arc::new(Column::new("key", 0)) as Arc<dyn PhysicalExpr>,
                "key".to_string(),
            )]),
            vec![],
            vec![],
            input,
            schema,
        )?);

        let optimized = DictionaryEncoding.optimize(aggregate, &ConfigOptions::new())?;

        assert!(optimized.downcast_ref::<AggregateExec>().is_some());
        Ok(())
    }

    #[test]
    fn keeps_all_dictionary_keys_with_a_tiny_source() -> Result<()> {
        let dictionary_type =
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        let schema = Arc::new(Schema::new(vec![
            Field::new("dense", dictionary_type.clone(), false),
            Field::new("tiny", dictionary_type, false),
        ]));
        let column_statistics = |byte_size| ColumnStatistics {
            null_count: Precision::Exact(0),
            max_value: Precision::Absent,
            min_value: Precision::Absent,
            sum_value: Precision::Absent,
            distinct_count: Precision::Absent,
            byte_size: Precision::Inexact(byte_size),
        };
        let statistics = Statistics {
            num_rows: Precision::Exact(5),
            total_byte_size: Precision::Inexact(7_400),
            column_statistics: vec![column_statistics(6_400), column_statistics(1_000)],
        };
        let input: Arc<dyn ExecutionPlan> =
            Arc::new(StatisticsExec::new(statistics, schema.as_ref().clone()));
        let aggregate: Arc<dyn ExecutionPlan> = Arc::new(AggregateExec::try_new(
            AggregateMode::Single,
            PhysicalGroupBy::new_single(vec![
                (
                    Arc::new(Column::new("dense", 0)) as Arc<dyn PhysicalExpr>,
                    "dense".to_string(),
                ),
                (
                    Arc::new(Column::new("tiny", 1)) as Arc<dyn PhysicalExpr>,
                    "tiny".to_string(),
                ),
            ]),
            vec![],
            vec![],
            input,
            schema,
        )?);

        let optimized = DictionaryEncoding.optimize(aggregate, &ConfigOptions::new())?;

        let aggregate = optimized
            .downcast_ref::<AggregateExec>()
            .expect("the aggregate should not be rewritten");
        assert!(
            aggregate
                .group_expr()
                .expr()
                .iter()
                .all(|(expression, _)| expression.downcast_ref::<Column>().is_some())
        );
        Ok(())
    }

    #[test]
    fn decodes_unprofitable_group_key_and_preserves_output_schema() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "key",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        )]));
        let statistics = dictionary_statistics(
            Precision::Exact(1_000),
            Precision::Exact(0),
            Precision::Exact(500),
            Precision::Exact(dictionary_bytes(1_000, 500, 64)),
        );
        let input: Arc<dyn ExecutionPlan> =
            Arc::new(StatisticsExec::new(statistics, schema.as_ref().clone()));
        let group_by = PhysicalGroupBy::new_single(vec![(
            Arc::new(Column::new("key", 0)) as Arc<dyn PhysicalExpr>,
            "key".to_string(),
        )]);
        let aggregate: Arc<dyn ExecutionPlan> = Arc::new(AggregateExec::try_new(
            AggregateMode::Single,
            group_by,
            vec![],
            vec![],
            input,
            Arc::clone(&schema),
        )?);
        let output_schema = aggregate.schema();

        let optimized = DictionaryEncoding.optimize(aggregate, &ConfigOptions::new())?;

        assert_eq!(optimized.schema(), output_schema);
        let projection = optimized
            .downcast_ref::<ProjectionExec>()
            .expect("output dictionary type should be restored");
        let aggregate = projection
            .input()
            .downcast_ref::<AggregateExec>()
            .expect("projection should wrap the rewritten aggregate");
        let group_expression = &aggregate.group_expr().expr()[0].0;
        let cast = group_expression
            .downcast_ref::<CastExpr>()
            .expect("unprofitable dictionary key should be decoded");
        assert_eq!(cast.cast_type(), &DataType::Utf8);
        Ok(())
    }

    #[test]
    fn decodes_before_partial_and_final_aggregation() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "key",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        )]));
        let statistics = dictionary_statistics(
            Precision::Exact(1_000),
            Precision::Exact(0),
            Precision::Exact(500),
            Precision::Exact(dictionary_bytes(1_000, 500, 64)),
        );
        let input: Arc<dyn ExecutionPlan> =
            Arc::new(StatisticsExec::new(statistics, schema.as_ref().clone()));
        let group_by = PhysicalGroupBy::new_single(vec![(
            Arc::new(Column::new("key", 0)) as Arc<dyn PhysicalExpr>,
            "key".to_string(),
        )]);
        let final_group_by = group_by.as_final();
        let partial: Arc<dyn ExecutionPlan> = Arc::new(AggregateExec::try_new(
            AggregateMode::Partial,
            group_by,
            vec![],
            vec![],
            input,
            Arc::clone(&schema),
        )?);
        let final_aggregate: Arc<dyn ExecutionPlan> = Arc::new(AggregateExec::try_new(
            AggregateMode::FinalPartitioned,
            final_group_by,
            vec![],
            vec![],
            partial,
            Arc::clone(&schema),
        )?);
        let output_schema = final_aggregate.schema();

        let optimized =
            DictionaryEncoding.optimize(final_aggregate, &ConfigOptions::new())?;

        assert_eq!(optimized.schema(), output_schema);
        let projection = optimized.downcast_ref::<ProjectionExec>().expect(
            "output dictionary type should be restored after the final aggregate",
        );
        let final_aggregate = projection
            .input()
            .downcast_ref::<AggregateExec>()
            .expect("projection should wrap the final aggregate");
        let partial = final_aggregate
            .input()
            .downcast_ref::<AggregateExec>()
            .expect("final aggregate should consume the rewritten partial aggregate");
        assert!(
            partial.group_expr().expr()[0]
                .0
                .downcast_ref::<CastExpr>()
                .is_some()
        );
        Ok(())
    }

    #[tokio::test]
    async fn decoded_group_key_executes() -> Result<()> {
        let mut builder = StringDictionaryBuilder::<Int32Type>::new();
        builder.append("alpha")?;
        builder.append("beta")?;
        builder.append("alpha")?;
        let dictionary = Arc::new(builder.finish());
        let schema = Arc::new(Schema::new(vec![Field::new(
            "key",
            dictionary.data_type().clone(),
            false,
        )]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![dictionary])?;
        let input: Arc<dyn ExecutionPlan> =
            TestMemoryExec::try_new_exec(&[vec![batch]], Arc::clone(&schema), None)?;
        let group_by = PhysicalGroupBy::new_single(vec![(
            Arc::new(Column::new("key", 0)) as Arc<dyn PhysicalExpr>,
            "key".to_string(),
        )]);
        let aggregate: Arc<dyn ExecutionPlan> = Arc::new(AggregateExec::try_new(
            AggregateMode::Single,
            group_by,
            vec![],
            vec![],
            input,
            schema,
        )?);

        let optimized = DictionaryEncoding.optimize(aggregate, &ConfigOptions::new())?;
        let batches = collect(optimized, Arc::new(TaskContext::default())).await?;

        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
        assert!(matches!(
            batches[0].schema().field(0).data_type(),
            DataType::Dictionary(_, _)
        ));
        Ok(())
    }
}
