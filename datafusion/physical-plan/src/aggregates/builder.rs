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

//! [`AggregateExecBuilder`]: build and rewrite [`AggregateExec`] nodes

use std::sync::Arc;

use super::{
    AggrDynFilter, AggregateExec, AggregateMode, LimitOptions, PhysicalGroupBy,
    create_schema, get_finer_aggregate_exprs_requirement,
};
use crate::metrics::ExecutionPlanMetricsSet;
use crate::{ExecutionPlan, ExecutionPlanProperties, InputOrderMode, PlanProperties};

use arrow::datatypes::SchemaRef;
use datafusion_common::{Result, assert_eq_or_internal_err};
use datafusion_physical_expr::aggregate::AggregateFunctionExpr;
use datafusion_physical_expr::equivalence::ProjectionMapping;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use datafusion_physical_expr_common::sort_expr::{
    LexRequirement, OrderingRequirements, PhysicalSortRequirement,
};

/// The `FILTER` expression of each aggregate expression, `None` where an
/// aggregate has no filter.
type FilterExprs = Arc<[Option<Arc<dyn PhysicalExpr>>]>;

/// Builds an [`AggregateExec`], and is the single place one is constructed.
///
/// Reached through [`AggregateExec::builder`] for a new node and
/// [`AggregateExec::to_builder`] for a rewrite of an existing one; a rewrite
/// keeps the derived state (output schema, plan properties, ordering
/// requirements, dynamic filter) of the node it came from unless a field it is
/// computed from changes, so it costs no more than the hand-written
/// clone-with-one-change methods it replaces.
///
/// `build` is fallible so that the node can be checked here, in the one place
/// it is built, rather than at execution time. It currently only checks that
/// the aggregate and `FILTER` expressions have the same length.
///
/// Public for internal use only: this is how DataFusion's own physical
/// optimizer rules build and rewrite aggregates, and it may change without
/// notice.
///
/// ```
/// # use std::sync::Arc;
/// # use arrow::datatypes::{DataType, Field, Schema};
/// # use datafusion_physical_plan::aggregates::{
/// #     AggregateExec, AggregateMode, LimitOptions, PhysicalGroupBy,
/// # };
/// # use datafusion_physical_plan::{ExecutionPlan, empty::EmptyExec};
/// # use datafusion_physical_expr::expressions::col;
/// # fn main() -> datafusion_common::Result<()> {
/// # let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
/// # let input = Arc::new(EmptyExec::new(Arc::clone(&schema)));
/// # let group_by =
/// #     PhysicalGroupBy::new_single(vec![(col("a", &schema)?, "a".to_string())]);
/// let exec = AggregateExec::builder(AggregateMode::Single, input)
///     .with_group_by(group_by)
///     .build()?;
///
/// // push a limit into it, keeping its schema and plan properties
/// let limited = exec.to_builder().with_limit_options(LimitOptions::new(10)).build()?;
/// assert_eq!(limited.schema(), exec.schema());
/// # Ok(())
/// # }
/// ```
#[doc(hidden)]
#[derive(Debug, Clone)]
pub struct AggregateExecBuilder {
    mode: AggregateMode,
    group_by: Arc<PhysicalGroupBy>,
    aggr_expr: Arc<[Arc<AggregateFunctionExpr>]>,
    /// `None` means "no filter for any aggregate expression"
    filter_expr: Option<FilterExprs>,
    input: Arc<dyn ExecutionPlan>,
    /// `None` means "the schema of `input`"
    input_schema: Option<SchemaRef>,
    limit_options: Option<LimitOptions>,
    /// Output schema explicitly supplied by the caller, see
    /// [`AggregateExecBuilder::with_output_schema`]. Always honored.
    output_schema: Option<SchemaRef>,
    /// State carried over from the [`AggregateExec`] this builder was derived
    /// from, dropped as soon as a field it is computed from changes.
    derived: Option<DerivedState>,
}

/// State of an [`AggregateExec`] that is computed from its inputs, and which is
/// preserved verbatim when a node is rewritten without touching what it is
/// computed from.
#[derive(Debug, Clone)]
struct DerivedState {
    schema: SchemaRef,
    cache: Arc<PlanProperties>,
    required_input_ordering: Option<OrderingRequirements>,
    input_order_mode: InputOrderMode,
    dynamic_filter: Option<Arc<AggrDynFilter>>,
}

impl AggregateExecBuilder {
    /// Create a builder for an aggregate over `input`.
    ///
    /// Unless overridden the aggregate has no group by expressions, no
    /// aggregate expressions, no filters, no limit, and uses the schema of
    /// `input` as its [input schema](AggregateExec::input_schema).
    pub fn new(mode: AggregateMode, input: Arc<dyn ExecutionPlan>) -> Self {
        Self {
            mode,
            group_by: Arc::new(PhysicalGroupBy::default()),
            aggr_expr: Arc::from([]),
            filter_expr: None,
            input,
            input_schema: None,
            limit_options: None,
            output_schema: None,
            derived: None,
        }
    }

    /// Create a builder pre-populated from `exec`.
    ///
    /// Takes `&AggregateExec` rather than ownership because every caller holds
    /// a borrow from `downcast_ref` on an `Arc<dyn ExecutionPlan>`; nothing is
    /// deep-copied, the fields are `Arc`s.
    pub(crate) fn from_exec(exec: &AggregateExec) -> Self {
        Self {
            mode: exec.mode,
            group_by: Arc::clone(&exec.group_by),
            aggr_expr: Arc::clone(&exec.aggr_expr),
            filter_expr: Some(Arc::clone(&exec.filter_expr)),
            input: Arc::clone(&exec.input),
            input_schema: Some(Arc::clone(&exec.input_schema)),
            limit_options: exec.limit_options,
            output_schema: None,
            derived: Some(DerivedState {
                schema: Arc::clone(&exec.schema),
                cache: Arc::clone(&exec.cache),
                required_input_ordering: exec.required_input_ordering.clone(),
                input_order_mode: exec.input_order_mode.clone(),
                dynamic_filter: exec.dynamic_filter.clone(),
            }),
        }
    }

    /// Set the [`AggregateMode`].
    pub fn with_mode(mut self, mode: AggregateMode) -> Self {
        if mode == self.mode {
            return self;
        }
        self.mode = mode;
        self.invalidate_derived()
    }

    /// Set the group by expressions.
    pub fn with_group_by(mut self, group_by: impl Into<Arc<PhysicalGroupBy>>) -> Self {
        let group_by = group_by.into();
        if Arc::ptr_eq(&self.group_by, &group_by) || *self.group_by == *group_by {
            return self;
        }
        self.group_by = group_by;
        self.invalidate_derived()
    }

    /// Set the aggregate expressions.
    ///
    /// A builder derived from an existing node keeps that node's output schema,
    /// so rewriting the aggregate expressions (for example reversing them in
    /// `OptimizeAggregateOrder`) cannot change output field names. This matches
    /// the `AggregateExec::with_new_aggr_exprs` it replaces; nothing checks that
    /// the new expressions still describe that schema.
    pub fn with_aggr_exprs(
        mut self,
        aggr_expr: impl Into<Arc<[Arc<AggregateFunctionExpr>]>>,
    ) -> Self {
        self.aggr_expr = aggr_expr.into();
        self
    }

    /// Set the `FILTER` expression of each aggregate expression.
    ///
    /// Must have the same length as the aggregate expressions; `build` returns
    /// an error otherwise. If never called, no aggregate is filtered.
    pub fn with_filter_exprs(mut self, filter_expr: impl Into<FilterExprs>) -> Self {
        let filter_expr = filter_expr.into();
        if self.filter_expr.as_ref().is_some_and(|existing| {
            Arc::ptr_eq(existing, &filter_expr) || **existing == *filter_expr
        }) {
            return self;
        }
        self.filter_expr = Some(filter_expr);
        self.invalidate_derived()
    }

    /// Set the input plan.
    pub fn with_input(mut self, input: Arc<dyn ExecutionPlan>) -> Self {
        if Arc::ptr_eq(&self.input, &input) {
            return self;
        }
        self.input = input;
        self.invalidate_derived()
    }

    /// Set the [input schema](AggregateExec::input_schema): the schema of the
    /// data *before* any aggregation is applied.
    ///
    /// For `Partial` and `Single` aggregates this is the schema of the input
    /// plan (the default). For `Final` and `FinalPartitioned` aggregates it is
    /// the input schema of the matching partial aggregate, which is *not* the
    /// schema of the input plan.
    pub fn with_input_schema(mut self, input_schema: SchemaRef) -> Self {
        self.input_schema = Some(input_schema);
        self
    }

    /// Set the limit pushed down into this aggregate, or `None` to remove it.
    ///
    /// The limit is a hint: operators above the aggregate still enforce it.
    /// Accepts both `LimitOptions` and `Option<LimitOptions>`.
    ///
    /// Note that not every aggregate can execute every limit. `build` does not
    /// check that yet, so the caller still owns it, exactly as it did before
    /// this builder existed.
    pub fn with_limit_options(
        mut self,
        limit_options: impl Into<Option<LimitOptions>>,
    ) -> Self {
        self.limit_options = limit_options.into();
        self
    }

    /// Use `schema` as the output schema instead of computing it.
    ///
    /// For callers that must preserve a schema exactly, such as decoding a
    /// serialized plan. The caller owns the schema being correct.
    pub(crate) fn with_output_schema(mut self, schema: SchemaRef) -> Self {
        self.output_schema = Some(schema);
        self
    }

    /// Drop state derived from the node this builder came from, because a field
    /// it is computed from was replaced.
    fn invalidate_derived(mut self) -> Self {
        self.derived = None;
        self
    }

    /// Build the [`AggregateExec`].
    pub fn build(self) -> Result<AggregateExec> {
        let Self {
            mode,
            group_by,
            aggr_expr,
            filter_expr,
            input,
            input_schema,
            limit_options,
            output_schema,
            derived,
        } = self;

        let input_schema = input_schema.unwrap_or_else(|| input.schema());
        let filter_expr = filter_expr
            .unwrap_or_else(|| std::iter::repeat_n(None, aggr_expr.len()).collect());

        assert_eq_or_internal_err!(
            aggr_expr.len(),
            filter_expr.len(),
            "Inconsistent aggregate expr: {:?} and filter expr: {:?} for AggregateExec, their size should match",
            aggr_expr,
            filter_expr
        );

        let mut exec = match derived {
            // Nothing the derived state is computed from changed: clone the
            // node this builder came from with the new values rather than
            // recomputing. In particular its output schema is kept, so a
            // rewrite of the aggregate expressions cannot rename output fields.
            Some(derived) if output_schema.is_none() => AggregateExec {
                mode,
                group_by,
                aggr_expr,
                filter_expr,
                input,
                schema: derived.schema,
                input_schema,
                metrics: ExecutionPlanMetricsSet::new(),
                required_input_ordering: derived.required_input_ordering,
                input_order_mode: derived.input_order_mode,
                cache: derived.cache,
                limit_options: None,
                dynamic_filter: derived.dynamic_filter,
            },
            _ => build_from_scratch(
                mode,
                group_by,
                &aggr_expr,
                filter_expr,
                input,
                input_schema,
                output_schema,
            )?,
        };

        exec.limit_options = limit_options;
        Ok(exec)
    }
}

/// Compute every derived part of an [`AggregateExec`] from its inputs: the
/// output schema (unless `output_schema` supplies one), the ordering the input
/// must have, how the input is ordered relative to the group by, the plan
/// properties, and the dynamic filter.
fn build_from_scratch(
    mode: AggregateMode,
    group_by: Arc<PhysicalGroupBy>,
    aggr_expr: &[Arc<AggregateFunctionExpr>],
    filter_expr: FilterExprs,
    input: Arc<dyn ExecutionPlan>,
    input_schema: SchemaRef,
    output_schema: Option<SchemaRef>,
) -> Result<AggregateExec> {
    // `get_finer_aggregate_exprs_requirement` may rewrite the aggregate
    // expressions (e.g. reverse them), so it needs them owned.
    let mut aggr_expr = aggr_expr.to_vec();

    // The output schema is computed from the aggregate expressions *as given*,
    // before the requirement analysis below may rewrite them: output field
    // names come from those expressions and must not change as a side effect
    // of a rewrite. This is why an explicitly supplied schema exists at all.
    let schema = match output_schema {
        Some(schema) => schema,
        None => Arc::new(create_schema(&input.schema(), &group_by, &aggr_expr, mode)?),
    };

    let input_eq_properties = input.equivalence_properties();
    // Get GROUP BY expressions:
    let groupby_exprs = group_by.input_exprs();
    // If existing ordering satisfies a prefix of the GROUP BY expressions,
    // prefix requirements with this section. In this case, aggregation will
    // work more efficiently.
    // Copy the `PhysicalSortExpr`s to retain the sort options.
    let (new_sort_exprs, indices) =
        input_eq_properties.find_longest_permutation(&groupby_exprs)?;

    let mut new_requirements = new_sort_exprs
        .into_iter()
        .map(PhysicalSortRequirement::from)
        .collect::<Vec<_>>();

    let req = get_finer_aggregate_exprs_requirement(
        &mut aggr_expr,
        &group_by,
        input_eq_properties,
        &mode,
    )?;
    new_requirements.extend(req);

    let required_input_ordering =
        LexRequirement::new(new_requirements).map(OrderingRequirements::new_soft);

    // Constant expressions never change, so they cannot mark a completed group.
    // Exclude them from both the ordering indices and the group expression count.
    // If our aggregation has grouping sets then our base grouping exprs will
    // be expanded based on the flags in `group_by.groups` where for each
    // group we swap the grouping expr for `null` if the flag is `true`
    // That means that each index in `indices` is valid if and only if
    // it is not null in every group
    let indices: Vec<usize> = indices
        .into_iter()
        .filter(|idx| group_by.groups.iter().all(|group| !group[*idx]))
        .filter(|idx| {
            input_eq_properties
                .is_expr_constant(&groupby_exprs[*idx])
                .is_none()
        })
        .collect();

    let num_non_constant_groupby_exprs = groupby_exprs
        .iter()
        .filter(|expr| input_eq_properties.is_expr_constant(expr).is_none())
        .count();
    let mut input_order_mode = if indices.len() == num_non_constant_groupby_exprs
        && !indices.is_empty()
        && group_by.groups.len() == 1
    {
        InputOrderMode::Sorted
    } else if !indices.is_empty() {
        InputOrderMode::PartiallySorted(indices)
    } else {
        InputOrderMode::Linear
    };

    // Input order mode is also used to advertise plan output ordering, grouping
    // sets handling, and partial reduce aggregation can't promise that.
    if group_by.has_grouping_set() || mode == AggregateMode::PartialReduce {
        input_order_mode = InputOrderMode::Linear;
    }

    // construct a map from the input expression to the output expression of the Aggregation group by
    let group_expr_mapping =
        ProjectionMapping::try_new(group_by.expr.clone(), &input.schema())?;

    let cache = if group_by.has_grouping_set() {
        AggregateExec::compute_grouping_set_properties(&input, Arc::clone(&schema))
    } else {
        AggregateExec::compute_properties(
            &input,
            Arc::clone(&schema),
            &group_expr_mapping,
            group_by.is_true_no_grouping(),
            &mode,
            &input_order_mode,
            aggr_expr.as_ref(),
        )?
    };

    let mut exec = AggregateExec {
        mode,
        group_by,
        aggr_expr: aggr_expr.into(),
        filter_expr,
        input,
        schema,
        input_schema,
        metrics: ExecutionPlanMetricsSet::new(),
        required_input_ordering,
        limit_options: None,
        input_order_mode,
        cache: Arc::new(cache),
        dynamic_filter: None,
    };

    exec.init_dynamic_filter();

    Ok(exec)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::empty::EmptyExec;

    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_functions_aggregate::count::count_udaf;
    use datafusion_functions_aggregate::min_max::min_udaf;
    use datafusion_physical_expr::aggregate::AggregateExprBuilder;
    use datafusion_physical_expr::expressions::col;

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
        ]))
    }

    fn test_input(schema: &SchemaRef) -> Arc<dyn ExecutionPlan> {
        Arc::new(EmptyExec::new(Arc::clone(schema)))
    }

    fn group_by_a(schema: &SchemaRef) -> Result<PhysicalGroupBy> {
        Ok(PhysicalGroupBy::new_single(vec![(
            col("a", schema)?,
            "a".to_string(),
        )]))
    }

    fn min_b(schema: &SchemaRef) -> Result<Arc<AggregateFunctionExpr>> {
        Ok(Arc::new(
            AggregateExprBuilder::new(min_udaf(), vec![col("b", schema)?])
                .schema(Arc::clone(schema))
                .alias("min_b")
                .build()?,
        ))
    }

    fn count_b(schema: &SchemaRef) -> Result<Arc<AggregateFunctionExpr>> {
        Ok(Arc::new(
            AggregateExprBuilder::new(count_udaf(), vec![col("b", schema)?])
                .schema(Arc::clone(schema))
                .alias("count_b")
                .build()?,
        ))
    }

    /// `filter_expr` defaults to "no filter" instead of having to be a vector of
    /// `None`s of exactly the right length.
    #[test]
    fn filter_exprs_default_to_none() -> Result<()> {
        let schema = test_schema();
        let exec = AggregateExec::builder(AggregateMode::Single, test_input(&schema))
            .with_group_by(group_by_a(&schema)?)
            .with_aggr_exprs(vec![min_b(&schema)?, count_b(&schema)?])
            .build()?;
        assert_eq!(exec.filter_expr(), &[None, None]);
        Ok(())
    }

    #[test]
    fn mismatched_filter_exprs_are_rejected() -> Result<()> {
        let schema = test_schema();
        let err = AggregateExec::builder(AggregateMode::Single, test_input(&schema))
            .with_group_by(group_by_a(&schema)?)
            .with_aggr_exprs(vec![min_b(&schema)?])
            .with_filter_exprs(vec![])
            .build()
            .unwrap_err();
        assert!(
            err.message().contains("their size should match"),
            "unexpected error: {err}"
        );
        Ok(())
    }

    /// Rewriting a node keeps the output schema and plan properties of the node
    /// it was derived from, and resets its metrics.
    #[test]
    fn rewriting_preserves_derived_state() -> Result<()> {
        let schema = test_schema();
        let exec = AggregateExec::builder(AggregateMode::Single, test_input(&schema))
            .with_group_by(group_by_a(&schema)?)
            .with_aggr_exprs(vec![min_b(&schema)?])
            .build()?;

        let limited = exec
            .to_builder()
            .with_limit_options(LimitOptions::new(10))
            .build()?;

        assert_eq!(limited.limit_options(), Some(LimitOptions::new(10)));
        assert_eq!(limited.schema(), exec.schema());
        assert_eq!(limited.input_schema(), exec.input_schema());
        assert_eq!(limited.mode(), exec.mode());
        // the plan properties were reused rather than recomputed
        assert!(Arc::ptr_eq(&limited.cache, &exec.cache));
        // but the metrics of the original node were not carried over
        assert_eq!(limited.metrics().unwrap().iter().count(), 0);
        Ok(())
    }

    /// Changing the mode is a structural change: the output schema of a
    /// `Partial` aggregate holds intermediate state, so it must be recomputed.
    #[test]
    fn changing_the_mode_recomputes_the_schema() -> Result<()> {
        let schema = test_schema();
        let partial = AggregateExec::builder(AggregateMode::Partial, test_input(&schema))
            .with_group_by(group_by_a(&schema)?)
            .with_aggr_exprs(vec![count_b(&schema)?])
            .build()?;
        let single = partial
            .to_builder()
            .with_mode(AggregateMode::Single)
            .build()?;

        // `Partial` emits the accumulator state, `Single` the final count
        assert_eq!(partial.schema().field(1).data_type(), &DataType::Int64);
        assert_eq!(single.schema().field(1).data_type(), &DataType::Int64);
        assert_ne!(
            partial.schema().field(1).name(),
            single.schema().field(1).name()
        );
        Ok(())
    }

    /// Setting a field to the value it already has must not invalidate the
    /// derived state: a rewrite that changes nothing should cost nothing.
    #[test]
    fn setting_a_field_to_its_current_value_keeps_derived_state() -> Result<()> {
        let schema = test_schema();
        let exec = AggregateExec::builder(AggregateMode::Single, test_input(&schema))
            .with_group_by(group_by_a(&schema)?)
            .with_aggr_exprs(vec![min_b(&schema)?])
            .build()?;

        let unchanged = exec
            .to_builder()
            .with_mode(AggregateMode::Single)
            .with_group_by(group_by_a(&schema)?)
            .with_input(Arc::clone(exec.input()))
            .with_filter_exprs(vec![None])
            .with_aggr_exprs(exec.aggr_expr().to_vec())
            .build()?;
        assert!(Arc::ptr_eq(&unchanged.cache, &exec.cache));

        // and a real change still does invalidate it
        let changed = exec
            .to_builder()
            .with_mode(AggregateMode::Partial)
            .build()?;
        assert!(!Arc::ptr_eq(&changed.cache, &exec.cache));
        Ok(())
    }
}
