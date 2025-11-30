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

//! Physical expressions for window functions

mod bounded_window_agg_exec;
mod utils;
mod window_agg_exec;

use std::borrow::Borrow;
use std::sync::Arc;

use crate::{
    expressions::PhysicalSortExpr, ExecutionPlan, ExecutionPlanProperties,
    InputOrderMode, PhysicalExpr,
};

use arrow::datatypes::{Schema, SchemaRef};
use arrow_schema::{FieldRef, SortOptions};
use datafusion_common::{exec_err, Result};
use datafusion_expr::{
    LimitEffect, PartitionEvaluator, ReversedUDWF, SetMonotonicity, WindowFrame,
    WindowFunctionDefinition, WindowUDF,
};
use datafusion_functions_window_common::expr::ExpressionArgs;
use datafusion_functions_window_common::field::WindowUDFFieldArgs;
use datafusion_functions_window_common::partition::PartitionEvaluatorArgs;
use datafusion_physical_expr::aggregate::{AggregateExprBuilder, AggregateFunctionExpr};
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr::window::{
    SlidingAggregateWindowExpr, StandardWindowFunctionExpr,
};
use datafusion_physical_expr::{ConstExpr, EquivalenceProperties};
use datafusion_physical_expr_common::sort_expr::{
    LexOrdering, LexRequirement, OrderingRequirements, PhysicalSortRequirement,
};

use itertools::Itertools;

// Public interface:
pub use bounded_window_agg_exec::BoundedWindowAggExec;
pub use datafusion_physical_expr::window::{
    PlainAggregateWindowExpr, StandardWindowExpr, WindowExpr,
};
pub use window_agg_exec::WindowAggExec;

/// Build field from window function and add it into schema
pub fn schema_add_window_field(
    args: &[Arc<dyn PhysicalExpr>],
    schema: &Schema,
    window_fn: &WindowFunctionDefinition,
    fn_name: &str,
) -> Result<Arc<Schema>> {
    let fields = args
        .iter()
        .map(|e| Arc::clone(e).as_ref().return_field(schema))
        .collect::<Result<Vec<_>>>()?;
    let window_expr_return_field = window_fn.return_field(&fields, fn_name)?;
    let mut window_fields = schema
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect_vec();
    // Skip extending schema for UDAF
    if let WindowFunctionDefinition::AggregateUDF(_) = window_fn {
        Ok(Arc::new(Schema::new(window_fields)))
    } else {
        window_fields.extend_from_slice(&[window_expr_return_field
            .as_ref()
            .clone()
            .with_name(fn_name)]);
        Ok(Arc::new(Schema::new(window_fields)))
    }
}

/// Create a physical expression for window function
#[expect(clippy::too_many_arguments)]
pub fn create_window_expr(
    fun: &WindowFunctionDefinition,
    name: String,
    args: &[Arc<dyn PhysicalExpr>],
    partition_by: &[Arc<dyn PhysicalExpr>],
    order_by: &[PhysicalSortExpr],
    window_frame: Arc<WindowFrame>,
    input_schema: SchemaRef,
    ignore_nulls: bool,
    distinct: bool,
    filter: Option<Arc<dyn PhysicalExpr>>,
) -> Result<Arc<dyn WindowExpr>> {
    Ok(match fun {
        WindowFunctionDefinition::AggregateUDF(fun) => {
            let aggregate = if distinct {
                AggregateExprBuilder::new(Arc::clone(fun), args.to_vec())
                    .schema(input_schema)
                    .alias(name)
                    .with_ignore_nulls(ignore_nulls)
                    .distinct()
                    .build()
                    .map(Arc::new)?
            } else {
                AggregateExprBuilder::new(Arc::clone(fun), args.to_vec())
                    .schema(input_schema)
                    .alias(name)
                    .with_ignore_nulls(ignore_nulls)
                    .build()
                    .map(Arc::new)?
            };
            window_expr_from_aggregate_expr(
                partition_by,
                order_by,
                window_frame,
                aggregate,
                filter,
            )
        }
        WindowFunctionDefinition::WindowUDF(fun) => Arc::new(StandardWindowExpr::new(
            create_udwf_window_expr(fun, args, &input_schema, name, ignore_nulls)?,
            partition_by,
            order_by,
            window_frame,
        )),
    })
}

/// Creates an appropriate [`WindowExpr`] based on the window frame and
fn window_expr_from_aggregate_expr(
    partition_by: &[Arc<dyn PhysicalExpr>],
    order_by: &[PhysicalSortExpr],
    window_frame: Arc<WindowFrame>,
    aggregate: Arc<AggregateFunctionExpr>,
    filter: Option<Arc<dyn PhysicalExpr>>,
) -> Arc<dyn WindowExpr> {
    // Is there a potentially unlimited sized window frame?
    let unbounded_window = window_frame.is_ever_expanding();

    if !unbounded_window {
        Arc::new(SlidingAggregateWindowExpr::new(
            aggregate,
            partition_by,
            order_by,
            window_frame,
            filter,
        ))
    } else {
        Arc::new(PlainAggregateWindowExpr::new(
            aggregate,
            partition_by,
            order_by,
            window_frame,
            filter,
        ))
    }
}

/// Creates a `StandardWindowFunctionExpr` suitable for a user defined window function
pub fn create_udwf_window_expr(
    fun: &Arc<WindowUDF>,
    args: &[Arc<dyn PhysicalExpr>],
    input_schema: &Schema,
    name: String,
    ignore_nulls: bool,
) -> Result<Arc<dyn StandardWindowFunctionExpr>> {
    // need to get the types into an owned vec for some reason
    let input_fields: Vec<_> = args
        .iter()
        .map(|arg| arg.return_field(input_schema))
        .collect::<Result<_>>()?;

    let udwf_expr = Arc::new(WindowUDFExpr {
        fun: Arc::clone(fun),
        args: args.to_vec(),
        input_fields,
        name,
        is_reversed: false,
        ignore_nulls,
    });

    // Early validation of input expressions
    // We create a partition evaluator because in the user-defined window
    // implementation this is where code for parsing input expressions
    // exist. The benefits are:
    // - If any of the input expressions are invalid we catch them early
    // in the planning phase, rather than during execution.
    // - Maintains compatibility with built-in (now removed) window
    // functions validation behavior.
    // - Predictable and reliable error handling.
    // See discussion here:
    // https://github.com/apache/datafusion/pull/13201#issuecomment-2454209975
    let _ = udwf_expr.create_evaluator()?;

    Ok(udwf_expr)
}

/// Implements [`StandardWindowFunctionExpr`] for [`WindowUDF`]
#[derive(Clone, Debug)]
pub struct WindowUDFExpr {
    fun: Arc<WindowUDF>,
    args: Vec<Arc<dyn PhysicalExpr>>,
    /// Display name
    name: String,
    /// Fields of input expressions
    input_fields: Vec<FieldRef>,
    /// This is set to `true` only if the user-defined window function
    /// expression supports evaluation in reverse order, and the
    /// evaluation order is reversed.
    is_reversed: bool,
    /// Set to `true` if `IGNORE NULLS` is defined, `false` otherwise.
    ignore_nulls: bool,
}

impl WindowUDFExpr {
    pub fn fun(&self) -> &Arc<WindowUDF> {
        &self.fun
    }
}

impl StandardWindowFunctionExpr for WindowUDFExpr {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn field(&self) -> Result<FieldRef> {
        self.fun
            .field(WindowUDFFieldArgs::new(&self.input_fields, &self.name))
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        self.fun
            .expressions(ExpressionArgs::new(&self.args, &self.input_fields))
    }

    fn create_evaluator(&self) -> Result<Box<dyn PartitionEvaluator>> {
        self.fun
            .partition_evaluator_factory(PartitionEvaluatorArgs::new(
                &self.args,
                &self.input_fields,
                self.is_reversed,
                self.ignore_nulls,
            ))
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn reverse_expr(&self) -> Option<Arc<dyn StandardWindowFunctionExpr>> {
        match self.fun.reverse_expr() {
            ReversedUDWF::Identical => Some(Arc::new(self.clone())),
            ReversedUDWF::NotSupported => None,
            ReversedUDWF::Reversed(fun) => Some(Arc::new(WindowUDFExpr {
                fun,
                args: self.args.clone(),
                name: self.name.clone(),
                input_fields: self.input_fields.clone(),
                is_reversed: !self.is_reversed,
                ignore_nulls: self.ignore_nulls,
            })),
        }
    }

    fn get_result_ordering(&self, schema: &SchemaRef) -> Option<PhysicalSortExpr> {
        self.fun
            .sort_options()
            .zip(schema.column_with_name(self.name()))
            .map(|(options, (idx, field))| {
                let expr = Arc::new(Column::new(field.name(), idx));
                PhysicalSortExpr { expr, options }
            })
    }

    fn limit_effect(&self) -> LimitEffect {
        self.fun.inner().limit_effect(self.args.as_slice())
    }
}

pub(crate) fn calc_requirements<
    T: Borrow<Arc<dyn PhysicalExpr>>,
    S: Borrow<PhysicalSortExpr>,
>(
    partition_by_exprs: impl IntoIterator<Item = T>,
    orderby_sort_exprs: impl IntoIterator<Item = S>,
) -> Option<OrderingRequirements> {
    let mut sort_reqs_with_partition = partition_by_exprs
        .into_iter()
        .map(|partition_by| {
            PhysicalSortRequirement::new(Arc::clone(partition_by.borrow()), None)
        })
        .collect::<Vec<_>>();
    let mut sort_reqs = vec![];
    for element in orderby_sort_exprs.into_iter() {
        let PhysicalSortExpr { expr, options } = element.borrow();
        let sort_req = PhysicalSortRequirement::new(Arc::clone(expr), Some(*options));
        if !sort_reqs_with_partition.iter().any(|e| e.expr.eq(expr)) {
            sort_reqs_with_partition.push(sort_req.clone());
        }
        if !sort_reqs
            .iter()
            .any(|e: &PhysicalSortRequirement| e.expr.eq(expr))
        {
            sort_reqs.push(sort_req);
        }
    }

    let mut alternatives = vec![];
    alternatives.extend(LexRequirement::new(sort_reqs_with_partition));
    alternatives.extend(LexRequirement::new(sort_reqs));

    OrderingRequirements::new_alternatives(alternatives, false)
}

/// This function calculates the indices such that when partition by expressions reordered with the indices
/// resulting expressions define a preset for existing ordering.
/// For instance, if input is ordered by a, b, c and PARTITION BY b, a is used,
/// this vector will be [1, 0]. It means that when we iterate b, a columns with the order [1, 0]
/// resulting vector (a, b) is a preset of the existing ordering (a, b, c).
pub fn get_ordered_partition_by_indices(
    partition_by_exprs: &[Arc<dyn PhysicalExpr>],
    input: &Arc<dyn ExecutionPlan>,
) -> Result<Vec<usize>> {
    let (_, indices) = input
        .equivalence_properties()
        .find_longest_permutation(partition_by_exprs)?;
    Ok(indices)
}

pub(crate) fn get_partition_by_sort_exprs(
    input: &Arc<dyn ExecutionPlan>,
    partition_by_exprs: &[Arc<dyn PhysicalExpr>],
    ordered_partition_by_indices: &[usize],
) -> Result<Vec<PhysicalSortExpr>> {
    let ordered_partition_exprs = ordered_partition_by_indices
        .iter()
        .map(|idx| Arc::clone(&partition_by_exprs[*idx]))
        .collect::<Vec<_>>();
    // Make sure ordered section doesn't move over the partition by expression
    assert!(ordered_partition_by_indices.len() <= partition_by_exprs.len());
    let (ordering, _) = input
        .equivalence_properties()
        .find_longest_permutation(&ordered_partition_exprs)?;
    if ordering.len() == ordered_partition_exprs.len() {
        Ok(ordering)
    } else {
        exec_err!("Expects PARTITION BY expression to be ordered")
    }
}

pub(crate) fn window_equivalence_properties(
    schema: &SchemaRef,
    input: &Arc<dyn ExecutionPlan>,
    window_exprs: &[Arc<dyn WindowExpr>],
) -> Result<EquivalenceProperties> {
    // We need to update the schema, so we can't directly use input's equivalence
    // properties.
    let mut window_eq_properties = EquivalenceProperties::new(Arc::clone(schema))
        .extend(input.equivalence_properties().clone())?;

    let window_schema_len = schema.fields.len();
    let input_schema_len = window_schema_len - window_exprs.len();
    let window_expr_indices = (input_schema_len..window_schema_len).collect::<Vec<_>>();

    for (i, expr) in window_exprs.iter().enumerate() {
        let partitioning_exprs = expr.partition_by();
        let no_partitioning = partitioning_exprs.is_empty();

        // Find "one" valid ordering for partition columns to avoid exponential complexity.
        // see https://github.com/apache/datafusion/issues/17401
        let mut all_satisfied_lexs = vec![];
        let mut candidate_ordering = vec![];

        for partition_expr in partitioning_exprs.iter() {
            let sort_options =
                sort_options_resolving_constant(Arc::clone(partition_expr), true);

            // Try each sort option and pick the first one that works
            let mut found = false;
            for sort_expr in sort_options.into_iter() {
                candidate_ordering.push(sort_expr);
                if let Some(lex) = LexOrdering::new(candidate_ordering.clone()) {
                    if window_eq_properties.ordering_satisfy(lex)? {
                        found = true;
                        break;
                    }
                }
                // This option didn't work, remove it and try the next one
                candidate_ordering.pop();
            }
            // If no sort option works for this column, we can't build a valid ordering
            if !found {
                candidate_ordering.clear();
                break;
            }
        }

        // If we successfully built an ordering for all columns, use it
        // When there are no partition expressions, candidate_ordering will be empty and won't be added
        if candidate_ordering.len() == partitioning_exprs.len() {
            if let Some(lex) = LexOrdering::new(candidate_ordering) {
                all_satisfied_lexs.push(lex);
            }
        }
        // If there is a partitioning, and no possible ordering cannot satisfy
        // the input plan's orderings, then we cannot further introduce any
        // new orderings for the window plan.
        if !no_partitioning && all_satisfied_lexs.is_empty() {
            return Ok(window_eq_properties);
        } else if let Some(std_expr) = expr.as_any().downcast_ref::<StandardWindowExpr>()
        {
            std_expr.add_equal_orderings(&mut window_eq_properties)?;
        } else if let Some(plain_expr) =
            expr.as_any().downcast_ref::<PlainAggregateWindowExpr>()
        {
            // We are dealing with plain window frames; i.e. frames having an
            // unbounded starting point.
            // First, check if the frame covers the whole table:
            if plain_expr.get_window_frame().end_bound.is_unbounded() {
                let window_col =
                    Arc::new(Column::new(expr.name(), i + input_schema_len)) as _;
                if no_partitioning {
                    // Window function has a constant result across the table:
                    window_eq_properties
                        .add_constants(std::iter::once(ConstExpr::from(window_col)))?
                } else {
                    // Window function results in a partial constant value in
                    // some ordering. Adjust the ordering equivalences accordingly:
                    let new_lexs = all_satisfied_lexs.into_iter().flat_map(|lex| {
                        let new_partial_consts = sort_options_resolving_constant(
                            Arc::clone(&window_col),
                            false,
                        );

                        new_partial_consts.into_iter().map(move |partial| {
                            let mut existing = lex.clone();
                            existing.push(partial);
                            existing
                        })
                    });
                    window_eq_properties.add_orderings(new_lexs);
                }
            } else {
                // The window frame is ever expanding, so set monotonicity comes
                // into play.
                plain_expr.add_equal_orderings(
                    &mut window_eq_properties,
                    window_expr_indices[i],
                )?;
            }
        } else if let Some(sliding_expr) =
            expr.as_any().downcast_ref::<SlidingAggregateWindowExpr>()
        {
            // We are dealing with sliding window frames; i.e. frames having an
            // advancing starting point. If we have a set-monotonic expression,
            // we might be able to leverage this property.
            let set_monotonicity = sliding_expr.get_aggregate_expr().set_monotonicity();
            if set_monotonicity.ne(&SetMonotonicity::NotMonotonic) {
                // If the window frame is ever-receding, and we have set
                // monotonicity, we can utilize it to introduce new orderings.
                let frame = sliding_expr.get_window_frame();
                if frame.end_bound.is_unbounded() {
                    let increasing = set_monotonicity.eq(&SetMonotonicity::Increasing);
                    let window_col = Column::new(expr.name(), i + input_schema_len);
                    if no_partitioning {
                        // Reverse set-monotonic cases with no partitioning:
                        window_eq_properties.add_ordering([PhysicalSortExpr::new(
                            Arc::new(window_col),
                            SortOptions::new(increasing, true),
                        )]);
                    } else {
                        // Reverse set-monotonic cases for all orderings:
                        for mut lex in all_satisfied_lexs.into_iter() {
                            lex.push(PhysicalSortExpr::new(
                                Arc::new(window_col.clone()),
                                SortOptions::new(increasing, true),
                            ));
                            window_eq_properties.add_ordering(lex);
                        }
                    }
                }
                // If we ensure that the elements entering the frame is greater
                // than the ones leaving, and we have increasing set-monotonicity,
                // then the window function result will be increasing. However,
                // we also need to check if the frame is causal. If not, we cannot
                // utilize set-monotonicity since the set shrinks as the frame
                // boundary starts "touching" the end of the table.
                else if frame.is_causal() {
                    // Find one valid ordering for aggregate arguments instead of
                    // checking all combinations
                    let aggregate_exprs = sliding_expr.get_aggregate_expr().expressions();
                    let mut candidate_order = vec![];
                    let mut asc = false;

                    for (idx, expr) in aggregate_exprs.iter().enumerate() {
                        let mut found = false;
                        let sort_options =
                            sort_options_resolving_constant(Arc::clone(expr), false);

                        // Try each option and pick the first that works
                        for sort_expr in sort_options.into_iter() {
                            let is_asc = !sort_expr.options.descending;
                            candidate_order.push(sort_expr);

                            if let Some(lex) = LexOrdering::new(candidate_order.clone()) {
                                if window_eq_properties.ordering_satisfy(lex)? {
                                    if idx == 0 {
                                        // The first column's ordering direction determines the overall
                                        // monotonicity behavior of the window result.
                                        // - If the aggregate has increasing set monotonicity (e.g., MAX, COUNT)
                                        //   and the first arg is ascending, the window result is increasing
                                        // - If the aggregate has decreasing set monotonicity (e.g., MIN)
                                        //   and the first arg is ascending, the window result is also increasing
                                        // This flag is used to determine the final window column ordering.
                                        asc = is_asc;
                                    }
                                    found = true;
                                    break;
                                }
                            }
                            // This option didn't work, remove it and try the next one
                            candidate_order.pop();
                        }

                        // If we couldn't extend the ordering, stop trying
                        if !found {
                            break;
                        }
                    }

                    // Check if we successfully built a complete ordering
                    let satisfied = candidate_order.len() == aggregate_exprs.len()
                        && !aggregate_exprs.is_empty();

                    if satisfied {
                        let increasing =
                            set_monotonicity.eq(&SetMonotonicity::Increasing);
                        let window_col = Column::new(expr.name(), i + input_schema_len);
                        if increasing && (asc || no_partitioning) {
                            window_eq_properties.add_ordering([PhysicalSortExpr::new(
                                Arc::new(window_col),
                                SortOptions::new(false, false),
                            )]);
                        } else if !increasing && (!asc || no_partitioning) {
                            window_eq_properties.add_ordering([PhysicalSortExpr::new(
                                Arc::new(window_col),
                                SortOptions::new(true, false),
                            )]);
                        };
                    }
                }
            }
        }
    }
    Ok(window_eq_properties)
}

/// Constructs the best-fitting windowing operator (a `WindowAggExec` or a
/// `BoundedWindowExec`) for the given `input` according to the specifications
/// of `window_exprs` and `physical_partition_keys`. Here, best-fitting means
/// not requiring additional sorting and/or partitioning for the given input.
/// - A return value of `None` represents that there is no way to construct a
///   windowing operator that doesn't need additional sorting/partitioning for
///   the given input. Existing ordering should be changed to run the given
///   windowing operation.
/// - A `Some(window exec)` value contains the optimal windowing operator (a
///   `WindowAggExec` or a `BoundedWindowExec`) for the given input.
pub fn get_best_fitting_window(
    window_exprs: &[Arc<dyn WindowExpr>],
    input: &Arc<dyn ExecutionPlan>,
    // These are the partition keys used during repartitioning.
    // They are either the same with `window_expr`'s PARTITION BY columns,
    // or it is empty if partitioning is not desirable for this windowing operator.
    physical_partition_keys: &[Arc<dyn PhysicalExpr>],
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    // Contains at least one window expr and all of the partition by and order by sections
    // of the window_exprs are same.
    let partitionby_exprs = window_exprs[0].partition_by();
    let orderby_keys = window_exprs[0].order_by();
    let (should_reverse, input_order_mode) =
        if let Some((should_reverse, input_order_mode)) =
            get_window_mode(partitionby_exprs, orderby_keys, input)?
        {
            (should_reverse, input_order_mode)
        } else {
            return Ok(None);
        };
    let is_unbounded = input.boundedness().is_unbounded();
    if !is_unbounded && input_order_mode != InputOrderMode::Sorted {
        // Executor has bounded input and `input_order_mode` is not `InputOrderMode::Sorted`
        // in this case removing the sort is not helpful, return:
        return Ok(None);
    };

    let window_expr = if should_reverse {
        if let Some(reversed_window_expr) = window_exprs
            .iter()
            .map(|e| e.get_reverse_expr())
            .collect::<Option<Vec<_>>>()
        {
            reversed_window_expr
        } else {
            // Cannot take reverse of any of the window expr
            // In this case, with existing ordering window cannot be run
            return Ok(None);
        }
    } else {
        window_exprs.to_vec()
    };

    // If all window expressions can run with bounded memory, choose the
    // bounded window variant:
    if window_expr.iter().all(|e| e.uses_bounded_memory()) {
        Ok(Some(Arc::new(BoundedWindowAggExec::try_new(
            window_expr,
            Arc::clone(input),
            input_order_mode,
            !physical_partition_keys.is_empty(),
        )?) as _))
    } else if input_order_mode != InputOrderMode::Sorted {
        // For `WindowAggExec` to work correctly PARTITION BY columns should be sorted.
        // Hence, if `input_order_mode` is not `Sorted` we should convert
        // input ordering such that it can work with `Sorted` (add `SortExec`).
        // Effectively `WindowAggExec` works only in `Sorted` mode.
        Ok(None)
    } else {
        Ok(Some(Arc::new(WindowAggExec::try_new(
            window_expr,
            Arc::clone(input),
            !physical_partition_keys.is_empty(),
        )?) as _))
    }
}

/// Compares physical ordering (output ordering of the `input` operator) with
/// `partitionby_exprs` and `orderby_keys` to decide whether existing ordering
/// is sufficient to run the current window operator.
/// - A `None` return value indicates that we can not remove the sort in question
///   (input ordering is not sufficient to run current window executor).
/// - A `Some((bool, InputOrderMode))` value indicates that the window operator
///   can run with existing input ordering, so we can remove `SortExec` before it.
///
/// The `bool` field in the return value represents whether we should reverse window
/// operator to remove `SortExec` before it. The `InputOrderMode` field represents
/// the mode this window operator should work in to accommodate the existing ordering.
pub fn get_window_mode(
    partitionby_exprs: &[Arc<dyn PhysicalExpr>],
    orderby_keys: &[PhysicalSortExpr],
    input: &Arc<dyn ExecutionPlan>,
) -> Result<Option<(bool, InputOrderMode)>> {
    let mut input_eqs = input.equivalence_properties().clone();
    let (_, indices) = input_eqs.find_longest_permutation(partitionby_exprs)?;
    let partition_by_reqs = indices
        .iter()
        .map(|&idx| PhysicalSortRequirement {
            expr: Arc::clone(&partitionby_exprs[idx]),
            options: None,
        })
        .collect::<Vec<_>>();
    // Treat partition by exprs as constant. During analysis of requirements are satisfied.
    let const_exprs = partitionby_exprs.iter().cloned().map(ConstExpr::from);
    input_eqs.add_constants(const_exprs)?;
    let reverse_orderby_keys =
        orderby_keys.iter().map(|e| e.reverse()).collect::<Vec<_>>();
    for (should_swap, orderbys) in
        [(false, orderby_keys), (true, reverse_orderby_keys.as_ref())]
    {
        let mut req = partition_by_reqs.clone();
        req.extend(orderbys.iter().cloned().map(Into::into));
        if req.is_empty() || input_eqs.ordering_satisfy_requirement(req)? {
            // Window can be run with existing ordering
            let mode = if indices.len() == partitionby_exprs.len() {
                InputOrderMode::Sorted
            } else if indices.is_empty() {
                InputOrderMode::Linear
            } else {
                InputOrderMode::PartiallySorted(indices)
            };
            return Ok(Some((should_swap, mode)));
        }
    }
    Ok(None)
}

/// Generates sort option variations for a given expression.
///
/// This function is used to handle constant columns in window operations. Since constant
/// columns can be considered as having any ordering, we generate multiple sort options
/// to explore different ordering possibilities.
///
/// # Parameters
/// - `expr`: The physical expression to generate sort options for
/// - `only_monotonic`: If false, generates all 4 possible sort options (ASC/DESC × NULLS FIRST/LAST).
///   If true, generates only 2 options that preserve set monotonicity.
///
/// # When to use `only_monotonic = false`:
/// Use for PARTITION BY columns where we want to explore all possible orderings to find
/// one that matches the existing data ordering.
///
/// # When to use `only_monotonic = true`:
/// Use for aggregate/window function arguments where set monotonicity needs to be preserved.
/// Only generates ASC NULLS LAST and DESC NULLS FIRST because:
/// - Set monotonicity is broken if data has increasing order but nulls come first
/// - Set monotonicity is broken if data has decreasing order but nulls come last
fn sort_options_resolving_constant(
    expr: Arc<dyn PhysicalExpr>,
    only_monotonic: bool,
) -> Vec<PhysicalSortExpr> {
    if only_monotonic {
        // Generate only the 2 options that preserve set monotonicity
        vec![
            PhysicalSortExpr::new(Arc::clone(&expr), SortOptions::new(false, false)), // ASC NULLS LAST
            PhysicalSortExpr::new(expr, SortOptions::new(true, true)), // DESC NULLS FIRST
        ]
    } else {
        // Generate all 4 possible sort options for partition columns
        vec![
            PhysicalSortExpr::new(Arc::clone(&expr), SortOptions::new(false, false)), // ASC NULLS LAST
            PhysicalSortExpr::new(Arc::clone(&expr), SortOptions::new(false, true)), // ASC NULLS FIRST
            PhysicalSortExpr::new(Arc::clone(&expr), SortOptions::new(true, false)), // DESC NULLS LAST
            PhysicalSortExpr::new(expr, SortOptions::new(true, true)), // DESC NULLS FIRST
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collect;
    use crate::expressions::col;
    use crate::streaming::StreamingTableExec;
    use crate::test::assert_is_pending;
    use crate::test::exec::{assert_strong_count_converges_to_zero, BlockingExec};

    use arrow::compute::SortOptions;
    use arrow_schema::{DataType, Field};
    use datafusion_execution::TaskContext;
    use datafusion_functions_aggregate::count::count_udaf;
    use InputOrderMode::{Linear, PartiallySorted, Sorted};

    use futures::FutureExt;

    fn create_test_schema() -> Result<SchemaRef> {
        let nullable_column = Field::new("nullable_col", DataType::Int32, true);
        let non_nullable_column = Field::new("non_nullable_col", DataType::Int32, false);
        let schema = Arc::new(Schema::new(vec![nullable_column, non_nullable_column]));

        Ok(schema)
    }

    fn create_test_schema2() -> Result<SchemaRef> {
        let a = Field::new("a", DataType::Int32, true);
        let b = Field::new("b", DataType::Int32, true);
        let c = Field::new("c", DataType::Int32, true);
        let d = Field::new("d", DataType::Int32, true);
        let e = Field::new("e", DataType::Int32, true);
        let schema = Arc::new(Schema::new(vec![a, b, c, d, e]));
        Ok(schema)
    }

    // Generate a schema which consists of 5 columns (a, b, c, d, e)
    fn create_test_schema3() -> Result<SchemaRef> {
        let a = Field::new("a", DataType::Int32, true);
        let b = Field::new("b", DataType::Int32, false);
        let c = Field::new("c", DataType::Int32, true);
        let d = Field::new("d", DataType::Int32, false);
        let e = Field::new("e", DataType::Int32, false);
        let schema = Arc::new(Schema::new(vec![a, b, c, d, e]));
        Ok(schema)
    }

    /// make PhysicalSortExpr with default options
    pub fn sort_expr(name: &str, schema: &Schema) -> PhysicalSortExpr {
        sort_expr_options(name, schema, SortOptions::default())
    }

    /// PhysicalSortExpr with specified options
    pub fn sort_expr_options(
        name: &str,
        schema: &Schema,
        options: SortOptions,
    ) -> PhysicalSortExpr {
        PhysicalSortExpr {
            expr: col(name, schema).unwrap(),
            options,
        }
    }

    /// Created a sorted Streaming Table exec
    pub fn streaming_table_exec(
        schema: &SchemaRef,
        ordering: LexOrdering,
        infinite_source: bool,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(StreamingTableExec::try_new(
            Arc::clone(schema),
            vec![],
            None,
            Some(ordering),
            infinite_source,
            None,
        )?))
    }

    #[tokio::test]
    async fn test_calc_requirements() -> Result<()> {
        let schema = create_test_schema2()?;
        let test_data = vec![
            // PARTITION BY a, ORDER BY b ASC NULLS FIRST
            (
                vec!["a"],
                vec![("b", true, true)],
                vec![
                    vec![("a", None), ("b", Some((true, true)))],
                    vec![("b", Some((true, true)))],
                ],
            ),
            // PARTITION BY a, ORDER BY a ASC NULLS FIRST
            (
                vec!["a"],
                vec![("a", true, true)],
                vec![vec![("a", None)], vec![("a", Some((true, true)))]],
            ),
            // PARTITION BY a, ORDER BY b ASC NULLS FIRST, c DESC NULLS LAST
            (
                vec!["a"],
                vec![("b", true, true), ("c", false, false)],
                vec![
                    vec![
                        ("a", None),
                        ("b", Some((true, true))),
                        ("c", Some((false, false))),
                    ],
                    vec![("b", Some((true, true))), ("c", Some((false, false)))],
                ],
            ),
            // PARTITION BY a, c, ORDER BY b ASC NULLS FIRST, c DESC NULLS LAST
            (
                vec!["a", "c"],
                vec![("b", true, true), ("c", false, false)],
                vec![
                    vec![("a", None), ("c", None), ("b", Some((true, true)))],
                    vec![("b", Some((true, true))), ("c", Some((false, false)))],
                ],
            ),
        ];
        for (pb_params, ob_params, expected_params) in test_data {
            let mut partitionbys = vec![];
            for col_name in pb_params {
                partitionbys.push(col(col_name, &schema)?);
            }

            let mut orderbys = vec![];
            for (col_name, descending, nulls_first) in ob_params {
                let expr = col(col_name, &schema)?;
                let options = SortOptions::new(descending, nulls_first);
                orderbys.push(PhysicalSortExpr::new(expr, options));
            }

            let mut expected: Option<OrderingRequirements> = None;
            for expected_param in expected_params.clone() {
                let mut requirements = vec![];
                for (col_name, reqs) in expected_param {
                    let options = reqs.map(|(descending, nulls_first)| {
                        SortOptions::new(descending, nulls_first)
                    });
                    let expr = col(col_name, &schema)?;
                    requirements.push(PhysicalSortRequirement::new(expr, options));
                }
                if let Some(requirements) = LexRequirement::new(requirements) {
                    if let Some(alts) = expected.as_mut() {
                        alts.add_alternative(requirements);
                    } else {
                        expected = Some(OrderingRequirements::new(requirements));
                    }
                }
            }
            assert_eq!(calc_requirements(partitionbys, orderbys), expected);
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_drop_cancel() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let schema =
            Arc::new(Schema::new(vec![Field::new("a", DataType::Float32, true)]));

        let blocking_exec = Arc::new(BlockingExec::new(Arc::clone(&schema), 1));
        let refs = blocking_exec.refs();
        let window_agg_exec = Arc::new(WindowAggExec::try_new(
            vec![create_window_expr(
                &WindowFunctionDefinition::AggregateUDF(count_udaf()),
                "count".to_owned(),
                &[col("a", &schema)?],
                &[],
                &[],
                Arc::new(WindowFrame::new(None)),
                schema,
                false,
                false,
                None,
            )?],
            blocking_exec,
            false,
        )?);

        let fut = collect(window_agg_exec, task_ctx);
        let mut fut = fut.boxed();

        assert_is_pending(&mut fut);
        drop(fut);
        assert_strong_count_converges_to_zero(refs).await;

        Ok(())
    }

    #[tokio::test]
    async fn test_satisfy_nullable() -> Result<()> {
        let schema = create_test_schema()?;
        let params = vec![
            ((true, true), (false, false), false),
            ((true, true), (false, true), false),
            ((true, true), (true, false), false),
            ((true, false), (false, true), false),
            ((true, false), (false, false), false),
            ((true, false), (true, true), false),
            ((true, false), (true, false), true),
        ];
        for (
            (physical_desc, physical_nulls_first),
            (req_desc, req_nulls_first),
            expected,
        ) in params
        {
            let physical_ordering = PhysicalSortExpr {
                expr: col("nullable_col", &schema)?,
                options: SortOptions {
                    descending: physical_desc,
                    nulls_first: physical_nulls_first,
                },
            };
            let required_ordering = PhysicalSortExpr {
                expr: col("nullable_col", &schema)?,
                options: SortOptions {
                    descending: req_desc,
                    nulls_first: req_nulls_first,
                },
            };
            let res = physical_ordering.satisfy(&required_ordering.into(), &schema);
            assert_eq!(res, expected);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_satisfy_non_nullable() -> Result<()> {
        let schema = create_test_schema()?;

        let params = vec![
            ((true, true), (false, false), false),
            ((true, true), (false, true), false),
            ((true, true), (true, false), true),
            ((true, false), (false, true), false),
            ((true, false), (false, false), false),
            ((true, false), (true, true), true),
            ((true, false), (true, false), true),
        ];
        for (
            (physical_desc, physical_nulls_first),
            (req_desc, req_nulls_first),
            expected,
        ) in params
        {
            let physical_ordering = PhysicalSortExpr {
                expr: col("non_nullable_col", &schema)?,
                options: SortOptions {
                    descending: physical_desc,
                    nulls_first: physical_nulls_first,
                },
            };
            let required_ordering = PhysicalSortExpr {
                expr: col("non_nullable_col", &schema)?,
                options: SortOptions {
                    descending: req_desc,
                    nulls_first: req_nulls_first,
                },
            };
            let res = physical_ordering.satisfy(&required_ordering.into(), &schema);
            assert_eq!(res, expected);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_get_window_mode_exhaustive() -> Result<()> {
        let test_schema = create_test_schema3()?;
        // Columns a,c are nullable whereas b,d are not nullable.
        // Source is sorted by a ASC NULLS FIRST, b ASC NULLS FIRST, c ASC NULLS FIRST, d ASC NULLS FIRST
        // Column e is not ordered.
        let ordering = [
            sort_expr("a", &test_schema),
            sort_expr("b", &test_schema),
            sort_expr("c", &test_schema),
            sort_expr("d", &test_schema),
        ]
        .into();
        let exec_unbounded = streaming_table_exec(&test_schema, ordering, true)?;

        // test cases consists of vector of tuples. Where each tuple represents a single test case.
        // First field in the tuple is Vec<str> where each element in the vector represents PARTITION BY columns
        // For instance `vec!["a", "b"]` corresponds to PARTITION BY a, b
        // Second field in the tuple is Vec<str> where each element in the vector represents ORDER BY columns
        // For instance, vec!["c"], corresponds to ORDER BY c ASC NULLS FIRST, (ordering is default ordering. We do not check
        // for reversibility in this test).
        // Third field in the tuple is Option<InputOrderMode>, which corresponds to expected algorithm mode.
        // None represents that existing ordering is not sufficient to run executor with any one of the algorithms
        // (We need to add SortExec to be able to run it).
        // Some(InputOrderMode) represents, we can run algorithm with existing ordering; and algorithm should work in
        // InputOrderMode.
        let test_cases = vec![
            (vec!["a"], vec!["a"], Some(Sorted)),
            (vec!["a"], vec!["b"], Some(Sorted)),
            (vec!["a"], vec!["c"], None),
            (vec!["a"], vec!["a", "b"], Some(Sorted)),
            (vec!["a"], vec!["b", "c"], Some(Sorted)),
            (vec!["a"], vec!["a", "c"], None),
            (vec!["a"], vec!["a", "b", "c"], Some(Sorted)),
            (vec!["b"], vec!["a"], Some(Linear)),
            (vec!["b"], vec!["b"], Some(Linear)),
            (vec!["b"], vec!["c"], None),
            (vec!["b"], vec!["a", "b"], Some(Linear)),
            (vec!["b"], vec!["b", "c"], None),
            (vec!["b"], vec!["a", "c"], Some(Linear)),
            (vec!["b"], vec!["a", "b", "c"], Some(Linear)),
            (vec!["c"], vec!["a"], Some(Linear)),
            (vec!["c"], vec!["b"], None),
            (vec!["c"], vec!["c"], Some(Linear)),
            (vec!["c"], vec!["a", "b"], Some(Linear)),
            (vec!["c"], vec!["b", "c"], None),
            (vec!["c"], vec!["a", "c"], Some(Linear)),
            (vec!["c"], vec!["a", "b", "c"], Some(Linear)),
            (vec!["b", "a"], vec!["a"], Some(Sorted)),
            (vec!["b", "a"], vec!["b"], Some(Sorted)),
            (vec!["b", "a"], vec!["c"], Some(Sorted)),
            (vec!["b", "a"], vec!["a", "b"], Some(Sorted)),
            (vec!["b", "a"], vec!["b", "c"], Some(Sorted)),
            (vec!["b", "a"], vec!["a", "c"], Some(Sorted)),
            (vec!["b", "a"], vec!["a", "b", "c"], Some(Sorted)),
            (vec!["c", "b"], vec!["a"], Some(Linear)),
            (vec!["c", "b"], vec!["b"], Some(Linear)),
            (vec!["c", "b"], vec!["c"], Some(Linear)),
            (vec!["c", "b"], vec!["a", "b"], Some(Linear)),
            (vec!["c", "b"], vec!["b", "c"], Some(Linear)),
            (vec!["c", "b"], vec!["a", "c"], Some(Linear)),
            (vec!["c", "b"], vec!["a", "b", "c"], Some(Linear)),
            (vec!["c", "a"], vec!["a"], Some(PartiallySorted(vec![1]))),
            (vec!["c", "a"], vec!["b"], Some(PartiallySorted(vec![1]))),
            (vec!["c", "a"], vec!["c"], Some(PartiallySorted(vec![1]))),
            (
                vec!["c", "a"],
                vec!["a", "b"],
                Some(PartiallySorted(vec![1])),
            ),
            (
                vec!["c", "a"],
                vec!["b", "c"],
                Some(PartiallySorted(vec![1])),
            ),
            (
                vec!["c", "a"],
                vec!["a", "c"],
                Some(PartiallySorted(vec![1])),
            ),
            (
                vec!["c", "a"],
                vec!["a", "b", "c"],
                Some(PartiallySorted(vec![1])),
            ),
            (vec!["c", "b", "a"], vec!["a"], Some(Sorted)),
            (vec!["c", "b", "a"], vec!["b"], Some(Sorted)),
            (vec!["c", "b", "a"], vec!["c"], Some(Sorted)),
            (vec!["c", "b", "a"], vec!["a", "b"], Some(Sorted)),
            (vec!["c", "b", "a"], vec!["b", "c"], Some(Sorted)),
            (vec!["c", "b", "a"], vec!["a", "c"], Some(Sorted)),
            (vec!["c", "b", "a"], vec!["a", "b", "c"], Some(Sorted)),
        ];
        for (case_idx, test_case) in test_cases.iter().enumerate() {
            let (partition_by_columns, order_by_params, expected) = &test_case;
            let mut partition_by_exprs = vec![];
            for col_name in partition_by_columns {
                partition_by_exprs.push(col(col_name, &test_schema)?);
            }

            let mut order_by_exprs = vec![];
            for col_name in order_by_params {
                let expr = col(col_name, &test_schema)?;
                // Give default ordering, this is same with input ordering direction
                // In this test we do check for reversibility.
                let options = SortOptions::default();
                order_by_exprs.push(PhysicalSortExpr { expr, options });
            }
            let res =
                get_window_mode(&partition_by_exprs, &order_by_exprs, &exec_unbounded)?;
            // Since reversibility is not important in this test. Convert Option<(bool, InputOrderMode)> to Option<InputOrderMode>
            let res = res.map(|(_, mode)| mode);
            assert_eq!(
                res, *expected,
                "Unexpected result for in unbounded test case#: {case_idx:?}, case: {test_case:?}"
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_get_window_mode() -> Result<()> {
        let test_schema = create_test_schema3()?;
        // Columns a,c are nullable whereas b,d are not nullable.
        // Source is sorted by a ASC NULLS FIRST, b ASC NULLS FIRST, c ASC NULLS FIRST, d ASC NULLS FIRST
        // Column e is not ordered.
        let ordering = [
            sort_expr("a", &test_schema),
            sort_expr("b", &test_schema),
            sort_expr("c", &test_schema),
            sort_expr("d", &test_schema),
        ]
        .into();
        let exec_unbounded = streaming_table_exec(&test_schema, ordering, true)?;

        // test cases consists of vector of tuples. Where each tuple represents a single test case.
        // First field in the tuple is Vec<str> where each element in the vector represents PARTITION BY columns
        // For instance `vec!["a", "b"]` corresponds to PARTITION BY a, b
        // Second field in the tuple is Vec<(str, bool, bool)> where each element in the vector represents ORDER BY columns
        // For instance, vec![("c", false, false)], corresponds to ORDER BY c ASC NULLS LAST,
        // similarly, vec![("c", true, true)], corresponds to ORDER BY c DESC NULLS FIRST,
        // Third field in the tuple is Option<(bool, InputOrderMode)>, which corresponds to expected result.
        // None represents that existing ordering is not sufficient to run executor with any one of the algorithms
        // (We need to add SortExec to be able to run it).
        // Some((bool, InputOrderMode)) represents, we can run algorithm with existing ordering. Algorithm should work in
        // InputOrderMode, bool field represents whether we should reverse window expressions to run executor with existing ordering.
        // For instance, `Some((false, InputOrderMode::Sorted))`, represents that we shouldn't reverse window expressions. And algorithm
        // should work in Sorted mode to work with existing ordering.
        let test_cases = vec![
            // PARTITION BY a, b ORDER BY c ASC NULLS LAST
            (vec!["a", "b"], vec![("c", false, false)], None),
            // ORDER BY c ASC NULLS FIRST
            (vec![], vec![("c", false, true)], None),
            // PARTITION BY b, ORDER BY c ASC NULLS FIRST
            (vec!["b"], vec![("c", false, true)], None),
            // PARTITION BY a, ORDER BY c ASC NULLS FIRST
            (vec!["a"], vec![("c", false, true)], None),
            // PARTITION BY b, ORDER BY c ASC NULLS FIRST
            (
                vec!["a", "b"],
                vec![("c", false, true), ("e", false, true)],
                None,
            ),
            // PARTITION BY a, ORDER BY b ASC NULLS FIRST
            (vec!["a"], vec![("b", false, true)], Some((false, Sorted))),
            // PARTITION BY a, ORDER BY a ASC NULLS FIRST
            (vec!["a"], vec![("a", false, true)], Some((false, Sorted))),
            // PARTITION BY a, ORDER BY a ASC NULLS LAST
            (vec!["a"], vec![("a", false, false)], Some((false, Sorted))),
            // PARTITION BY a, ORDER BY a DESC NULLS FIRST
            (vec!["a"], vec![("a", true, true)], Some((false, Sorted))),
            // PARTITION BY a, ORDER BY a DESC NULLS LAST
            (vec!["a"], vec![("a", true, false)], Some((false, Sorted))),
            // PARTITION BY a, ORDER BY b ASC NULLS LAST
            (vec!["a"], vec![("b", false, false)], Some((false, Sorted))),
            // PARTITION BY a, ORDER BY b DESC NULLS LAST
            (vec!["a"], vec![("b", true, false)], Some((true, Sorted))),
            // PARTITION BY a, b ORDER BY c ASC NULLS FIRST
            (
                vec!["a", "b"],
                vec![("c", false, true)],
                Some((false, Sorted)),
            ),
            // PARTITION BY b, a ORDER BY c ASC NULLS FIRST
            (
                vec!["b", "a"],
                vec![("c", false, true)],
                Some((false, Sorted)),
            ),
            // PARTITION BY a, b ORDER BY c DESC NULLS LAST
            (
                vec!["a", "b"],
                vec![("c", true, false)],
                Some((true, Sorted)),
            ),
            // PARTITION BY e ORDER BY a ASC NULLS FIRST
            (
                vec!["e"],
                vec![("a", false, true)],
                // For unbounded, expects to work in Linear mode. Shouldn't reverse window function.
                Some((false, Linear)),
            ),
            // PARTITION BY b, c ORDER BY a ASC NULLS FIRST, c ASC NULLS FIRST
            (
                vec!["b", "c"],
                vec![("a", false, true), ("c", false, true)],
                Some((false, Linear)),
            ),
            // PARTITION BY b ORDER BY a ASC NULLS FIRST
            (vec!["b"], vec![("a", false, true)], Some((false, Linear))),
            // PARTITION BY a, e ORDER BY b ASC NULLS FIRST
            (
                vec!["a", "e"],
                vec![("b", false, true)],
                Some((false, PartiallySorted(vec![0]))),
            ),
            // PARTITION BY a, c ORDER BY b ASC NULLS FIRST
            (
                vec!["a", "c"],
                vec![("b", false, true)],
                Some((false, PartiallySorted(vec![0]))),
            ),
            // PARTITION BY c, a ORDER BY b ASC NULLS FIRST
            (
                vec!["c", "a"],
                vec![("b", false, true)],
                Some((false, PartiallySorted(vec![1]))),
            ),
            // PARTITION BY d, b, a ORDER BY c ASC NULLS FIRST
            (
                vec!["d", "b", "a"],
                vec![("c", false, true)],
                Some((false, PartiallySorted(vec![2, 1]))),
            ),
            // PARTITION BY e, b, a ORDER BY c ASC NULLS FIRST
            (
                vec!["e", "b", "a"],
                vec![("c", false, true)],
                Some((false, PartiallySorted(vec![2, 1]))),
            ),
            // PARTITION BY d, a ORDER BY b ASC NULLS FIRST
            (
                vec!["d", "a"],
                vec![("b", false, true)],
                Some((false, PartiallySorted(vec![1]))),
            ),
            // PARTITION BY b, ORDER BY b, a ASC NULLS FIRST
            (
                vec!["a"],
                vec![("b", false, true), ("a", false, true)],
                Some((false, Sorted)),
            ),
            // ORDER BY b, a ASC NULLS FIRST
            (vec![], vec![("b", false, true), ("a", false, true)], None),
        ];
        for (case_idx, test_case) in test_cases.iter().enumerate() {
            let (partition_by_columns, order_by_params, expected) = &test_case;
            let mut partition_by_exprs = vec![];
            for col_name in partition_by_columns {
                partition_by_exprs.push(col(col_name, &test_schema)?);
            }

            let mut order_by_exprs = vec![];
            for (col_name, descending, nulls_first) in order_by_params {
                let expr = col(col_name, &test_schema)?;
                let options = SortOptions {
                    descending: *descending,
                    nulls_first: *nulls_first,
                };
                order_by_exprs.push(PhysicalSortExpr { expr, options });
            }

            assert_eq!(
                get_window_mode(&partition_by_exprs, &order_by_exprs, &exec_unbounded)?,
                *expected,
                "Unexpected result for in unbounded test case#: {case_idx:?}, case: {test_case:?}"
            );
        }

        Ok(())
    }
}
