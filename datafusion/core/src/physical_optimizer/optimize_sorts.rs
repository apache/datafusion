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

//! OptimizeSorts optimizer rule inspects [SortExec]s in the given physical
//! plan and removes the ones it can prove unnecessary. The rule can work on
//! valid *and* invalid physical plans with respect to sorting requirements,
//! but always produces a valid physical plan in this sense.
//!
//! A non-realistic but easy to follow example: Assume that we somehow get the fragment
//! "SortExec: [nullable_col@0 ASC]",
//! "  SortExec: [non_nullable_col@1 ASC]",
//! in the physical plan. The first sort is unnecessary since its result is overwritten
//! by another SortExec. Therefore, this rule removes it from the physical plan.
use crate::config::ConfigOptions;
use crate::error::Result;
use crate::physical_optimizer::utils::{
    add_sort_above_child, ordering_satisfy, ordering_satisfy_concrete,
};
use crate::physical_optimizer::PhysicalOptimizerRule;
use crate::physical_plan::rewrite::TreeNodeRewritable;
use crate::physical_plan::sorts::sort::SortExec;
use crate::physical_plan::windows::{BoundedWindowAggExec, WindowAggExec};
use crate::physical_plan::{with_new_children_if_necessary, ExecutionPlan};
use arrow::datatypes::SchemaRef;
use datafusion_common::{reverse_sort_options, DataFusionError};
use datafusion_physical_expr::window::WindowExpr;
use datafusion_physical_expr::{PhysicalExpr, PhysicalSortExpr};
use itertools::izip;
use std::iter::zip;
use std::sync::Arc;

/// This rule inspects SortExec's in the given physical plan and removes the
/// ones it can prove unnecessary.
#[derive(Default)]
pub struct OptimizeSorts {}

impl OptimizeSorts {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

/// This is a "data class" we use within the [OptimizeSorts] rule that
/// tracks the closest `SortExec` descendant for every child of a plan.
#[derive(Debug, Clone)]
struct PlanWithCorrespondingSort {
    plan: Arc<dyn ExecutionPlan>,
    // For every child, keep a vector of `ExecutionPlan`s starting from the
    // closest `SortExec` till the current plan. The first index of the tuple is
    // the child index of the plan -- we need this information as we make updates.
    sort_onwards: Vec<Vec<(usize, Arc<dyn ExecutionPlan>)>>,
}

impl PlanWithCorrespondingSort {
    pub fn new(plan: Arc<dyn ExecutionPlan>) -> Self {
        let length = plan.children().len();
        PlanWithCorrespondingSort {
            plan,
            sort_onwards: vec![vec![]; length],
        }
    }

    pub fn children(&self) -> Vec<PlanWithCorrespondingSort> {
        self.plan
            .children()
            .into_iter()
            .map(|child| PlanWithCorrespondingSort::new(child))
            .collect()
    }
}

impl TreeNodeRewritable for PlanWithCorrespondingSort {
    fn map_children<F>(self, transform: F) -> Result<Self>
    where
        F: FnMut(Self) -> Result<Self>,
    {
        let children = self.children();
        if children.is_empty() {
            Ok(self)
        } else {
            let children_requirements = children
                .into_iter()
                .map(transform)
                .collect::<Result<Vec<_>>>()?;
            let children_plans = children_requirements
                .iter()
                .map(|elem| elem.plan.clone())
                .collect::<Vec<_>>();
            let sort_onwards = children_requirements
                .iter()
                .map(|item| {
                    if item.sort_onwards.is_empty() {
                        vec![]
                    } else {
                        // TODO: When `maintains_input_order` returns Vec<bool>,
                        //       pass the order-enforcing sort upwards.
                        item.sort_onwards[0].clone()
                    }
                })
                .collect::<Vec<_>>();
            let plan = with_new_children_if_necessary(self.plan, children_plans)?;
            Ok(PlanWithCorrespondingSort { plan, sort_onwards })
        }
    }
}

impl PhysicalOptimizerRule for OptimizeSorts {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Execute a post-order traversal to adjust input key ordering:
        let plan_requirements = PlanWithCorrespondingSort::new(plan);
        let adjusted = plan_requirements.transform_up(&optimize_sorts)?;
        Ok(adjusted.plan)
    }

    fn name(&self) -> &str {
        "OptimizeSorts"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

fn optimize_sorts(
    requirements: PlanWithCorrespondingSort,
) -> Result<Option<PlanWithCorrespondingSort>> {
    // Perform naive analysis at the beginning -- remove already-satisfied sorts:
    if let Some(result) = analyze_immediate_sort_removal(&requirements)? {
        return Ok(Some(result));
    }
    let plan = &requirements.plan;
    let mut new_children = plan.children().clone();
    let mut new_onwards = requirements.sort_onwards.clone();
    for (idx, (child, sort_onwards, required_ordering)) in izip!(
        new_children.iter_mut(),
        new_onwards.iter_mut(),
        plan.required_input_ordering()
    )
    .enumerate()
    {
        let physical_ordering = child.output_ordering();
        match (required_ordering, physical_ordering) {
            (Some(required_ordering), Some(physical_ordering)) => {
                let is_ordering_satisfied = ordering_satisfy_concrete(
                    physical_ordering,
                    required_ordering,
                    || child.equivalence_properties(),
                );
                if !is_ordering_satisfied {
                    // Make sure we preserve the ordering requirements:
                    update_child_to_remove_unnecessary_sort(child, sort_onwards)?;
                    let sort_expr = required_ordering.to_vec();
                    *child = add_sort_above_child(child, sort_expr)?;
                    sort_onwards.push((idx, child.clone()))
                } else if let [first, ..] = sort_onwards.as_slice() {
                    // The ordering requirement is met, we can analyze if there is an unnecessary sort:
                    let sort_any = first.1.clone();
                    let sort_exec = convert_to_sort_exec(&sort_any)?;
                    let sort_output_ordering = sort_exec.output_ordering();
                    let sort_input_ordering = sort_exec.input().output_ordering();
                    // Simple analysis: Does the input of the sort in question already satisfy the ordering requirements?
                    if ordering_satisfy(sort_input_ordering, sort_output_ordering, || {
                        sort_exec.input().equivalence_properties()
                    }) {
                        update_child_to_remove_unnecessary_sort(child, sort_onwards)?;
                    }
                    // For window expressions, we can remove some sorts when we can
                    // calculate the result in reverse:
                    else if let Some(exec) =
                        requirements.plan.as_any().downcast_ref::<WindowAggExec>()
                    {
                        if let Some(result) = analyze_window_sort_removal(
                            exec.window_expr(),
                            &exec.partition_keys,
                            sort_exec,
                            sort_onwards,
                        )? {
                            return Ok(Some(result));
                        }
                    } else if let Some(exec) = requirements
                        .plan
                        .as_any()
                        .downcast_ref::<BoundedWindowAggExec>()
                    {
                        if let Some(result) = analyze_window_sort_removal(
                            exec.window_expr(),
                            &exec.partition_keys,
                            sort_exec,
                            sort_onwards,
                        )? {
                            return Ok(Some(result));
                        }
                    }
                    // TODO: Once we can ensure that required ordering information propagates with
                    //       necessary lineage information, compare `sort_input_ordering` and `required_ordering`.
                    //       This will enable us to handle cases such as (a,b) -> Sort -> (a,b,c) -> Required(a,b).
                    //       Currently, we can not remove such sorts.
                }
            }
            (Some(required), None) => {
                // Ordering requirement is not met, we should add a SortExec to the plan.
                let sort_expr = required.to_vec();
                *child = add_sort_above_child(child, sort_expr)?;
                *sort_onwards = vec![(idx, child.clone())];
            }
            (None, Some(_)) => {
                // We have a SortExec whose effect may be neutralized by a order-imposing
                // operator. In this case, remove this sort:
                if !requirements.plan.maintains_input_order() {
                    update_child_to_remove_unnecessary_sort(child, sort_onwards)?;
                }
            }
            (None, None) => {}
        }
    }
    if plan.children().is_empty() {
        Ok(Some(requirements))
    } else {
        let new_plan = requirements.plan.with_new_children(new_children)?;
        for (idx, (trace, required_ordering)) in new_onwards
            .iter_mut()
            .zip(new_plan.required_input_ordering())
            .enumerate()
            .take(new_plan.children().len())
        {
            // TODO: When `maintains_input_order` returns a `Vec<bool>`, use corresponding index.
            if new_plan.maintains_input_order()
                && required_ordering.is_none()
                && !trace.is_empty()
            {
                trace.push((idx, new_plan.clone()));
            } else {
                trace.clear();
                if new_plan.as_any().is::<SortExec>() {
                    trace.push((idx, new_plan.clone()));
                }
            }
        }
        Ok(Some(PlanWithCorrespondingSort {
            plan: new_plan,
            sort_onwards: new_onwards,
        }))
    }
}

/// Analyzes a given `SortExec` to determine whether its input already has
/// a finer ordering than this `SortExec` enforces.
fn analyze_immediate_sort_removal(
    requirements: &PlanWithCorrespondingSort,
) -> Result<Option<PlanWithCorrespondingSort>> {
    if let Some(sort_exec) = requirements.plan.as_any().downcast_ref::<SortExec>() {
        // If this sort is unnecessary, we should remove it:
        if ordering_satisfy(
            sort_exec.input().output_ordering(),
            sort_exec.output_ordering(),
            || sort_exec.input().equivalence_properties(),
        ) {
            // Since we know that a `SortExec` has exactly one child,
            // we can use the zero index safely:
            let mut new_onwards = requirements.sort_onwards[0].to_vec();
            if !new_onwards.is_empty() {
                new_onwards.pop();
            }
            return Ok(Some(PlanWithCorrespondingSort {
                plan: sort_exec.input().clone(),
                sort_onwards: vec![new_onwards],
            }));
        }
    }
    Ok(None)
}

/// Analyzes a [WindowAggExec] or a [BoundedWindowAggExec] to determine whether
/// it may allow removing a sort.
fn analyze_window_sort_removal(
    window_expr: &[Arc<dyn WindowExpr>],
    partition_keys: &[Arc<dyn PhysicalExpr>],
    sort_exec: &SortExec,
    sort_onward: &mut Vec<(usize, Arc<dyn ExecutionPlan>)>,
) -> Result<Option<PlanWithCorrespondingSort>> {
    let required_ordering = sort_exec.output_ordering().ok_or_else(|| {
        DataFusionError::Plan("A SortExec should have output ordering".to_string())
    })?;
    let physical_ordering = sort_exec.input().output_ordering();
    let physical_ordering = if let Some(physical_ordering) = physical_ordering {
        physical_ordering
    } else {
        // If there is no physical ordering, there is no way to remove a sort -- immediately return:
        return Ok(None);
    };
    let (can_skip_sorting, should_reverse) = can_skip_sort(
        window_expr[0].partition_by(),
        required_ordering,
        &sort_exec.input().schema(),
        physical_ordering,
    )?;
    if can_skip_sorting {
        let new_window_expr = if should_reverse {
            window_expr
                .iter()
                .map(|e| e.get_reverse_expr())
                .collect::<Option<Vec<_>>>()
        } else {
            Some(window_expr.to_vec())
        };
        if let Some(window_expr) = new_window_expr {
            let new_child = remove_corresponding_sort_from_sub_plan(sort_onward)?;
            let new_schema = new_child.schema();

            let uses_bounded_memory = window_expr.iter().all(|e| e.uses_bounded_memory());
            // If all window exprs can run with bounded memory choose bounded window variant
            let new_plan = if uses_bounded_memory {
                Arc::new(BoundedWindowAggExec::try_new(
                    window_expr,
                    new_child,
                    new_schema,
                    partition_keys.to_vec(),
                    Some(physical_ordering.to_vec()),
                )?) as _
            } else {
                Arc::new(WindowAggExec::try_new(
                    window_expr,
                    new_child,
                    new_schema,
                    partition_keys.to_vec(),
                    Some(physical_ordering.to_vec()),
                )?) as _
            };
            return Ok(Some(PlanWithCorrespondingSort::new(new_plan)));
        }
    }
    Ok(None)
}

/// Updates child to remove the unnecessary sorting below it.
fn update_child_to_remove_unnecessary_sort(
    child: &mut Arc<dyn ExecutionPlan>,
    sort_onwards: &mut Vec<(usize, Arc<dyn ExecutionPlan>)>,
) -> Result<()> {
    if !sort_onwards.is_empty() {
        *child = remove_corresponding_sort_from_sub_plan(sort_onwards)?;
    }
    Ok(())
}

/// Converts an [ExecutionPlan] trait object to a [SortExec] when possible.
fn convert_to_sort_exec(sort_any: &Arc<dyn ExecutionPlan>) -> Result<&SortExec> {
    sort_any.as_any().downcast_ref::<SortExec>().ok_or_else(|| {
        DataFusionError::Plan("Given ExecutionPlan is not a SortExec".to_string())
    })
}

/// Removes the sort from the plan in `sort_onwards`.
fn remove_corresponding_sort_from_sub_plan(
    sort_onwards: &mut Vec<(usize, Arc<dyn ExecutionPlan>)>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let (sort_child_idx, sort_any) = sort_onwards[0].clone();
    let sort_exec = convert_to_sort_exec(&sort_any)?;
    let mut prev_layer = sort_exec.input().clone();
    let mut prev_child_idx = sort_child_idx;
    // In the loop below, se start from 1 as the first one is a SortExec
    // and we are removing it from the plan.
    for (child_idx, layer) in sort_onwards.iter().skip(1) {
        let mut children = layer.children();
        children[prev_child_idx] = prev_layer;
        prev_layer = layer.clone().with_new_children(children)?;
        prev_child_idx = *child_idx;
    }
    // We have removed the sort, hence empty the sort_onwards:
    sort_onwards.clear();
    Ok(prev_layer)
}

#[derive(Debug)]
/// This structure stores extra column information required to remove unnecessary sorts.
pub struct ColumnInfo {
    is_aligned: bool,
    reverse: bool,
    is_partition: bool,
}

/// Compares physical ordering and required ordering of all `PhysicalSortExpr`s and returns a tuple.
/// The first element indicates whether these `PhysicalSortExpr`s can be removed from the physical plan.
/// The second element is a flag indicating whether we should reverse the sort direction in order to
/// remove physical sort expressions from the plan.
pub fn can_skip_sort(
    partition_keys: &[Arc<dyn PhysicalExpr>],
    required: &[PhysicalSortExpr],
    input_schema: &SchemaRef,
    physical_ordering: &[PhysicalSortExpr],
) -> Result<(bool, bool)> {
    if required.len() > physical_ordering.len() {
        return Ok((false, false));
    }
    let mut col_infos = vec![];
    for (sort_expr, physical_expr) in zip(required, physical_ordering) {
        let column = sort_expr.expr.clone();
        let is_partition = partition_keys.iter().any(|e| e.eq(&column));
        let (is_aligned, reverse) =
            check_alignment(input_schema, physical_expr, sort_expr);
        col_infos.push(ColumnInfo {
            is_aligned,
            reverse,
            is_partition,
        });
    }
    let partition_by_sections = col_infos
        .iter()
        .filter(|elem| elem.is_partition)
        .collect::<Vec<_>>();
    let can_skip_partition_bys = if partition_by_sections.is_empty() {
        true
    } else {
        let first_reverse = partition_by_sections[0].reverse;
        let can_skip_partition_bys = partition_by_sections
            .iter()
            .all(|c| c.is_aligned && c.reverse == first_reverse);
        can_skip_partition_bys
    };
    let order_by_sections = col_infos
        .iter()
        .filter(|elem| !elem.is_partition)
        .collect::<Vec<_>>();
    let (can_skip_order_bys, should_reverse_order_bys) = if order_by_sections.is_empty() {
        (true, false)
    } else {
        let first_reverse = order_by_sections[0].reverse;
        let can_skip_order_bys = order_by_sections
            .iter()
            .all(|c| c.is_aligned && c.reverse == first_reverse);
        (can_skip_order_bys, first_reverse)
    };
    let can_skip = can_skip_order_bys && can_skip_partition_bys;
    Ok((can_skip, should_reverse_order_bys))
}

/// Compares `physical_ordering` and `required` ordering, returns a tuple
/// indicating (1) whether this column requires sorting, and (2) whether we
/// should reverse the window expression in order to avoid sorting.
fn check_alignment(
    input_schema: &SchemaRef,
    physical_ordering: &PhysicalSortExpr,
    required: &PhysicalSortExpr,
) -> (bool, bool) {
    if required.expr.eq(&physical_ordering.expr) {
        let nullable = required.expr.nullable(input_schema).unwrap();
        let physical_opts = physical_ordering.options;
        let required_opts = required.options;
        let is_reversed = if nullable {
            physical_opts == reverse_sort_options(required_opts)
        } else {
            // If the column is not nullable, NULLS FIRST/LAST is not important.
            physical_opts.descending != required_opts.descending
        };
        let can_skip = !nullable || is_reversed || (physical_opts == required_opts);
        (can_skip, is_reversed)
    } else {
        (false, false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::physical_plan::displayable;
    use crate::physical_plan::filter::FilterExec;
    use crate::physical_plan::memory::MemoryExec;
    use crate::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
    use crate::physical_plan::windows::create_window_expr;
    use crate::prelude::SessionContext;
    use arrow::compute::SortOptions;
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion_common::Result;
    use datafusion_expr::{AggregateFunction, WindowFrame, WindowFunction};
    use datafusion_physical_expr::expressions::{col, NotExpr};
    use datafusion_physical_expr::PhysicalSortExpr;
    use std::sync::Arc;

    fn create_test_schema() -> Result<SchemaRef> {
        let nullable_column = Field::new("nullable_col", DataType::Int32, true);
        let non_nullable_column = Field::new("non_nullable_col", DataType::Int32, false);
        let schema = Arc::new(Schema::new(vec![nullable_column, non_nullable_column]));

        Ok(schema)
    }

    #[tokio::test]
    async fn test_is_column_aligned_nullable() -> Result<()> {
        let schema = create_test_schema()?;
        let params = vec![
            ((true, true), (false, false), (true, true)),
            ((true, true), (false, true), (false, false)),
            ((true, true), (true, false), (false, false)),
            ((true, false), (false, true), (true, true)),
            ((true, false), (false, false), (false, false)),
            ((true, false), (true, true), (false, false)),
        ];
        for (
            (physical_desc, physical_nulls_first),
            (req_desc, req_nulls_first),
            (is_aligned_expected, reverse_expected),
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
            let (is_aligned, reverse) =
                check_alignment(&schema, &physical_ordering, &required_ordering);
            assert_eq!(is_aligned, is_aligned_expected);
            assert_eq!(reverse, reverse_expected);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_is_column_aligned_non_nullable() -> Result<()> {
        let schema = create_test_schema()?;

        let params = vec![
            ((true, true), (false, false), (true, true)),
            ((true, true), (false, true), (true, true)),
            ((true, true), (true, false), (true, false)),
            ((true, false), (false, true), (true, true)),
            ((true, false), (false, false), (true, true)),
            ((true, false), (true, true), (true, false)),
        ];
        for (
            (physical_desc, physical_nulls_first),
            (req_desc, req_nulls_first),
            (is_aligned_expected, reverse_expected),
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
            let (is_aligned, reverse) =
                check_alignment(&schema, &physical_ordering, &required_ordering);
            assert_eq!(is_aligned, is_aligned_expected);
            assert_eq!(reverse, reverse_expected);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_remove_unnecessary_sort() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let schema = create_test_schema()?;
        let source = Arc::new(MemoryExec::try_new(&[], schema.clone(), None)?)
            as Arc<dyn ExecutionPlan>;
        let sort_exprs = vec![PhysicalSortExpr {
            expr: col("non_nullable_col", schema.as_ref()).unwrap(),
            options: SortOptions::default(),
        }];
        let sort_exec = Arc::new(SortExec::try_new(sort_exprs, source, None)?)
            as Arc<dyn ExecutionPlan>;
        let sort_exprs = vec![PhysicalSortExpr {
            expr: col("nullable_col", schema.as_ref()).unwrap(),
            options: SortOptions::default(),
        }];
        let physical_plan = Arc::new(SortExec::try_new(sort_exprs, sort_exec, None)?)
            as Arc<dyn ExecutionPlan>;
        let formatted = displayable(physical_plan.as_ref()).indent().to_string();
        let expected = {
            vec![
                "SortExec: [nullable_col@0 ASC]",
                "  SortExec: [non_nullable_col@1 ASC]",
            ]
        };
        let actual: Vec<&str> = formatted.trim().lines().collect();
        let actual_len = actual.len();
        let actual_trim_last = &actual[..actual_len - 1];
        assert_eq!(
            expected, actual_trim_last,
            "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
        );
        let optimized_physical_plan =
            OptimizeSorts::new().optimize(physical_plan, state.config_options())?;
        let formatted = displayable(optimized_physical_plan.as_ref())
            .indent()
            .to_string();
        let expected = { vec!["SortExec: [nullable_col@0 ASC]"] };
        let actual: Vec<&str> = formatted.trim().lines().collect();
        let actual_len = actual.len();
        let actual_trim_last = &actual[..actual_len - 1];
        assert_eq!(
            expected, actual_trim_last,
            "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_remove_unnecessary_sort_window_multilayer() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let schema = create_test_schema()?;
        let source = Arc::new(MemoryExec::try_new(&[], schema.clone(), None)?)
            as Arc<dyn ExecutionPlan>;
        let sort_exprs = vec![PhysicalSortExpr {
            expr: col("non_nullable_col", source.schema().as_ref()).unwrap(),
            options: SortOptions {
                descending: true,
                nulls_first: true,
            },
        }];
        let sort_exec = Arc::new(SortExec::try_new(sort_exprs.clone(), source, None)?)
            as Arc<dyn ExecutionPlan>;
        let window_agg_exec = Arc::new(WindowAggExec::try_new(
            vec![create_window_expr(
                &WindowFunction::AggregateFunction(AggregateFunction::Count),
                "count".to_owned(),
                &[col("non_nullable_col", &schema)?],
                &[],
                &sort_exprs,
                Arc::new(WindowFrame::new(true)),
                schema.as_ref(),
            )?],
            sort_exec.clone(),
            sort_exec.schema(),
            vec![],
            Some(sort_exprs),
        )?) as Arc<dyn ExecutionPlan>;
        let sort_exprs = vec![PhysicalSortExpr {
            expr: col("non_nullable_col", window_agg_exec.schema().as_ref()).unwrap(),
            options: SortOptions {
                descending: false,
                nulls_first: false,
            },
        }];
        let sort_exec = Arc::new(SortExec::try_new(
            sort_exprs.clone(),
            window_agg_exec,
            None,
        )?) as Arc<dyn ExecutionPlan>;
        // Add dummy layer propagating Sort above, to test whether sort can be removed from multi layer before
        let filter_exec = Arc::new(FilterExec::try_new(
            Arc::new(NotExpr::new(
                col("non_nullable_col", schema.as_ref()).unwrap(),
            )),
            sort_exec,
        )?) as Arc<dyn ExecutionPlan>;
        // let filter_exec = sort_exec;
        let window_agg_exec = Arc::new(WindowAggExec::try_new(
            vec![create_window_expr(
                &WindowFunction::AggregateFunction(AggregateFunction::Count),
                "count".to_owned(),
                &[col("non_nullable_col", &schema)?],
                &[],
                &sort_exprs,
                Arc::new(WindowFrame::new(true)),
                schema.as_ref(),
            )?],
            filter_exec.clone(),
            filter_exec.schema(),
            vec![],
            Some(sort_exprs),
        )?) as Arc<dyn ExecutionPlan>;
        let physical_plan = window_agg_exec;
        let formatted = displayable(physical_plan.as_ref()).indent().to_string();
        let expected = {
            vec![
                "WindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow }]",
                "  FilterExec: NOT non_nullable_col@1",
                "    SortExec: [non_nullable_col@1 ASC NULLS LAST]",
                "      WindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow }]",
                "        SortExec: [non_nullable_col@1 DESC]",
                "          MemoryExec: partitions=0, partition_sizes=[]",
            ]
        };
        let actual: Vec<&str> = formatted.trim().lines().collect();
        assert_eq!(
            expected, actual,
            "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
        );
        let optimized_physical_plan =
            OptimizeSorts::new().optimize(physical_plan, state.config_options())?;
        let formatted = displayable(optimized_physical_plan.as_ref())
            .indent()
            .to_string();
        let expected = {
            vec![
                "WindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: CurrentRow, end_bound: Following(NULL) }]",
                "  FilterExec: NOT non_nullable_col@1",
                "    WindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow }]",
                "      SortExec: [non_nullable_col@1 DESC]",
                "        MemoryExec: partitions=0, partition_sizes=[]",
            ]
        };
        let actual: Vec<&str> = formatted.trim().lines().collect();
        assert_eq!(
            expected, actual,
            "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_add_required_sort() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let schema = create_test_schema()?;
        let source = Arc::new(MemoryExec::try_new(&[], schema.clone(), None)?)
            as Arc<dyn ExecutionPlan>;
        let sort_exprs = vec![PhysicalSortExpr {
            expr: col("nullable_col", schema.as_ref()).unwrap(),
            options: SortOptions::default(),
        }];
        let physical_plan = Arc::new(SortPreservingMergeExec::new(sort_exprs, source))
            as Arc<dyn ExecutionPlan>;
        let formatted = displayable(physical_plan.as_ref()).indent().to_string();
        let expected = { vec!["SortPreservingMergeExec: [nullable_col@0 ASC]"] };
        let actual: Vec<&str> = formatted.trim().lines().collect();
        let actual_len = actual.len();
        let actual_trim_last = &actual[..actual_len - 1];
        assert_eq!(
            expected, actual_trim_last,
            "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
        );
        let optimized_physical_plan =
            OptimizeSorts::new().optimize(physical_plan, state.config_options())?;
        let formatted = displayable(optimized_physical_plan.as_ref())
            .indent()
            .to_string();
        let expected = {
            vec![
                "SortPreservingMergeExec: [nullable_col@0 ASC]",
                "  SortExec: [nullable_col@0 ASC]",
            ]
        };
        let actual: Vec<&str> = formatted.trim().lines().collect();
        let actual_len = actual.len();
        let actual_trim_last = &actual[..actual_len - 1];
        assert_eq!(
            expected, actual_trim_last,
            "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_remove_unnecessary_sort1() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let schema = create_test_schema()?;
        let source = Arc::new(MemoryExec::try_new(&[], schema.clone(), None)?)
            as Arc<dyn ExecutionPlan>;
        let sort_exprs = vec![PhysicalSortExpr {
            expr: col("nullable_col", schema.as_ref()).unwrap(),
            options: SortOptions::default(),
        }];
        let sort_exec = Arc::new(SortExec::try_new(sort_exprs.clone(), source, None)?)
            as Arc<dyn ExecutionPlan>;
        let sort_preserving_merge_exec =
            Arc::new(SortPreservingMergeExec::new(sort_exprs, sort_exec))
                as Arc<dyn ExecutionPlan>;
        let sort_exprs = vec![PhysicalSortExpr {
            expr: col("nullable_col", schema.as_ref()).unwrap(),
            options: SortOptions::default(),
        }];
        let sort_exec = Arc::new(SortExec::try_new(
            sort_exprs.clone(),
            sort_preserving_merge_exec,
            None,
        )?) as Arc<dyn ExecutionPlan>;
        let sort_preserving_merge_exec =
            Arc::new(SortPreservingMergeExec::new(sort_exprs, sort_exec))
                as Arc<dyn ExecutionPlan>;
        let physical_plan = sort_preserving_merge_exec;
        let formatted = displayable(physical_plan.as_ref()).indent().to_string();
        let expected = {
            vec![
                "SortPreservingMergeExec: [nullable_col@0 ASC]",
                "  SortExec: [nullable_col@0 ASC]",
                "    SortPreservingMergeExec: [nullable_col@0 ASC]",
                "      SortExec: [nullable_col@0 ASC]",
                "        MemoryExec: partitions=0, partition_sizes=[]",
            ]
        };
        let actual: Vec<&str> = formatted.trim().lines().collect();
        assert_eq!(
            expected, actual,
            "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
        );
        let optimized_physical_plan =
            OptimizeSorts::new().optimize(physical_plan, state.config_options())?;
        let formatted = displayable(optimized_physical_plan.as_ref())
            .indent()
            .to_string();
        let expected = {
            vec![
                "SortPreservingMergeExec: [nullable_col@0 ASC]",
                "  SortPreservingMergeExec: [nullable_col@0 ASC]",
                "    SortExec: [nullable_col@0 ASC]",
                "      MemoryExec: partitions=0, partition_sizes=[]",
            ]
        };
        let actual: Vec<&str> = formatted.trim().lines().collect();
        assert_eq!(
            expected, actual,
            "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_change_wrong_sorting() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let schema = create_test_schema()?;
        let source = Arc::new(MemoryExec::try_new(&[], schema.clone(), None)?)
            as Arc<dyn ExecutionPlan>;
        let sort_exprs = vec![
            PhysicalSortExpr {
                expr: col("nullable_col", schema.as_ref()).unwrap(),
                options: SortOptions::default(),
            },
            PhysicalSortExpr {
                expr: col("non_nullable_col", schema.as_ref()).unwrap(),
                options: SortOptions::default(),
            },
        ];
        let sort_exec = Arc::new(SortExec::try_new(
            vec![sort_exprs[0].clone()],
            source,
            None,
        )?) as Arc<dyn ExecutionPlan>;
        let sort_preserving_merge_exec =
            Arc::new(SortPreservingMergeExec::new(sort_exprs, sort_exec))
                as Arc<dyn ExecutionPlan>;
        let physical_plan = sort_preserving_merge_exec;
        let formatted = displayable(physical_plan.as_ref()).indent().to_string();
        let expected = {
            vec![
                "SortPreservingMergeExec: [nullable_col@0 ASC,non_nullable_col@1 ASC]",
                "  SortExec: [nullable_col@0 ASC]",
                "    MemoryExec: partitions=0, partition_sizes=[]",
            ]
        };
        let actual: Vec<&str> = formatted.trim().lines().collect();
        assert_eq!(
            expected, actual,
            "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
        );
        let optimized_physical_plan =
            OptimizeSorts::new().optimize(physical_plan, state.config_options())?;
        let formatted = displayable(optimized_physical_plan.as_ref())
            .indent()
            .to_string();
        let expected = {
            vec![
                "SortPreservingMergeExec: [nullable_col@0 ASC,non_nullable_col@1 ASC]",
                "  SortExec: [nullable_col@0 ASC,non_nullable_col@1 ASC]",
                "    MemoryExec: partitions=0, partition_sizes=[]",
            ]
        };
        let actual: Vec<&str> = formatted.trim().lines().collect();
        assert_eq!(
            expected, actual,
            "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
        );
        Ok(())
    }
}
