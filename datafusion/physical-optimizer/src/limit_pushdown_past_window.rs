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
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::ScalarValue;
use datafusion_expr::{WindowFrameBound, WindowFrameUnits};
use datafusion_physical_plan::execution_plan::CardinalityEffect;
use datafusion_physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use datafusion_physical_plan::sorts::sort::SortExec;
use datafusion_physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion_physical_plan::windows::BoundedWindowAggExec;
use datafusion_physical_plan::ExecutionPlan;
use std::cmp;
use std::sync::Arc;

/// This rule inspects [`ExecutionPlan`]'s attempting to find fetch limits that were not pushed
/// down by `LimitPushdown` because [BoundedWindowAggExec]s were "in the way". If the window is
/// bounded by [WindowFrameUnits::Rows] then we calculate the adjustment needed to grow the limit
/// and continue pushdown.
#[derive(Default, Clone, Debug)]
pub struct LimitPushPastWindows;

impl LimitPushPastWindows {
    pub fn new() -> Self {
        Self
    }
}

impl PhysicalOptimizerRule for LimitPushPastWindows {
    fn optimize(
        &self,
        original: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        if !config.optimizer.enable_window_limits {
            return Ok(original);
        }
        let mut latest_limit: Option<usize> = None;
        let mut latest_max = 0;
        let result = original.transform_down(|node| {
            // helper closure to DRY out most the early return cases
            let mut reset = |node,
                             max: &mut usize|
             -> datafusion_common::Result<
                Transformed<Arc<dyn ExecutionPlan>>,
            > {
                latest_limit = None;
                *max = 0;
                Ok(Transformed::no(node))
            };

            // traversing sides of joins will require more thought
            if node.children().len() > 1 {
                return reset(node, &mut latest_max);
            }

            // grab the latest limit we see
            if let Some(limit) = node.as_any().downcast_ref::<GlobalLimitExec>() {
                latest_limit = limit.fetch().map(|fetch| fetch + limit.skip());
                latest_max = 0;
                return Ok(Transformed::no(node));
            }

            // In distributed execution, GlobalLimitExec becomes LocalLimitExec
            // per partition. Handle it the same way (LocalLimitExec has no skip).
            if let Some(limit) = node.as_any().downcast_ref::<LocalLimitExec>() {
                latest_limit = Some(limit.fetch());
                latest_max = 0;
                return Ok(Transformed::no(node));
            }

            // grow the limit if we hit a window function
            if let Some(window) = node.as_any().downcast_ref::<BoundedWindowAggExec>() {
                for expr in window.window_expr().iter() {
                    let frame = expr.get_window_frame();
                    if frame.units != WindowFrameUnits::Rows {
                        return reset(node, &mut latest_max); // expression-based limits?
                    }
                    let Some(end_bound) = bound_to_usize(&frame.end_bound) else {
                        return reset(node, &mut latest_max);
                    };
                    latest_max = cmp::max(end_bound, latest_max);
                }
                return Ok(Transformed::no(node));
            }

            // Apply the limit if we hit a sortpreservingmerge node
            if let Some(spm) = node.as_any().downcast_ref::<SortPreservingMergeExec>() {
                let latest = latest_limit.take();
                let Some(fetch) = latest else {
                    latest_max = 0;
                    return Ok(Transformed::no(node));
                };
                let fetch = match spm.fetch() {
                    None => fetch + latest_max,
                    Some(existing) => cmp::min(existing, fetch + latest_max),
                };
                let spm: Arc<dyn ExecutionPlan> = spm.with_fetch(Some(fetch)).unwrap();
                latest_max = 0;
                return Ok(Transformed::complete(spm));
            }

            // Apply the limit if we hit a sort node
            if let Some(sort) = node.as_any().downcast_ref::<SortExec>() {
                let latest = latest_limit.take();
                let Some(fetch) = latest else {
                    latest_max = 0;
                    return Ok(Transformed::no(node));
                };
                let fetch = match sort.fetch() {
                    None => fetch + latest_max,
                    Some(existing) => cmp::min(existing, fetch + latest_max),
                };
                let sort: Arc<dyn ExecutionPlan> = Arc::new(sort.with_fetch(Some(fetch)));
                latest_max = 0;
                return Ok(Transformed::complete(sort));
            }

            // we can't push the limit past nodes that decrease row count
            match node.cardinality_effect() {
                CardinalityEffect::Equal => {}
                _ => return reset(node, &mut latest_max),
            }

            Ok(Transformed::no(node))
        })?;
        Ok(result.data)
    }

    fn name(&self) -> &str {
        "LimitPushPastWindows"
    }

    fn schema_check(&self) -> bool {
        false // we don't change the schema
    }
}

fn bound_to_usize(bound: &WindowFrameBound) -> Option<usize> {
    match bound {
        WindowFrameBound::Preceding(_) => Some(0),
        WindowFrameBound::CurrentRow => Some(0),
        WindowFrameBound::Following(ScalarValue::UInt64(Some(scalar))) => {
            Some(*scalar as usize)
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::ScalarValue;
    use datafusion_expr::{WindowFrame, WindowFrameBound, WindowFrameUnits};
    use datafusion_functions_window::row_number::row_number_udwf;
    use datafusion_physical_expr::expressions::col;
    use datafusion_physical_expr::window::StandardWindowExpr;
    use datafusion_physical_expr_common::sort_expr::{LexOrdering, PhysicalSortExpr};
    use datafusion_physical_plan::displayable;
    use datafusion_physical_plan::limit::LocalLimitExec;
    use datafusion_physical_plan::placeholder_row::PlaceholderRowExec;
    use datafusion_physical_plan::windows::{
        create_udwf_window_expr, BoundedWindowAggExec,
    };
    use datafusion_physical_plan::InputOrderMode;
    use std::sync::Arc;

    fn plan_str(plan: &dyn ExecutionPlan) -> String {
        displayable(plan).indent(true).to_string()
    }

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]))
    }

    /// Build: LocalLimitExec or GlobalLimitExec → BoundedWindowAggExec(row_number) → SortExec
    fn build_window_plan(
        use_local_limit: bool,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        let s = schema();
        let input: Arc<dyn ExecutionPlan> =
            Arc::new(PlaceholderRowExec::new(Arc::clone(&s)));

        let ordering =
            LexOrdering::new(vec![PhysicalSortExpr::new_default(col("a", &s)?).asc()]);

        let sort: Arc<dyn ExecutionPlan> = Arc::new(
            SortExec::new(ordering.clone(), input).with_preserve_partitioning(true),
        );

        let window_expr = Arc::new(StandardWindowExpr::new(
            create_udwf_window_expr(
                &row_number_udwf(),
                &[],
                &s,
                "row_number".to_string(),
                false,
            )?,
            &[],
            &ordering,
            Arc::new(WindowFrame::new_bounds(
                WindowFrameUnits::Rows,
                WindowFrameBound::Preceding(ScalarValue::UInt64(None)),
                WindowFrameBound::CurrentRow,
            )),
        ));

        let window: Arc<dyn ExecutionPlan> = Arc::new(BoundedWindowAggExec::try_new(
            vec![window_expr],
            sort,
            InputOrderMode::Sorted,
            true,
        )?);

        let limit: Arc<dyn ExecutionPlan> = if use_local_limit {
            Arc::new(LocalLimitExec::new(window, 100))
        } else {
            Arc::new(GlobalLimitExec::new(window, 0, Some(100)))
        };

        Ok(limit)
    }

    fn optimize(plan: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        let mut config = ConfigOptions::new();
        config.optimizer.enable_window_limits = true;
        LimitPushPastWindows::new().optimize(plan, &config).unwrap()
    }

    /// GlobalLimitExec above a windowed sort should push fetch into the SortExec.
    #[test]
    fn global_limit_pushes_past_window() {
        let plan = build_window_plan(false).unwrap();
        let optimized = optimize(plan);
        let display = plan_str(optimized.as_ref());
        assert_eq!(
            display.trim(),
            r#"
GlobalLimitExec: skip=0, fetch=100
  BoundedWindowAggExec: wdw=[row_number: Ok(Field { name: "row_number", data_type: UInt64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(NULL)), end_bound: CurrentRow, is_causal: true }], mode=[Sorted]
    SortExec: TopK(fetch=100), expr=[a@0 ASC], preserve_partitioning=[true]
      PlaceholderRowExec
"#
            .trim()
        );
    }

    /// LocalLimitExec above a windowed sort should also push fetch into the SortExec.
    /// This is the case in distributed execution where GlobalLimitExec becomes LocalLimitExec.
    #[test]
    fn local_limit_pushes_past_window() {
        let plan = build_window_plan(true).unwrap();
        let optimized = optimize(plan);
        let display = plan_str(optimized.as_ref());
        assert_eq!(
            display.trim(),
            r#"
LocalLimitExec: fetch=100
  BoundedWindowAggExec: wdw=[row_number: Ok(Field { name: "row_number", data_type: UInt64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(NULL)), end_bound: CurrentRow, is_causal: true }], mode=[Sorted]
    SortExec: TopK(fetch=100), expr=[a@0 ASC], preserve_partitioning=[true]
      PlaceholderRowExec
"#
            .trim()
        );
    }
}
