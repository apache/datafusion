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

//! SQL planning extensions like [`MatchRecognizeFunctionPlanner`]

use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::Result;
use datafusion_expr::{
    planner::{ExprPlanner, PlannerResult},
    Expr,
};

use crate::rules::{
    is_mr_symbol, rewrite_aggregate_expr, rewrite_mr_symbol, rewrite_nullary_scalar,
    rewrite_window_expr,
};

/// Planner that rewrites MATCH_RECOGNIZE-specific expressions into
/// executable equivalents used by the rest of DataFusion.
#[derive(Debug)]
pub struct MatchRecognizeFunctionPlanner;

impl ExprPlanner for MatchRecognizeFunctionPlanner {
    fn plan_match_recognize(
        &self,
        expr: Expr,
        aggregate_context: bool,
    ) -> Result<PlannerResult<Expr>> {
        let rewritten = Self::transform_match_recognize_expr(expr, aggregate_context)?;
        if rewritten.transformed {
            Ok(PlannerResult::Planned(rewritten.data))
        } else {
            Ok(PlannerResult::Original(rewritten.data))
        }
    }
}

impl MatchRecognizeFunctionPlanner {
    // Recursively transform MATCH_RECOGNIZE expressions to their execution equivalents
    fn transform_match_recognize_expr(
        e: Expr,
        aggregate_context: bool,
    ) -> Result<Transformed<Expr>> {
        match &e {
            // Rewrite nullary scalar calls to pass-through virtual metadata columns:
            // classifier() -> __mr_classifier, match_number() -> __mr_match_number, match_sequence_number() -> __mr_match_sequence_number
            Expr::ScalarFunction(f) if f.args.is_empty() => {
                match rewrite_nullary_scalar(f) {
                    Some(new_expr) => Ok(Transformed::yes(new_expr)),
                    None => Ok(Transformed::no(e)),
                }
            }
            // Bare symbol column: represented as mr_symbol(col, 'SYM').
            // Rewrite to last_value(col) with FILTER (__mr_classifier = 'SYM') and
            // ORDER BY __mr_match_sequence_number ASC NULLS LAST; drop the mr_symbol wrapper.
            Expr::ScalarFunction(f) if is_mr_symbol(f) => {
                let new_expr = rewrite_mr_symbol(f, aggregate_context)?;
                Ok(Transformed::yes(new_expr))
            }

            Expr::WindowFunction(wf) => match rewrite_window_expr(wf)? {
                Some(new_expr) => Ok(Transformed::yes(new_expr)),
                None => Ok(Transformed::no(e)),
            },

            Expr::AggregateFunction(af) => match rewrite_aggregate_expr(af)? {
                Some(new_expr) => Ok(Transformed::yes(new_expr)),
                None => Ok(Transformed::no(e)),
            },

            _ => e.map_children(|child| {
                Self::transform_match_recognize_expr(child, aggregate_context)
            }),
        }
    }
}
