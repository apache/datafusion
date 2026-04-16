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

//! Default expression analyzer with Selinger-style estimation.

use std::sync::Arc;

use datafusion_common::{ColumnStatistics, ScalarValue, Statistics};
use datafusion_expr::Operator;

use crate::PhysicalExpr;
use crate::expressions::{BinaryExpr, Column, Literal, NotExpr};

use super::{AnalysisResult, ExpressionAnalyzer, ExpressionAnalyzerRegistry};

/// Default expression analyzer with Selinger-style estimation.
///
/// Handles common expression types:
/// - Column references (uses column statistics)
/// - Binary expressions (AND, OR, comparison operators)
/// - Literals (constant selectivity/NDV)
/// - NOT expressions (1 - child selectivity)
#[derive(Debug, Default, Clone)]
pub struct DefaultExpressionAnalyzer;

impl DefaultExpressionAnalyzer {
    /// Get column index from a Column expression
    fn get_column_index(expr: &Arc<dyn PhysicalExpr>) -> Option<usize> {
        expr.downcast_ref::<Column>().map(|c| c.index())
    }

    /// Get column statistics for an expression if it's a column reference
    fn get_column_stats<'a>(
        expr: &Arc<dyn PhysicalExpr>,
        input_stats: &'a Statistics,
    ) -> Option<&'a ColumnStatistics> {
        Self::get_column_index(expr)
            .and_then(|idx| input_stats.column_statistics.get(idx))
    }

    /// Resolve NDV for a binary predicate by taking the max of both sides.
    ///
    /// Using max is symmetric (order-independent) and handles column-vs-column,
    /// column-vs-expression, and expression-vs-expression uniformly through the
    /// registry chain. Returns `None` when either side has unknown NDV.
    fn resolve_ndv(
        left: &Arc<dyn PhysicalExpr>,
        right: &Arc<dyn PhysicalExpr>,
        input_stats: &Statistics,
        registry: &ExpressionAnalyzerRegistry,
    ) -> Option<usize> {
        let l = registry.get_distinct_count(left, input_stats);
        let r = registry.get_distinct_count(right, input_stats);
        match (l, r) {
            (Some(a), Some(b)) => Some(a.max(b)).filter(|&n| n > 0),
            _ => None,
        }
    }
}

impl ExpressionAnalyzer for DefaultExpressionAnalyzer {
    fn get_selectivity(
        &self,
        expr: &Arc<dyn PhysicalExpr>,
        input_stats: &Statistics,
        registry: &ExpressionAnalyzerRegistry,
    ) -> AnalysisResult<f64> {
        // Binary expressions: AND, OR, comparisons
        if let Some(binary) = expr.downcast_ref::<BinaryExpr>() {
            match binary.op() {
                // AND/OR: only provide a value when both children have estimates.
                // Delegating when either child has no information prevents arbitrary
                // constants from contaminating the product/inclusion-exclusion formula.
                Operator::And | Operator::Or => {
                    if let (Some(left), Some(right)) = (
                        registry.get_selectivity(binary.left(), input_stats),
                        registry.get_selectivity(binary.right(), input_stats),
                    ) {
                        let sel = match binary.op() {
                            Operator::And => left * right,
                            Operator::Or => left + right - left * right,
                            _ => unreachable!(),
                        };
                        return AnalysisResult::Computed(sel);
                    }
                }

                // Equality: 1/NDV. Delegate when NDV is unknown so default_selectivity applies.
                Operator::Eq => {
                    if let Some(ndv) = Self::resolve_ndv(
                        binary.left(),
                        binary.right(),
                        input_stats,
                        registry,
                    ) {
                        return AnalysisResult::Computed(1.0 / (ndv as f64));
                    }
                }

                // Inequality: 1 - 1/NDV. Delegate when NDV is unknown.
                Operator::NotEq => {
                    if let Some(ndv) = Self::resolve_ndv(
                        binary.left(),
                        binary.right(),
                        input_stats,
                        registry,
                    ) {
                        return AnalysisResult::Computed(1.0 - (1.0 / (ndv as f64)));
                    }
                }

                // All other operators: no statistics available, let caller decide.
                _ => {}
            }

            return AnalysisResult::Delegate;
        }

        // NOT expression: 1 - child selectivity. Delegate if child has no estimate.
        if let Some(not_expr) = expr.downcast_ref::<NotExpr>() {
            if let Some(child_sel) = registry.get_selectivity(not_expr.arg(), input_stats)
            {
                return AnalysisResult::Computed(1.0 - child_sel);
            }
            return AnalysisResult::Delegate;
        }

        // Literal boolean: exact selectivity, no statistics needed.
        if let Some(b) =
            expr.downcast_ref::<Literal>()
                .and_then(|lit| match lit.value() {
                    ScalarValue::Boolean(Some(b)) => Some(*b),
                    _ => None,
                })
        {
            return AnalysisResult::Computed(if b { 1.0 } else { 0.0 });
        }

        AnalysisResult::Delegate
    }

    fn get_distinct_count(
        &self,
        expr: &Arc<dyn PhysicalExpr>,
        input_stats: &Statistics,
        registry: &ExpressionAnalyzerRegistry,
    ) -> AnalysisResult<usize> {
        // Column reference: use column NDV
        if let Some(ndv) = Self::get_column_stats(expr, input_stats)
            .and_then(|col_stats| col_stats.distinct_count.get_value().copied())
        {
            return AnalysisResult::Computed(ndv);
        }

        // Literal: NDV = 1
        if expr.downcast_ref::<Literal>().is_some() {
            return AnalysisResult::Computed(1);
        }

        // BinaryExpr: addition/subtraction with a literal is always injective
        // TODO: support more injective operators (e.g. multiply by non-zero)
        if let Some(binary) = expr.downcast_ref::<BinaryExpr>() {
            let is_injective = matches!(binary.op(), Operator::Plus | Operator::Minus);

            if is_injective {
                // If one side is a literal, the operation is injective on the other side
                let left_is_literal = binary.left().is::<Literal>();
                let right_is_literal = binary.right().is::<Literal>();

                if left_is_literal
                    && let Some(ndv) =
                        registry.get_distinct_count(binary.right(), input_stats)
                {
                    return AnalysisResult::Computed(ndv);
                } else if right_is_literal
                    && let Some(ndv) =
                        registry.get_distinct_count(binary.left(), input_stats)
                {
                    return AnalysisResult::Computed(ndv);
                }
                // Both sides are non-literals: could combine, but delegate for now
            }
        }

        AnalysisResult::Delegate
    }

    fn get_min_max(
        &self,
        expr: &Arc<dyn PhysicalExpr>,
        input_stats: &Statistics,
        _registry: &ExpressionAnalyzerRegistry,
    ) -> AnalysisResult<(ScalarValue, ScalarValue)> {
        // Column reference: use column min/max
        if let Some((min, max)) =
            Self::get_column_stats(expr, input_stats).and_then(|col_stats| {
                match (
                    col_stats.min_value.get_value(),
                    col_stats.max_value.get_value(),
                ) {
                    (Some(min), Some(max)) => Some((min.clone(), max.clone())),
                    _ => None,
                }
            })
        {
            return AnalysisResult::Computed((min, max));
        }

        // Literal: min = max = value
        if let Some(lit_expr) = expr.downcast_ref::<Literal>() {
            let val = lit_expr.value().clone();
            return AnalysisResult::Computed((val.clone(), val));
        }

        AnalysisResult::Delegate
    }

    fn get_null_fraction(
        &self,
        expr: &Arc<dyn PhysicalExpr>,
        input_stats: &Statistics,
        _registry: &ExpressionAnalyzerRegistry,
    ) -> AnalysisResult<f64> {
        // Column reference: null_count / num_rows
        if let Some(fraction) =
            Self::get_column_stats(expr, input_stats).and_then(|col_stats| {
                let null_count = col_stats.null_count.get_value().copied()?;
                let num_rows = input_stats.num_rows.get_value().copied()?;
                if num_rows > 0 {
                    Some(null_count as f64 / num_rows as f64)
                } else {
                    None
                }
            })
        {
            return AnalysisResult::Computed(fraction);
        }

        // Literal: null fraction depends on whether it's null
        if let Some(lit_expr) = expr.downcast_ref::<Literal>() {
            let is_null = lit_expr.value().is_null();
            return AnalysisResult::Computed(if is_null { 1.0 } else { 0.0 });
        }

        AnalysisResult::Delegate
    }
}
