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
        expr.as_any().downcast_ref::<Column>().map(|c| c.index())
    }

    /// Get column statistics for an expression if it's a column reference
    fn get_column_stats<'a>(
        expr: &Arc<dyn PhysicalExpr>,
        input_stats: &'a Statistics,
    ) -> Option<&'a ColumnStatistics> {
        Self::get_column_index(expr)
            .and_then(|idx| input_stats.column_statistics.get(idx))
    }

    /// Resolve NDV for a binary expression: try direct column stats first,
    /// then fall back to the registry for arbitrary expressions
    fn resolve_ndv(
        left: &Arc<dyn PhysicalExpr>,
        right: &Arc<dyn PhysicalExpr>,
        input_stats: &Statistics,
        registry: &ExpressionAnalyzerRegistry,
    ) -> Option<usize> {
        Self::get_column_stats(left, input_stats)
            .or_else(|| Self::get_column_stats(right, input_stats))
            .and_then(|s| s.distinct_count.get_value())
            .filter(|&&ndv| ndv > 0)
            .copied()
            .or_else(|| {
                let l = registry.get_distinct_count(left, input_stats);
                let r = registry.get_distinct_count(right, input_stats);
                l.max(r)
            })
            .filter(|&n| n > 0)
    }

    /// Recursive selectivity estimation through the registry chain
    fn estimate_selectivity_recursive(
        &self,
        expr: &Arc<dyn PhysicalExpr>,
        input_stats: &Statistics,
        registry: &ExpressionAnalyzerRegistry,
    ) -> f64 {
        registry.get_selectivity(expr, input_stats).unwrap_or(0.5)
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
        if let Some(binary) = expr.as_any().downcast_ref::<BinaryExpr>() {
            let sel = match binary.op() {
                // Logical operators: need child selectivities
                Operator::And => {
                    let left_sel = self.estimate_selectivity_recursive(
                        binary.left(),
                        input_stats,
                        registry,
                    );
                    let right_sel = self.estimate_selectivity_recursive(
                        binary.right(),
                        input_stats,
                        registry,
                    );
                    left_sel * right_sel
                }
                Operator::Or => {
                    let left_sel = self.estimate_selectivity_recursive(
                        binary.left(),
                        input_stats,
                        registry,
                    );
                    let right_sel = self.estimate_selectivity_recursive(
                        binary.right(),
                        input_stats,
                        registry,
                    );
                    left_sel + right_sel - (left_sel * right_sel)
                }

                // Equality: selectivity = 1/NDV
                Operator::Eq => {
                    if let Some(ndv) = Self::resolve_ndv(
                        binary.left(),
                        binary.right(),
                        input_stats,
                        registry,
                    ) {
                        return AnalysisResult::Computed(1.0 / (ndv as f64));
                    }
                    0.1 // Default equality selectivity
                }

                // Inequality: selectivity = 1 - 1/NDV
                Operator::NotEq => {
                    if let Some(ndv) = Self::resolve_ndv(
                        binary.left(),
                        binary.right(),
                        input_stats,
                        registry,
                    ) {
                        return AnalysisResult::Computed(1.0 - (1.0 / (ndv as f64)));
                    }
                    0.9
                }

                // Range predicates: classic 1/3 estimate
                Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq => 0.33,

                // LIKE: depends on pattern, use conservative estimate
                Operator::LikeMatch | Operator::ILikeMatch => 0.25,
                Operator::NotLikeMatch | Operator::NotILikeMatch => 0.75,

                // Other operators: default
                _ => 0.5,
            };

            return AnalysisResult::Computed(sel);
        }

        // NOT expression: 1 - child selectivity
        if let Some(not_expr) = expr.as_any().downcast_ref::<NotExpr>() {
            let child_sel = self.estimate_selectivity_recursive(
                not_expr.arg(),
                input_stats,
                registry,
            );
            return AnalysisResult::Computed(1.0 - child_sel);
        }

        // Literal boolean: 0.0 or 1.0
        if let Some(b) = expr
            .as_any()
            .downcast_ref::<Literal>()
            .and_then(|lit| match lit.value() {
                ScalarValue::Boolean(Some(b)) => Some(*b),
                _ => None,
            })
        {
            return AnalysisResult::Computed(if b { 1.0 } else { 0.0 });
        }

        // Column reference as predicate (boolean column)
        if expr.as_any().downcast_ref::<Column>().is_some() {
            return AnalysisResult::Computed(0.5);
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
        if expr.as_any().downcast_ref::<Literal>().is_some() {
            return AnalysisResult::Computed(1);
        }

        // BinaryExpr: for arithmetic with a literal operand, treat as injective
        // (preserves NDV). This is an approximation: col * 0 or col % 1 are
        // technically not injective, but the common case (col + 1, col * 2, etc.) is
        if let Some(binary) = expr.as_any().downcast_ref::<BinaryExpr>() {
            let is_arithmetic = matches!(
                binary.op(),
                Operator::Plus
                    | Operator::Minus
                    | Operator::Multiply
                    | Operator::Divide
                    | Operator::Modulo
            );

            if is_arithmetic {
                // If one side is a literal, the operation is injective on the other side
                let left_is_literal = binary.left().as_any().is::<Literal>();
                let right_is_literal = binary.right().as_any().is::<Literal>();

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
        if let Some(lit_expr) = expr.as_any().downcast_ref::<Literal>() {
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
        if let Some(lit_expr) = expr.as_any().downcast_ref::<Literal>() {
            let is_null = lit_expr.value().is_null();
            return AnalysisResult::Computed(if is_null { 1.0 } else { 0.0 });
        }

        AnalysisResult::Delegate
    }
}
