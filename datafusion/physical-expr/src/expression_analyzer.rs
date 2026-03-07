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

//! Pluggable expression-level statistics analysis.
//!
//! This module provides an extensible mechanism for computing expression-level
//! statistics metadata (selectivity, NDV, min/max bounds) following the chain
//! of responsibility pattern.
//!
//! # Overview
//!
//! Different expressions have different statistical properties:
//!
//! - **Injective functions** (UPPER, LOWER, ABS on non-negative): preserve NDV
//! - **Non-injective functions** (FLOOR, YEAR, SUBSTRING): reduce NDV
//! - **Monotonic functions**: allow min/max bound propagation
//! - **Constants**: NDV = 1, selectivity depends on value
//!
//! The default implementation uses classic Selinger-style estimation. Users can
//! register custom [`ExpressionAnalyzer`] implementations to:
//!
//! 1. Provide statistics for custom UDFs
//! 2. Override default estimation with domain-specific knowledge
//! 3. Plug in advanced approaches (e.g., histogram-based estimation)
//!
//! # Example
//!
//! ```ignore
//! use datafusion_physical_plan::expression_analyzer::*;
//!
//! // Create registry with default analyzer
//! let mut registry = ExpressionAnalyzerRegistry::new();
//!
//! // Register custom analyzer (higher priority)
//! registry.register(Arc::new(MyCustomAnalyzer));
//!
//! // Query expression statistics
//! let selectivity = registry.get_selectivity(&predicate, &input_stats);
//! ```

use std::fmt::Debug;
use std::sync::Arc;

use datafusion_common::{ColumnStatistics, ScalarValue, Statistics};
use datafusion_expr::Operator;

use crate::expressions::{BinaryExpr, Column, Literal, NotExpr};
use crate::{PhysicalExpr, ScalarFunctionExpr};

// ============================================================================
// AnalysisResult: Chain of responsibility result type
// ============================================================================

/// Result of expression analysis - either computed or delegate to next analyzer.
#[derive(Debug, Clone)]
pub enum AnalysisResult<T> {
    /// Analysis was performed, here's the result
    Computed(T),
    /// This analyzer doesn't handle this expression; delegate to next
    Delegate,
}

impl<T> AnalysisResult<T> {
    /// Convert to Option, returning None for Delegate
    pub fn into_option(self) -> Option<T> {
        match self {
            AnalysisResult::Computed(v) => Some(v),
            AnalysisResult::Delegate => None,
        }
    }

    /// Returns true if this is a Computed result
    pub fn is_computed(&self) -> bool {
        matches!(self, AnalysisResult::Computed(_))
    }
}

// ============================================================================
// ExpressionAnalyzer trait
// ============================================================================

/// Expression-level metadata analysis.
///
/// Implementations can handle specific expression types or provide domain
/// knowledge for custom UDFs. The chain of analyzers is traversed until one
/// returns [`AnalysisResult::Computed`].
///
/// # Implementing a Custom Analyzer
///
/// ```ignore
/// #[derive(Debug)]
/// struct MyUdfAnalyzer;
///
/// impl ExpressionAnalyzer for MyUdfAnalyzer {
///     fn get_selectivity(
///         &self,
///         expr: &Arc<dyn PhysicalExpr>,
///         input_stats: &Statistics,
///     ) -> AnalysisResult<f64> {
///         // Recognize my custom is_valid_email() UDF
///         if is_my_email_validator(expr) {
///             return AnalysisResult::Computed(0.8); // ~80% valid
///         }
///         AnalysisResult::Delegate
///     }
/// }
/// ```
pub trait ExpressionAnalyzer: Debug + Send + Sync {
    /// Estimate selectivity when this expression is used as a predicate.
    ///
    /// Returns a value in [0.0, 1.0] representing the fraction of rows
    /// that satisfy the predicate.
    fn get_selectivity(
        &self,
        _expr: &Arc<dyn PhysicalExpr>,
        _input_stats: &Statistics,
    ) -> AnalysisResult<f64> {
        AnalysisResult::Delegate
    }

    /// Estimate the number of distinct values in the expression's output.
    ///
    /// Properties:
    /// - Injective functions preserve input NDV
    /// - Non-injective functions reduce NDV (e.g., FLOOR, YEAR)
    /// - Constants have NDV = 1
    fn get_distinct_count(
        &self,
        _expr: &Arc<dyn PhysicalExpr>,
        _input_stats: &Statistics,
    ) -> AnalysisResult<usize> {
        AnalysisResult::Delegate
    }

    /// Estimate min/max bounds of the expression's output.
    ///
    /// Monotonic functions can transform input bounds:
    /// - Increasing: (f(min), f(max))
    /// - Decreasing: (f(max), f(min))
    /// - Non-monotonic: may need wider bounds or return Delegate
    fn get_min_max(
        &self,
        _expr: &Arc<dyn PhysicalExpr>,
        _input_stats: &Statistics,
    ) -> AnalysisResult<(ScalarValue, ScalarValue)> {
        AnalysisResult::Delegate
    }

    /// Estimate the fraction of null values in the expression's output.
    ///
    /// Returns a value in [0.0, 1.0].
    fn get_null_fraction(
        &self,
        _expr: &Arc<dyn PhysicalExpr>,
        _input_stats: &Statistics,
    ) -> AnalysisResult<f64> {
        AnalysisResult::Delegate
    }
}

// ============================================================================
// ExpressionAnalyzerRegistry
// ============================================================================

/// Registry that chains [`ExpressionAnalyzer`] implementations.
///
/// Analyzers are tried in order; the first to return [`AnalysisResult::Computed`]
/// wins. Register domain-specific analyzers before the default for override.
#[derive(Debug, Clone)]
pub struct ExpressionAnalyzerRegistry {
    analyzers: Vec<Arc<dyn ExpressionAnalyzer>>,
}

impl Default for ExpressionAnalyzerRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl ExpressionAnalyzerRegistry {
    /// Create a new registry with the default expression analyzer.
    pub fn new() -> Self {
        Self {
            analyzers: vec![Arc::new(DefaultExpressionAnalyzer)],
        }
    }

    /// Create a registry with all built-in analyzers (string, math, datetime, default).
    pub fn with_builtin_analyzers() -> Self {
        Self {
            analyzers: vec![
                Arc::new(StringFunctionAnalyzer),
                Arc::new(MathFunctionAnalyzer),
                Arc::new(DateTimeFunctionAnalyzer),
                Arc::new(DefaultExpressionAnalyzer),
            ],
        }
    }

    /// Create a registry with custom analyzers (no default).
    pub fn with_analyzers(analyzers: Vec<Arc<dyn ExpressionAnalyzer>>) -> Self {
        Self { analyzers }
    }

    /// Create a registry with custom analyzers plus default as fallback.
    pub fn with_analyzers_and_default(
        analyzers: impl IntoIterator<Item = Arc<dyn ExpressionAnalyzer>>,
    ) -> Self {
        let mut all: Vec<Arc<dyn ExpressionAnalyzer>> = analyzers.into_iter().collect();
        all.push(Arc::new(DefaultExpressionAnalyzer));
        Self { analyzers: all }
    }

    /// Register an analyzer at the front of the chain (higher priority).
    pub fn register(&mut self, analyzer: Arc<dyn ExpressionAnalyzer>) {
        self.analyzers.insert(0, analyzer);
    }

    /// Get selectivity through the analyzer chain.
    pub fn get_selectivity(
        &self,
        expr: &Arc<dyn PhysicalExpr>,
        input_stats: &Statistics,
    ) -> Option<f64> {
        for analyzer in &self.analyzers {
            if let AnalysisResult::Computed(sel) =
                analyzer.get_selectivity(expr, input_stats)
            {
                return Some(sel);
            }
        }
        None
    }

    /// Get distinct count through the analyzer chain.
    pub fn get_distinct_count(
        &self,
        expr: &Arc<dyn PhysicalExpr>,
        input_stats: &Statistics,
    ) -> Option<usize> {
        for analyzer in &self.analyzers {
            if let AnalysisResult::Computed(ndv) =
                analyzer.get_distinct_count(expr, input_stats)
            {
                return Some(ndv);
            }
        }
        None
    }

    /// Get min/max bounds through the analyzer chain.
    pub fn get_min_max(
        &self,
        expr: &Arc<dyn PhysicalExpr>,
        input_stats: &Statistics,
    ) -> Option<(ScalarValue, ScalarValue)> {
        for analyzer in &self.analyzers {
            if let AnalysisResult::Computed(bounds) =
                analyzer.get_min_max(expr, input_stats)
            {
                return Some(bounds);
            }
        }
        None
    }

    /// Get null fraction through the analyzer chain.
    pub fn get_null_fraction(
        &self,
        expr: &Arc<dyn PhysicalExpr>,
        input_stats: &Statistics,
    ) -> Option<f64> {
        for analyzer in &self.analyzers {
            if let AnalysisResult::Computed(frac) =
                analyzer.get_null_fraction(expr, input_stats)
            {
                return Some(frac);
            }
        }
        None
    }
}

// ============================================================================
// DefaultExpressionAnalyzer
// ============================================================================

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

    /// Recursive selectivity estimation
    fn estimate_selectivity_recursive(
        &self,
        expr: &Arc<dyn PhysicalExpr>,
        input_stats: &Statistics,
    ) -> f64 {
        if let AnalysisResult::Computed(sel) = self.get_selectivity(expr, input_stats) {
            return sel;
        }
        0.5 // Default fallback
    }
}

impl ExpressionAnalyzer for DefaultExpressionAnalyzer {
    fn get_selectivity(
        &self,
        expr: &Arc<dyn PhysicalExpr>,
        input_stats: &Statistics,
    ) -> AnalysisResult<f64> {
        // Binary expressions: AND, OR, comparisons
        if let Some(binary) = expr.as_any().downcast_ref::<BinaryExpr>() {
            let left_sel =
                self.estimate_selectivity_recursive(binary.left(), input_stats);
            let right_sel =
                self.estimate_selectivity_recursive(binary.right(), input_stats);

            let sel = match binary.op() {
                // Logical operators
                Operator::And => left_sel * right_sel,
                Operator::Or => left_sel + right_sel - (left_sel * right_sel),

                // Equality: selectivity = 1/NDV
                Operator::Eq => {
                    if let Some(ndv) = Self::get_column_stats(binary.left(), input_stats)
                        .and_then(|s| s.distinct_count.get_value())
                        .filter(|&&ndv| ndv > 0)
                    {
                        return AnalysisResult::Computed(1.0 / (*ndv as f64));
                    }
                    0.1 // Default equality selectivity
                }

                // Inequality: selectivity = 1 - 1/NDV
                Operator::NotEq => {
                    if let Some(ndv) = Self::get_column_stats(binary.left(), input_stats)
                        .and_then(|s| s.distinct_count.get_value())
                        .filter(|&&ndv| ndv > 0)
                    {
                        return AnalysisResult::Computed(1.0 - (1.0 / (*ndv as f64)));
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
            let child_sel =
                self.estimate_selectivity_recursive(not_expr.arg(), input_stats);
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

        // BinaryExpr: for injective operations (arithmetic with literal), preserve NDV
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

                if left_is_literal {
                    // NDV comes from right side
                    if let AnalysisResult::Computed(ndv) =
                        self.get_distinct_count(binary.right(), input_stats)
                    {
                        return AnalysisResult::Computed(ndv);
                    }
                } else if right_is_literal {
                    // NDV comes from left side
                    if let AnalysisResult::Computed(ndv) =
                        self.get_distinct_count(binary.left(), input_stats)
                    {
                        return AnalysisResult::Computed(ndv);
                    }
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

// ============================================================================
// StringFunctionAnalyzer
// ============================================================================

/// Analyzer for string functions.
///
/// - Injective (preserve NDV): UPPER, LOWER, TRIM, LTRIM, RTRIM, REVERSE
/// - Non-injective (reduce NDV): SUBSTRING, LEFT, RIGHT, REPLACE
#[derive(Debug, Default, Clone)]
pub struct StringFunctionAnalyzer;

impl StringFunctionAnalyzer {
    /// Check if a function is injective (one-to-one)
    pub fn is_injective(func_name: &str) -> bool {
        matches!(
            func_name.to_uppercase().as_str(),
            "UPPER" | "LOWER" | "TRIM" | "LTRIM" | "RTRIM" | "REVERSE" | "INITCAP"
        )
    }

    /// Get NDV reduction factor for non-injective functions
    pub fn ndv_reduction_factor(func_name: &str) -> Option<f64> {
        match func_name.to_uppercase().as_str() {
            "SUBSTRING" | "LEFT" | "RIGHT" => Some(0.5),
            "REPLACE" => Some(0.8),
            _ => None,
        }
    }
}

impl ExpressionAnalyzer for StringFunctionAnalyzer {
    fn get_distinct_count(
        &self,
        expr: &Arc<dyn PhysicalExpr>,
        input_stats: &Statistics,
    ) -> AnalysisResult<usize> {
        let Some(func) = expr.as_any().downcast_ref::<ScalarFunctionExpr>() else {
            return AnalysisResult::Delegate;
        };

        let func_name = func.name();
        let Some(first_arg) = func.args().first() else {
            return AnalysisResult::Delegate;
        };

        // Get input NDV
        let Some(input_ndv) = DefaultExpressionAnalyzer
            .get_distinct_count(first_arg, input_stats)
            .into_option()
        else {
            return AnalysisResult::Delegate;
        };

        // Injective functions preserve NDV
        if Self::is_injective(func_name) {
            return AnalysisResult::Computed(input_ndv);
        }

        // Non-injective functions reduce NDV
        if let Some(factor) = Self::ndv_reduction_factor(func_name) {
            let reduced = ((input_ndv as f64) * factor).ceil() as usize;
            return AnalysisResult::Computed(reduced.max(1));
        }

        AnalysisResult::Delegate
    }
}

// ============================================================================
// MathFunctionAnalyzer
// ============================================================================

/// Analyzer for mathematical functions.
///
/// - Injective on domain: ABS (on non-negative), SQRT (on non-negative)
/// - Non-injective: FLOOR, CEIL, ROUND, SIGN
/// - Monotonic: EXP, LN, LOG
#[derive(Debug, Default, Clone)]
pub struct MathFunctionAnalyzer;

impl MathFunctionAnalyzer {
    /// Check if function is injective (preserves NDV)
    pub fn is_injective(func_name: &str) -> bool {
        matches!(
            func_name.to_uppercase().as_str(),
            "EXP" | "LN" | "LOG" | "LOG2" | "LOG10"
        )
    }

    /// Get NDV reduction factor for non-injective functions
    pub fn ndv_reduction_factor(func_name: &str) -> Option<f64> {
        match func_name.to_uppercase().as_str() {
            "FLOOR" | "CEIL" | "ROUND" | "TRUNC" => Some(0.1),
            "SIGN" => Some(0.01), // Only -1, 0, 1
            "ABS" => Some(0.5),   // Roughly halves NDV
            _ => None,
        }
    }
}

impl ExpressionAnalyzer for MathFunctionAnalyzer {
    fn get_distinct_count(
        &self,
        expr: &Arc<dyn PhysicalExpr>,
        input_stats: &Statistics,
    ) -> AnalysisResult<usize> {
        let Some(func) = expr.as_any().downcast_ref::<ScalarFunctionExpr>() else {
            return AnalysisResult::Delegate;
        };

        let func_name = func.name();
        let Some(first_arg) = func.args().first() else {
            return AnalysisResult::Delegate;
        };

        let Some(input_ndv) = DefaultExpressionAnalyzer
            .get_distinct_count(first_arg, input_stats)
            .into_option()
        else {
            return AnalysisResult::Delegate;
        };

        // Injective functions preserve NDV
        if Self::is_injective(func_name) {
            return AnalysisResult::Computed(input_ndv);
        }

        // Non-injective functions reduce NDV
        if let Some(factor) = Self::ndv_reduction_factor(func_name) {
            let reduced = ((input_ndv as f64) * factor).ceil() as usize;
            return AnalysisResult::Computed(reduced.max(1));
        }

        AnalysisResult::Delegate
    }
}

// ============================================================================
// DateTimeFunctionAnalyzer
// ============================================================================

/// Analyzer for date/time functions.
///
/// - Non-injective with known bounds: YEAR, MONTH, DAY, HOUR, etc.
/// - These extract components with limited cardinality
#[derive(Debug, Default, Clone)]
pub struct DateTimeFunctionAnalyzer;

impl DateTimeFunctionAnalyzer {
    /// Get maximum possible NDV for date/time extraction functions
    pub fn max_ndv(func_name: &str) -> Option<usize> {
        match func_name.to_uppercase().as_str() {
            "YEAR" | "EXTRACT_YEAR" => None, // Unbounded, but typically < input NDV
            "MONTH" | "EXTRACT_MONTH" => Some(12),
            "DAY" | "EXTRACT_DAY" | "DAY_OF_MONTH" => Some(31),
            "HOUR" | "EXTRACT_HOUR" => Some(24),
            "MINUTE" | "EXTRACT_MINUTE" => Some(60),
            "SECOND" | "EXTRACT_SECOND" => Some(60),
            "DAYOFWEEK" | "DOW" | "EXTRACT_DOW" => Some(7),
            "DAYOFYEAR" | "DOY" | "EXTRACT_DOY" => Some(366),
            "WEEK" | "EXTRACT_WEEK" => Some(53),
            "QUARTER" | "EXTRACT_QUARTER" => Some(4),
            _ => None,
        }
    }
}

impl ExpressionAnalyzer for DateTimeFunctionAnalyzer {
    fn get_distinct_count(
        &self,
        expr: &Arc<dyn PhysicalExpr>,
        input_stats: &Statistics,
    ) -> AnalysisResult<usize> {
        let Some(func) = expr.as_any().downcast_ref::<ScalarFunctionExpr>() else {
            return AnalysisResult::Delegate;
        };

        let func_name = func.name();
        let Some(first_arg) = func.args().first() else {
            return AnalysisResult::Delegate;
        };

        // Get max possible NDV for this function
        let Some(max_ndv) = Self::max_ndv(func_name) else {
            return AnalysisResult::Delegate;
        };

        // Get input NDV if available
        let input_ndv = DefaultExpressionAnalyzer
            .get_distinct_count(first_arg, input_stats)
            .into_option();

        // NDV is min(input_ndv, max_possible)
        let result_ndv = match input_ndv {
            Some(ndv) => ndv.min(max_ndv),
            None => max_ndv,
        };

        AnalysisResult::Computed(result_ndv)
    }
}

// ============================================================================
// Utility functions for filter statistics
// ============================================================================

/// Estimate selectivity for a filter predicate.
///
/// This is a convenience function that uses the default analyzer chain.
/// For custom analysis, use [`ExpressionAnalyzerRegistry`] directly.
pub fn estimate_filter_selectivity(
    predicate: &Arc<dyn PhysicalExpr>,
    input_stats: &Statistics,
) -> f64 {
    ExpressionAnalyzerRegistry::with_builtin_analyzers()
        .get_selectivity(predicate, input_stats)
        .unwrap_or(0.5)
}

/// Estimate NDV after applying a filter with given selectivity.
///
/// Uses the formula: NDV_after = NDV_before * (1 - (1 - selectivity)^(num_rows / NDV_before))
///
/// This models the probability that at least one row with each distinct value survives.
pub fn ndv_after_selectivity(
    original_ndv: usize,
    original_rows: usize,
    selectivity: f64,
) -> usize {
    if original_ndv == 0 || original_rows == 0 || selectivity <= 0.0 {
        return 0;
    }
    if selectivity >= 1.0 {
        return original_ndv;
    }

    // Average rows per distinct value
    let rows_per_value = original_rows as f64 / original_ndv as f64;

    // Probability that all rows for a value are filtered out
    let prob_all_filtered = (1.0 - selectivity).powf(rows_per_value);

    // Expected number of distinct values remaining
    let expected_ndv = (original_ndv as f64) * (1.0 - prob_all_filtered);

    expected_ndv.ceil() as usize
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion_common::stats::Precision;
    use std::sync::Arc;

    fn make_stats_with_ndv(num_rows: usize, ndv: usize) -> Statistics {
        Statistics {
            num_rows: Precision::Exact(num_rows),
            total_byte_size: Precision::Absent,
            column_statistics: vec![ColumnStatistics {
                null_count: Precision::Exact(0),
                max_value: Precision::Absent,
                min_value: Precision::Absent,
                sum_value: Precision::Absent,
                distinct_count: Precision::Exact(ndv),
                byte_size: Precision::Absent,
            }],
        }
    }

    #[test]
    fn test_default_analyzer_column_ndv() {
        let stats = make_stats_with_ndv(1000, 100);
        let col = Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>;

        let analyzer = DefaultExpressionAnalyzer;
        let result = analyzer.get_distinct_count(&col, &stats);

        assert!(matches!(result, AnalysisResult::Computed(100)));
    }

    #[test]
    fn test_default_analyzer_literal_ndv() {
        let stats = make_stats_with_ndv(1000, 100);
        let lit =
            Arc::new(Literal::new(ScalarValue::Int32(Some(42)))) as Arc<dyn PhysicalExpr>;

        let analyzer = DefaultExpressionAnalyzer;
        let result = analyzer.get_distinct_count(&lit, &stats);

        assert!(matches!(result, AnalysisResult::Computed(1)));
    }

    #[test]
    fn test_default_analyzer_equality_selectivity() {
        let stats = make_stats_with_ndv(1000, 100);
        let col = Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>;
        let lit =
            Arc::new(Literal::new(ScalarValue::Int32(Some(42)))) as Arc<dyn PhysicalExpr>;
        let eq =
            Arc::new(BinaryExpr::new(col, Operator::Eq, lit)) as Arc<dyn PhysicalExpr>;

        let analyzer = DefaultExpressionAnalyzer;
        let result = analyzer.get_selectivity(&eq, &stats);

        // Selectivity should be 1/NDV = 1/100 = 0.01
        match result {
            AnalysisResult::Computed(sel) => {
                assert!((sel - 0.01).abs() < 0.001);
            }
            _ => panic!("Expected Computed result"),
        }
    }

    #[test]
    fn test_registry_chain() {
        let stats = make_stats_with_ndv(1000, 100);
        let col = Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>;

        let registry = ExpressionAnalyzerRegistry::with_builtin_analyzers();
        let ndv = registry.get_distinct_count(&col, &stats);

        assert_eq!(ndv, Some(100));
    }

    #[test]
    fn test_ndv_after_selectivity() {
        // 1000 rows, 100 NDV, 10% selectivity
        let result = ndv_after_selectivity(100, 1000, 0.1);
        // With 10 rows per value and 10% selectivity, most values should survive
        assert!(result > 50 && result <= 100);

        // 100% selectivity preserves NDV
        assert_eq!(ndv_after_selectivity(100, 1000, 1.0), 100);

        // 0% selectivity gives 0 NDV
        assert_eq!(ndv_after_selectivity(100, 1000, 0.0), 0);
    }

    #[test]
    fn test_datetime_function_analyzer() {
        // MONTH should have max NDV of 12
        assert_eq!(DateTimeFunctionAnalyzer::max_ndv("MONTH"), Some(12));
        assert_eq!(DateTimeFunctionAnalyzer::max_ndv("HOUR"), Some(24));
        assert_eq!(DateTimeFunctionAnalyzer::max_ndv("QUARTER"), Some(4));
    }

    #[test]
    fn test_string_function_analyzer() {
        assert!(StringFunctionAnalyzer::is_injective("UPPER"));
        assert!(StringFunctionAnalyzer::is_injective("lower"));
        assert!(!StringFunctionAnalyzer::is_injective("SUBSTRING"));

        assert_eq!(
            StringFunctionAnalyzer::ndv_reduction_factor("SUBSTRING"),
            Some(0.5)
        );
    }

    #[test]
    fn test_math_function_analyzer() {
        assert!(MathFunctionAnalyzer::is_injective("EXP"));
        assert!(MathFunctionAnalyzer::is_injective("LN"));
        assert!(!MathFunctionAnalyzer::is_injective("FLOOR"));

        assert_eq!(
            MathFunctionAnalyzer::ndv_reduction_factor("FLOOR"),
            Some(0.1)
        );
        assert_eq!(
            MathFunctionAnalyzer::ndv_reduction_factor("SIGN"),
            Some(0.01)
        );
    }

    // ========================================================================
    // Tests for AND/OR/NOT logical operators
    // ========================================================================

    #[test]
    fn test_and_selectivity() {
        let stats = make_stats_with_ndv(1000, 100);
        let col = Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>;
        let lit1 =
            Arc::new(Literal::new(ScalarValue::Int32(Some(42)))) as Arc<dyn PhysicalExpr>;
        let lit2 =
            Arc::new(Literal::new(ScalarValue::Int32(Some(10)))) as Arc<dyn PhysicalExpr>;

        // a = 42 AND a > 10
        let eq = Arc::new(BinaryExpr::new(Arc::clone(&col), Operator::Eq, lit1))
            as Arc<dyn PhysicalExpr>;
        let gt =
            Arc::new(BinaryExpr::new(col, Operator::Gt, lit2)) as Arc<dyn PhysicalExpr>;
        let and_expr =
            Arc::new(BinaryExpr::new(eq, Operator::And, gt)) as Arc<dyn PhysicalExpr>;

        let registry = ExpressionAnalyzerRegistry::new();
        let sel = registry.get_selectivity(&and_expr, &stats).unwrap();

        // AND: 0.01 * 0.33 = 0.0033
        assert!((sel - 0.0033).abs() < 0.001);
    }

    #[test]
    fn test_or_selectivity() {
        let stats = make_stats_with_ndv(1000, 100);
        let col = Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>;
        let lit1 =
            Arc::new(Literal::new(ScalarValue::Int32(Some(42)))) as Arc<dyn PhysicalExpr>;
        let lit2 =
            Arc::new(Literal::new(ScalarValue::Int32(Some(10)))) as Arc<dyn PhysicalExpr>;

        // a = 42 OR a > 10
        let eq = Arc::new(BinaryExpr::new(Arc::clone(&col), Operator::Eq, lit1))
            as Arc<dyn PhysicalExpr>;
        let gt =
            Arc::new(BinaryExpr::new(col, Operator::Gt, lit2)) as Arc<dyn PhysicalExpr>;
        let or_expr =
            Arc::new(BinaryExpr::new(eq, Operator::Or, gt)) as Arc<dyn PhysicalExpr>;

        let registry = ExpressionAnalyzerRegistry::new();
        let sel = registry.get_selectivity(&or_expr, &stats).unwrap();

        // OR: 0.01 + 0.33 - (0.01 * 0.33) = 0.3367
        assert!((sel - 0.3367).abs() < 0.001);
    }

    #[test]
    fn test_not_selectivity() {
        let stats = make_stats_with_ndv(1000, 100);
        let col = Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>;
        let lit =
            Arc::new(Literal::new(ScalarValue::Int32(Some(42)))) as Arc<dyn PhysicalExpr>;

        // NOT (a = 42)
        let eq =
            Arc::new(BinaryExpr::new(col, Operator::Eq, lit)) as Arc<dyn PhysicalExpr>;
        let not_expr = Arc::new(NotExpr::new(eq)) as Arc<dyn PhysicalExpr>;

        let registry = ExpressionAnalyzerRegistry::new();
        let sel = registry.get_selectivity(&not_expr, &stats).unwrap();

        // NOT: 1 - 0.01 = 0.99
        assert!((sel - 0.99).abs() < 0.001);
    }

    #[test]
    fn test_nested_and_or() {
        let stats = make_stats_with_ndv(1000, 100);
        let col = Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>;
        let lit1 =
            Arc::new(Literal::new(ScalarValue::Int32(Some(1)))) as Arc<dyn PhysicalExpr>;
        let lit2 =
            Arc::new(Literal::new(ScalarValue::Int32(Some(2)))) as Arc<dyn PhysicalExpr>;
        let lit3 =
            Arc::new(Literal::new(ScalarValue::Int32(Some(3)))) as Arc<dyn PhysicalExpr>;

        // (a = 1 OR a = 2) AND a > 3
        let eq1 = Arc::new(BinaryExpr::new(Arc::clone(&col), Operator::Eq, lit1))
            as Arc<dyn PhysicalExpr>;
        let eq2 = Arc::new(BinaryExpr::new(Arc::clone(&col), Operator::Eq, lit2))
            as Arc<dyn PhysicalExpr>;
        let gt3 =
            Arc::new(BinaryExpr::new(col, Operator::Gt, lit3)) as Arc<dyn PhysicalExpr>;

        let or_expr =
            Arc::new(BinaryExpr::new(eq1, Operator::Or, eq2)) as Arc<dyn PhysicalExpr>;
        let and_expr = Arc::new(BinaryExpr::new(or_expr, Operator::And, gt3))
            as Arc<dyn PhysicalExpr>;

        let registry = ExpressionAnalyzerRegistry::new();
        let sel = registry.get_selectivity(&and_expr, &stats).unwrap();

        // (0.01 + 0.01 - 0.0001) * 0.33 ≈ 0.0066
        assert!(sel > 0.005 && sel < 0.01);
    }

    // ========================================================================
    // Tests for custom analyzer override
    // ========================================================================

    /// Custom analyzer that always returns selectivity of 0.42 for any expression
    #[derive(Debug)]
    struct FixedSelectivityAnalyzer(f64);

    impl ExpressionAnalyzer for FixedSelectivityAnalyzer {
        fn get_selectivity(
            &self,
            _expr: &Arc<dyn PhysicalExpr>,
            _input_stats: &Statistics,
        ) -> AnalysisResult<f64> {
            AnalysisResult::Computed(self.0)
        }
    }

    #[test]
    fn test_custom_analyzer_overrides_default() {
        let stats = make_stats_with_ndv(1000, 100);
        let col = Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>;
        let lit =
            Arc::new(Literal::new(ScalarValue::Int32(Some(42)))) as Arc<dyn PhysicalExpr>;
        let eq =
            Arc::new(BinaryExpr::new(col, Operator::Eq, lit)) as Arc<dyn PhysicalExpr>;

        // Default would give 1/100 = 0.01
        let default_registry = ExpressionAnalyzerRegistry::new();
        let default_sel = default_registry.get_selectivity(&eq, &stats).unwrap();
        assert!((default_sel - 0.01).abs() < 0.001);

        // Custom analyzer overrides to 0.42
        let mut custom_registry = ExpressionAnalyzerRegistry::new();
        custom_registry.register(Arc::new(FixedSelectivityAnalyzer(0.42)));
        let custom_sel = custom_registry.get_selectivity(&eq, &stats).unwrap();
        assert!((custom_sel - 0.42).abs() < 0.001);
    }

    /// Custom analyzer that only handles specific expressions
    #[derive(Debug)]
    struct ColumnAOnlyAnalyzer;

    impl ExpressionAnalyzer for ColumnAOnlyAnalyzer {
        fn get_selectivity(
            &self,
            expr: &Arc<dyn PhysicalExpr>,
            _input_stats: &Statistics,
        ) -> AnalysisResult<f64> {
            // Only handle column "a" equality
            if let Some(binary) = expr.as_any().downcast_ref::<BinaryExpr>()
                && let Some(col) = binary.left().as_any().downcast_ref::<Column>()
                && col.name() == "a"
                && matches!(binary.op(), Operator::Eq)
            {
                return AnalysisResult::Computed(0.99); // Override for col a
            }
            AnalysisResult::Delegate // Let default handle everything else
        }
    }

    #[test]
    fn test_custom_analyzer_delegates_to_default() {
        let stats = make_stats_with_ndv(1000, 100);
        let col_a = Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>;
        let col_b = Arc::new(Column::new("b", 0)) as Arc<dyn PhysicalExpr>;
        let lit =
            Arc::new(Literal::new(ScalarValue::Int32(Some(42)))) as Arc<dyn PhysicalExpr>;

        let eq_a = Arc::new(BinaryExpr::new(col_a, Operator::Eq, Arc::clone(&lit)))
            as Arc<dyn PhysicalExpr>;
        let eq_b =
            Arc::new(BinaryExpr::new(col_b, Operator::Eq, lit)) as Arc<dyn PhysicalExpr>;

        let mut registry = ExpressionAnalyzerRegistry::new();
        registry.register(Arc::new(ColumnAOnlyAnalyzer));

        // Column "a" equality uses custom (0.99)
        let sel_a = registry.get_selectivity(&eq_a, &stats).unwrap();
        assert!((sel_a - 0.99).abs() < 0.001);

        // Column "b" equality delegates to default (0.01)
        let sel_b = registry.get_selectivity(&eq_b, &stats).unwrap();
        assert!((sel_b - 0.01).abs() < 0.001);
    }

    #[test]
    fn test_registry_with_no_default() {
        let stats = make_stats_with_ndv(1000, 100);
        let col = Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>;
        let lit =
            Arc::new(Literal::new(ScalarValue::Int32(Some(42)))) as Arc<dyn PhysicalExpr>;
        let eq =
            Arc::new(BinaryExpr::new(col, Operator::Eq, lit)) as Arc<dyn PhysicalExpr>;

        // Registry with only custom analyzer (no default)
        let registry = ExpressionAnalyzerRegistry::with_analyzers(vec![Arc::new(
            FixedSelectivityAnalyzer(0.77),
        )]);

        let sel = registry.get_selectivity(&eq, &stats).unwrap();
        assert!((sel - 0.77).abs() < 0.001);
    }

    #[test]
    fn test_registry_with_multiple_custom_analyzers() {
        let stats = make_stats_with_ndv(1000, 100);
        let col = Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>;
        let lit =
            Arc::new(Literal::new(ScalarValue::Int32(Some(42)))) as Arc<dyn PhysicalExpr>;
        let eq =
            Arc::new(BinaryExpr::new(col, Operator::Eq, lit)) as Arc<dyn PhysicalExpr>;

        // First analyzer in chain wins
        let registry = ExpressionAnalyzerRegistry::with_analyzers(vec![
            Arc::new(FixedSelectivityAnalyzer(0.11)),
            Arc::new(FixedSelectivityAnalyzer(0.22)),
            Arc::new(DefaultExpressionAnalyzer),
        ]);

        let sel = registry.get_selectivity(&eq, &stats).unwrap();
        assert!((sel - 0.11).abs() < 0.001); // First one wins
    }

    #[test]
    fn test_custom_ndv_analyzer() {
        /// Custom analyzer that doubles NDV
        #[derive(Debug)]
        struct DoubleNdvAnalyzer;

        impl ExpressionAnalyzer for DoubleNdvAnalyzer {
            fn get_distinct_count(
                &self,
                expr: &Arc<dyn PhysicalExpr>,
                input_stats: &Statistics,
            ) -> AnalysisResult<usize> {
                // Get default NDV and double it
                if let Some(ndv) = DefaultExpressionAnalyzer
                    .get_distinct_count(expr, input_stats)
                    .into_option()
                {
                    return AnalysisResult::Computed(ndv * 2);
                }
                AnalysisResult::Delegate
            }
        }

        let stats = make_stats_with_ndv(1000, 100);
        let col = Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>;

        let mut registry = ExpressionAnalyzerRegistry::new();
        registry.register(Arc::new(DoubleNdvAnalyzer));

        let ndv = registry.get_distinct_count(&col, &stats).unwrap();
        assert_eq!(ndv, 200); // Doubled from 100
    }

    #[test]
    fn test_with_analyzers_and_default() {
        let stats = make_stats_with_ndv(1000, 100);
        let col = Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>;

        // ColumnAOnlyAnalyzer only handles equality on "a", delegates NDV to default
        let registry =
            ExpressionAnalyzerRegistry::with_analyzers_and_default(vec![Arc::new(
                ColumnAOnlyAnalyzer,
            )
                as Arc<dyn ExpressionAnalyzer>]);

        // NDV should come from default (100)
        let ndv = registry.get_distinct_count(&col, &stats).unwrap();
        assert_eq!(ndv, 100);
    }

    #[test]
    fn test_binary_expr_ndv_arithmetic() {
        let stats = make_stats_with_ndv(1000, 100);
        let col = Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>;
        let lit =
            Arc::new(Literal::new(ScalarValue::Int64(Some(1)))) as Arc<dyn PhysicalExpr>;

        let registry = ExpressionAnalyzerRegistry::new();

        // col + 1: injective, preserves NDV
        let plus = Arc::new(BinaryExpr::new(
            Arc::clone(&col),
            Operator::Plus,
            Arc::clone(&lit),
        )) as Arc<dyn PhysicalExpr>;
        assert_eq!(registry.get_distinct_count(&plus, &stats), Some(100));

        // col - 1: injective, preserves NDV
        let minus = Arc::new(BinaryExpr::new(
            Arc::clone(&col),
            Operator::Minus,
            Arc::clone(&lit),
        )) as Arc<dyn PhysicalExpr>;
        assert_eq!(registry.get_distinct_count(&minus, &stats), Some(100));

        // col * 2: injective, preserves NDV
        let lit2 =
            Arc::new(Literal::new(ScalarValue::Int64(Some(2)))) as Arc<dyn PhysicalExpr>;
        let mul = Arc::new(BinaryExpr::new(Arc::clone(&col), Operator::Multiply, lit2))
            as Arc<dyn PhysicalExpr>;
        assert_eq!(registry.get_distinct_count(&mul, &stats), Some(100));

        // 1 + col: also injective (literal on left)
        let plus_rev =
            Arc::new(BinaryExpr::new(lit, Operator::Plus, col)) as Arc<dyn PhysicalExpr>;
        assert_eq!(registry.get_distinct_count(&plus_rev, &stats), Some(100));
    }
}
