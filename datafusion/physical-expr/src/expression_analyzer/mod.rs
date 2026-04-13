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
//! use datafusion_physical_expr::expression_analyzer::*;
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

mod default;

#[cfg(test)]
mod tests;

pub use default::DefaultExpressionAnalyzer;

use std::fmt::Debug;
use std::sync::Arc;

use datafusion_common::{ScalarValue, Statistics};

use crate::PhysicalExpr;

/// Result of expression analysis - either computed or delegate to next analyzer.
#[derive(Debug, Clone)]
pub enum AnalysisResult<T> {
    /// Analysis was performed, here's the result
    Computed(T),
    /// This analyzer doesn't handle this expression; delegate to next
    Delegate,
}

/// Expression-level metadata analysis.
///
/// Implementations can handle specific expression types or provide domain
/// knowledge for custom UDFs. The chain of analyzers is traversed until one
/// returns [`AnalysisResult::Computed`].
///
/// The `registry` parameter allows analyzers to delegate sub-expression
/// analysis back through the full chain, rather than hard-coding a specific
/// analyzer. For example, a function analyzer can ask the registry for the
/// NDV of its input argument, which will traverse the full chain (including
/// any custom analyzers the user registered).
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
///         registry: &ExpressionAnalyzerRegistry,
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
        _registry: &ExpressionAnalyzerRegistry,
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
        _registry: &ExpressionAnalyzerRegistry,
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
        _registry: &ExpressionAnalyzerRegistry,
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
        _registry: &ExpressionAnalyzerRegistry,
    ) -> AnalysisResult<f64> {
        AnalysisResult::Delegate
    }
}

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
    /// Create a registry pre-populated with [`DefaultExpressionAnalyzer`].
    pub fn new() -> Self {
        Self {
            analyzers: vec![Arc::new(DefaultExpressionAnalyzer)],
        }
    }

    /// Create a registry with only the given analyzers (no builtins).
    /// Create a registry with only the given analyzers and no built-in fallback.
    ///
    /// If none of the provided analyzers can handle a request, the registry
    /// returns `None` and the caller applies its own default (e.g.
    /// `default_selectivity` for filters). [`DefaultExpressionAnalyzer`] is
    /// the built-in analyzer that handles common patterns (equality,
    /// AND/OR, literals); use [`with_analyzers_and_default`] to include it
    /// as a fallback after your custom analyzers.
    ///
    /// [`with_analyzers_and_default`]: ExpressionAnalyzerRegistry::with_analyzers_and_default
    pub fn with_analyzers_only(analyzers: Vec<Arc<dyn ExpressionAnalyzer>>) -> Self {
        Self { analyzers }
    }

    /// Create a registry with custom analyzers followed by the
    /// [`DefaultExpressionAnalyzer`] as fallback.
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
                analyzer.get_selectivity(expr, input_stats, self)
            {
                return Some(sel.clamp(0.0, 1.0));
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
                analyzer.get_distinct_count(expr, input_stats, self)
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
                analyzer.get_min_max(expr, input_stats, self)
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
                analyzer.get_null_fraction(expr, input_stats, self)
            {
                return Some(frac.clamp(0.0, 1.0));
            }
        }
        None
    }
}
