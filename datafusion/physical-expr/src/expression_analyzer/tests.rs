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

use super::*;
use crate::PhysicalExpr;
use crate::expressions::{BinaryExpr, Column, Literal, NotExpr};
use datafusion_common::stats::Precision;
use datafusion_common::{ColumnStatistics, ScalarValue, Statistics};
use datafusion_expr::Operator;
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

// NDV tests

#[test]
fn test_column_ndv() {
    let stats = make_stats_with_ndv(1000, 100);
    let col = Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>;
    let registry = ExpressionAnalyzerRegistry::new();
    assert_eq!(registry.get_distinct_count(&col, &stats), Some(100));
}

#[test]
fn test_literal_ndv() {
    let stats = make_stats_with_ndv(1000, 100);
    let lit =
        Arc::new(Literal::new(ScalarValue::Int32(Some(42)))) as Arc<dyn PhysicalExpr>;
    let registry = ExpressionAnalyzerRegistry::new();
    assert_eq!(registry.get_distinct_count(&lit, &stats), Some(1));
}

#[test]
fn test_arithmetic_ndv() {
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

    // 1 + col: also injective (literal on left)
    let plus_rev =
        Arc::new(BinaryExpr::new(lit, Operator::Plus, col)) as Arc<dyn PhysicalExpr>;
    assert_eq!(registry.get_distinct_count(&plus_rev, &stats), Some(100));
}

// Selectivity tests

#[test]
fn test_equality_selectivity() {
    let stats = make_stats_with_ndv(1000, 100);
    let col = Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>;
    let lit =
        Arc::new(Literal::new(ScalarValue::Int32(Some(42)))) as Arc<dyn PhysicalExpr>;
    let eq = Arc::new(BinaryExpr::new(col, Operator::Eq, lit)) as Arc<dyn PhysicalExpr>;

    let registry = ExpressionAnalyzerRegistry::new();
    let sel = registry.get_selectivity(&eq, &stats).unwrap();
    assert!((sel - 0.01).abs() < 0.001); // 1/NDV = 1/100
}

#[test]
fn test_equality_selectivity_column_on_right() {
    let stats = make_stats_with_ndv(1000, 100);
    let col = Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>;
    let lit =
        Arc::new(Literal::new(ScalarValue::Int32(Some(42)))) as Arc<dyn PhysicalExpr>;
    let eq = Arc::new(BinaryExpr::new(lit, Operator::Eq, col)) as Arc<dyn PhysicalExpr>;

    let registry = ExpressionAnalyzerRegistry::new();
    let sel = registry.get_selectivity(&eq, &stats).unwrap();
    assert!((sel - 0.01).abs() < 0.001);
}

#[test]
fn test_and_selectivity() {
    let stats = make_stats_with_ndv(1000, 100);
    let col = Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>;
    let lit1 =
        Arc::new(Literal::new(ScalarValue::Int32(Some(42)))) as Arc<dyn PhysicalExpr>;
    let lit2 =
        Arc::new(Literal::new(ScalarValue::Int32(Some(10)))) as Arc<dyn PhysicalExpr>;

    let eq = Arc::new(BinaryExpr::new(Arc::clone(&col), Operator::Eq, lit1))
        as Arc<dyn PhysicalExpr>;
    let gt = Arc::new(BinaryExpr::new(col, Operator::Gt, lit2)) as Arc<dyn PhysicalExpr>;
    let and_expr =
        Arc::new(BinaryExpr::new(eq, Operator::And, gt)) as Arc<dyn PhysicalExpr>;

    let registry = ExpressionAnalyzerRegistry::new();
    let sel = registry.get_selectivity(&and_expr, &stats).unwrap();
    assert!((sel - 0.0033).abs() < 0.001); // 0.01 * 0.33
}

#[test]
fn test_or_selectivity() {
    let stats = make_stats_with_ndv(1000, 100);
    let col = Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>;
    let lit1 =
        Arc::new(Literal::new(ScalarValue::Int32(Some(42)))) as Arc<dyn PhysicalExpr>;
    let lit2 =
        Arc::new(Literal::new(ScalarValue::Int32(Some(10)))) as Arc<dyn PhysicalExpr>;

    let eq = Arc::new(BinaryExpr::new(Arc::clone(&col), Operator::Eq, lit1))
        as Arc<dyn PhysicalExpr>;
    let gt = Arc::new(BinaryExpr::new(col, Operator::Gt, lit2)) as Arc<dyn PhysicalExpr>;
    let or_expr =
        Arc::new(BinaryExpr::new(eq, Operator::Or, gt)) as Arc<dyn PhysicalExpr>;

    let registry = ExpressionAnalyzerRegistry::new();
    let sel = registry.get_selectivity(&or_expr, &stats).unwrap();
    assert!((sel - 0.3367).abs() < 0.001); // 0.01 + 0.33 - 0.01*0.33
}

#[test]
fn test_not_selectivity() {
    let stats = make_stats_with_ndv(1000, 100);
    let col = Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>;
    let lit =
        Arc::new(Literal::new(ScalarValue::Int32(Some(42)))) as Arc<dyn PhysicalExpr>;

    let eq = Arc::new(BinaryExpr::new(col, Operator::Eq, lit)) as Arc<dyn PhysicalExpr>;
    let not_expr = Arc::new(NotExpr::new(eq)) as Arc<dyn PhysicalExpr>;

    let registry = ExpressionAnalyzerRegistry::new();
    let sel = registry.get_selectivity(&not_expr, &stats).unwrap();
    assert!((sel - 0.99).abs() < 0.001); // 1 - 0.01
}

#[test]
fn test_equality_selectivity_expression_eq_literal() {
    let stats = make_stats_with_ndv(1000, 100);
    let col = Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>;
    let one =
        Arc::new(Literal::new(ScalarValue::Int32(Some(1)))) as Arc<dyn PhysicalExpr>;
    let forty_two =
        Arc::new(Literal::new(ScalarValue::Int32(Some(42)))) as Arc<dyn PhysicalExpr>;
    let a_plus_1 =
        Arc::new(BinaryExpr::new(col, Operator::Plus, one)) as Arc<dyn PhysicalExpr>;
    let eq = Arc::new(BinaryExpr::new(a_plus_1, Operator::Eq, forty_two))
        as Arc<dyn PhysicalExpr>;

    let registry = ExpressionAnalyzerRegistry::new();
    let sel = registry.get_selectivity(&eq, &stats).unwrap();
    // NDV(a + 1) = NDV(a) = 100, so selectivity = 1/100 = 0.01
    assert!((sel - 0.01).abs() < 0.001);
}

#[test]
fn test_inequality_selectivity_expression_neq_literal() {
    let stats = make_stats_with_ndv(1000, 100);
    let col = Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>;
    let one =
        Arc::new(Literal::new(ScalarValue::Int32(Some(1)))) as Arc<dyn PhysicalExpr>;
    let forty_two =
        Arc::new(Literal::new(ScalarValue::Int32(Some(42)))) as Arc<dyn PhysicalExpr>;
    let a_plus_1 =
        Arc::new(BinaryExpr::new(col, Operator::Plus, one)) as Arc<dyn PhysicalExpr>;
    let neq = Arc::new(BinaryExpr::new(a_plus_1, Operator::NotEq, forty_two))
        as Arc<dyn PhysicalExpr>;

    let registry = ExpressionAnalyzerRegistry::new();
    let sel = registry.get_selectivity(&neq, &stats).unwrap();
    // NDV(a + 1) = 100, selectivity = 1 - 1/100 = 0.99
    assert!((sel - 0.99).abs() < 0.001);
}

// Min/max tests

#[test]
fn test_column_min_max() {
    let stats = Statistics {
        num_rows: Precision::Exact(100),
        total_byte_size: Precision::Absent,
        column_statistics: vec![ColumnStatistics {
            min_value: Precision::Exact(ScalarValue::Int32(Some(1))),
            max_value: Precision::Exact(ScalarValue::Int32(Some(100))),
            distinct_count: Precision::Absent,
            null_count: Precision::Exact(0),
            sum_value: Precision::Absent,
            byte_size: Precision::Absent,
        }],
    };
    let col = Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>;
    let registry = ExpressionAnalyzerRegistry::new();

    assert_eq!(
        registry.get_min_max(&col, &stats),
        Some((ScalarValue::Int32(Some(1)), ScalarValue::Int32(Some(100))))
    );
}

#[test]
fn test_literal_min_max() {
    let stats = make_stats_with_ndv(100, 10);
    let lit =
        Arc::new(Literal::new(ScalarValue::Int32(Some(42)))) as Arc<dyn PhysicalExpr>;
    let registry = ExpressionAnalyzerRegistry::new();

    assert_eq!(
        registry.get_min_max(&lit, &stats),
        Some((ScalarValue::Int32(Some(42)), ScalarValue::Int32(Some(42))))
    );
}

// Null fraction tests

#[test]
fn test_column_null_fraction() {
    let stats = Statistics {
        num_rows: Precision::Exact(1000),
        total_byte_size: Precision::Absent,
        column_statistics: vec![ColumnStatistics {
            null_count: Precision::Exact(250),
            min_value: Precision::Absent,
            max_value: Precision::Absent,
            sum_value: Precision::Absent,
            distinct_count: Precision::Absent,
            byte_size: Precision::Absent,
        }],
    };
    let col = Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>;
    let registry = ExpressionAnalyzerRegistry::new();

    let frac = registry.get_null_fraction(&col, &stats).unwrap();
    assert!((frac - 0.25).abs() < 0.001);
}

#[test]
fn test_literal_null_fraction() {
    let stats = make_stats_with_ndv(100, 10);
    let registry = ExpressionAnalyzerRegistry::new();

    let lit =
        Arc::new(Literal::new(ScalarValue::Int32(Some(42)))) as Arc<dyn PhysicalExpr>;
    assert_eq!(registry.get_null_fraction(&lit, &stats), Some(0.0));

    let null_lit =
        Arc::new(Literal::new(ScalarValue::Int32(None))) as Arc<dyn PhysicalExpr>;
    assert_eq!(registry.get_null_fraction(&null_lit, &stats), Some(1.0));
}

// Custom analyzer tests

#[derive(Debug)]
struct FixedSelectivityAnalyzer(f64);

impl ExpressionAnalyzer for FixedSelectivityAnalyzer {
    fn get_selectivity(
        &self,
        _expr: &Arc<dyn PhysicalExpr>,
        _input_stats: &Statistics,
        _registry: &ExpressionAnalyzerRegistry,
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
    let eq = Arc::new(BinaryExpr::new(col, Operator::Eq, lit)) as Arc<dyn PhysicalExpr>;

    let mut registry = ExpressionAnalyzerRegistry::new();
    registry.register(Arc::new(FixedSelectivityAnalyzer(0.42)));
    let sel = registry.get_selectivity(&eq, &stats).unwrap();
    assert!((sel - 0.42).abs() < 0.001);
}

#[derive(Debug)]
struct ColumnAOnlyAnalyzer;

impl ExpressionAnalyzer for ColumnAOnlyAnalyzer {
    fn get_selectivity(
        &self,
        expr: &Arc<dyn PhysicalExpr>,
        _input_stats: &Statistics,
        _registry: &ExpressionAnalyzerRegistry,
    ) -> AnalysisResult<f64> {
        if let Some(binary) = expr.as_any().downcast_ref::<BinaryExpr>()
            && let Some(col) = binary.left().as_any().downcast_ref::<Column>()
            && col.name() == "a"
            && matches!(binary.op(), Operator::Eq)
        {
            return AnalysisResult::Computed(0.99);
        }
        AnalysisResult::Delegate
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

    let sel_a = registry.get_selectivity(&eq_a, &stats).unwrap();
    assert!((sel_a - 0.99).abs() < 0.001);

    let sel_b = registry.get_selectivity(&eq_b, &stats).unwrap();
    assert!((sel_b - 0.01).abs() < 0.001);
}

#[test]
fn test_with_analyzers_and_default() {
    let stats = make_stats_with_ndv(1000, 100);
    let col = Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>;

    let registry =
        ExpressionAnalyzerRegistry::with_analyzers_and_default(vec![Arc::new(
            ColumnAOnlyAnalyzer,
        )
            as Arc<dyn ExpressionAnalyzer>]);

    assert_eq!(registry.get_distinct_count(&col, &stats), Some(100));
}
