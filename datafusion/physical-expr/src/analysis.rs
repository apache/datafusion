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

//! Interval and selectivity in [`AnalysisContext`]

use std::fmt::Debug;
use std::sync::Arc;

use crate::expressions::Column;
use crate::intervals::cp_solver::{ExprIntervalGraph, PropagationResult};
use crate::utils::collect_columns;
use crate::PhysicalExpr;

use arrow::datatypes::Schema;
use datafusion_common::stats::Precision;
use datafusion_common::{
    assert_or_internal_err, internal_datafusion_err, internal_err, ColumnStatistics,
    Result, ScalarValue,
};
use datafusion_expr::interval_arithmetic::{cardinality_ratio, Interval};

/// The shared context used during the analysis of an expression. Includes
/// the boundaries for all known columns.
#[derive(Clone, Debug, PartialEq)]
pub struct AnalysisContext {
    // A list of known column boundaries, ordered by the index
    // of the column in the current schema.
    pub boundaries: Vec<ExprBoundaries>,
    /// The estimated percentage of rows that this expression would select, if
    /// it were to be used as a boolean predicate on a filter. The value will be
    /// between 0.0 (selects nothing) and 1.0 (selects everything).
    pub selectivity: Option<f64>,
}

impl AnalysisContext {
    pub fn new(boundaries: Vec<ExprBoundaries>) -> Self {
        Self {
            boundaries,
            selectivity: None,
        }
    }

    pub fn with_selectivity(mut self, selectivity: f64) -> Self {
        self.selectivity = Some(selectivity);
        self
    }

    /// Create a new analysis context from column statistics.
    pub fn try_from_statistics(
        input_schema: &Schema,
        statistics: &[ColumnStatistics],
    ) -> Result<Self> {
        statistics
            .iter()
            .enumerate()
            .map(|(idx, stats)| ExprBoundaries::try_from_column(input_schema, stats, idx))
            .collect::<Result<Vec<_>>>()
            .map(Self::new)
    }
}

/// Represents the boundaries (e.g. min and max values) of a particular column
///
/// This is used range analysis of expressions, to determine if the expression
/// limits the value of particular columns (e.g. analyzing an expression such as
/// `time < 50` would result in a boundary interval for `time` having a max
/// value of `50`).
#[derive(Clone, Debug, PartialEq)]
pub struct ExprBoundaries {
    pub column: Column,
    /// Minimum and maximum values this expression can have. A `None` value
    /// indicates that evaluating the given column results in an empty set.
    /// For example, if the column `a` has values in the range [10, 20],
    /// and there is a filter asserting that `a > 50`, then the resulting interval
    /// range of `a` will be `None`.
    pub interval: Option<Interval>,
    /// Maximum number of distinct values this expression can produce, if known.
    pub distinct_count: Precision<usize>,
}

impl ExprBoundaries {
    /// Create a new `ExprBoundaries` object from column level statistics.
    pub fn try_from_column(
        schema: &Schema,
        col_stats: &ColumnStatistics,
        col_index: usize,
    ) -> Result<Self> {
        let field = schema.fields().get(col_index).ok_or_else(|| {
            internal_datafusion_err!(
                "Could not create `ExprBoundaries`: in `try_from_column` `col_index`
                has gone out of bounds with a value of {col_index}, the schema has {} columns.",
                schema.fields.len()
            )
        })?;
        let empty_field =
            ScalarValue::try_from(field.data_type()).unwrap_or(ScalarValue::Null);
        let interval = Interval::try_new(
            col_stats
                .min_value
                .get_value()
                .cloned()
                .unwrap_or_else(|| empty_field.clone()),
            col_stats
                .max_value
                .get_value()
                .cloned()
                .unwrap_or(empty_field),
        )?;
        let column = Column::new(field.name(), col_index);
        Ok(ExprBoundaries {
            column,
            interval: Some(interval),
            distinct_count: col_stats.distinct_count,
        })
    }

    /// Create `ExprBoundaries` that represent no known bounds for all the
    /// columns in `schema`
    pub fn try_new_unbounded(schema: &Schema) -> Result<Vec<Self>> {
        schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, field)| {
                Ok(Self {
                    column: Column::new(field.name(), i),
                    interval: Some(Interval::make_unbounded(field.data_type())?),
                    distinct_count: Precision::Absent,
                })
            })
            .collect()
    }
}

/// Attempts to refine column boundaries and compute a selectivity value.
///
/// The function accepts boundaries of the input columns in the `context` parameter.
/// It then tries to tighten these boundaries based on the provided `expr`.
/// The resulting selectivity value is calculated by comparing the initial and final boundaries.
/// The computation assumes that the data within the column is uniformly distributed and not sorted.
///
/// # Arguments
///
/// * `context` - The context holding input column boundaries.
/// * `expr` - The expression used to shrink the column boundaries.
///
/// # Returns
///
/// * `AnalysisContext` constructed by pruned boundaries and a selectivity value.
pub fn analyze(
    expr: &Arc<dyn PhysicalExpr>,
    context: AnalysisContext,
    schema: &Schema,
) -> Result<AnalysisContext> {
    let initial_boundaries = &context.boundaries;
    if initial_boundaries
        .iter()
        .all(|bound| bound.interval.is_none())
    {
        assert_or_internal_err!(
            !initial_boundaries
                .iter()
                .any(|bound| bound.distinct_count != Precision::Exact(0)),
            "ExprBoundaries has a non-zero distinct count although it represents an empty table"
        );
        assert_or_internal_err!(
            context.selectivity == Some(0.0),
            "AnalysisContext has a non-zero selectivity although it represents an empty table"
        );
        Ok(context)
    } else if initial_boundaries
        .iter()
        .any(|bound| bound.interval.is_none())
    {
        internal_err!(
                "AnalysisContext is an inconsistent state. Some columns represent empty table while others don't"
            )
    } else {
        let mut target_boundaries = context.boundaries;
        let mut graph = ExprIntervalGraph::try_new(Arc::clone(expr), schema)?;
        let columns = collect_columns(expr)
            .into_iter()
            .map(|c| Arc::new(c) as _)
            .collect::<Vec<_>>();

        let mut target_indices_and_boundaries = vec![];
        let target_expr_and_indices = graph.gather_node_indices(columns.as_slice());

        for (expr, index) in &target_expr_and_indices {
            if let Some(column) = expr.as_any().downcast_ref::<Column>() {
                if let Some(bound) =
                    target_boundaries.iter().find(|b| b.column == *column)
                {
                    // Now, it's safe to unwrap
                    target_indices_and_boundaries
                        .push((*index, bound.interval.as_ref().unwrap().clone()));
                }
            }
        }

        match graph.update_ranges(&mut target_indices_and_boundaries, Interval::TRUE)? {
            PropagationResult::Success => {
                shrink_boundaries(&graph, target_boundaries, &target_expr_and_indices)
            }
            PropagationResult::Infeasible => {
                // If the propagation result is infeasible, set intervals to None
                target_boundaries
                    .iter_mut()
                    .for_each(|bound| bound.interval = None);
                Ok(AnalysisContext::new(target_boundaries).with_selectivity(0.0))
            }
            PropagationResult::CannotPropagate => {
                Ok(AnalysisContext::new(target_boundaries).with_selectivity(1.0))
            }
        }
    }
}

/// If the `PropagationResult` indicates success, this function calculates the
/// selectivity value by comparing the initial and final column boundaries.
/// Following this, it constructs and returns a new `AnalysisContext` with the
/// updated parameters.
fn shrink_boundaries(
    graph: &ExprIntervalGraph,
    mut target_boundaries: Vec<ExprBoundaries>,
    target_expr_and_indices: &[(Arc<dyn PhysicalExpr>, usize)],
) -> Result<AnalysisContext> {
    let initial_boundaries = target_boundaries.clone();
    target_expr_and_indices.iter().for_each(|(expr, i)| {
        if let Some(column) = expr.as_any().downcast_ref::<Column>() {
            if let Some(bound) = target_boundaries
                .iter_mut()
                .find(|bound| bound.column.eq(column))
            {
                bound.interval = Some(graph.get_interval(*i));
            };
        }
    });

    let selectivity = calculate_selectivity(&target_boundaries, &initial_boundaries)?;

    assert_or_internal_err!(
        (0.0..=1.0).contains(&selectivity),
        "Selectivity is out of limit: {selectivity}",
    );

    Ok(AnalysisContext::new(target_boundaries).with_selectivity(selectivity))
}

/// This function calculates the filter predicate's selectivity by comparing
/// the initial and pruned column boundaries. Selectivity is defined as the
/// ratio of rows in a table that satisfy the filter's predicate.
fn calculate_selectivity(
    target_boundaries: &[ExprBoundaries],
    initial_boundaries: &[ExprBoundaries],
) -> Result<f64> {
    // Since the intervals are assumed uniform and the values
    // are not correlated, we need to multiply the selectivities
    // of multiple columns to get the overall selectivity.
    if target_boundaries.len() != initial_boundaries.len() {
        return Err(internal_datafusion_err!(
            "The number of columns in the initial and target boundaries should be the same"
        ));
    }
    let mut acc: f64 = 1.0;
    for (initial, target) in initial_boundaries.iter().zip(target_boundaries) {
        match (initial.interval.as_ref(), target.interval.as_ref()) {
            (Some(initial), Some(target)) => {
                acc *= cardinality_ratio(initial, target);
            }
            (None, Some(_)) => {
                return internal_err!(
                "Initial boundary cannot be None while having a Some() target boundary"
            );
            }
            _ => return Ok(0.0),
        }
    }

    Ok(acc)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::{assert_contains, DFSchema};
    use datafusion_expr::{
        col, execution_props::ExecutionProps, interval_arithmetic::Interval, lit, Expr,
    };

    use crate::{create_physical_expr, AnalysisContext};

    use super::{analyze, ExprBoundaries};

    fn make_field(name: &str, data_type: DataType) -> Field {
        let nullable = false;
        Field::new(name, data_type, nullable)
    }

    #[test]
    fn test_analyze_boundary_exprs() {
        let schema = Arc::new(Schema::new(vec![make_field("a", DataType::Int32)]));

        /// Test case containing (expression tree, lower bound, upper bound)
        type TestCase = (Expr, Option<i32>, Option<i32>);

        let test_cases: Vec<TestCase> = vec![
            // a > 10
            (col("a").gt(lit(10)), Some(11), None),
            // a < 20
            (col("a").lt(lit(20)), None, Some(19)),
            // a > 10 AND a < 20
            (
                col("a").gt(lit(10)).and(col("a").lt(lit(20))),
                Some(11),
                Some(19),
            ),
            // a >= 10
            (col("a").gt_eq(lit(10)), Some(10), None),
            // a <= 20
            (col("a").lt_eq(lit(20)), None, Some(20)),
            // a >= 10 AND a <= 20
            (
                col("a").gt_eq(lit(10)).and(col("a").lt_eq(lit(20))),
                Some(10),
                Some(20),
            ),
            // a > 10 AND a < 20 AND a < 15
            (
                col("a")
                    .gt(lit(10))
                    .and(col("a").lt(lit(20)))
                    .and(col("a").lt(lit(15))),
                Some(11),
                Some(14),
            ),
            // (a > 10 AND a < 20) AND (a > 15 AND a < 25)
            (
                col("a")
                    .gt(lit(10))
                    .and(col("a").lt(lit(20)))
                    .and(col("a").gt(lit(15)))
                    .and(col("a").lt(lit(25))),
                Some(16),
                Some(19),
            ),
        ];
        for (expr, lower, upper) in test_cases {
            let boundaries = ExprBoundaries::try_new_unbounded(&schema).unwrap();
            let df_schema = DFSchema::try_from(Arc::clone(&schema)).unwrap();
            let physical_expr =
                create_physical_expr(&expr, &df_schema, &ExecutionProps::new()).unwrap();
            let analysis_result = analyze(
                &physical_expr,
                AnalysisContext::new(boundaries),
                df_schema.as_ref(),
            )
            .unwrap();
            let Some(actual) = &analysis_result.boundaries[0].interval else {
                panic!("The analysis result should contain non-empty intervals for all columns");
            };
            let expected = Interval::make(lower, upper).unwrap();
            assert_eq!(
                &expected, actual,
                "did not get correct interval for SQL expression: {expr:?}"
            );
        }
    }

    #[test]
    fn test_analyze_empty_set_boundary_exprs() {
        let schema = Arc::new(Schema::new(vec![make_field("a", DataType::Int32)]));

        let test_cases: Vec<Expr> = vec![
            // a > 10 AND a < 10
            col("a").gt(lit(10)).and(col("a").lt(lit(10))),
            // a > 5 AND (a < 20 OR a > 20)
            // a > 10 AND a < 20
            // (a > 10 AND a < 20) AND (a > 20 AND a < 30)
            col("a")
                .gt(lit(10))
                .and(col("a").lt(lit(20)))
                .and(col("a").gt(lit(20)))
                .and(col("a").lt(lit(30))),
        ];

        for expr in test_cases {
            let boundaries = ExprBoundaries::try_new_unbounded(&schema).unwrap();
            let df_schema = DFSchema::try_from(Arc::clone(&schema)).unwrap();
            let physical_expr =
                create_physical_expr(&expr, &df_schema, &ExecutionProps::new()).unwrap();
            let analysis_result = analyze(
                &physical_expr,
                AnalysisContext::new(boundaries),
                df_schema.as_ref(),
            )
            .unwrap();

            for boundary in analysis_result.boundaries {
                assert!(boundary.interval.is_none());
            }
        }
    }

    #[test]
    fn test_analyze_invalid_boundary_exprs() {
        let schema = Arc::new(Schema::new(vec![make_field("a", DataType::Int32)]));
        let expr = col("a").lt(lit(10)).or(col("a").gt(lit(20)));
        let expected_error = "OR operator cannot yet propagate true intervals";
        let boundaries = ExprBoundaries::try_new_unbounded(&schema).unwrap();
        let df_schema = DFSchema::try_from(Arc::clone(&schema)).unwrap();
        let physical_expr =
            create_physical_expr(&expr, &df_schema, &ExecutionProps::new()).unwrap();
        let analysis_error = analyze(
            &physical_expr,
            AnalysisContext::new(boundaries),
            df_schema.as_ref(),
        )
        .unwrap_err();
        assert_contains!(analysis_error.to_string(), expected_error);
    }
}
