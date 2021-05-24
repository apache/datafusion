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

//! This module contains code to prune "containers" of row groups
//! based on statistics prior to execution. This can lead to
//! significant performance improvements by avoiding the need
//! to evaluate a plan on entire containers (e.g. an entire file)
//!
//! For example, it is used to prune (skip) row groups while reading
//! parquet files if it can be determined from the predicate that
//! nothing in the row group can match.
//!
//! This code is currently specific to Parquet, but soon (TM), via
//! https://github.com/apache/arrow-datafusion/issues/363 it will
//! be genericized.

use std::{collections::HashSet, convert::TryInto, sync::Arc};

use arrow::{
    array::{ArrayRef, BooleanArray},
    datatypes::{Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};

use crate::{
    error::{DataFusionError, Result},
    execution::context::ExecutionContextState,
    logical_plan::{Expr, Operator},
    optimizer::utils,
    physical_plan::{planner::DefaultPhysicalPlanner, ColumnarValue, PhysicalExpr},
    scalar::ScalarValue,
};

/// Interface to pass statistics information to [`PruningPredicates`]
pub trait PruningStatistics {
    /// return the minimum value for the named column, if known
    fn min_value(&self, column: &str) -> Option<ScalarValue>;

    /// return the maximum value for the named column, if known
    fn max_value(&self, column: &str) -> Option<ScalarValue>;
}

/// Evaluates filter expressions on statistics in order to
/// prune data containers (e.g. parquet row group)
///
/// See [`try_new`] for more information.
#[derive(Debug, Clone)]
pub struct PruningPredicate {
    /// The input schema against which the predicate will be evaluated
    schema: SchemaRef,
    /// Actual pruning predicate (rewritten in terms of column min/max statistics)
    predicate_expr: Arc<dyn PhysicalExpr>,
    /// The statistics required to evaluate this predicate:
    /// * The column name in the input schema
    /// * Statstics type (e.g. Min or Max)
    /// * The field the statistics value should be placed in for
    ///   pruning predicate evaluation
    stat_column_req: Vec<(String, StatisticsType, Field)>,
}

impl PruningPredicate {
    /// Try to create a new instance of [`PruningPredicate`]
    ///
    /// This will translate the provided `expr` filter expression into
    /// a *pruning predicate*.
    ///
    /// A pruning predicate is one that has been rewritten in terms of
    /// the min and max values of column references and that evaluates
    /// to FALSE if the filter predicate would evaluate FALSE *for
    /// every row* whose values fell within the min / max ranges (aka
    /// could be pruned).
    ///
    /// The pruning predicate evaluates to TRUE or NULL
    /// if the filter predicate *might* evaluate to TRUE for at least
    /// one row whose vaules fell within the min/max ranges (in other
    /// words they might pass the predicate)
    ///
    /// For example, the filter expression `(column / 2) = 4` becomes
    /// the pruning predicate
    /// `(column_min / 2) <= 4 && 4 <= (column_max / 2))`
    pub fn try_new(expr: &Expr, schema: SchemaRef) -> Result<Self> {
        // build predicate expression once
        let mut stat_column_req = Vec::<(String, StatisticsType, Field)>::new();
        let logical_predicate_expr =
            build_predicate_expression(expr, schema.as_ref(), &mut stat_column_req)?;
        let stat_fields = stat_column_req
            .iter()
            .map(|(_, _, f)| f.clone())
            .collect::<Vec<_>>();
        let stat_schema = Schema::new(stat_fields);
        let execution_context_state = ExecutionContextState::new();
        let predicate_expr = DefaultPhysicalPlanner::default().create_physical_expr(
            &logical_predicate_expr,
            &stat_schema,
            &execution_context_state,
        )?;
        Ok(Self {
            schema,
            predicate_expr,
            stat_column_req,
        })
    }

    /// For each set of statistics, evalates the pruning predicate
    /// and returns a `bool` with the following meaning for a
    /// all rows whose values match the statistics:
    ///
    /// `true`: There MAY be rows that match the predicate
    ///
    /// `false`: There are no rows that could match the predicate
    ///
    /// Note this function takes a slice of statistics as a parameter
    /// to amortize the cost of the evaluation of the predicate
    /// against a single record batch.
    pub fn prune<S: PruningStatistics>(&self, statistics: &[S]) -> Result<Vec<bool>> {
        // build statistics record batch
        let predicate_array =
            build_statistics_record_batch(statistics, &self.stat_column_req)
                .and_then(|statistics_batch| {
                    // execute predicate expression
                    self.predicate_expr.evaluate(&statistics_batch)
                })
                .and_then(|v| match v {
                    ColumnarValue::Array(array) => Ok(array),
                    ColumnarValue::Scalar(_) => Err(DataFusionError::Internal(
                        "predicate expression didn't return an array".to_string(),
                    )),
                })?;

        let predicate_array = predicate_array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "Expected pruning predicate evaluation to be BooleanArray, \
                     but was {:?}",
                    predicate_array
                ))
            })?;

        // when the result of the predicate expression for a row group is null / undefined,
        // e.g. due to missing statistics, this row group can't be filtered out,
        // so replace with true
        Ok(predicate_array
            .into_iter()
            .map(|x| x.unwrap_or(true))
            .collect::<Vec<_>>())
    }

    /// Return a reference to the input schema
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }
}

/// Build a RecordBatch from a list of statistics, creating arrays,
/// with one row for each PruningStatistics and columns specified in
/// in the stat_column_req parameter.
///
/// For example, if the requested columns are
/// ```text
/// ("s1", Min, Field:s1_min)
/// ("s2", Max, field:s2_max)
///```
///
/// And the input statistics had
/// ```text
/// S1(Min: 5, Max: 10)
/// S2(Min: 99, Max: 1000)
/// S3(Min: 1, Max: 2)
/// ```
///
/// Then this function would build a record batch with 2 columns and
/// one row s1_min and s2_maxx as follows (s3 is not requested):
///
/// ```text
/// s1_min | s2_max
/// -------+--------
///   5    | 1000
/// ```
fn build_statistics_record_batch<S: PruningStatistics>(
    statistics: &[S],
    stat_column_req: &[(String, StatisticsType, Field)],
) -> Result<RecordBatch> {
    let mut fields = Vec::<Field>::new();
    let mut arrays = Vec::<ArrayRef>::new();
    // For each needed statistics column:
    for (column_name, statistics_type, stat_field) in stat_column_req {
        // Create a None value of the appropriate scalar type
        let data_type = stat_field.data_type();
        let null_scalar: ScalarValue = data_type.try_into()?;

        let array = match statistics_type {
            StatisticsType::Min => {
                let values: Vec<_> = statistics
                    .iter()
                    .map(|s| {
                        s.min_value(&column_name)
                            .unwrap_or_else(|| null_scalar.clone())
                    })
                    .collect();
                ScalarValue::iter_to_array(values.iter())?
            }
            StatisticsType::Max => {
                let values: Vec<_> = statistics
                    .iter()
                    .map(|s| {
                        s.max_value(&column_name)
                            .unwrap_or_else(|| null_scalar.clone())
                    })
                    .collect();
                ScalarValue::iter_to_array(values.iter())?
            }
        };

        // cast statistics array to required data type (e.g. parquet
        // provides timestamp statistics as "Int64")
        let array = arrow::compute::cast(&array, data_type)?;

        fields.push(stat_field.clone());
        arrays.push(array);
    }

    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, arrays)
        .map_err(|err| DataFusionError::Plan(err.to_string()))
}

struct PruningExpressionBuilder<'a> {
    column_name: String,
    column_expr: &'a Expr,
    scalar_expr: &'a Expr,
    field: &'a Field,
    stat_column_req: &'a mut Vec<(String, StatisticsType, Field)>,
    reverse_operator: bool,
}

impl<'a> PruningExpressionBuilder<'a> {
    fn try_new(
        left: &'a Expr,
        right: &'a Expr,
        schema: &'a Schema,
        stat_column_req: &'a mut Vec<(String, StatisticsType, Field)>,
    ) -> Result<Self> {
        // find column name; input could be a more complicated expression
        let mut left_columns = HashSet::<String>::new();
        utils::expr_to_column_names(left, &mut left_columns)?;
        let mut right_columns = HashSet::<String>::new();
        utils::expr_to_column_names(right, &mut right_columns)?;
        let (column_expr, scalar_expr, column_names, reverse_operator) =
            match (left_columns.len(), right_columns.len()) {
                (1, 0) => (left, right, left_columns, false),
                (0, 1) => (right, left, right_columns, true),
                _ => {
                    // if more than one column used in expression - not supported
                    return Err(DataFusionError::Plan(
                        "Multi-column expressions are not currently supported"
                            .to_string(),
                    ));
                }
            };
        let column_name = column_names.iter().next().unwrap().clone();
        let field = match schema.column_with_name(&column_name) {
            Some((_, f)) => f,
            _ => {
                return Err(DataFusionError::Plan(
                    "Field not found in schema".to_string(),
                ));
            }
        };

        Ok(Self {
            column_name,
            column_expr,
            scalar_expr,
            field,
            stat_column_req,
            reverse_operator,
        })
    }

    fn correct_operator(&self, op: Operator) -> Operator {
        if !self.reverse_operator {
            return op;
        }

        match op {
            Operator::Lt => Operator::Gt,
            Operator::Gt => Operator::Lt,
            Operator::LtEq => Operator::GtEq,
            Operator::GtEq => Operator::LtEq,
            _ => op,
        }
    }

    fn scalar_expr(&self) -> &Expr {
        self.scalar_expr
    }

    fn is_stat_column_missing(&self, statistics_type: StatisticsType) -> bool {
        !self
            .stat_column_req
            .iter()
            .any(|(c, t, _f)| c == &self.column_name && t == &statistics_type)
    }

    fn stat_column_expr(
        &mut self,
        stat_type: StatisticsType,
        suffix: &str,
    ) -> Result<Expr> {
        let stat_column_name = format!("{}_{}", self.column_name, suffix);
        let stat_field = Field::new(
            stat_column_name.as_str(),
            self.field.data_type().clone(),
            self.field.is_nullable(),
        );
        if self.is_stat_column_missing(stat_type) {
            // only add statistics column if not previously added
            self.stat_column_req
                .push((self.column_name.clone(), stat_type, stat_field));
        }
        rewrite_column_expr(
            self.column_expr,
            self.column_name.as_str(),
            stat_column_name.as_str(),
        )
    }

    fn min_column_expr(&mut self) -> Result<Expr> {
        self.stat_column_expr(StatisticsType::Min, "min")
    }

    fn max_column_expr(&mut self) -> Result<Expr> {
        self.stat_column_expr(StatisticsType::Max, "max")
    }
}

/// replaces a column with an old name with a new name in an expression
fn rewrite_column_expr(
    expr: &Expr,
    column_old_name: &str,
    column_new_name: &str,
) -> Result<Expr> {
    let expressions = utils::expr_sub_expressions(&expr)?;
    let expressions = expressions
        .iter()
        .map(|e| rewrite_column_expr(e, column_old_name, column_new_name))
        .collect::<Result<Vec<_>>>()?;

    if let Expr::Column(name) = expr {
        if name == column_old_name {
            return Ok(Expr::Column(column_new_name.to_string()));
        }
    }
    utils::rewrite_expression(&expr, &expressions)
}

/// Translate logical filter expression into pruning predicate
/// expression that will evaluate to FALSE if it can be determined no
/// rows between the min/max values could pass the predicates.
///
/// Returns the pruning predicate as an [`Expr`]
fn build_predicate_expression(
    expr: &Expr,
    schema: &Schema,
    stat_column_req: &mut Vec<(String, StatisticsType, Field)>,
) -> Result<Expr> {
    use crate::logical_plan;
    // predicate expression can only be a binary expression
    let (left, op, right) = match expr {
        Expr::BinaryExpr { left, op, right } => (left, *op, right),
        _ => {
            // unsupported expression - replace with TRUE
            // this can still be useful when multiple conditions are joined using AND
            // such as: column > 10 AND TRUE
            return Ok(logical_plan::lit(true));
        }
    };

    if op == Operator::And || op == Operator::Or {
        let left_expr = build_predicate_expression(left, schema, stat_column_req)?;
        let right_expr = build_predicate_expression(right, schema, stat_column_req)?;
        return Ok(logical_plan::binary_expr(left_expr, op, right_expr));
    }

    let expr_builder =
        PruningExpressionBuilder::try_new(left, right, schema, stat_column_req);
    let mut expr_builder = match expr_builder {
        Ok(builder) => builder,
        // allow partial failure in predicate expression generation
        // this can still produce a useful predicate when multiple conditions are joined using AND
        Err(_) => {
            return Ok(logical_plan::lit(true));
        }
    };
    let corrected_op = expr_builder.correct_operator(op);
    let statistics_expr = match corrected_op {
        Operator::Eq => {
            // column = literal => (min, max) = literal => min <= literal && literal <= max
            // (column / 2) = 4 => (column_min / 2) <= 4 && 4 <= (column_max / 2)
            let min_column_expr = expr_builder.min_column_expr()?;
            let max_column_expr = expr_builder.max_column_expr()?;
            min_column_expr
                .lt_eq(expr_builder.scalar_expr().clone())
                .and(expr_builder.scalar_expr().clone().lt_eq(max_column_expr))
        }
        Operator::Gt => {
            // column > literal => (min, max) > literal => max > literal
            expr_builder
                .max_column_expr()?
                .gt(expr_builder.scalar_expr().clone())
        }
        Operator::GtEq => {
            // column >= literal => (min, max) >= literal => max >= literal
            expr_builder
                .max_column_expr()?
                .gt_eq(expr_builder.scalar_expr().clone())
        }
        Operator::Lt => {
            // column < literal => (min, max) < literal => min < literal
            expr_builder
                .min_column_expr()?
                .lt(expr_builder.scalar_expr().clone())
        }
        Operator::LtEq => {
            // column <= literal => (min, max) <= literal => min <= literal
            expr_builder
                .min_column_expr()?
                .lt_eq(expr_builder.scalar_expr().clone())
        }
        // other expressions are not supported
        _ => logical_plan::lit(true),
    };
    Ok(statistics_expr)
}

#[derive(Debug, Copy, Clone, PartialEq)]
enum StatisticsType {
    Min,
    Max,
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::logical_plan::{col, lit};
    use crate::{assert_batches_eq, physical_optimizer::pruning::StatisticsType};
    use arrow::datatypes::{DataType, TimeUnit};

    #[derive(Debug, Default)]
    struct MinMax {
        min: Option<ScalarValue>,
        max: Option<ScalarValue>,
    }

    impl MinMax {
        fn new(min: Option<ScalarValue>, max: Option<ScalarValue>) -> Self {
            Self { min, max }
        }
    }

    #[derive(Debug, Default)]
    struct TestStatistics {
        // key: column name
        stats: HashMap<String, MinMax>,
    }

    impl TestStatistics {
        fn new() -> Self {
            Self::default()
        }

        fn with(mut self, name: impl Into<String>, min_max: MinMax) -> Self {
            self.stats.insert(name.into(), min_max);
            self
        }
    }

    impl PruningStatistics for TestStatistics {
        fn min_value(&self, column: &str) -> Option<ScalarValue> {
            self.stats
                .get(column)
                .map(|s| s.min.clone())
                .unwrap_or(None)
        }

        fn max_value(&self, column: &str) -> Option<ScalarValue> {
            self.stats
                .get(column)
                .map(|s| s.max.clone())
                .unwrap_or(None)
        }
    }

    #[test]
    fn test_build_statistics_record_batch() {
        // Request a record batch with of s1_min, s2_max, s3_max, s3_min
        let stat_column_req = vec![
            // min of original column s1, named s1_min
            (
                "s1".to_string(),
                StatisticsType::Min,
                Field::new("s1_min", DataType::Int32, true),
            ),
            // max of original column s2, named s2_max
            (
                "s2".to_string(),
                StatisticsType::Max,
                Field::new("s2_max", DataType::Int32, true),
            ),
            // max of original column s3, named s3_max
            (
                "s3".to_string(),
                StatisticsType::Max,
                Field::new("s3_max", DataType::Utf8, true),
            ),
            // min of original column s3, named s3_min
            (
                "s3".to_string(),
                StatisticsType::Min,
                Field::new("s3_min", DataType::Utf8, true),
            ),
        ];

        // s1: [None, 10]
        // s2: [2, 20]
        // s3: ["a", "q"]
        let stats1 = TestStatistics::new()
            .with("s1", MinMax::new(None, Some(10i32.into())))
            .with("s2", MinMax::new(Some(2i32.into()), Some(20i32.into())))
            .with("s3", MinMax::new(Some("a".into()), Some("q".into())));

        // s1: [None, None]
        // s2: [None, None]
        // s3: [None, None]
        let stats2 = TestStatistics::new()
            .with("s1", MinMax::new(None, None))
            .with("s2", MinMax::new(None, None))
            .with("s3", MinMax::new(None, None));

        // s1: [9, None]
        // s2: None
        // s3: [None, "r"]
        let stats3 = TestStatistics::new()
            .with("s1", MinMax::new(Some(9i32.into()), None))
            .with("s3", MinMax::new(None, Some("r".into())));

        // This one returns a statistics value, but the value itself is NULL
        // s1: [Some(None), None]
        let stats4 = TestStatistics::new()
            .with("s1", MinMax::new(Some(ScalarValue::Int32(None)), None));

        let statistics = [stats1, stats2, stats3, stats4];
        let batch = build_statistics_record_batch(&statistics, &stat_column_req).unwrap();
        let expected = vec![
            "+--------+--------+--------+--------+",
            "| s1_min | s2_max | s3_max | s3_min |",
            "+--------+--------+--------+--------+",
            "|        | 20     | q      | a      |",
            "|        |        |        |        |",
            "| 9      |        | r      |        |",
            "|        |        |        |        |",
            "+--------+--------+--------+--------+",
        ];

        assert_batches_eq!(expected, &[batch]);
    }

    #[test]
    fn test_build_statistics_casting() {
        // Test requesting a Timestamp column, but getting statistics as Int64
        // which is what Parquet does

        // Request a record batch with of s1_min as a timestamp
        let stat_column_req = vec![(
            "s1".to_string(),
            StatisticsType::Min,
            Field::new(
                "s1_min",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
        )];

        // Note the statistics pass back i64 (not timestamp)
        // s1: [None, 10]
        let stats1 = TestStatistics::new()
            .with("s1", MinMax::new(Some(10i64.into()), Some(20i32.into())));

        let statistics = [stats1];
        let batch = build_statistics_record_batch(&statistics, &stat_column_req).unwrap();
        let expected = vec![
            "+-------------------------------+",
            "| s1_min                        |",
            "+-------------------------------+",
            "| 1970-01-01 00:00:00.000000010 |",
            "+-------------------------------+",
        ];

        assert_batches_eq!(expected, &[batch]);
    }

    #[test]
    fn test_build_statistics_no_stats() {
        let stat_column_req = vec![];

        let stats1 = TestStatistics::new()
            .with("s1", MinMax::new(Some(10i64.into()), Some(20i32.into())));

        let statistics = [stats1];
        let result =
            build_statistics_record_batch(&statistics, &stat_column_req).unwrap_err();
        assert!(
            result.to_string().contains("Invalid argument error"),
            "{}",
            result
        );
    }

    #[test]
    fn test_build_statistics_inconsistent_types() {
        // Test requesting a Utf8 column when the stats return some other type

        // Request a record batch with of s1_min as a timestamp
        let stat_column_req = vec![(
            "s1".to_string(),
            StatisticsType::Min,
            Field::new("s1_min", DataType::Utf8, true),
        )];

        // Note the statistics return binary (which can't be cast to string)
        // s1: [0x255, None]
        let stats1 = TestStatistics::new().with(
            "s1",
            MinMax::new(Some(ScalarValue::Binary(Some(vec![255]))), None),
        );

        let statistics = [stats1];
        let batch = build_statistics_record_batch(&statistics, &stat_column_req).unwrap();
        let expected = vec![
            "+--------+",
            "| s1_min |",
            "+--------+",
            "|        |",
            "+--------+",
        ];

        assert_batches_eq!(expected, &[batch]);
    }

    #[test]
    fn row_group_predicate_eq() -> Result<()> {
        let schema = Schema::new(vec![Field::new("c1", DataType::Int32, false)]);
        let expected_expr = "#c1_min LtEq Int32(1) And Int32(1) LtEq #c1_max";

        // test column on the left
        let expr = col("c1").eq(lit(1));
        let predicate_expr = build_predicate_expression(&expr, &schema, &mut vec![])?;
        assert_eq!(format!("{:?}", predicate_expr), expected_expr);

        // test column on the right
        let expr = lit(1).eq(col("c1"));
        let predicate_expr = build_predicate_expression(&expr, &schema, &mut vec![])?;
        assert_eq!(format!("{:?}", predicate_expr), expected_expr);

        Ok(())
    }

    #[test]
    fn row_group_predicate_gt() -> Result<()> {
        let schema = Schema::new(vec![Field::new("c1", DataType::Int32, false)]);
        let expected_expr = "#c1_max Gt Int32(1)";

        // test column on the left
        let expr = col("c1").gt(lit(1));
        let predicate_expr = build_predicate_expression(&expr, &schema, &mut vec![])?;
        assert_eq!(format!("{:?}", predicate_expr), expected_expr);

        // test column on the right
        let expr = lit(1).lt(col("c1"));
        let predicate_expr = build_predicate_expression(&expr, &schema, &mut vec![])?;
        assert_eq!(format!("{:?}", predicate_expr), expected_expr);

        Ok(())
    }

    #[test]
    fn row_group_predicate_gt_eq() -> Result<()> {
        let schema = Schema::new(vec![Field::new("c1", DataType::Int32, false)]);
        let expected_expr = "#c1_max GtEq Int32(1)";

        // test column on the left
        let expr = col("c1").gt_eq(lit(1));
        let predicate_expr = build_predicate_expression(&expr, &schema, &mut vec![])?;
        assert_eq!(format!("{:?}", predicate_expr), expected_expr);
        // test column on the right
        let expr = lit(1).lt_eq(col("c1"));
        let predicate_expr = build_predicate_expression(&expr, &schema, &mut vec![])?;
        assert_eq!(format!("{:?}", predicate_expr), expected_expr);

        Ok(())
    }

    #[test]
    fn row_group_predicate_lt() -> Result<()> {
        let schema = Schema::new(vec![Field::new("c1", DataType::Int32, false)]);
        let expected_expr = "#c1_min Lt Int32(1)";

        // test column on the left
        let expr = col("c1").lt(lit(1));
        let predicate_expr = build_predicate_expression(&expr, &schema, &mut vec![])?;
        assert_eq!(format!("{:?}", predicate_expr), expected_expr);

        // test column on the right
        let expr = lit(1).gt(col("c1"));
        let predicate_expr = build_predicate_expression(&expr, &schema, &mut vec![])?;
        assert_eq!(format!("{:?}", predicate_expr), expected_expr);

        Ok(())
    }

    #[test]
    fn row_group_predicate_lt_eq() -> Result<()> {
        let schema = Schema::new(vec![Field::new("c1", DataType::Int32, false)]);
        let expected_expr = "#c1_min LtEq Int32(1)";

        // test column on the left
        let expr = col("c1").lt_eq(lit(1));
        let predicate_expr = build_predicate_expression(&expr, &schema, &mut vec![])?;
        assert_eq!(format!("{:?}", predicate_expr), expected_expr);
        // test column on the right
        let expr = lit(1).gt_eq(col("c1"));
        let predicate_expr = build_predicate_expression(&expr, &schema, &mut vec![])?;
        assert_eq!(format!("{:?}", predicate_expr), expected_expr);

        Ok(())
    }

    #[test]
    fn row_group_predicate_and() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("c1", DataType::Int32, false),
            Field::new("c2", DataType::Int32, false),
            Field::new("c3", DataType::Int32, false),
        ]);
        // test AND operator joining supported c1 < 1 expression and unsupported c2 > c3 expression
        let expr = col("c1").lt(lit(1)).and(col("c2").lt(col("c3")));
        let expected_expr = "#c1_min Lt Int32(1) And Boolean(true)";
        let predicate_expr = build_predicate_expression(&expr, &schema, &mut vec![])?;
        assert_eq!(format!("{:?}", predicate_expr), expected_expr);

        Ok(())
    }

    #[test]
    fn row_group_predicate_or() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("c1", DataType::Int32, false),
            Field::new("c2", DataType::Int32, false),
        ]);
        // test OR operator joining supported c1 < 1 expression and unsupported c2 % 2 expression
        let expr = col("c1").lt(lit(1)).or(col("c2").modulus(lit(2)));
        let expected_expr = "#c1_min Lt Int32(1) Or Boolean(true)";
        let predicate_expr = build_predicate_expression(&expr, &schema, &mut vec![])?;
        assert_eq!(format!("{:?}", predicate_expr), expected_expr);

        Ok(())
    }

    #[test]
    fn row_group_predicate_stat_column_req() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("c1", DataType::Int32, false),
            Field::new("c2", DataType::Int32, false),
        ]);
        let mut stat_column_req = vec![];
        // c1 < 1 and (c2 = 2 or c2 = 3)
        let expr = col("c1")
            .lt(lit(1))
            .and(col("c2").eq(lit(2)).or(col("c2").eq(lit(3))));
        let expected_expr = "#c1_min Lt Int32(1) And #c2_min LtEq Int32(2) And Int32(2) LtEq #c2_max Or #c2_min LtEq Int32(3) And Int32(3) LtEq #c2_max";
        let predicate_expr =
            build_predicate_expression(&expr, &schema, &mut stat_column_req)?;
        assert_eq!(format!("{:?}", predicate_expr), expected_expr);
        // c1 < 1 should add c1_min
        let c1_min_field = Field::new("c1_min", DataType::Int32, false);
        assert_eq!(
            stat_column_req[0],
            ("c1".to_owned(), StatisticsType::Min, c1_min_field)
        );
        // c2 = 2 should add c2_min and c2_max
        let c2_min_field = Field::new("c2_min", DataType::Int32, false);
        assert_eq!(
            stat_column_req[1],
            ("c2".to_owned(), StatisticsType::Min, c2_min_field)
        );
        let c2_max_field = Field::new("c2_max", DataType::Int32, false);
        assert_eq!(
            stat_column_req[2],
            ("c2".to_owned(), StatisticsType::Max, c2_max_field)
        );
        // c2 = 3 shouldn't add any new statistics fields
        assert_eq!(stat_column_req.len(), 3);

        Ok(())
    }

    #[test]
    fn prune_api() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("s1", DataType::Utf8, false),
            Field::new("s2", DataType::Int32, false),
        ]));

        // Prune using s2 > 5
        let expr = col("s2").gt(lit(5));

        // s2 [0, 5] ==> no rows should pass
        let stats1 = TestStatistics::new()
            .with("s1", MinMax::new(None, None))
            .with("s2", MinMax::new(Some(0i32.into()), Some(5i32.into())));

        // s2 [4, 6] ==> some rows could pass
        let stats2 = TestStatistics::new()
            .with("s1", MinMax::new(None, None))
            .with("s2", MinMax::new(Some(4i32.into()), Some(6i32.into())));

        // No stats for s2 ==> some rows could pass
        let stats3 = TestStatistics::new();

        // s2 [3, None] (null max) ==> some rows could pass
        let stats4 = TestStatistics::new().with(
            "s2",
            MinMax::new(Some(3i32.into()), Some(ScalarValue::Int32(None))),
        );

        let p = PruningPredicate::try_new(&expr, schema).unwrap();
        let result = p.prune(&[stats1, stats2, stats3, stats4]).unwrap();
        let expected = vec![false, true, true, true];

        assert_eq!(result, expected);
    }
}
