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

use std::{collections::HashSet, sync::Arc};

use arrow::{
    array::{
        make_array, new_null_array, ArrayData, ArrayRef, BooleanArray,
        BooleanBufferBuilder,
    },
    buffer::MutableBuffer,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};

use parquet::file::{
    metadata::RowGroupMetaData, statistics::Statistics as ParquetStatistics,
};

use crate::{
    error::{DataFusionError, Result},
    execution::context::ExecutionContextState,
    logical_plan::{Expr, Operator},
    optimizer::utils,
    physical_plan::{planner::DefaultPhysicalPlanner, ColumnarValue, PhysicalExpr},
};

#[derive(Debug, Clone)]
/// Builder used for generating predicate functions that can be used
/// to prune data based on statistics (e.g. parquet row group metadata)
pub struct PruningPredicateBuilder {
    schema: Schema,
    predicate_expr: Arc<dyn PhysicalExpr>,
    stat_column_req: Vec<(String, StatisticsType, Field)>,
}

impl PruningPredicateBuilder {
    /// Try to create a new instance of [`PruningPredicateBuilder`]
    ///
    /// This will translate the filter expression into a statistics predicate expression
    ///
    /// For example,  `(column / 2) = 4` becomes `(column_min / 2) <= 4 && 4 <= (column_max / 2))`
    pub fn try_new(expr: &Expr, schema: Schema) -> Result<Self> {
        // build predicate expression once
        let mut stat_column_req = Vec::<(String, StatisticsType, Field)>::new();
        let logical_predicate_expr =
            build_predicate_expression(expr, &schema, &mut stat_column_req)?;
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

    /// For each set of statistics, evalates the predicate in this
    /// builder and returns a `bool` with the following meaning for a
    /// container with those statistics:
    ///
    /// `true`: The container MAY contain rows that match the predicate
    ///
    /// `false`: The container MUST NOT contain rows that match the predicate
    ///
    /// Note this function takes a slice of statistics as a parameter
    /// to amortize the cost of the evaluation of the predicate
    /// against a single record batch.
    pub fn build_pruning_predicate(
        &self,
        statistics: &[RowGroupMetaData],
    ) -> Result<Vec<bool>> {
        // build statistics record batch
        let predicate_array = build_statistics_record_batch(
            statistics,
            &self.schema,
            &self.stat_column_req,
        )
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
}

/// Build a RecordBatch from a list of statistics (currently parquet
/// [`RowGroupMetadata`] structs), creating arrays, one for each
/// statistics column, as requested in the stat_column_req parameter.
fn build_statistics_record_batch(
    statistics: &[RowGroupMetaData],
    schema: &Schema,
    stat_column_req: &[(String, StatisticsType, Field)],
) -> Result<RecordBatch> {
    let mut fields = Vec::<Field>::new();
    let mut arrays = Vec::<ArrayRef>::new();
    for (column_name, statistics_type, stat_field) in stat_column_req {
        if let Some((column_index, _)) = schema.column_with_name(column_name) {
            let statistics = statistics
                .iter()
                .map(|g| g.column(column_index).statistics())
                .collect::<Vec<_>>();
            let array = build_statistics_array(
                &statistics,
                *statistics_type,
                stat_field.data_type(),
            );
            fields.push(stat_field.clone());
            arrays.push(array);
        }
    }
    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, arrays)
        .map_err(|err| DataFusionError::Plan(err.to_string()))
}

struct StatisticsExpressionBuilder<'a> {
    column_name: String,
    column_expr: &'a Expr,
    scalar_expr: &'a Expr,
    field: &'a Field,
    stat_column_req: &'a mut Vec<(String, StatisticsType, Field)>,
    reverse_operator: bool,
}

impl<'a> StatisticsExpressionBuilder<'a> {
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

/// Translate logical filter expression into statistics predicate expression
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
        StatisticsExpressionBuilder::try_new(left, right, schema, stat_column_req);
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

fn build_statistics_array(
    statistics: &[Option<&ParquetStatistics>],
    statistics_type: StatisticsType,
    data_type: &DataType,
) -> ArrayRef {
    let statistics_count = statistics.len();
    let first_group_stats = statistics.iter().find(|s| s.is_some());
    let first_group_stats = if let Some(Some(statistics)) = first_group_stats {
        // found first row group with statistics defined
        statistics
    } else {
        // no row group has statistics defined
        return new_null_array(data_type, statistics_count);
    };

    let (data_size, arrow_type) = match first_group_stats {
        ParquetStatistics::Int32(_) => (std::mem::size_of::<i32>(), DataType::Int32),
        ParquetStatistics::Int64(_) => (std::mem::size_of::<i64>(), DataType::Int64),
        ParquetStatistics::Float(_) => (std::mem::size_of::<f32>(), DataType::Float32),
        ParquetStatistics::Double(_) => (std::mem::size_of::<f64>(), DataType::Float64),
        ParquetStatistics::ByteArray(_) if data_type == &DataType::Utf8 => {
            (0, DataType::Utf8)
        }
        _ => {
            // type of statistics not supported
            return new_null_array(data_type, statistics_count);
        }
    };

    let statistics = statistics.iter().map(|s| {
        s.filter(|s| s.has_min_max_set())
            .map(|s| match statistics_type {
                StatisticsType::Min => s.min_bytes(),
                StatisticsType::Max => s.max_bytes(),
            })
    });

    if arrow_type == DataType::Utf8 {
        let data_size = statistics
            .clone()
            .map(|x| x.map(|b| b.len()).unwrap_or(0))
            .sum();
        let mut builder =
            arrow::array::StringBuilder::with_capacity(statistics_count, data_size);
        let string_statistics =
            statistics.map(|x| x.and_then(|bytes| std::str::from_utf8(bytes).ok()));
        for maybe_string in string_statistics {
            match maybe_string {
                Some(string_value) => builder.append_value(string_value).unwrap(),
                None => builder.append_null().unwrap(),
            };
        }
        return Arc::new(builder.finish());
    }

    let mut data_buffer = MutableBuffer::new(statistics_count * data_size);
    let mut bitmap_builder = BooleanBufferBuilder::new(statistics_count);
    let mut null_count = 0;
    for s in statistics {
        if let Some(stat_data) = s {
            bitmap_builder.append(true);
            data_buffer.extend_from_slice(stat_data);
        } else {
            bitmap_builder.append(false);
            data_buffer.resize(data_buffer.len() + data_size, 0);
            null_count += 1;
        }
    }

    let mut builder = ArrayData::builder(arrow_type)
        .len(statistics_count)
        .add_buffer(data_buffer.into());
    if null_count > 0 {
        builder = builder.null_bit_buffer(bitmap_builder.finish());
    }
    let array_data = builder.build();
    let statistics_array = make_array(array_data);
    if statistics_array.data_type() == data_type {
        return statistics_array;
    }
    // cast statistics array to required data type
    arrow::compute::cast(&statistics_array, data_type)
        .unwrap_or_else(|_| new_null_array(data_type, statistics_count))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::physical_optimizer::pruning::StatisticsType;
    use arrow::{
        array::{Int32Array, StringArray},
        datatypes::DataType,
    };
    use parquet::file::statistics::Statistics as ParquetStatistics;

    #[test]
    fn build_statistics_array_int32() {
        // build row group metadata array
        let s1 = ParquetStatistics::int32(None, Some(10), None, 0, false);
        let s2 = ParquetStatistics::int32(Some(2), Some(20), None, 0, false);
        let s3 = ParquetStatistics::int32(Some(3), Some(30), None, 0, false);
        let statistics = vec![Some(&s1), Some(&s2), Some(&s3)];

        let statistics_array =
            build_statistics_array(&statistics, StatisticsType::Min, &DataType::Int32);
        let int32_array = statistics_array
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let int32_vec = int32_array.into_iter().collect::<Vec<_>>();
        assert_eq!(int32_vec, vec![None, Some(2), Some(3)]);

        let statistics_array =
            build_statistics_array(&statistics, StatisticsType::Max, &DataType::Int32);
        let int32_array = statistics_array
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let int32_vec = int32_array.into_iter().collect::<Vec<_>>();
        // here the first max value is None and not the Some(10) value which was actually set
        // because the min value is None
        assert_eq!(int32_vec, vec![None, Some(20), Some(30)]);
    }

    #[test]
    fn build_statistics_array_utf8() {
        // build row group metadata array
        let s1 = ParquetStatistics::byte_array(None, Some("10".into()), None, 0, false);
        let s2 = ParquetStatistics::byte_array(
            Some("2".into()),
            Some("20".into()),
            None,
            0,
            false,
        );
        let s3 = ParquetStatistics::byte_array(
            Some("3".into()),
            Some("30".into()),
            None,
            0,
            false,
        );
        let statistics = vec![Some(&s1), Some(&s2), Some(&s3)];

        let statistics_array =
            build_statistics_array(&statistics, StatisticsType::Min, &DataType::Utf8);
        let string_array = statistics_array
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let string_vec = string_array.into_iter().collect::<Vec<_>>();
        assert_eq!(string_vec, vec![None, Some("2"), Some("3")]);

        let statistics_array =
            build_statistics_array(&statistics, StatisticsType::Max, &DataType::Utf8);
        let string_array = statistics_array
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let string_vec = string_array.into_iter().collect::<Vec<_>>();
        // here the first max value is None and not the Some("10") value which was actually set
        // because the min value is None
        assert_eq!(string_vec, vec![None, Some("20"), Some("30")]);
    }

    #[test]
    fn build_statistics_array_empty_stats() {
        let data_type = DataType::Int32;
        let statistics = vec![];
        let statistics_array =
            build_statistics_array(&statistics, StatisticsType::Min, &data_type);
        assert_eq!(statistics_array.len(), 0);

        let statistics = vec![None, None];
        let statistics_array =
            build_statistics_array(&statistics, StatisticsType::Min, &data_type);
        assert_eq!(statistics_array.len(), statistics.len());
        assert_eq!(statistics_array.data_type(), &data_type);
        for i in 0..statistics_array.len() {
            assert_eq!(statistics_array.is_null(i), true);
            assert_eq!(statistics_array.is_valid(i), false);
        }
    }

    #[test]
    fn build_statistics_array_unsupported_type() {
        // boolean is not currently a supported type for statistics
        let s1 = ParquetStatistics::boolean(Some(false), Some(true), None, 0, false);
        let s2 = ParquetStatistics::boolean(Some(false), Some(true), None, 0, false);
        let statistics = vec![Some(&s1), Some(&s2)];
        let data_type = DataType::Boolean;
        let statistics_array =
            build_statistics_array(&statistics, StatisticsType::Min, &data_type);
        assert_eq!(statistics_array.len(), statistics.len());
        assert_eq!(statistics_array.data_type(), &data_type);
        for i in 0..statistics_array.len() {
            assert_eq!(statistics_array.is_null(i), true);
            assert_eq!(statistics_array.is_valid(i), false);
        }
    }

    #[test]
    fn row_group_predicate_eq() -> Result<()> {
        use crate::logical_plan::{col, lit};
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
        use crate::logical_plan::{col, lit};
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
        use crate::logical_plan::{col, lit};
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
        use crate::logical_plan::{col, lit};
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
        use crate::logical_plan::{col, lit};
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
        use crate::logical_plan::{col, lit};
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
        use crate::logical_plan::{col, lit};
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
        use crate::logical_plan::{col, lit};
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
}
