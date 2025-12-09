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

use std::any::Any;
use std::fmt;
use std::hash::Hash;
use std::sync::Arc;

use crate::physical_expr::PhysicalExpr;

use arrow::array::{new_null_array, Array, ArrayRef};
use arrow::compute::{can_cast_types, CastOptions};
use arrow::datatypes::{DataType, DataType::*, FieldRef, Schema};
use arrow::record_batch::RecordBatch;
use datafusion_common::format::DEFAULT_FORMAT_OPTIONS;
use datafusion_common::{nested_struct::cast_column, not_impl_err, Result, ScalarValue};
use datafusion_expr_common::columnar_value::ColumnarValue;
use datafusion_expr_common::interval_arithmetic::Interval;
use datafusion_expr_common::sort_properties::ExprProperties;

const DEFAULT_CAST_OPTIONS: CastOptions<'static> = CastOptions {
    safe: false,
    format_options: DEFAULT_FORMAT_OPTIONS,
};

const DEFAULT_SAFE_CAST_OPTIONS: CastOptions<'static> = CastOptions {
    safe: true,
    format_options: DEFAULT_FORMAT_OPTIONS,
};

/// CAST expression casts an expression to a specific data type and returns a runtime error on invalid cast
#[derive(Debug, Clone, Eq)]
pub struct CastExpr {
    /// The expression to cast
    pub expr: Arc<dyn PhysicalExpr>,
    /// The logical field of the input column.
    input_field: FieldRef,
    /// The field metadata describing the desired output column.
    target_field: FieldRef,
    /// Cast options
    cast_options: CastOptions<'static>,
}

// Manually derive PartialEq and Hash to work around https://github.com/rust-lang/rust/issues/78808
impl PartialEq for CastExpr {
    fn eq(&self, other: &Self) -> bool {
        self.expr.eq(&other.expr)
            && self.input_field.eq(&other.input_field)
            && self.target_field.eq(&other.target_field)
            && self.cast_options.eq(&other.cast_options)
    }
}

impl Hash for CastExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.expr.hash(state);
        self.input_field.hash(state);
        self.target_field.hash(state);
        self.cast_options.hash(state);
    }
}

impl CastExpr {
    /// Create a new CastExpr
    pub fn new(
        expr: Arc<dyn PhysicalExpr>,
        input_field: FieldRef,
        target_field: FieldRef,
        cast_options: Option<CastOptions<'static>>,
    ) -> Self {
        Self {
            expr,
            input_field,
            target_field,
            cast_options: cast_options.unwrap_or(DEFAULT_CAST_OPTIONS),
        }
    }

    /// The expression to cast
    pub fn expr(&self) -> &Arc<dyn PhysicalExpr> {
        &self.expr
    }

    /// The field metadata describing the resolved input column.
    pub fn input_field(&self) -> &FieldRef {
        &self.input_field
    }

    /// Field metadata describing the output column after casting.
    pub fn target_field(&self) -> &FieldRef {
        &self.target_field
    }

    /// The data type to cast to
    pub fn cast_type(&self) -> &DataType {
        self.target_field.data_type()
    }

    /// The cast options
    pub fn cast_options(&self) -> &CastOptions<'static> {
        &self.cast_options
    }

    /// Check if casting from the specified source type to the target type is a
    /// widening cast (e.g. from `Int8` to `Int16`).
    pub fn check_bigger_cast(cast_type: &DataType, src: &DataType) -> bool {
        if cast_type.eq(src) {
            return true;
        }
        matches!(
            (src, cast_type),
            (Int8, Int16 | Int32 | Int64)
                | (Int16, Int32 | Int64)
                | (Int32, Int64)
                | (UInt8, UInt16 | UInt32 | UInt64)
                | (UInt16, UInt32 | UInt64)
                | (UInt32, UInt64)
                | (
                    Int8 | Int16 | Int32 | UInt8 | UInt16 | UInt32,
                    Float32 | Float64
                )
                | (Int64 | UInt64, Float64)
                | (Utf8, LargeUtf8)
        )
    }

    /// Check if the cast is a widening cast (e.g. from `Int8` to `Int16`).
    pub fn is_bigger_cast(&self, src: &DataType) -> bool {
        Self::check_bigger_cast(self.target_field.data_type(), src)
    }

    fn cast_array(&self, array: &ArrayRef) -> Result<ArrayRef> {
        if matches!(self.target_field.data_type(), Struct(_))
            && matches!(array.data_type(), Null)
        {
            return Ok(new_null_array(self.target_field.data_type(), array.len()));
        }

        cast_column(array, self.target_field.as_ref(), &self.cast_options)
    }
}

impl fmt::Display for CastExpr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "CAST({} AS {:?})",
            self.expr,
            self.target_field.data_type()
        )
    }
}

impl PhysicalExpr for CastExpr {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(self.target_field.data_type().clone())
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        // Nullability is defined by the target field metadata to allow
        // callers to enforce a particular nullability when rewriting schemas.
        // This may differ from the input expression's nullability (for example
        // when filling missing struct fields with nulls).
        let _ = input_schema; // reserved for future schema dependent logic
        Ok(self.target_field.is_nullable())
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let value = self.expr.evaluate(batch)?;
        match value {
            ColumnarValue::Array(array) => {
                let casted = self.cast_array(&array)?;
                Ok(ColumnarValue::Array(casted))
            }
            ColumnarValue::Scalar(scalar) => {
                let as_array = scalar.to_array_of_size(1)?;
                let casted = self.cast_array(&as_array)?;
                Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                    casted.as_ref(),
                    0,
                )?))
            }
        }
    }

    fn return_field(&self, input_schema: &Schema) -> Result<FieldRef> {
        let _ = input_schema;
        Ok(Arc::clone(&self.target_field))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.expr]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(CastExpr::new(
            Arc::clone(&children[0]),
            Arc::clone(&self.input_field),
            Arc::clone(&self.target_field),
            Some(self.cast_options.clone()),
        )))
    }

    fn evaluate_bounds(&self, children: &[&Interval]) -> Result<Interval> {
        // Cast current node's interval to the right type:
        children[0].cast_to(self.target_field.data_type(), &self.cast_options)
    }

    fn propagate_constraints(
        &self,
        interval: &Interval,
        children: &[&Interval],
    ) -> Result<Option<Vec<Interval>>> {
        let child_interval = children[0];
        // Get child's datatype:
        let cast_type = child_interval.data_type();
        Ok(Some(vec![
            interval.cast_to(&cast_type, &DEFAULT_SAFE_CAST_OPTIONS)?
        ]))
    }

    /// A [`CastExpr`] preserves the ordering of its child if the cast is done
    /// under the same datatype family.
    fn get_properties(&self, children: &[ExprProperties]) -> Result<ExprProperties> {
        let source_datatype = children[0].range.data_type();
        let target_type = self.target_field.data_type();

        let unbounded = Interval::make_unbounded(target_type)?;
        if (source_datatype.is_numeric() || source_datatype == Boolean)
            && target_type.is_numeric()
            || source_datatype.is_temporal() && target_type.is_temporal()
            || source_datatype.eq(target_type)
        {
            Ok(children[0].clone().with_range(unbounded))
        } else {
            Ok(ExprProperties::new_unknown().with_range(unbounded))
        }
    }

    fn fmt_sql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CAST(")?;
        self.expr.fmt_sql(f)?;
        write!(f, " AS {:?}", self.target_field.data_type())?;

        write!(f, ")")
    }
}

/// Return a PhysicalExpression representing `expr` casted to
/// `cast_type`, if any casting is needed.
///
/// Note that such casts may lose type information
pub fn cast_with_options(
    expr: Arc<dyn PhysicalExpr>,
    input_schema: &Schema,
    cast_type: DataType,
    cast_options: Option<CastOptions<'static>>,
) -> Result<Arc<dyn PhysicalExpr>> {
    let input_field = expr.return_field(input_schema)?;
    let expr_type = input_field.data_type();
    let target_field: FieldRef = Arc::new(
        input_field
            .as_ref()
            .clone()
            .with_data_type(cast_type.clone()),
    );
    let target_type = target_field.data_type();

    if expr_type == target_type {
        Ok(Arc::clone(&expr))
    } else if matches!(target_type, Struct(_)) || can_cast_types(expr_type, target_type) {
        Ok(Arc::new(CastExpr::new(
            expr,
            input_field,
            target_field,
            cast_options,
        )))
    } else {
        not_impl_err!("Unsupported CAST from {expr_type} to {cast_type}")
    }
}

/// Return a PhysicalExpression representing `expr` casted to
/// `cast_type`, if any casting is needed.
///
/// Note that such casts may lose type information
pub fn cast(
    expr: Arc<dyn PhysicalExpr>,
    input_schema: &Schema,
    cast_type: DataType,
) -> Result<Arc<dyn PhysicalExpr>> {
    cast_with_options(expr, input_schema, cast_type, None)
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::expressions::column::col;

    use arrow::{
        array::{
            Array, Decimal128Array, Float32Array, Float64Array, Int16Array, Int32Array,
            Int64Array, Int8Array, StringArray, Time64NanosecondArray,
            TimestampNanosecondArray, UInt32Array,
        },
        datatypes::*,
    };
    use datafusion_physical_expr_common::physical_expr::fmt_sql;
    use insta::assert_snapshot;

    // runs an end-to-end test of physical type cast
    // 1. construct a record batch with a column "a" of type A
    // 2. construct a physical expression of CAST(a AS B)
    // 3. evaluate the expression
    // 4. verify that the resulting expression is of type B
    // 5. verify that the resulting values are downcastable and correct
    macro_rules! generic_decimal_to_other_test_cast {
        ($DECIMAL_ARRAY:ident, $A_TYPE:expr, $TYPEARRAY:ident, $TYPE:expr, $VEC:expr,$CAST_OPTIONS:expr) => {{
            let schema = Schema::new(vec![Field::new("a", $A_TYPE, true)]);
            let batch = RecordBatch::try_new(
                Arc::new(schema.clone()),
                vec![Arc::new($DECIMAL_ARRAY)],
            )?;
            // verify that we can construct the expression
            let expression =
                cast_with_options(col("a", &schema)?, &schema, $TYPE, $CAST_OPTIONS)?;

            // verify that its display is correct
            assert_eq!(
                format!("CAST(a@0 AS {:?})", $TYPE),
                format!("{}", expression)
            );

            // verify that the expression's type is correct
            assert_eq!(expression.data_type(&schema)?, $TYPE);

            // compute
            let result = expression
                .evaluate(&batch)?
                .into_array(batch.num_rows())
                .expect("Failed to convert to array");

            // verify that the array's data_type is correct
            assert_eq!(*result.data_type(), $TYPE);

            // verify that the data itself is downcastable
            let result = result
                .as_any()
                .downcast_ref::<$TYPEARRAY>()
                .expect("failed to downcast");

            // verify that the result itself is correct
            for (i, x) in $VEC.iter().enumerate() {
                match x {
                    Some(x) => assert_eq!(result.value(i), *x),
                    None => assert!(!result.is_valid(i)),
                }
            }
        }};
    }

    // runs an end-to-end test of physical type cast
    // 1. construct a record batch with a column "a" of type A
    // 2. construct a physical expression of CAST(a AS B)
    // 3. evaluate the expression
    // 4. verify that the resulting expression is of type B
    // 5. verify that the resulting values are downcastable and correct
    macro_rules! generic_test_cast {
        ($A_ARRAY:ident, $A_TYPE:expr, $A_VEC:expr, $TYPEARRAY:ident, $TYPE:expr, $VEC:expr, $CAST_OPTIONS:expr) => {{
            let schema = Schema::new(vec![Field::new("a", $A_TYPE, true)]);
            let a_vec_len = $A_VEC.len();
            let a = $A_ARRAY::from($A_VEC);
            let batch =
                RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(a)])?;

            // verify that we can construct the expression
            let expression =
                cast_with_options(col("a", &schema)?, &schema, $TYPE, $CAST_OPTIONS)?;

            // verify that its display is correct
            assert_eq!(
                format!("CAST(a@0 AS {:?})", $TYPE),
                format!("{}", expression)
            );

            // verify that the expression's type is correct
            assert_eq!(expression.data_type(&schema)?, $TYPE);

            // compute
            let result = expression
                .evaluate(&batch)?
                .into_array(batch.num_rows())
                .expect("Failed to convert to array");

            // verify that the array's data_type is correct
            assert_eq!(*result.data_type(), $TYPE);

            // verify that the len is correct
            assert_eq!(result.len(), a_vec_len);

            // verify that the data itself is downcastable
            let result = result
                .as_any()
                .downcast_ref::<$TYPEARRAY>()
                .expect("failed to downcast");

            // verify that the result itself is correct
            for (i, x) in $VEC.iter().enumerate() {
                match x {
                    Some(x) => assert_eq!(result.value(i), *x),
                    None => assert!(!result.is_valid(i)),
                }
            }
        }};
    }

    #[test]
    fn test_cast_decimal_to_decimal() -> Result<()> {
        let array = vec![
            Some(1234),
            Some(2222),
            Some(3),
            Some(4000),
            Some(5000),
            None,
        ];

        let decimal_array = array
            .clone()
            .into_iter()
            .collect::<Decimal128Array>()
            .with_precision_and_scale(10, 3)?;

        generic_decimal_to_other_test_cast!(
            decimal_array,
            Decimal128(10, 3),
            Decimal128Array,
            Decimal128(20, 6),
            [
                Some(1_234_000),
                Some(2_222_000),
                Some(3_000),
                Some(4_000_000),
                Some(5_000_000),
                None
            ],
            None
        );

        let decimal_array = array
            .into_iter()
            .collect::<Decimal128Array>()
            .with_precision_and_scale(10, 3)?;

        generic_decimal_to_other_test_cast!(
            decimal_array,
            Decimal128(10, 3),
            Decimal128Array,
            Decimal128(10, 2),
            [Some(123), Some(222), Some(0), Some(400), Some(500), None],
            None
        );

        Ok(())
    }

    #[test]
    fn test_cast_decimal_to_decimal_overflow() -> Result<()> {
        let array = vec![Some(123456789)];

        let decimal_array = array
            .clone()
            .into_iter()
            .collect::<Decimal128Array>()
            .with_precision_and_scale(10, 3)?;

        let schema = Schema::new(vec![Field::new("a", Decimal128(10, 3), false)]);
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(decimal_array)],
        )?;
        let expression =
            cast_with_options(col("a", &schema)?, &schema, Decimal128(6, 2), None)?;
        let e = expression.evaluate(&batch).unwrap_err().strip_backtrace(); // panics on OK
        assert_snapshot!(e, @"Arrow error: Invalid argument error: 123456.79 is too large to store in a Decimal128 of precision 6. Max is 9999.99");
        // safe cast should return null
        let expression_safe = cast_with_options(
            col("a", &schema)?,
            &schema,
            Decimal128(6, 2),
            Some(DEFAULT_SAFE_CAST_OPTIONS),
        )?;
        let result_safe = expression_safe
            .evaluate(&batch)?
            .into_array(batch.num_rows())
            .expect("failed to convert to array");

        assert!(result_safe.is_null(0));

        Ok(())
    }

    #[test]
    fn test_cast_decimal_to_numeric() -> Result<()> {
        let array = vec![Some(1), Some(2), Some(3), Some(4), Some(5), None];
        // decimal to i8
        let decimal_array = array
            .clone()
            .into_iter()
            .collect::<Decimal128Array>()
            .with_precision_and_scale(10, 0)?;
        generic_decimal_to_other_test_cast!(
            decimal_array,
            Decimal128(10, 0),
            Int8Array,
            Int8,
            [
                Some(1_i8),
                Some(2_i8),
                Some(3_i8),
                Some(4_i8),
                Some(5_i8),
                None
            ],
            None
        );

        // decimal to i16
        let decimal_array = array
            .clone()
            .into_iter()
            .collect::<Decimal128Array>()
            .with_precision_and_scale(10, 0)?;
        generic_decimal_to_other_test_cast!(
            decimal_array,
            Decimal128(10, 0),
            Int16Array,
            Int16,
            [
                Some(1_i16),
                Some(2_i16),
                Some(3_i16),
                Some(4_i16),
                Some(5_i16),
                None
            ],
            None
        );

        // decimal to i32
        let decimal_array = array
            .clone()
            .into_iter()
            .collect::<Decimal128Array>()
            .with_precision_and_scale(10, 0)?;
        generic_decimal_to_other_test_cast!(
            decimal_array,
            Decimal128(10, 0),
            Int32Array,
            Int32,
            [
                Some(1_i32),
                Some(2_i32),
                Some(3_i32),
                Some(4_i32),
                Some(5_i32),
                None
            ],
            None
        );

        // decimal to i64
        let decimal_array = array
            .into_iter()
            .collect::<Decimal128Array>()
            .with_precision_and_scale(10, 0)?;
        generic_decimal_to_other_test_cast!(
            decimal_array,
            Decimal128(10, 0),
            Int64Array,
            Int64,
            [
                Some(1_i64),
                Some(2_i64),
                Some(3_i64),
                Some(4_i64),
                Some(5_i64),
                None
            ],
            None
        );

        // decimal to float32
        let array = vec![
            Some(1234),
            Some(2222),
            Some(3),
            Some(4000),
            Some(5000),
            None,
        ];
        let decimal_array = array
            .clone()
            .into_iter()
            .collect::<Decimal128Array>()
            .with_precision_and_scale(10, 3)?;
        generic_decimal_to_other_test_cast!(
            decimal_array,
            Decimal128(10, 3),
            Float32Array,
            Float32,
            [
                Some(1.234_f32),
                Some(2.222_f32),
                Some(0.003_f32),
                Some(4.0_f32),
                Some(5.0_f32),
                None
            ],
            None
        );

        // decimal to float64
        let decimal_array = array
            .into_iter()
            .collect::<Decimal128Array>()
            .with_precision_and_scale(20, 6)?;
        generic_decimal_to_other_test_cast!(
            decimal_array,
            Decimal128(20, 6),
            Float64Array,
            Float64,
            [
                Some(0.001234_f64),
                Some(0.002222_f64),
                Some(0.000003_f64),
                Some(0.004_f64),
                Some(0.005_f64),
                None
            ],
            None
        );
        Ok(())
    }

    #[test]
    fn test_cast_numeric_to_decimal() -> Result<()> {
        // int8
        generic_test_cast!(
            Int8Array,
            Int8,
            vec![1, 2, 3, 4, 5],
            Decimal128Array,
            Decimal128(3, 0),
            [Some(1), Some(2), Some(3), Some(4), Some(5)],
            None
        );

        // int16
        generic_test_cast!(
            Int16Array,
            Int16,
            vec![1, 2, 3, 4, 5],
            Decimal128Array,
            Decimal128(5, 0),
            [Some(1), Some(2), Some(3), Some(4), Some(5)],
            None
        );

        // int32
        generic_test_cast!(
            Int32Array,
            Int32,
            vec![1, 2, 3, 4, 5],
            Decimal128Array,
            Decimal128(10, 0),
            [Some(1), Some(2), Some(3), Some(4), Some(5)],
            None
        );

        // int64
        generic_test_cast!(
            Int64Array,
            Int64,
            vec![1, 2, 3, 4, 5],
            Decimal128Array,
            Decimal128(20, 0),
            [Some(1), Some(2), Some(3), Some(4), Some(5)],
            None
        );

        // int64 to different scale
        generic_test_cast!(
            Int64Array,
            Int64,
            vec![1, 2, 3, 4, 5],
            Decimal128Array,
            Decimal128(20, 2),
            [Some(100), Some(200), Some(300), Some(400), Some(500)],
            None
        );

        // float32
        generic_test_cast!(
            Float32Array,
            Float32,
            vec![1.5, 2.5, 3.0, 1.123_456_8, 5.50],
            Decimal128Array,
            Decimal128(10, 2),
            [Some(150), Some(250), Some(300), Some(112), Some(550)],
            None
        );

        // float64
        generic_test_cast!(
            Float64Array,
            Float64,
            vec![1.5, 2.5, 3.0, 1.123_456_8, 5.50],
            Decimal128Array,
            Decimal128(20, 4),
            [
                Some(15000),
                Some(25000),
                Some(30000),
                Some(11235),
                Some(55000)
            ],
            None
        );
        Ok(())
    }

    #[test]
    fn test_cast_i32_u32() -> Result<()> {
        generic_test_cast!(
            Int32Array,
            Int32,
            vec![1, 2, 3, 4, 5],
            UInt32Array,
            UInt32,
            [
                Some(1_u32),
                Some(2_u32),
                Some(3_u32),
                Some(4_u32),
                Some(5_u32)
            ],
            None
        );
        Ok(())
    }

    #[test]
    fn test_cast_i32_utf8() -> Result<()> {
        generic_test_cast!(
            Int32Array,
            Int32,
            vec![1, 2, 3, 4, 5],
            StringArray,
            Utf8,
            [Some("1"), Some("2"), Some("3"), Some("4"), Some("5")],
            None
        );
        Ok(())
    }

    #[test]
    fn test_cast_i64_t64() -> Result<()> {
        let original = vec![1, 2, 3, 4, 5];
        let expected: Vec<Option<i64>> = original
            .iter()
            .map(|i| Some(Time64NanosecondArray::from(vec![*i]).value(0)))
            .collect();
        generic_test_cast!(
            Int64Array,
            Int64,
            original,
            TimestampNanosecondArray,
            Timestamp(TimeUnit::Nanosecond, None),
            expected,
            None
        );
        Ok(())
    }

    // Tests for timestamp timezone casting have been moved to timestamps.slt
    // See the "Casting between timestamp with and without timezone" section

    #[test]
    fn invalid_cast() {
        // Ensure a useful error happens at plan time if invalid casts are used
        let schema = Schema::new(vec![Field::new("a", Int32, false)]);

        let result = cast(
            col("a", &schema).unwrap(),
            &schema,
            Interval(IntervalUnit::MonthDayNano),
        );
        result.expect_err("expected Invalid CAST");
    }

    #[test]
    fn invalid_cast_with_options_error() -> Result<()> {
        // Ensure a useful error happens at plan time if invalid casts are used
        let schema = Schema::new(vec![Field::new("a", Utf8, false)]);
        let a = StringArray::from(vec!["9.1"]);
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(a)])?;
        let expression = cast_with_options(col("a", &schema)?, &schema, Int32, None)?;
        let result = expression.evaluate(&batch);

        match result {
            Ok(_) => panic!("expected error"),
            Err(e) => {
                assert!(e
                    .to_string()
                    .contains("Cannot cast string '9.1' to value of Int32 type"))
            }
        }
        Ok(())
    }

    #[test]
    #[ignore] // TODO: https://github.com/apache/datafusion/issues/5396
    fn test_cast_decimal() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", Int64, false)]);
        let a = Int64Array::from(vec![100]);
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(a)])?;
        let expression =
            cast_with_options(col("a", &schema)?, &schema, Decimal128(38, 38), None)?;
        expression.evaluate(&batch)?;
        Ok(())
    }

    #[test]
    fn test_fmt_sql() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", Int32, true)]);

        // Test numeric casting
        let expr = cast(col("a", &schema)?, &schema, Int64)?;
        let display_string = expr.to_string();
        assert_eq!(display_string, "CAST(a@0 AS Int64)");
        let sql_string = fmt_sql(expr.as_ref()).to_string();
        assert_eq!(sql_string, "CAST(a AS Int64)");

        // Test string casting
        let schema = Schema::new(vec![Field::new("b", Utf8, true)]);
        let expr = cast(col("b", &schema)?, &schema, Int32)?;
        let display_string = expr.to_string();
        assert_eq!(display_string, "CAST(b@0 AS Int32)");
        let sql_string = fmt_sql(expr.as_ref()).to_string();
        assert_eq!(sql_string, "CAST(b AS Int32)");

        Ok(())
    }

    mod struct_casts {
        use super::*;
        use crate::expressions::{Column, Literal};
        use arrow::array::{
            Array, ArrayRef, BooleanArray, Int32Array, StringArray, StructArray,
        };
        use arrow::datatypes::{Field, Fields, SchemaRef};
        use datafusion_common::{
            cast::{as_int64_array, as_string_array, as_struct_array, as_uint8_array},
            Result as DFResult, ScalarValue,
        };
        use datafusion_expr_common::columnar_value::ColumnarValue;

        fn make_schema(field: &Field) -> SchemaRef {
            Arc::new(Schema::new(vec![field.clone()]))
        }

        fn make_struct_array(fields: Fields, arrays: Vec<ArrayRef>) -> StructArray {
            StructArray::new(fields, arrays, None)
        }

        #[test]
        fn cast_primitive_array() -> DFResult<()> {
            let input_field = Field::new("a", Int32, true);
            let target_field = Field::new("a", Int64, true);
            let schema = make_schema(&input_field);

            let values = Arc::new(Int32Array::from(vec![Some(1), None, Some(3)]));
            let batch = RecordBatch::try_new(Arc::clone(&schema), vec![values])?;

            let column = Arc::new(Column::new_with_schema("a", schema.as_ref())?);
            let expr = CastExpr::new(
                column,
                Arc::new(input_field.clone()),
                Arc::new(target_field.clone()),
                None,
            );

            let result = expr.evaluate(&batch)?;
            let ColumnarValue::Array(array) = result else {
                panic!("expected array");
            };
            let casted = as_int64_array(array.as_ref())?;
            assert_eq!(casted.value(0), 1);
            assert!(casted.is_null(1));
            assert_eq!(casted.value(2), 3);
            Ok(())
        }

        #[test]
        fn cast_struct_array_missing_child() -> DFResult<()> {
            let source_a = Field::new("a", Int32, true);
            let source_b = Field::new("b", Utf8, true);
            let input_field = Field::new(
                "s",
                Struct(
                    vec![Arc::new(source_a.clone()), Arc::new(source_b.clone())].into(),
                ),
                true,
            );
            let target_a = Field::new("a", Int64, true);
            let target_c = Field::new("c", Utf8, true);
            let target_field = Field::new(
                "s",
                Struct(
                    vec![Arc::new(target_a.clone()), Arc::new(target_c.clone())].into(),
                ),
                true,
            );

            let schema = make_schema(&input_field);
            let struct_array = make_struct_array(
                vec![Arc::new(source_a.clone()), Arc::new(source_b.clone())].into(),
                vec![
                    Arc::new(Int32Array::from(vec![Some(1), None])) as ArrayRef,
                    Arc::new(StringArray::from(vec![Some("alpha"), Some("beta")]))
                        as ArrayRef,
                ],
            );
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(struct_array) as Arc<_>],
            )?;

            let column = Arc::new(Column::new_with_schema("s", schema.as_ref())?);
            let expr = CastExpr::new(
                column,
                Arc::new(input_field.clone()),
                Arc::new(target_field.clone()),
                None,
            );

            let result = expr.evaluate(&batch)?;
            let ColumnarValue::Array(array) = result else {
                panic!("expected array");
            };
            let struct_array = as_struct_array(array.as_ref())?;
            let cast_a =
                as_int64_array(struct_array.column_by_name("a").unwrap().as_ref())?;
            assert_eq!(cast_a.value(0), 1);
            assert!(cast_a.is_null(1));

            let cast_c =
                as_string_array(struct_array.column_by_name("c").unwrap().as_ref())?;
            assert!(cast_c.is_null(0));
            assert!(cast_c.is_null(1));
            Ok(())
        }

        #[test]
        fn cast_nested_struct_array() -> DFResult<()> {
            let inner_source = Field::new(
                "inner",
                Struct(vec![Arc::new(Field::new("x", Int32, true))].into()),
                true,
            );
            let outer_field = Field::new(
                "root",
                Struct(vec![Arc::new(inner_source.clone())].into()),
                true,
            );

            let inner_target = Field::new(
                "inner",
                Struct(
                    vec![
                        Arc::new(Field::new("x", Int64, true)),
                        Arc::new(Field::new("y", Boolean, true)),
                    ]
                    .into(),
                ),
                true,
            );
            let target_field = Field::new(
                "root",
                Struct(vec![Arc::new(inner_target.clone())].into()),
                true,
            );

            let schema = make_schema(&outer_field);

            let inner_struct = make_struct_array(
                vec![Arc::new(Field::new("x", Int32, true))].into(),
                vec![Arc::new(Int32Array::from(vec![Some(7), None])) as ArrayRef],
            );
            let outer_struct = make_struct_array(
                vec![Arc::new(inner_source.clone())].into(),
                vec![Arc::new(inner_struct) as ArrayRef],
            );
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(outer_struct) as ArrayRef],
            )?;

            let column = Arc::new(Column::new_with_schema("root", schema.as_ref())?);
            let expr = CastExpr::new(
                column,
                Arc::new(outer_field.clone()),
                Arc::new(target_field.clone()),
                None,
            );

            let result = expr.evaluate(&batch)?;
            let ColumnarValue::Array(array) = result else {
                panic!("expected array");
            };
            let struct_array = as_struct_array(array.as_ref())?;
            let inner =
                as_struct_array(struct_array.column_by_name("inner").unwrap().as_ref())?;
            let x = as_int64_array(inner.column_by_name("x").unwrap().as_ref())?;
            assert_eq!(x.value(0), 7);
            assert!(x.is_null(1));
            let y = inner.column_by_name("y").unwrap();
            let y = y
                .as_any()
                .downcast_ref::<BooleanArray>()
                .expect("boolean array");
            assert!(y.is_null(0));
            assert!(y.is_null(1));
            Ok(())
        }

        #[test]
        fn cast_struct_scalar() -> DFResult<()> {
            let source_field = Field::new("a", Int32, true);
            let input_field = Field::new(
                "s",
                Struct(vec![Arc::new(source_field.clone())].into()),
                true,
            );
            let target_field = Field::new(
                "s",
                Struct(vec![Arc::new(Field::new("a", UInt8, true))].into()),
                true,
            );

            let schema = make_schema(&input_field);
            let scalar_struct = StructArray::new(
                vec![Arc::new(source_field.clone())].into(),
                vec![Arc::new(Int32Array::from(vec![Some(9)])) as ArrayRef],
                None,
            );
            let literal =
                Arc::new(Literal::new(ScalarValue::Struct(Arc::new(scalar_struct))));
            let expr = CastExpr::new(
                literal,
                Arc::new(input_field.clone()),
                Arc::new(target_field.clone()),
                None,
            );

            let batch = RecordBatch::new_empty(Arc::clone(&schema));
            let result = expr.evaluate(&batch)?;
            let ColumnarValue::Scalar(ScalarValue::Struct(array)) = result else {
                panic!("expected struct scalar");
            };
            let casted = array.column_by_name("a").unwrap();
            let casted = as_uint8_array(casted.as_ref())?;
            assert_eq!(casted.value(0), 9);
            Ok(())
        }
    }
}
