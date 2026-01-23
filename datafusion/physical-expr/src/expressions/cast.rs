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

use arrow::compute::{CastOptions, can_cast_types};
use arrow::datatypes::{DataType, DataType::*, FieldRef, Schema};
use arrow::record_batch::RecordBatch;
use datafusion_common::format::DEFAULT_FORMAT_OPTIONS;
use datafusion_common::nested_struct::validate_struct_compatibility;
use datafusion_common::{Result, not_impl_err};
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

/// Check if struct-to-struct casting is allowed by validating field compatibility.
///
/// This function applies the same validation rules as execution time to ensure
/// planning-time validation matches runtime validation, enabling fail-fast behavior
/// instead of deferring errors to execution.
fn can_cast_struct_types(source: &DataType, target: &DataType) -> bool {
    match (source, target) {
        (Struct(source_fields), Struct(target_fields)) => {
            // Apply the same struct compatibility rules as at execution time.
            // This ensures planning-time validation matches execution-time validation.
            validate_struct_compatibility(source_fields, target_fields).is_ok()
        }
        _ => false,
    }
}

/// CAST expression casts an expression to a specific data type and returns a runtime error on invalid cast
#[derive(Debug, Clone, Eq)]
pub struct CastExpr {
    /// The expression to cast
    pub expr: Arc<dyn PhysicalExpr>,
    /// The data type to cast to
    cast_type: DataType,
    /// Cast options
    cast_options: CastOptions<'static>,
}

// Manually derive PartialEq and Hash to work around https://github.com/rust-lang/rust/issues/78808
impl PartialEq for CastExpr {
    fn eq(&self, other: &Self) -> bool {
        self.expr.eq(&other.expr)
            && self.cast_type.eq(&other.cast_type)
            && self.cast_options.eq(&other.cast_options)
    }
}

impl Hash for CastExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.expr.hash(state);
        self.cast_type.hash(state);
        self.cast_options.hash(state);
    }
}

impl CastExpr {
    /// Create a new CastExpr
    pub fn new(
        expr: Arc<dyn PhysicalExpr>,
        cast_type: DataType,
        cast_options: Option<CastOptions<'static>>,
    ) -> Self {
        Self {
            expr,
            cast_type,
            cast_options: cast_options.unwrap_or(DEFAULT_CAST_OPTIONS),
        }
    }

    /// The expression to cast
    pub fn expr(&self) -> &Arc<dyn PhysicalExpr> {
        &self.expr
    }

    /// The data type to cast to
    pub fn cast_type(&self) -> &DataType {
        &self.cast_type
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
        Self::check_bigger_cast(&self.cast_type, src)
    }
}

impl fmt::Display for CastExpr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CAST({} AS {:?})", self.expr, self.cast_type)
    }
}

impl PhysicalExpr for CastExpr {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(self.cast_type.clone())
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        self.expr.nullable(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let value = self.expr.evaluate(batch)?;
        value.cast_to(&self.cast_type, Some(&self.cast_options))
    }

    fn return_field(&self, input_schema: &Schema) -> Result<FieldRef> {
        Ok(self
            .expr
            .return_field(input_schema)?
            .as_ref()
            .clone()
            .with_data_type(self.cast_type.clone())
            .into())
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
            self.cast_type.clone(),
            Some(self.cast_options.clone()),
        )))
    }

    fn evaluate_bounds(&self, children: &[&Interval]) -> Result<Interval> {
        // Cast current node's interval to the right type:
        children[0].cast_to(&self.cast_type, &self.cast_options)
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
            interval.cast_to(&cast_type, &DEFAULT_SAFE_CAST_OPTIONS)?,
        ]))
    }

    /// A [`CastExpr`] preserves the ordering of its child if the cast is done
    /// under the same datatype family.
    fn get_properties(&self, children: &[ExprProperties]) -> Result<ExprProperties> {
        let source_datatype = children[0].range.data_type();
        let target_type = &self.cast_type;

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
        write!(f, " AS {:?}", self.cast_type)?;

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
    let expr_type = expr.data_type(input_schema)?;
    if expr_type == cast_type {
        Ok(Arc::clone(&expr))
    } else if can_cast_types(&expr_type, &cast_type) {
        Ok(Arc::new(CastExpr::new(expr, cast_type, cast_options)))
    } else if can_cast_struct_types(&expr_type, &cast_type) {
        // Allow struct-to-struct casts that pass name-based compatibility validation.
        // This validation is applied at planning time (now) to fail fast, rather than
        // deferring errors to execution time. The name-based casting logic will be
        // executed at runtime via ColumnarValue::cast_to.
        Ok(Arc::new(CastExpr::new(expr, cast_type, cast_options)))
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
            Array, Decimal128Array, Float32Array, Float64Array, Int8Array, Int16Array,
            Int32Array, Int64Array, StringArray, Time64NanosecondArray,
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
                assert!(
                    e.to_string()
                        .contains("Cannot cast string '9.1' to value of Int32 type")
                )
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
}
