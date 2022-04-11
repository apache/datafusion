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

use std::{any::Any, sync::Arc};

use crate::expressions::try_cast;
use crate::PhysicalExpr;
use arrow::array::*;
use arrow::compute::kernels::zip::zip;
use arrow::compute::{and, eq_dyn, is_null, not, or, or_kleene};
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::ColumnarValue;

type WhenThen = (Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>);

/// The CASE expression is similar to a series of nested if/else and there are two forms that
/// can be used. The first form consists of a series of boolean "when" expressions with
/// corresponding "then" expressions, and an optional "else" expression.
///
/// CASE WHEN condition THEN result
///      [WHEN ...]
///      [ELSE result]
/// END
///
/// The second form uses a base expression and then a series of "when" clauses that match on a
/// literal value.
///
/// CASE expression
///     WHEN value THEN result
///     [WHEN ...]
///     [ELSE result]
/// END
#[derive(Debug)]
pub struct CaseExpr {
    /// Optional base expression that can be compared to literal values in the "when" expressions
    expr: Option<Arc<dyn PhysicalExpr>>,
    /// One or more when/then expressions
    when_then_expr: Vec<WhenThen>,
    /// Optional "else" expression
    else_expr: Option<Arc<dyn PhysicalExpr>>,
}

impl std::fmt::Display for CaseExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "CASE ")?;
        if let Some(e) = &self.expr {
            write!(f, "{} ", e)?;
        }
        for (w, t) in &self.when_then_expr {
            write!(f, "WHEN {} THEN {} ", w, t)?;
        }
        if let Some(e) = &self.else_expr {
            write!(f, "ELSE {} ", e)?;
        }
        write!(f, "END")
    }
}

impl CaseExpr {
    /// Create a new CASE WHEN expression
    pub fn try_new(
        expr: Option<Arc<dyn PhysicalExpr>>,
        when_then_expr: &[WhenThen],
        else_expr: Option<Arc<dyn PhysicalExpr>>,
    ) -> Result<Self> {
        if when_then_expr.is_empty() {
            Err(DataFusionError::Execution(
                "There must be at least one WHEN clause".to_string(),
            ))
        } else {
            Ok(Self {
                expr,
                when_then_expr: when_then_expr.to_vec(),
                else_expr,
            })
        }
    }

    /// Optional base expression that can be compared to literal values in the "when" expressions
    pub fn expr(&self) -> &Option<Arc<dyn PhysicalExpr>> {
        &self.expr
    }

    /// One or more when/then expressions
    pub fn when_then_expr(&self) -> &[WhenThen] {
        &self.when_then_expr
    }

    /// Optional "else" expression
    pub fn else_expr(&self) -> Option<&Arc<dyn PhysicalExpr>> {
        self.else_expr.as_ref()
    }
}

impl CaseExpr {
    /// This function evaluates the form of CASE that matches an expression to fixed values.
    ///
    /// CASE expression
    ///     WHEN value THEN result
    ///     [WHEN ...]
    ///     [ELSE result]
    /// END
    fn case_when_with_expr(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let return_type = self.when_then_expr[0].1.data_type(&batch.schema())?;
        let expr = self.expr.as_ref().unwrap();
        let base_value = expr.evaluate(batch)?;
        let base_value = base_value.into_array(batch.num_rows());
        let base_nulls = is_null(base_value.as_ref())?;

        // start with nulls as default output
        let mut current_value = new_null_array(&return_type, batch.num_rows());
        // We only consider non-null values while comparing with whens
        let mut remainder = not(&base_nulls)?;
        for i in 0..self.when_then_expr.len() {
            let when_value = self.when_then_expr[i]
                .0
                .evaluate_selection(batch, &remainder)?;
            let when_value = when_value.into_array(batch.num_rows());
            // build boolean array representing which rows match the "when" value
            let when_match = eq_dyn(&when_value, base_value.as_ref())?;

            let then_value = self.when_then_expr[i]
                .1
                .evaluate_selection(batch, &when_match)?;
            let then_value = then_value.into_array(batch.num_rows());

            current_value =
                zip(&when_match, then_value.as_ref(), current_value.as_ref())?;

            remainder = and(&remainder, &or_kleene(&not(&when_match)?, &base_nulls)?)?;
        }

        if let Some(e) = &self.else_expr {
            // keep `else_expr`'s data type and return type consistent
            let expr = try_cast(e.clone(), &*batch.schema(), return_type.clone())
                .unwrap_or_else(|_| e.clone());
            // null and unmatched tuples should be assigned else value
            remainder = or(&base_nulls, &remainder)?;
            let else_ = expr
                .evaluate_selection(batch, &remainder)?
                .into_array(batch.num_rows());
            current_value = zip(&remainder, else_.as_ref(), current_value.as_ref())?;
        }

        Ok(ColumnarValue::Array(current_value))
    }

    /// This function evaluates the form of CASE where each WHEN expression is a boolean
    /// expression.
    ///
    /// CASE WHEN condition THEN result
    ///      [WHEN ...]
    ///      [ELSE result]
    /// END
    fn case_when_no_expr(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let return_type = self.when_then_expr[0].1.data_type(&batch.schema())?;

        // start with nulls as default output
        let mut current_value = new_null_array(&return_type, batch.num_rows());
        let mut remainder = BooleanArray::from(vec![true; batch.num_rows()]);
        for i in 0..self.when_then_expr.len() {
            let when_value = self.when_then_expr[i]
                .0
                .evaluate_selection(batch, &remainder)?;
            // Treat 'NULL' as false value
            let when_value = match when_value {
                ColumnarValue::Scalar(value) if value.is_null() => {
                    continue;
                }
                _ => when_value,
            };
            let when_value = when_value.into_array(batch.num_rows());
            let when_value = when_value
                .as_ref()
                .as_any()
                .downcast_ref::<BooleanArray>()
                .expect("WHEN expression did not return a BooleanArray");

            let then_value = self.when_then_expr[i]
                .1
                .evaluate_selection(batch, when_value)?;
            let then_value = then_value.into_array(batch.num_rows());

            current_value = zip(when_value, then_value.as_ref(), current_value.as_ref())?;

            // Succeed tuples should be filtered out for short-circuit evaluation,
            // null values for the current when expr should be kept
            remainder = and(
                &remainder,
                &or_kleene(&not(when_value)?, &is_null(when_value)?)?,
            )?;
        }

        if let Some(e) = &self.else_expr {
            // keep `else_expr`'s data type and return type consistent
            let expr = try_cast(e.clone(), &*batch.schema(), return_type.clone())
                .unwrap_or_else(|_| e.clone());
            let else_ = expr
                .evaluate_selection(batch, &remainder)?
                .into_array(batch.num_rows());
            current_value = zip(&remainder, else_.as_ref(), current_value.as_ref())?;
        }

        Ok(ColumnarValue::Array(current_value))
    }
}

impl PhysicalExpr for CaseExpr {
    /// Return a reference to Any that can be used for down-casting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        self.when_then_expr[0].1.data_type(input_schema)
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        // this expression is nullable if any of the input expressions are nullable
        let then_nullable = self
            .when_then_expr
            .iter()
            .map(|(_, t)| t.nullable(input_schema))
            .collect::<Result<Vec<_>>>()?;
        if then_nullable.contains(&true) {
            Ok(true)
        } else if let Some(e) = &self.else_expr {
            e.nullable(input_schema)
        } else {
            Ok(false)
        }
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        if self.expr.is_some() {
            // this use case evaluates "expr" and then compares the values with the "when"
            // values
            self.case_when_with_expr(batch)
        } else {
            // The "when" conditions all evaluate to boolean in this use case and can be
            // arbitrary expressions
            self.case_when_no_expr(batch)
        }
    }
}

/// Create a CASE expression
pub fn case(
    expr: Option<Arc<dyn PhysicalExpr>>,
    when_thens: &[WhenThen],
    else_expr: Option<Arc<dyn PhysicalExpr>>,
) -> Result<Arc<dyn PhysicalExpr>> {
    Ok(Arc::new(CaseExpr::try_new(expr, when_thens, else_expr)?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::col;
    use crate::expressions::lit;
    use crate::expressions::{binary, cast};
    use arrow::array::StringArray;
    use arrow::buffer::Buffer;
    use arrow::datatypes::DataType::Float64;
    use arrow::datatypes::*;
    use datafusion_common::ScalarValue;
    use datafusion_expr::Operator;

    #[test]
    fn case_with_expr() -> Result<()> {
        let batch = case_test_batch()?;
        let schema = batch.schema();

        // CASE a WHEN 'foo' THEN 123 WHEN 'bar' THEN 456 END
        let when1 = lit(ScalarValue::Utf8(Some("foo".to_string())));
        let then1 = lit(ScalarValue::Int32(Some(123)));
        let when2 = lit(ScalarValue::Utf8(Some("bar".to_string())));
        let then2 = lit(ScalarValue::Int32(Some(456)));

        let expr = case(
            Some(col("a", &schema)?),
            &[(when1, then1), (when2, then2)],
            None,
        )?;
        let result = expr.evaluate(&batch)?.into_array(batch.num_rows());
        let result = result
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("failed to downcast to Int32Array");

        let expected = &Int32Array::from(vec![Some(123), None, None, Some(456)]);

        assert_eq!(expected, result);

        Ok(())
    }

    #[test]
    fn case_with_expr_else() -> Result<()> {
        let batch = case_test_batch()?;
        let schema = batch.schema();

        // CASE a WHEN 'foo' THEN 123 WHEN 'bar' THEN 456 ELSE 999 END
        let when1 = lit(ScalarValue::Utf8(Some("foo".to_string())));
        let then1 = lit(ScalarValue::Int32(Some(123)));
        let when2 = lit(ScalarValue::Utf8(Some("bar".to_string())));
        let then2 = lit(ScalarValue::Int32(Some(456)));
        let else_value = lit(ScalarValue::Int32(Some(999)));

        let expr = case(
            Some(col("a", &schema)?),
            &[(when1, then1), (when2, then2)],
            Some(else_value),
        )?;
        let result = expr.evaluate(&batch)?.into_array(batch.num_rows());
        let result = result
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("failed to downcast to Int32Array");

        let expected =
            &Int32Array::from(vec![Some(123), Some(999), Some(999), Some(456)]);

        assert_eq!(expected, result);

        Ok(())
    }

    #[test]
    fn case_with_expr_divide_by_zero() -> Result<()> {
        let batch = case_test_batch1()?;
        let schema = batch.schema();

        // CASE a when 0 THEN float64(null) ELSE 25.0 / cast(a, float64)  END
        let when1 = lit(ScalarValue::Int32(Some(0)));
        let then1 = lit(ScalarValue::Float64(None));
        let else_value = binary(
            lit(ScalarValue::Float64(Some(25.0))),
            Operator::Divide,
            cast(col("a", &schema)?, &batch.schema(), Float64)?,
            &batch.schema(),
        )?;

        let expr = case(
            Some(col("a", &schema)?),
            &[(when1, then1)],
            Some(else_value),
        )?;
        let result = expr.evaluate(&batch)?.into_array(batch.num_rows());
        let result = result
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("failed to downcast to Int32Array");

        let expected = &Float64Array::from(vec![Some(25.0), None, None, Some(5.0)]);

        assert_eq!(expected, result);

        Ok(())
    }

    #[test]
    fn case_without_expr() -> Result<()> {
        let batch = case_test_batch()?;
        let schema = batch.schema();

        // CASE WHEN a = 'foo' THEN 123 WHEN a = 'bar' THEN 456 END
        let when1 = binary(
            col("a", &schema)?,
            Operator::Eq,
            lit(ScalarValue::Utf8(Some("foo".to_string()))),
            &batch.schema(),
        )?;
        let then1 = lit(ScalarValue::Int32(Some(123)));
        let when2 = binary(
            col("a", &schema)?,
            Operator::Eq,
            lit(ScalarValue::Utf8(Some("bar".to_string()))),
            &batch.schema(),
        )?;
        let then2 = lit(ScalarValue::Int32(Some(456)));

        let expr = case(None, &[(when1, then1), (when2, then2)], None)?;
        let result = expr.evaluate(&batch)?.into_array(batch.num_rows());
        let result = result
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("failed to downcast to Int32Array");

        let expected = &Int32Array::from(vec![Some(123), None, None, Some(456)]);

        assert_eq!(expected, result);

        Ok(())
    }

    #[test]
    fn case_without_expr_divide_by_zero() -> Result<()> {
        let batch = case_test_batch1()?;
        let schema = batch.schema();

        // CASE WHEN a > 0 THEN 25.0 / cast(a, float64) ELSE float64(null) END
        let when1 = binary(
            col("a", &schema)?,
            Operator::Gt,
            lit(ScalarValue::Int32(Some(0))),
            &batch.schema(),
        )?;
        let then1 = binary(
            lit(ScalarValue::Float64(Some(25.0))),
            Operator::Divide,
            cast(col("a", &schema)?, &batch.schema(), Float64)?,
            &batch.schema(),
        )?;
        let x = lit(ScalarValue::Float64(None));

        let expr = case(None, &[(when1, then1)], Some(x))?;
        let result = expr.evaluate(&batch)?.into_array(batch.num_rows());
        let result = result
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("failed to downcast to Int32Array");

        let expected = &Float64Array::from(vec![Some(25.0), None, None, Some(5.0)]);

        assert_eq!(expected, result);

        Ok(())
    }

    fn case_test_batch1() -> Result<RecordBatch> {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
        let a = Int32Array::from(vec![Some(1), Some(0), None, Some(5)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)])?;
        Ok(batch)
    }

    #[test]
    fn case_without_expr_else() -> Result<()> {
        let batch = case_test_batch()?;
        let schema = batch.schema();

        // CASE WHEN a = 'foo' THEN 123 WHEN a = 'bar' THEN 456 ELSE 999 END
        let when1 = binary(
            col("a", &schema)?,
            Operator::Eq,
            lit(ScalarValue::Utf8(Some("foo".to_string()))),
            &batch.schema(),
        )?;
        let then1 = lit(ScalarValue::Int32(Some(123)));
        let when2 = binary(
            col("a", &schema)?,
            Operator::Eq,
            lit(ScalarValue::Utf8(Some("bar".to_string()))),
            &batch.schema(),
        )?;
        let then2 = lit(ScalarValue::Int32(Some(456)));
        let else_value = lit(ScalarValue::Int32(Some(999)));

        let expr = case(None, &[(when1, then1), (when2, then2)], Some(else_value))?;
        let result = expr.evaluate(&batch)?.into_array(batch.num_rows());
        let result = result
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("failed to downcast to Int32Array");

        let expected =
            &Int32Array::from(vec![Some(123), Some(999), Some(999), Some(456)]);

        assert_eq!(expected, result);

        Ok(())
    }

    #[test]
    fn case_with_type_cast() -> Result<()> {
        let batch = case_test_batch()?;
        let schema = batch.schema();

        // CASE WHEN a = 'foo' THEN 123.3 ELSE 999 END
        let when = binary(
            col("a", &schema)?,
            Operator::Eq,
            lit(ScalarValue::Utf8(Some("foo".to_string()))),
            &batch.schema(),
        )?;
        let then = lit(ScalarValue::Float64(Some(123.3)));
        let else_value = lit(ScalarValue::Int32(Some(999)));

        let expr = case(None, &[(when, then)], Some(else_value))?;
        let result = expr.evaluate(&batch)?.into_array(batch.num_rows());
        let result = result
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("failed to downcast to Float64Array");

        let expected =
            &Float64Array::from(vec![Some(123.3), Some(999.0), Some(999.0), Some(999.0)]);

        assert_eq!(expected, result);

        Ok(())
    }

    #[test]
    fn case_with_matches_and_nulls() -> Result<()> {
        let batch = case_test_batch_nulls()?;
        let schema = batch.schema();

        // SELECT CASE WHEN load4 = 1.77 THEN load4 END
        let when = binary(
            col("load4", &schema)?,
            Operator::Eq,
            lit(ScalarValue::Float64(Some(1.77))),
            &batch.schema(),
        )?;
        let then = col("load4", &schema)?;

        let expr = case(None, &[(when, then)], None)?;
        let result = expr.evaluate(&batch)?.into_array(batch.num_rows());
        let result = result
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("failed to downcast to Float64Array");

        let expected =
            &Float64Array::from(vec![Some(1.77), None, None, None, None, Some(1.77)]);

        assert_eq!(expected, result);

        Ok(())
    }

    #[test]
    fn case_expr_matches_and_nulls() -> Result<()> {
        let batch = case_test_batch_nulls()?;
        let schema = batch.schema();

        // SELECT CASE load4 WHEN 1.77 THEN load4 END
        let expr = col("load4", &schema)?;
        let when = lit(ScalarValue::Float64(Some(1.77)));
        let then = col("load4", &schema)?;

        let expr = case(Some(expr), &[(when, then)], None)?;
        let result = expr.evaluate(&batch)?.into_array(batch.num_rows());
        let result = result
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("failed to downcast to Float64Array");

        let expected =
            &Float64Array::from(vec![Some(1.77), None, None, None, None, Some(1.77)]);

        assert_eq!(expected, result);

        Ok(())
    }

    fn case_test_batch() -> Result<RecordBatch> {
        let schema = Schema::new(vec![Field::new("a", DataType::Utf8, true)]);
        let a = StringArray::from(vec![Some("foo"), Some("baz"), None, Some("bar")]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)])?;
        Ok(batch)
    }

    // Construct an array that has several NULL values whose
    // underlying buffer actually matches the where expr predicate
    fn case_test_batch_nulls() -> Result<RecordBatch> {
        let load4: Float64Array = vec![
            Some(1.77), // 1.77
            Some(1.77), // null <-- same value, but will be set to null
            Some(1.77), // null <-- same value, but will be set to null
            Some(1.78), // 1.78
            None,       // null
            Some(1.77), // 1.77
        ]
        .into_iter()
        .collect();

        //let valid_array = vec![true, false, false, true, false, tru
        let null_buffer = Buffer::from_slice_ref(&[0b00101001u8]);
        let load4 = ArrayDataBuilder::new(load4.data_type().clone())
            .len(load4.len())
            .null_bit_buffer(null_buffer)
            .buffers(load4.data().buffers().to_vec())
            .build()
            .unwrap();
        let load4: Float64Array = load4.into();

        let batch =
            RecordBatch::try_from_iter(vec![("load4", Arc::new(load4) as ArrayRef)])?;
        Ok(batch)
    }
}
