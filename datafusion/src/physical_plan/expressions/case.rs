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

use arrow::array::*;
use arrow::compute::comparison;
use arrow::compute::if_then_else;
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;

use crate::error::{DataFusionError, Result};
use crate::physical_plan::{ColumnarValue, PhysicalExpr};

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
    when_then_expr: Vec<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)>,
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
        when_then_expr: &[(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)],
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
    pub fn when_then_expr(&self) -> &[(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)] {
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
        let return_type = self.when_then_expr[0].1.data_type(batch.schema())?;
        let expr = self.expr.as_ref().unwrap();
        let base_value = expr.evaluate(batch)?;
        let base_value = base_value.into_array(batch.num_rows());

        // start with the else condition, or nulls
        let mut current_value = if let Some(e) = &self.else_expr {
            e.evaluate(batch)?.into_array(batch.num_rows())
        } else {
            new_null_array(return_type, batch.num_rows()).into()
        };

        // walk backwards through the when/then expressions
        for i in (0..self.when_then_expr.len()).rev() {
            let i = i as usize;

            let when_value = self.when_then_expr[i].0.evaluate(batch)?;
            let when_value = when_value.into_array(batch.num_rows());

            let then_value = self.when_then_expr[i].1.evaluate(batch)?;
            let then_value = then_value.into_array(batch.num_rows());

            // build boolean array representing which rows match the "when" value
            let when_match = comparison::compare(
                when_value.as_ref(),
                base_value.as_ref(),
                comparison::Operator::Eq,
            )?;
            let when_match = if let Some(validity) = when_match.validity() {
                // null values are never matched and should thus be "else".
                BooleanArray::from_data(
                    DataType::Boolean,
                    when_match.values() & validity,
                    None,
                )
            } else {
                when_match
            };

            current_value = if_then_else::if_then_else(
                &when_match,
                then_value.as_ref(),
                current_value.as_ref(),
            )?
            .into();
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
        let return_type = self.when_then_expr[0].1.data_type(batch.schema())?;

        // start with the else condition, or nulls
        let mut current_value = if let Some(e) = &self.else_expr {
            e.evaluate(batch)?.into_array(batch.num_rows())
        } else {
            new_null_array(return_type, batch.num_rows()).into()
        };

        // walk backwards through the when/then expressions
        for i in (0..self.when_then_expr.len()).rev() {
            let i = i as usize;

            let when_value = self.when_then_expr[i].0.evaluate(batch)?;
            let when_value = when_value.into_array(batch.num_rows());
            let when_value = when_value
                .as_ref()
                .as_any()
                .downcast_ref::<BooleanArray>()
                .expect("WHEN expression did not return a BooleanArray")
                .clone();
            let when_value = if let Some(validity) = when_value.validity() {
                // null values are never matched and should thus be "else".
                BooleanArray::from_data(
                    DataType::Boolean,
                    when_value.values() & validity,
                    None,
                )
            } else {
                when_value
            };

            let then_value = self.when_then_expr[i].1.evaluate(batch)?;
            let then_value = then_value.into_array(batch.num_rows());

            current_value = if_then_else::if_then_else(
                &when_value,
                then_value.as_ref(),
                current_value.as_ref(),
            )?
            .into();
        }

        Ok(ColumnarValue::Array(current_value))
    }
}

impl PhysicalExpr for CaseExpr {
    /// Return a reference to Any that can be used for downcasting
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
    when_thens: &[(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)],
    else_expr: Option<Arc<dyn PhysicalExpr>>,
) -> Result<Arc<dyn PhysicalExpr>> {
    Ok(Arc::new(CaseExpr::try_new(expr, when_thens, else_expr)?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        error::Result,
        logical_plan::Operator,
        physical_plan::expressions::{binary, col, lit},
        scalar::ScalarValue,
    };
    use arrow::array::Utf8Array;
    use arrow::datatypes::*;

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
            Some(col("a", schema)?),
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
            Some(col("a", schema)?),
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
    fn case_without_expr() -> Result<()> {
        let batch = case_test_batch()?;
        let schema = batch.schema();

        // CASE WHEN a = 'foo' THEN 123 WHEN a = 'bar' THEN 456 END
        let when1 = binary(
            col("a", schema)?,
            Operator::Eq,
            lit(ScalarValue::Utf8(Some("foo".to_string()))),
            batch.schema(),
        )?;
        let then1 = lit(ScalarValue::Int32(Some(123)));
        let when2 = binary(
            col("a", schema)?,
            Operator::Eq,
            lit(ScalarValue::Utf8(Some("bar".to_string()))),
            batch.schema(),
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
    fn case_without_expr_else() -> Result<()> {
        let batch = case_test_batch()?;
        let schema = batch.schema();

        // CASE WHEN a = 'foo' THEN 123 WHEN a = 'bar' THEN 456 ELSE 999 END
        let when1 = binary(
            col("a", schema)?,
            Operator::Eq,
            lit(ScalarValue::Utf8(Some("foo".to_string()))),
            batch.schema(),
        )?;
        let then1 = lit(ScalarValue::Int32(Some(123)));
        let when2 = binary(
            col("a", schema)?,
            Operator::Eq,
            lit(ScalarValue::Utf8(Some("bar".to_string()))),
            batch.schema(),
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

    fn case_test_batch() -> Result<RecordBatch> {
        let schema = Schema::new(vec![Field::new("a", DataType::Utf8, true)]);
        let a = Utf8Array::<i32>::from(vec![Some("foo"), Some("baz"), None, Some("bar")]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)])?;
        Ok(batch)
    }
}
