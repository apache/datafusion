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

mod literal_lookup_table;

use crate::expressions::{try_cast, Column, Literal};
use crate::PhysicalExpr;
use std::borrow::Cow;
use std::hash::Hash;
use std::{any::Any, sync::Arc};

use arrow::array::*;
use arrow::compute::kernels::zip::zip;
use arrow::compute::{and, and_not, is_null, not, nullif, or, prep_null_mask_filter};
use arrow::datatypes::{DataType, Schema};
use datafusion_common::cast::as_boolean_array;
use datafusion_common::{
    exec_err, internal_datafusion_err, internal_err, DataFusionError, Result, ScalarValue,
};
use datafusion_expr::ColumnarValue;

use crate::expressions::case::literal_lookup_table::LiteralLookupTable;
use datafusion_physical_expr_common::datum::compare_with_eq;
use itertools::Itertools;

pub(super) type WhenThen = (Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>);

#[derive(Debug, Hash, PartialEq, Eq)]
enum EvalMethod {
    /// CASE WHEN condition THEN result
    ///      [WHEN ...]
    ///      [ELSE result]
    /// END
    NoExpression,
    /// CASE expression
    ///     WHEN value THEN result
    ///     [WHEN ...]
    ///     [ELSE result]
    /// END
    WithExpression,
    /// This is a specialization for a specific use case where we can take a fast path
    /// for expressions that are infallible and can be cheaply computed for the entire
    /// record batch rather than just for the rows where the predicate is true.
    ///
    /// CASE WHEN condition THEN column [ELSE NULL] END
    InfallibleExprOrNull,
    /// This is a specialization for a specific use case where we can take a fast path
    /// if there is just one when/then pair and both the `then` and `else` expressions
    /// are literal values
    /// CASE WHEN condition THEN literal ELSE literal END
    ScalarOrScalar,
    /// This is a specialization for a specific use case where we can take a fast path
    /// if there is just one when/then pair and both the `then` and `else` are expressions
    ///
    /// CASE WHEN condition THEN expression ELSE expression END
    ExpressionOrExpression,

    /// This is a specialization for [`EvalMethod::WithExpression`] when the value and results are literals
    ///
    /// See [`LiteralLookupTable`] for more details
    WithExprScalarLookupTable(LiteralLookupTable),
}

// Implement empty hash as the data is derived from PhysicalExprs which are already hashed
impl Hash for LiteralLookupTable {
    fn hash<H: std::hash::Hasher>(&self, _state: &mut H) {}
}

// Implement always equal as the data is derived from PhysicalExprs which are already compared
impl PartialEq for LiteralLookupTable {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for LiteralLookupTable {}

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
#[derive(Debug, Hash, PartialEq, Eq)]
pub struct CaseExpr {
    /// Optional base expression that can be compared to literal values in the "when" expressions
    expr: Option<Arc<dyn PhysicalExpr>>,
    /// One or more when/then expressions
    when_then_expr: Vec<WhenThen>,
    /// Optional "else" expression
    else_expr: Option<Arc<dyn PhysicalExpr>>,
    /// Evaluation method to use
    eval_method: EvalMethod,
}

impl std::fmt::Display for CaseExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "CASE ")?;
        if let Some(e) = &self.expr {
            write!(f, "{e} ")?;
        }
        for (w, t) in &self.when_then_expr {
            write!(f, "WHEN {w} THEN {t} ")?;
        }
        if let Some(e) = &self.else_expr {
            write!(f, "ELSE {e} ")?;
        }
        write!(f, "END")
    }
}

/// This is a specialization for a specific use case where we can take a fast path
/// for expressions that are infallible and can be cheaply computed for the entire
/// record batch rather than just for the rows where the predicate is true. For now,
/// this is limited to use with Column expressions but could potentially be used for other
/// expressions in the future
fn is_cheap_and_infallible(expr: &Arc<dyn PhysicalExpr>) -> bool {
    expr.as_any().is::<Column>()
}

impl CaseExpr {
    /// Create a new CASE WHEN expression
    pub fn try_new(
        expr: Option<Arc<dyn PhysicalExpr>>,
        when_then_expr: Vec<WhenThen>,
        else_expr: Option<Arc<dyn PhysicalExpr>>,
    ) -> Result<Self> {
        // normalize null literals to None in the else_expr (this already happens
        // during SQL planning, but not necessarily for other use cases)
        let else_expr = match &else_expr {
            Some(e) => match e.as_any().downcast_ref::<Literal>() {
                Some(lit) if lit.value().is_null() => None,
                _ => else_expr,
            },
            _ => else_expr,
        };

        if when_then_expr.is_empty() {
            exec_err!("There must be at least one WHEN clause")
        } else {
            let eval_method =
                Self::find_best_eval_method(&expr, &when_then_expr, &else_expr);

            Ok(Self {
                expr,
                when_then_expr,
                else_expr,
                eval_method,
            })
        }
    }

    fn find_best_eval_method(
        expr: &Option<Arc<dyn PhysicalExpr>>,
        when_then_expr: &Vec<WhenThen>,
        else_expr: &Option<Arc<dyn PhysicalExpr>>,
    ) -> EvalMethod {
        if expr.is_some() {
            if let Some(mapping) =
                LiteralLookupTable::maybe_new(when_then_expr, else_expr)
            {
                return EvalMethod::WithExprScalarLookupTable(mapping);
            }

            return EvalMethod::WithExpression;
        }

        if when_then_expr.len() == 1
            && is_cheap_and_infallible(&(when_then_expr[0].1))
            && else_expr.is_none()
        {
            EvalMethod::InfallibleExprOrNull
        } else if when_then_expr.len() == 1
            && when_then_expr[0].1.as_any().is::<Literal>()
            && else_expr.is_some()
            && else_expr.as_ref().unwrap().as_any().is::<Literal>()
        {
            EvalMethod::ScalarOrScalar
        } else if when_then_expr.len() == 1 && else_expr.is_some() {
            EvalMethod::ExpressionOrExpression
        } else {
            EvalMethod::NoExpression
        }
    }

    /// Optional base expression that can be compared to literal values in the "when" expressions
    pub fn expr(&self) -> Option<&Arc<dyn PhysicalExpr>> {
        self.expr.as_ref()
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
        let return_type = self.data_type(&batch.schema())?;
        let expr = self.expr.as_ref().unwrap();
        let base_value = expr.evaluate(batch)?;
        let base_value = base_value.into_array(batch.num_rows())?;
        let base_nulls = is_null(base_value.as_ref())?;

        // start with nulls as default output
        let mut current_value = new_null_array(&return_type, batch.num_rows());
        // We only consider non-null values while comparing with whens
        let mut remainder = not(&base_nulls)?;
        let mut non_null_remainder_count = remainder.true_count();
        for i in 0..self.when_then_expr.len() {
            // If there are no rows left to process, break out of the loop early
            if non_null_remainder_count == 0 {
                break;
            }

            let when_predicate = &self.when_then_expr[i].0;
            let when_value = when_predicate.evaluate_selection(batch, &remainder)?;
            let when_value = when_value.into_array(batch.num_rows())?;
            // build boolean array representing which rows match the "when" value
            let when_match = compare_with_eq(
                &when_value,
                &base_value,
                // The types of case and when expressions will be coerced to match.
                // We only need to check if the base_value is nested.
                base_value.data_type().is_nested(),
            )?;
            // Treat nulls as false
            let when_match = match when_match.null_count() {
                0 => Cow::Borrowed(&when_match),
                _ => Cow::Owned(prep_null_mask_filter(&when_match)),
            };
            // Make sure we only consider rows that have not been matched yet
            let when_value = and(&when_match, &remainder)?;

            // If the predicate did not match any rows, continue to the next branch immediately
            let when_match_count = when_value.true_count();
            if when_match_count == 0 {
                continue;
            }

            let then_expression = &self.when_then_expr[i].1;
            let then_value = then_expression.evaluate_selection(batch, &when_value)?;

            current_value = match then_value {
                ColumnarValue::Scalar(ScalarValue::Null) => {
                    nullif(current_value.as_ref(), &when_value)?
                }
                ColumnarValue::Scalar(then_value) => {
                    zip(&when_value, &then_value.to_scalar()?, &current_value)?
                }
                ColumnarValue::Array(then_value) => {
                    zip(&when_value, &then_value, &current_value)?
                }
            };

            remainder = and_not(&remainder, &when_value)?;
            non_null_remainder_count -= when_match_count;
        }

        if let Some(e) = self.else_expr() {
            // null and unmatched tuples should be assigned else value
            remainder = or(&base_nulls, &remainder)?;

            if remainder.true_count() > 0 {
                // keep `else_expr`'s data type and return type consistent
                let expr = try_cast(Arc::clone(e), &batch.schema(), return_type.clone())?;

                let else_ = expr
                    .evaluate_selection(batch, &remainder)?
                    .into_array(batch.num_rows())?;
                current_value = zip(&remainder, &else_, &current_value)?;
            }
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
        let return_type = self.data_type(&batch.schema())?;

        // start with nulls as default output
        let mut current_value = new_null_array(&return_type, batch.num_rows());
        let mut remainder = BooleanArray::from(vec![true; batch.num_rows()]);
        let mut remainder_count = batch.num_rows();
        for i in 0..self.when_then_expr.len() {
            // If there are no rows left to process, break out of the loop early
            if remainder_count == 0 {
                break;
            }

            let when_predicate = &self.when_then_expr[i].0;
            let when_value = when_predicate.evaluate_selection(batch, &remainder)?;
            let when_value = when_value.into_array(batch.num_rows())?;
            let when_value = as_boolean_array(&when_value).map_err(|_| {
                internal_datafusion_err!("WHEN expression did not return a BooleanArray")
            })?;
            // Treat 'NULL' as false value
            let when_value = match when_value.null_count() {
                0 => Cow::Borrowed(when_value),
                _ => Cow::Owned(prep_null_mask_filter(when_value)),
            };
            // Make sure we only consider rows that have not been matched yet
            let when_value = and(&when_value, &remainder)?;

            // If the predicate did not match any rows, continue to the next branch immediately
            let when_match_count = when_value.true_count();
            if when_match_count == 0 {
                continue;
            }

            let then_expression = &self.when_then_expr[i].1;
            let then_value = then_expression.evaluate_selection(batch, &when_value)?;

            current_value = match then_value {
                ColumnarValue::Scalar(ScalarValue::Null) => {
                    nullif(current_value.as_ref(), &when_value)?
                }
                ColumnarValue::Scalar(then_value) => {
                    zip(&when_value, &then_value.to_scalar()?, &current_value)?
                }
                ColumnarValue::Array(then_value) => {
                    zip(&when_value, &then_value, &current_value)?
                }
            };

            // Succeed tuples should be filtered out for short-circuit evaluation,
            // null values for the current when expr should be kept
            remainder = and_not(&remainder, &when_value)?;
            remainder_count -= when_match_count;
        }

        if let Some(e) = self.else_expr() {
            if remainder_count > 0 {
                // keep `else_expr`'s data type and return type consistent
                let expr = try_cast(Arc::clone(e), &batch.schema(), return_type.clone())?;
                let else_ = expr
                    .evaluate_selection(batch, &remainder)?
                    .into_array(batch.num_rows())?;
                current_value = zip(&remainder, &else_, &current_value)?;
            }
        }

        Ok(ColumnarValue::Array(current_value))
    }

    /// This function evaluates the specialized case of:
    ///
    /// CASE WHEN condition THEN column
    ///      [ELSE NULL]
    /// END
    ///
    /// Note that this function is only safe to use for "then" expressions
    /// that are infallible because the expression will be evaluated for all
    /// rows in the input batch.
    fn case_column_or_null(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let when_expr = &self.when_then_expr[0].0;
        let then_expr = &self.when_then_expr[0].1;

        match when_expr.evaluate(batch)? {
            // WHEN true --> column
            ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))) => {
                then_expr.evaluate(batch)
            }
            // WHEN [false | null] --> NULL
            ColumnarValue::Scalar(_) => {
                // return scalar NULL value
                ScalarValue::try_from(self.data_type(&batch.schema())?)
                    .map(ColumnarValue::Scalar)
            }
            // WHEN column --> column
            ColumnarValue::Array(bit_mask) => {
                let bit_mask = bit_mask
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .expect("predicate should evaluate to a boolean array");
                // invert the bitmask
                let bit_mask = match bit_mask.null_count() {
                    0 => not(bit_mask)?,
                    _ => not(&prep_null_mask_filter(bit_mask))?,
                };
                match then_expr.evaluate(batch)? {
                    ColumnarValue::Array(array) => {
                        Ok(ColumnarValue::Array(nullif(&array, &bit_mask)?))
                    }
                    ColumnarValue::Scalar(_) => {
                        internal_err!("expression did not evaluate to an array")
                    }
                }
            }
        }
    }

    fn scalar_or_scalar(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let return_type = self.data_type(&batch.schema())?;

        // evaluate when expression
        let when_value = self.when_then_expr[0].0.evaluate(batch)?;
        let when_value = when_value.into_array(batch.num_rows())?;
        let when_value = as_boolean_array(&when_value).map_err(|_| {
            internal_datafusion_err!("WHEN expression did not return a BooleanArray")
        })?;

        // Treat 'NULL' as false value
        let when_value = match when_value.null_count() {
            0 => Cow::Borrowed(when_value),
            _ => Cow::Owned(prep_null_mask_filter(when_value)),
        };

        // evaluate then_value
        let then_value = self.when_then_expr[0].1.evaluate(batch)?;
        let then_value = Scalar::new(then_value.into_array(1)?);

        let Some(e) = self.else_expr() else {
            return internal_err!("expression did not evaluate to an array");
        };
        // keep `else_expr`'s data type and return type consistent
        let expr = try_cast(Arc::clone(e), &batch.schema(), return_type)?;
        let else_ = Scalar::new(expr.evaluate(batch)?.into_array(1)?);
        Ok(ColumnarValue::Array(zip(&when_value, &then_value, &else_)?))
    }

    fn expr_or_expr(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let return_type = self.data_type(&batch.schema())?;

        // evaluate when condition on batch
        let when_value = self.when_then_expr[0].0.evaluate(batch)?;
        let when_value = when_value.into_array(batch.num_rows())?;
        let when_value = as_boolean_array(&when_value).map_err(|e| {
            DataFusionError::Context(
                "WHEN expression did not return a BooleanArray".to_string(),
                Box::new(e),
            )
        })?;

        // For the true and false/null selection vectors, bypass `evaluate_selection` and merging
        // results. This avoids materializing the array for the other branch which we will discard
        // entirely anyway.
        let true_count = when_value.true_count();
        if true_count == batch.num_rows() {
            return self.when_then_expr[0].1.evaluate(batch);
        } else if true_count == 0 {
            return self.else_expr.as_ref().unwrap().evaluate(batch);
        }

        // Treat 'NULL' as false value
        let when_value = match when_value.null_count() {
            0 => Cow::Borrowed(when_value),
            _ => Cow::Owned(prep_null_mask_filter(when_value)),
        };

        let then_value = self.when_then_expr[0]
            .1
            .evaluate_selection(batch, &when_value)?
            .into_array(batch.num_rows())?;

        // evaluate else expression on the values not covered by when_value
        let remainder = not(&when_value)?;
        let e = self.else_expr.as_ref().unwrap();
        // keep `else_expr`'s data type and return type consistent
        let expr = try_cast(Arc::clone(e), &batch.schema(), return_type.clone())
            .unwrap_or_else(|_| Arc::clone(e));
        let else_ = expr
            .evaluate_selection(batch, &remainder)?
            .into_array(batch.num_rows())?;

        Ok(ColumnarValue::Array(zip(&remainder, &else_, &then_value)?))
    }

    fn with_lookup_table(
        &self,
        batch: &RecordBatch,
        scalars_or_null_lookup: &LiteralLookupTable,
    ) -> Result<ColumnarValue> {
        let expr = self.expr.as_ref().unwrap();
        let evaluated_expression = expr.evaluate(batch)?;

        let is_scalar = matches!(evaluated_expression, ColumnarValue::Scalar(_));
        let evaluated_expression = evaluated_expression.to_array(1)?;

        let output = scalars_or_null_lookup.create_output(&evaluated_expression)?;

        let result = if is_scalar {
            ColumnarValue::Scalar(ScalarValue::try_from_array(output.as_ref(), 0)?)
        } else {
            ColumnarValue::Array(output)
        };

        Ok(result)
    }
}

impl PhysicalExpr for CaseExpr {
    /// Return a reference to Any that can be used for down-casting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        // since all then results have the same data type, we can choose any one as the
        // return data type except for the null.
        let mut data_type = DataType::Null;
        for i in 0..self.when_then_expr.len() {
            data_type = self.when_then_expr[i].1.data_type(input_schema)?;
            if !data_type.equals_datatype(&DataType::Null) {
                break;
            }
        }
        // if all then results are null, we use data type of else expr instead if possible.
        if data_type.equals_datatype(&DataType::Null) {
            if let Some(e) = &self.else_expr {
                data_type = e.data_type(input_schema)?;
            }
        }

        Ok(data_type)
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
            // CASE produces NULL if there is no `else` expr
            // (aka when none of the `when_then_exprs` match)
            Ok(true)
        }
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        match self.eval_method {
            EvalMethod::WithExpression => {
                // this use case evaluates "expr" and then compares the values with the "when"
                // values
                self.case_when_with_expr(batch)
            }
            EvalMethod::NoExpression => {
                // The "when" conditions all evaluate to boolean in this use case and can be
                // arbitrary expressions
                self.case_when_no_expr(batch)
            }
            EvalMethod::InfallibleExprOrNull => {
                // Specialization for CASE WHEN expr THEN column [ELSE NULL] END
                self.case_column_or_null(batch)
            }
            EvalMethod::ScalarOrScalar => self.scalar_or_scalar(batch),
            EvalMethod::ExpressionOrExpression => self.expr_or_expr(batch),
            EvalMethod::WithExprScalarLookupTable(ref e) => {
                self.with_lookup_table(batch, e)
            }
        }
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        let mut children = vec![];
        if let Some(expr) = &self.expr {
            children.push(expr)
        }
        self.when_then_expr.iter().for_each(|(cond, value)| {
            children.push(cond);
            children.push(value);
        });

        if let Some(else_expr) = &self.else_expr {
            children.push(else_expr)
        }
        children
    }

    // For physical CaseExpr, we do not allow modifying children size
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        if children.len() != self.children().len() {
            internal_err!("CaseExpr: Wrong number of children")
        } else {
            let (expr, when_then_expr, else_expr) =
                match (self.expr().is_some(), self.else_expr().is_some()) {
                    (true, true) => (
                        Some(&children[0]),
                        &children[1..children.len() - 1],
                        Some(&children[children.len() - 1]),
                    ),
                    (true, false) => {
                        (Some(&children[0]), &children[1..children.len()], None)
                    }
                    (false, true) => (
                        None,
                        &children[0..children.len() - 1],
                        Some(&children[children.len() - 1]),
                    ),
                    (false, false) => (None, &children[0..children.len()], None),
                };
            Ok(Arc::new(CaseExpr::try_new(
                expr.cloned(),
                when_then_expr.iter().cloned().tuples().collect(),
                else_expr.cloned(),
            )?))
        }
    }

    fn fmt_sql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CASE ")?;
        if let Some(e) = &self.expr {
            e.fmt_sql(f)?;
            write!(f, " ")?;
        }

        for (w, t) in &self.when_then_expr {
            write!(f, "WHEN ")?;
            w.fmt_sql(f)?;
            write!(f, " THEN ")?;
            t.fmt_sql(f)?;
            write!(f, " ")?;
        }

        if let Some(e) = &self.else_expr {
            write!(f, "ELSE ")?;
            e.fmt_sql(f)?;
            write!(f, " ")?;
        }
        write!(f, "END")
    }
}

/// Create a CASE expression
pub fn case(
    expr: Option<Arc<dyn PhysicalExpr>>,
    when_thens: Vec<WhenThen>,
    else_expr: Option<Arc<dyn PhysicalExpr>>,
) -> Result<Arc<dyn PhysicalExpr>> {
    Ok(Arc::new(CaseExpr::try_new(expr, when_thens, else_expr)?))
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::expressions::{binary, cast, col, lit, BinaryExpr};
    use arrow::buffer::Buffer;
    use arrow::datatypes::DataType::Float64;
    use arrow::datatypes::{Field, Int32Type};
    use datafusion_common::cast::{as_float64_array, as_int32_array};
    use datafusion_common::plan_err;
    use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
    use datafusion_expr::type_coercion::binary::comparison_coercion;
    use datafusion_expr::Operator;
    use datafusion_physical_expr_common::physical_expr::fmt_sql;
    use half::f16;

    #[test]
    fn case_with_expr() -> Result<()> {
        let batch = case_test_batch()?;
        let schema = batch.schema();

        // CASE a WHEN 'foo' THEN 123 WHEN 'bar' THEN 456 END
        let when1 = lit("foo");
        let then1 = lit(123i32);
        let when2 = lit("bar");
        let then2 = lit(456i32);

        let expr = generate_case_when_with_type_coercion(
            Some(col("a", &schema)?),
            vec![(when1, then1), (when2, then2)],
            None,
            schema.as_ref(),
        )?;
        let result = expr
            .evaluate(&batch)?
            .into_array(batch.num_rows())
            .expect("Failed to convert to array");
        let result = as_int32_array(&result)?;

        let expected = &Int32Array::from(vec![Some(123), None, None, Some(456)]);

        assert_eq!(expected, result);

        Ok(())
    }

    #[test]
    fn case_with_expr_else() -> Result<()> {
        let batch = case_test_batch()?;
        let schema = batch.schema();

        // CASE a WHEN 'foo' THEN 123 WHEN 'bar' THEN 456 ELSE 999 END
        let when1 = lit("foo");
        let then1 = lit(123i32);
        let when2 = lit("bar");
        let then2 = lit(456i32);
        let else_value = lit(999i32);

        let expr = generate_case_when_with_type_coercion(
            Some(col("a", &schema)?),
            vec![(when1, then1), (when2, then2)],
            Some(else_value),
            schema.as_ref(),
        )?;
        let result = expr
            .evaluate(&batch)?
            .into_array(batch.num_rows())
            .expect("Failed to convert to array");
        let result = as_int32_array(&result)?;

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
        let when1 = lit(0i32);
        let then1 = lit(ScalarValue::Float64(None));
        let else_value = binary(
            lit(25.0f64),
            Operator::Divide,
            cast(col("a", &schema)?, &batch.schema(), Float64)?,
            &batch.schema(),
        )?;

        let expr = generate_case_when_with_type_coercion(
            Some(col("a", &schema)?),
            vec![(when1, then1)],
            Some(else_value),
            schema.as_ref(),
        )?;
        let result = expr
            .evaluate(&batch)?
            .into_array(batch.num_rows())
            .expect("Failed to convert to array");
        let result =
            as_float64_array(&result).expect("failed to downcast to Float64Array");

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
            lit("foo"),
            &batch.schema(),
        )?;
        let then1 = lit(123i32);
        let when2 = binary(
            col("a", &schema)?,
            Operator::Eq,
            lit("bar"),
            &batch.schema(),
        )?;
        let then2 = lit(456i32);

        let expr = generate_case_when_with_type_coercion(
            None,
            vec![(when1, then1), (when2, then2)],
            None,
            schema.as_ref(),
        )?;
        let result = expr
            .evaluate(&batch)?
            .into_array(batch.num_rows())
            .expect("Failed to convert to array");
        let result = as_int32_array(&result)?;

        let expected = &Int32Array::from(vec![Some(123), None, None, Some(456)]);

        assert_eq!(expected, result);

        Ok(())
    }

    #[test]
    fn case_with_expr_when_null() -> Result<()> {
        let batch = case_test_batch()?;
        let schema = batch.schema();

        // CASE a WHEN NULL THEN 0 WHEN a THEN 123 ELSE 999 END
        let when1 = lit(ScalarValue::Utf8(None));
        let then1 = lit(0i32);
        let when2 = col("a", &schema)?;
        let then2 = lit(123i32);
        let else_value = lit(999i32);

        let expr = generate_case_when_with_type_coercion(
            Some(col("a", &schema)?),
            vec![(when1, then1), (when2, then2)],
            Some(else_value),
            schema.as_ref(),
        )?;
        let result = expr
            .evaluate(&batch)?
            .into_array(batch.num_rows())
            .expect("Failed to convert to array");
        let result = as_int32_array(&result)?;

        let expected =
            &Int32Array::from(vec![Some(123), Some(123), Some(999), Some(123)]);

        assert_eq!(expected, result);

        Ok(())
    }

    #[test]
    fn case_without_expr_divide_by_zero() -> Result<()> {
        let batch = case_test_batch1()?;
        let schema = batch.schema();

        // CASE WHEN a > 0 THEN 25.0 / cast(a, float64) ELSE float64(null) END
        let when1 = binary(col("a", &schema)?, Operator::Gt, lit(0i32), &batch.schema())?;
        let then1 = binary(
            lit(25.0f64),
            Operator::Divide,
            cast(col("a", &schema)?, &batch.schema(), Float64)?,
            &batch.schema(),
        )?;
        let x = lit(ScalarValue::Float64(None));

        let expr = generate_case_when_with_type_coercion(
            None,
            vec![(when1, then1)],
            Some(x),
            schema.as_ref(),
        )?;
        let result = expr
            .evaluate(&batch)?
            .into_array(batch.num_rows())
            .expect("Failed to convert to array");
        let result =
            as_float64_array(&result).expect("failed to downcast to Float64Array");

        let expected = &Float64Array::from(vec![Some(25.0), None, None, Some(5.0)]);

        assert_eq!(expected, result);

        Ok(())
    }

    fn case_test_batch1() -> Result<RecordBatch> {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
            Field::new("c", DataType::Int32, true),
        ]);
        let a = Int32Array::from(vec![Some(1), Some(0), None, Some(5)]);
        let b = Int32Array::from(vec![Some(3), None, Some(14), Some(7)]);
        let c = Int32Array::from(vec![Some(0), Some(-3), Some(777), None]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(a), Arc::new(b), Arc::new(c)],
        )?;
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
            lit("foo"),
            &batch.schema(),
        )?;
        let then1 = lit(123i32);
        let when2 = binary(
            col("a", &schema)?,
            Operator::Eq,
            lit("bar"),
            &batch.schema(),
        )?;
        let then2 = lit(456i32);
        let else_value = lit(999i32);

        let expr = generate_case_when_with_type_coercion(
            None,
            vec![(when1, then1), (when2, then2)],
            Some(else_value),
            schema.as_ref(),
        )?;
        let result = expr
            .evaluate(&batch)?
            .into_array(batch.num_rows())
            .expect("Failed to convert to array");
        let result = as_int32_array(&result)?;

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
            lit("foo"),
            &batch.schema(),
        )?;
        let then = lit(123.3f64);
        let else_value = lit(999i32);

        let expr = generate_case_when_with_type_coercion(
            None,
            vec![(when, then)],
            Some(else_value),
            schema.as_ref(),
        )?;
        let result = expr
            .evaluate(&batch)?
            .into_array(batch.num_rows())
            .expect("Failed to convert to array");
        let result =
            as_float64_array(&result).expect("failed to downcast to Float64Array");

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
            lit(1.77f64),
            &batch.schema(),
        )?;
        let then = col("load4", &schema)?;

        let expr = generate_case_when_with_type_coercion(
            None,
            vec![(when, then)],
            None,
            schema.as_ref(),
        )?;
        let result = expr
            .evaluate(&batch)?
            .into_array(batch.num_rows())
            .expect("Failed to convert to array");
        let result =
            as_float64_array(&result).expect("failed to downcast to Float64Array");

        let expected =
            &Float64Array::from(vec![Some(1.77), None, None, None, None, Some(1.77)]);

        assert_eq!(expected, result);

        Ok(())
    }

    #[test]
    fn case_with_scalar_predicate() -> Result<()> {
        let batch = case_test_batch_nulls()?;
        let schema = batch.schema();

        // SELECT CASE WHEN TRUE THEN load4 END
        let when = lit(true);
        let then = col("load4", &schema)?;
        let expr = generate_case_when_with_type_coercion(
            None,
            vec![(when, then)],
            None,
            schema.as_ref(),
        )?;

        // many rows
        let result = expr
            .evaluate(&batch)?
            .into_array(batch.num_rows())
            .expect("Failed to convert to array");
        let result =
            as_float64_array(&result).expect("failed to downcast to Float64Array");
        let expected = &Float64Array::from(vec![
            Some(1.77),
            None,
            None,
            Some(1.78),
            None,
            Some(1.77),
        ]);
        assert_eq!(expected, result);

        // one row
        let expected = Float64Array::from(vec![Some(1.1)]);
        let batch =
            RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(expected.clone())])?;
        let result = expr
            .evaluate(&batch)?
            .into_array(batch.num_rows())
            .expect("Failed to convert to array");
        let result =
            as_float64_array(&result).expect("failed to downcast to Float64Array");
        assert_eq!(&expected, result);

        Ok(())
    }

    #[test]
    fn case_expr_matches_and_nulls() -> Result<()> {
        let batch = case_test_batch_nulls()?;
        let schema = batch.schema();

        // SELECT CASE load4 WHEN 1.77 THEN load4 END
        let expr = col("load4", &schema)?;
        let when = lit(1.77f64);
        let then = col("load4", &schema)?;

        let expr = generate_case_when_with_type_coercion(
            Some(expr),
            vec![(when, then)],
            None,
            schema.as_ref(),
        )?;
        let result = expr
            .evaluate(&batch)?
            .into_array(batch.num_rows())
            .expect("Failed to convert to array");
        let result =
            as_float64_array(&result).expect("failed to downcast to Float64Array");

        let expected =
            &Float64Array::from(vec![Some(1.77), None, None, None, None, Some(1.77)]);

        assert_eq!(expected, result);

        Ok(())
    }

    #[test]
    fn test_when_null_and_some_cond_else_null() -> Result<()> {
        let batch = case_test_batch()?;
        let schema = batch.schema();

        let when = binary(
            Arc::new(Literal::new(ScalarValue::Boolean(None))),
            Operator::And,
            binary(col("a", &schema)?, Operator::Eq, lit("foo"), &schema)?,
            &schema,
        )?;
        let then = col("a", &schema)?;

        // SELECT CASE WHEN (NULL AND a = 'foo') THEN a ELSE NULL END
        let expr = Arc::new(CaseExpr::try_new(None, vec![(when, then)], None)?);
        let result = expr
            .evaluate(&batch)?
            .into_array(batch.num_rows())
            .expect("Failed to convert to array");
        let result = as_string_array(&result);

        // all result values should be null
        assert_eq!(result.logical_null_count(), batch.num_rows());
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

        let null_buffer = Buffer::from([0b00101001u8]);
        let load4 = load4
            .into_data()
            .into_builder()
            .null_bit_buffer(Some(null_buffer))
            .build()
            .unwrap();
        let load4: Float64Array = load4.into();

        let batch =
            RecordBatch::try_from_iter(vec![("load4", Arc::new(load4) as ArrayRef)])?;
        Ok(batch)
    }

    #[test]
    fn case_test_incompatible() -> Result<()> {
        // 1 then is int64
        // 2 then is boolean
        let batch = case_test_batch()?;
        let schema = batch.schema();

        // CASE WHEN a = 'foo' THEN 123 WHEN a = 'bar' THEN true END
        let when1 = binary(
            col("a", &schema)?,
            Operator::Eq,
            lit("foo"),
            &batch.schema(),
        )?;
        let then1 = lit(123i32);
        let when2 = binary(
            col("a", &schema)?,
            Operator::Eq,
            lit("bar"),
            &batch.schema(),
        )?;
        let then2 = lit(true);

        let expr = generate_case_when_with_type_coercion(
            None,
            vec![(when1, then1), (when2, then2)],
            None,
            schema.as_ref(),
        );
        assert!(expr.is_err());

        // then 1 is int32
        // then 2 is int64
        // else is float
        // CASE WHEN a = 'foo' THEN 123 WHEN a = 'bar' THEN 456 ELSE 1.23 END
        let when1 = binary(
            col("a", &schema)?,
            Operator::Eq,
            lit("foo"),
            &batch.schema(),
        )?;
        let then1 = lit(123i32);
        let when2 = binary(
            col("a", &schema)?,
            Operator::Eq,
            lit("bar"),
            &batch.schema(),
        )?;
        let then2 = lit(456i64);
        let else_expr = lit(1.23f64);

        let expr = generate_case_when_with_type_coercion(
            None,
            vec![(when1, then1), (when2, then2)],
            Some(else_expr),
            schema.as_ref(),
        );
        assert!(expr.is_ok());
        let result_type = expr.unwrap().data_type(schema.as_ref())?;
        assert_eq!(Float64, result_type);
        Ok(())
    }

    #[test]
    fn case_eq() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);

        let when1 = lit("foo");
        let then1 = lit(123i32);
        let when2 = lit("bar");
        let then2 = lit(456i32);
        let else_value = lit(999i32);

        let expr1 = generate_case_when_with_type_coercion(
            Some(col("a", &schema)?),
            vec![
                (Arc::clone(&when1), Arc::clone(&then1)),
                (Arc::clone(&when2), Arc::clone(&then2)),
            ],
            Some(Arc::clone(&else_value)),
            &schema,
        )?;

        let expr2 = generate_case_when_with_type_coercion(
            Some(col("a", &schema)?),
            vec![
                (Arc::clone(&when1), Arc::clone(&then1)),
                (Arc::clone(&when2), Arc::clone(&then2)),
            ],
            Some(Arc::clone(&else_value)),
            &schema,
        )?;

        let expr3 = generate_case_when_with_type_coercion(
            Some(col("a", &schema)?),
            vec![(Arc::clone(&when1), Arc::clone(&then1)), (when2, then2)],
            None,
            &schema,
        )?;

        let expr4 = generate_case_when_with_type_coercion(
            Some(col("a", &schema)?),
            vec![(when1, then1)],
            Some(else_value),
            &schema,
        )?;

        assert!(expr1.eq(&expr2));
        assert!(expr2.eq(&expr1));

        assert!(expr2.ne(&expr3));
        assert!(expr3.ne(&expr2));

        assert!(expr1.ne(&expr4));
        assert!(expr4.ne(&expr1));

        Ok(())
    }

    #[test]
    fn case_transform() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);

        let when1 = lit("foo");
        let then1 = lit(123i32);
        let when2 = lit("bar");
        let then2 = lit(456i32);
        let else_value = lit(999i32);

        let expr = generate_case_when_with_type_coercion(
            Some(col("a", &schema)?),
            vec![
                (Arc::clone(&when1), Arc::clone(&then1)),
                (Arc::clone(&when2), Arc::clone(&then2)),
            ],
            Some(Arc::clone(&else_value)),
            &schema,
        )?;

        let expr2 = Arc::clone(&expr)
            .transform(|e| {
                let transformed = match e.as_any().downcast_ref::<Literal>() {
                    Some(lit_value) => match lit_value.value() {
                        ScalarValue::Utf8(Some(str_value)) => {
                            Some(lit(str_value.to_uppercase()))
                        }
                        _ => None,
                    },
                    _ => None,
                };
                Ok(if let Some(transformed) = transformed {
                    Transformed::yes(transformed)
                } else {
                    Transformed::no(e)
                })
            })
            .data()
            .unwrap();

        let expr3 = Arc::clone(&expr)
            .transform_down(|e| {
                let transformed = match e.as_any().downcast_ref::<Literal>() {
                    Some(lit_value) => match lit_value.value() {
                        ScalarValue::Utf8(Some(str_value)) => {
                            Some(lit(str_value.to_uppercase()))
                        }
                        _ => None,
                    },
                    _ => None,
                };
                Ok(if let Some(transformed) = transformed {
                    Transformed::yes(transformed)
                } else {
                    Transformed::no(e)
                })
            })
            .data()
            .unwrap();

        assert!(expr.ne(&expr2));
        assert!(expr2.eq(&expr3));

        Ok(())
    }

    #[test]
    fn test_column_or_null_specialization() -> Result<()> {
        // create input data
        let mut c1 = Int32Builder::new();
        let mut c2 = StringBuilder::new();
        for i in 0..1000 {
            c1.append_value(i);
            if i % 7 == 0 {
                c2.append_null();
            } else {
                c2.append_value(format!("string {i}"));
            }
        }
        let c1 = Arc::new(c1.finish());
        let c2 = Arc::new(c2.finish());
        let schema = Schema::new(vec![
            Field::new("c1", DataType::Int32, true),
            Field::new("c2", DataType::Utf8, true),
        ]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![c1, c2]).unwrap();

        // CaseWhenExprOrNull should produce same results as CaseExpr
        let predicate = Arc::new(BinaryExpr::new(
            make_col("c1", 0),
            Operator::LtEq,
            make_lit_i32(250),
        ));
        let expr = CaseExpr::try_new(None, vec![(predicate, make_col("c2", 1))], None)?;
        assert!(matches!(expr.eval_method, EvalMethod::InfallibleExprOrNull));
        match expr.evaluate(&batch)? {
            ColumnarValue::Array(array) => {
                assert_eq!(1000, array.len());
                assert_eq!(785, array.null_count());
            }
            _ => unreachable!(),
        }
        Ok(())
    }

    #[test]
    fn test_expr_or_expr_specialization() -> Result<()> {
        let batch = case_test_batch1()?;
        let schema = batch.schema();
        let when = binary(
            col("a", &schema)?,
            Operator::LtEq,
            lit(2i32),
            &batch.schema(),
        )?;
        let then = col("b", &schema)?;
        let else_expr = col("c", &schema)?;
        let expr = CaseExpr::try_new(None, vec![(when, then)], Some(else_expr))?;
        assert!(matches!(
            expr.eval_method,
            EvalMethod::ExpressionOrExpression
        ));
        let result = expr
            .evaluate(&batch)?
            .into_array(batch.num_rows())
            .expect("Failed to convert to array");
        let result = as_int32_array(&result).expect("failed to downcast to Int32Array");

        let expected = &Int32Array::from(vec![Some(3), None, Some(777), None]);

        assert_eq!(expected, result);
        Ok(())
    }

    fn make_col(name: &str, index: usize) -> Arc<dyn PhysicalExpr> {
        Arc::new(Column::new(name, index))
    }

    fn make_lit_i32(n: i32) -> Arc<dyn PhysicalExpr> {
        Arc::new(Literal::new(ScalarValue::Int32(Some(n))))
    }

    fn generate_case_when_with_type_coercion(
        expr: Option<Arc<dyn PhysicalExpr>>,
        when_thens: Vec<WhenThen>,
        else_expr: Option<Arc<dyn PhysicalExpr>>,
        input_schema: &Schema,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let coerce_type =
            get_case_common_type(&when_thens, else_expr.clone(), input_schema);
        let (when_thens, else_expr) = match coerce_type {
            None => plan_err!(
                "Can't get a common type for then {when_thens:?} and else {else_expr:?} expression"
            ),
            Some(data_type) => {
                // cast then expr
                let left = when_thens
                    .into_iter()
                    .map(|(when, then)| {
                        let then = try_cast(then, input_schema, data_type.clone())?;
                        Ok((when, then))
                    })
                    .collect::<Result<Vec<_>>>()?;
                let right = match else_expr {
                    None => None,
                    Some(expr) => Some(try_cast(expr, input_schema, data_type.clone())?),
                };

                Ok((left, right))
            }
        }?;
        case(expr, when_thens, else_expr)
    }

    fn get_case_common_type(
        when_thens: &[WhenThen],
        else_expr: Option<Arc<dyn PhysicalExpr>>,
        input_schema: &Schema,
    ) -> Option<DataType> {
        let thens_type = when_thens
            .iter()
            .map(|when_then| {
                let data_type = &when_then.1.data_type(input_schema).unwrap();
                data_type.clone()
            })
            .collect::<Vec<_>>();
        let else_type = match else_expr {
            None => {
                // case when then exprs must have one then value
                thens_type[0].clone()
            }
            Some(else_phy_expr) => else_phy_expr.data_type(input_schema).unwrap(),
        };
        thens_type
            .iter()
            .try_fold(else_type, |left_type, right_type| {
                // TODO: now just use the `equal` coercion rule for case when. If find the issue, and
                // refactor again.
                comparison_coercion(&left_type, right_type)
            })
    }

    #[test]
    fn test_fmt_sql() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Utf8, true)]);

        // CASE WHEN a = 'foo' THEN 123.3 ELSE 999 END
        let when = binary(col("a", &schema)?, Operator::Eq, lit("foo"), &schema)?;
        let then = lit(123.3f64);
        let else_value = lit(999i32);

        let expr = generate_case_when_with_type_coercion(
            None,
            vec![(when, then)],
            Some(else_value),
            &schema,
        )?;

        let display_string = expr.to_string();
        assert_eq!(
            display_string,
            "CASE WHEN a@0 = foo THEN 123.3 ELSE TRY_CAST(999 AS Float64) END"
        );

        let sql_string = fmt_sql(expr.as_ref()).to_string();
        assert_eq!(
            sql_string,
            "CASE WHEN a = foo THEN 123.3 ELSE TRY_CAST(999 AS Float64) END"
        );

        Ok(())
    }

    // Test Lookup evaluation

    enum AssertLookupEvaluation {
        Used,
        NotUsed,
    }

    fn test_case_when_literal_lookup(
        values: ArrayRef,
        lookup_map: &[(ScalarValue, ScalarValue)],
        else_value: Option<ScalarValue>,
        expected: ArrayRef,
        assert_lookup_evaluation: AssertLookupEvaluation,
    ) {
        // Create lookup
        // CASE <expr>
        // WHEN <when_constant_1> THEN <then_constant_1>
        // WHEN <when_constant_2> THEN <then_constant_2>
        // [ ELSE <else_constant> ]

        let schema = Schema::new(vec![Field::new(
            "a",
            values.data_type().clone(),
            values.is_nullable(),
        )]);
        let schema = Arc::new(schema);

        let batch = RecordBatch::try_new(schema, vec![values])
            .expect("failed to create RecordBatch");

        let schema = batch.schema_ref();
        let case = col("a", schema).expect("failed to create col");

        let when_then = lookup_map
            .iter()
            .map(|(when, then)| {
                (
                    Arc::new(Literal::new(when.clone())) as _,
                    Arc::new(Literal::new(then.clone())) as _,
                )
            })
            .collect::<Vec<WhenThen>>();

        let else_expr = else_value.map(|else_value| {
            Arc::new(Literal::new(else_value)) as Arc<dyn PhysicalExpr>
        });
        let expr = CaseExpr::try_new(Some(case), when_then, else_expr)
            .expect("failed to create case");

        // Assert that we are testing what we intend to assert
        match assert_lookup_evaluation {
            AssertLookupEvaluation::Used => {
                assert!(
                    matches!(expr.eval_method, EvalMethod::WithExprScalarLookupTable(_)),
                    "we should use the expected eval method"
                );
            }
            AssertLookupEvaluation::NotUsed => {
                assert!(
                    !matches!(expr.eval_method, EvalMethod::WithExprScalarLookupTable(_)),
                    "we should not use lookup evaluation method"
                );
            }
        }

        let actual = expr
            .evaluate(&batch)
            .expect("failed to evaluate case")
            .into_array(batch.num_rows())
            .expect("Failed to convert to array");

        assert_eq!(
            actual.data_type(),
            expected.data_type(),
            "Data type mismatch"
        );

        assert_eq!(
            actual.as_ref(),
            expected.as_ref(),
            "actual (left) does not match expected (right)"
        );
    }

    fn create_lookup<When, Then>(
        when_then_pairs: impl IntoIterator<Item = (When, Then)>,
    ) -> Vec<(ScalarValue, ScalarValue)>
    where
        ScalarValue: From<When>,
        ScalarValue: From<Then>,
    {
        when_then_pairs
            .into_iter()
            .map(|(when, then)| (ScalarValue::from(when), ScalarValue::from(then)))
            .collect()
    }

    fn create_input_and_expected<Input, Expected, InputFromItem, ExpectedFromItem>(
        input_and_expected_pairs: impl IntoIterator<Item = (InputFromItem, ExpectedFromItem)>,
    ) -> (Input, Expected)
    where
        Input: Array + From<Vec<InputFromItem>>,
        Expected: Array + From<Vec<ExpectedFromItem>>,
    {
        let (input_items, expected_items): (Vec<InputFromItem>, Vec<ExpectedFromItem>) =
            input_and_expected_pairs.into_iter().unzip();

        (Input::from(input_items), Expected::from(expected_items))
    }

    fn test_lookup_eval_with_and_without_else(
        lookup_map: &[(ScalarValue, ScalarValue)],
        input_values: ArrayRef,
        expected: StringArray,
    ) {
        // Testing without ELSE should fallback to None
        test_case_when_literal_lookup(
            Arc::clone(&input_values),
            lookup_map,
            None,
            Arc::new(expected.clone()),
            AssertLookupEvaluation::Used,
        );

        // Testing with Else
        let else_value = "___fallback___";

        // Changing each expected None to be fallback
        let expected_with_else = expected
            .iter()
            .map(|item| item.unwrap_or(else_value))
            .map(Some)
            .collect::<StringArray>();

        // Test case
        test_case_when_literal_lookup(
            input_values,
            lookup_map,
            Some(ScalarValue::Utf8(Some(else_value.to_string()))),
            Arc::new(expected_with_else),
            AssertLookupEvaluation::Used,
        );
    }

    #[test]
    fn test_case_when_literal_lookup_int32_to_string() {
        let lookup_map = create_lookup([
            (Some(4), Some("four")),
            (Some(2), Some("two")),
            (Some(3), Some("three")),
            (Some(1), Some("one")),
        ]);

        let (input_values, expected) =
            create_input_and_expected::<Int32Array, StringArray, _, _>([
                (1, Some("one")),
                (2, Some("two")),
                (3, Some("three")),
                (3, Some("three")),
                (2, Some("two")),
                (3, Some("three")),
                (5, None), // No match in WHEN
                (5, None), // No match in WHEN
                (3, Some("three")),
                (5, None), // No match in WHEN
            ]);

        test_lookup_eval_with_and_without_else(
            &lookup_map,
            Arc::new(input_values),
            expected,
        );
    }

    #[test]
    fn test_case_when_literal_lookup_none_case_should_never_match() {
        let lookup_map = create_lookup([
            (Some(4), Some("four")),
            (None, Some("none")),
            (Some(2), Some("two")),
            (Some(1), Some("one")),
        ]);

        let (input_values, expected) =
            create_input_and_expected::<Int32Array, StringArray, _, _>([
                (Some(1), Some("one")),
                (Some(5), None), // No match in WHEN
                (None, None), // None cases are never match in CASE <expr> WHEN <value> syntax
                (Some(2), Some("two")),
                (None, None), // None cases are never match in CASE <expr> WHEN <value> syntax
                (None, None), // None cases are never match in CASE <expr> WHEN <value> syntax
                (Some(2), Some("two")),
                (Some(5), None), // No match in WHEN
            ]);

        test_lookup_eval_with_and_without_else(
            &lookup_map,
            Arc::new(input_values),
            expected,
        );
    }

    #[test]
    fn test_case_when_literal_lookup_int32_to_string_with_duplicate_cases() {
        let lookup_map = create_lookup([
            (Some(4), Some("four")),
            (Some(4), Some("no 4")),
            (Some(2), Some("two")),
            (Some(2), Some("no 2")),
            (Some(3), Some("three")),
            (Some(3), Some("no 3")),
            (Some(2), Some("no 2")),
            (Some(4), Some("no 4")),
            (Some(2), Some("no 2")),
            (Some(3), Some("no 3")),
            (Some(4), Some("no 4")),
            (Some(2), Some("no 2")),
            (Some(3), Some("no 3")),
            (Some(3), Some("no 3")),
        ]);

        let (input_values, expected) =
            create_input_and_expected::<Int32Array, StringArray, _, _>([
                (1, None), // No match in WHEN
                (2, Some("two")),
                (3, Some("three")),
                (3, Some("three")),
                (2, Some("two")),
                (3, Some("three")),
                (5, None), // No match in WHEN
                (5, None), // No match in WHEN
                (3, Some("three")),
                (5, None), // No match in WHEN
            ]);

        test_lookup_eval_with_and_without_else(
            &lookup_map,
            Arc::new(input_values),
            expected,
        );
    }

    #[test]
    fn test_case_when_literal_lookup_f32_to_string_with_special_values_and_duplicate_cases(
    ) {
        let lookup_map = create_lookup([
            (Some(4.0), Some("four point zero")),
            (Some(f32::NAN), Some("NaN")),
            (Some(3.2), Some("three point two")),
            // Duplicate case to make sure it is not used
            (Some(f32::NAN), Some("should not use this NaN branch")),
            (Some(f32::INFINITY), Some("Infinity")),
            (Some(0.0), Some("zero")),
            // Duplicate case to make sure it is not used
            (
                Some(f32::INFINITY),
                Some("should not use this Infinity branch"),
            ),
            (Some(1.1), Some("one point one")),
        ]);

        let (input_values, expected) =
            create_input_and_expected::<Float32Array, StringArray, _, _>([
                (1.1, Some("one point one")),
                (f32::NAN, Some("NaN")),
                (3.2, Some("three point two")),
                (3.2, Some("three point two")),
                (0.0, Some("zero")),
                (f32::INFINITY, Some("Infinity")),
                (3.2, Some("three point two")),
                (f32::NEG_INFINITY, None), // No match in WHEN
                (f32::NEG_INFINITY, None), // No match in WHEN
                (3.2, Some("three point two")),
                (-0.0, None), // No match in WHEN
            ]);

        test_lookup_eval_with_and_without_else(
            &lookup_map,
            Arc::new(input_values),
            expected,
        );
    }

    #[test]
    fn test_case_when_literal_lookup_f16_to_string_with_special_values() {
        let lookup_map = create_lookup([
            (
                ScalarValue::Float16(Some(f16::from_f32(3.2))),
                Some("3 dot 2"),
            ),
            (ScalarValue::Float16(Some(f16::NAN)), Some("NaN")),
            (
                ScalarValue::Float16(Some(f16::from_f32(17.4))),
                Some("17 dot 4"),
            ),
            (ScalarValue::Float16(Some(f16::INFINITY)), Some("Infinity")),
            (ScalarValue::Float16(Some(f16::ZERO)), Some("zero")),
        ]);

        let (input_values, expected) =
            create_input_and_expected::<Float16Array, StringArray, _, _>([
                (f16::from_f32(3.2), Some("3 dot 2")),
                (f16::NAN, Some("NaN")),
                (f16::from_f32(17.4), Some("17 dot 4")),
                (f16::from_f32(17.4), Some("17 dot 4")),
                (f16::INFINITY, Some("Infinity")),
                (f16::from_f32(17.4), Some("17 dot 4")),
                (f16::NEG_INFINITY, None), // No match in WHEN
                (f16::NEG_INFINITY, None), // No match in WHEN
                (f16::from_f32(17.4), Some("17 dot 4")),
                (f16::NEG_ZERO, None), // No match in WHEN
            ]);

        test_lookup_eval_with_and_without_else(
            &lookup_map,
            Arc::new(input_values),
            expected,
        );
    }

    #[test]
    fn test_case_when_literal_lookup_f32_to_string_with_special_values() {
        let lookup_map = create_lookup([
            (3.2, Some("3 dot 2")),
            (f32::NAN, Some("NaN")),
            (17.4, Some("17 dot 4")),
            (f32::INFINITY, Some("Infinity")),
            (f32::ZERO, Some("zero")),
        ]);

        let (input_values, expected) =
            create_input_and_expected::<Float32Array, StringArray, _, _>([
                (3.2, Some("3 dot 2")),
                (f32::NAN, Some("NaN")),
                (17.4, Some("17 dot 4")),
                (17.4, Some("17 dot 4")),
                (f32::INFINITY, Some("Infinity")),
                (17.4, Some("17 dot 4")),
                (f32::NEG_INFINITY, None), // No match in WHEN
                (f32::NEG_INFINITY, None), // No match in WHEN
                (17.4, Some("17 dot 4")),
                (-0.0, None), // No match in WHEN
            ]);

        test_lookup_eval_with_and_without_else(
            &lookup_map,
            Arc::new(input_values),
            expected,
        );
    }

    #[test]
    fn test_case_when_literal_lookup_f64_to_string_with_special_values() {
        let lookup_map = create_lookup([
            (3.2, Some("3 dot 2")),
            (f64::NAN, Some("NaN")),
            (17.4, Some("17 dot 4")),
            (f64::INFINITY, Some("Infinity")),
            (f64::ZERO, Some("zero")),
        ]);

        let (input_values, expected) =
            create_input_and_expected::<Float64Array, StringArray, _, _>([
                (3.2, Some("3 dot 2")),
                (f64::NAN, Some("NaN")),
                (17.4, Some("17 dot 4")),
                (17.4, Some("17 dot 4")),
                (f64::INFINITY, Some("Infinity")),
                (17.4, Some("17 dot 4")),
                (f64::NEG_INFINITY, None), // No match in WHEN
                (f64::NEG_INFINITY, None), // No match in WHEN
                (17.4, Some("17 dot 4")),
                (-0.0, None), // No match in WHEN
            ]);

        test_lookup_eval_with_and_without_else(
            &lookup_map,
            Arc::new(input_values),
            expected,
        );
    }

    // Test that we don't lose the decimal precision and scale info
    #[test]
    fn test_decimal_with_non_default_precision_and_scale() {
        let lookup_map = create_lookup([
            (ScalarValue::Decimal32(Some(4), 3, 2), Some("four")),
            (ScalarValue::Decimal32(Some(2), 3, 2), Some("two")),
            (ScalarValue::Decimal32(Some(3), 3, 2), Some("three")),
            (ScalarValue::Decimal32(Some(1), 3, 2), Some("one")),
        ]);

        let (input_values, expected) =
            create_input_and_expected::<Decimal32Array, StringArray, _, _>([
                (1, Some("one")),
                (2, Some("two")),
                (3, Some("three")),
                (3, Some("three")),
                (2, Some("two")),
                (3, Some("three")),
                (5, None), // No match in WHEN
                (5, None), // No match in WHEN
                (3, Some("three")),
                (5, None), // No match in WHEN
            ]);

        let input_values = input_values
            .with_precision_and_scale(3, 2)
            .expect("must be able to set precision and scale");

        test_lookup_eval_with_and_without_else(
            &lookup_map,
            Arc::new(input_values),
            expected,
        );
    }

    // Test that we don't lose the timezone info
    #[test]
    fn test_timestamp_with_non_default_timezone() {
        let timezone: Option<Arc<str>> = Some("-10:00".into());
        let lookup_map = create_lookup([
            (
                ScalarValue::TimestampMillisecond(Some(4), timezone.clone()),
                Some("four"),
            ),
            (
                ScalarValue::TimestampMillisecond(Some(2), timezone.clone()),
                Some("two"),
            ),
            (
                ScalarValue::TimestampMillisecond(Some(3), timezone.clone()),
                Some("three"),
            ),
            (
                ScalarValue::TimestampMillisecond(Some(1), timezone.clone()),
                Some("one"),
            ),
        ]);

        let (input_values, expected) =
            create_input_and_expected::<TimestampMillisecondArray, StringArray, _, _>([
                (1, Some("one")),
                (2, Some("two")),
                (3, Some("three")),
                (3, Some("three")),
                (2, Some("two")),
                (3, Some("three")),
                (5, None), // No match in WHEN
                (5, None), // No match in WHEN
                (3, Some("three")),
                (5, None), // No match in WHEN
            ]);

        let input_values = input_values.with_timezone_opt(timezone);

        test_lookup_eval_with_and_without_else(
            &lookup_map,
            Arc::new(input_values),
            expected,
        );
    }

    #[test]
    fn test_with_strings_to_int32() {
        let lookup_map = create_lookup([
            (Some("why"), Some(42)),
            (Some("what"), Some(22)),
            (Some("when"), Some(17)),
        ]);

        let (input_values, expected) =
            create_input_and_expected::<StringArray, Int32Array, _, _>([
                (Some("why"), Some(42)),
                (Some("5"), None), // No match in WHEN
                (None, None), // None cases are never match in CASE <expr> WHEN <value> syntax
                (Some("what"), Some(22)),
                (None, None), // None cases are never match in CASE <expr> WHEN <value> syntax
                (None, None), // None cases are never match in CASE <expr> WHEN <value> syntax
                (Some("what"), Some(22)),
                (Some("5"), None), // No match in WHEN
            ]);

        let input_values = Arc::new(input_values) as ArrayRef;

        // Testing without ELSE should fallback to None
        test_case_when_literal_lookup(
            Arc::clone(&input_values),
            &lookup_map,
            None,
            Arc::new(expected.clone()),
            AssertLookupEvaluation::Used,
        );

        // Testing with Else
        let else_value = 101;

        // Changing each expected None to be fallback
        let expected_with_else = expected
            .iter()
            .map(|item| item.unwrap_or(else_value))
            .map(Some)
            .collect::<Int32Array>();

        // Test case
        test_case_when_literal_lookup(
            input_values,
            &lookup_map,
            Some(ScalarValue::Int32(Some(else_value))),
            Arc::new(expected_with_else),
            AssertLookupEvaluation::Used,
        );
    }

    #[test]
    fn test_with_bytes_to_string() {
        test_string_casted_to_string(DataType::Binary);
    }

    #[test]
    fn test_with_large_bytes_to_string() {
        test_string_casted_to_string(DataType::LargeBinary);
    }

    #[test]
    fn test_with_fixed_size_bytes_to_string() {
        test_fixed_binary_casted_to_string(DataType::FixedSizeBinary(3));
    }

    #[test]
    fn test_with_string_view_to_string() {
        test_string_casted_to_string(DataType::Utf8View);
    }

    #[test]
    fn test_with_binary_view_to_string() {
        test_string_casted_to_string(DataType::BinaryView);
    }

    fn test_string_casted_to_string(input_data_type: DataType) {
        let mut lookup_map = create_lookup([
            (Some("why".to_string()), Some("one")),
            (Some("what".to_string()), Some("two")),
            (Some("when".to_string()), Some("three")),
        ]);

        // Cast all when to the input data type
        lookup_map.iter_mut().for_each(|(when, _)| {
            *when = when
                .cast_to(&input_data_type)
                .expect("should be able to cast");
        });

        let (input_values, expected) =
            create_input_and_expected::<StringArray, StringArray, _, _>([
                (Some("why"), Some("one")),
                (Some("5"), None), // No match in WHEN
                (None, None), // None cases are never match in CASE <expr> WHEN <value> syntax
                (Some("what"), Some("two")),
                (None, None), // None cases are never match in CASE <expr> WHEN <value> syntax
                (None, None), // None cases are never match in CASE <expr> WHEN <value> syntax
                (Some("what"), Some("two")),
                (Some("5"), None), // No match in WHEN
            ]);

        let input_values = arrow::compute::cast(&input_values, &input_data_type)
            .expect("should be able to cast");

        test_lookup_eval_with_and_without_else(
            &lookup_map,
            Arc::new(input_values),
            expected,
        );
    }

    fn test_fixed_binary_casted_to_string(input_data_type: DataType) {
        let mut lookup_map = create_lookup([
            (
                ScalarValue::FixedSizeBinary(3, Some("why".as_bytes().to_vec())),
                Some("one"),
            ),
            (
                ScalarValue::FixedSizeBinary(3, Some("wha".as_bytes().to_vec())),
                Some("two"),
            ),
            (
                ScalarValue::FixedSizeBinary(3, Some("whe".as_bytes().to_vec())),
                Some("three"),
            ),
        ]);

        // Cast all when to the input data type
        lookup_map.iter_mut().for_each(|(when, _)| {
            *when = when
                .cast_to(&input_data_type)
                .expect("should be able to cast");
        });

        let (input_values, expected) =
            create_input_and_expected::<FixedSizeBinaryArray, StringArray, _, _>([
                (Some(b"why" as &[u8]), Some("one")),
                (Some(b"555"), None), // No match in WHEN
                (None, None), // None cases are never match in CASE <expr> WHEN <value> syntax
                (Some(b"wha"), Some("two")),
                (None, None), // None cases are never match in CASE <expr> WHEN <value> syntax
                (None, None), // None cases are never match in CASE <expr> WHEN <value> syntax
                (Some(b"wha"), Some("two")),
                (Some(b"555"), None), // No match in WHEN
            ]);

        let input_values = arrow::compute::cast(&input_values, &input_data_type)
            .expect("should be able to cast");

        test_lookup_eval_with_and_without_else(
            &lookup_map,
            Arc::new(input_values),
            expected,
        );
    }

    #[test]
    fn test_large_string_to_string() {
        test_string_casted_to_string(DataType::LargeUtf8);
    }

    #[test]
    fn test_different_dictionary_keys_with_string_values_to_string() {
        for key_type in [
            DataType::Int8,
            DataType::UInt8,
            DataType::Int16,
            DataType::UInt16,
            DataType::Int32,
            DataType::UInt32,
            DataType::Int64,
            DataType::UInt64,
        ] {
            test_string_casted_to_string(DataType::Dictionary(
                Box::new(key_type),
                Box::new(DataType::Utf8),
            ));
        }
    }

    #[test]
    fn test_string_like_dictionary_to_string() {
        for data_type in [DataType::Utf8, DataType::LargeUtf8] {
            test_string_casted_to_string(
                // test int
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(data_type)),
            );
        }
    }

    #[test]
    fn test_binary_like_dictionary_to_string() {
        for data_type in [
            DataType::Binary,
            DataType::LargeBinary,
            DataType::FixedSizeBinary(3),
        ] {
            test_fixed_binary_casted_to_string(
                // test int
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(data_type)),
            );
        }
    }

    fn test_dictionary_view_value_to_string(input_data_type: DataType) {
        /// Because casting From String to Dictionary<Int32, StringView> or to Dictionary<Int32, BinaryView>
        /// we need to do manual casting
        fn cast_to_dictionary_of_view(
            array_to_cast: &dyn Array,
            input_data_type: &DataType,
        ) -> ArrayRef {
            let string_dictionary = arrow::compute::cast(
                array_to_cast,
                &DataType::Dictionary(
                    Box::new(DataType::Int32),
                    Box::new(DataType::Utf8),
                ),
            )
            .expect("should be able to cast");

            let string_dictionary = string_dictionary.as_dictionary::<Int32Type>();
            let (keys, values) = string_dictionary.clone().into_parts();
            let dictionary_values_casted = arrow::compute::cast(&values, input_data_type)
                .expect("should be able to cast");

            let final_dictionary_array =
                DictionaryArray::new(keys, dictionary_values_casted);

            Arc::new(final_dictionary_array)
        }

        let mut lookup_map = create_lookup([
            (Some("why".to_string()), Some("one")),
            (Some("what".to_string()), Some("two")),
            (Some("when".to_string()), Some("three")),
        ]);

        // Cast all when to the input data type
        lookup_map.iter_mut().for_each(|(when, _)| {
            // First cast to dictionary of string
            let when_array = when
                .to_array_of_size(1)
                .expect("should be able to convert scalar to array");
            let casted_array = cast_to_dictionary_of_view(&when_array, &input_data_type);
            *when = ScalarValue::try_from_array(&casted_array, 0)
                .expect("should be able to convert array to scalar");
        });

        let (input_values, expected) =
            create_input_and_expected::<StringArray, StringArray, _, _>([
                (Some("why"), Some("one")),
                (Some("5"), None), // No match in WHEN
                (None, None), // None cases are never match in CASE <expr> WHEN <value> syntax
                (Some("what"), Some("two")),
                (None, None), // None cases are never match in CASE <expr> WHEN <value> syntax
                (None, None), // None cases are never match in CASE <expr> WHEN <value> syntax
                (Some("what"), Some("two")),
                (Some("5"), None), // No match in WHEN
            ]);

        let input_values = cast_to_dictionary_of_view(&input_values, &input_data_type);

        test_lookup_eval_with_and_without_else(
            &lookup_map,
            Arc::new(input_values),
            expected,
        );
    }

    #[test]
    fn test_string_and_binary_view_dictionary_to_string() {
        for data_type in [DataType::Utf8View, DataType::BinaryView] {
            test_dictionary_view_value_to_string(data_type);
        }
    }

    #[test]
    fn test_boolean_to_string_lookup_table() {
        let lookup_map = create_lookup([
            (Some(true), Some("one")),
            (Some(false), Some("two")),
            (Some(true), Some("three")),
        ]);

        let (input_values, expected) =
            create_input_and_expected::<BooleanArray, StringArray, _, _>([
                (Some(true), Some("one")),
                (None, None),
                (Some(false), Some("two")),
                (Some(true), Some("one")),
                (Some(false), Some("two")),
                (Some(false), Some("two")),
                (None, None),
            ]);

        test_lookup_eval_with_and_without_else(
            &lookup_map,
            Arc::new(input_values),
            expected,
        );
    }

    #[test]
    fn test_with_strings_to_int32_with_different_else_type() {
        let lookup_map = create_lookup([
            (Some("why"), Some(42)),
            (Some("what"), Some(22)),
            (Some("when"), Some(17)),
        ]);

        let else_value = 101;

        let (input_values, expected) =
            create_input_and_expected::<StringArray, Int32Array, _, _>([
                (Some("why"), Some(42)),
                (Some("5"), Some(else_value)), // No match in WHEN
                (None, Some(else_value)), // None cases are never match in CASE <expr> WHEN <value> syntax
                (Some("what"), Some(22)),
                (None, Some(else_value)), // None cases are never match in CASE <expr> WHEN <value> syntax
                (None, Some(else_value)), // None cases are never match in CASE <expr> WHEN <value> syntax
                (Some("what"), Some(22)),
                (Some("5"), Some(else_value)), // No match in WHEN
            ]);

        let input_values = Arc::new(input_values) as ArrayRef;

        // Test case
        test_case_when_literal_lookup(
            input_values,
            &lookup_map,
            Some(ScalarValue::Int8(Some(else_value as i8))),
            Arc::new(expected),
            // Assert not used as the else type is different than the then data types
            AssertLookupEvaluation::NotUsed,
        );
    }
}
