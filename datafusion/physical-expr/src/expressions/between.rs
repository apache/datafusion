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

//! BETWEEN expression

use std::fmt;
use std::hash::Hash;
use std::sync::Arc;

use crate::PhysicalExpr;

use arrow::array::BooleanArray;
use arrow::compute::kernels::boolean;
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use datafusion_common::{Result, ScalarValue, cast::as_boolean_array, internal_err};
use datafusion_expr::{ColumnarValue, Operator};
use datafusion_physical_expr_common::datum::apply_cmp;

/// `BETWEEN` expression: `expr BETWEEN low AND high`.
///
/// `expr BETWEEN low AND high` gives the same result as
/// `expr >= low AND expr <= high`. The planner uses the second form by default,
/// because the optimizer, the interval analysis and the pruning predicates all
/// understand plain binary comparisons.
///
/// The second form names `expr` two times, so it also evaluates `expr` two
/// times. That is wrong when `expr` is volatile, because the two evaluations
/// give two different values. This expression evaluates `expr` one time and
/// compares that single result against both bounds. See
/// <https://github.com/apache/datafusion/issues/25457>.
#[derive(Debug, Eq)]
pub struct BetweenExpr {
    /// The value to test
    expr: Arc<dyn PhysicalExpr>,
    /// `NOT BETWEEN` when true
    negated: bool,
    /// The lower bound, inclusive
    low: Arc<dyn PhysicalExpr>,
    /// The upper bound, inclusive
    high: Arc<dyn PhysicalExpr>,
}

// Manually derive PartialEq and Hash to work around https://github.com/rust-lang/rust/issues/78808
impl PartialEq for BetweenExpr {
    fn eq(&self, other: &Self) -> bool {
        self.expr.eq(&other.expr)
            && self.negated == other.negated
            && self.low.eq(&other.low)
            && self.high.eq(&other.high)
    }
}

impl Hash for BetweenExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.expr.hash(state);
        self.negated.hash(state);
        self.low.hash(state);
        self.high.hash(state);
    }
}

impl BetweenExpr {
    /// Create a new `BETWEEN` expression
    pub fn new(
        expr: Arc<dyn PhysicalExpr>,
        negated: bool,
        low: Arc<dyn PhysicalExpr>,
        high: Arc<dyn PhysicalExpr>,
    ) -> Self {
        Self {
            expr,
            negated,
            low,
            high,
        }
    }

    /// The value to test
    pub fn expr(&self) -> &Arc<dyn PhysicalExpr> {
        &self.expr
    }

    /// True for `NOT BETWEEN`
    pub fn negated(&self) -> bool {
        self.negated
    }

    /// The lower bound, inclusive
    pub fn low(&self) -> &Arc<dyn PhysicalExpr> {
        &self.low
    }

    /// The upper bound, inclusive
    pub fn high(&self) -> &Arc<dyn PhysicalExpr> {
        &self.high
    }

    /// Apply the `NOT` of `NOT BETWEEN` to an array result
    fn negate_array(&self, result: BooleanArray) -> Result<BooleanArray> {
        if self.negated {
            Ok(boolean::not(&result)?)
        } else {
            Ok(result)
        }
    }

    /// Apply the `NOT` of `NOT BETWEEN` to a scalar result
    fn negate_scalar(&self, result: Option<bool>) -> Option<bool> {
        if self.negated {
            result.map(|v| !v)
        } else {
            result
        }
    }
}

impl fmt::Display for BetweenExpr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let negated = if self.negated { "NOT " } else { "" };
        write!(
            f,
            "{} {negated}BETWEEN {} AND {}",
            self.expr, self.low, self.high
        )
    }
}

impl PhysicalExpr for BetweenExpr {
    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        Ok(self.expr.nullable(input_schema)?
            || self.low.nullable(input_schema)?
            || self.high.nullable(input_schema)?)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        // Evaluate the value one time and compare that single result against
        // both bounds. This is what makes a volatile value correct.
        let value = self.expr.evaluate(batch)?;
        let low = self.low.evaluate(batch)?;
        let high = self.high.evaluate(batch)?;

        let ge_low = apply_cmp(Operator::GtEq, &value, &low)?;
        let le_high = apply_cmp(Operator::LtEq, &value, &high)?;

        match (ge_low, le_high) {
            (ColumnarValue::Scalar(ge_low), ColumnarValue::Scalar(le_high)) => {
                let result =
                    kleene_and(as_boolean_scalar(&ge_low)?, as_boolean_scalar(&le_high)?);
                Ok(ColumnarValue::Scalar(ScalarValue::Boolean(
                    self.negate_scalar(result),
                )))
            }
            (ge_low, le_high) => {
                let ge_low = ge_low.into_array(batch.num_rows())?;
                let le_high = le_high.into_array(batch.num_rows())?;
                let result = boolean::and_kleene(
                    as_boolean_array(&ge_low)?,
                    as_boolean_array(&le_high)?,
                )?;
                Ok(ColumnarValue::Array(Arc::new(self.negate_array(result)?)))
            }
        }
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.expr, &self.low, &self.high]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(BetweenExpr::new(
            Arc::clone(&children[0]),
            self.negated,
            Arc::clone(&children[1]),
            Arc::clone(&children[2]),
        )))
    }

    fn fmt_sql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.expr.fmt_sql(f)?;
        if self.negated {
            write!(f, " NOT")?;
        }
        write!(f, " BETWEEN ")?;
        self.low.fmt_sql(f)?;
        write!(f, " AND ")?;
        self.high.fmt_sql(f)
    }

    #[cfg(feature = "proto")]
    fn try_to_proto(
        &self,
        ctx: &datafusion_physical_expr_common::physical_expr::proto_encode::PhysicalExprEncodeCtx<'_>,
    ) -> Result<Option<datafusion_proto_models::protobuf::PhysicalExprNode>> {
        use datafusion_proto_models::protobuf;

        Ok(Some(protobuf::PhysicalExprNode {
            expr_id: None,
            expr_type: Some(protobuf::physical_expr_node::ExprType::Between(Box::new(
                protobuf::PhysicalBetweenNode {
                    expr: Some(Box::new(ctx.encode_child(&self.expr)?)),
                    negated: self.negated,
                    low: Some(Box::new(ctx.encode_child(&self.low)?)),
                    high: Some(Box::new(ctx.encode_child(&self.high)?)),
                },
            ))),
        }))
    }
}

#[cfg(feature = "proto")]
impl BetweenExpr {
    /// Reconstruct a [`BetweenExpr`] from its protobuf representation.
    pub fn try_from_proto(
        node: &datafusion_proto_models::protobuf::PhysicalExprNode,
        ctx: &datafusion_physical_expr_common::physical_expr::proto_decode::PhysicalExprDecodeCtx<'_>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        use datafusion_physical_expr_common::expect_expr_variant;
        use datafusion_proto_models::protobuf;

        let between = expect_expr_variant!(
            node,
            protobuf::physical_expr_node::ExprType::Between,
            "BetweenExpr",
        );

        let expr = ctx.decode_required_expression(
            between.expr.as_deref(),
            "BetweenExpr",
            "expr",
        )?;
        let low =
            ctx.decode_required_expression(between.low.as_deref(), "BetweenExpr", "low")?;
        let high = ctx.decode_required_expression(
            between.high.as_deref(),
            "BetweenExpr",
            "high",
        )?;

        Ok(Arc::new(BetweenExpr::new(expr, between.negated, low, high)))
    }
}

/// Kleene `AND` over two three valued booleans
fn kleene_and(lhs: Option<bool>, rhs: Option<bool>) -> Option<bool> {
    match (lhs, rhs) {
        (Some(false), _) | (_, Some(false)) => Some(false),
        (Some(true), Some(true)) => Some(true),
        _ => None,
    }
}

fn as_boolean_scalar(value: &ScalarValue) -> Result<Option<bool>> {
    match value {
        ScalarValue::Boolean(v) => Ok(*v),
        other => {
            internal_err!("BETWEEN comparison returned a non boolean scalar: {other:?}")
        }
    }
}

/// Create a `BETWEEN` expression
pub fn between(
    expr: Arc<dyn PhysicalExpr>,
    negated: bool,
    low: Arc<dyn PhysicalExpr>,
    high: Arc<dyn PhysicalExpr>,
) -> Arc<dyn PhysicalExpr> {
    Arc::new(BetweenExpr::new(expr, negated, low, high))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::{col, lit};

    use arrow::array::Int32Array;
    use arrow::datatypes::Field;
    use datafusion_physical_expr_common::physical_expr::fmt_sql;

    fn test_batch() -> Result<RecordBatch> {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
        let a = Int32Array::from(vec![Some(1), Some(3), Some(5), Some(7), None]);
        Ok(RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)])?)
    }

    fn evaluate_between(negated: bool) -> Result<Vec<Option<bool>>> {
        let batch = test_batch()?;
        let schema = batch.schema();
        let expr = between(col("a", &schema)?, negated, lit(3i32), lit(5i32));
        let result = expr.evaluate(&batch)?.into_array(batch.num_rows())?;
        Ok(as_boolean_array(&result)?.iter().collect())
    }

    #[test]
    fn between_bounds_are_inclusive() -> Result<()> {
        assert_eq!(
            evaluate_between(false)?,
            vec![Some(false), Some(true), Some(true), Some(false), None]
        );
        Ok(())
    }

    #[test]
    fn not_between_is_the_negation() -> Result<()> {
        assert_eq!(
            evaluate_between(true)?,
            vec![Some(true), Some(false), Some(false), Some(true), None]
        );
        Ok(())
    }

    #[test]
    fn scalar_input_gives_a_scalar_result() -> Result<()> {
        let batch = test_batch()?;

        let expr = between(lit(4i32), false, lit(3i32), lit(5i32));
        let ColumnarValue::Scalar(value) = expr.evaluate(&batch)? else {
            panic!("expected a scalar result");
        };
        assert_eq!(value, ScalarValue::Boolean(Some(true)));

        let expr = between(lit(ScalarValue::Int32(None)), false, lit(3i32), lit(5i32));
        let ColumnarValue::Scalar(value) = expr.evaluate(&batch)? else {
            panic!("expected a scalar result");
        };
        assert_eq!(value, ScalarValue::Boolean(None));
        Ok(())
    }

    #[test]
    fn display_and_fmt_sql() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
        let expr = between(col("a", &schema)?, false, lit(3i32), lit(5i32));
        assert_eq!(expr.to_string(), "a@0 BETWEEN 3 AND 5");
        assert_eq!(fmt_sql(expr.as_ref()).to_string(), "a BETWEEN 3 AND 5");

        let expr = between(col("a", &schema)?, true, lit(3i32), lit(5i32));
        assert_eq!(expr.to_string(), "a@0 NOT BETWEEN 3 AND 5");
        assert_eq!(fmt_sql(expr.as_ref()).to_string(), "a NOT BETWEEN 3 AND 5");
        Ok(())
    }
}
