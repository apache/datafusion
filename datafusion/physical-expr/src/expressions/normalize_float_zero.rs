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

//! Floating-point signed-zero normalization expression.

use std::hash::Hash;
use std::sync::Arc;

use arrow::datatypes::{DataType, FieldRef, Schema};
use arrow::record_batch::RecordBatch;
use datafusion_common::Result;
use datafusion_common::utils::{normalize_float_zero, normalize_float_zero_scalar};
use datafusion_expr::ColumnarValue;
use datafusion_expr::interval_arithmetic::Interval;
use datafusion_expr::sort_properties::ExprProperties;

use crate::PhysicalExpr;

/// Replaces floating-point `-0.0` values with `+0.0`.
///
/// Other values and data types are returned unchanged. This expression is
/// order-preserving but not strictly order-preserving because it collapses the
/// two signed-zero representations.
#[derive(Debug, Eq)]
pub struct NormalizeFloatZeroExpr {
    arg: Arc<dyn PhysicalExpr>,
}

impl PartialEq for NormalizeFloatZeroExpr {
    fn eq(&self, other: &Self) -> bool {
        self.arg.eq(&other.arg)
    }
}

impl Hash for NormalizeFloatZeroExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.arg.hash(state);
    }
}

impl NormalizeFloatZeroExpr {
    /// Creates a signed-zero normalization expression.
    pub fn new(arg: Arc<dyn PhysicalExpr>) -> Self {
        Self { arg }
    }

    /// Returns the input expression.
    pub fn arg(&self) -> &Arc<dyn PhysicalExpr> {
        &self.arg
    }
}

impl std::fmt::Display for NormalizeFloatZeroExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "normalize_float_zero({})", self.arg)
    }
}

impl PhysicalExpr for NormalizeFloatZeroExpr {
    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        self.arg.data_type(input_schema)
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        self.arg.nullable(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        Ok(match self.arg.evaluate(batch)? {
            ColumnarValue::Array(array) => {
                ColumnarValue::Array(normalize_float_zero(&array))
            }
            ColumnarValue::Scalar(scalar) => {
                ColumnarValue::Scalar(normalize_float_zero_scalar(scalar))
            }
        })
    }

    fn return_field(&self, input_schema: &Schema) -> Result<FieldRef> {
        self.arg.return_field(input_schema)
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.arg]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(Self::new(Arc::clone(&children[0]))))
    }

    fn evaluate_bounds(&self, children: &[&Interval]) -> Result<Interval> {
        Ok(children[0].clone())
    }

    fn get_properties(&self, children: &[ExprProperties]) -> Result<ExprProperties> {
        Ok(children[0].clone().with_strictly_order_preserving(false))
    }

    fn fmt_sql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "normalize_float_zero(")?;
        self.arg.fmt_sql(f)?;
        write!(f, ")")
    }

    #[cfg(feature = "proto")]
    fn try_to_proto(
        &self,
        ctx: &datafusion_physical_expr_common::physical_expr::proto_encode::PhysicalExprEncodeCtx<'_>,
    ) -> Result<Option<datafusion_proto_models::protobuf::PhysicalExprNode>> {
        use datafusion_proto_models::protobuf;

        Ok(Some(protobuf::PhysicalExprNode {
            expr_id: None,
            expr_type: Some(protobuf::physical_expr_node::ExprType::NormalizeFloatZero(
                Box::new(protobuf::PhysicalNormalizeFloatZeroNode {
                    expr: Some(Box::new(ctx.encode_child(&self.arg)?)),
                }),
            )),
        }))
    }
}

#[cfg(feature = "proto")]
impl NormalizeFloatZeroExpr {
    /// Reconstructs a [`NormalizeFloatZeroExpr`] from protobuf.
    pub fn try_from_proto(
        node: &datafusion_proto_models::protobuf::PhysicalExprNode,
        ctx: &datafusion_physical_expr_common::physical_expr::proto_decode::PhysicalExprDecodeCtx<'_>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        use datafusion_physical_expr_common::expect_expr_variant;
        use datafusion_proto_models::protobuf;

        let node = expect_expr_variant!(
            node,
            protobuf::physical_expr_node::ExprType::NormalizeFloatZero,
            "NormalizeFloatZero",
        );
        let arg = ctx.decode_required_expression(
            node.expr.as_deref(),
            "NormalizeFloatZeroExpr",
            "expr",
        )?;
        Ok(Arc::new(Self::new(arg)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::array::{ArrayRef, AsArray, Float64Array};
    use datafusion_common::ScalarValue;

    use crate::expressions::{Column, Literal};

    #[test]
    fn normalizes_array_and_scalar_signed_zero() -> Result<()> {
        let batch = RecordBatch::try_from_iter(vec![(
            "a",
            Arc::new(Float64Array::from(vec![-0.0, 0.0, 1.0])) as ArrayRef,
        )])?;
        let expr = NormalizeFloatZeroExpr::new(Arc::new(Column::new("a", 0)));
        let ColumnarValue::Array(array) = expr.evaluate(&batch)? else {
            panic!("column evaluation must return an array");
        };
        let array = array.as_primitive::<arrow::datatypes::Float64Type>();
        assert_eq!(array.value(0).to_bits(), 0.0_f64.to_bits());
        assert_eq!(array.value(1).to_bits(), 0.0_f64.to_bits());
        assert_eq!(array.value(2), 1.0);

        let expr = NormalizeFloatZeroExpr::new(Arc::new(Literal::new(
            ScalarValue::Float64(Some(-0.0)),
        )));
        let ColumnarValue::Scalar(ScalarValue::Float64(Some(value))) =
            expr.evaluate(&RecordBatch::new_empty(Arc::new(Schema::empty())))?
        else {
            panic!("literal evaluation must return a Float64 scalar");
        };
        assert_eq!(value.to_bits(), 0.0_f64.to_bits());
        Ok(())
    }
}
