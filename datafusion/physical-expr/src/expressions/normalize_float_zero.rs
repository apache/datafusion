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

//! Canonical floating-point zeros in window partition keys.

use std::hash::Hash;
use std::sync::Arc;

use arrow::datatypes::{DataType, FieldRef, Schema};
use arrow::record_batch::RecordBatch;
use datafusion_common::utils::{normalize_float_zero, normalize_float_zero_scalar};
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::ColumnarValue;
use datafusion_expr::interval_arithmetic::Interval;
use datafusion_expr::sort_properties::ExprProperties;

use crate::PhysicalExpr;

/// Replaces negative floating-point zeros with positive zeros, including nested values.
///
/// # Public Only for Internal Use
///
/// Used by window planning and physical-plan serialization. Normalizing the partition
/// expression makes sorting, partition boundaries, and cross-batch keys agree without
/// changing the original input columns.
#[doc(hidden)]
#[derive(Debug, Eq)]
pub struct NormalizeFloatZeroExpr {
    arg: Arc<dyn PhysicalExpr>,
}

// Manually implement PartialEq and Hash to work around rust-lang/rust#78808.
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
    /// Create a normalized window partition key.
    pub fn new(arg: Arc<dyn PhysicalExpr>) -> Self {
        Self { arg }
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

    fn return_field(&self, input_schema: &Schema) -> Result<FieldRef> {
        self.arg.return_field(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        Ok(match self.arg.evaluate(batch)? {
            ColumnarValue::Array(array) => {
                ColumnarValue::Array(normalize_float_zero(&array))
            }
            ColumnarValue::Scalar(scalar) if scalar.data_type().is_floating() => {
                ColumnarValue::Scalar(normalize_float_zero_scalar(scalar))
            }
            ColumnarValue::Scalar(scalar) => {
                // Nested scalar values need the recursive array normalization too.
                let array = normalize_float_zero(&scalar.to_array()?);
                ColumnarValue::Scalar(ScalarValue::try_from_array(&array, 0)?)
            }
        })
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

    fn get_properties(&self, children: &[ExprProperties]) -> Result<ExprProperties> {
        let child = &children[0];
        if child.range.data_type().is_floating() {
            Ok(ExprProperties {
                sort_properties: child.sort_properties,
                range: Interval::try_new(
                    normalize_float_zero_scalar(child.range.lower().clone()),
                    normalize_float_zero_scalar(child.range.upper().clone()),
                )?,
                preserves_lex_ordering: true,
                // Merging signed-zero groups can invalidate suffix sort keys.
                strictly_order_preserving: false,
            })
        } else {
            // Nested values are not monotone: [-0, 2] < [+0, 1], but [0, 2] > [0, 1].
            Ok(ExprProperties::new_unknown())
        }
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
    /// Reconstruct a normalized partition key from its protobuf representation.
    pub fn try_from_proto(
        node: &datafusion_proto_models::protobuf::PhysicalExprNode,
        ctx: &datafusion_physical_expr_common::physical_expr::proto_decode::PhysicalExprDecodeCtx<'_>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        use datafusion_physical_expr_common::expect_expr_variant;
        use datafusion_proto_models::protobuf;

        let n = expect_expr_variant!(
            node,
            protobuf::physical_expr_node::ExprType::NormalizeFloatZero,
            "NormalizeFloatZero",
        );
        let expr = ctx.decode_required_expression(
            n.expr.as_deref(),
            "NormalizeFloatZeroExpr",
            "expr",
        )?;
        Ok(Arc::new(Self::new(expr)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::Literal;
    use arrow::array::{ArrayRef, Float64Array, ListArray};
    use arrow::datatypes::Float64Type;

    #[test]
    fn normalize_scalar_and_nested_scalar() -> Result<()> {
        let batch = RecordBatch::new_empty(Arc::new(Schema::empty()));
        let nested: ArrayRef =
            Arc::new(ListArray::from_iter_primitive::<Float64Type, _, _>(vec![
                Some(vec![Some(-0.0), None, Some(1.0)]),
            ]));
        for scalar in [
            ScalarValue::Float64(Some(-0.0)),
            ScalarValue::try_from_array(&nested, 0)?,
        ] {
            let expected = ScalarValue::try_from_array(
                &normalize_float_zero(&scalar.to_array()?),
                0,
            )?;
            let expr = NormalizeFloatZeroExpr::new(Arc::new(Literal::new(scalar)));
            let ColumnarValue::Scalar(actual) = expr.evaluate(&batch)? else {
                panic!("scalar input must remain scalar");
            };
            assert_eq!(actual, expected);
        }
        // The array path must preserve nulls and the original input buffer.
        let input: ArrayRef = Arc::new(Float64Array::from(vec![Some(-0.0), None]));
        let schema = Arc::new(Schema::new(vec![arrow::datatypes::Field::new(
            "a",
            DataType::Float64,
            true,
        )]));
        let batch = RecordBatch::try_new(schema, vec![Arc::clone(&input)])?;
        let expr = NormalizeFloatZeroExpr::new(Arc::new(
            crate::expressions::Column::new("a", 0),
        ));
        let actual = expr.evaluate(&batch)?.into_array(2)?;
        assert_eq!(
            ScalarValue::try_from_array(&actual, 0)?,
            ScalarValue::Float64(Some(0.0))
        );
        assert!(actual.is_null(1));
        assert_eq!(
            ScalarValue::try_from_array(&input, 0)?,
            ScalarValue::Float64(Some(-0.0))
        );
        Ok(())
    }
}
