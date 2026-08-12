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

//! Negation (-) expression

use std::hash::Hash;
use std::sync::Arc;

use crate::PhysicalExpr;

use arrow::datatypes::FieldRef;
use arrow::{
    compute::kernels::numeric::neg_wrapping,
    datatypes::{DataType, Schema},
    record_batch::RecordBatch,
};
use datafusion_common::{Result, internal_err, plan_err};
use datafusion_expr::interval_arithmetic::Interval;
use datafusion_expr::sort_properties::ExprProperties;
#[expect(deprecated)]
use datafusion_expr::statistics::Distribution::{
    self, Bernoulli, Exponential, Gaussian, Generic, Uniform,
};
use datafusion_expr::{
    ColumnarValue,
    type_coercion::{is_interval, is_signed_numeric, is_timestamp},
};

/// Negative expression
#[derive(Debug, Eq)]
pub struct NegativeExpr {
    /// Input expression
    arg: Arc<dyn PhysicalExpr>,
}

// Manually derive PartialEq and Hash to work around https://github.com/rust-lang/rust/issues/78808
impl PartialEq for NegativeExpr {
    fn eq(&self, other: &Self) -> bool {
        self.arg.eq(&other.arg)
    }
}

impl Hash for NegativeExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.arg.hash(state);
    }
}

impl NegativeExpr {
    /// Create new not expression
    pub fn new(arg: Arc<dyn PhysicalExpr>) -> Self {
        Self { arg }
    }

    /// Get the input expression
    pub fn arg(&self) -> &Arc<dyn PhysicalExpr> {
        &self.arg
    }
}

impl std::fmt::Display for NegativeExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "(- {})", self.arg)
    }
}

impl PhysicalExpr for NegativeExpr {
    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        self.arg.data_type(input_schema)
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        self.arg.nullable(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        match self.arg.evaluate(batch)? {
            ColumnarValue::Array(array) => {
                let result = neg_wrapping(array.as_ref())?;
                Ok(ColumnarValue::Array(result))
            }
            ColumnarValue::Scalar(scalar) => {
                Ok(ColumnarValue::Scalar(scalar.arithmetic_negate()?))
            }
        }
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
        Ok(Arc::new(NegativeExpr::new(Arc::clone(&children[0]))))
    }

    /// Given the child interval of a NegativeExpr, it calculates the NegativeExpr's interval.
    /// It replaces the upper and lower bounds after multiplying them with -1.
    /// Ex: `(a, b]` => `[-b, -a)`
    fn evaluate_bounds(&self, children: &[&Interval]) -> Result<Interval> {
        children[0].arithmetic_negate()
    }

    /// Returns a new [`Interval`] of a NegativeExpr  that has the existing `interval` given that
    /// given the input interval is known to be `children`.
    fn propagate_constraints(
        &self,
        interval: &Interval,
        children: &[&Interval],
    ) -> Result<Option<Vec<Interval>>> {
        let negated_interval = interval.arithmetic_negate()?;

        Ok(children[0]
            .intersect(negated_interval)?
            .map(|result| vec![result]))
    }

    #[expect(deprecated)]
    fn evaluate_statistics(&self, children: &[&Distribution]) -> Result<Distribution> {
        match children[0] {
            Uniform(u) => Distribution::new_uniform(u.range().arithmetic_negate()?),
            Exponential(e) => Distribution::new_exponential(
                e.rate().clone(),
                e.offset().arithmetic_negate()?,
                !e.positive_tail(),
            ),
            Gaussian(g) => Distribution::new_gaussian(
                g.mean().arithmetic_negate()?,
                g.variance().clone(),
            ),
            Bernoulli(_) => {
                internal_err!("NegativeExpr cannot operate on Boolean datatypes")
            }
            Generic(u) => Distribution::new_generic(
                u.mean().arithmetic_negate()?,
                u.median().arithmetic_negate()?,
                u.variance().clone(),
                u.range().arithmetic_negate()?,
            ),
        }
    }

    /// The ordering of a [`NegativeExpr`] is simply the reverse of its child.
    fn get_properties(&self, children: &[ExprProperties]) -> Result<ExprProperties> {
        Ok(ExprProperties {
            sort_properties: -children[0].sort_properties,
            range: children[0].range.clone().arithmetic_negate()?,
            preserves_lex_ordering: false,
        })
    }

    fn fmt_sql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "(- ")?;
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
            expr_type: Some(protobuf::physical_expr_node::ExprType::Negative(Box::new(
                protobuf::PhysicalNegativeNode {
                    expr: Some(Box::new(ctx.encode_child(&self.arg)?)),
                },
            ))),
        }))
    }
}

#[cfg(feature = "proto")]
impl NegativeExpr {
    /// Reconstruct a [`NegativeExpr`] from its protobuf representation.
    pub fn try_from_proto(
        node: &datafusion_proto_models::protobuf::PhysicalExprNode,
        ctx: &datafusion_physical_expr_common::physical_expr::proto_decode::PhysicalExprDecodeCtx<'_>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        use datafusion_physical_expr_common::expect_expr_variant;
        use datafusion_proto_models::protobuf;

        let n = expect_expr_variant!(
            node,
            protobuf::physical_expr_node::ExprType::Negative,
            "Negative",
        );
        let expr =
            ctx.decode_required_expression(n.expr.as_deref(), "NegativeExpr", "expr")?;

        Ok(Arc::new(NegativeExpr::new(expr)))
    }
}

/// Creates a unary expression NEGATIVE
///
/// # Errors
///
/// This function errors when the argument's type is not signed numeric
pub fn negative(
    arg: Arc<dyn PhysicalExpr>,
    input_schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    let data_type = arg.data_type(input_schema)?;
    if data_type.is_null() {
        Ok(arg)
    } else if !is_signed_numeric(&data_type)
        && !is_interval(&data_type)
        && !is_timestamp(&data_type)
    {
        plan_err!("Negation only supports numeric, interval and timestamp types")
    } else {
        Ok(Arc::new(NegativeExpr::new(arg)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::{Column, col};

    use arrow::array::*;
    use arrow::datatypes::DataType::{Float32, Float64, Int8, Int16, Int32, Int64};
    use arrow::datatypes::*;
    use datafusion_common::cast::as_primitive_array;
    use datafusion_common::{DataFusionError, ScalarValue};

    use datafusion_physical_expr_common::physical_expr::fmt_sql;

    macro_rules! test_array_negative_op {
        ($DATA_TY:tt, $ARRAY_TY:ty, $($VALUE:expr),*   ) => {
            let schema = Schema::new(vec![Field::new("a", DataType::$DATA_TY, true)]);
            let expr = negative(col("a", &schema)?, &schema)?;
            assert_eq!(expr.data_type(&schema)?, DataType::$DATA_TY);
            assert!(expr.nullable(&schema)?);
            let mut arr = Vec::new();
            let mut arr_expected = Vec::new();
            $(
                arr.push(Some($VALUE));
                arr_expected.push(Some(-$VALUE));
            )+
            arr.push(None);
            arr_expected.push(None);
            let input = <$ARRAY_TY>::from(arr);
            let expected = &<$ARRAY_TY>::from(arr_expected);
            let batch =
                RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(input)])?;
            let result = expr.evaluate(&batch)?.into_array(batch.num_rows()).expect("Failed to convert to array");
            let result =
                as_primitive_array(&result).expect(format!("failed to downcast to {:?}Array", $DATA_TY).as_str());
            assert_eq!(result, expected);
        };
    }

    #[test]
    fn array_negative_op() -> Result<()> {
        test_array_negative_op!(Int8, Int8Array, 2i8, 1i8);
        test_array_negative_op!(Int16, Int16Array, 234i16, 123i16);
        test_array_negative_op!(Int32, Int32Array, 2345i32, 1234i32);
        test_array_negative_op!(Int64, Int64Array, 23456i64, 12345i64);
        test_array_negative_op!(Float32, Float32Array, 2345.0f32, 1234.0f32);
        test_array_negative_op!(Float64, Float64Array, 23456.0f64, 12345.0f64);
        Ok(())
    }

    #[test]
    fn test_evaluate_bounds() -> Result<()> {
        let negative_expr = NegativeExpr::new(Arc::new(Column::new("a", 0)));
        let child_interval = Interval::make(Some(-2), Some(1))?;
        let negative_expr_interval = Interval::make(Some(-1), Some(2))?;
        assert_eq!(
            negative_expr.evaluate_bounds(&[&child_interval])?,
            negative_expr_interval
        );
        Ok(())
    }

    #[test]
    #[expect(deprecated)]
    fn test_evaluate_statistics() -> Result<()> {
        let negative_expr = NegativeExpr::new(Arc::new(Column::new("a", 0)));

        // Uniform
        assert_eq!(
            negative_expr.evaluate_statistics(&[&Distribution::new_uniform(
                Interval::make(Some(-2.), Some(3.))?
            )?])?,
            Distribution::new_uniform(Interval::make(Some(-3.), Some(2.))?)?
        );

        // Bernoulli
        assert!(
            negative_expr
                .evaluate_statistics(&[&Distribution::new_bernoulli(ScalarValue::from(
                    0.75
                ))?])
                .is_err()
        );

        // Exponential
        assert_eq!(
            negative_expr.evaluate_statistics(&[&Distribution::new_exponential(
                ScalarValue::from(1.),
                ScalarValue::from(1.),
                true
            )?])?,
            Distribution::new_exponential(
                ScalarValue::from(1.),
                ScalarValue::from(-1.),
                false
            )?
        );

        // Gaussian
        assert_eq!(
            negative_expr.evaluate_statistics(&[&Distribution::new_gaussian(
                ScalarValue::from(15),
                ScalarValue::from(225),
            )?])?,
            Distribution::new_gaussian(ScalarValue::from(-15), ScalarValue::from(225),)?
        );

        // Unknown
        assert_eq!(
            negative_expr.evaluate_statistics(&[&Distribution::new_generic(
                ScalarValue::from(15),
                ScalarValue::from(15),
                ScalarValue::from(10),
                Interval::make(Some(10), Some(20))?
            )?])?,
            Distribution::new_generic(
                ScalarValue::from(-15),
                ScalarValue::from(-15),
                ScalarValue::from(10),
                Interval::make(Some(-20), Some(-10))?
            )?
        );

        Ok(())
    }

    #[test]
    fn test_propagate_constraints() -> Result<()> {
        let negative_expr = NegativeExpr::new(Arc::new(Column::new("a", 0)));
        let original_child_interval = Interval::make(Some(-2), Some(3))?;
        let negative_expr_interval = Interval::make(Some(0), Some(4))?;
        let after_propagation = Some(vec![Interval::make(Some(-2), Some(0))?]);
        assert_eq!(
            negative_expr.propagate_constraints(
                &negative_expr_interval,
                &[&original_child_interval]
            )?,
            after_propagation
        );
        Ok(())
    }

    #[test]
    #[expect(deprecated)]
    fn test_propagate_statistics_range_holders() -> Result<()> {
        let negative_expr = NegativeExpr::new(Arc::new(Column::new("a", 0)));
        let original_child_interval = Interval::make(Some(-2), Some(3))?;
        let after_propagation = Interval::make(Some(-2), Some(0))?;

        let parent = Distribution::new_uniform(Interval::make(Some(0), Some(4))?)?;
        let children: Vec<Vec<Distribution>> = vec![
            vec![Distribution::new_uniform(original_child_interval.clone())?],
            vec![Distribution::new_generic(
                ScalarValue::from(0),
                ScalarValue::from(0),
                ScalarValue::Int32(None),
                original_child_interval.clone(),
            )?],
        ];

        for child_view in children {
            let child_refs: Vec<_> = child_view.iter().collect();
            let actual = negative_expr.propagate_statistics(&parent, &child_refs)?;
            let expected = Some(vec![Distribution::new_from_interval(
                after_propagation.clone(),
            )?]);
            assert_eq!(actual, expected);
        }

        Ok(())
    }

    #[test]
    fn test_negation_valid_types() -> Result<()> {
        let negatable_types = [
            Int8,
            DataType::Timestamp(TimeUnit::Second, None),
            DataType::Interval(IntervalUnit::YearMonth),
        ];
        for negatable_type in negatable_types {
            let schema = Schema::new(vec![Field::new("a", negatable_type, true)]);
            let _expr = negative(col("a", &schema)?, &schema)?;
        }
        Ok(())
    }

    #[test]
    fn test_negation_invalid_types() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Utf8, true)]);
        let expr = negative(col("a", &schema)?, &schema).unwrap_err();
        matches!(expr, DataFusionError::Plan(_));
        Ok(())
    }

    #[test]
    fn test_fmt_sql() -> Result<()> {
        let expr = NegativeExpr::new(Arc::new(Column::new("a", 0)));
        let display_string = expr.to_string();
        assert_eq!(display_string, "(- a@0)");
        let sql_string = fmt_sql(&expr).to_string();
        assert_eq!(sql_string, "(- a)");

        Ok(())
    }
}

#[cfg(all(test, feature = "proto"))]
mod proto_tests {
    use super::*;
    use crate::expressions::{Column, col};
    use crate::proto_test_util::{
        StubDecoder, StubEncoder, UnreachableDecoder, column_node,
    };
    use arrow::datatypes::Field;
    use datafusion_common::DataFusionError;
    use datafusion_physical_expr_common::physical_expr::proto_decode::PhysicalExprDecodeCtx;
    use datafusion_physical_expr_common::physical_expr::proto_encode::PhysicalExprEncodeCtx;
    use datafusion_proto_models::protobuf::{
        PhysicalExprNode, PhysicalNegativeNode, physical_expr_node,
    };

    /// Build a `NegativeExpr` proto node with the given children.
    fn negative_node(expr: Option<Box<PhysicalExprNode>>) -> PhysicalExprNode {
        PhysicalExprNode {
            expr_id: None,
            expr_type: Some(physical_expr_node::ExprType::Negative(Box::new(
                PhysicalNegativeNode { expr },
            ))),
        }
    }

    /// A `NegativeExpr` over a column of type Int32.
    fn negative_fixture() -> NegativeExpr {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
        NegativeExpr::new(col("a", &schema).unwrap())
    }

    #[test]
    fn try_to_proto_encodes_negative_expr() {
        let negative = negative_fixture();
        let encoder = StubEncoder::ok();
        let ctx = PhysicalExprEncodeCtx::new(&encoder);

        let node = negative
            .try_to_proto(&ctx)
            .unwrap()
            .expect("NegativeExpr should encode to Some(node)");

        assert!(node.expr_id.is_none());
        let negative_node = match node.expr_type {
            Some(physical_expr_node::ExprType::Negative(boxed)) => *boxed,
            other => panic!("expected a NegativeExpr node, got {other:?}"),
        };
        assert!(negative_node.expr.is_some());
    }

    #[test]
    fn try_to_proto_propagates_expr_encode_error() {
        let negative = negative_fixture();
        let encoder = StubEncoder::failing_on(1);
        let ctx = PhysicalExprEncodeCtx::new(&encoder);
        let err = negative.try_to_proto(&ctx).unwrap_err();
        assert!(matches!(err, DataFusionError::Internal(msg) if msg.contains("call 1")));
    }

    #[test]
    fn try_from_proto_decodes_negative_expr() {
        let node = negative_node(Some(Box::new(column_node("a"))));
        let schema = Schema::empty();
        let decoder = StubDecoder::ok();
        let ctx = PhysicalExprDecodeCtx::new(&schema, &decoder);

        let decoded = NegativeExpr::try_from_proto(&node, &ctx).unwrap();
        let negative = decoded
            .downcast_ref::<NegativeExpr>()
            .expect("decoded expr should be a NegativeExpr");
        assert!(negative.arg().downcast_ref::<Column>().is_some());
    }

    #[test]
    fn try_from_proto_rejects_non_negative_node() {
        let node = column_node("a");
        let schema = Schema::empty();
        let decoder = UnreachableDecoder;
        let ctx = PhysicalExprDecodeCtx::new(&schema, &decoder);
        let err = NegativeExpr::try_from_proto(&node, &ctx).unwrap_err();
        assert!(
            matches!(err, DataFusionError::Internal(msg) if msg.contains("PhysicalExprNode is not a Negative"))
        );
    }

    #[test]
    fn try_from_proto_rejects_missing_expr() {
        let node = negative_node(None);
        let schema = Schema::empty();
        let decoder = UnreachableDecoder;
        let ctx = PhysicalExprDecodeCtx::new(&schema, &decoder);
        let err = NegativeExpr::try_from_proto(&node, &ctx).unwrap_err();
        assert!(
            matches!(err, DataFusionError::Internal(msg) if msg.contains("NegativeExpr is missing required field 'expr'"))
        );
    }

    #[test]
    fn try_from_proto_propagates_expr_decode_error() {
        let node = negative_node(Some(Box::new(column_node("a"))));
        let schema = Schema::empty();
        let decoder = StubDecoder::failing_on(1);
        let ctx = PhysicalExprDecodeCtx::new(&schema, &decoder);
        let err = NegativeExpr::try_from_proto(&node, &ctx).unwrap_err();
        assert!(matches!(err, DataFusionError::Internal(msg) if msg.contains("call 1")));
    }
}
