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

//! IS NULL expression

use crate::PhysicalExpr;
use arrow::{
    datatypes::{DataType, Schema},
    record_batch::RecordBatch,
};
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::ColumnarValue;
use std::hash::Hash;
use std::sync::Arc;

/// IS NULL expression
#[derive(Debug, Eq)]
pub struct IsNullExpr {
    /// Input expression
    arg: Arc<dyn PhysicalExpr>,
}

// Manually derive PartialEq and Hash to work around https://github.com/rust-lang/rust/issues/78808
impl PartialEq for IsNullExpr {
    fn eq(&self, other: &Self) -> bool {
        self.arg.eq(&other.arg)
    }
}

impl Hash for IsNullExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.arg.hash(state);
    }
}

impl IsNullExpr {
    /// Create new not expression
    pub fn new(arg: Arc<dyn PhysicalExpr>) -> Self {
        Self { arg }
    }

    /// Get the input expression
    pub fn arg(&self) -> &Arc<dyn PhysicalExpr> {
        &self.arg
    }
}

impl std::fmt::Display for IsNullExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{} IS NULL", self.arg)
    }
}

impl PhysicalExpr for IsNullExpr {
    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(false)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let arg = self.arg.evaluate(batch)?;
        match arg {
            ColumnarValue::Array(array) => Ok(ColumnarValue::Array(Arc::new(
                arrow::compute::is_null(&array)?,
            ))),
            ColumnarValue::Scalar(scalar) => Ok(ColumnarValue::Scalar(
                ScalarValue::Boolean(Some(scalar.is_null())),
            )),
        }
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.arg]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(IsNullExpr::new(Arc::clone(&children[0]))))
    }

    fn fmt_sql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.arg.fmt_sql(f)?;
        write!(f, " IS NULL")
    }

    #[cfg(feature = "proto")]
    fn try_to_proto(
        &self,
        ctx: &datafusion_physical_expr_common::physical_expr::proto_encode::PhysicalExprEncodeCtx<'_>,
    ) -> Result<Option<datafusion_proto_models::protobuf::PhysicalExprNode>> {
        use datafusion_proto_models::protobuf;

        Ok(Some(protobuf::PhysicalExprNode {
            expr_id: None,
            expr_type: Some(protobuf::physical_expr_node::ExprType::IsNullExpr(
                Box::new(protobuf::PhysicalIsNull {
                    expr: Some(Box::new(ctx.encode_child(&self.arg)?)),
                }),
            )),
        }))
    }
}

#[cfg(feature = "proto")]
impl IsNullExpr {
    /// Reconstruct an [`IsNullExpr`] from its protobuf representation.
    pub fn try_from_proto(
        node: &datafusion_proto_models::protobuf::PhysicalExprNode,
        ctx: &datafusion_physical_expr_common::physical_expr::proto_decode::PhysicalExprDecodeCtx<'_>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        use datafusion_physical_expr_common::expect_expr_variant;
        use datafusion_proto_models::protobuf;

        let node = expect_expr_variant!(
            node,
            protobuf::physical_expr_node::ExprType::IsNullExpr,
            "IsNullExpr",
        );
        let expr =
            ctx.decode_required_expression(node.expr.as_deref(), "IsNullExpr", "expr")?;

        Ok(Arc::new(IsNullExpr::new(expr)))
    }
}

/// Create an IS NULL expression
pub fn is_null(arg: Arc<dyn PhysicalExpr>) -> Result<Arc<dyn PhysicalExpr>> {
    Ok(Arc::new(IsNullExpr::new(arg)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::col;
    use arrow::array::{
        Array, BooleanArray, Float64Array, Int32Array, StringArray, UnionArray,
    };
    use arrow::buffer::ScalarBuffer;
    use arrow::datatypes::*;
    use datafusion_common::cast::as_boolean_array;
    use datafusion_physical_expr_common::physical_expr::fmt_sql;

    #[test]
    fn is_null_op() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Utf8, true)]);
        let a = StringArray::from(vec![Some("foo"), None]);

        // expression: "a is null"
        let expr = is_null(col("a", &schema)?).unwrap();
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)])?;

        let result = expr
            .evaluate(&batch)?
            .into_array(batch.num_rows())
            .expect("Failed to convert to array");
        let result =
            as_boolean_array(&result).expect("failed to downcast to BooleanArray");

        let expected = &BooleanArray::from(vec![false, true]);

        assert_eq!(expected, result);

        Ok(())
    }

    fn union_fields() -> UnionFields {
        [
            (0, Arc::new(Field::new("A", DataType::Int32, true))),
            (1, Arc::new(Field::new("B", DataType::Float64, true))),
            (2, Arc::new(Field::new("C", DataType::Utf8, true))),
        ]
        .into_iter()
        .collect()
    }

    #[test]
    fn sparse_union_is_null() {
        // union of [{A=1}, {A=}, {B=1.1}, {B=1.2}, {B=}, {C=}, {C="a"}]
        let int_array =
            Int32Array::from(vec![Some(1), None, None, None, None, None, None]);
        let float_array =
            Float64Array::from(vec![None, None, Some(1.1), Some(1.2), None, None, None]);
        let str_array =
            StringArray::from(vec![None, None, None, None, None, None, Some("a")]);
        let type_ids = [0, 0, 1, 1, 1, 2, 2]
            .into_iter()
            .collect::<ScalarBuffer<i8>>();

        let children = vec![
            Arc::new(int_array) as Arc<dyn Array>,
            Arc::new(float_array),
            Arc::new(str_array),
        ];

        let array =
            UnionArray::try_new(union_fields(), type_ids, None, children).unwrap();

        let result = arrow::compute::is_null(&array).unwrap();

        let expected =
            &BooleanArray::from(vec![false, true, false, false, true, true, false]);
        assert_eq!(expected, &result);
    }

    #[test]
    fn dense_union_is_null() {
        // union of [{A=1}, {A=}, {B=3.2}, {B=}, {C="a"}, {C=}]
        let int_array = Int32Array::from(vec![Some(1), None]);
        let float_array = Float64Array::from(vec![Some(3.2), None]);
        let str_array = StringArray::from(vec![Some("a"), None]);
        let type_ids = [0, 0, 1, 1, 2, 2].into_iter().collect::<ScalarBuffer<i8>>();
        let offsets = [0, 1, 0, 1, 0, 1]
            .into_iter()
            .collect::<ScalarBuffer<i32>>();

        let children = vec![
            Arc::new(int_array) as Arc<dyn Array>,
            Arc::new(float_array),
            Arc::new(str_array),
        ];

        let array =
            UnionArray::try_new(union_fields(), type_ids, Some(offsets), children)
                .unwrap();

        let result = arrow::compute::is_null(&array).unwrap();

        let expected = &BooleanArray::from(vec![false, true, false, true, false, true]);
        assert_eq!(expected, &result);
    }

    #[test]
    fn test_fmt_sql() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Utf8, true)]);

        // expression: "a is null"
        let expr = is_null(col("a", &schema)?).unwrap();
        let display_string = expr.to_string();
        assert_eq!(display_string, "a@0 IS NULL");
        let sql_string = fmt_sql(expr.as_ref()).to_string();
        assert_eq!(sql_string, "a IS NULL");

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
        PhysicalExprNode, PhysicalIsNull, physical_expr_node,
    };

    fn is_null_node(expr: Option<Box<PhysicalExprNode>>) -> PhysicalExprNode {
        PhysicalExprNode {
            expr_id: None,
            expr_type: Some(physical_expr_node::ExprType::IsNullExpr(Box::new(
                PhysicalIsNull { expr },
            ))),
        }
    }

    fn is_null_fixture() -> IsNullExpr {
        let schema = Schema::new(vec![Field::new("a", DataType::Int64, true)]);
        IsNullExpr::new(col("a", &schema).unwrap())
    }

    #[test]
    fn try_to_proto_encodes_is_null_expr() {
        let is_null = is_null_fixture();
        let encoder = StubEncoder::ok();
        let ctx = PhysicalExprEncodeCtx::new(&encoder);

        let node = is_null
            .try_to_proto(&ctx)
            .unwrap()
            .expect("IsNullExpr should encode to Some(node)");

        assert!(node.expr_id.is_none());
        let is_null_node = match node.expr_type {
            Some(physical_expr_node::ExprType::IsNullExpr(boxed)) => *boxed,
            other => panic!("expected an IsNullExpr node, got {other:?}"),
        };
        assert!(is_null_node.expr.is_some());
    }

    #[test]
    fn try_to_proto_propagates_expr_encode_error() {
        let is_null = is_null_fixture();
        let encoder = StubEncoder::failing_on(1);
        let ctx = PhysicalExprEncodeCtx::new(&encoder);
        let err = is_null.try_to_proto(&ctx).unwrap_err();
        assert!(matches!(err, DataFusionError::Internal(msg) if msg.contains("call 1")));
    }

    #[test]
    fn try_from_proto_decodes_is_null_expr() {
        let node = is_null_node(Some(Box::new(column_node("a"))));
        let schema = Schema::empty();
        let decoder = StubDecoder::ok();
        let ctx = PhysicalExprDecodeCtx::new(&schema, &decoder);

        let decoded = IsNullExpr::try_from_proto(&node, &ctx).unwrap();
        let is_null = decoded
            .downcast_ref::<IsNullExpr>()
            .expect("decoded expr should be an IsNullExpr");
        assert!(is_null.arg().downcast_ref::<Column>().is_some());
    }

    #[test]
    fn try_from_proto_rejects_non_is_null_node() {
        let node = column_node("a");
        let schema = Schema::empty();
        let decoder = UnreachableDecoder;
        let ctx = PhysicalExprDecodeCtx::new(&schema, &decoder);
        let err = IsNullExpr::try_from_proto(&node, &ctx).unwrap_err();
        assert!(
            matches!(err, DataFusionError::Internal(msg) if msg.contains("PhysicalExprNode is not a IsNullExpr"))
        );
    }

    #[test]
    fn try_from_proto_rejects_missing_expr() {
        let node = is_null_node(None);
        let schema = Schema::empty();
        let decoder = UnreachableDecoder;
        let ctx = PhysicalExprDecodeCtx::new(&schema, &decoder);
        let err = IsNullExpr::try_from_proto(&node, &ctx).unwrap_err();
        assert!(
            matches!(err, DataFusionError::Internal(msg) if msg.contains("IsNullExpr is missing required field 'expr'"))
        );
    }

    #[test]
    fn try_from_proto_propagates_expr_decode_error() {
        let node = is_null_node(Some(Box::new(column_node("a"))));
        let schema = Schema::empty();
        let decoder = StubDecoder::failing_on(1);
        let ctx = PhysicalExprDecodeCtx::new(&schema, &decoder);
        let err = IsNullExpr::try_from_proto(&node, &ctx).unwrap_err();
        assert!(matches!(err, DataFusionError::Internal(msg) if msg.contains("call 1")));
    }
}
