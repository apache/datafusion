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

//! IS NOT NULL expression

use crate::PhysicalExpr;
use arrow::{
    datatypes::{DataType, Schema},
    record_batch::RecordBatch,
};
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::ColumnarValue;
use std::hash::Hash;
use std::sync::Arc;

/// IS NOT NULL expression
#[derive(Debug, Eq)]
pub struct IsNotNullExpr {
    /// The input expression
    arg: Arc<dyn PhysicalExpr>,
}

// Manually derive PartialEq and Hash to work around https://github.com/rust-lang/rust/issues/78808
impl PartialEq for IsNotNullExpr {
    fn eq(&self, other: &Self) -> bool {
        self.arg.eq(&other.arg)
    }
}

impl Hash for IsNotNullExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.arg.hash(state);
    }
}

impl IsNotNullExpr {
    /// Create new not expression
    pub fn new(arg: Arc<dyn PhysicalExpr>) -> Self {
        Self { arg }
    }

    /// Get the input expression
    pub fn arg(&self) -> &Arc<dyn PhysicalExpr> {
        &self.arg
    }
}

impl std::fmt::Display for IsNotNullExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{} IS NOT NULL", self.arg)
    }
}

impl PhysicalExpr for IsNotNullExpr {
    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(false)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let arg = self.arg.evaluate(batch)?;
        match arg {
            ColumnarValue::Array(array) => {
                let is_not_null = arrow::compute::is_not_null(&array)?;
                Ok(ColumnarValue::Array(Arc::new(is_not_null)))
            }
            ColumnarValue::Scalar(scalar) => Ok(ColumnarValue::Scalar(
                ScalarValue::Boolean(Some(!scalar.is_null())),
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
        Ok(Arc::new(IsNotNullExpr::new(Arc::clone(&children[0]))))
    }

    fn fmt_sql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.arg.fmt_sql(f)?;
        write!(f, " IS NOT NULL")
    }

    #[cfg(feature = "proto")]
    fn try_to_proto(
        &self,
        ctx: &datafusion_physical_expr_common::physical_expr::proto_encode::PhysicalExprEncodeCtx<'_>,
    ) -> Result<Option<datafusion_proto_models::protobuf::PhysicalExprNode>> {
        use datafusion_proto_models::protobuf;

        Ok(Some(protobuf::PhysicalExprNode {
            expr_id: None,
            expr_type: Some(protobuf::physical_expr_node::ExprType::IsNotNullExpr(
                Box::new(protobuf::PhysicalIsNotNull {
                    expr: Some(Box::new(ctx.encode_child(&self.arg)?)),
                }),
            )),
        }))
    }
}

#[cfg(feature = "proto")]
impl IsNotNullExpr {
    /// Reconstruct an [`IsNotNullExpr`] from its protobuf representation.
    pub fn try_from_proto(
        node: &datafusion_proto_models::protobuf::PhysicalExprNode,
        ctx: &datafusion_physical_expr_common::physical_expr::proto_decode::PhysicalExprDecodeCtx<'_>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        use datafusion_physical_expr_common::expect_expr_variant;
        use datafusion_proto_models::protobuf;

        let node = expect_expr_variant!(
            node,
            protobuf::physical_expr_node::ExprType::IsNotNullExpr,
            "IsNotNullExpr",
        );
        let expr = ctx.decode_required_expression(
            node.expr.as_deref(),
            "IsNotNullExpr",
            "expr",
        )?;

        Ok(Arc::new(IsNotNullExpr::new(expr)))
    }
}

/// Create an IS NOT NULL expression
pub fn is_not_null(arg: Arc<dyn PhysicalExpr>) -> Result<Arc<dyn PhysicalExpr>> {
    Ok(Arc::new(IsNotNullExpr::new(arg)))
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
    fn is_not_null_op() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Utf8, true)]);
        let a = StringArray::from(vec![Some("foo"), None]);
        let expr = is_not_null(col("a", &schema)?).unwrap();
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)])?;

        // expression: "a is not null"
        let result = expr
            .evaluate(&batch)?
            .into_array(batch.num_rows())
            .expect("Failed to convert to array");
        let result =
            as_boolean_array(&result).expect("failed to downcast to BooleanArray");

        let expected = &BooleanArray::from(vec![true, false]);

        assert_eq!(expected, result);

        Ok(())
    }

    #[test]
    fn union_is_not_null_op() {
        // union of [{A=1}, {A=}, {B=1.1}, {B=1.2}, {B=}]
        let int_array = Int32Array::from(vec![Some(1), None, None, None, None]);
        let float_array =
            Float64Array::from(vec![None, None, Some(1.1), Some(1.2), None]);
        let type_ids = [0, 0, 1, 1, 1].into_iter().collect::<ScalarBuffer<i8>>();

        let children = vec![Arc::new(int_array) as Arc<dyn Array>, Arc::new(float_array)];

        let union_fields: UnionFields = [
            (0, Arc::new(Field::new("A", DataType::Int32, true))),
            (1, Arc::new(Field::new("B", DataType::Float64, true))),
        ]
        .into_iter()
        .collect();

        let array =
            UnionArray::try_new(union_fields.clone(), type_ids, None, children).unwrap();

        let field = Field::new(
            "my_union",
            DataType::Union(union_fields, UnionMode::Sparse),
            true,
        );

        let schema = Schema::new(vec![field]);
        let expr = is_not_null(col("my_union", &schema).unwrap()).unwrap();
        let batch =
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array)]).unwrap();

        // expression: "a is not null"
        let actual = expr
            .evaluate(&batch)
            .unwrap()
            .into_array(batch.num_rows())
            .expect("Failed to convert to array");
        let actual = as_boolean_array(&actual).unwrap();

        let expected = &BooleanArray::from(vec![true, false, true, true, false]);

        assert_eq!(expected, actual);
    }

    #[test]
    fn test_fmt_sql() -> Result<()> {
        let union_fields: UnionFields = [
            (0, Arc::new(Field::new("A", DataType::Int32, true))),
            (1, Arc::new(Field::new("B", DataType::Float64, true))),
        ]
        .into_iter()
        .collect();

        let field = Field::new(
            "my_union",
            DataType::Union(union_fields, UnionMode::Sparse),
            true,
        );

        let schema = Schema::new(vec![field]);
        let expr = is_not_null(col("my_union", &schema).unwrap()).unwrap();
        let display_string = expr.to_string();
        assert_eq!(display_string, "my_union@0 IS NOT NULL");
        let sql_string = fmt_sql(expr.as_ref()).to_string();
        assert_eq!(sql_string, "my_union IS NOT NULL");

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
        PhysicalExprNode, PhysicalIsNotNull, physical_expr_node,
    };

    fn is_not_null_node(expr: Option<Box<PhysicalExprNode>>) -> PhysicalExprNode {
        PhysicalExprNode {
            expr_id: None,
            expr_type: Some(physical_expr_node::ExprType::IsNotNullExpr(Box::new(
                PhysicalIsNotNull { expr },
            ))),
        }
    }

    fn is_not_null_fixture() -> IsNotNullExpr {
        let schema = Schema::new(vec![Field::new("a", DataType::Int64, true)]);
        IsNotNullExpr::new(col("a", &schema).unwrap())
    }

    #[test]
    fn try_to_proto_encodes_is_not_null_expr() {
        let is_not_null = is_not_null_fixture();
        let encoder = StubEncoder::ok();
        let ctx = PhysicalExprEncodeCtx::new(&encoder);

        let node = is_not_null
            .try_to_proto(&ctx)
            .unwrap()
            .expect("IsNotNullExpr should encode to Some(node)");

        assert!(node.expr_id.is_none());
        let is_not_null_node = match node.expr_type {
            Some(physical_expr_node::ExprType::IsNotNullExpr(boxed)) => *boxed,
            other => panic!("expected an IsNotNullExpr node, got {other:?}"),
        };
        assert!(is_not_null_node.expr.is_some());
    }

    #[test]
    fn try_to_proto_propagates_expr_encode_error() {
        let is_not_null = is_not_null_fixture();
        let encoder = StubEncoder::failing_on(1);
        let ctx = PhysicalExprEncodeCtx::new(&encoder);
        let err = is_not_null.try_to_proto(&ctx).unwrap_err();
        assert!(matches!(err, DataFusionError::Internal(msg) if msg.contains("call 1")));
    }

    #[test]
    fn try_from_proto_decodes_is_not_null_expr() {
        let node = is_not_null_node(Some(Box::new(column_node("a"))));
        let schema = Schema::empty();
        let decoder = StubDecoder::ok();
        let ctx = PhysicalExprDecodeCtx::new(&schema, &decoder);

        let decoded = IsNotNullExpr::try_from_proto(&node, &ctx).unwrap();
        let is_not_null = decoded
            .downcast_ref::<IsNotNullExpr>()
            .expect("decoded expr should be an IsNotNullExpr");
        assert!(is_not_null.arg().downcast_ref::<Column>().is_some());
    }

    #[test]
    fn try_from_proto_rejects_non_is_not_null_node() {
        let node = column_node("a");
        let schema = Schema::empty();
        let decoder = UnreachableDecoder;
        let ctx = PhysicalExprDecodeCtx::new(&schema, &decoder);
        let err = IsNotNullExpr::try_from_proto(&node, &ctx).unwrap_err();
        assert!(
            matches!(err, DataFusionError::Internal(msg) if msg.contains("PhysicalExprNode is not a IsNotNullExpr"))
        );
    }

    #[test]
    fn try_from_proto_rejects_missing_expr() {
        let node = is_not_null_node(None);
        let schema = Schema::empty();
        let decoder = UnreachableDecoder;
        let ctx = PhysicalExprDecodeCtx::new(&schema, &decoder);
        let err = IsNotNullExpr::try_from_proto(&node, &ctx).unwrap_err();
        assert!(
            matches!(err, DataFusionError::Internal(msg) if msg.contains("IsNotNullExpr is missing required field 'expr'"))
        );
    }

    #[test]
    fn try_from_proto_propagates_expr_decode_error() {
        let node = is_not_null_node(Some(Box::new(column_node("a"))));
        let schema = Schema::empty();
        let decoder = StubDecoder::failing_on(1);
        let ctx = PhysicalExprDecodeCtx::new(&schema, &decoder);
        let err = IsNotNullExpr::try_from_proto(&node, &ctx).unwrap_err();
        assert!(matches!(err, DataFusionError::Internal(msg) if msg.contains("call 1")));
    }
}
