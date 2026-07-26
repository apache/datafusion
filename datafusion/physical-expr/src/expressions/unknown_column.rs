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

//! UnKnownColumn expression

use std::hash::{Hash, Hasher};
use std::sync::Arc;

use crate::PhysicalExpr;

use arrow::{
    datatypes::{DataType, Schema},
    record_batch::RecordBatch,
};
use datafusion_common::{Result, internal_err};

use datafusion_expr::ColumnarValue;

#[derive(Debug, Clone, Eq)]
pub struct UnKnownColumn {
    name: String,
}

impl UnKnownColumn {
    /// Create a new unknown column expression
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_owned(),
        }
    }

    /// Get the column name
    pub fn name(&self) -> &str {
        &self.name
    }
}

impl std::fmt::Display for UnKnownColumn {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.name)
    }
}

impl PhysicalExpr for UnKnownColumn {
    /// Get the data type of this expression, given the schema of the input
    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Null)
    }

    /// Decide whether this expression is nullable, given the schema of the input
    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(true)
    }

    /// Evaluate the expression
    fn evaluate(&self, _batch: &RecordBatch) -> Result<ColumnarValue> {
        internal_err!("UnKnownColumn::evaluate() should not be called")
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(self)
    }

    fn fmt_sql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }

    #[cfg(feature = "proto")]
    fn try_to_proto(
        &self,
        _ctx: &datafusion_physical_expr_common::physical_expr::proto_encode::PhysicalExprEncodeCtx<'_>,
    ) -> Result<Option<datafusion_proto_models::protobuf::PhysicalExprNode>> {
        use datafusion_proto_models::protobuf;

        Ok(Some(protobuf::PhysicalExprNode {
            expr_id: None,
            expr_type: Some(protobuf::physical_expr_node::ExprType::UnknownColumn(
                protobuf::UnknownColumn {
                    name: self.name.clone(),
                },
            )),
        }))
    }
}

#[cfg(feature = "proto")]
impl UnKnownColumn {
    /// Reconstruct an [`UnKnownColumn`] from its protobuf representation.
    pub fn try_from_proto(
        node: &datafusion_proto_models::protobuf::PhysicalExprNode,
        _ctx: &datafusion_physical_expr_common::physical_expr::proto_decode::PhysicalExprDecodeCtx<'_>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        use datafusion_physical_expr_common::expect_expr_variant;
        use datafusion_proto_models::protobuf;

        let unknown_col = expect_expr_variant!(
            node,
            protobuf::physical_expr_node::ExprType::UnknownColumn,
            "UnKnownColumn",
        );
        Ok(Arc::new(UnKnownColumn::new(&unknown_col.name)))
    }
}

impl Hash for UnKnownColumn {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

impl PartialEq for UnKnownColumn {
    fn eq(&self, _other: &Self) -> bool {
        // UnknownColumn is not a valid expression, so it should not be equal to any other expression.
        // See https://github.com/apache/datafusion/pull/11536
        false
    }
}

/// Tests for the `try_to_proto` / `try_from_proto` hooks.
#[cfg(all(test, feature = "proto"))]
mod proto_tests {
    use super::*;
    use crate::proto_test_util::{StubEncoder, UnreachableDecoder, column_node};
    use arrow::datatypes::Schema;
    use datafusion_common::DataFusionError;
    use datafusion_physical_expr_common::physical_expr::proto_decode::PhysicalExprDecodeCtx;
    use datafusion_physical_expr_common::physical_expr::proto_encode::PhysicalExprEncodeCtx;
    use datafusion_proto_models::protobuf::{self, physical_expr_node};

    // ── try_to_proto ─────────────────────────────────────────────────────────

    #[test]
    fn try_to_proto_encodes_unknown_column() {
        let expr = UnKnownColumn::new("my_col");
        let encoder = StubEncoder::ok();
        let ctx = PhysicalExprEncodeCtx::new(&encoder);

        let node = expr
            .try_to_proto(&ctx)
            .unwrap()
            .expect("UnKnownColumn should encode to Some(node)");

        // Built-in exprs never set expr_id; only dynamic filters do.
        assert!(node.expr_id.is_none());

        // Verify the encoded name matches the original.
        let protobuf::UnknownColumn { name } = match node.expr_type {
            Some(physical_expr_node::ExprType::UnknownColumn(c)) => c,
            other => panic!("expected UnknownColumn proto node, got {other:?}"),
        };
        assert_eq!(name, "my_col");
    }

    // ── try_from_proto ───────────────────────────────────────────────────────

    #[test]
    fn try_from_proto_decodes_name() {
        let node = protobuf::PhysicalExprNode {
            expr_id: None,
            expr_type: Some(physical_expr_node::ExprType::UnknownColumn(
                protobuf::UnknownColumn {
                    name: "my_col".to_string(),
                },
            )),
        };
        let schema = Schema::empty();
        // UnKnownColumn has no child exprs so the decoder is never called.
        let decoder = UnreachableDecoder;
        let ctx = PhysicalExprDecodeCtx::new(&schema, &decoder);

        let decoded = UnKnownColumn::try_from_proto(&node, &ctx).unwrap();
        let col = decoded
            .downcast_ref::<UnKnownColumn>()
            .expect("decoded expr should be an UnKnownColumn");
        assert_eq!(col.name(), "my_col");
    }

    #[test]
    fn try_from_proto_rejects_non_unknown_column_node() {
        // column_node produces an ExprType::Column node, not UnknownColumn.
        let node = column_node("a");
        let schema = Schema::empty();
        let decoder = UnreachableDecoder;
        let ctx = PhysicalExprDecodeCtx::new(&schema, &decoder);
        let err = UnKnownColumn::try_from_proto(&node, &ctx).unwrap_err();
        assert!(matches!(
            err,
            DataFusionError::Internal(ref msg)
                if msg.contains("PhysicalExprNode is not a UnKnownColumn")
        ));
    }

    // ── roundtrip ────────────────────────────────────────────────────────────

    #[test]
    fn unknown_column_proto_roundtrip() {
        let expr = UnKnownColumn::new("col_b");
        let encoder = StubEncoder::ok();
        let enc_ctx = PhysicalExprEncodeCtx::new(&encoder);

        let node = expr
            .try_to_proto(&enc_ctx)
            .unwrap()
            .expect("UnKnownColumn should encode to Some(node)");

        let schema = Schema::empty();
        // UnKnownColumn has no child exprs so the decoder is never called.
        let decoder = UnreachableDecoder;
        let dec_ctx = PhysicalExprDecodeCtx::new(&schema, &decoder);

        let decoded = UnKnownColumn::try_from_proto(&node, &dec_ctx).unwrap();
        let col = decoded
            .downcast_ref::<UnKnownColumn>()
            .expect("decoded expr should be an UnKnownColumn");
        assert_eq!(col.name(), "col_b");
    }
}
