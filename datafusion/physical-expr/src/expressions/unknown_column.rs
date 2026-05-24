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
        use datafusion_proto_models::protobuf;

        let protobuf::UnknownColumn { name } = match &node.expr_type {
            Some(protobuf::physical_expr_node::ExprType::UnknownColumn(c)) => c,
            other => {
                return internal_err!(
                    "PhysicalExprNode is not an UnKnownColumn (expr_id={:?}, expr_type={other:?})",
                    node.expr_id
                );
            }
        };
        Ok(Arc::new(UnKnownColumn::new(name)))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "proto")]
    use std::sync::Arc;

    #[cfg(feature = "proto")]
    use arrow::datatypes::Schema;

    #[cfg(feature = "proto")]
    use datafusion_common::{Result, internal_err};

    #[cfg(feature = "proto")]
    use datafusion_physical_expr_common::physical_expr::proto_decode::PhysicalExprDecodeCtx;

    #[cfg(feature = "proto")]
    struct DummyDecode;

    #[cfg(feature = "proto")]
    impl datafusion_physical_expr_common::physical_expr::proto_decode::PhysicalExprDecode
        for DummyDecode
    {
        fn decode(
            &self,
            _node: &datafusion_proto_models::protobuf::PhysicalExprNode,
            _schema: &Schema,
        ) -> Result<Arc<dyn PhysicalExpr>> {
            internal_err!("decode should not be called")
        }
    }

    #[cfg(feature = "proto")]
    #[test]
    fn try_from_proto_reports_found_variant() {
        use datafusion_proto_models::protobuf;

        let node = protobuf::PhysicalExprNode {
            expr_id: Some(7),
            expr_type: Some(protobuf::physical_expr_node::ExprType::Column(
                protobuf::PhysicalColumn {
                    name: "col_a".to_string(),
                    index: 0,
                },
            )),
        };
        let schema = Schema::empty();
        let decoder = DummyDecode;
        let ctx = PhysicalExprDecodeCtx::new(&schema, &decoder);

        let err = UnKnownColumn::try_from_proto(&node, &ctx).unwrap_err();
        let err = err.to_string();

        assert!(err.contains("expr_id=Some(7)"));
        assert!(err.contains("PhysicalColumn"));
    }

    #[cfg(feature = "proto")]
    #[test]
    fn unknown_column_proto_roundtrip() {
        use datafusion_proto_models::protobuf;

        let name = "col_b".to_string();
        let node = protobuf::PhysicalExprNode {
            expr_id: Some(42),
            expr_type: Some(protobuf::physical_expr_node::ExprType::UnknownColumn(
                protobuf::UnknownColumn { name: name.clone() },
            )),
        };

        let schema = Schema::empty();
        let decoder = DummyDecode;
        let ctx = PhysicalExprDecodeCtx::new(&schema, &decoder);

        let decoded = UnKnownColumn::try_from_proto(&node, &ctx).unwrap();
        let col = decoded.as_ref().downcast_ref::<UnKnownColumn>().unwrap();
        assert_eq!(col.name(), name.as_str());
    }
}
