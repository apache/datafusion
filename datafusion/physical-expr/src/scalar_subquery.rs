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

//! Physical expression for uncorrelated scalar subqueries.

use std::fmt;
use std::hash::Hash;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, FieldRef, Schema};
use arrow::record_batch::RecordBatch;
use datafusion_common::{Result, internal_datafusion_err};
use datafusion_expr::physical_planning_context::{ScalarSubqueryResults, SubqueryIndex};
use datafusion_expr_common::columnar_value::ColumnarValue;
use datafusion_expr_common::sort_properties::{ExprProperties, SortProperties};
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;

/// A physical expression whose value is provided by a scalar subquery.
///
/// Subquery execution is handled by `ScalarSubqueryExec`, which stores the
/// result in a shared [`ScalarSubqueryResults`] container. This expression
/// simply reads from that container at the appropriate index.
#[derive(Debug)]
pub struct ScalarSubqueryExpr {
    data_type: DataType,
    nullable: bool,
    /// Index of this subquery in the shared results container.
    index: SubqueryIndex,
    /// Shared results container populated by `ScalarSubqueryExec`.
    results: ScalarSubqueryResults,
}

impl ScalarSubqueryExpr {
    pub fn new(
        data_type: DataType,
        nullable: bool,
        index: SubqueryIndex,
        results: ScalarSubqueryResults,
    ) -> Self {
        Self {
            data_type,
            nullable,
            index,
            results,
        }
    }

    pub fn results(&self) -> &ScalarSubqueryResults {
        &self.results
    }

    #[deprecated(
        since = "55.0.0",
        note = "was only used for proto serialization, which no longer needs it; use `return_field` for type/nullability. It will be removed in 61.0.0 or 6 months after 55.0.0 is released, whichever is longer."
    )]
    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }

    #[deprecated(
        since = "55.0.0",
        note = "was only used for proto serialization, which no longer needs it; use `return_field` for type/nullability. It will be removed in 61.0.0 or 6 months after 55.0.0 is released, whichever is longer."
    )]
    pub fn nullable(&self) -> bool {
        self.nullable
    }

    /// Returns the index of this subquery in the shared results container.
    #[deprecated(
        since = "55.0.0",
        note = "was only used for proto serialization, which no longer needs it. It will be removed in 61.0.0 or 6 months after 55.0.0 is released, whichever is longer."
    )]
    pub fn index(&self) -> SubqueryIndex {
        self.index
    }
}

impl fmt::Display for ScalarSubqueryExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.results.get(self.index) {
            Some(v) => write!(f, "scalar_subquery({v})"),
            None => write!(f, "scalar_subquery(<pending>)"),
        }
    }
}

// Two ScalarSubqueryExprs are considered the "same" if they refer to the
// same underlying shared results container and the same index within it.
impl Hash for ScalarSubqueryExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.results.hash(state);
        self.index.hash(state);
    }
}

impl PartialEq for ScalarSubqueryExpr {
    fn eq(&self, other: &Self) -> bool {
        self.results == other.results && self.index == other.index
    }
}

impl Eq for ScalarSubqueryExpr {}

impl PhysicalExpr for ScalarSubqueryExpr {
    fn return_field(&self, _input_schema: &Schema) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(
            "scalar_subquery",
            self.data_type.clone(),
            self.nullable,
        )))
    }

    fn evaluate(&self, _batch: &RecordBatch) -> Result<ColumnarValue> {
        let value = self.results.get(self.index).ok_or_else(|| {
            internal_datafusion_err!(
                "ScalarSubqueryExpr evaluated before the subquery was executed"
            )
        })?;
        Ok(ColumnarValue::Scalar(value))
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

    fn get_properties(&self, _children: &[ExprProperties]) -> Result<ExprProperties> {
        Ok(ExprProperties::new_unknown().with_order(SortProperties::Singleton))
    }

    fn fmt_sql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "(scalar subquery)")
    }

    #[cfg(feature = "proto")]
    fn try_to_proto(
        &self,
        _ctx: &datafusion_physical_expr_common::physical_expr::proto_encode::PhysicalExprEncodeCtx<'_>,
    ) -> Result<Option<datafusion_proto_models::protobuf::PhysicalExprNode>> {
        use datafusion_proto_models::protobuf;
        Ok(Some(protobuf::PhysicalExprNode {
            expr_id: None,
            expr_type: Some(protobuf::physical_expr_node::ExprType::ScalarSubquery(
                protobuf::PhysicalScalarSubqueryExprNode {
                    data_type: Some((&self.data_type).try_into()?),
                    nullable: self.nullable,
                    index: u32::try_from(self.index.as_usize()).map_err(|_| {
                        internal_datafusion_err!(
                            "scalar subquery index {} does not fit in u32",
                            self.index.as_usize()
                        )
                    })?,
                },
            )),
        }))
    }
}

#[cfg(feature = "proto")]
impl ScalarSubqueryExpr {
    /// Reconstruct a [`ScalarSubqueryExpr`] from its protobuf representation.
    ///
    /// Unlike other expressions, this takes a third argument: the shared
    /// [`ScalarSubqueryResults`] container. That container is a runtime-only
    /// `Arc` shared with the surrounding `ScalarSubqueryExec` and is not part of
    /// the wire format, so it cannot be reconstructed here or carried on the
    /// decode context (which lives in a crate that cannot depend on
    /// `datafusion-expr`). The match arm in `from_proto.rs` fetches it from the
    /// plan-level decode context and passes it in.
    pub fn try_from_proto(
        node: &datafusion_proto_models::protobuf::PhysicalExprNode,
        _ctx: &datafusion_physical_expr_common::physical_expr::proto_decode::PhysicalExprDecodeCtx<'_>,
        results: &ScalarSubqueryResults,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        use datafusion_physical_expr_common::expect_expr_variant;
        use datafusion_physical_expr_common::physical_expr::proto_decode::require_proto_field;
        use datafusion_proto_models::protobuf;

        let sq = expect_expr_variant!(
            node,
            protobuf::physical_expr_node::ExprType::ScalarSubquery,
            "ScalarSubqueryExpr",
        );
        let data_type = require_proto_field(
            sq.data_type.as_ref(),
            "ScalarSubqueryExpr",
            "data_type",
        )?
        .try_into()?;
        Ok(Arc::new(ScalarSubqueryExpr::new(
            data_type,
            sq.nullable,
            SubqueryIndex::new(sq.index as usize),
            results.clone(),
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::array::Int32Array;
    use arrow::datatypes::Field;
    use datafusion_common::ScalarValue;

    fn make_results(values: Vec<Option<ScalarValue>>) -> ScalarSubqueryResults {
        let results = ScalarSubqueryResults::new(values.len());
        for (index, value) in values.into_iter().enumerate() {
            if let Some(value) = value {
                results.set(SubqueryIndex::new(index), value).unwrap();
            }
        }
        results
    }

    #[test]
    fn test_evaluate_with_value() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
        let a = Int32Array::from(vec![1, 2, 3]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)])?;

        let results = make_results(vec![Some(ScalarValue::Int32(Some(42)))]);
        let expr = ScalarSubqueryExpr::new(
            DataType::Int32,
            false,
            SubqueryIndex::new(0),
            results,
        );

        let result = expr.evaluate(&batch)?;
        match result {
            ColumnarValue::Scalar(ScalarValue::Int32(Some(42))) => {}
            other => panic!("Expected Scalar(Int32(42)), got {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn test_evaluate_before_populated() {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
        let a = Int32Array::from(vec![1]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)]).unwrap();

        let results = ScalarSubqueryResults::new(1);
        let expr = ScalarSubqueryExpr::new(
            DataType::Int32,
            false,
            SubqueryIndex::new(0),
            results,
        );

        let result = expr.evaluate(&batch);
        assert!(result.is_err());
    }

    #[test]
    fn test_identity_equality() {
        let results = make_results(vec![None, None]);

        let e1a = ScalarSubqueryExpr::new(
            DataType::Int32,
            false,
            SubqueryIndex::new(0),
            results.clone(),
        );
        let e1b = ScalarSubqueryExpr::new(
            DataType::Int32,
            false,
            SubqueryIndex::new(0),
            results.clone(),
        );
        let e2 = ScalarSubqueryExpr::new(
            DataType::Int32,
            false,
            SubqueryIndex::new(1),
            results.clone(),
        );

        // Same container + same index → equal
        assert_eq!(e1a, e1b);
        // Same container, different index → not equal
        assert_ne!(e1a, e2);

        // Different container, same index → not equal
        let other_results = make_results(vec![None]);
        let e3 = ScalarSubqueryExpr::new(
            DataType::Int32,
            false,
            SubqueryIndex::new(0),
            other_results,
        );
        assert_ne!(e1a, e3);
    }
}

/// Tests for the `try_to_proto` / `try_from_proto` hooks.
#[cfg(all(test, feature = "proto"))]
mod proto_tests {
    use super::*;
    use crate::proto_test_util::{StubEncoder, UnreachableDecoder, column_node};
    use datafusion_common::DataFusionError;
    use datafusion_physical_expr_common::physical_expr::proto_decode::PhysicalExprDecodeCtx;
    use datafusion_physical_expr_common::physical_expr::proto_encode::PhysicalExprEncodeCtx;
    use datafusion_proto_models::protobuf::{
        PhysicalExprNode, PhysicalScalarSubqueryExprNode, physical_expr_node,
    };

    /// Build a `ScalarSubquery` proto node directly, with control over each
    /// field, so the decode error paths can be exercised independently.
    fn proto_scalar_subquery_node(
        data_type: Option<datafusion_proto_models::datafusion_common::ArrowType>,
        nullable: bool,
        index: u32,
    ) -> PhysicalExprNode {
        PhysicalExprNode {
            expr_id: None,
            expr_type: Some(physical_expr_node::ExprType::ScalarSubquery(
                PhysicalScalarSubqueryExprNode {
                    data_type,
                    nullable,
                    index,
                },
            )),
        }
    }

    #[test]
    fn round_trips_through_proto() {
        // A three-slot results container so index 2 is meaningful.
        let results = ScalarSubqueryResults::new(3);
        let expr = ScalarSubqueryExpr::new(
            DataType::Int32,
            true,
            SubqueryIndex::new(2),
            results.clone(),
        );

        // Encode: the expression serializes itself via try_to_proto.
        let encoder = StubEncoder::ok();
        let enc_ctx = PhysicalExprEncodeCtx::new(&encoder);
        let node = expr
            .try_to_proto(&enc_ctx)
            .unwrap()
            .expect("ScalarSubqueryExpr should encode to Some(node)");

        assert!(node.expr_id.is_none());
        let sq = match &node.expr_type {
            Some(physical_expr_node::ExprType::ScalarSubquery(sq)) => sq,
            other => panic!("expected a ScalarSubquery node, got {other:?}"),
        };
        assert!(sq.nullable);
        assert_eq!(sq.index, 2);
        let encoded_type: DataType = sq
            .data_type
            .as_ref()
            .expect("data_type encoded")
            .try_into()
            .unwrap();
        assert_eq!(encoded_type, DataType::Int32);

        // Decode: reconstruct from the proto node, threading in the shared
        // results container the surrounding exec would provide.
        let decoder = UnreachableDecoder;
        let schema = Schema::empty();
        let dec_ctx = PhysicalExprDecodeCtx::new(&schema, &decoder);
        let decoded =
            ScalarSubqueryExpr::try_from_proto(&node, &dec_ctx, &results).unwrap();
        let decoded = decoded
            .downcast_ref::<ScalarSubqueryExpr>()
            .expect("decoded expr should be a ScalarSubqueryExpr");

        // data_type + nullable survive the round-trip (observed via return_field).
        let field = decoded.return_field(&Schema::empty()).unwrap();
        assert_eq!(field.data_type(), &DataType::Int32);
        assert!(field.is_nullable());

        // Same shared container + same index → equal to the original.
        assert_eq!(decoded, &expr);
    }

    #[test]
    fn rejects_non_scalar_subquery_node() {
        let node = column_node("a");
        let results = ScalarSubqueryResults::new(1);
        let decoder = UnreachableDecoder;
        let schema = Schema::empty();
        let dec_ctx = PhysicalExprDecodeCtx::new(&schema, &decoder);

        let err =
            ScalarSubqueryExpr::try_from_proto(&node, &dec_ctx, &results).unwrap_err();
        assert!(matches!(
            err,
            DataFusionError::Internal(msg)
                if msg.contains("PhysicalExprNode is not a ScalarSubqueryExpr")
        ));
    }

    #[test]
    fn rejects_missing_data_type() {
        let node = proto_scalar_subquery_node(None, false, 0);
        let results = ScalarSubqueryResults::new(1);
        let decoder = UnreachableDecoder;
        let schema = Schema::empty();
        let dec_ctx = PhysicalExprDecodeCtx::new(&schema, &decoder);

        let err =
            ScalarSubqueryExpr::try_from_proto(&node, &dec_ctx, &results).unwrap_err();
        assert!(matches!(
            err,
            DataFusionError::Internal(msg)
                if msg.contains("ScalarSubqueryExpr is missing required field 'data_type'")
        ));
    }
}
