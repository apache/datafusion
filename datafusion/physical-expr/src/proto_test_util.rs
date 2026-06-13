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

//! Shared test helpers for proto serialization / deserialization in expression unit tests
//! without depending on `datafusion-proto` (which would create circular deps).

use std::cell::Cell;
use std::sync::Arc;

use arrow::datatypes::Schema;
use datafusion_common::{DataFusionError, Result};
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use datafusion_physical_expr_common::physical_expr::proto_decode::PhysicalExprDecode;
use datafusion_physical_expr_common::physical_expr::proto_encode::PhysicalExprEncode;
use datafusion_proto_models::protobuf::{self, PhysicalExprNode, physical_expr_node};

use crate::expressions::Column;

/// A proto node for a `Column`, useful as a stand-in child node when building
/// an expression's proto representation in tests.
pub(crate) fn column_node(name: &str) -> PhysicalExprNode {
    PhysicalExprNode {
        expr_id: None,
        expr_type: Some(physical_expr_node::ExprType::Column(
            protobuf::PhysicalColumn {
                name: name.to_string(),
                index: 0,
            },
        )),
    }
}

/// Decoder stub for driving `try_from_proto`: returns a fixed `Column` for each
/// child node, optionally failing on the Nth `decode` call so the
/// `ctx.decode(..)?` error arms can be exercised.
pub(crate) struct StubDecoder {
    fail_on_call: Option<usize>,
    calls: Cell<usize>,
}

impl StubDecoder {
    /// Always succeeds, returning a placeholder `Column` per child.
    pub(crate) fn ok() -> Self {
        Self {
            fail_on_call: None,
            calls: Cell::new(0),
        }
    }

    /// Fails on the `call`-th invocation (1-based), succeeding otherwise.
    pub(crate) fn failing_on(call: usize) -> Self {
        Self {
            fail_on_call: Some(call),
            calls: Cell::new(0),
        }
    }
}

impl PhysicalExprDecode for StubDecoder {
    fn decode(
        &self,
        _node: &PhysicalExprNode,
        _schema: &Schema,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let call = self.calls.get() + 1;
        self.calls.set(call);
        if Some(call) == self.fail_on_call {
            return Err(DataFusionError::Internal(format!(
                "stub decode failure on call {call}"
            )));
        }
        Ok(Arc::new(Column::new("decoded", 0)))
    }
}

/// Decoder that must never run: used to assert that the reject paths of a
/// `try_from_proto` (wrong node, missing child) bail out before decoding.
pub(crate) struct UnreachableDecoder;

impl PhysicalExprDecode for UnreachableDecoder {
    fn decode(
        &self,
        _node: &PhysicalExprNode,
        _schema: &Schema,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        unreachable!("decode must not be reached when the node is rejected")
    }
}

/// Encoder stub for driving `try_to_proto`: emits a placeholder `Column` node
/// for each child, optionally failing on the Nth `encode` call so the
/// `ctx.encode_child(..)?` error arms can be exercised.
pub(crate) struct StubEncoder {
    fail_on_call: Option<usize>,
    calls: Cell<usize>,
}

impl StubEncoder {
    /// Always succeeds, emitting a placeholder `Column` node per child.
    pub(crate) fn ok() -> Self {
        Self {
            fail_on_call: None,
            calls: Cell::new(0),
        }
    }

    /// Fails on the `call`-th invocation (1-based), succeeding otherwise.
    pub(crate) fn failing_on(call: usize) -> Self {
        Self {
            fail_on_call: Some(call),
            calls: Cell::new(0),
        }
    }
}

impl PhysicalExprEncode for StubEncoder {
    fn encode(&self, _expr: &Arc<dyn PhysicalExpr>) -> Result<PhysicalExprNode> {
        let call = self.calls.get() + 1;
        self.calls.set(call);
        if Some(call) == self.fail_on_call {
            return Err(DataFusionError::Internal(format!(
                "stub encode failure on call {call}"
            )));
        }
        Ok(column_node("child"))
    }
}
