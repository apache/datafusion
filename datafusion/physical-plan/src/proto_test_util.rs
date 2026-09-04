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

//! Shared test helpers for the colocated `try_to_proto` / `try_from_proto`
//! plan unit tests.
//!
//! These let a test drive a plan's serde hooks without depending on
//! `datafusion-proto` (which sits above this crate and would create a
//! dependency cycle): the dispatch inversion in [`crate::proto`] means a test
//! can supply its own [`ExecutionPlanEncode`] / [`ExecutionPlanDecode`].
//!
//! This is the plan-level sibling of `datafusion_physical_expr::proto_test_util`.
//!
//! # What this tier is for
//!
//! Colocated tests prove a plan handles *its own fields*: enum conversions that
//! must be by-name, the `[u32::MAX]` empty-projection sentinel, `fetch`
//! presence semantics (absent → `None`, not `Some(0)`). They live next to the
//! field so they rot when someone adds one, and they can assert on wire state
//! that a plan's `Debug` output never shows.
//!
//! They do *not* replace the central round-trip tests in `datafusion-proto`,
//! which prove things this tier structurally cannot: that the real
//! `PhysicalExtensionCodec` works, that dispatch actually reaches the hook, and
//! that bytes survive bytes.

use std::cell::Cell;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion_common::{DataFusionError, Result, internal_err};
use datafusion_execution::TaskContext;
use datafusion_expr::physical_planning_context::ScalarSubqueryResults;
use datafusion_expr::{AggregateUDF, ScalarUDF, WindowUDF};
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::expressions::Column;
use datafusion_proto_models::protobuf::{
    self, PhysicalExprNode, PhysicalPlanNode, physical_expr_node,
};

use crate::ExecutionPlan;
use crate::empty::EmptyExec;
use crate::proto::{ExecutionPlanDecode, ExecutionPlanEncode};

/// The schema shared by the stub child plans: `a: Int32, b: Int32`.
///
/// Two columns so a test can build an ordering (or a join key pair) whose
/// members stay distinguishable through the hooks.
pub(crate) fn stub_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int32, true),
        Field::new("b", DataType::Int32, true),
    ]))
}

/// A child plan to hang the plan under test off of.
pub(crate) fn stub_child() -> Arc<dyn ExecutionPlan> {
    Arc::new(EmptyExec::new(stub_schema()))
}

/// A proto node for a `Column`, as a stand-in child node when building a plan's
/// proto representation by hand.
pub(crate) fn column_node(name: &str, index: u32) -> PhysicalExprNode {
    PhysicalExprNode {
        expr_id: None,
        expr_type: Some(physical_expr_node::ExprType::Column(
            protobuf::PhysicalColumn {
                name: name.to_string(),
                index,
            },
        )),
    }
}

/// A proto node for a sort expression over `name`, as written by the sort
/// plans' `try_to_proto`.
pub(crate) fn sort_expr_node(
    name: &str,
    index: u32,
    asc: bool,
    nulls_first: bool,
) -> PhysicalExprNode {
    PhysicalExprNode {
        expr_id: None,
        expr_type: Some(physical_expr_node::ExprType::Sort(Box::new(
            protobuf::PhysicalSortExprNode {
                expr: Some(Box::new(column_node(name, index))),
                asc,
                nulls_first,
            },
        ))),
    }
}

/// The placeholder node [`StubPlanEncoder`] emits for every child plan.
///
/// Distinct from `PhysicalPlanNode::default()` so a test can tell "the hook
/// encoded the child" apart from "the field was left at its default".
pub(crate) fn encoded_child_node() -> PhysicalPlanNode {
    PhysicalPlanNode {
        physical_plan_type: Some(protobuf::physical_plan_node::PhysicalPlanType::Empty(
            protobuf::EmptyExecNode {
                schema: None,
                partitions: 0,
            },
        )),
    }
}

/// Encoder stub for driving `try_to_proto`.
///
/// Emits a recognizable placeholder for each child plan and expression, counts
/// the calls, and can fail on the Nth call so the `ctx.encode_child(..)?` /
/// `ctx.encode_expr(..)?` error arms are exercised too.
pub(crate) struct StubPlanEncoder {
    plan_calls: Cell<usize>,
    expr_calls: Cell<usize>,
    fail_plan_on: Option<usize>,
    fail_expr_on: Option<usize>,
}

impl StubPlanEncoder {
    /// Always succeeds.
    pub(crate) fn ok() -> Self {
        Self {
            plan_calls: Cell::new(0),
            expr_calls: Cell::new(0),
            fail_plan_on: None,
            fail_expr_on: None,
        }
    }

    /// Fails on the `call`-th child-plan encode (1-based).
    pub(crate) fn failing_on_plan(call: usize) -> Self {
        Self {
            fail_plan_on: Some(call),
            ..Self::ok()
        }
    }

    /// Fails on the `call`-th expression encode (1-based).
    pub(crate) fn failing_on_expr(call: usize) -> Self {
        Self {
            fail_expr_on: Some(call),
            ..Self::ok()
        }
    }

    /// How many child plans were encoded.
    pub(crate) fn plan_calls(&self) -> usize {
        self.plan_calls.get()
    }

    /// How many expressions were encoded.
    pub(crate) fn expr_calls(&self) -> usize {
        self.expr_calls.get()
    }
}

impl ExecutionPlanEncode for StubPlanEncoder {
    fn encode_plan(&self, _plan: &Arc<dyn ExecutionPlan>) -> Result<PhysicalPlanNode> {
        let call = self.plan_calls.get() + 1;
        self.plan_calls.set(call);
        if Some(call) == self.fail_plan_on {
            return Err(DataFusionError::Internal(format!(
                "stub plan encode failure on call {call}"
            )));
        }
        Ok(encoded_child_node())
    }

    fn encode_expr(&self, _expr: &Arc<dyn PhysicalExpr>) -> Result<PhysicalExprNode> {
        let call = self.expr_calls.get() + 1;
        self.expr_calls.set(call);
        if Some(call) == self.fail_expr_on {
            return Err(DataFusionError::Internal(format!(
                "stub expr encode failure on call {call}"
            )));
        }
        Ok(column_node("child", 0))
    }

    fn encode_udf(&self, _udf: &ScalarUDF) -> Result<Option<Vec<u8>>> {
        Ok(None)
    }

    fn encode_udaf(&self, _udaf: &AggregateUDF) -> Result<Option<Vec<u8>>> {
        Ok(None)
    }

    fn encode_udwf(&self, _udwf: &WindowUDF) -> Result<Option<Vec<u8>>> {
        Ok(None)
    }
}

/// Decoder stub for driving `try_from_proto`.
///
/// Returns a fixed [`stub_child`] plan for every child node and decodes column
/// nodes for real (so a test can still tell two sort keys apart), counting
/// calls and optionally failing on the Nth one.
pub(crate) struct StubPlanDecoder {
    task_ctx: Arc<TaskContext>,
    plan_calls: Cell<usize>,
    expr_calls: Cell<usize>,
    fail_plan_on: Option<usize>,
    fail_expr_on: Option<usize>,
}

impl StubPlanDecoder {
    /// Always succeeds.
    pub(crate) fn ok() -> Self {
        Self {
            task_ctx: Arc::new(TaskContext::default()),
            plan_calls: Cell::new(0),
            expr_calls: Cell::new(0),
            fail_plan_on: None,
            fail_expr_on: None,
        }
    }

    /// Fails on the `call`-th child-plan decode (1-based).
    pub(crate) fn failing_on_plan(call: usize) -> Self {
        Self {
            fail_plan_on: Some(call),
            ..Self::ok()
        }
    }

    /// Fails on the `call`-th expression decode (1-based).
    pub(crate) fn failing_on_expr(call: usize) -> Self {
        Self {
            fail_expr_on: Some(call),
            ..Self::ok()
        }
    }

    /// How many child plans were decoded.
    pub(crate) fn plan_calls(&self) -> usize {
        self.plan_calls.get()
    }

    /// How many expressions were decoded.
    pub(crate) fn expr_calls(&self) -> usize {
        self.expr_calls.get()
    }
}

impl ExecutionPlanDecode for StubPlanDecoder {
    fn decode_plan(&self, _node: &PhysicalPlanNode) -> Result<Arc<dyn ExecutionPlan>> {
        let call = self.plan_calls.get() + 1;
        self.plan_calls.set(call);
        if Some(call) == self.fail_plan_on {
            return Err(DataFusionError::Internal(format!(
                "stub plan decode failure on call {call}"
            )));
        }
        Ok(stub_child())
    }

    fn decode_plan_with_scalar_subquery_results(
        &self,
        node: &PhysicalPlanNode,
        _results: ScalarSubqueryResults,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.decode_plan(node)
    }

    fn decode_expr(
        &self,
        node: &PhysicalExprNode,
        _input_schema: &Schema,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let call = self.expr_calls.get() + 1;
        self.expr_calls.set(call);
        if Some(call) == self.fail_expr_on {
            return Err(DataFusionError::Internal(format!(
                "stub expr decode failure on call {call}"
            )));
        }
        match &node.expr_type {
            Some(physical_expr_node::ExprType::Column(c)) => {
                Ok(Arc::new(Column::new(&c.name, c.index as usize)))
            }
            _ => Ok(Arc::new(Column::new("a", 0))),
        }
    }

    fn task_ctx(&self) -> &TaskContext {
        &self.task_ctx
    }

    fn decode_udf(&self, name: &str, _payload: Option<&[u8]>) -> Result<Arc<ScalarUDF>> {
        internal_err!("stub decoder cannot decode the scalar UDF {name}")
    }

    fn decode_udaf(
        &self,
        name: &str,
        _payload: Option<&[u8]>,
    ) -> Result<Arc<AggregateUDF>> {
        internal_err!("stub decoder cannot decode the aggregate UDF {name}")
    }

    fn decode_udwf(&self, name: &str, _payload: Option<&[u8]>) -> Result<Arc<WindowUDF>> {
        internal_err!("stub decoder cannot decode the window UDF {name}")
    }
}

/// Decoder that must never run: asserts that the reject paths of a
/// `try_from_proto` (wrong node variant, missing required child) bail out
/// before any decoding happens.
pub(crate) struct UnreachablePlanDecoder {
    task_ctx: Arc<TaskContext>,
}

impl UnreachablePlanDecoder {
    pub(crate) fn new() -> Self {
        Self {
            task_ctx: Arc::new(TaskContext::default()),
        }
    }
}

impl ExecutionPlanDecode for UnreachablePlanDecoder {
    fn decode_plan(&self, _node: &PhysicalPlanNode) -> Result<Arc<dyn ExecutionPlan>> {
        unreachable!("decode_plan must not be reached when the node is rejected")
    }

    fn decode_plan_with_scalar_subquery_results(
        &self,
        _node: &PhysicalPlanNode,
        _results: ScalarSubqueryResults,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        unreachable!("decode_plan must not be reached when the node is rejected")
    }

    fn decode_expr(
        &self,
        _node: &PhysicalExprNode,
        _input_schema: &Schema,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        unreachable!("decode_expr must not be reached when the node is rejected")
    }

    fn task_ctx(&self) -> &TaskContext {
        &self.task_ctx
    }

    fn decode_udf(&self, _name: &str, _payload: Option<&[u8]>) -> Result<Arc<ScalarUDF>> {
        unreachable!("decode_udf must not be reached when the node is rejected")
    }

    fn decode_udaf(
        &self,
        _name: &str,
        _payload: Option<&[u8]>,
    ) -> Result<Arc<AggregateUDF>> {
        unreachable!("decode_udaf must not be reached when the node is rejected")
    }

    fn decode_udwf(
        &self,
        _name: &str,
        _payload: Option<&[u8]>,
    ) -> Result<Arc<WindowUDF>> {
        unreachable!("decode_udwf must not be reached when the node is rejected")
    }
}
