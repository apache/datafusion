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

//! See `main.rs` for how to run it.
//!
//! This example demonstrates how to compose multiple PhysicalExtensionCodecs
//!
//! This can be helpful when an Execution plan tree has different nodes from different crates
//! that need to be serialized.
//!
//! For example if your plan has `ShuffleWriterExec` from `datafusion-ballista` and `DeltaScan` from `deltalake`
//! both crates both provide PhysicalExtensionCodec and this example shows how to combine them together
//!
//! ```text
//! ShuffleWriterExec
//!     ProjectionExec
//!        ...
//!           DeltaScan
//! ```

use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use datafusion::common::internal_err;
use datafusion::common::Result;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::{DisplayAs, ExecutionPlan};
use datafusion::prelude::SessionContext;
use datafusion_proto::physical_plan::{
    AsExecutionPlan, ComposedPhysicalExtensionCodec, PhysicalExtensionCodec,
};
use datafusion_proto::protobuf;

/// Example of using multiple extension codecs for serialization / deserialization
pub async fn composed_extension_codec() -> Result<()> {
    // build execution plan that has both types of nodes
    //
    // Note each node requires a different `PhysicalExtensionCodec` to decode
    let exec_plan = Arc::new(ParentExec {
        input: Arc::new(ChildExec {}),
    });
    let ctx = SessionContext::new();

    // Position in this list is important as it will be used for decoding.
    // If new codec is added it should go to last position.
    let composed_codec = ComposedPhysicalExtensionCodec::new(vec![
        Arc::new(ParentPhysicalExtensionCodec {}),
        Arc::new(ChildPhysicalExtensionCodec {}),
    ]);

    // serialize execution plan to proto
    let proto: protobuf::PhysicalPlanNode =
        protobuf::PhysicalPlanNode::try_from_physical_plan(
            exec_plan.clone(),
            &composed_codec,
        )?;

    // deserialize proto back to execution plan
    let result_exec_plan: Arc<dyn ExecutionPlan> =
        proto.try_into_physical_plan(&ctx.task_ctx(), &composed_codec)?;

    // assert that the original and deserialized execution plans are equal
    assert_eq!(format!("{exec_plan:?}"), format!("{result_exec_plan:?}"));

    Ok(())
}

/// This example has two types of nodes: `ParentExec` and `ChildExec` which can only
/// be serialized with different `PhysicalExtensionCodec`s
#[derive(Debug)]
struct ParentExec {
    input: Arc<dyn ExecutionPlan>,
}

impl DisplayAs for ParentExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "ParentExec")
    }
}

impl ExecutionPlan for ParentExec {
    fn name(&self) -> &str {
        "ParentExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &datafusion::physical_plan::PlanProperties {
        unreachable!()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        unreachable!()
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<datafusion::physical_plan::SendableRecordBatchStream> {
        unreachable!()
    }
}

/// A PhysicalExtensionCodec that can serialize and deserialize ParentExec
#[derive(Debug)]
struct ParentPhysicalExtensionCodec;

impl PhysicalExtensionCodec for ParentPhysicalExtensionCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        _ctx: &TaskContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if buf == "ParentExec".as_bytes() {
            Ok(Arc::new(ParentExec {
                input: inputs[0].clone(),
            }))
        } else {
            internal_err!("Not supported")
        }
    }

    fn try_encode(&self, node: Arc<dyn ExecutionPlan>, buf: &mut Vec<u8>) -> Result<()> {
        if node.as_any().downcast_ref::<ParentExec>().is_some() {
            buf.extend_from_slice("ParentExec".as_bytes());
            Ok(())
        } else {
            internal_err!("Not supported")
        }
    }
}

#[derive(Debug)]
struct ChildExec {}

impl DisplayAs for ChildExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "ChildExec")
    }
}

impl ExecutionPlan for ChildExec {
    fn name(&self) -> &str {
        "ChildExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &datafusion::physical_plan::PlanProperties {
        unreachable!()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        unreachable!()
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<datafusion::physical_plan::SendableRecordBatchStream> {
        unreachable!()
    }
}

/// A PhysicalExtensionCodec that can serialize and deserialize ChildExec
#[derive(Debug)]
struct ChildPhysicalExtensionCodec;

impl PhysicalExtensionCodec for ChildPhysicalExtensionCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        _inputs: &[Arc<dyn ExecutionPlan>],
        _ctx: &TaskContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if buf == "ChildExec".as_bytes() {
            Ok(Arc::new(ChildExec {}))
        } else {
            internal_err!("Not supported")
        }
    }

    fn try_encode(&self, node: Arc<dyn ExecutionPlan>, buf: &mut Vec<u8>) -> Result<()> {
        if node.as_any().downcast_ref::<ChildExec>().is_some() {
            buf.extend_from_slice("ChildExec".as_bytes());
            Ok(())
        } else {
            internal_err!("Not supported")
        }
    }
}
