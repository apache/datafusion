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
//! The same problem as `composed_extension_codec.rs`, solved without composing
//! codecs: two plans from two independent crates in one tree.
//!
//! ```text
//! ParentExec      (from crate A)
//!     ChildExec   (from crate B)
//! ```
//!
//! With `ComposedPhysicalExtensionCodec` the decoding side has to register the
//! same codecs in the same *order* the encoder used, because the composed codec
//! writes the position of the encoding codec into the payload. Here each plan
//! names itself on the wire instead, so order does not matter and a name two
//! crates both claim is an error at registration rather than a wrong decode.

use std::fmt::Formatter;
use std::sync::Arc;

use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{Result, internal_err};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::proto::{
    ExecutionPlanDecodeCtx, ExecutionPlanEncodeCtx, ExtensionPlanFromProto,
    ProtoDecoderRegistry, register_execution_plan,
};
use datafusion::physical_plan::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    ReplaceChildrenOptions, SendableRecordBatchStream,
};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_proto::physical_plan::{AsExecutionPlan, DefaultPhysicalExtensionCodec};
use datafusion_proto::protobuf::PhysicalPlanNode;

pub fn extension_plan_registry() -> Result<()> {
    let plan: Arc<dyn ExecutionPlan> = Arc::new(ParentExec {
        input: Arc::new(ChildExec {}),
    });

    // Each crate exposes a function that fills a registry it is handed; the
    // application that owns the session composes them. The two crates need to
    // know nothing about each other, and the call order does not matter.
    let mut registry = ProtoDecoderRegistry::new();
    crate_a::register(&mut registry)?;
    crate_b::register(&mut registry)?;

    let ctx = SessionContext::new_with_config(
        SessionConfig::new().with_extension(Arc::new(registry)),
    );

    // `DefaultPhysicalExtensionCodec` decodes nothing at all, so whatever
    // survives the round trip came from the registry.
    let codec = DefaultPhysicalExtensionCodec {};
    let node = PhysicalPlanNode::try_from_physical_plan(Arc::clone(&plan), &codec)?;
    let decoded = node.try_into_physical_plan(ctx.task_ctx().as_ref(), &codec)?;

    assert_eq!(format!("{plan:?}"), format!("{decoded:?}"));

    // A session that does not register the plans cannot decode them, and the
    // error names what is missing rather than only what the codec said. Children
    // are decoded before their parent falls back to the codec, so the innermost
    // unregistered plan is the one reported:
    //
    //     No decoder is registered for the extension ExecutionPlan
    //     'example-crate-b.ChildExec'. Register the plan in the
    //     ProtoDecoderRegistry attached to the decoding session's
    //     SessionConfig, ... Registered extension plans: none
    let bare = SessionContext::new();
    let err = node
        .try_into_physical_plan(bare.task_ctx().as_ref(), &codec)
        .expect_err("an unregistered plan must not decode");
    assert!(err.to_string().contains("No decoder is registered"));
    assert!(err.to_string().contains(ChildExec::NAME));

    Ok(())
}

/// What the crate that owns `ParentExec` would export.
mod crate_a {
    use super::*;

    pub fn register(registry: &mut ProtoDecoderRegistry) -> Result<()> {
        register_execution_plan::<ParentExec>(registry)
    }
}

/// What the crate that owns `ChildExec` would export.
mod crate_b {
    use super::*;

    pub fn register(registry: &mut ProtoDecoderRegistry) -> Result<()> {
        register_execution_plan::<ChildExec>(registry)
    }
}

#[derive(Debug)]
struct ParentExec {
    input: Arc<dyn ExecutionPlan>,
}

impl ExtensionPlanFromProto for ParentExec {
    // Namespaced with the owning crate, so a collision with another crate is
    // an error at registration rather than a wrong decode.
    const NAME: &'static str = "example-crate-a.ParentExec";

    fn try_from_proto(
        node: &PhysicalPlanNode,
        ctx: &ExecutionPlanDecodeCtx<'_>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let extension = datafusion::physical_plan::expect_plan_variant!(
            node,
            datafusion_proto::protobuf::physical_plan_node::PhysicalPlanType::Extension,
            "Extension",
        );
        let mut children = ctx.decode_children(&extension.inputs)?;
        if children.len() != 1 {
            return internal_err!("ParentExec expects exactly one child");
        }
        Ok(Arc::new(ParentExec {
            input: children.remove(0),
        }))
    }
}

#[derive(Debug)]
struct ChildExec {}

impl ExtensionPlanFromProto for ChildExec {
    const NAME: &'static str = "example-crate-b.ChildExec";

    fn try_from_proto(
        _node: &PhysicalPlanNode,
        _ctx: &ExecutionPlanDecodeCtx<'_>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(ChildExec {}))
    }
}

/// Both plans are serde-only: this example never executes them.
macro_rules! serde_only_body {
    ($name:ident) => {
        fn name(&self) -> &str {
            stringify!($name)
        }

        fn properties(&self) -> &Arc<PlanProperties> {
            unreachable!("this example only serializes")
        }

        fn apply_expressions(
            &self,
            _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
        ) -> Result<TreeNodeRecursion> {
            Ok(TreeNodeRecursion::Continue)
        }

        fn with_new_children(
            self: Arc<Self>,
            children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            self.replace_children(
                children,
                ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
            )
        }

        fn execute(
            &self,
            _partition: usize,
            _context: Arc<TaskContext>,
        ) -> Result<SendableRecordBatchStream> {
            internal_err!("{} is a serde-only example plan", stringify!($name))
        }

        /// `extension_node` stamps `Self::NAME`, so the name on the wire and
        /// the registry key cannot drift apart. Neither plan has state of its
        /// own, hence the empty payload.
        fn try_to_proto(
            &self,
            ctx: &ExecutionPlanEncodeCtx<'_>,
        ) -> Result<Option<PhysicalPlanNode>> {
            Ok(Some(ctx.extension_node::<Self>(vec![], self.children())?))
        }
    };
}

impl DisplayAs for ParentExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "ParentExec")
    }
}

impl ExecutionPlan for ParentExec {
    serde_only_body!(ParentExec);

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn replace_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
        _: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return internal_err!("ParentExec expects exactly one child");
        }
        Ok(Arc::new(ParentExec {
            input: children.remove(0),
        }))
    }
}

impl DisplayAs for ChildExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "ChildExec")
    }
}

impl ExecutionPlan for ChildExec {
    serde_only_body!(ChildExec);

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn replace_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
        _: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }
}
