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

//! Serde dispatch itself: which hook the central (de)serializer reaches,
//! and how a custom converter or a deprecated shim participates.

use super::roundtrip_test_and_return;
use datafusion::arrow::compute::kernels::sort::SortOptions;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::expressions::{PhysicalSortExpr, col};
use datafusion::physical_plan::projection::{ProjectionExec, ProjectionExpr};
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan, PhysicalExpr,
    PlanProperties, ReplaceChildrenOptions, SendableRecordBatchStream,
};
use datafusion::prelude::SessionContext;
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::{Result, exec_datafusion_err};
use datafusion_proto::physical_plan::to_proto::serialize_physical_expr_with_converter;
use datafusion_proto::physical_plan::{
    AsExecutionPlan, DefaultPhysicalExtensionCodec, PhysicalExtensionCodec,
    PhysicalPlanDecodeContext, PhysicalPlanNodeExt, PhysicalProtoConverterExtension,
};
use datafusion_proto::protobuf;
use datafusion_proto::protobuf::{PhysicalExprNode, PhysicalPlanNode};
use std::fmt::Formatter;
use std::sync::{Arc, RwLock};
use std::vec;

#[derive(Debug)]
struct DowncastDelegatingExec {
    inner: Arc<dyn ExecutionPlan>,
}

impl DowncastDelegatingExec {
    fn new(inner: Arc<dyn ExecutionPlan>) -> Self {
        Self { inner }
    }
}

impl DisplayAs for DowncastDelegatingExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        self.inner.fmt_as(t, f)
    }
}

impl ExecutionPlan for DowncastDelegatingExec {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.inner.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.inner.children()
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        self.inner.apply_expressions(f)
    }

    fn replace_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        _: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let inner = Arc::clone(&self.inner).replace_children(
            children,
            ReplaceChildrenOptions {
                children_properties: ChildrenPropertiesMode::Recompute,
            },
        )?;
        Ok(Arc::new(Self::new(inner)))
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.replace_children(
            children,
            ReplaceChildrenOptions {
                children_properties: ChildrenPropertiesMode::Recompute,
            },
        )
    }

    fn downcast_delegate(&self) -> Option<&dyn ExecutionPlan> {
        Some(self.inner.as_ref())
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.inner.execute(partition, context)
    }
}

#[test]
fn serialize_uses_downcast_delegate() -> Result<()> {
    let inner: Arc<dyn ExecutionPlan> =
        Arc::new(EmptyExec::new(Arc::new(Schema::empty())));
    let plan: Arc<dyn ExecutionPlan> = Arc::new(DowncastDelegatingExec::new(inner));
    let codec = DefaultPhysicalExtensionCodec {};

    let proto = PhysicalPlanNode::try_from_physical_plan(plan, &codec)?;

    assert!(matches!(
        proto.physical_plan_type,
        Some(protobuf::physical_plan_node::PhysicalPlanType::Empty(_))
    ));

    Ok(())
}

/// A wrapper delegating to a plan that serializes itself via the
/// `try_to_proto` hook must serialize as its delegate: the wrapper's default
/// hook returns `Ok(None)` and the delegate has no downcast-chain fallback.
#[test]
fn serialize_uses_downcast_delegate_for_self_serializing_plan() -> Result<()> {
    let schema = Schema::new(vec![Field::new("a", DataType::Int64, false)]);
    let input = Arc::new(EmptyExec::new(Arc::new(schema.clone())));
    let inner: Arc<dyn ExecutionPlan> = Arc::new(ProjectionExec::try_new(
        vec![ProjectionExpr {
            expr: col("a", &schema)?,
            alias: "a".to_string(),
        }],
        input,
    )?);
    let plan: Arc<dyn ExecutionPlan> = Arc::new(DowncastDelegatingExec::new(inner));
    let codec = DefaultPhysicalExtensionCodec {};

    let proto = PhysicalPlanNode::try_from_physical_plan(plan, &codec)?;

    assert!(matches!(
        proto.physical_plan_type,
        Some(protobuf::physical_plan_node::PhysicalPlanType::Projection(
            _
        ))
    ));

    Ok(())
}

#[tokio::test]
async fn roundtrip_physical_plan_node() {
    use datafusion::prelude::*;
    use datafusion_proto::physical_plan::{
        AsExecutionPlan, DefaultPhysicalExtensionCodec,
    };
    use datafusion_proto::protobuf::PhysicalPlanNode;

    let ctx = SessionContext::new();

    ctx.register_parquet(
        "pt",
        &format!(
            "{}/alltypes_plain.snappy.parquet",
            datafusion_common::test_util::parquet_test_data()
        ),
        ParquetReadOptions::default(),
    )
    .await
    .unwrap();

    let plan = ctx
        .sql("select id, string_col, timestamp_col from pt where id > 4 order by string_col")
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();

    let node: PhysicalPlanNode =
        PhysicalPlanNode::try_from_physical_plan(plan, &DefaultPhysicalExtensionCodec {})
            .unwrap();

    let plan = node
        .try_into_physical_plan(&ctx.task_ctx(), &DefaultPhysicalExtensionCodec {})
        .unwrap();

    let _ = plan.execute(0, ctx.task_ctx()).unwrap();
}

#[test]
fn custom_proto_converter_intercepts() -> Result<()> {
    #[derive(Default)]
    struct CustomConverterInterceptor {
        num_proto_plans: RwLock<usize>,
        num_physical_plans: RwLock<usize>,
        num_proto_exprs: RwLock<usize>,
        num_physical_exprs: RwLock<usize>,
    }

    impl PhysicalProtoConverterExtension for CustomConverterInterceptor {
        fn proto_to_execution_plan(
            &self,
            proto: &PhysicalPlanNode,
            ctx: &PhysicalPlanDecodeContext<'_>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            {
                let mut counter = self
                    .num_proto_plans
                    .write()
                    .map_err(|err| exec_datafusion_err!("{err}"))?;
                *counter += 1;
            }
            self.default_proto_to_execution_plan(proto, ctx)
        }

        fn execution_plan_to_proto(
            &self,
            plan: &Arc<dyn ExecutionPlan>,
            codec: &dyn PhysicalExtensionCodec,
        ) -> Result<PhysicalPlanNode>
        where
            Self: Sized,
        {
            {
                let mut counter = self
                    .num_physical_plans
                    .write()
                    .map_err(|err| exec_datafusion_err!("{err}"))?;
                *counter += 1;
            }
            PhysicalPlanNode::try_from_physical_plan_with_converter(
                Arc::clone(plan),
                codec,
                self,
            )
        }

        fn proto_to_physical_expr(
            &self,
            proto: &PhysicalExprNode,
            input_schema: &Schema,
            ctx: &PhysicalPlanDecodeContext<'_>,
        ) -> Result<Arc<dyn PhysicalExpr>>
        where
            Self: Sized,
        {
            {
                let mut counter = self
                    .num_proto_exprs
                    .write()
                    .map_err(|err| exec_datafusion_err!("{err}"))?;
                *counter += 1;
            }
            self.default_proto_to_physical_expr(proto, input_schema, ctx)
        }

        fn physical_expr_to_proto(
            &self,
            expr: &Arc<dyn PhysicalExpr>,
            codec: &dyn PhysicalExtensionCodec,
        ) -> Result<PhysicalExprNode> {
            {
                let mut counter = self
                    .num_physical_exprs
                    .write()
                    .map_err(|err| exec_datafusion_err!("{err}"))?;
                *counter += 1;
            }
            serialize_physical_expr_with_converter(expr, codec, self)
        }
    }

    let field_a = Field::new("a", DataType::Boolean, false);
    let field_b = Field::new("b", DataType::Int64, false);
    let schema = Arc::new(Schema::new(vec![field_a, field_b]));
    let sort_exprs = [
        PhysicalSortExpr {
            expr: col("a", &schema)?,
            options: SortOptions {
                descending: true,
                nulls_first: false,
            },
        },
        PhysicalSortExpr {
            expr: col("b", &schema)?,
            options: SortOptions {
                descending: false,
                nulls_first: true,
            },
        },
    ]
    .into();

    let exec_plan = Arc::new(SortExec::new(sort_exprs, Arc::new(EmptyExec::new(schema))));

    let ctx = SessionContext::new();
    let codec = DefaultPhysicalExtensionCodec {};
    let proto_converter = CustomConverterInterceptor::default();
    roundtrip_test_and_return(exec_plan, &ctx, &codec, &proto_converter)?;

    assert_eq!(*proto_converter.num_proto_exprs.read().unwrap(), 2);
    assert_eq!(*proto_converter.num_physical_exprs.read().unwrap(), 2);
    assert_eq!(*proto_converter.num_proto_plans.read().unwrap(), 2);
    assert_eq!(*proto_converter.num_physical_plans.read().unwrap(), 2);

    Ok(())
}
