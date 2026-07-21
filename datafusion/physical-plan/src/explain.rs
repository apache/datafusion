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

//! Defines the EXPLAIN operator

use std::sync::Arc;

use super::{DisplayAs, PlanProperties, SendableRecordBatchStream};
use crate::execution_plan::{Boundedness, EmissionType};
use crate::stream::RecordBatchStreamAdapter;
use crate::{DisplayFormatType, ExecutionPlan, Partitioning};

use arrow::{array::StringBuilder, datatypes::SchemaRef, record_batch::RecordBatch};
use datafusion_common::display::StringifiedPlan;
use datafusion_common::{Result, assert_eq_or_internal_err};
use datafusion_execution::TaskContext;
use datafusion_physical_expr::EquivalenceProperties;

use log::trace;

/// Explain execution plan operator. This operator contains the string
/// values of the various plans it has when it is created, and passes
/// them to its output.
#[derive(Debug, Clone)]
pub struct ExplainExec {
    /// The schema that this exec plan node outputs
    schema: SchemaRef,
    /// The strings to be printed
    stringified_plans: Vec<StringifiedPlan>,
    /// control which plans to print
    verbose: bool,
    cache: Arc<PlanProperties>,
}

impl ExplainExec {
    /// Create a new ExplainExec
    pub fn new(
        schema: SchemaRef,
        stringified_plans: Vec<StringifiedPlan>,
        verbose: bool,
    ) -> Self {
        let cache = Self::compute_properties(Arc::clone(&schema));
        ExplainExec {
            schema,
            stringified_plans,
            verbose,
            cache: Arc::new(cache),
        }
    }

    /// The strings to be printed
    pub fn stringified_plans(&self) -> &[StringifiedPlan] {
        &self.stringified_plans
    }

    /// Access to verbose
    pub fn verbose(&self) -> bool {
        self.verbose
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(schema: SchemaRef) -> PlanProperties {
        PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        )
    }
}

impl DisplayAs for ExplainExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "ExplainExec")
            }
            DisplayFormatType::TreeRender => {
                // TODO: collect info
                write!(f, "")
            }
        }
    }
}

impl ExecutionPlan for ExplainExec {
    fn name(&self) -> &'static str {
        "ExplainExec"
    }

    /// Return a reference to Any that can be used for downcasting
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        // This is a leaf node and has no children
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        trace!(
            "Start ExplainExec::execute for partition {} of context session_id {} and task_id {:?}",
            partition,
            context.session_id(),
            context.task_id()
        );
        assert_eq_or_internal_err!(
            partition,
            0,
            "ExplainExec invalid partition {partition}"
        );
        let mut type_builder =
            StringBuilder::with_capacity(self.stringified_plans.len(), 1024);
        let mut plan_builder =
            StringBuilder::with_capacity(self.stringified_plans.len(), 1024);

        let plans_to_print = self
            .stringified_plans
            .iter()
            .filter(|s| s.should_display(self.verbose));

        // Identify plans that are not changed
        let mut prev: Option<&StringifiedPlan> = None;

        for p in plans_to_print {
            type_builder.append_value(p.plan_type.to_string());
            match prev {
                Some(prev) if !should_show(prev, p) => {
                    plan_builder.append_value("SAME TEXT AS ABOVE");
                }
                Some(_) | None => {
                    plan_builder.append_value(&*p.plan);
                }
            }
            prev = Some(p);
        }

        let record_batch = RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![
                Arc::new(type_builder.finish()),
                Arc::new(plan_builder.finish()),
            ],
        )?;

        trace!(
            "Before returning RecordBatchStream in ExplainExec::execute for partition {} of context session_id {} and task_id {:?}",
            partition,
            context.session_id(),
            context.task_id()
        );

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            futures::stream::iter(vec![Ok(record_batch)]),
        )))
    }

    #[cfg(feature = "proto")]
    fn try_to_proto(
        &self,
        _ctx: &crate::proto::ExecutionPlanEncodeCtx<'_>,
    ) -> Result<Option<datafusion_proto_models::protobuf::PhysicalPlanNode>> {
        use datafusion_proto_models::protobuf;
        // ExplainExec is a leaf plan: it carries its schema, the stringified
        // plans, and the verbose flag on the wire (no exec children).
        let schema = self.schema().as_ref().try_into()?;
        Ok(Some(protobuf::PhysicalPlanNode {
            physical_plan_type: Some(
                protobuf::physical_plan_node::PhysicalPlanType::Explain(
                    protobuf::ExplainExecNode {
                        schema: Some(schema),
                        stringified_plans: self
                            .stringified_plans
                            .iter()
                            .map(stringified_plan_to_proto)
                            .collect(),
                        verbose: self.verbose,
                    },
                ),
            ),
        }))
    }
}

#[cfg(feature = "proto")]
impl ExplainExec {
    /// Reconstruct an [`ExplainExec`] from its protobuf representation.
    ///
    /// The inverse of [`ExecutionPlan::try_to_proto`]: it takes the whole
    /// [`PhysicalPlanNode`] and reads back the schema, the stringified plans and
    /// the verbose flag (this leaf plan has no children to decode).
    ///
    /// [`PhysicalPlanNode`]: datafusion_proto_models::protobuf::PhysicalPlanNode
    /// [`ExecutionPlan::try_to_proto`]: crate::ExecutionPlan::try_to_proto
    pub fn try_from_proto(
        node: &datafusion_proto_models::protobuf::PhysicalPlanNode,
        _ctx: &crate::proto::ExecutionPlanDecodeCtx<'_>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        use datafusion_proto_models::protobuf;
        let explain = crate::expect_plan_variant!(
            node,
            protobuf::physical_plan_node::PhysicalPlanType::Explain,
            "ExplainExec",
        );
        let schema = explain.schema.as_ref().ok_or_else(|| {
            datafusion_common::internal_datafusion_err!(
                "ExplainExec is missing required field 'schema'"
            )
        })?;
        let schema = Arc::new(arrow::datatypes::Schema::try_from(schema)?);
        Ok(Arc::new(ExplainExec::new(
            schema,
            explain
                .stringified_plans
                .iter()
                .map(stringified_plan_from_proto)
                .collect(),
            explain.verbose,
        )))
    }
}

/// Convert a [`StringifiedPlan`] into its protobuf representation.
///
/// Inlined here (rather than reused from `datafusion-proto`) because the
/// `StringifiedPlan` <-> proto conversion lives in `datafusion-proto`, which
/// sits *above* `datafusion-physical-plan` in the crate graph. The conversion is
/// pure (a match over [`PlanType`](datafusion_common::display::PlanType) and the
/// pure prost model types), so it can be expressed against
/// `datafusion-proto-models` directly. The exhaustive match keeps the wire
/// format byte-identical and makes any added `PlanType` variant a compile error.
#[cfg(feature = "proto")]
fn stringified_plan_to_proto(
    stringified_plan: &StringifiedPlan,
) -> datafusion_proto_models::protobuf::StringifiedPlan {
    use datafusion_common::display::PlanType;
    use datafusion_proto_models::datafusion_common::EmptyMessage;
    use datafusion_proto_models::protobuf;
    use protobuf::plan_type::PlanTypeEnum::{
        AnalyzedLogicalPlan, FinalAnalyzedLogicalPlan, FinalLogicalPlan,
        FinalPhysicalPlan, FinalPhysicalPlanWithSchema, FinalPhysicalPlanWithStats,
        InitialLogicalPlan, InitialPhysicalPlan, InitialPhysicalPlanWithSchema,
        InitialPhysicalPlanWithStats, OptimizedLogicalPlan, OptimizedPhysicalPlan,
        PhysicalPlanError,
    };

    protobuf::StringifiedPlan {
        plan_type: match stringified_plan.clone().plan_type {
            PlanType::InitialLogicalPlan => Some(protobuf::PlanType {
                plan_type_enum: Some(InitialLogicalPlan(EmptyMessage {})),
            }),
            PlanType::AnalyzedLogicalPlan { analyzer_name } => Some(protobuf::PlanType {
                plan_type_enum: Some(AnalyzedLogicalPlan(
                    protobuf::AnalyzedLogicalPlanType { analyzer_name },
                )),
            }),
            PlanType::FinalAnalyzedLogicalPlan => Some(protobuf::PlanType {
                plan_type_enum: Some(FinalAnalyzedLogicalPlan(EmptyMessage {})),
            }),
            PlanType::OptimizedLogicalPlan { optimizer_name } => {
                Some(protobuf::PlanType {
                    plan_type_enum: Some(OptimizedLogicalPlan(
                        protobuf::OptimizedLogicalPlanType { optimizer_name },
                    )),
                })
            }
            PlanType::FinalLogicalPlan => Some(protobuf::PlanType {
                plan_type_enum: Some(FinalLogicalPlan(EmptyMessage {})),
            }),
            PlanType::InitialPhysicalPlan => Some(protobuf::PlanType {
                plan_type_enum: Some(InitialPhysicalPlan(EmptyMessage {})),
            }),
            PlanType::OptimizedPhysicalPlan { optimizer_name } => {
                Some(protobuf::PlanType {
                    plan_type_enum: Some(OptimizedPhysicalPlan(
                        protobuf::OptimizedPhysicalPlanType { optimizer_name },
                    )),
                })
            }
            PlanType::FinalPhysicalPlan => Some(protobuf::PlanType {
                plan_type_enum: Some(FinalPhysicalPlan(EmptyMessage {})),
            }),
            PlanType::InitialPhysicalPlanWithStats => Some(protobuf::PlanType {
                plan_type_enum: Some(InitialPhysicalPlanWithStats(EmptyMessage {})),
            }),
            PlanType::InitialPhysicalPlanWithSchema => Some(protobuf::PlanType {
                plan_type_enum: Some(InitialPhysicalPlanWithSchema(EmptyMessage {})),
            }),
            PlanType::FinalPhysicalPlanWithStats => Some(protobuf::PlanType {
                plan_type_enum: Some(FinalPhysicalPlanWithStats(EmptyMessage {})),
            }),
            PlanType::FinalPhysicalPlanWithSchema => Some(protobuf::PlanType {
                plan_type_enum: Some(FinalPhysicalPlanWithSchema(EmptyMessage {})),
            }),
            PlanType::PhysicalPlanError => Some(protobuf::PlanType {
                plan_type_enum: Some(PhysicalPlanError(EmptyMessage {})),
            }),
        },
        plan: stringified_plan.plan.to_string(),
    }
}

/// Reconstruct a [`StringifiedPlan`] from its protobuf representation. The
/// inverse of [`stringified_plan_to_proto`]; see that function for why this is
/// inlined here rather than reused from `datafusion-proto`.
#[cfg(feature = "proto")]
fn stringified_plan_from_proto(
    stringified_plan: &datafusion_proto_models::protobuf::StringifiedPlan,
) -> StringifiedPlan {
    use datafusion_common::display::PlanType;
    use datafusion_proto_models::protobuf::plan_type::PlanTypeEnum::{
        AnalyzedLogicalPlan, FinalAnalyzedLogicalPlan, FinalLogicalPlan,
        FinalPhysicalPlan, FinalPhysicalPlanWithSchema, FinalPhysicalPlanWithStats,
        InitialLogicalPlan, InitialPhysicalPlan, InitialPhysicalPlanWithSchema,
        InitialPhysicalPlanWithStats, OptimizedLogicalPlan, OptimizedPhysicalPlan,
        PhysicalPlanError,
    };
    use datafusion_proto_models::protobuf::{
        AnalyzedLogicalPlanType, OptimizedLogicalPlanType, OptimizedPhysicalPlanType,
    };

    StringifiedPlan {
        plan_type: match stringified_plan
            .plan_type
            .as_ref()
            .and_then(|pt| pt.plan_type_enum.as_ref())
            .unwrap_or_else(|| {
                panic!(
                    "Cannot create protobuf::StringifiedPlan from {stringified_plan:?}"
                )
            }) {
            InitialLogicalPlan(_) => PlanType::InitialLogicalPlan,
            AnalyzedLogicalPlan(AnalyzedLogicalPlanType { analyzer_name }) => {
                PlanType::AnalyzedLogicalPlan {
                    analyzer_name: analyzer_name.clone(),
                }
            }
            FinalAnalyzedLogicalPlan(_) => PlanType::FinalAnalyzedLogicalPlan,
            OptimizedLogicalPlan(OptimizedLogicalPlanType { optimizer_name }) => {
                PlanType::OptimizedLogicalPlan {
                    optimizer_name: optimizer_name.clone(),
                }
            }
            FinalLogicalPlan(_) => PlanType::FinalLogicalPlan,
            InitialPhysicalPlan(_) => PlanType::InitialPhysicalPlan,
            InitialPhysicalPlanWithStats(_) => PlanType::InitialPhysicalPlanWithStats,
            InitialPhysicalPlanWithSchema(_) => PlanType::InitialPhysicalPlanWithSchema,
            OptimizedPhysicalPlan(OptimizedPhysicalPlanType { optimizer_name }) => {
                PlanType::OptimizedPhysicalPlan {
                    optimizer_name: optimizer_name.clone(),
                }
            }
            FinalPhysicalPlan(_) => PlanType::FinalPhysicalPlan,
            FinalPhysicalPlanWithStats(_) => PlanType::FinalPhysicalPlanWithStats,
            FinalPhysicalPlanWithSchema(_) => PlanType::FinalPhysicalPlanWithSchema,
            PhysicalPlanError(_) => PlanType::PhysicalPlanError,
        },
        plan: Arc::new(stringified_plan.plan.clone()),
    }
}

/// If this plan should be shown, given the previous plan that was
/// displayed.
///
/// This is meant to avoid repeating the same plan over and over again
/// in explain plans to make clear what is changing
fn should_show(previous_plan: &StringifiedPlan, this_plan: &StringifiedPlan) -> bool {
    // if the plans are different, or if they would have been
    // displayed in the normal explain (aka non verbose) plan
    (previous_plan.plan != this_plan.plan) || this_plan.should_display(false)
}
