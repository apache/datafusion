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

use std::any::Any;
use std::sync::Arc;

use super::expressions::PhysicalSortExpr;
use super::{DisplayAs, SendableRecordBatchStream};
use crate::stream::RecordBatchStreamAdapter;
use crate::{DisplayFormatType, ExecutionPlan, Partitioning, Statistics};

use arrow::{array::StringBuilder, datatypes::SchemaRef, record_batch::RecordBatch};
use datafusion_common::display::StringifiedPlan;
use datafusion_common::{internal_err, DataFusionError, Result};
use datafusion_execution::TaskContext;

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
}

impl ExplainExec {
    /// Create a new ExplainExec
    pub fn new(
        schema: SchemaRef,
        stringified_plans: Vec<StringifiedPlan>,
        verbose: bool,
    ) -> Self {
        ExplainExec {
            schema,
            stringified_plans,
            verbose,
        }
    }

    /// The strings to be printed
    pub fn stringified_plans(&self) -> &[StringifiedPlan] {
        &self.stringified_plans
    }

    /// access to verbose
    pub fn verbose(&self) -> bool {
        self.verbose
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
        }
    }
}

impl ExecutionPlan for ExplainExec {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        // this is a leaf node and has no children
        vec![]
    }

    /// Get the output partitioning of this plan
    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
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
        trace!("Start ExplainExec::execute for partition {} of context session_id {} and task_id {:?}", partition, context.session_id(), context.task_id());
        if 0 != partition {
            return internal_err!("ExplainExec invalid partition {partition}");
        }

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
            self.schema.clone(),
            vec![
                Arc::new(type_builder.finish()),
                Arc::new(plan_builder.finish()),
            ],
        )?;

        trace!(
            "Before returning RecordBatchStream in ExplainExec::execute for partition {} of context session_id {} and task_id {:?}", partition, context.session_id(), context.task_id());

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            futures::stream::iter(vec![Ok(record_batch)]),
        )))
    }

    fn statistics(&self) -> Result<Statistics> {
        // Statistics an EXPLAIN plan are not relevant
        Ok(Statistics::new_unknown(&self.schema()))
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
