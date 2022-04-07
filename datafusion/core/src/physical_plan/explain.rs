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

use crate::{
    error::{DataFusionError, Result},
    logical_plan::StringifiedPlan,
    physical_plan::{
        common::SizedRecordBatchStream, DisplayFormatType, ExecutionPlan, Partitioning,
        Statistics,
    },
};
use arrow::{array::StringBuilder, datatypes::SchemaRef, record_batch::RecordBatch};

use super::{expressions::PhysicalSortExpr, SendableRecordBatchStream};
use crate::execution::context::TaskContext;
use crate::physical_plan::metrics::{ExecutionPlanMetricsSet, MemTrackingMetrics};
use async_trait::async_trait;

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
}

#[async_trait]
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

    fn relies_on_input_order(&self) -> bool {
        false
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    async fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if 0 != partition {
            return Err(DataFusionError::Internal(format!(
                "ExplainExec invalid partition {}",
                partition
            )));
        }

        let mut type_builder = StringBuilder::new(self.stringified_plans.len());
        let mut plan_builder = StringBuilder::new(self.stringified_plans.len());

        let plans_to_print = self
            .stringified_plans
            .iter()
            .filter(|s| s.should_display(self.verbose));

        // Identify plans that are not changed
        let mut prev: Option<&StringifiedPlan> = None;

        for p in plans_to_print {
            type_builder.append_value(p.plan_type.to_string())?;
            match prev {
                Some(prev) if !should_show(prev, p) => {
                    plan_builder.append_value("SAME TEXT AS ABOVE")?;
                }
                Some(_) | None => {
                    plan_builder.append_value(&*p.plan)?;
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

        let metrics = ExecutionPlanMetricsSet::new();
        let tracking_metrics = MemTrackingMetrics::new(&metrics, partition);

        Ok(Box::pin(SizedRecordBatchStream::new(
            self.schema.clone(),
            vec![Arc::new(record_batch)],
            tracking_metrics,
        )))
    }

    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(f, "ExplainExec")
            }
        }
    }

    fn statistics(&self) -> Statistics {
        // Statistics an EXPLAIN plan are not relevant
        Statistics::default()
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
