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

//! Eager right-side variant of group join.
//!
//! This execution plan carries the optimizer's Strategy 1 choice. Execution
//! currently delegates to [`GroupJoinExec`], keeping semantics identical while
//! allowing the optimizer and plan display to distinguish the selected strategy.

use std::fmt;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::{JoinType, Result, Statistics};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_expr::aggregate::AggregateFunctionExpr;
use datafusion_physical_expr::{PhysicalExpr, PhysicalExprRef};

use crate::joins::group_join::GroupJoinExec;
use crate::metrics::MetricsSet;
use crate::{DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, PlanProperties};

/// A fused join + group-by operator selected when eager right-side aggregation
/// is expected to reduce the probe side before producing group join output.
#[derive(Debug)]
pub struct EagerRightGroupJoinExec {
    inner: GroupJoinExec,
}

impl EagerRightGroupJoinExec {
    /// Create a new `EagerRightGroupJoinExec`.
    pub fn try_new(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: Vec<(PhysicalExprRef, PhysicalExprRef)>,
        join_type: JoinType,
        group_by_exprs: Vec<(PhysicalExprRef, String)>,
        aggr_expr: Vec<Arc<AggregateFunctionExpr>>,
    ) -> Result<Self> {
        let aggr_input_schema = right.schema();
        Self::try_new_with_aggr_input_schema(
            left,
            right,
            on,
            join_type,
            group_by_exprs,
            aggr_expr,
            aggr_input_schema,
        )
    }

    /// Create a new `EagerRightGroupJoinExec` with the schema used to build
    /// aggregate expressions.
    pub fn try_new_with_aggr_input_schema(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: Vec<(PhysicalExprRef, PhysicalExprRef)>,
        join_type: JoinType,
        group_by_exprs: Vec<(PhysicalExprRef, String)>,
        aggr_expr: Vec<Arc<AggregateFunctionExpr>>,
        aggr_input_schema: SchemaRef,
    ) -> Result<Self> {
        Ok(Self {
            inner: GroupJoinExec::try_new_with_aggr_input_schema(
                left,
                right,
                on,
                join_type,
                group_by_exprs,
                aggr_expr,
                aggr_input_schema,
            )?,
        })
    }

    /// Build side input.
    pub fn left(&self) -> &Arc<dyn ExecutionPlan> {
        self.inner.left()
    }

    /// Probe side input.
    pub fn right(&self) -> &Arc<dyn ExecutionPlan> {
        self.inner.right()
    }

    /// Equi-join key expressions.
    pub fn on(&self) -> &[(PhysicalExprRef, PhysicalExprRef)] {
        self.inner.on()
    }

    /// Join type.
    pub fn join_type(&self) -> &JoinType {
        self.inner.join_type()
    }

    /// GROUP BY expressions with output aliases.
    pub fn group_by_exprs(&self) -> &[(PhysicalExprRef, String)] {
        self.inner.group_by_exprs()
    }

    /// Aggregate expressions.
    pub fn aggr_expr(&self) -> &[Arc<AggregateFunctionExpr>] {
        self.inner.aggr_expr()
    }

    /// Input schema used to build aggregate expressions.
    pub fn aggr_input_schema(&self) -> &SchemaRef {
        self.inner.aggr_input_schema()
    }
}

impl DisplayAs for EagerRightGroupJoinExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                let on: Vec<String> = self
                    .inner
                    .on()
                    .iter()
                    .map(|(l, r)| format!("({l}, {r})"))
                    .collect();
                let aggrs: Vec<String> = self
                    .inner
                    .aggr_expr()
                    .iter()
                    .map(|a| a.name().to_string())
                    .collect();
                write!(
                    f,
                    "EagerRightGroupJoinExec: join_type={:?}, on=[{}], aggr=[{}]",
                    self.inner.join_type(),
                    on.join(", "),
                    aggrs.join(", "),
                )
            }
            DisplayFormatType::TreeRender => {
                write!(f, "EagerRightGroupJoinExec")
            }
        }
    }
}

impl ExecutionPlan for EagerRightGroupJoinExec {
    fn name(&self) -> &'static str {
        "EagerRightGroupJoinExec"
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.inner.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.inner.children()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(
            EagerRightGroupJoinExec::try_new_with_aggr_input_schema(
                Arc::clone(&children[0]),
                Arc::clone(&children[1]),
                self.inner.on().to_vec(),
                *self.inner.join_type(),
                self.inner.group_by_exprs().to_vec(),
                self.inner.aggr_expr().to_vec(),
                Arc::clone(self.inner.aggr_input_schema()),
            )?,
        ))
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        self.inner.required_input_distribution()
    }

    fn metrics(&self) -> Option<MetricsSet> {
        self.inner.metrics()
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&dyn PhysicalExpr) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        self.inner.apply_expressions(f)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.inner.execute(partition, context)
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Arc<Statistics>> {
        self.inner.partition_statistics(partition)
    }
}
