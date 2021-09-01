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

//! Implementation of physical plan display. See
//! [`crate::physical_plan::displayable`] for examples of how to
//! format

use std::fmt;

use crate::logical_plan::{StringifiedPlan, ToStringifiedPlan};

use super::{accept, ExecutionPlan, ExecutionPlanVisitor};

/// Options for controlling how each [`ExecutionPlan`] should format itself
#[derive(Debug, Clone, Copy)]
pub enum DisplayFormatType {
    /// Default, compact format. Example: `FilterExec: c12 < 10.0`
    Default,
}

/// Wraps an `ExecutionPlan` with various ways to display this plan
pub struct DisplayableExecutionPlan<'a> {
    inner: &'a dyn ExecutionPlan,
    /// How to show metrics
    show_metrics: ShowMetrics,
}

impl<'a> DisplayableExecutionPlan<'a> {
    /// Create a wrapper around an [`'ExecutionPlan'] which can be
    /// pretty printed in a variety of ways
    pub fn new(inner: &'a dyn ExecutionPlan) -> Self {
        Self {
            inner,
            show_metrics: ShowMetrics::None,
        }
    }

    /// Create a wrapper around an [`'ExecutionPlan'] which can be
    /// pretty printed in a variety of ways that also shows aggregated
    /// metrics
    pub fn with_metrics(inner: &'a dyn ExecutionPlan) -> Self {
        Self {
            inner,
            show_metrics: ShowMetrics::Aggregated,
        }
    }

    /// Create a wrapper around an [`'ExecutionPlan'] which can be
    /// pretty printed in a variety of ways that also shows all low
    /// level metrics
    pub fn with_full_metrics(inner: &'a dyn ExecutionPlan) -> Self {
        Self {
            inner,
            show_metrics: ShowMetrics::Full,
        }
    }

    /// Return a `format`able structure that produces a single line
    /// per node.
    ///
    /// ```text
    /// ProjectionExec: expr=[a]
    ///   CoalesceBatchesExec: target_batch_size=4096
    ///     FilterExec: a < 5
    ///       RepartitionExec: partitioning=RoundRobinBatch(16)
    ///         CsvExec: source=...",
    /// ```
    pub fn indent(&self) -> impl fmt::Display + 'a {
        struct Wrapper<'a> {
            plan: &'a dyn ExecutionPlan,
            show_metrics: ShowMetrics,
        }
        impl<'a> fmt::Display for Wrapper<'a> {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                let t = DisplayFormatType::Default;
                let mut visitor = IndentVisitor {
                    t,
                    f,
                    indent: 0,
                    show_metrics: self.show_metrics,
                };
                accept(self.plan, &mut visitor)
            }
        }
        Wrapper {
            plan: self.inner,
            show_metrics: self.show_metrics,
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum ShowMetrics {
    /// Do not show any metrics
    None,

    /// Show aggregrated metrics across partition
    Aggregated,

    /// Show full per-partition metrics
    Full,
}

/// Formats plans with a single line per node.
struct IndentVisitor<'a, 'b> {
    /// How to format each node
    t: DisplayFormatType,
    /// Write to this formatter
    f: &'a mut fmt::Formatter<'b>,
    /// Indent size
    indent: usize,
    /// How to show metrics
    show_metrics: ShowMetrics,
}

impl<'a, 'b> ExecutionPlanVisitor for IndentVisitor<'a, 'b> {
    type Error = fmt::Error;
    fn pre_visit(
        &mut self,
        plan: &dyn ExecutionPlan,
    ) -> std::result::Result<bool, Self::Error> {
        write!(self.f, "{:indent$}", "", indent = self.indent * 2)?;
        plan.fmt_as(self.t, self.f)?;
        match self.show_metrics {
            ShowMetrics::None => {}
            ShowMetrics::Aggregated => {
                if let Some(metrics) = plan.metrics() {
                    let metrics = metrics
                        .aggregate_by_partition()
                        .sorted_for_display()
                        .timestamps_removed();

                    write!(self.f, ", metrics=[{}]", metrics)?;
                } else {
                    write!(self.f, ", metrics=[]")?;
                }
            }
            ShowMetrics::Full => {
                if let Some(metrics) = plan.metrics() {
                    write!(self.f, ", metrics=[{}]", metrics)?;
                } else {
                    write!(self.f, ", metrics=[]")?;
                }
            }
        }
        writeln!(self.f)?;
        self.indent += 1;
        Ok(true)
    }

    fn post_visit(&mut self, _plan: &dyn ExecutionPlan) -> Result<bool, Self::Error> {
        self.indent -= 1;
        Ok(true)
    }
}

impl<'a> ToStringifiedPlan for DisplayableExecutionPlan<'a> {
    fn to_stringified(
        &self,
        plan_type: crate::logical_plan::PlanType,
    ) -> StringifiedPlan {
        StringifiedPlan::new(plan_type, self.indent().to_string())
    }
}
