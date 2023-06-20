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

use datafusion_common::display::StringifiedPlan;

use super::{accept, ExecutionPlan, ExecutionPlanVisitor};

/// Options for controlling how each [`ExecutionPlan`] should format itself
#[derive(Debug, Clone, Copy)]
pub enum DisplayFormatType {
    /// Default, compact format. Example: `FilterExec: c12 < 10.0`
    Default,
    /// Verbose, showing all available details
    Verbose,
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
    ///   CoalesceBatchesExec: target_batch_size=8192
    ///     FilterExec: a < 5
    ///       RepartitionExec: partitioning=RoundRobinBatch(16)
    ///         CsvExec: source=...",
    /// ```
    pub fn indent(&self, verbose: bool) -> impl fmt::Display + 'a {
        let format_type = if verbose {
            DisplayFormatType::Verbose
        } else {
            DisplayFormatType::Default
        };
        struct Wrapper<'a> {
            format_type: DisplayFormatType,
            plan: &'a dyn ExecutionPlan,
            show_metrics: ShowMetrics,
        }
        impl<'a> fmt::Display for Wrapper<'a> {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                let mut visitor = IndentVisitor {
                    t: self.format_type,
                    f,
                    indent: 0,
                    show_metrics: self.show_metrics,
                };
                accept(self.plan, &mut visitor)
            }
        }
        Wrapper {
            format_type,
            plan: self.inner,
            show_metrics: self.show_metrics,
        }
    }

    /// Return a single-line summary of the root of the plan
    /// Example: `ProjectionExec: expr=[a@0 as a]`.
    pub fn one_line(&self) -> impl fmt::Display + 'a {
        struct Wrapper<'a> {
            plan: &'a dyn ExecutionPlan,
            show_metrics: ShowMetrics,
        }

        impl<'a> fmt::Display for Wrapper<'a> {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                let mut visitor = IndentVisitor {
                    f,
                    t: DisplayFormatType::Default,
                    indent: 0,
                    show_metrics: self.show_metrics,
                };
                visitor.pre_visit(self.plan)?;
                Ok(())
            }
        }

        Wrapper {
            plan: self.inner,
            show_metrics: self.show_metrics,
        }
    }

    /// format as a `StringifiedPlan`
    pub fn to_stringified(
        &self,
        verbose: bool,
        plan_type: crate::logical_expr::PlanType,
    ) -> StringifiedPlan {
        StringifiedPlan::new(plan_type, self.indent(verbose).to_string())
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
    fn pre_visit(&mut self, plan: &dyn ExecutionPlan) -> Result<bool, Self::Error> {
        write!(self.f, "{:indent$}", "", indent = self.indent * 2)?;
        plan.fmt_as(self.t, self.f)?;
        match self.show_metrics {
            ShowMetrics::None => {}
            ShowMetrics::Aggregated => {
                if let Some(metrics) = plan.metrics() {
                    let metrics = metrics
                        .aggregate_by_name()
                        .sorted_for_display()
                        .timestamps_removed();

                    write!(self.f, ", metrics=[{metrics}]")?;
                } else {
                    write!(self.f, ", metrics=[]")?;
                }
            }
            ShowMetrics::Full => {
                if let Some(metrics) = plan.metrics() {
                    write!(self.f, ", metrics=[{metrics}]")?;
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

/// Trait for types which could have additional details when formatted in `Verbose` mode
pub trait DisplayAs {
    /// Format according to `DisplayFormatType`, used when verbose representation looks
    /// different from the default one
    ///
    /// Should not include a newline
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result;
}

/// A newtype wrapper to display `T` implementing`DisplayAs` using the `Default` mode
pub struct DefaultDisplay<T>(pub T);

impl<T: DisplayAs> fmt::Display for DefaultDisplay<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt_as(DisplayFormatType::Default, f)
    }
}

/// A newtype wrapper to display `T` implementing `DisplayAs` using the `Verbose` mode
pub struct VerboseDisplay<T>(pub T);

impl<T: DisplayAs> fmt::Display for VerboseDisplay<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt_as(DisplayFormatType::Verbose, f)
    }
}
