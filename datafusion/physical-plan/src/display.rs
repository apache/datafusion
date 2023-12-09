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
//! [`crate::displayable`] for examples of how to format

use std::fmt;

use super::{accept, ExecutionPlan, ExecutionPlanVisitor};

use arrow_schema::SchemaRef;
use datafusion_common::display::{GraphvizBuilder, PlanType, StringifiedPlan};
use datafusion_physical_expr::PhysicalSortExpr;

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
    /// If statistics should be displayed
    show_statistics: bool,
}

impl<'a> DisplayableExecutionPlan<'a> {
    /// Create a wrapper around an [`ExecutionPlan`] which can be
    /// pretty printed in a variety of ways
    pub fn new(inner: &'a dyn ExecutionPlan) -> Self {
        Self {
            inner,
            show_metrics: ShowMetrics::None,
            show_statistics: false,
        }
    }

    /// Create a wrapper around an [`ExecutionPlan`] which can be
    /// pretty printed in a variety of ways that also shows aggregated
    /// metrics
    pub fn with_metrics(inner: &'a dyn ExecutionPlan) -> Self {
        Self {
            inner,
            show_metrics: ShowMetrics::Aggregated,
            show_statistics: false,
        }
    }

    /// Create a wrapper around an [`ExecutionPlan`] which can be
    /// pretty printed in a variety of ways that also shows all low
    /// level metrics
    pub fn with_full_metrics(inner: &'a dyn ExecutionPlan) -> Self {
        Self {
            inner,
            show_metrics: ShowMetrics::Full,
            show_statistics: false,
        }
    }

    /// Enable display of statistics
    pub fn set_show_statistics(mut self, show_statistics: bool) -> Self {
        self.show_statistics = show_statistics;
        self
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
            show_statistics: bool,
        }
        impl<'a> fmt::Display for Wrapper<'a> {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                let mut visitor = IndentVisitor {
                    t: self.format_type,
                    f,
                    indent: 0,
                    show_metrics: self.show_metrics,
                    show_statistics: self.show_statistics,
                };
                accept(self.plan, &mut visitor)
            }
        }
        Wrapper {
            format_type,
            plan: self.inner,
            show_metrics: self.show_metrics,
            show_statistics: self.show_statistics,
        }
    }

    /// Returns a `format`able structure that produces graphviz format for execution plan, which can
    /// be directly visualized [here](https://dreampuf.github.io/GraphvizOnline).
    ///
    /// An example is
    /// ```dot
    /// strict digraph dot_plan {
    //     0[label="ProjectionExec: expr=[id@0 + 2 as employee.id + Int32(2)]",tooltip=""]
    //     1[label="EmptyExec",tooltip=""]
    //     0 -> 1
    // }
    /// ```
    pub fn graphviz(&self) -> impl fmt::Display + 'a {
        struct Wrapper<'a> {
            plan: &'a dyn ExecutionPlan,
            show_metrics: ShowMetrics,
            show_statistics: bool,
        }
        impl<'a> fmt::Display for Wrapper<'a> {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                let t = DisplayFormatType::Default;

                let mut visitor = GraphvizVisitor {
                    f,
                    t,
                    show_metrics: self.show_metrics,
                    show_statistics: self.show_statistics,
                    graphviz_builder: GraphvizBuilder::default(),
                    parents: Vec::new(),
                };

                visitor.start_graph()?;

                accept(self.plan, &mut visitor)?;

                visitor.end_graph()?;
                Ok(())
            }
        }

        Wrapper {
            plan: self.inner,
            show_metrics: self.show_metrics,
            show_statistics: self.show_statistics,
        }
    }

    /// Return a single-line summary of the root of the plan
    /// Example: `ProjectionExec: expr=[a@0 as a]`.
    pub fn one_line(&self) -> impl fmt::Display + 'a {
        struct Wrapper<'a> {
            plan: &'a dyn ExecutionPlan,
            show_metrics: ShowMetrics,
            show_statistics: bool,
        }

        impl<'a> fmt::Display for Wrapper<'a> {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                let mut visitor = IndentVisitor {
                    f,
                    t: DisplayFormatType::Default,
                    indent: 0,
                    show_metrics: self.show_metrics,
                    show_statistics: self.show_statistics,
                };
                visitor.pre_visit(self.plan)?;
                Ok(())
            }
        }

        Wrapper {
            plan: self.inner,
            show_metrics: self.show_metrics,
            show_statistics: self.show_statistics,
        }
    }

    /// format as a `StringifiedPlan`
    pub fn to_stringified(&self, verbose: bool, plan_type: PlanType) -> StringifiedPlan {
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
    /// If statistics should be displayed
    show_statistics: bool,
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
        let stats = plan.statistics().map_err(|_e| fmt::Error)?;
        if self.show_statistics {
            write!(self.f, ", statistics=[{}]", stats)?;
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

struct GraphvizVisitor<'a, 'b> {
    f: &'a mut fmt::Formatter<'b>,
    /// How to format each node
    t: DisplayFormatType,
    /// How to show metrics
    show_metrics: ShowMetrics,
    /// If statistics should be displayed
    show_statistics: bool,

    graphviz_builder: GraphvizBuilder,
    /// Used to record parent node ids when visiting a plan.
    parents: Vec<usize>,
}

impl GraphvizVisitor<'_, '_> {
    fn start_graph(&mut self) -> fmt::Result {
        self.graphviz_builder.start_graph(self.f)
    }

    fn end_graph(&mut self) -> fmt::Result {
        self.graphviz_builder.end_graph(self.f)
    }
}

impl ExecutionPlanVisitor for GraphvizVisitor<'_, '_> {
    type Error = fmt::Error;

    fn pre_visit(
        &mut self,
        plan: &dyn ExecutionPlan,
    ) -> datafusion_common::Result<bool, Self::Error> {
        let id = self.graphviz_builder.next_id();

        struct Wrapper<'a>(&'a dyn ExecutionPlan, DisplayFormatType);

        impl<'a> std::fmt::Display for Wrapper<'a> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt_as(self.1, f)
            }
        }

        let label = { format!("{}", Wrapper(plan, self.t)) };

        let metrics = match self.show_metrics {
            ShowMetrics::None => "".to_string(),
            ShowMetrics::Aggregated => {
                if let Some(metrics) = plan.metrics() {
                    let metrics = metrics
                        .aggregate_by_name()
                        .sorted_for_display()
                        .timestamps_removed();

                    format!("metrics=[{metrics}]")
                } else {
                    "metrics=[]".to_string()
                }
            }
            ShowMetrics::Full => {
                if let Some(metrics) = plan.metrics() {
                    format!("metrics=[{metrics}]")
                } else {
                    "metrics=[]".to_string()
                }
            }
        };

        let stats = plan.statistics().map_err(|_e| fmt::Error)?;
        let statistics = if self.show_statistics {
            format!("statistics=[{}]", stats)
        } else {
            "".to_string()
        };

        let delimiter = if !metrics.is_empty() && !statistics.is_empty() {
            ", "
        } else {
            ""
        };

        self.graphviz_builder.add_node(
            self.f,
            id,
            &label,
            Some(&format!("{}{}{}", metrics, delimiter, statistics)),
        )?;

        if let Some(parent_node_id) = self.parents.last() {
            self.graphviz_builder
                .add_edge(self.f, *parent_node_id, id)?;
        }

        self.parents.push(id);

        Ok(true)
    }

    fn post_visit(&mut self, _plan: &dyn ExecutionPlan) -> Result<bool, Self::Error> {
        self.parents.pop();
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

/// A wrapper to customize partitioned file display
#[derive(Debug)]
pub struct ProjectSchemaDisplay<'a>(pub &'a SchemaRef);

impl<'a> fmt::Display for ProjectSchemaDisplay<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let parts: Vec<_> = self
            .0
            .fields()
            .iter()
            .map(|x| x.name().to_owned())
            .collect::<Vec<String>>();
        write!(f, "[{}]", parts.join(", "))
    }
}

/// A wrapper to customize output ordering display.
#[derive(Debug)]
pub struct OutputOrderingDisplay<'a>(pub &'a [PhysicalSortExpr]);

impl<'a> fmt::Display for OutputOrderingDisplay<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "[")?;
        for (i, e) in self.0.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?
            }
            write!(f, "{e}")?;
        }
        write!(f, "]")
    }
}
