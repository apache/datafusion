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
use std::fmt::Formatter;

use arrow_schema::SchemaRef;

use datafusion_common::display::{GraphvizBuilder, PlanType, StringifiedPlan};
use datafusion_expr::display_schema;
use datafusion_physical_expr::{LexOrdering, PhysicalSortExpr};

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
#[derive(Debug, Clone)]
pub struct DisplayableExecutionPlan<'a> {
    inner: &'a dyn ExecutionPlan,
    /// How to show metrics
    show_metrics: ShowMetrics,
    /// If statistics should be displayed
    show_statistics: bool,
    /// If schema should be displayed. See [`Self::set_show_schema`]
    show_schema: bool,
}

impl<'a> DisplayableExecutionPlan<'a> {
    /// Create a wrapper around an [`ExecutionPlan`] which can be
    /// pretty printed in a variety of ways
    pub fn new(inner: &'a dyn ExecutionPlan) -> Self {
        Self {
            inner,
            show_metrics: ShowMetrics::None,
            show_statistics: false,
            show_schema: false,
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
            show_schema: false,
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
            show_schema: false,
        }
    }

    /// Enable display of schema
    ///
    /// If true, plans will be displayed with schema information at the end
    /// of each line. The format is `schema=[[a:Int32;N, b:Int32;N, c:Int32;N]]`
    pub fn set_show_schema(mut self, show_schema: bool) -> Self {
        self.show_schema = show_schema;
        self
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
            show_schema: bool,
        }
        impl<'a> fmt::Display for Wrapper<'a> {
            fn fmt(&self, f: &mut Formatter) -> fmt::Result {
                let mut visitor = IndentVisitor {
                    t: self.format_type,
                    f,
                    indent: 0,
                    show_metrics: self.show_metrics,
                    show_statistics: self.show_statistics,
                    show_schema: self.show_schema,
                };
                accept(self.plan, &mut visitor)
            }
        }
        Wrapper {
            format_type,
            plan: self.inner,
            show_metrics: self.show_metrics,
            show_statistics: self.show_statistics,
            show_schema: self.show_schema,
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
            fn fmt(&self, f: &mut Formatter) -> fmt::Result {
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
            show_schema: bool,
        }

        impl<'a> fmt::Display for Wrapper<'a> {
            fn fmt(&self, f: &mut Formatter) -> fmt::Result {
                let mut visitor = IndentVisitor {
                    f,
                    t: DisplayFormatType::Default,
                    indent: 0,
                    show_metrics: self.show_metrics,
                    show_statistics: self.show_statistics,
                    show_schema: self.show_schema,
                };
                visitor.pre_visit(self.plan)?;
                Ok(())
            }
        }

        Wrapper {
            plan: self.inner,
            show_metrics: self.show_metrics,
            show_statistics: self.show_statistics,
            show_schema: self.show_schema,
        }
    }

    /// format as a `StringifiedPlan`
    pub fn to_stringified(&self, verbose: bool, plan_type: PlanType) -> StringifiedPlan {
        StringifiedPlan::new(plan_type, self.indent(verbose).to_string())
    }
}

/// Enum representing the different levels of metrics to display
#[derive(Debug, Clone, Copy)]
enum ShowMetrics {
    /// Do not show any metrics
    None,

    /// Show aggregated metrics across partition
    Aggregated,

    /// Show full per-partition metrics
    Full,
}

/// Formats plans with a single line per node.
///
/// # Example
///
/// ```text
/// ProjectionExec: expr=[column1@0 + 2 as column1 + Int64(2)]
///   FilterExec: column1@0 = 5
///     ValuesExec
/// ```
struct IndentVisitor<'a, 'b> {
    /// How to format each node
    t: DisplayFormatType,
    /// Write to this formatter
    f: &'a mut Formatter<'b>,
    /// Indent size
    indent: usize,
    /// How to show metrics
    show_metrics: ShowMetrics,
    /// If statistics should be displayed
    show_statistics: bool,
    /// If schema should be displayed
    show_schema: bool,
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
        if self.show_statistics {
            let stats = plan.statistics().map_err(|_e| fmt::Error)?;
            write!(self.f, ", statistics=[{}]", stats)?;
        }
        if self.show_schema {
            write!(
                self.f,
                ", schema={}",
                display_schema(plan.schema().as_ref())
            )?;
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
    f: &'a mut Formatter<'b>,
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

    fn pre_visit(&mut self, plan: &dyn ExecutionPlan) -> Result<bool, Self::Error> {
        let id = self.graphviz_builder.next_id();

        struct Wrapper<'a>(&'a dyn ExecutionPlan, DisplayFormatType);

        impl<'a> fmt::Display for Wrapper<'a> {
            fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
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

        let statistics = if self.show_statistics {
            let stats = plan.statistics().map_err(|_e| fmt::Error)?;
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
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> fmt::Result;
}

/// A newtype wrapper to display `T` implementing`DisplayAs` using the `Default` mode
pub struct DefaultDisplay<T>(pub T);

impl<T: DisplayAs> fmt::Display for DefaultDisplay<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.0.fmt_as(DisplayFormatType::Default, f)
    }
}

/// A newtype wrapper to display `T` implementing `DisplayAs` using the `Verbose` mode
pub struct VerboseDisplay<T>(pub T);

impl<T: DisplayAs> fmt::Display for VerboseDisplay<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.0.fmt_as(DisplayFormatType::Verbose, f)
    }
}

/// A wrapper to customize partitioned file display
#[derive(Debug)]
pub struct ProjectSchemaDisplay<'a>(pub &'a SchemaRef);

impl<'a> fmt::Display for ProjectSchemaDisplay<'a> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
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
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
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

pub fn display_orderings(f: &mut Formatter, orderings: &[LexOrdering]) -> fmt::Result {
    if let Some(ordering) = orderings.first() {
        if !ordering.is_empty() {
            let start = if orderings.len() == 1 {
                ", output_ordering="
            } else {
                ", output_orderings=["
            };
            write!(f, "{}", start)?;
            for (idx, ordering) in
                orderings.iter().enumerate().filter(|(_, o)| !o.is_empty())
            {
                match idx {
                    0 => write!(f, "{}", OutputOrderingDisplay(ordering))?,
                    _ => write!(f, ", {}", OutputOrderingDisplay(ordering))?,
                }
            }
            let end = if orderings.len() == 1 { "" } else { "]" };
            write!(f, "{}", end)?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fmt::Write;
    use std::sync::Arc;

    use datafusion_common::{DataFusionError, Result, Statistics};
    use datafusion_execution::{SendableRecordBatchStream, TaskContext};

    use crate::{DisplayAs, ExecutionPlan, PlanProperties};

    use super::DisplayableExecutionPlan;

    #[derive(Debug, Clone, Copy)]
    enum TestStatsExecPlan {
        Panic,
        Error,
        Ok,
    }

    impl DisplayAs for TestStatsExecPlan {
        fn fmt_as(
            &self,
            _t: crate::DisplayFormatType,
            f: &mut std::fmt::Formatter,
        ) -> std::fmt::Result {
            write!(f, "TestStatsExecPlan")
        }
    }

    impl ExecutionPlan for TestStatsExecPlan {
        fn name(&self) -> &'static str {
            "TestStatsExecPlan"
        }

        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn properties(&self) -> &PlanProperties {
            unimplemented!()
        }

        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![]
        }

        fn with_new_children(
            self: Arc<Self>,
            _: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            unimplemented!()
        }

        fn execute(
            &self,
            _: usize,
            _: Arc<TaskContext>,
        ) -> Result<SendableRecordBatchStream> {
            todo!()
        }

        fn statistics(&self) -> Result<Statistics> {
            match self {
                Self::Panic => panic!("expected panic"),
                Self::Error => {
                    Err(DataFusionError::Internal("expected error".to_string()))
                }
                Self::Ok => Ok(Statistics::new_unknown(self.schema().as_ref())),
            }
        }
    }

    fn test_stats_display(exec: TestStatsExecPlan, show_stats: bool) {
        let display =
            DisplayableExecutionPlan::new(&exec).set_show_statistics(show_stats);

        let mut buf = String::new();
        write!(&mut buf, "{}", display.one_line()).unwrap();
        let buf = buf.trim();
        assert_eq!(buf, "TestStatsExecPlan");
    }

    #[test]
    fn test_display_when_stats_panic_with_no_show_stats() {
        test_stats_display(TestStatsExecPlan::Panic, false);
    }

    #[test]
    fn test_display_when_stats_error_with_no_show_stats() {
        test_stats_display(TestStatsExecPlan::Error, false);
    }

    #[test]
    fn test_display_when_stats_ok_with_no_show_stats() {
        test_stats_display(TestStatsExecPlan::Ok, false);
    }

    #[test]
    #[should_panic(expected = "expected panic")]
    fn test_display_when_stats_panic_with_show_stats() {
        test_stats_display(TestStatsExecPlan::Panic, true);
    }

    #[test]
    #[should_panic(expected = "Error")] // fmt::Error
    fn test_display_when_stats_error_with_show_stats() {
        test_stats_display(TestStatsExecPlan::Error, true);
    }

    #[test]
    fn test_display_when_stats_ok_with_show_stats() {
        test_stats_display(TestStatsExecPlan::Ok, false);
    }
}
