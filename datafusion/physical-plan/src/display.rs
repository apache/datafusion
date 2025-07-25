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

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::fmt::Formatter;

use arrow::datatypes::SchemaRef;

use datafusion_common::display::{GraphvizBuilder, PlanType, StringifiedPlan};
use datafusion_expr::display_schema;
use datafusion_physical_expr::LexOrdering;

use crate::render_tree::RenderTree;

use super::{accept, ExecutionPlan, ExecutionPlanVisitor};

/// Options for controlling how each [`ExecutionPlan`] should format itself
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DisplayFormatType {
    /// Default, compact format. Example: `FilterExec: c12 < 10.0`
    ///
    /// This format is designed to provide a detailed textual description
    /// of all parts of the plan.
    Default,
    /// Verbose, showing all available details.
    ///
    /// This form is even more detailed than [`Self::Default`]
    Verbose,
    /// TreeRender, displayed in the `tree` explain type.
    ///
    /// This format is inspired by DuckDB's explain plans. The information
    /// presented should be "user friendly", and contain only the most relevant
    /// information for understanding a plan. It should NOT contain the same level
    /// of detail information as the  [`Self::Default`] format.
    ///
    /// In this mode, each line has one of two formats:
    ///
    /// 1. A string without a `=`, which is printed in its own line
    ///
    /// 2. A string with a `=` that is treated as a `key=value pair`. Everything
    ///    before the first `=` is treated as the key, and everything after the
    ///    first `=` is treated as the value.
    ///
    /// For example, if the output of `TreeRender` is this:
    /// ```text
    /// Parquet
    /// partition_sizes=[1]
    /// ```
    ///
    /// It is rendered in the center of a box in the following way:
    ///
    /// ```text
    /// ┌───────────────────────────┐
    /// │       DataSourceExec      │
    /// │    --------------------   │
    /// │    partition_sizes: [1]   │
    /// │          Parquet          │
    /// └───────────────────────────┘
    ///  ```
    TreeRender,
}

/// Wraps an `ExecutionPlan` with various methods for formatting
///
///
/// # Example
/// ```
/// # use std::sync::Arc;
/// # use arrow::datatypes::{Field, Schema, DataType};
/// # use datafusion_expr::Operator;
/// # use datafusion_physical_expr::expressions::{binary, col, lit};
/// # use datafusion_physical_plan::{displayable, ExecutionPlan};
/// # use datafusion_physical_plan::empty::EmptyExec;
/// # use datafusion_physical_plan::filter::FilterExec;
/// # let schema = Schema::new(vec![Field::new("i", DataType::Int32, false)]);
/// # let plan = EmptyExec::new(Arc::new(schema));
/// # let i = col("i", &plan.schema()).unwrap();
/// # let predicate = binary(i, Operator::Eq, lit(1), &plan.schema()).unwrap();
/// # let plan: Arc<dyn ExecutionPlan> = Arc::new(FilterExec::try_new(predicate, Arc::new(plan)).unwrap());
/// // Get a one line description (Displayable)
/// let display_plan = displayable(plan.as_ref());
///
/// // you can use the returned objects to format plans
/// // where you can use `Display` such as  format! or println!
/// assert_eq!(
///    &format!("The plan is: {}", display_plan.one_line()),
///   "The plan is: FilterExec: i@0 = 1\n"
/// );
/// // You can also print out the plan and its children in indented mode
/// assert_eq!(display_plan.indent(false).to_string(),
///   "FilterExec: i@0 = 1\
///   \n  EmptyExec\
///   \n"
/// );
/// ```
#[derive(Debug, Clone)]
pub struct DisplayableExecutionPlan<'a> {
    inner: &'a dyn ExecutionPlan,
    /// How to show metrics
    show_metrics: ShowMetrics,
    /// If statistics should be displayed
    show_statistics: bool,
    /// If schema should be displayed. See [`Self::set_show_schema`]
    show_schema: bool,
    // (TreeRender) Maximum total width of the rendered tree
    tree_maximum_render_width: usize,
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
            tree_maximum_render_width: 240,
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
            tree_maximum_render_width: 240,
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
            tree_maximum_render_width: 240,
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

    /// Set the maximum render width for the tree format
    pub fn set_tree_maximum_render_width(mut self, width: usize) -> Self {
        self.tree_maximum_render_width = width;
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
    ///         DataSourceExec: source=...",
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
        impl fmt::Display for Wrapper<'_> {
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
        impl fmt::Display for Wrapper<'_> {
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

    /// Formats the plan using a ASCII art like tree
    ///
    /// See [`DisplayFormatType::TreeRender`] for more details.
    pub fn tree_render(&self) -> impl fmt::Display + 'a {
        struct Wrapper<'a> {
            plan: &'a dyn ExecutionPlan,
            maximum_render_width: usize,
        }
        impl fmt::Display for Wrapper<'_> {
            fn fmt(&self, f: &mut Formatter) -> fmt::Result {
                let mut visitor = TreeRenderVisitor {
                    f,
                    maximum_render_width: self.maximum_render_width,
                };
                visitor.visit(self.plan)
            }
        }
        Wrapper {
            plan: self.inner,
            maximum_render_width: self.tree_maximum_render_width,
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

        impl fmt::Display for Wrapper<'_> {
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

    #[deprecated(since = "47.0.0", note = "indent() or tree_render() instead")]
    pub fn to_stringified(
        &self,
        verbose: bool,
        plan_type: PlanType,
        explain_format: DisplayFormatType,
    ) -> StringifiedPlan {
        match (&explain_format, &plan_type) {
            (DisplayFormatType::TreeRender, PlanType::FinalPhysicalPlan) => {
                StringifiedPlan::new(plan_type, self.tree_render().to_string())
            }
            _ => StringifiedPlan::new(plan_type, self.indent(verbose).to_string()),
        }
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

impl ExecutionPlanVisitor for IndentVisitor<'_, '_> {
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
            let stats = plan.partition_statistics(None).map_err(|_e| fmt::Error)?;
            write!(self.f, ", statistics=[{stats}]")?;
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

        impl fmt::Display for Wrapper<'_> {
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
            let stats = plan.partition_statistics(None).map_err(|_e| fmt::Error)?;
            format!("statistics=[{stats}]")
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
            Some(&format!("{metrics}{delimiter}{statistics}")),
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

/// This module implements a tree-like art renderer for execution plans,
/// based on DuckDB's implementation:
/// <https://github.com/duckdb/duckdb/blob/main/src/include/duckdb/common/tree_renderer/text_tree_renderer.hpp>
///
/// The rendered output looks like this:
/// ```text
/// ┌───────────────────────────┐
/// │    CoalesceBatchesExec    │
/// └─────────────┬─────────────┘
/// ┌─────────────┴─────────────┐
/// │        HashJoinExec       ├──────────────┐
/// └─────────────┬─────────────┘              │
/// ┌─────────────┴─────────────┐┌─────────────┴─────────────┐
/// │       DataSourceExec      ││       DataSourceExec      │
/// └───────────────────────────┘└───────────────────────────┘
/// ```
///
/// The renderer uses a three-layer approach for each node:
/// 1. Top layer: renders the top borders and connections
/// 2. Content layer: renders the node content and vertical connections
/// 3. Bottom layer: renders the bottom borders and connections
///
/// Each node is rendered in a box of fixed width (NODE_RENDER_WIDTH).
struct TreeRenderVisitor<'a, 'b> {
    /// Write to this formatter
    f: &'a mut Formatter<'b>,
    /// Maximum total width of the rendered tree
    maximum_render_width: usize,
}

impl TreeRenderVisitor<'_, '_> {
    // Unicode box-drawing characters for creating borders and connections.
    const LTCORNER: &'static str = "┌"; // Left top corner
    const RTCORNER: &'static str = "┐"; // Right top corner
    const LDCORNER: &'static str = "└"; // Left bottom corner
    const RDCORNER: &'static str = "┘"; // Right bottom corner

    const TMIDDLE: &'static str = "┬"; // Top T-junction (connects down)
    const LMIDDLE: &'static str = "├"; // Left T-junction (connects right)
    const DMIDDLE: &'static str = "┴"; // Bottom T-junction (connects up)

    const VERTICAL: &'static str = "│"; // Vertical line
    const HORIZONTAL: &'static str = "─"; // Horizontal line

    // TODO: Make these variables configurable.
    const NODE_RENDER_WIDTH: usize = 29; // Width of each node's box
    const MAX_EXTRA_LINES: usize = 30; // Maximum number of extra info lines per node

    /// Main entry point for rendering an execution plan as a tree.
    /// The rendering process happens in three stages for each level of the tree:
    /// 1. Render top borders and connections
    /// 2. Render node content and vertical connections
    /// 3. Render bottom borders and connections
    pub fn visit(&mut self, plan: &dyn ExecutionPlan) -> Result<(), fmt::Error> {
        let root = RenderTree::create_tree(plan);

        for y in 0..root.height {
            // Start by rendering the top layer.
            self.render_top_layer(&root, y)?;
            // Now we render the content of the boxes
            self.render_box_content(&root, y)?;
            // Render the bottom layer of each of the boxes
            self.render_bottom_layer(&root, y)?;
        }

        Ok(())
    }

    /// Renders the top layer of boxes at the given y-level of the tree.
    /// This includes:
    /// - Top corners (┌─┐) for nodes
    /// - Horizontal connections between nodes
    /// - Vertical connections to parent nodes
    fn render_top_layer(
        &mut self,
        root: &RenderTree,
        y: usize,
    ) -> Result<(), fmt::Error> {
        for x in 0..root.width {
            if self.maximum_render_width > 0
                && x * Self::NODE_RENDER_WIDTH >= self.maximum_render_width
            {
                break;
            }

            if root.has_node(x, y) {
                write!(self.f, "{}", Self::LTCORNER)?;
                write!(
                    self.f,
                    "{}",
                    Self::HORIZONTAL.repeat(Self::NODE_RENDER_WIDTH / 2 - 1)
                )?;
                if y == 0 {
                    // top level node: no node above this one
                    write!(self.f, "{}", Self::HORIZONTAL)?;
                } else {
                    // render connection to node above this one
                    write!(self.f, "{}", Self::DMIDDLE)?;
                }
                write!(
                    self.f,
                    "{}",
                    Self::HORIZONTAL.repeat(Self::NODE_RENDER_WIDTH / 2 - 1)
                )?;
                write!(self.f, "{}", Self::RTCORNER)?;
            } else {
                let mut has_adjacent_nodes = false;
                for i in 0..(root.width - x) {
                    has_adjacent_nodes = has_adjacent_nodes || root.has_node(x + i, y);
                }
                if !has_adjacent_nodes {
                    // There are no nodes to the right side of this position
                    // no need to fill the empty space
                    continue;
                }
                // there are nodes next to this, fill the space
                write!(self.f, "{}", &" ".repeat(Self::NODE_RENDER_WIDTH))?;
            }
        }
        writeln!(self.f)?;

        Ok(())
    }

    /// Renders the content layer of boxes at the given y-level of the tree.
    /// This includes:
    /// - Node names and extra information
    /// - Vertical borders (│) for boxes
    /// - Vertical connections between nodes
    fn render_box_content(
        &mut self,
        root: &RenderTree,
        y: usize,
    ) -> Result<(), fmt::Error> {
        let mut extra_info: Vec<Vec<String>> = vec![vec![]; root.width];
        let mut extra_height = 0;

        for (x, extra_info_item) in extra_info.iter_mut().enumerate().take(root.width) {
            if let Some(node) = root.get_node(x, y) {
                Self::split_up_extra_info(
                    &node.extra_text,
                    extra_info_item,
                    Self::MAX_EXTRA_LINES,
                );
                if extra_info_item.len() > extra_height {
                    extra_height = extra_info_item.len();
                }
            }
        }

        let halfway_point = extra_height.div_ceil(2);

        // Render the actual node.
        for render_y in 0..=extra_height {
            for (x, _) in root.nodes.iter().enumerate().take(root.width) {
                if self.maximum_render_width > 0
                    && x * Self::NODE_RENDER_WIDTH >= self.maximum_render_width
                {
                    break;
                }

                let mut has_adjacent_nodes = false;
                for i in 0..(root.width - x) {
                    has_adjacent_nodes = has_adjacent_nodes || root.has_node(x + i, y);
                }

                if let Some(node) = root.get_node(x, y) {
                    write!(self.f, "{}", Self::VERTICAL)?;

                    // Rigure out what to render.
                    let mut render_text = String::new();
                    if render_y == 0 {
                        render_text = node.name.clone();
                    } else if render_y <= extra_info[x].len() {
                        render_text = extra_info[x][render_y - 1].clone();
                    }

                    render_text = Self::adjust_text_for_rendering(
                        &render_text,
                        Self::NODE_RENDER_WIDTH - 2,
                    );
                    write!(self.f, "{render_text}")?;

                    if render_y == halfway_point && node.child_positions.len() > 1 {
                        write!(self.f, "{}", Self::LMIDDLE)?;
                    } else {
                        write!(self.f, "{}", Self::VERTICAL)?;
                    }
                } else if render_y == halfway_point {
                    let has_child_to_the_right =
                        Self::should_render_whitespace(root, x, y);
                    if root.has_node(x, y + 1) {
                        // Node right below this one.
                        write!(
                            self.f,
                            "{}",
                            Self::HORIZONTAL.repeat(Self::NODE_RENDER_WIDTH / 2)
                        )?;
                        if has_child_to_the_right {
                            write!(self.f, "{}", Self::TMIDDLE)?;
                            // Have another child to the right, Keep rendering the line.
                            write!(
                                self.f,
                                "{}",
                                Self::HORIZONTAL.repeat(Self::NODE_RENDER_WIDTH / 2)
                            )?;
                        } else {
                            write!(self.f, "{}", Self::RTCORNER)?;
                            if has_adjacent_nodes {
                                // Only a child below this one: fill the reset with spaces.
                                write!(
                                    self.f,
                                    "{}",
                                    " ".repeat(Self::NODE_RENDER_WIDTH / 2)
                                )?;
                            }
                        }
                    } else if has_child_to_the_right {
                        // Child to the right, but no child right below this one: render a full
                        // line.
                        write!(
                            self.f,
                            "{}",
                            Self::HORIZONTAL.repeat(Self::NODE_RENDER_WIDTH)
                        )?;
                    } else if has_adjacent_nodes {
                        // Empty spot: render spaces.
                        write!(self.f, "{}", " ".repeat(Self::NODE_RENDER_WIDTH))?;
                    }
                } else if render_y >= halfway_point {
                    if root.has_node(x, y + 1) {
                        // Have a node below this empty spot: render a vertical line.
                        write!(
                            self.f,
                            "{}{}",
                            " ".repeat(Self::NODE_RENDER_WIDTH / 2),
                            Self::VERTICAL
                        )?;
                        if has_adjacent_nodes
                            || Self::should_render_whitespace(root, x, y)
                        {
                            write!(
                                self.f,
                                "{}",
                                " ".repeat(Self::NODE_RENDER_WIDTH / 2)
                            )?;
                        }
                    } else if has_adjacent_nodes
                        || Self::should_render_whitespace(root, x, y)
                    {
                        // Empty spot: render spaces.
                        write!(self.f, "{}", " ".repeat(Self::NODE_RENDER_WIDTH))?;
                    }
                } else if has_adjacent_nodes {
                    // Empty spot: render spaces.
                    write!(self.f, "{}", " ".repeat(Self::NODE_RENDER_WIDTH))?;
                }
            }
            writeln!(self.f)?;
        }

        Ok(())
    }

    /// Renders the bottom layer of boxes at the given y-level of the tree.
    /// This includes:
    /// - Bottom corners (└─┘) for nodes
    /// - Horizontal connections between nodes
    /// - Vertical connections to child nodes
    fn render_bottom_layer(
        &mut self,
        root: &RenderTree,
        y: usize,
    ) -> Result<(), fmt::Error> {
        for x in 0..=root.width {
            if self.maximum_render_width > 0
                && x * Self::NODE_RENDER_WIDTH >= self.maximum_render_width
            {
                break;
            }
            let mut has_adjacent_nodes = false;
            for i in 0..(root.width - x) {
                has_adjacent_nodes = has_adjacent_nodes || root.has_node(x + i, y);
            }
            if root.get_node(x, y).is_some() {
                write!(self.f, "{}", Self::LDCORNER)?;
                write!(
                    self.f,
                    "{}",
                    Self::HORIZONTAL.repeat(Self::NODE_RENDER_WIDTH / 2 - 1)
                )?;
                if root.has_node(x, y + 1) {
                    // node below this one: connect to that one
                    write!(self.f, "{}", Self::TMIDDLE)?;
                } else {
                    // no node below this one: end the box
                    write!(self.f, "{}", Self::HORIZONTAL)?;
                }
                write!(
                    self.f,
                    "{}",
                    Self::HORIZONTAL.repeat(Self::NODE_RENDER_WIDTH / 2 - 1)
                )?;
                write!(self.f, "{}", Self::RDCORNER)?;
            } else if root.has_node(x, y + 1) {
                write!(self.f, "{}", &" ".repeat(Self::NODE_RENDER_WIDTH / 2))?;
                write!(self.f, "{}", Self::VERTICAL)?;
                if has_adjacent_nodes || Self::should_render_whitespace(root, x, y) {
                    write!(self.f, "{}", &" ".repeat(Self::NODE_RENDER_WIDTH / 2))?;
                }
            } else if has_adjacent_nodes || Self::should_render_whitespace(root, x, y) {
                write!(self.f, "{}", &" ".repeat(Self::NODE_RENDER_WIDTH))?;
            }
        }
        writeln!(self.f)?;

        Ok(())
    }

    fn extra_info_separator() -> String {
        "-".repeat(Self::NODE_RENDER_WIDTH - 9)
    }

    fn remove_padding(s: &str) -> String {
        s.trim().to_string()
    }

    pub fn split_up_extra_info(
        extra_info: &HashMap<String, String>,
        result: &mut Vec<String>,
        max_lines: usize,
    ) {
        if extra_info.is_empty() {
            return;
        }

        result.push(Self::extra_info_separator());

        let mut requires_padding = false;
        let mut was_inlined = false;

        // use BTreeMap for repeatable key order
        let sorted_extra_info: BTreeMap<_, _> = extra_info.iter().collect();
        for (key, value) in sorted_extra_info {
            let mut str = Self::remove_padding(value);
            let mut is_inlined = false;
            let available_width = Self::NODE_RENDER_WIDTH - 7;
            let total_size = key.len() + str.len() + 2;
            let is_multiline = str.contains('\n');

            if str.is_empty() {
                str = key.to_string();
            } else if !is_multiline && total_size < available_width {
                str = format!("{key}: {str}");
                is_inlined = true;
            } else {
                str = format!("{key}:\n{str}");
            }

            if is_inlined && was_inlined {
                requires_padding = false;
            }

            if requires_padding {
                result.push(String::new());
            }

            let mut splits: Vec<String> = str.split('\n').map(String::from).collect();
            if splits.len() > max_lines {
                let mut truncated_splits = Vec::new();
                for split in splits.iter().take(max_lines / 2) {
                    truncated_splits.push(split.clone());
                }
                truncated_splits.push("...".to_string());
                for split in splits.iter().skip(splits.len() - max_lines / 2) {
                    truncated_splits.push(split.clone());
                }
                splits = truncated_splits;
            }
            for split in splits {
                Self::split_string_buffer(&split, result);
            }
            if result.len() > max_lines {
                result.truncate(max_lines);
                result.push("...".to_string());
            }

            requires_padding = true;
            was_inlined = is_inlined;
        }
    }

    /// Adjusts text to fit within the specified width by:
    /// 1. Truncating with ellipsis if too long
    /// 2. Center-aligning within the available space if shorter
    fn adjust_text_for_rendering(source: &str, max_render_width: usize) -> String {
        let render_width = source.chars().count();
        if render_width > max_render_width {
            let truncated = &source[..max_render_width - 3];
            format!("{truncated}...")
        } else {
            let total_spaces = max_render_width - render_width;
            let half_spaces = total_spaces / 2;
            let extra_left_space = if total_spaces % 2 == 0 { 0 } else { 1 };
            format!(
                "{}{}{}",
                " ".repeat(half_spaces + extra_left_space),
                source,
                " ".repeat(half_spaces)
            )
        }
    }

    /// Determines if whitespace should be rendered at a given position.
    /// This is important for:
    /// 1. Maintaining proper spacing between sibling nodes
    /// 2. Ensuring correct alignment of connections between parents and children
    /// 3. Preserving the tree structure's visual clarity
    fn should_render_whitespace(root: &RenderTree, x: usize, y: usize) -> bool {
        let mut found_children = 0;

        for i in (0..=x).rev() {
            let node = root.get_node(i, y);
            if root.has_node(i, y + 1) {
                found_children += 1;
            }
            if let Some(node) = node {
                if node.child_positions.len() > 1
                    && found_children < node.child_positions.len()
                {
                    return true;
                }

                return false;
            }
        }

        false
    }

    fn split_string_buffer(source: &str, result: &mut Vec<String>) {
        let mut character_pos = 0;
        let mut start_pos = 0;
        let mut render_width = 0;
        let mut last_possible_split = 0;

        let chars: Vec<char> = source.chars().collect();

        while character_pos < chars.len() {
            // Treating each char as width 1 for simplification
            let char_width = 1;

            // Does the next character make us exceed the line length?
            if render_width + char_width > Self::NODE_RENDER_WIDTH - 2 {
                if start_pos + 8 > last_possible_split {
                    // The last character we can split on is one of the first 8 characters of the line
                    // to not create very small lines we instead split on the current character
                    last_possible_split = character_pos;
                }

                result.push(source[start_pos..last_possible_split].to_string());
                render_width = character_pos - last_possible_split;
                start_pos = last_possible_split;
                character_pos = last_possible_split;
            }

            // check if we can split on this character
            if Self::can_split_on_this_char(chars[character_pos]) {
                last_possible_split = character_pos;
            }

            character_pos += 1;
            render_width += char_width;
        }

        if source.len() > start_pos {
            // append the remainder of the input
            result.push(source[start_pos..].to_string());
        }
    }

    fn can_split_on_this_char(c: char) -> bool {
        (!c.is_ascii_digit() && !c.is_ascii_uppercase() && !c.is_ascii_lowercase())
            && c != '_'
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

/// A new type wrapper to display `T` implementing`DisplayAs` using the `Default` mode
pub struct DefaultDisplay<T>(pub T);

impl<T: DisplayAs> fmt::Display for DefaultDisplay<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.0.fmt_as(DisplayFormatType::Default, f)
    }
}

/// A new type wrapper to display `T` implementing `DisplayAs` using the `Verbose` mode
pub struct VerboseDisplay<T>(pub T);

impl<T: DisplayAs> fmt::Display for VerboseDisplay<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.0.fmt_as(DisplayFormatType::Verbose, f)
    }
}

/// A wrapper to customize partitioned file display
#[derive(Debug)]
pub struct ProjectSchemaDisplay<'a>(pub &'a SchemaRef);

impl fmt::Display for ProjectSchemaDisplay<'_> {
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

pub fn display_orderings(f: &mut Formatter, orderings: &[LexOrdering]) -> fmt::Result {
    if !orderings.is_empty() {
        let start = if orderings.len() == 1 {
            ", output_ordering="
        } else {
            ", output_orderings=["
        };
        write!(f, "{start}")?;
        for (idx, ordering) in orderings.iter().enumerate() {
            match idx {
                0 => write!(f, "[{ordering}]")?,
                _ => write!(f, ", [{ordering}]")?,
            }
        }
        let end = if orderings.len() == 1 { "" } else { "]" };
        write!(f, "{end}")?;
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
            self.partition_statistics(None)
        }

        fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
            if partition.is_some() {
                return Ok(Statistics::new_unknown(self.schema().as_ref()));
            }
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
